use rdkafka::consumer::StreamConsumer;
use rdkafka::error::KafkaError;
use rdkafka::util::Timeout;
use rdkafka::ClientContext;
use std::fmt::{Debug, Display};
use std::time::Duration;
use tracing::{event, instrument, Level};

use rdkafka::config::{ClientConfig, RDKafkaLogLevel};
use rdkafka::producer::ProducerContext;

use crate::admin::RedPandaAdminClient;
use crate::config::CompressionType;
use crate::consumer::RedPandaConsumer;
use crate::RedPandaProducer;

#[derive(Debug)]
pub struct RedPandaBuilder {
    client_config: ClientConfig,
    creation_timeout: Timeout,
}

// A simple context to customize the producer behavior and emit a trace every time
// a message is produced
pub struct TracingProducerContext;

impl ClientContext for TracingProducerContext {}

impl ProducerContext for TracingProducerContext {
    type DeliveryOpaque = ();

    fn delivery(
        &self,
        delivery_result: &rdkafka::producer::DeliveryResult<'_>,
        _delivery_opaque: Self::DeliveryOpaque,
    ) {
        match delivery_result {
            Ok(m) => event!(Level::INFO, "Produced {:?}", m),
            Err(e) => {
                event!(Level::ERROR, "Failed to produce message {:?} {}", e.1, e.0);
            }
        }
    }
}

pub enum AutoOffsetReset {
    Smallest,
    Earliest,
    Beginning,
    Largest,
    Latest,
    End,
    Error,
}

impl Display for AutoOffsetReset {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AutoOffsetReset::Smallest => write!(f, "smallest"),
            AutoOffsetReset::Earliest => write!(f, "earliest"),
            AutoOffsetReset::Beginning => write!(f, "beginning"),
            AutoOffsetReset::Largest => write!(f, "largest"),
            AutoOffsetReset::Latest => write!(f, "latest"),
            AutoOffsetReset::End => write!(f, "end"),
            AutoOffsetReset::Error => write!(f, "error"),
        }
    }
}

impl Default for RedPandaBuilder {
    /// Reduce the number of config methods that need to be chained to achieve a sensible
    /// default configuration
    ///
    /// Any of these defaults can be overridden by calling the
    fn default() -> Self {
        let mut builder = Self::new();
        builder.set_auto_offset_reset(AutoOffsetReset::Smallest);
        // builder.enable_partition_eof();
        builder.set_socket_timeout_ms(15000);
        builder.set_session_timeout_ms(15000);
        builder.set_compression_type(CompressionType::Zstd);
        builder.set_rdkafka_log_level(RDKafkaLogLevel::Info);
        builder.set_creation_timeout_ms(15000);

        builder
    }
}

impl RedPandaBuilder {
    pub fn new() -> Self {
        let mut client_config = ClientConfig::new();
        client_config.set("client.id", "redpanda-rs");
        client_config.set("group.id", "default-group");
        // From RedPanda console inferred Kafka version
        client_config.set("broker.version.fallback", "0.10.2.0");
        let creation_timeout = Timeout::Never;
        Self {
            client_config,
            creation_timeout,
        }
    }

    /// Built a RedPandaProducer from the builder's client_config
    #[instrument]
    pub fn build_producer(&self) -> Result<RedPandaProducer, KafkaError> {
        let producer_context = TracingProducerContext {};
        let producer = self
            .client_config
            .create_with_context(producer_context)
            .expect("Producer creation failed");

        RedPandaProducer::new(producer, self.creation_timeout)
    }

    /// Built a RedPandaConsumer from the builder's client_config
    #[instrument]
    pub fn build_consumer(&self) -> Result<RedPandaConsumer, KafkaError> {
        let consumer: StreamConsumer = self
            .client_config
            .create()
            .expect("Consumer creation failed");
        RedPandaConsumer::new(consumer, self.creation_timeout)
    }

    /// Built a RedPandaAdminClient from the builder's client_config
    #[instrument]
    pub async fn build_admin_client(&self) -> Result<RedPandaAdminClient, KafkaError> {
        let admin_client = self
            .client_config
            .create()
            .expect("AdminClient creation failed");
        RedPandaAdminClient::new(admin_client).await
    }

    /////////////////////////////////////////////////////////////////////////////
    //                        Configuration Functions                          //
    //                                                                         //
    // See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md //
    // for complete configuration reference.                                   //
    /////////////////////////////////////////////////////////////////////////////

    /// Set an arbitrary configuration parameter not explicitly defined below
    pub fn set(&mut self, key: &str, value: &str) -> &mut RedPandaBuilder {
        self.client_config.set(key, value);

        self
    }

    /// Set the Broker URLs to connect to
    ///
    /// servers: `host:port,host:port`
    pub fn set_bootstrap_servers(&mut self, servers: &str) -> &mut RedPandaBuilder {
        self.client_config.set("bootstrap.servers", servers);

        self
    }

    /// When set to true, the producer will ensure that messages are successfully produced exactly once and
    /// in the original produce order.
    ///
    /// Default: False
    pub fn enable_idempotence(&mut self) -> &mut RedPandaBuilder {
        self.client_config.set("enable.idempotence", "true");

        self
    }

    /// Emit RD_KAFKA_RESP_ERR__PARTITION_EOF event whenever the consumer reaches the end of a partition.
    ///
    /// rust-rdkafka wraps this error into KafkaError (Partition EOF: 1)
    ///
    /// This is VERY USEFUL for debugging consumption errors...if your consumer group has a saved offset (in the
    /// __consumer_offsets topic) and your consumers keep hanging, this will give you handy error if your
    /// problem is that you're stuck at the end of the partition.
    ///
    /// Default: False
    pub fn enable_partition_eof(&mut self) -> &mut RedPandaBuilder {
        self.client_config.set("enable.partition.eof", "true");

        self
    }

    /// Set the compression type for produced messages
    ///
    /// Default: none
    pub fn set_compression_type(
        &mut self,
        compression_type: CompressionType,
    ) -> &mut RedPandaBuilder {
        self.client_config
            .set("compression.type", &compression_type.to_string());

        self
    }

    /// Action to take when there is no initial offset in offset store or the desired offset is
    /// out of range: 'smallest','earliest' - automatically reset the offset to the smallest
    /// offset, 'largest','latest' - automatically reset the offset to the largest offset,
    /// 'error' - trigger an error (ERR__AUTO_OFFSET_RESET) which is retrieved by consuming
    /// messages and checking 'message->err'.
    ///
    /// Default: largest
    pub fn set_auto_offset_reset(&mut self, offset: AutoOffsetReset) -> &mut RedPandaBuilder {
        self.client_config
            .set("auto.offset.reset", offset.to_string());

        self
    }

    /// Client group session and failure detection timeout.
    ///
    /// Default: 45000ms
    pub fn set_session_timeout_ms(&mut self, timeout_ms: u32) -> &mut RedPandaBuilder {
        self.client_config
            .set("session.timeout.ms", timeout_ms.to_string());

        self
    }

    /// Set the logging level of rdkafka
    ///
    /// TODO: This doesn't seem to be very effective at getting rdkafka to emit logs...
    pub fn set_rdkafka_log_level(&mut self, level: RDKafkaLogLevel) -> &mut RedPandaBuilder {
        self.client_config.set_log_level(level);

        self
    }

    /// Default timeout for network requests
    ///
    /// Default: 60000ms
    pub fn set_socket_timeout_ms(&mut self, timeout_ms: u32) -> &mut RedPandaBuilder {
        self.client_config
            .set("socket.timeout.ms", timeout_ms.to_string());

        self
    }

    /// Maximum time allowed for broker connection setup (TCP connection setup as well SSL and
    /// SASL handshake). If the connection to the broker is not fully functional after this the
    /// connection will be closed and retried.
    ///
    /// Default: 30000ms
    pub fn set_socket_connection_setup_timeout_ms(
        &mut self,
        timeout_ms: u32,
    ) -> &mut RedPandaBuilder {
        self.client_config
            .set("socket.connection.setup.timeout.ms", timeout_ms.to_string());

        self
    }

    /// Client group id string. All clients sharing the same group.id belong to the same group.
    /// group_id is a string, not an int
    pub fn set_group_id(&mut self, group_id: &str) -> &mut RedPandaBuilder {
        self.client_config.set("group.id", group_id.to_string());

        self
    }

    /// Creation timeout in ms
    ///
    /// For consumers, this is the time that fetch_metadata() will wait
    /// For producers, TODO
    pub fn set_creation_timeout_ms(&mut self, timeout_ms: u64) -> &mut RedPandaBuilder {
        self.creation_timeout = Timeout::After(Duration::from_millis(timeout_ms));

        self
    }
}
