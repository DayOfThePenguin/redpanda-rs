use rdkafka::consumer::StreamConsumer;
use rdkafka::error::KafkaError;
use rdkafka::util::Timeout;
use std::fmt::Debug;
use std::time::Duration;
use tracing::instrument;

use rdkafka::config::{ClientConfig, RDKafkaLogLevel};
use rdkafka::producer::FutureProducer;

use crate::config::CompressionType;
use crate::consumer::RedPandaConsumer;

#[derive(Debug)]
pub struct RedPandaBuilder {
    client_config: ClientConfig,
    creation_timeout: Timeout,
}

impl RedPandaBuilder {
    pub fn new() -> Self {
        let mut client_config = ClientConfig::new();
        client_config.set("group.id", "1");
        let creation_timeout = Timeout::After(Duration::from_secs(3));
        Self {
            client_config,
            creation_timeout,
        }
    }

    /// Set the Broker URLs to connect to
    ///
    /// servers: `host:port,host:port`
    pub fn set_bootstrap_servers<'a>(&'a mut self, servers: &str) -> &'a mut RedPandaBuilder {
        self.client_config.set("bootstrap.servers", servers);

        self
    }

    /// Set the compression type for produced messages
    pub fn set_compression_type<'a>(
        &'a mut self,
        compression_type: CompressionType,
    ) -> &'a mut RedPandaBuilder {
        self.client_config
            .set("compression.type", &compression_type.to_string());

        self
    }

    pub fn set_session_timeout_ms<'a>(&'a mut self, timeout_ms: u32) -> &'a mut RedPandaBuilder {
        self.client_config
            .set("session.timeout.ms", timeout_ms.to_string());

        self
    }

    pub fn set_rdkafka_log_level<'a>(
        &'a mut self,
        level: RDKafkaLogLevel,
    ) -> &'a mut RedPandaBuilder {
        self.client_config.set_log_level(level);

        self
    }

    pub fn set_socket_timeout_ms<'a>(&'a mut self, timeout_ms: u32) -> &'a mut RedPandaBuilder {
        self.client_config
            .set("socket.timeout.ms", timeout_ms.to_string());

        self
    }

    pub fn set_socket_connection_setup_timeout_ms<'a>(
        &'a mut self,
        timeout_ms: u32,
    ) -> &'a mut RedPandaBuilder {
        self.client_config
            .set("socket.connection.setup.timeout.ms", timeout_ms.to_string());

        self
    }

    /// Client group id string. All clients sharing the same group.id belong to the same group.
    /// group_id is a string, not an int
    pub fn set_group_id<'a>(&'a mut self, group_id: &str) -> &'a mut RedPandaBuilder {
        self.client_config.set("group.id", group_id.to_string());

        self
    }

    /// Creation timeout in ms
    ///
    /// For consumers, this is the time that fetch_metadata() will wait
    /// For producers, TODO
    pub fn set_creation_timeout_ms<'a>(&'a mut self, timeout_ms: u64) -> &'a mut RedPandaBuilder {
        self.creation_timeout = Timeout::After(Duration::from_millis(timeout_ms));

        self
    }

    #[instrument]
    pub fn build_producer(&self) -> FutureProducer {
        todo!();
        self.client_config
            .create()
            .expect("Producer creation failed")
    }

    #[instrument]
    pub fn build_consumer(&self) -> Result<RedPandaConsumer, KafkaError> {
        let consumer: StreamConsumer = self
            .client_config
            .create()
            .expect("Consumer creation failed");
        RedPandaConsumer::new(consumer, self.creation_timeout)
    }

    // pub fn build_admin_client(&self) -> AdminClient {

    // }
}
