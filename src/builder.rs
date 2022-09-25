use rdkafka::consumer::StreamConsumer;
use rdkafka::error::KafkaError;
use rdkafka::util::Timeout;
use rdkafka::ClientContext;
use std::fmt::Debug;
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

impl Default for RedPandaBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl RedPandaBuilder {
    pub fn new() -> Self {
        let mut client_config = ClientConfig::new();
        client_config.set("client.id", "redpanda-rs");
        client_config.set("group.id", "asdf");
        client_config.set("enable.auto.commit", "true");
        client_config.set("enable.partition.eof", "false");
        client_config.set("debug", "consumer,cgrp,topic,fetch");
        let creation_timeout = Timeout::After(Duration::from_secs(3));
        Self {
            client_config,
            creation_timeout,
        }
    }

    /// Set the Broker URLs to connect to
    ///
    /// servers: `host:port,host:port`
    pub fn set_bootstrap_servers(&mut self, servers: &str) -> &mut RedPandaBuilder {
        self.client_config.set("bootstrap.servers", servers);

        self
    }

    /// Set the compression type for produced messages
    pub fn set_compression_type(
        &mut self,
        compression_type: CompressionType,
    ) -> &mut RedPandaBuilder {
        self.client_config
            .set("compression.type", &compression_type.to_string());

        self
    }

    pub fn set_session_timeout_ms(&mut self, timeout_ms: u32) -> &mut RedPandaBuilder {
        self.client_config
            .set("session.timeout.ms", timeout_ms.to_string());

        self
    }

    pub fn set_rdkafka_log_level(
        &mut self,
        level: RDKafkaLogLevel,
    ) -> &mut RedPandaBuilder {
        self.client_config.set_log_level(level);

        self
    }

    pub fn set_socket_timeout_ms(&mut self, timeout_ms: u32) -> &mut RedPandaBuilder {
        self.client_config
            .set("socket.timeout.ms", timeout_ms.to_string());

        self
    }

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

    #[instrument]
    pub fn build_producer(&self) -> Result<RedPandaProducer, KafkaError> {
        let producer_context = TracingProducerContext {};
        let producer = self
            .client_config
            .create_with_context(producer_context)
            .expect("Producer creation failed");

        RedPandaProducer::new(producer, self.creation_timeout)
    }

    #[instrument]
    pub fn build_consumer(&self) -> Result<RedPandaConsumer, KafkaError> {
        let consumer: StreamConsumer = self
            .client_config
            .create()
            .expect("Consumer creation failed");
        RedPandaConsumer::new(consumer, self.creation_timeout)
    }

    #[instrument]
    pub async fn build_admin_client(&self) -> Result<RedPandaAdminClient, KafkaError> {
        let admin_client = self
            .client_config
            .create()
            .expect("AdminClient creation failed");
        RedPandaAdminClient::new(admin_client).await
    }
}
