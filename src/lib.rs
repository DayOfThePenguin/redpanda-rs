use rdkafka::admin::AdminClient;
use rdkafka::client;
use rdkafka::consumer::StreamConsumer;
use std::fmt::{Debug, Display};
use tracing::{event, instrument, Level};

use rdkafka::config::{ClientConfig, RDKafkaLogLevel};
use rdkafka::producer::{FutureProducer, FutureRecord};

#[cfg(test)]
mod tests;

mod consumer;
mod metadata;

#[derive(Debug)]
pub struct RedPandaBuilder {
    client_config: ClientConfig,
}

pub enum CompressionType {
    /// No compression
    None,
    /// zstd compression
    Zstd,
    /// gzip compression
    Gzip,
}

impl Display for CompressionType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CompressionType::None => write!(f, ""),
            CompressionType::Zstd => write!(f, "zstd"),
            CompressionType::Gzip => write!(f, "gzip"),
        }
    }
}

impl RedPandaBuilder {
    pub fn new() -> Self {
        let mut client_config = ClientConfig::new();
        client_config.set("group.id", "1");
        Self { client_config }
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

    #[instrument]
    pub fn build_producer(&self) -> FutureProducer {
        self.client_config
            .create()
            .expect("Producer creation failed")
    }

    #[instrument]
    pub fn build_consumer(&self) -> StreamConsumer {
        self.client_config
            .create()
            .expect("Consumer creation failed")
    }

    // pub fn build_admin_client(&self) -> AdminClient {

    // }
}
