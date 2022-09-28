pub mod admin;
pub mod builder;
pub mod config;
pub mod consumer;
pub mod metadata;
pub mod producer;

#[cfg(test)]
mod tests;

pub use admin::RedPandaAdminClient;
pub use builder::RedPandaBuilder;
use chrono::Utc;
pub use consumer::RedPandaConsumer;
pub use producer::RedPandaProducer;

pub use rdkafka::error;
pub use rdkafka::groups;
pub use rdkafka::message;
use rdkafka::message::OwnedHeaders;
pub use rdkafka::statistics;
pub use rdkafka::types;

pub struct RedPandaRecord {
    key: Option<Vec<u8>>,
    value: Option<Vec<u8>>,
    headers: Option<OwnedHeaders>,
    timestamp: Option<Utc>,
}