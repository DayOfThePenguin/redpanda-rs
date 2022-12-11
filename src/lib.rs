pub mod admin;
pub mod builder;
pub mod config;
pub mod consumer;
pub mod error;
pub mod metadata;
pub mod producer;

#[cfg(test)]
mod tests;

pub use admin::RedpandaAdminClient;
pub use builder::RedpandaBuilder;
pub use consumer::RedpandaConsumer;
pub use producer::RedpandaProducer;
pub use rdkafka::groups;
pub use rdkafka::message;
pub use rdkafka::statistics;

pub mod types {
    pub use rdkafka::types::*;
    pub use rdkafka::util::Timeout;
}