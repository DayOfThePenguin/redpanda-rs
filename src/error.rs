use std::array::TryFromSliceError;

pub use rdkafka::error::*;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum RedpandaError {
    #[error("Redpanda encountered a Kafka error")]
    Kafka(#[from] rdkafka::error::KafkaError),
    #[error("unknown Redpanda error")]
    Unknown,
}

#[derive(Error, Debug)]
pub enum RecordError {
    #[error("key doesn't deserialize to a DateTime<Utc>")]
    KeyDeserializeError(#[from] TryFromSliceError),
    #[error("unknown Record error")]
    Unknown,
}
