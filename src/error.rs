use std::array::TryFromSliceError;

pub use rdkafka::error::*;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum RedPandaError {
    #[error("RedPanda encountered a Kafka error")]
    Kafka(#[from] rdkafka::error::KafkaError),
    #[error("unknown RedPanda error")]
    Unknown,
}

#[derive(Error, Debug)]
pub enum RecordError {
    #[error("key doesn't deserialize to a DateTime<Utc>")]
    KeyDeserializeError(#[from] TryFromSliceError),
    #[error("unknown Record error")]
    Unknown,
}
