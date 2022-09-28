use thiserror::Error;
pub use rdkafka::error::*;

#[derive(Error, Debug)]
pub enum RedPandaError {
    #[error("RedPanda encountered a Kafka error")]
    Kafka(#[from] rdkafka::error::KafkaError),
    #[error("unknown RedPanda error")]
    Unknown,
}

#[derive(Error, Debug)]
pub enum RecordError {
    #[error("key {value:?} doesn't deserialize to a DateTime<Utc>")]
    KeyDeserializeError{
        value: Vec<u8>,
    },
    #[error("unknown Record error")]
    Unknown,
}
