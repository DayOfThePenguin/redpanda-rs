pub mod admin;
pub mod builder;
pub mod config;
pub mod consumer;
pub mod metadata;
pub mod producer;
pub mod error;

#[cfg(test)]
mod tests;

pub use admin::RedPandaAdminClient;
pub use builder::RedPandaBuilder;
use chrono::DateTime;
use chrono::TimeZone;
use chrono::Utc;
pub use consumer::RedPandaConsumer;
use error::RecordError;
pub use producer::RedPandaProducer;
pub use rdkafka::groups;
pub use rdkafka::message;
pub use rdkafka::statistics;
pub use rdkafka::types;
use tracing::instrument;

/// Serialize a UTC DateTime with nanosecond precision to a byte Vec
pub fn serialize_key(key: DateTime<Utc>) -> Vec<u8> {
    key.timestamp_nanos().to_le_bytes().to_vec()
}

/// Deserialize a byte vector to a UTC DateTime or return a KeyDeserializeError
#[instrument]
pub fn deserialize_key(input: Vec<u8>) -> Result<DateTime<Utc>, RecordError> {
    let le_bytes: [u8; 8] = match input.try_into() {
        Ok(b) => b,
        Err(e) => {
            let err = RecordError::KeyDeserializeError { value: e };
            return Err(err)
        },
    };
    let timestamp = i64::from_le_bytes(le_bytes);
    Ok(Utc.timestamp_nanos(timestamp))
}