use crate::{builder::TracingProducerContext, metadata::RedpandaMetadata};
use chrono::{Utc, DateTime};
use rdkafka::{
    error::KafkaError,
    message::OwnedHeaders,
    producer::{DeliveryFuture, FutureProducer},
    util::Timeout,
};
use tracing::{event, instrument, Level};

type TracingProducer = FutureProducer<TracingProducerContext>;

pub use rdkafka::producer::FutureRecord;
pub use rdkafka::producer::Producer;

/// Represent the time a RedpandaRecord is created (when RedpandaRecord::new() is called)
/// 
/// UTC nanoseconds since epoch
#[derive(Debug, Clone, Copy)]
pub struct TimestampNanos {
    /// UTC DateTime since epoch
    timestamp: DateTime<Utc>,
}

impl Default for TimestampNanos {
    /// Alias for TimestampNanos::now()
    fn default() -> Self {
        TimestampNanos::now()
    }
}

impl TimestampNanos {
    /// Alias for TimestampNanos::now()
    pub fn new() -> Self {
        TimestampNanos::now()
    }

    /// Current UTC nanoseconds since epoch
    pub fn now() -> Self {
        TimestampNanos {
            timestamp: Utc::now(),
        }
    }

    /// Get the UTC timestamp associated with this TimestampNanos as an i64
    /// 
    /// Used to include the timestamp in a RedpandaRecord
    pub fn timestamp_nanos(&self) -> i64 {
        self.timestamp.timestamp_nanos()
    }
}

#[derive(Debug, Clone)]
pub struct RedpandaRecord {
    topic: String,
    key: Vec<u8>,
    payload: Vec<u8>,
    headers: OwnedHeaders,
    created_timestamp: TimestampNanos,
}

impl RedpandaRecord {
    /// Construct a new RedpandaRecord
    /// 
    /// The timestamp of the message is set to the time the RedpandaRecord is created (when this function is called)
    pub fn new(topic: &str, key: Vec<u8>, payload: Vec<u8>, headers: OwnedHeaders) -> Self {
        Self { 
            topic: topic.to_owned(),
            key,
            payload,
            headers,
            created_timestamp: TimestampNanos::now(),
        }
    }
}

impl<'a> From<&'a RedpandaRecord> for FutureRecord<'a, Vec<u8>, Vec<u8>> {
    /// Create a FutureRecord that lives as long as the RedpandaRecord it is created from
    /// 
    /// Timestamp is set to create time of the RedpandaRecord
    fn from(r: &'a RedpandaRecord) -> Self {
        
        FutureRecord {
            topic: &r.topic,
            partition: Option::None,
            payload: Some(&r.payload),
            key: Some(&r.key),
            timestamp: Some(r.created_timestamp.timestamp_nanos()),
            headers: Some(r.headers.clone()),
        }
    }
}

/// Derive Clone is fine because the underlying rdkafka::producer::FutureProducer is meant
/// to be cloned cheaply.
/// ref: https://docs.rs/rdkafka/0.28.0/rdkafka/producer/struct.FutureProducer.html
#[derive(Clone)]
pub struct RedpandaProducer {
    pub producer: TracingProducer,
}

impl RedpandaProducer {
    /// Create a new RedpandaProducer
    #[instrument(skip(producer))]
    pub fn new(producer: TracingProducer, request_timeout: Timeout) -> Result<Self, KafkaError> {
        let client = producer.client();
        match client.fetch_metadata(Option::None, request_timeout) {
            Ok(m) => {
                let m: RedpandaMetadata = m.into();
                event!(
                    Level::INFO,
                    "Connected consumer to Redpanda cluster {:?}",
                    m
                );
                m
            }
            Err(e) => return Err(e),
        };
        Ok(Self { producer })
    }

    /// Re-implementation of FutureProducer.send_result that takes a RedpandaRecord instead of a FutureRecord
    /// 
    /// RedpandaRecords are normal structs that own all their data & are much nicer to pass around vs FutureRecords
    /// that don't own the data in topic, payload, and key. These design decisions in rdkafka make it necessary
    /// to have a separate RedpandaRecord struct and implement the From trait
    pub fn send_result<'a>(&self, record: &'a RedpandaRecord) -> Result<DeliveryFuture, (KafkaError, FutureRecord<'a, Vec<u8>, Vec<u8>>)> {     
        self.producer.send_result(record.into())
    }

    #[deprecated(since = "0.3", note = "All record production should be through the send_result method")]
    pub fn send_result_topic_key_payload(
        &self,
        topic: &str,
        key: &Vec<u8>,
        payload: &Vec<u8>,
    ) -> Result<DeliveryFuture, KafkaError> {
        let record = FutureRecord {
            topic,
            partition: Option::None,
            payload: Some(payload),
            key: Some(key),
            timestamp: Option::None,
            headers: Option::None,
        };
        match self.producer.send_result(record) {
            Ok(d) => Ok(d),
            Err(e) => {
                event!(Level::ERROR, "Failed to queue message {:?} {}", e.1, e.0);
                Err(e.0)
            }
        }
    }

    #[deprecated(since = "0.3", note = "All record production should be through the send_result method")]
    pub fn send_result_topic_partition_payload_key_headers(
        &self,
        topic: &str,
        partition: Option<i32>,
        payload: &Vec<u8>,
        key: &Vec<u8>,
        headers: OwnedHeaders,
    ) -> Result<DeliveryFuture, KafkaError> {
        let record = FutureRecord {
            topic,
            partition,
            payload: Some(payload),
            key: Some(key),
            timestamp: Option::None,
            headers: Some(headers),
        };
        match self.producer.send_result(record) {
            Ok(d) => Ok(d),
            Err(e) => {
                event!(Level::ERROR, "Failed to queue message {:?} {}", e.1, e.0);
                Err(e.0)
            }
        }
    }
}
