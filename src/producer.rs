use crate::{builder::TracingProducerContext, metadata::RedpandaMetadata};
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
