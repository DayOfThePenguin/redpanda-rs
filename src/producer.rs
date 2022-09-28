use crate::{builder::TracingProducerContext, metadata::RedPandaMetadata};
use rdkafka::{
    error::KafkaError,
    message::OwnedHeaders,
    producer::{DeliveryFuture, FutureProducer, Producer},
    util::Timeout,
};
use tracing::{event, instrument, Level};

type TracingProducer = FutureProducer<TracingProducerContext>;

pub use rdkafka::producer::FutureRecord;

pub struct RedPandaProducer {
    pub producer: TracingProducer,
}

impl RedPandaProducer {
    /// Create a new RedPandaProducer
    #[instrument(skip(producer))]
    pub fn new(producer: TracingProducer, request_timeout: Timeout) -> Result<Self, KafkaError> {
        let client = producer.client();
        match client.fetch_metadata(Option::None, request_timeout) {
            Ok(m) => {
                let m: RedPandaMetadata = m.into();
                event!(
                    Level::INFO,
                    "Connected consumer to RedPanda cluster {:?}",
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
