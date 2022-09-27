use crate::{builder::TracingProducerContext, metadata::RedPandaMetadata};
use rdkafka::{
    error::KafkaError,
    producer::{DeliveryFuture, FutureProducer, FutureRecord, Producer},
    util::Timeout,
};
use tracing::{event, instrument, Level};

type TracingProducer = FutureProducer<TracingProducerContext>;

pub struct RedPandaProducer {
    producer: TracingProducer,
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

    pub fn send_result(
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
}
