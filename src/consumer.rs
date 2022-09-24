use std::time::Duration;

use rdkafka::{
    consumer::{Consumer, StreamConsumer},
    error::KafkaError,
    util::Timeout,
};
use tracing::{event, instrument, Level};

use crate::metadata::RedPandaMetadata;

pub struct RedPandaConsumer {
    consumer: StreamConsumer,
}

impl RedPandaConsumer {
    /// Create a new RedPandaConsumer, validating that the brokers respond to connections within timeout
    #[instrument(skip(consumer))]
    pub fn new(consumer: StreamConsumer, timeout: Timeout) -> Result<Self, KafkaError> {
        match consumer.fetch_metadata(Option::None, timeout) {
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

        Ok(Self { consumer })
    }

    pub fn fetch_metadata(&self, timeout: Timeout) -> Result<RedPandaMetadata, KafkaError> {
        match self.consumer.fetch_metadata(Option::None, timeout) {
            Ok(m) => Ok(m.into()),
            Err(e) => Err(e),
        }
    }
}
