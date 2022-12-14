use rdkafka::{
    consumer::{MessageStream, StreamConsumer},
    error::KafkaError,
    message::BorrowedMessage,
    util::Timeout,
};
use tracing::{event, instrument, Level};

use crate::metadata::RedpandaMetadata;

pub use rdkafka::consumer::Consumer;
pub use rdkafka::consumer::CommitMode;

pub struct RedpandaConsumer {
    pub consumer: StreamConsumer,
    request_timeout: Timeout,
}

impl RedpandaConsumer {
    /// Create a new RedpandaConsumer, validating that the brokers respond to connections within timeout
    #[instrument(skip(consumer))]
    pub fn new(consumer: StreamConsumer, request_timeout: Timeout) -> Result<Self, KafkaError> {
        match consumer.fetch_metadata(Option::None, request_timeout) {
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

        Ok(Self {
            consumer,
            request_timeout,
        })
    }

    /// Get consumer metadata
    pub fn fetch_metadata(&self) -> Result<RedpandaMetadata, KafkaError> {
        let metadata = self
            .consumer
            .fetch_metadata(Option::None, self.request_timeout)?
            .into();

        Ok(metadata)
    }

    /// Subscribe the consumer to an array of topic names, checking that the topic names are valid
    /// 
    /// Subsequent calls will replace existing topics and only subscribe to the new topics provided
    #[instrument(skip(self))]
    pub fn subscribe(&self, topic_names: &[&str]) -> Result<(), KafkaError> {
        let cluster_topic_names = self.fetch_metadata()?.topic_names();

        for topic in topic_names {
            let valid_name = cluster_topic_names.binary_search(&topic.to_owned().to_owned());
            if valid_name.is_err() {
                let e = KafkaError::Subscription(format!("Invalid topic name {}", topic));
                return Err(e);
            }
        }

        match self.consumer.subscribe(topic_names) {
            Ok(_) => {
                event!(Level::INFO, "Subscribed to topics {:?}", topic_names);
                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    /// Get the names of the currently subscribed topics
    pub fn get_subscription_topic_names(&self) -> Vec<String> {
        let topic_partition_list = self.consumer.subscription().unwrap();
        let mut topic_names = Vec::new();
        for elem in topic_partition_list.elements() {
            topic_names.push(elem.topic().to_owned());
        }

        topic_names
    }

    /// Receive a single message
    pub async fn recv(&self) -> Result<BorrowedMessage<'_>, KafkaError> {
        self.consumer.recv().await
    }

    /// Create a message stream from the subscribed topics
    pub fn stream(&self) -> MessageStream {
        self.consumer.stream()
    }
}
