use std::fmt::Debug;
use tracing::{event, instrument, Level};

#[cfg(test)]
mod tests;

pub struct RedPandaClient {
    client: rskafka::client::Client,
}

impl Debug for RedPandaClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedPandaClient")
            .field("client", &self.client)
            .finish()
    }
}

impl RedPandaClient {
    /// Create a RedPandaClient
    /// TODO: add sensible failure conditions
    #[instrument]
    pub async fn new(hosts: Vec<String>) -> Self {
        let client = rskafka::client::ClientBuilder::new(hosts)
            .build()
            .await
            .unwrap();

        Self { client }
    }

    /// Create a new topic on a RedPandaClient
    /// TODO: define custom error type and make this function return a Result<> with that type
    /// TODO: separate out the TopicAlreadyExists error and let this one go gracefully after logging
    ///       the occurrence
    #[instrument]
    pub async fn create_topic(
        &self,
        topic_name: &str,
        num_partitions: i32,
        replication_factor: i16,
        timeout_ms: i32,
    ) {
        let controller_client = self.client.controller_client().unwrap();
        match controller_client
            .create_topic(topic_name, 3, 1, 5_000)
            .await
        {
            Ok(_) => {
                event!(Level::INFO, "Created topic!");
            }
            Err(e) => {
                // Will return an error of type I can't determine that ServerError(TopicAlreadyExists, "")
                event!(Level::WARN, "{:?}", e);
            }
        };
    }

    pub fn partition_client(
        &self,
        topic: &str,
        partition: i32,
    ) -> rskafka::client::partition::PartitionClient {
        self.client.partition_client(topic, partition).unwrap()
    }
}
