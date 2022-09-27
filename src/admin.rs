use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, ResourceSpecifier};
use rdkafka::client::DefaultClientContext;
use rdkafka::error::KafkaError;
use tracing::{event, instrument, Level};

type DefaultAdminClient = AdminClient<DefaultClientContext>;

pub struct RedPandaAdminClient {
    admin_client: DefaultAdminClient,
}

impl RedPandaAdminClient {
    /// Construct a new RedPandaAdminClient
    #[instrument(skip(admin_client))]
    pub async fn new(admin_client: DefaultAdminClient) -> Result<Self, KafkaError> {
        let opts = AdminOptions::new();
        let configs = ResourceSpecifier::Topic("_schemas");
        match admin_client.describe_configs([&configs], &opts).await {
            Ok(_) => {
                event!(Level::INFO, "Connected admin client to RedPanda cluster",);
            }
            Err(e) => return Err(e),
        };

        Ok(Self { admin_client })
    }

    // TODO: This is unexpectedly broken...librdkafka will return successful topic creation but not actually create the topic...
    /// Configure and create a topic
    #[instrument(skip(self))]
    pub async fn create_topic(
        &self,
        name: &str,
        num_partitions: u16,
        replication_factor: u16,
    ) -> Result<(), KafkaError> {
        let opts = AdminOptions::new();
        // Fixed replication = all partitions have the same replication factor
        let replication = rdkafka::admin::TopicReplication::Fixed(replication_factor.into());
        let config = vec![
            ("compression.type", "zstd"),
            ("auto.offset.reset", "beginning"),
        ];

        let topic = NewTopic {
            name,
            num_partitions: num_partitions.into(),
            replication,
            config,
        };

        match self.admin_client.create_topics([&topic], &opts).await {
            Ok(results_vec) => {
                // Since we're only creating topics one at a time, we can safely just match the first element
                match &results_vec[0] {
                    Ok(_) => {
                        event!(
                            Level::INFO,
                            "Created topic {} with {} partitions, replication factor {}",
                            name,
                            num_partitions,
                            replication_factor
                        );
                        Ok(())
                    }
                    Err(e) => {
                        event!(
                            Level::ERROR,
                            "Failed to create topic {}, {:?}",
                            e.0, // topic name
                            e.1  // RDKafkaErrorCode
                        );
                        Err(KafkaError::AdminOp(e.1))
                    }
                }
            }
            Err(e) => Err(e),
        }
    }

    /// Delete a topic
    #[instrument(skip(self))]
    pub async fn delete_topic(&self, name: &str) -> Result<(), KafkaError> {
        let opts = AdminOptions::new();
        match self.admin_client.delete_topics(&[name], &opts).await {
            Ok(results_vec) => {
                // Since we're only deleting topics one at a time, we can safely just match the first element
                match &results_vec[0] {
                    Ok(deleted_name) => {
                        event!(Level::INFO, "Deleted topic {}", deleted_name);
                        Ok(())
                    }
                    Err(e) => {
                        event!(
                            Level::ERROR,
                            "Failed to delete topic {}, {:?}",
                            e.0, // topic name
                            e.1  // RDKafkaErrorCode
                        );
                        Err(KafkaError::AdminOp(e.1))
                    }
                }
            }
            Err(e) => Err(e),
        }
    }
}
