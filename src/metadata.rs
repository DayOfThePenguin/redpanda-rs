use rdkafka::{
    metadata::{Metadata, MetadataBroker, MetadataPartition, MetadataTopic},
    types::RDKafkaRespErr,
};
use std::convert::From;

#[derive(Debug)]
pub struct RedPandaMetadata {
    /// ID of the broker originating this metadata
    pub orig_broker_id: i32,
    /// hostname of the broker originating this metadata
    pub orig_broker_name: String,
    /// metadata information for all the brokers in the cluster
    pub brokers: Vec<RedPandaBroker>,
    /// metadata information for all the topics in the cluster
    pub topics: Vec<RedPandaTopic>,
}

impl RedPandaMetadata {
    /// Get a sorted list of the names of all topics in the cluster
    pub fn topic_names(&self) -> Vec<String> {
        let mut topic_names = Vec::new();
        for topic in &self.topics {
            topic_names.push(topic.name.clone());
        }
        topic_names.sort();

        topic_names
    }
}

impl From<Metadata> for RedPandaMetadata {
    fn from(m: Metadata) -> Self {
        let mut brokers: Vec<RedPandaBroker> = Vec::new();
        for b in m.brokers() {
            brokers.push(b.into());
        }

        let mut topics: Vec<RedPandaTopic> = Vec::new();
        for t in m.topics() {
            topics.push(t.into());
        }
        Self {
            orig_broker_id: m.orig_broker_id(),
            orig_broker_name: m.orig_broker_name().to_owned(),
            brokers,
            topics,
        }
    }
}

#[derive(Debug)]
pub struct RedPandaBroker {
    /// ID of broker
    pub id: i32,
    /// hostname of broker
    pub hostname: String,
    /// port of broker
    pub port: u16,
}

impl From<&MetadataBroker> for RedPandaBroker {
    fn from(metadata_broker: &MetadataBroker) -> Self {
        let port = metadata_broker
            .port()
            .try_into()
            .expect("Failed to convert port to u16; max port number is 65,535");
        Self {
            id: metadata_broker.id(),
            hostname: metadata_broker.host().to_owned(),
            port,
        }
    }
}

#[derive(Debug)]
pub struct RedPandaTopic {
    /// name of the topic
    pub name: String,
    /// partition metadata information for all partitions
    pub partitions: Vec<RedPandaPartition>,
    /// metadata error for the topic, or `None` if there was no error
    pub error: Option<RDKafkaRespErr>,
}

impl From<&MetadataTopic> for RedPandaTopic {
    fn from(metadata_topic: &MetadataTopic) -> Self {
        let mut partitions: Vec<RedPandaPartition> = Vec::new();
        for p in metadata_topic.partitions() {
            partitions.push(p.into());
        }

        Self {
            name: metadata_topic.name().to_owned(),
            partitions,
            error: metadata_topic.error(),
        }
    }
}

#[derive(Debug)]
pub struct RedPandaPartition {
    /// id of the partition
    pub id: i32,
    /// broker id of the leader broker for the partition
    pub leader: i32,
    /// metadata error for the partition, or `None` if there is no error
    pub error: Option<RDKafkaRespErr>,
    /// broker id of replicas
    pub replicas: Vec<i32>,
    /// broker IDs of the in-sync replicas
    pub in_sync_replicas: Vec<i32>,
}

/// Construct RedPandaPartition from rdkafka::metadata::MetadataPartition
impl From<&MetadataPartition> for RedPandaPartition {
    fn from(metadata_partition: &MetadataPartition) -> Self {
        Self {
            id: metadata_partition.id(),
            leader: metadata_partition.leader(),
            error: metadata_partition.error(),
            replicas: metadata_partition.replicas().to_owned(),
            in_sync_replicas: metadata_partition.isr().to_owned(),
        }
    }
}
