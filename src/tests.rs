use std::time::Duration;

use rdkafka::{config::RDKafkaLogLevel, util::Timeout};
use rdkafka::types::RDKafkaErrorCode;
use rdkafka::consumer::Consumer;
use rdkafka::message::Message;
use rdkafka::topic_partition_list::Offset;
use tracing::{event, Level};
use tracing_test::traced_test;
use futures::stream::StreamExt;

use crate::metadata::RedPandaMetadata;
use crate::{builder::RedPandaBuilder, config::CompressionType};

/// Does RedPandaConsumer fail to construct with the proper error code if the bootstrap_server doesn't exist?
#[tokio::test]
#[traced_test]
pub async fn test_consumer_invalid_server() {
    let mut b = RedPandaBuilder::new();
    // Assumes you don't have a RedPanda broker running on this port
    b.set_bootstrap_servers("localhost:9000");
    b.set_socket_timeout_ms(3000);
    b.set_socket_connection_setup_timeout_ms(3000);
    b.set_rdkafka_log_level(RDKafkaLogLevel::Info);
    let err = b.build_consumer();

    assert!(err.is_err());

    // This has to unwrap successfully because of the is_err() check above
    let err_code = err.err().unwrap().rdkafka_error_code().unwrap();

    assert!(err_code == RDKafkaErrorCode::BrokerTransportFailure);
}

/// Does RedPandaConsumer successfully construct if some of the bootstrap_servers are invalid?
#[tokio::test]
#[traced_test]
pub async fn test_consumer_some_bad_servers() {
    let mut b = RedPandaBuilder::new();
    // Assumes you don't have a RedPanda broker running on port 9000
    // ...and that you DO have one running on port 9010
    b.set_bootstrap_servers("localhost:9000,localhost:9010");
    b.set_creation_timeout_ms(3000);
    b.set_rdkafka_log_level(RDKafkaLogLevel::Info);
    let consumer = b.build_consumer();

    assert!(consumer.is_ok());
}

/// Does RedPandaConsumer successfully construct if bootstrap_servers is valid?
#[tokio::test]
#[traced_test]
pub async fn test_consumer_valid_server() {
    let mut b = RedPandaBuilder::new();
    // Assumes you have a RedPanda broker running on 9010, 9011, 9012
    b.set_bootstrap_servers("localhost:9010,localhost:9011,localhost:9012");
    b.set_creation_timeout_ms(3000);
    b.set_rdkafka_log_level(RDKafkaLogLevel::Info);
    let consumer = b.build_consumer();

    assert!(consumer.is_ok());
}

/// Test invalid topic response on a valid broker
#[tokio::test]
#[traced_test]
pub async fn test_consumer_invalid_topic() {
    let mut b = RedPandaBuilder::new();
    // Assumes you have a RedPanda broker running on this port
    b.set_bootstrap_servers("localhost:9010");
    b.set_creation_timeout_ms(3000);
    b.set_rdkafka_log_level(RDKafkaLogLevel::Info);
    let invalid_topic = "i_do_not_exist";

    let consumer = b.build_consumer().unwrap();

    let err = consumer.subscribe(&[invalid_topic]);
    assert!(err.is_err());
}

/// Test listing cluster topic names
#[tokio::test]
#[traced_test]
pub async fn test_metadata_topic_names() {
    let mut b = RedPandaBuilder::new();
    // Assumes you have a RedPanda broker running on this port
    b.set_bootstrap_servers("localhost:9010");
    b.set_creation_timeout_ms(3000);
    b.set_rdkafka_log_level(RDKafkaLogLevel::Info);
    let consumer = b.build_consumer().unwrap();
    let metadata = consumer.fetch_metadata().unwrap();
    let topic_names = metadata.topic_names();

    // All RedPanda clusters contain at least these topic names
    assert!(topic_names.contains(&"_schemas".to_owned()));
    assert!(topic_names.contains(&"__consumer_offsets".to_owned()));

    event!(Level::INFO, "{:?}", topic_names);
}

/// Test listing consumer subscription names
/// Validate that if you call subscribe() multiple times, subsequent calls replace the consumer's
/// subscriptions
#[tokio::test]
#[traced_test]
pub async fn test_consumer_subscription() {
    let mut b = RedPandaBuilder::new();
    // Assumes you have a RedPanda broker running on this port
    b.set_bootstrap_servers("localhost:9010");
    b.set_creation_timeout_ms(3000);
    b.set_rdkafka_log_level(RDKafkaLogLevel::Info);
    let consumer = b.build_consumer().unwrap();

    // Subscribe to _schemas & verify subscription
    consumer.subscribe(&["_schemas"]).unwrap();
    let subscriptions = consumer.get_subscription_topic_names();
    assert_eq!(subscriptions, vec!["_schemas"]);

    // Subscribe to __consumer_offsets & verify this completely replaces the original subscription
    consumer
        .subscribe(&["__consumer_offsets"].to_owned())
        .unwrap();
    let subscriptions = consumer.get_subscription_topic_names();
    assert_eq!(subscriptions, vec!["__consumer_offsets"]);
}

/// Test RedPandaConsumer consumption recv()
#[tokio::test]
#[traced_test]
pub async fn test_consumer_recv() {
    let mut b = RedPandaBuilder::new();
    // Assumes you have a RedPanda broker running on this port
    b.set_bootstrap_servers("localhost:9010");
    b.set_creation_timeout_ms(3000);
    b.set_rdkafka_log_level(RDKafkaLogLevel::Info);
    let consumer = b.build_consumer().unwrap();

    let topic = "test_producer_topic";
    // Subscribe to _schemas & verify subscription
    consumer.subscribe(&[topic]).unwrap();
    let subscriptions = consumer.get_subscription_topic_names();
    assert_eq!(subscriptions, vec![topic]);

    let meta: RedPandaMetadata = consumer.fetch_metadata().unwrap().into();
    event!(Level::INFO, "{:?}", meta);

    event!(Level::INFO, "{:?}", consumer.consumer.subscription().unwrap());
    consumer.consumer.seek(topic, 0, Offset::Beginning, Timeout::After(Duration::from_secs(1))).unwrap();
    event!(Level::INFO, "{:?}", consumer.consumer.subscription().unwrap());
    // consumer.consumer.stream().next().await;
}

/// Does RedPandaProducer fail to construct with the proper error code if the bootstrap_server doesn't exist?
#[tokio::test]
#[traced_test]
pub async fn test_producer_invalid_server() {
    let mut b = RedPandaBuilder::new();
    // Assumes you don't have a RedPanda broker running on this port
    b.set_bootstrap_servers("localhost:9000");
    b.set_creation_timeout_ms(3000);
    b.set_rdkafka_log_level(RDKafkaLogLevel::Info);
    let err = b.build_producer();

    assert!(err.is_err());

    // This has to unwrap successfully because of the is_err() check above
    let err_code = err.err().unwrap().rdkafka_error_code().unwrap();

    assert!(err_code == RDKafkaErrorCode::BrokerTransportFailure);
}

/// Does RedPandaProducer successfully construct if some of the bootstrap_servers are invalid?
#[tokio::test]
#[traced_test]
pub async fn test_producer_some_bad_servers() {
    let mut b = RedPandaBuilder::new();
    // Assumes you don't have a RedPanda broker running on port 9000
    // ...and that you DO have one running on port 9010
    b.set_bootstrap_servers("localhost:9000,localhost:9010");
    b.set_creation_timeout_ms(3000);
    b.set_rdkafka_log_level(RDKafkaLogLevel::Info);
    let consumer = b.build_consumer();

    assert!(consumer.is_ok());
}

/// Does RedPandaProducer successfully construct if bootstrap_servers is valid?
#[tokio::test]
#[traced_test]
pub async fn test_producer_valid_server() {
    let mut b = RedPandaBuilder::new();
    // Assumes you have a RedPanda broker running on ports 9010, 9011, 9012
    b.set_bootstrap_servers("localhost:9010,localhost:9011,localhost:9012");
    b.set_creation_timeout_ms(3000);
    b.set_rdkafka_log_level(RDKafkaLogLevel::Info);
    let producer = b.build_producer();

    assert!(producer.is_ok());
}

/// Does RedPandaProducer successfully produce to a valid topic that can be consumed by RedPandaConsumer?
#[tokio::test]
#[traced_test]
pub async fn test_producer_consumer_valid_topic() {
    let mut b = RedPandaBuilder::new();
    // Assumes you have a RedPanda broker running on ports 9010, 9011, 9012
    b.set_bootstrap_servers("localhost:9010,localhost:9011,localhost:9012");
    b.set_creation_timeout_ms(3000);
    b.set_socket_timeout_ms(3000);
    b.set_session_timeout_ms(15000);
    b.set_compression_type(CompressionType::Zstd);
    b.set_rdkafka_log_level(RDKafkaLogLevel::Info);
    let producer = b.build_producer().unwrap();
    let consumer = b.build_consumer().unwrap();
    let admin_client = b.build_admin_client().await.unwrap();
    let topic_name = "test_producer_topic";
    admin_client.create_topic(topic_name, 3, 3).await.unwrap();

    let key = 1_u32.to_le_bytes().to_vec();
    let payload = 2_u32.to_le_bytes().to_vec();
    producer.send_result(topic_name, &key, &payload).unwrap();
    // event!(Level::INFO, "Produced message");

    consumer.subscribe(&[topic_name]).unwrap();
    let msg = consumer.recv().await.unwrap();
    event!(Level::INFO, "Got message");
    assert_eq!(msg.key().unwrap(), key);
    assert_eq!(msg.payload().unwrap(), payload);

    admin_client.delete_topic(topic_name).await.unwrap();
}

/// Does RedPandaAdminClient fail to construct with the proper error code if the bootstrap_server doesn't exist?
#[tokio::test]
#[traced_test]
pub async fn test_admin_invalid_server() {
    let mut b = RedPandaBuilder::new();
    // Assumes you don't have a RedPanda broker running on this port
    b.set_bootstrap_servers("localhost:9000");
    b.set_socket_timeout_ms(3000);
    b.set_rdkafka_log_level(RDKafkaLogLevel::Info);
    let err = b.build_admin_client().await;

    assert!(err.is_err());
}

/// Does RedPandaAdminClient successfully construct if some of the bootstrap_servers are invalid?
#[tokio::test]
#[traced_test]
pub async fn test_admin_some_bad_servers() {
    let mut b = RedPandaBuilder::new();
    // Assumes you don't have a RedPanda broker running on port 9000
    // ...and that you DO have one running on port 9010
    b.set_bootstrap_servers("localhost:9000,localhost:9010");
    b.set_socket_timeout_ms(3000);
    b.set_rdkafka_log_level(RDKafkaLogLevel::Info);
    let admin_client = b.build_admin_client().await;

    assert!(admin_client.is_ok());
}

/// Does RedPandaAdminClient successfully construct if bootstrap_servers is valid?
#[tokio::test]
#[traced_test]
pub async fn test_admin_valid_server() {
    let mut b = RedPandaBuilder::new();
    // Assumes you have a RedPanda broker running on ports 9010, 9011, 9012
    b.set_bootstrap_servers("localhost:9010,localhost:9011,localhost:9012");
    b.set_socket_timeout_ms(3000);
    b.set_rdkafka_log_level(RDKafkaLogLevel::Info);
    let admin_client = b.build_admin_client().await;

    assert!(admin_client.is_ok());
}

/// Does RedPandaAdminClient successfully create + delete a topic
#[tokio::test]
#[traced_test]
pub async fn test_admin_create_delete_topic() {
    let mut b = RedPandaBuilder::new();
    // Assumes you have a RedPanda broker running on ports 9010, 9011, 9012
    b.set_bootstrap_servers("localhost:9010,localhost:9011,localhost:9012");
    b.set_socket_timeout_ms(3000);
    b.set_rdkafka_log_level(RDKafkaLogLevel::Info);
    let admin_client = b.build_admin_client().await.unwrap();

    let topic_name = "test_test_test";
    let create_topic = admin_client.create_topic(topic_name, 3, 3).await;
    assert!(create_topic.is_ok());

    let consumer = b.build_consumer().unwrap();
    let topic_names = consumer.fetch_metadata().unwrap().topic_names();
    assert!(topic_names.contains(&topic_name.to_owned()));

    let delete_topic = admin_client.delete_topic(topic_name).await;
    assert!(delete_topic.is_ok());
    let topic_names = consumer.fetch_metadata().unwrap().topic_names();
    assert!(!topic_names.contains(&topic_name.to_owned()));
}

// TODO: Test failures on invalid topic creation parameters (invalid name (topic already exists), replication factor > num_brokers, replication factor 0, partitions 0)