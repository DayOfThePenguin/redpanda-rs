use rdkafka::config::RDKafkaLogLevel;
use rdkafka::types::RDKafkaErrorCode;
use rdkafka::util::Timeout;
use tracing::{event, instrument, Level};
use tracing_test::traced_test;

use crate::{builder::RedPandaBuilder, metadata::RedPandaMetadata};
use std::time::Duration;

/// Does RedPandaConsumer fail to construct with the proper error code if the bootstrap_server doesn't exist?
#[tokio::test]
#[traced_test]
pub async fn test_consumer_bad_server() {
    let mut b = RedPandaBuilder::new();
    // Assumes you don't have a RedPanda broker running on this port
    b.set_bootstrap_servers("localhost:9000");
    b.set_socket_timeout_ms(3000);
    b.set_socket_connection_setup_timeout_ms(3000);

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

    let consumer = b.build_consumer();

    assert!(consumer.is_ok());
}

/// Test invalid topic response on a valid broker
#[tokio::test]
#[traced_test]
pub async fn test_consumer_invalid_topic() {
    let mut b = RedPandaBuilder::new();
    // Assumes you don't have a RedPanda broker running on this port
    b.set_bootstrap_servers("localhost:9010");
    b.set_session_timeout_ms(6000);
    b.set_socket_timeout_ms(3000);
    b.set_socket_connection_setup_timeout_ms(3000);
    b.set_rdkafka_log_level(RDKafkaLogLevel::Info);
    let topic = "i_dont_exist";

    let consumer = b.build_consumer().unwrap();

    todo!();
    // Create function that receives topic names & checks them against the consumer metadata...if
    // a name isn't in the topic names, return errors
}