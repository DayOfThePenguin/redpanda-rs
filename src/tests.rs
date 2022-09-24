use rdkafka::config::RDKafkaLogLevel;
use rdkafka::consumer::Consumer;
use rdkafka::error::KafkaError;
use rdkafka::message::OwnedMessage;
use rdkafka::metadata::Metadata;
use rdkafka::types::RDKafkaErrorCode;
use rdkafka::util::Timeout;
use tracing::{event, instrument, Level};
use tracing_test::traced_test;

use crate::{metadata::RedPandaMetadata, RedPandaBuilder};
use std::time::Duration;

fn setup_trace() {
    // construct a subscriber that prints formatted traces to stdout
    let subscriber = tracing_subscriber::fmt().compact().pretty().finish();
    // use that subscriber to process traces emitted after this point
    tracing::subscriber::set_global_default(subscriber).expect("Failed to set tracing subscriber");
}

#[test]
pub fn test_builder() {
    let mut b = RedPandaBuilder::new();
    b.set_bootstrap_servers("localhost:9012");
    println!("{:?}", b);
}

// Only fails on ports <1024.
// Even if no broker is running, this will complete
#[tokio::test]
#[traced_test]
pub async fn test_connection() {
    let mut b = RedPandaBuilder::new();
    // Assumes you don't have a RedPanda broker running on this port
    b.set_bootstrap_servers("localhost:9000");
    b.set_session_timeout_ms(6000);
    b.set_rdkafka_log_level(RDKafkaLogLevel::Info);
    let topic = "i_dont_exist";

    let duration = Duration::from_secs(1);
    let timeout = Timeout::After(duration);

    let consumer = b.build_consumer();
}

#[tokio::test]
#[traced_test]
pub async fn test_bad_hostname() {
    let mut b = RedPandaBuilder::new();
    // Assumes you don't have a RedPanda broker running on this port
    b.set_bootstrap_servers("localhost:9000");
    b.set_socket_timeout_ms(3000);
    b.set_socket_connection_setup_timeout_ms(3000);
    let topic = "i_dont_exist";

    let duration = Duration::from_secs(1);
    let timeout = Timeout::After(duration);

    let consumer = b.build_consumer();

    consumer
        .subscribe(&[&topic])
        .expect("Can't subscribe to specified topic");

    let err = consumer.fetch_metadata(Some(topic), timeout);
    assert!(err.is_err());

    // This has to unwrap successfully because of the is_err() check above
    let err_code = err.err().unwrap().rdkafka_error_code().unwrap();

    assert!(err_code == RDKafkaErrorCode::BrokerTransportFailure);
}

/// Test invalid topic response on a valid broker
#[tokio::test]
#[traced_test]
pub async fn test_valid_hostname_invalid_topic() {
    let mut b = RedPandaBuilder::new();
    // Assumes you don't have a RedPanda broker running on this port
    b.set_bootstrap_servers("localhost:9010");
    b.set_session_timeout_ms(6000);
    b.set_socket_timeout_ms(3000);
    b.set_socket_connection_setup_timeout_ms(3000);
    b.set_rdkafka_log_level(RDKafkaLogLevel::Info);
    let topic = "i_dont_exist";

    let duration = Duration::from_secs(1);
    let timeout = Timeout::After(duration);

    let consumer = b.build_consumer();

    // This should work but doesn't
    // let subscribe_err = consumer.subscribe(&[&topic]);
    // assert!(subscribe_err.is_err());

    // This will just keep trying to get a message even though topic doesn't exist...
    // let msg = consumer.recv().await.unwrap();

    let metadata: RedPandaMetadata = consumer
        .fetch_metadata(Some(topic), timeout)
        .expect("Metadata fetch should succeed for valid broker URLs")
        .into();
    event!(Level::DEBUG, "{:?}", metadata);

    // This has to unwrap successfully because of the is_err() check above
}

/// Regardless of whether there's a valid server this will work!
#[test]
pub fn test_connection_fails() {
    let mut b = RedPandaBuilder::new();
    b.set_bootstrap_servers("localhost:9000");
    b.build_producer();
}
