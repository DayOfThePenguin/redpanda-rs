# Redpanda-rs

Rust crate for interfacing with Redpanda message brokers.

Built on top of the [librdkafka](https://github.com/edenhill/librdkafka) C library and its [rust bindings](https://github.com/fede1024/rust-rdkafka).

Provides a layer of abstraction to make using Redpanda (and other Kafka-like) message brokers
easier in Rust.

`docker exec -it redpanda-0 bash`

Change segment size:

`rpk topic alter-config <topic> --set segment.bytes=<segment_size>`
`rpk topic alter-config simple --set segment.bytes=1000000`

## Start local Redpanda and MinIO cluster

`docker compose up`

### Check the broker status on Redpanda Console

`http://localhost:8080`

## References

librdkafka docs:

- [librdkafka wiki](https://github.com/edenhill/librdkafka/wiki)
- [librdkafka discussion](https://github.com/edenhill/librdkafka/discussions)
- [Manually set consumer start offset](https://github.com/edenhill/librdkafka/wiki/Manually-setting-the-consumer-start-offset)
