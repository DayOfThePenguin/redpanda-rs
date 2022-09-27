# RedPanda-rs

Rust crate for interfacing with RedPanda message brokers.

Built on top of the [librdkafka](https://github.com/edenhill/librdkafka) C library and its [rust bindings](https://github.com/fede1024/rust-rdkafka).

Provides a layer of abstraction to make using RedPanda (and other Kafka-like) message brokers
easier in Rust.

`docker exec -it redpanda-0 bash`

Change segment size:

`rpk topic alter-config <topic> --set segment.bytes=<segment_size>`
`rpk topic alter-config simple --set segment.bytes=1000000`

## Start local RedPanda and MinIO cluster

`docker-compose up`

### Check the broker status on RedPanda Console

`http://localhost:8080`

### Set up S3 Archiving

Archive all the topics in your cluster to local S3 storage using MinIO!

Run `./configure_cluster.sh` in the `config` directory.

## References

librdkafka docs:

- [librdkafka wiki](https://github.com/edenhill/librdkafka/wiki)
- [librdkafka discussion](https://github.com/edenhill/librdkafka/discussions)
- [Manually set consumer start offset](https://github.com/edenhill/librdkafka/wiki/Manually-setting-the-consumer-start-offset)
