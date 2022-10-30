use std::fmt::Display;
pub use rdkafka::config::RDKafkaLogLevel;

pub enum CompressionType {
    /// No compression
    None,
    /// zstd compression
    Zstd,
    /// gzip compression
    Gzip,
}

impl Display for CompressionType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CompressionType::None => write!(f, ""),
            CompressionType::Zstd => write!(f, "zstd"),
            CompressionType::Gzip => write!(f, "gzip"),
        }
    }
}
