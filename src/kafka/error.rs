use thiserror::Error;

#[derive(Debug, Error)]
pub enum KafkaError {
    #[error("unknown cluster '{0}'")]
    UnknownCluster(String),

    #[error("invalid configuration for cluster '{cluster}': {reason}")]
    InvalidConfig { cluster: String, reason: String },

    #[error(transparent)]
    Client(#[from] rdkafka::error::KafkaError),
}
