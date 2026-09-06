use thiserror::Error;

use crate::config::ConfigError;

#[derive(Debug, Error)]
pub enum KafkaError {
    #[error("unknown cluster '{0}'")]
    UnknownCluster(String),

    #[error(transparent)]
    Config(#[from] ConfigError),

    #[error(transparent)]
    Client(#[from] rdkafka::error::KafkaError),
}
