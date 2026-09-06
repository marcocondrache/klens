use thiserror::Error;

use crate::config::ConfigError;

#[derive(Debug, Error)]
pub enum KafkaError {
    #[error("unknown cluster '{0}'")]
    UnknownCluster(String),

    #[error("unknown topic '{topic}' in cluster '{cluster}'")]
    UnknownTopic { cluster: String, topic: String },

    #[error("unknown broker {id} in cluster '{cluster}'")]
    UnknownBroker { cluster: String, id: i32 },

    #[error("unknown consumer group '{id}' in cluster '{cluster}'")]
    UnknownGroup { cluster: String, id: String },

    #[error("unknown partition {partition} for topic '{topic}' in cluster '{cluster}'")]
    UnknownPartition {
        cluster: String,
        topic: String,
        partition: i32,
    },

    #[error("invalid record query: {0}")]
    InvalidQuery(String),

    #[error("kafka admin request failed: {0}")]
    Admin(String),

    #[error(transparent)]
    Config(#[from] ConfigError),

    #[error(transparent)]
    Client(#[from] rdkafka::error::KafkaError),

    #[error("background kafka task failed: {0}")]
    Join(#[from] tokio::task::JoinError),
}
