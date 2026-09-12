use thiserror::Error;

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
    InvalidQuery(#[from] QueryError),

    #[error("kafka request timed out")]
    Timeout,

    #[error("kafka admin request failed: {0}")]
    Admin(String),

    #[error("failed to describe broker {id} configs: {message}")]
    BrokerConfigs { id: i32, message: String },

    #[error("schema registry request failed for cluster '{cluster}': {message}")]
    SchemaRegistry { cluster: String, message: String },

    #[error(transparent)]
    Client(#[from] rdkafka::error::KafkaError),

    #[error("background kafka task failed: {0}")]
    Join(#[from] tokio::task::JoinError),
}

/// A record query rejected before any Kafka call is made.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum QueryError {
    #[error("limit must be at least 1")]
    LimitTooSmall,

    #[error("cursor is invalid")]
    InvalidCursor,

    #[error("timestampFrom must not be after timestampTo")]
    InvertedTimestampRange,

    #[error("invalid filter: {0}")]
    InvalidFilter(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timeout_display_is_not_an_admin_prefix() {
        assert_eq!(KafkaError::Timeout.to_string(), "kafka request timed out");
    }

    #[test]
    fn broker_configs_display_names_the_broker() {
        let error = KafkaError::BrokerConfigs {
            id: 3,
            message: "Broker: Not authorized".into(),
        };
        assert_eq!(
            error.to_string(),
            "failed to describe broker 3 configs: Broker: Not authorized"
        );
    }
}
