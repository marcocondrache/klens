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

impl KafkaError {
    pub fn code(&self) -> &'static str {
        match self {
            Self::UnknownCluster(_) => "UNKNOWN_CLUSTER",
            Self::UnknownTopic { .. } => "UNKNOWN_TOPIC",
            Self::UnknownBroker { .. } => "UNKNOWN_BROKER",
            Self::UnknownGroup { .. } => "UNKNOWN_GROUP",
            Self::UnknownPartition { .. } => "UNKNOWN_PARTITION",
            Self::InvalidQuery(query) => query.code(),
            Self::Timeout => "TIMEOUT",
            Self::Admin(_) => "ADMIN",
            Self::BrokerConfigs { .. } => "BROKER_CONFIGS",
            Self::SchemaRegistry { .. } => "SCHEMA_REGISTRY",
            Self::Client(_) => "CLIENT",
            Self::Join(_) => "JOIN",
        }
    }
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

impl QueryError {
    pub fn code(&self) -> &'static str {
        match self {
            Self::LimitTooSmall => "LIMIT_TOO_SMALL",
            Self::InvalidCursor => "INVALID_CURSOR",
            Self::InvertedTimestampRange => "INVERTED_TIMESTAMP_RANGE",
            Self::InvalidFilter(_) => "INVALID_FILTER",
        }
    }
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

    #[test]
    fn error_codes_are_the_variant_names() {
        assert_eq!(
            KafkaError::UnknownCluster("ghost".into()).code(),
            "UNKNOWN_CLUSTER"
        );
        assert_eq!(
            KafkaError::UnknownTopic {
                cluster: "local".into(),
                topic: "missing".into(),
            }
            .code(),
            "UNKNOWN_TOPIC"
        );
        assert_eq!(
            KafkaError::UnknownBroker {
                cluster: "local".into(),
                id: 9,
            }
            .code(),
            "UNKNOWN_BROKER"
        );
        assert_eq!(
            KafkaError::UnknownGroup {
                cluster: "local".into(),
                id: "ghost".into(),
            }
            .code(),
            "UNKNOWN_GROUP"
        );
        assert_eq!(
            KafkaError::UnknownPartition {
                cluster: "local".into(),
                topic: "orders".into(),
                partition: 3,
            }
            .code(),
            "UNKNOWN_PARTITION"
        );
        assert_eq!(
            KafkaError::InvalidQuery(QueryError::InvertedTimestampRange).code(),
            "INVERTED_TIMESTAMP_RANGE"
        );
        assert_eq!(
            KafkaError::InvalidQuery(QueryError::InvalidFilter("value.status ==".into())).code(),
            "INVALID_FILTER"
        );
        assert_eq!(KafkaError::Timeout.code(), "TIMEOUT");
        assert_eq!(KafkaError::Admin("broker down".into()).code(), "ADMIN");
        assert_eq!(
            KafkaError::BrokerConfigs {
                id: 1,
                message: "denied".into(),
            }
            .code(),
            "BROKER_CONFIGS"
        );
        assert_eq!(
            KafkaError::SchemaRegistry {
                cluster: "local".into(),
                message: "404".into(),
            }
            .code(),
            "SCHEMA_REGISTRY"
        );
        assert_eq!(QueryError::LimitTooSmall.code(), "LIMIT_TOO_SMALL");
        assert_eq!(QueryError::InvalidCursor.code(), "INVALID_CURSOR");
    }
}
