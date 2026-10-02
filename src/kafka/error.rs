use thiserror::Error;

#[derive(Debug, Error)]
pub enum KafkaError {
    #[error("unknown cluster '{0}'")]
    UnknownCluster(String),

    #[error("unknown topic '{topic}' in cluster '{cluster}'")]
    UnknownTopic { cluster: String, topic: String },

    #[error("unknown group '{group}' in cluster '{cluster}'")]
    UnknownGroup { cluster: String, group: String },

    #[error("unknown broker {id} in cluster '{cluster}'")]
    UnknownBroker { cluster: String, id: i32 },

    #[error("unknown schema subject '{subject}' version {version} in cluster '{cluster}'")]
    UnknownSubject {
        cluster: String,
        subject: String,
        version: i32,
    },

    #[error("unknown partition {partition} for topic '{topic}' in cluster '{cluster}'")]
    UnknownPartition {
        cluster: String,
        topic: String,
        partition: i32,
    },

    #[error(
        "no record at offset {offset} of partition {partition} in topic '{topic}' in cluster '{cluster}'"
    )]
    UnknownOffset {
        cluster: String,
        topic: String,
        partition: i32,
        offset: i64,
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

    /// The cluster refused a change, for example because the topic exists or
    /// a policy forbids it. Carries the broker's reason.
    #[error("the cluster refused the change: {0}")]
    Rejected(String),

    #[error(transparent)]
    Krafka(#[from] krafka::error::KrafkaError),
}

impl KafkaError {
    pub fn code(&self) -> &'static str {
        match self {
            Self::UnknownCluster(_) => "UNKNOWN_CLUSTER",
            Self::UnknownTopic { .. } => "UNKNOWN_TOPIC",
            Self::UnknownGroup { .. } => "UNKNOWN_GROUP",
            Self::UnknownBroker { .. } => "UNKNOWN_BROKER",
            Self::UnknownSubject { .. } => "UNKNOWN_SUBJECT",
            Self::UnknownPartition { .. } => "UNKNOWN_PARTITION",
            Self::UnknownOffset { .. } => "UNKNOWN_OFFSET",
            Self::InvalidQuery(query) => query.code(),
            Self::Timeout => "TIMEOUT",
            Self::Admin(_) => "ADMIN",
            Self::BrokerConfigs { .. } => "BROKER_CONFIGS",
            Self::SchemaRegistry { .. } => "SCHEMA_REGISTRY",
            Self::Rejected(_) => "REJECTED",
            Self::Krafka(_) => "CLIENT",
        }
    }
}

/// A broker that denies a describe explains it in prose, such as "Cluster
/// authorization failed." or "Request ... needs DESCRIBE permission.", or
/// sends no message and leaves only the error-code name.
pub(crate) fn is_cluster_authorization_text(message: &str) -> bool {
    let lower = message.to_ascii_lowercase();
    lower.contains("clusterauthorizationfailed")
        || lower.contains("cluster_authorization_failed")
        || lower.contains("cluster authorization failed")
        || lower.contains("needs describe permission")
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum QueryError {
    #[error("limit must be at least 1")]
    LimitTooSmall,

    #[error("cursor is invalid")]
    InvalidCursor,

    #[error("timestampFrom must not be after timestampTo")]
    InvertedTimestampRange,
}

impl QueryError {
    pub fn code(&self) -> &'static str {
        match self {
            Self::LimitTooSmall => "LIMIT_TOO_SMALL",
            Self::InvalidCursor => "INVALID_CURSOR",
            Self::InvertedTimestampRange => "INVERTED_TIMESTAMP_RANGE",
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
            KafkaError::UnknownGroup {
                cluster: "local".into(),
                group: "missing".into(),
            }
            .code(),
            "UNKNOWN_GROUP"
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
            KafkaError::UnknownPartition {
                cluster: "local".into(),
                topic: "orders".into(),
                partition: 3,
            }
            .code(),
            "UNKNOWN_PARTITION"
        );
        assert_eq!(
            KafkaError::UnknownOffset {
                cluster: "local".into(),
                topic: "orders".into(),
                partition: 3,
                offset: 42,
            }
            .code(),
            "UNKNOWN_OFFSET"
        );
        assert_eq!(
            KafkaError::InvalidQuery(QueryError::InvertedTimestampRange).code(),
            "INVERTED_TIMESTAMP_RANGE"
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
        assert_eq!(
            KafkaError::Rejected("Topic 'orders' already exists.".into()).code(),
            "REJECTED"
        );
        assert_eq!(QueryError::LimitTooSmall.code(), "LIMIT_TOO_SMALL");
        assert_eq!(QueryError::InvalidCursor.code(), "INVALID_CURSOR");
    }
}
