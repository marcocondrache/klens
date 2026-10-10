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

    #[error("unknown schema {id} in cluster '{cluster}'")]
    UnknownSchema { cluster: String, id: i32 },

    #[error("the payload does not fit schema {id}: {message}")]
    Unencodable { id: i32, message: String },

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

    #[error("kafka refused the change: {0}")]
    Refused(String),

    #[error("the schema registry refused the change: {0}")]
    RegistryRefused(String),

    #[error("cluster '{0}' has no schema registry")]
    NoSchemaRegistry(String),

    #[error("klens leaves the internal topic '{0}' alone")]
    InternalTopic(String),

    #[error("group '{group}' has members; stop its consumers first")]
    ActiveGroup { group: String },

    #[error("group '{group}' still consumes '{topic}'; stop its consumers first")]
    ConsumedTopic { group: String, topic: String },

    #[error(
        "group '{group}' has no committed offset on partition {partition} of '{topic}' to shift"
    )]
    NoCommittedOffset {
        group: String,
        topic: String,
        partition: i32,
    },

    #[error("failed to describe broker {id} configs: {message}")]
    BrokerConfigs { id: i32, message: String },

    #[error("schema registry request failed for cluster '{cluster}': {message}")]
    SchemaRegistry { cluster: String, message: String },

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
            Self::UnknownSchema { .. } => "UNKNOWN_SCHEMA",
            Self::Unencodable { .. } => "UNENCODABLE",
            Self::UnknownPartition { .. } => "UNKNOWN_PARTITION",
            Self::UnknownOffset { .. } => "UNKNOWN_OFFSET",
            Self::InvalidQuery(query) => query.code(),
            Self::Timeout => "TIMEOUT",
            Self::Admin(_) => "ADMIN",
            Self::Refused(_) => "REFUSED",
            Self::RegistryRefused(_) => "REGISTRY_REFUSED",
            Self::NoSchemaRegistry(_) => "NO_SCHEMA_REGISTRY",
            Self::InternalTopic(_) => "INTERNAL_TOPIC",
            Self::ActiveGroup { .. } => "ACTIVE_GROUP",
            Self::ConsumedTopic { .. } => "CONSUMED_TOPIC",
            Self::NoCommittedOffset { .. } => "NO_COMMITTED_OFFSET",
            Self::BrokerConfigs { .. } => "BROKER_CONFIGS",
            Self::SchemaRegistry { .. } => "SCHEMA_REGISTRY",
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

    #[error("`from` must not be after `to`")]
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
