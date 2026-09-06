use juniper::{GraphQLEnum, GraphQLInputObject, GraphQLObject};

use crate::kafka::{ClusterClient, SecurityProtocol as KafkaSecurityProtocol};

#[derive(GraphQLEnum, Clone, Copy)]
pub(super) enum ClusterStatus {
    #[graphql(name = "HEALTHY")]
    Healthy,
    #[graphql(name = "DEGRADED")]
    Degraded,
    #[graphql(name = "OFFLINE")]
    Offline,
}

#[derive(GraphQLEnum, Clone, Copy)]
pub(super) enum SecurityProtocol {
    #[graphql(name = "PLAINTEXT")]
    Plaintext,
    #[graphql(name = "SSL")]
    Ssl,
    #[graphql(name = "SASL_PLAINTEXT")]
    SaslPlaintext,
    #[graphql(name = "SASL_SSL")]
    SaslSsl,
}

impl From<KafkaSecurityProtocol> for SecurityProtocol {
    fn from(value: KafkaSecurityProtocol) -> Self {
        match value {
            KafkaSecurityProtocol::Plaintext => Self::Plaintext,
            KafkaSecurityProtocol::Ssl => Self::Ssl,
            KafkaSecurityProtocol::SaslPlaintext => Self::SaslPlaintext,
            KafkaSecurityProtocol::SaslSsl => Self::SaslSsl,
        }
    }
}

#[derive(GraphQLObject)]
pub(super) struct Cluster {
    pub name: String,
    pub label: String,
    pub cluster_id: String,
    pub bootstrap_servers: Vec<String>,
    pub security_protocol: SecurityProtocol,
    pub version: String,
    pub status: ClusterStatus,
    pub broker_count: i32,
    pub topic_count: i32,
    pub partition_count: i32,
    pub consumer_group_count: i32,
    pub under_replicated_partitions: i32,
    pub offline_partitions: i32,
    pub message_count: f64,
    pub size_bytes: f64,
    pub bytes_in_per_sec: f64,
    pub bytes_out_per_sec: f64,
}

impl Cluster {
    pub(super) fn from_client(client: &ClusterClient) -> Self {
        let security_protocol = client
            .config()
            .security
            .as_ref()
            .map(|security| SecurityProtocol::from(security.protocol))
            .unwrap_or(SecurityProtocol::Plaintext);

        Self {
            name: client.name().to_owned(),
            label: client.name().to_owned(),
            cluster_id: String::new(),
            bootstrap_servers: client.config().bootstrap_servers.clone(),
            security_protocol,
            version: String::new(),
            status: ClusterStatus::Healthy,
            broker_count: 0,
            topic_count: 0,
            partition_count: 0,
            consumer_group_count: 0,
            under_replicated_partitions: 0,
            offline_partitions: 0,
            message_count: 0.0,
            size_bytes: 0.0,
            bytes_in_per_sec: 0.0,
            bytes_out_per_sec: 0.0,
        }
    }
}

#[derive(GraphQLObject)]
pub(super) struct Broker {
    pub id: i32,
    pub host: String,
    pub port: i32,
    pub rack: Option<String>,
    pub controller: bool,
    pub partition_count: i32,
    pub leader_count: i32,
    pub log_dir_size_bytes: f64,
    pub bytes_in_per_sec: f64,
    pub bytes_out_per_sec: f64,
}

#[derive(GraphQLObject)]
pub(super) struct Partition {
    pub id: i32,
    pub leader: i32,
    pub replicas: Vec<i32>,
    pub isr: Vec<i32>,
    pub low_watermark: f64,
    pub high_watermark: f64,
    pub size_bytes: f64,
}

#[derive(GraphQLEnum, Clone, Copy)]
pub(super) enum CleanupPolicy {
    #[graphql(name = "DELETE")]
    Delete,
    #[graphql(name = "COMPACT")]
    Compact,
    #[graphql(name = "COMPACT_DELETE")]
    CompactDelete,
}

#[derive(GraphQLObject)]
pub(super) struct Topic {
    pub name: String,
    pub internal: bool,
    pub partitions: Vec<Partition>,
    pub replication_factor: i32,
    pub message_count: f64,
    pub size_bytes: f64,
    pub cleanup_policy: CleanupPolicy,
    pub retention_ms: f64,
    pub consumer_groups: Vec<String>,
    pub bytes_in_per_sec: f64,
    pub messages_per_sec: f64,
    pub under_replicated: bool,
}

#[derive(GraphQLEnum, Clone, Copy)]
#[allow(clippy::enum_variant_names)]
pub(super) enum ConfigSource {
    #[graphql(name = "DYNAMIC_TOPIC_CONFIG")]
    DynamicTopicConfig,
    #[graphql(name = "DYNAMIC_BROKER_CONFIG")]
    DynamicBrokerConfig,
    #[graphql(name = "STATIC_BROKER_CONFIG")]
    StaticBrokerConfig,
    #[graphql(name = "DEFAULT_CONFIG")]
    DefaultConfig,
}

#[derive(GraphQLObject)]
pub(super) struct ConfigEntry {
    pub name: String,
    pub value: Option<String>,
    pub source: ConfigSource,
    pub read_only: bool,
    pub sensitive: bool,
    pub documentation: Option<String>,
}

#[derive(GraphQLEnum, Clone, Copy)]
pub(super) enum ConsumerGroupState {
    #[graphql(name = "STABLE")]
    Stable,
    #[graphql(name = "EMPTY")]
    Empty,
    #[graphql(name = "PREPARING_REBALANCE")]
    PreparingRebalance,
    #[graphql(name = "COMPLETING_REBALANCE")]
    CompletingRebalance,
    #[graphql(name = "DEAD")]
    Dead,
}

#[derive(GraphQLObject)]
pub(super) struct MemberAssignment {
    pub topic: String,
    pub partitions: Vec<i32>,
}

#[derive(GraphQLObject)]
pub(super) struct ConsumerGroupMember {
    pub id: String,
    pub client_id: String,
    pub host: String,
    pub assignments: Vec<MemberAssignment>,
}

#[derive(GraphQLObject)]
pub(super) struct GroupOffset {
    pub topic: String,
    pub partition: i32,
    pub current_offset: f64,
    pub end_offset: f64,
    pub lag: f64,
    pub member_id: Option<String>,
}

#[derive(GraphQLObject)]
pub(super) struct ConsumerGroup {
    pub id: String,
    pub state: ConsumerGroupState,
    pub protocol: String,
    pub coordinator: i32,
    pub members: Vec<ConsumerGroupMember>,
    pub topics: Vec<String>,
    pub lag: f64,
    pub offsets: Vec<GroupOffset>,
}

#[derive(GraphQLEnum, Clone, Copy)]
pub(super) enum Compression {
    #[graphql(name = "NONE")]
    None,
    #[graphql(name = "GZIP")]
    Gzip,
    #[graphql(name = "SNAPPY")]
    Snappy,
    #[graphql(name = "LZ4")]
    Lz4,
    #[graphql(name = "ZSTD")]
    Zstd,
}

#[derive(GraphQLObject)]
pub(super) struct RecordHeader {
    pub key: String,
    pub value: String,
}

#[derive(GraphQLObject)]
pub(super) struct TopicRecord {
    pub topic: String,
    pub partition: i32,
    pub offset: f64,
    pub timestamp: f64,
    pub key: Option<String>,
    pub value: Option<String>,
    pub headers: Vec<RecordHeader>,
    pub size_bytes: f64,
    pub compression: Compression,
}

#[derive(GraphQLEnum, Clone, Copy)]
pub(super) enum RecordOrder {
    #[graphql(name = "NEWEST")]
    Newest,
    #[graphql(name = "OLDEST")]
    Oldest,
}

#[derive(GraphQLInputObject)]
pub(super) struct RecordQuery {
    pub cluster: String,
    pub topic: String,
    pub partition: Option<i32>,
    pub search: String,
    pub limit: i32,
    pub order: RecordOrder,
}

#[derive(GraphQLObject)]
pub(super) struct ThroughputPoint {
    pub timestamp: f64,
    pub bytes_in: f64,
    pub bytes_out: f64,
    pub messages: f64,
}

#[derive(GraphQLEnum, Clone, Copy)]
pub(super) enum SchemaType {
    #[graphql(name = "AVRO")]
    Avro,
    #[graphql(name = "JSON")]
    Json,
    #[graphql(name = "PROTOBUF")]
    Protobuf,
}

#[derive(GraphQLEnum, Clone, Copy)]
pub(super) enum SchemaCompatibility {
    #[graphql(name = "BACKWARD")]
    Backward,
    #[graphql(name = "FORWARD")]
    Forward,
    #[graphql(name = "FULL")]
    Full,
    #[graphql(name = "NONE")]
    None,
}

#[derive(GraphQLObject)]
pub(super) struct SchemaSubject {
    pub subject: String,
    pub id: i32,
    #[graphql(name = "type")]
    pub schema_type: SchemaType,
    pub latest_version: i32,
    pub versions: Vec<i32>,
    pub compatibility: SchemaCompatibility,
    pub schema: String,
}

#[derive(GraphQLEnum, Clone, Copy)]
pub(super) enum AclResourceType {
    #[graphql(name = "TOPIC")]
    Topic,
    #[graphql(name = "GROUP")]
    Group,
    #[graphql(name = "CLUSTER")]
    Cluster,
    #[graphql(name = "TRANSACTIONAL_ID")]
    TransactionalId,
}

#[derive(GraphQLEnum, Clone, Copy)]
pub(super) enum AclPatternType {
    #[graphql(name = "LITERAL")]
    Literal,
    #[graphql(name = "PREFIXED")]
    Prefixed,
}

#[derive(GraphQLEnum, Clone, Copy)]
pub(super) enum AclPermission {
    #[graphql(name = "ALLOW")]
    Allow,
    #[graphql(name = "DENY")]
    Deny,
}

#[derive(GraphQLObject)]
pub(super) struct Acl {
    pub principal: String,
    pub resource_type: AclResourceType,
    pub resource_name: String,
    pub pattern_type: AclPatternType,
    pub operation: String,
    pub permission: AclPermission,
    pub host: String,
}

#[derive(GraphQLEnum, Clone, Copy)]
pub(super) enum SearchResultKind {
    #[graphql(name = "TOPIC")]
    Topic,
    #[graphql(name = "GROUP")]
    Group,
    #[graphql(name = "NODE")]
    Node,
    #[graphql(name = "SUBJECT")]
    Subject,
}

#[derive(GraphQLObject)]
pub(super) struct SearchResult {
    pub kind: SearchResultKind,
    pub id: String,
    pub label: String,
    pub detail: String,
}
