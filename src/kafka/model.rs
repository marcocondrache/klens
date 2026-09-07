use crate::config::{ClusterConfig, SecurityProtocol};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterIdentity {
    pub name: String,
    pub bootstrap_servers: Vec<String>,
    pub security_protocol: SecurityProtocol,
}

impl From<&ClusterConfig> for ClusterIdentity {
    fn from(config: &ClusterConfig) -> Self {
        Self {
            name: config.name.trim().to_owned(),
            bootstrap_servers: config.bootstrap_servers.clone(),
            security_protocol: config
                .security
                .as_ref()
                .map(|security| security.protocol)
                .unwrap_or(SecurityProtocol::Plaintext),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClusterHealth {
    Healthy,
    Degraded,
    Offline,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterOverview {
    pub identity: ClusterIdentity,
    pub cluster_id: String,
    pub health: ClusterHealth,
    pub broker_count: i32,
    pub topic_count: i32,
    pub partition_count: i32,
    pub consumer_group_count: i32,
    pub under_replicated_partitions: i32,
    pub offline_partitions: i32,
    pub message_count: u64,
}

impl ClusterOverview {
    pub fn offline(identity: ClusterIdentity) -> Self {
        Self {
            identity,
            cluster_id: String::new(),
            health: ClusterHealth::Offline,
            broker_count: 0,
            topic_count: 0,
            partition_count: 0,
            consumer_group_count: 0,
            under_replicated_partitions: 0,
            offline_partitions: 0,
            message_count: 0,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BrokerMetadata {
    pub id: i32,
    pub host: String,
    pub port: i32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionMetadata {
    pub id: i32,
    pub leader: i32,
    pub replicas: Vec<i32>,
    pub isr: Vec<i32>,
}

impl PartitionMetadata {
    pub fn under_replicated(&self) -> bool {
        self.isr.len() < self.replicas.len()
    }

    pub fn offline(&self) -> bool {
        self.leader < 0
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicMetadata {
    pub name: String,
    pub internal: bool,
    pub partitions: Vec<PartitionMetadata>,
}

impl TopicMetadata {
    pub fn partition(&self, id: i32) -> Option<&PartitionMetadata> {
        self.partitions.iter().find(|partition| partition.id == id)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataSnapshot {
    pub cluster_id: Option<String>,
    pub brokers: Vec<BrokerMetadata>,
    pub topics: Vec<TopicMetadata>,
}

impl MetadataSnapshot {
    pub fn topic(&self, name: &str) -> Option<&TopicMetadata> {
        self.topics.iter().find(|topic| topic.name == name)
    }

    pub fn broker(&self, id: i32) -> Option<&BrokerMetadata> {
        self.brokers.iter().find(|broker| broker.id == id)
    }

    pub fn topic_names(&self) -> Vec<&str> {
        self.topics
            .iter()
            .map(|topic| topic.name.as_str())
            .collect()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Watermarks {
    pub low: i64,
    pub high: i64,
}

impl Watermarks {
    pub fn available(&self) -> i64 {
        (self.high - self.low).max(0)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfigSource {
    DynamicTopic,
    DynamicBroker,
    StaticBroker,
    Default,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigEntry {
    pub name: String,
    pub value: Option<String>,
    pub source: ConfigSource,
    pub read_only: bool,
    pub sensitive: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CleanupPolicy {
    Delete,
    Compact,
    CompactDelete,
}

impl CleanupPolicy {
    pub fn parse(value: &str) -> Self {
        let mut compact = false;
        let mut delete = false;

        for part in value.split(',') {
            match part.trim() {
                "compact" => compact = true,
                "delete" => delete = true,
                _ => {}
            }
        }

        match (compact, delete) {
            (true, true) => Self::CompactDelete,
            (true, false) => Self::Compact,
            _ => Self::Delete,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Partition {
    pub id: i32,
    pub leader: i32,
    pub replicas: Vec<i32>,
    pub isr: Vec<i32>,
    pub low_watermark: i64,
    pub high_watermark: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Topic {
    pub name: String,
    pub internal: bool,
    pub partitions: Vec<Partition>,
    pub replication_factor: i32,
    pub message_count: u64,
    pub cleanup_policy: CleanupPolicy,
    pub retention_ms: i64,
    pub consumer_groups: Vec<String>,
    pub under_replicated: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Broker {
    pub id: i32,
    pub host: String,
    pub port: i32,
    pub rack: Option<String>,
    pub controller: bool,
    pub partition_count: i32,
    pub leader_count: i32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GroupState {
    Stable,
    Empty,
    PreparingRebalance,
    CompletingRebalance,
    Dead,
}

impl std::fmt::Display for GroupState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Stable => "Stable",
            Self::Empty => "Empty",
            Self::PreparingRebalance => "PreparingRebalance",
            Self::CompletingRebalance => "CompletingRebalance",
            Self::Dead => "Dead",
        })
    }
}

impl GroupState {
    pub fn parse(raw: &str) -> Self {
        let normalized: String = raw
            .chars()
            .filter(|ch| ch.is_ascii_alphanumeric())
            .map(|ch| ch.to_ascii_lowercase())
            .collect();

        match normalized.as_str() {
            "stable" => Self::Stable,
            "preparingrebalance" => Self::PreparingRebalance,
            "completingrebalance" => Self::CompletingRebalance,
            "dead" => Self::Dead,
            _ => Self::Empty,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemberAssignment {
    pub topic: String,
    pub partitions: Vec<i32>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupMember {
    pub id: String,
    pub client_id: String,
    pub host: String,
    pub assignments: Vec<MemberAssignment>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommittedOffset {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupSnapshot {
    pub id: String,
    pub state: GroupState,
    pub protocol: String,
    pub coordinator: i32,
    pub members: Vec<GroupMember>,
    pub committed: Vec<CommittedOffset>,
}

impl GroupSnapshot {
    pub fn consumed_topics(&self) -> impl Iterator<Item = &str> {
        self.members
            .iter()
            .flat_map(|member| {
                member
                    .assignments
                    .iter()
                    .map(|assignment| assignment.topic.as_str())
            })
            .chain(self.committed.iter().map(|offset| offset.topic.as_str()))
    }

    pub fn consumes_topic(&self, topic: &str) -> bool {
        self.consumed_topics().any(|name| name == topic)
    }

    pub fn assigned_partition_refs(&self) -> impl Iterator<Item = (&str, i32)> {
        self.members.iter().flat_map(|member| {
            member.assignments.iter().flat_map(|assignment| {
                assignment
                    .partitions
                    .iter()
                    .copied()
                    .map(|partition| (assignment.topic.as_str(), partition))
            })
        })
    }

    pub fn assigned_partitions(&self) -> Vec<(String, i32)> {
        let mut partitions: Vec<(String, i32)> = self
            .assigned_partition_refs()
            .map(|(topic, partition)| (topic.to_owned(), partition))
            .collect();
        partitions.sort();
        partitions.dedup();
        partitions
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupOffset {
    pub topic: String,
    pub partition: i32,
    pub current_offset: i64,
    pub end_offset: i64,
    pub lag: i64,
    pub member_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumerGroup {
    pub id: String,
    pub state: GroupState,
    pub protocol: String,
    pub coordinator: i32,
    pub members: Vec<GroupMember>,
    pub topics: Vec<String>,
    pub lag: i64,
    pub offsets: Vec<GroupOffset>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecordOrder {
    Newest,
    Oldest,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordQuery {
    pub cluster: String,
    pub topic: String,
    pub partition: Option<i32>,
    pub search: String,
    pub limit: i32,
    pub order: RecordOrder,
    pub page: i32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Compression {
    None,
    Gzip,
    Snappy,
    Lz4,
    Zstd,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordHeader {
    pub key: String,
    pub value: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub timestamp: i64,
    pub key: Option<String>,
    pub value: Option<String>,
    pub headers: Vec<RecordHeader>,
    pub size_bytes: u64,
    pub compression: Compression,
}

impl Record {
    pub fn matches(&self, term: &str) -> bool {
        if term.is_empty() {
            return true;
        }

        self.key
            .as_deref()
            .is_some_and(|key| key.to_ascii_lowercase().contains(term))
            || self
                .value
                .as_deref()
                .is_some_and(|value| value.to_ascii_lowercase().contains(term))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionWindow {
    pub partition: i32,
    pub start: i64,
    pub end: i64,
}

impl PartitionWindow {
    pub fn is_empty(&self) -> bool {
        self.start >= self.end
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchPlan {
    pub topic: String,
    pub windows: Vec<PartitionWindow>,
    pub search: String,
    pub limit: usize,
    pub order: RecordOrder,
    pub has_more: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordPage {
    pub records: Vec<Record>,
    pub has_more: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SchemaType {
    Avro,
    Json,
    Protobuf,
}

impl SchemaType {
    pub fn from_registry(value: Option<&str>) -> Self {
        match value
            .map(|value| value.trim().to_ascii_uppercase())
            .as_deref()
        {
            Some("JSON") | Some("JSONSCHEMA") => Self::Json,
            Some("PROTOBUF") => Self::Protobuf,
            _ => Self::Avro,
        }
    }
}

impl std::fmt::Display for SchemaType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Avro => "AVRO",
            Self::Json => "JSON",
            Self::Protobuf => "PROTOBUF",
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SchemaCompatibility {
    Backward,
    Forward,
    Full,
    None,
}

impl SchemaCompatibility {
    pub fn from_registry(value: Option<&str>) -> Self {
        match value
            .map(|value| value.trim().to_ascii_uppercase().replace('-', "_"))
            .as_deref()
        {
            Some("FORWARD") | Some("FORWARD_TRANSITIVE") => Self::Forward,
            Some("FULL") | Some("FULL_TRANSITIVE") => Self::Full,
            Some("NONE") => Self::None,
            _ => Self::Backward,
        }
    }
}

impl std::fmt::Display for SchemaCompatibility {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Backward => "BACKWARD",
            Self::Forward => "FORWARD",
            Self::Full => "FULL",
            Self::None => "NONE",
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaSubject {
    pub subject: String,
    pub id: i32,
    pub schema_type: SchemaType,
    pub latest_version: i32,
    pub versions: Vec<i32>,
    pub compatibility: SchemaCompatibility,
    pub schema: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SearchKind {
    Topic,
    Group,
    Node,
    Subject,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchHit {
    pub kind: SearchKind,
    pub id: String,
    pub label: String,
    pub detail: String,
}

pub fn is_internal_topic(name: &str) -> bool {
    name.starts_with('_') || name.starts_with('.')
}

pub fn is_internal_group(id: &str) -> bool {
    id.starts_with(crate::environment::INTERNAL_GROUP_PREFIX)
}

pub fn decode_bytes(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).into_owned()
}
