use std::ops::{Bound, RangeBounds};

use chrono::{DateTime, Utc};

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

impl ClusterHealth {
    pub fn from_partitions<'a>(
        partitions: impl IntoIterator<Item = &'a PartitionMetadata>,
    ) -> Self {
        if partitions
            .into_iter()
            .any(|partition| partition.under_replicated() || partition.offline())
        {
            Self::Degraded
        } else {
            Self::Healthy
        }
    }
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

    pub fn with_watermarks(&self, marks: Watermarks) -> Partition {
        Partition {
            id: self.id,
            leader: self.leader,
            replicas: self.replicas.clone(),
            isr: self.isr.clone(),
            low_watermark: marks.low,
            high_watermark: marks.high,
        }
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

    pub fn partition_ids(&self) -> Vec<i32> {
        self.partitions
            .iter()
            .map(|partition| partition.id)
            .collect()
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

    pub fn topic_partitions(&self, name: &str) -> Vec<i32> {
        self.topic(name)
            .map(TopicMetadata::partition_ids)
            .unwrap_or_default()
    }

    pub fn topic_partition_pairs(&self, names: &[&str]) -> Vec<(String, i32)> {
        names
            .iter()
            .copied()
            .flat_map(|name| {
                self.topic_partitions(name)
                    .into_iter()
                    .map(|id| (name.to_owned(), id))
            })
            .collect()
    }

    pub fn partitions(&self) -> impl Iterator<Item = &PartitionMetadata> {
        self.topics.iter().flat_map(|topic| topic.partitions.iter())
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

    pub fn messages(&self) -> u64 {
        self.high.max(0) as u64
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

impl ConfigEntry {
    pub fn lookup<'a>(entries: &'a [Self], name: &str) -> Option<&'a str> {
        entries
            .iter()
            .find(|entry| entry.name == name)
            .and_then(|entry| entry.value.as_deref())
    }
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

impl Partition {
    pub fn available(&self) -> i64 {
        (self.high_watermark - self.low_watermark).max(0)
    }

    pub fn under_replicated(&self) -> bool {
        self.isr.len() < self.replicas.len()
    }
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

impl GroupMember {
    pub fn assigned_to(&self, topic: &str, partition: i32) -> bool {
        self.assignments.iter().any(|assignment| {
            assignment.topic == topic && assignment.partitions.contains(&partition)
        })
    }
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

    pub fn member_for(&self, topic: &str, partition: i32) -> Option<&str> {
        self.members
            .iter()
            .find(|member| member.assigned_to(topic, partition))
            .map(|member| member.id.as_str())
    }

    pub fn consumed_topic_names(groups: &[Self]) -> Vec<String> {
        let mut names: Vec<String> = groups
            .iter()
            .flat_map(|group| group.consumed_topics())
            .map(str::to_owned)
            .collect();
        names.sort();
        names.dedup();
        names
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
    pub topic: String,
    pub partition: Option<i32>,
    pub search: String,
    pub timestamps: TimestampRange,
    pub limit: i32,
    pub order: RecordOrder,
    pub page: i32,
}

/// UTC bounds for a record browse. Either side may be unbounded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TimestampRange {
    start: Bound<DateTime<Utc>>,
    end: Bound<DateTime<Utc>>,
}

impl TimestampRange {
    pub const UNBOUNDED: Self = Self {
        start: Bound::Unbounded,
        end: Bound::Unbounded,
    };

    pub fn new(from: Option<DateTime<Utc>>, to: Option<DateTime<Utc>>) -> Result<Self, String> {
        match (from, to) {
            (None, None) => Self::UNBOUNDED,
            (Some(from), None) => Self::from_bounds(from..),
            (None, Some(to)) => Self::from_bounds(..=to),
            (Some(from), Some(to)) => Self::from_bounds(from..=to),
        }
        .validate()
    }

    pub fn from_bounds(range: impl RangeBounds<DateTime<Utc>>) -> Self {
        Self {
            start: copy_bound(range.start_bound()),
            end: copy_bound(range.end_bound()),
        }
    }

    pub fn validate(self) -> Result<Self, String> {
        match (self.start, self.end) {
            (
                Bound::Included(from) | Bound::Excluded(from),
                Bound::Included(to) | Bound::Excluded(to),
            ) if from > to => Err("timestampFrom must not be after timestampTo".into()),
            _ => Ok(self),
        }
    }

    /// Timestamp for Kafka `offsetsForTimes` at the low bound, if any.
    pub fn start_seek(self) -> Option<i64> {
        timestamp_seek(self.start, false)
    }

    /// Timestamp for Kafka `offsetsForTimes` at the exclusive high bound, if any.
    pub fn end_seek(self) -> Option<i64> {
        timestamp_seek(self.end, true)
    }
}

impl Default for TimestampRange {
    fn default() -> Self {
        Self::UNBOUNDED
    }
}

impl RangeBounds<DateTime<Utc>> for TimestampRange {
    fn start_bound(&self) -> Bound<&DateTime<Utc>> {
        self.start.as_ref()
    }

    fn end_bound(&self) -> Bound<&DateTime<Utc>> {
        self.end.as_ref()
    }
}

fn copy_bound(bound: Bound<&DateTime<Utc>>) -> Bound<DateTime<Utc>> {
    match bound {
        Bound::Included(value) => Bound::Included(*value),
        Bound::Excluded(value) => Bound::Excluded(*value),
        Bound::Unbounded => Bound::Unbounded,
    }
}

fn timestamp_seek(bound: Bound<DateTime<Utc>>, is_end: bool) -> Option<i64> {
    match (bound, is_end) {
        (Bound::Unbounded, _) => None,
        (Bound::Included(timestamp), false) | (Bound::Excluded(timestamp), true) => {
            Some(timestamp.timestamp_millis())
        }
        (Bound::Included(timestamp), true) | (Bound::Excluded(timestamp), false) => {
            Some(timestamp.timestamp_millis().saturating_add(1))
        }
    }
}

pub(crate) fn unix_datetime(ms: i64) -> DateTime<Utc> {
    DateTime::from_timestamp_millis(ms).unwrap_or(DateTime::UNIX_EPOCH)
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaReference {
    pub name: String,
    pub subject: String,
    pub version: i32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegisteredSchema {
    pub id: i32,
    pub schema_type: SchemaType,
    pub schema: String,
    pub references: Vec<SchemaReference>,
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

#[cfg(test)]
mod tests {
    use super::*;

    fn snapshot() -> MetadataSnapshot {
        MetadataSnapshot {
            cluster_id: None,
            brokers: Vec::new(),
            topics: vec![TopicMetadata {
                name: "orders.created".into(),
                internal: false,
                partitions: vec![
                    PartitionMetadata {
                        id: 0,
                        leader: 1,
                        replicas: vec![1],
                        isr: vec![1],
                    },
                    PartitionMetadata {
                        id: 2,
                        leader: 1,
                        replicas: vec![1],
                        isr: vec![1],
                    },
                ],
            }],
        }
    }

    #[test]
    fn topic_partitions_returns_ids_and_skips_unknown() {
        let meta = snapshot();
        assert_eq!(meta.topic_partitions("orders.created"), vec![0, 2]);
        assert_eq!(meta.topic_partitions("missing"), Vec::<i32>::new());
        assert_eq!(
            meta.topic("orders.created").unwrap().partition_ids(),
            vec![0, 2]
        );
        assert_eq!(
            meta.topic_partition_pairs(&["missing", "orders.created"]),
            vec![("orders.created".into(), 0), ("orders.created".into(), 2),]
        );
    }

    #[test]
    fn config_lookup_and_group_assignment() {
        let entries = [ConfigEntry {
            name: "cleanup.policy".into(),
            value: Some("compact".into()),
            source: ConfigSource::Default,
            read_only: false,
            sensitive: false,
        }];
        assert_eq!(
            ConfigEntry::lookup(&entries, "cleanup.policy"),
            Some("compact")
        );
        assert_eq!(ConfigEntry::lookup(&entries, "retention.ms"), None);

        let group = GroupSnapshot {
            id: "g".into(),
            state: GroupState::Stable,
            protocol: "range".into(),
            coordinator: 1,
            members: vec![GroupMember {
                id: "m1".into(),
                client_id: "c1".into(),
                host: "127.0.0.1".into(),
                assignments: vec![MemberAssignment {
                    topic: "orders".into(),
                    partitions: vec![0, 1],
                }],
            }],
            committed: vec![CommittedOffset {
                topic: "payments".into(),
                partition: 0,
                offset: 3,
            }],
        };
        assert!(group.members[0].assigned_to("orders", 1));
        assert!(!group.members[0].assigned_to("orders", 2));
        assert_eq!(group.member_for("orders", 0), Some("m1"));
        assert_eq!(
            GroupSnapshot::consumed_topic_names(&[group]),
            vec!["orders".to_owned(), "payments".to_owned()]
        );
        assert_eq!(Watermarks { low: 2, high: 10 }.messages(), 10);
        assert_eq!(Watermarks { low: 0, high: -1 }.messages(), 0);
    }
}
