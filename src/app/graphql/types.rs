use chrono::{DateTime, Utc};
use juniper::{GraphQLEnum, GraphQLInputObject, GraphQLObject};

use crate::config::SecurityProtocol as ConfigSecurityProtocol;
use crate::kafka::model as domain;

#[derive(GraphQLEnum, Clone, Copy)]
pub(super) enum ClusterStatus {
    Healthy,
    Degraded,
    Offline,
}

#[derive(GraphQLEnum, Clone, Copy)]
pub(super) enum SecurityProtocol {
    Plaintext,
    Ssl,
    SaslPlaintext,
    SaslSsl,
}

impl From<ConfigSecurityProtocol> for SecurityProtocol {
    fn from(value: ConfigSecurityProtocol) -> Self {
        match value {
            ConfigSecurityProtocol::Plaintext => Self::Plaintext,
            ConfigSecurityProtocol::Ssl => Self::Ssl,
            ConfigSecurityProtocol::SaslPlaintext => Self::SaslPlaintext,
            ConfigSecurityProtocol::SaslSsl => Self::SaslSsl,
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

impl From<domain::ClusterOverview> for Cluster {
    fn from(overview: domain::ClusterOverview) -> Self {
        Self {
            name: overview.identity.name.clone(),
            label: overview.identity.name,
            cluster_id: overview.cluster_id,
            bootstrap_servers: overview.identity.bootstrap_servers,
            security_protocol: SecurityProtocol::from(overview.identity.security_protocol),
            version: String::new(),
            status: ClusterStatus::from(overview.health),
            broker_count: overview.broker_count,
            topic_count: overview.topic_count,
            partition_count: overview.partition_count,
            consumer_group_count: overview.consumer_group_count,
            under_replicated_partitions: overview.under_replicated_partitions,
            offline_partitions: overview.offline_partitions,
            message_count: overview.message_count as f64,
            size_bytes: 0.0,
            bytes_in_per_sec: 0.0,
            bytes_out_per_sec: 0.0,
        }
    }
}

impl From<domain::ClusterHealth> for ClusterStatus {
    fn from(health: domain::ClusterHealth) -> Self {
        match health {
            domain::ClusterHealth::Healthy => Self::Healthy,
            domain::ClusterHealth::Degraded => Self::Degraded,
            domain::ClusterHealth::Offline => Self::Offline,
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

impl From<domain::Broker> for Broker {
    fn from(broker: domain::Broker) -> Self {
        Self {
            id: broker.id,
            host: broker.host,
            port: broker.port,
            rack: broker.rack,
            controller: broker.controller,
            partition_count: broker.partition_count,
            leader_count: broker.leader_count,
            log_dir_size_bytes: 0.0,
            bytes_in_per_sec: 0.0,
            bytes_out_per_sec: 0.0,
        }
    }
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
    Delete,
    Compact,
    CompactDelete,
}

#[derive(GraphQLObject)]
pub(super) struct ClusterCatalog {
    pub updated_at: DateTime<Utc>,
    pub topics: Vec<Topic>,
    pub consumer_groups: Vec<ConsumerGroup>,
}

#[derive(GraphQLObject)]
pub(super) struct CatalogHealth {
    pub cluster: String,
    pub updated_at: Option<DateTime<Utc>>,
    pub subjects_updated_at: Option<DateTime<Utc>>,
    pub last_error: Option<String>,
    pub last_poll_duration_ms: Option<f64>,
    pub topic_count: i32,
    pub group_count: i32,
    pub broker_count: i32,
    pub subject_count: i32,
}

impl From<crate::kafka::CatalogHealth> for CatalogHealth {
    fn from(health: crate::kafka::CatalogHealth) -> Self {
        Self {
            cluster: health.cluster,
            updated_at: health.updated_at,
            subjects_updated_at: health.subjects_updated_at,
            last_error: health.last_error,
            last_poll_duration_ms: health.last_poll_duration_ms.map(|ms| ms as f64),
            topic_count: health.topic_count,
            group_count: health.group_count,
            broker_count: health.broker_count,
            subject_count: health.subject_count,
        }
    }
}

#[derive(GraphQLObject, Clone)]
pub(super) struct CatalogUpdated {
    pub cluster: String,
    pub updated_at: DateTime<Utc>,
    pub generation: i32,
}

impl From<crate::kafka::CatalogRevision> for CatalogUpdated {
    fn from(revision: crate::kafka::CatalogRevision) -> Self {
        Self {
            cluster: revision.cluster,
            updated_at: revision.updated_at,
            generation: revision.generation as i32,
        }
    }
}

#[derive(GraphQLObject)]
pub(super) struct Topic {
    pub name: String,
    pub internal: bool,
    pub partitions: Vec<Partition>,
    pub partition_count: i32,
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

impl Topic {
    pub(super) fn from_domain(
        topic: domain::Topic,
        rate: Option<&crate::kafka::TopicRate>,
    ) -> Self {
        let mut graph = Self::from(topic);
        if let Some(rate) = rate {
            graph.bytes_in_per_sec = rate.bytes_in_per_sec;
            graph.messages_per_sec = rate.messages_per_sec;
        }
        graph
    }
}

impl From<domain::Topic> for Topic {
    fn from(topic: domain::Topic) -> Self {
        Self {
            name: topic.name,
            internal: topic.internal,
            partition_count: topic.partitions.len() as i32,
            partitions: topic.partitions.into_iter().map(Partition::from).collect(),
            replication_factor: topic.replication_factor,
            message_count: topic.message_count as f64,
            size_bytes: 0.0,
            cleanup_policy: CleanupPolicy::from(topic.cleanup_policy),
            retention_ms: topic.retention_ms as f64,
            consumer_groups: topic.consumer_groups,
            bytes_in_per_sec: 0.0,
            messages_per_sec: 0.0,
            under_replicated: topic.under_replicated,
        }
    }
}

impl From<domain::Partition> for Partition {
    fn from(partition: domain::Partition) -> Self {
        Self {
            id: partition.id,
            leader: partition.leader,
            replicas: partition.replicas,
            isr: partition.isr,
            low_watermark: partition.low_watermark as f64,
            high_watermark: partition.high_watermark as f64,
            size_bytes: 0.0,
        }
    }
}

impl From<domain::CleanupPolicy> for CleanupPolicy {
    fn from(policy: domain::CleanupPolicy) -> Self {
        match policy {
            domain::CleanupPolicy::Delete => Self::Delete,
            domain::CleanupPolicy::Compact => Self::Compact,
            domain::CleanupPolicy::CompactDelete => Self::CompactDelete,
        }
    }
}

#[derive(GraphQLEnum, Clone, Copy)]
#[allow(clippy::enum_variant_names)]
pub(super) enum ConfigSource {
    DynamicTopicConfig,
    DynamicBrokerConfig,
    StaticBrokerConfig,
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

impl From<domain::ConfigEntry> for ConfigEntry {
    fn from(entry: domain::ConfigEntry) -> Self {
        Self {
            name: entry.name,
            value: entry.value,
            source: ConfigSource::from(entry.source),
            read_only: entry.read_only,
            sensitive: entry.sensitive,
            documentation: None,
        }
    }
}

impl From<domain::ConfigSource> for ConfigSource {
    fn from(source: domain::ConfigSource) -> Self {
        match source {
            domain::ConfigSource::DynamicTopic => Self::DynamicTopicConfig,
            domain::ConfigSource::DynamicBroker => Self::DynamicBrokerConfig,
            domain::ConfigSource::StaticBroker => Self::StaticBrokerConfig,
            domain::ConfigSource::Default => Self::DefaultConfig,
        }
    }
}

#[derive(GraphQLEnum, Clone, Copy)]
pub(super) enum ConsumerGroupState {
    Stable,
    Empty,
    PreparingRebalance,
    CompletingRebalance,
    Dead,
}

#[derive(GraphQLObject, Clone)]
pub(super) struct MemberAssignment {
    pub topic: String,
    pub partitions: Vec<i32>,
}

#[derive(GraphQLObject, Clone)]
pub(super) struct ConsumerGroupMember {
    pub id: String,
    pub client_id: String,
    pub host: String,
    pub assignments: Vec<MemberAssignment>,
}

#[derive(GraphQLObject, Clone)]
pub(super) struct GroupOffset {
    pub topic: String,
    pub partition: i32,
    pub current_offset: f64,
    pub end_offset: f64,
    pub lag: f64,
    pub member_id: Option<String>,
}

#[derive(GraphQLObject, Clone)]
pub(super) struct ConsumerGroup {
    pub id: String,
    pub state: ConsumerGroupState,
    pub protocol: String,
    pub coordinator: i32,
    pub members: Vec<ConsumerGroupMember>,
    pub member_count: i32,
    pub topics: Vec<String>,
    pub lag: f64,
    pub offsets: Vec<GroupOffset>,
    pub assigned_partition_count: i32,
}

impl From<domain::ConsumerGroup> for ConsumerGroup {
    fn from(group: domain::ConsumerGroup) -> Self {
        Self {
            id: group.id,
            state: ConsumerGroupState::from(group.state),
            protocol: group.protocol,
            coordinator: group.coordinator,
            member_count: group.members.len() as i32,
            members: group
                .members
                .into_iter()
                .map(ConsumerGroupMember::from)
                .collect(),
            topics: group.topics,
            lag: group.lag as f64,
            assigned_partition_count: group.offsets.len() as i32,
            offsets: group.offsets.into_iter().map(GroupOffset::from).collect(),
        }
    }
}

impl From<domain::GroupState> for ConsumerGroupState {
    fn from(state: domain::GroupState) -> Self {
        match state {
            domain::GroupState::Stable => Self::Stable,
            domain::GroupState::Empty => Self::Empty,
            domain::GroupState::PreparingRebalance => Self::PreparingRebalance,
            domain::GroupState::CompletingRebalance => Self::CompletingRebalance,
            domain::GroupState::Dead => Self::Dead,
        }
    }
}

impl From<domain::GroupMember> for ConsumerGroupMember {
    fn from(member: domain::GroupMember) -> Self {
        Self {
            id: member.id,
            client_id: member.client_id,
            host: member.host,
            assignments: member
                .assignments
                .into_iter()
                .map(MemberAssignment::from)
                .collect(),
        }
    }
}

impl From<domain::MemberAssignment> for MemberAssignment {
    fn from(assignment: domain::MemberAssignment) -> Self {
        Self {
            topic: assignment.topic,
            partitions: assignment.partitions,
        }
    }
}

impl From<domain::GroupOffset> for GroupOffset {
    fn from(offset: domain::GroupOffset) -> Self {
        Self {
            topic: offset.topic,
            partition: offset.partition,
            current_offset: offset.current_offset as f64,
            end_offset: offset.end_offset as f64,
            lag: offset.lag as f64,
            member_id: offset.member_id,
        }
    }
}

#[derive(GraphQLEnum, Clone, Copy)]
pub(super) enum Compression {
    None,
    Gzip,
    Snappy,
    Lz4,
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
    pub timestamp: DateTime<Utc>,
    pub key: Option<String>,
    pub value: Option<String>,
    pub schema_id: Option<i32>,
    pub headers: Vec<RecordHeader>,
    pub size_bytes: f64,
    pub compression: Compression,
}

#[derive(GraphQLObject)]
pub(super) struct RecordPage {
    pub records: Vec<TopicRecord>,
    pub has_more: bool,
    pub next_cursor: Option<String>,
}

#[derive(GraphQLEnum, Clone, Copy)]
pub(super) enum RecordOrder {
    Newest,
    Oldest,
}

#[derive(GraphQLInputObject)]
pub(super) struct RecordQuery {
    pub cluster: String,
    pub topic: String,
    pub partition: Option<i32>,
    pub filter: Option<String>,
    pub timestamp_from: Option<DateTime<Utc>>,
    pub timestamp_to: Option<DateTime<Utc>>,
    pub limit: i32,
    pub order: RecordOrder,
    pub cursor: Option<String>,
    pub schema_id: Option<i32>,
}

impl TryFrom<RecordQuery> for domain::RecordQuery {
    type Error = crate::kafka::QueryError;

    fn try_from(query: RecordQuery) -> Result<Self, Self::Error> {
        let timestamps = domain::TimestampRange::new(query.timestamp_from, query.timestamp_to)?;

        Ok(Self {
            topic: query.topic,
            partition: query.partition,
            filter: crate::kafka::compile_record_filter(query.filter.as_deref().unwrap_or(""))?,
            timestamps,
            limit: query.limit,
            order: domain::RecordOrder::from(query.order),
            cursor: match query.cursor.as_deref().map(str::trim) {
                None | Some("") => None,
                Some(cursor) => Some(crate::kafka::RecordCursor::parse(cursor)?),
            },
            schema_id: query.schema_id,
        })
    }
}

impl From<RecordOrder> for domain::RecordOrder {
    fn from(order: RecordOrder) -> Self {
        match order {
            RecordOrder::Newest => Self::Newest,
            RecordOrder::Oldest => Self::Oldest,
        }
    }
}

impl From<domain::Record> for TopicRecord {
    fn from(record: domain::Record) -> Self {
        Self {
            topic: record.topic,
            partition: record.partition,
            offset: record.offset as f64,
            timestamp: domain::unix_datetime(record.timestamp),
            key: record.key,
            value: record.value,
            schema_id: record.schema_id,
            headers: record.headers.into_iter().map(RecordHeader::from).collect(),
            size_bytes: record.size_bytes as f64,
            compression: Compression::from(record.compression),
        }
    }
}

impl From<domain::RecordPage> for RecordPage {
    fn from(page: domain::RecordPage) -> Self {
        Self {
            records: page.records.into_iter().map(TopicRecord::from).collect(),
            has_more: page.has_more,
            next_cursor: page.next_cursor,
        }
    }
}

impl From<domain::RecordHeader> for RecordHeader {
    fn from(header: domain::RecordHeader) -> Self {
        Self {
            key: header.key,
            value: header.value,
        }
    }
}

impl From<domain::Compression> for Compression {
    fn from(compression: domain::Compression) -> Self {
        match compression {
            domain::Compression::None => Self::None,
            domain::Compression::Gzip => Self::Gzip,
            domain::Compression::Snappy => Self::Snappy,
            domain::Compression::Lz4 => Self::Lz4,
            domain::Compression::Zstd => Self::Zstd,
        }
    }
}

#[derive(GraphQLObject)]
pub(super) struct ThroughputPoint {
    pub timestamp: DateTime<Utc>,
    pub bytes_in: f64,
    pub bytes_out: f64,
    pub messages: f64,
}

impl From<crate::kafka::ThroughputPoint> for ThroughputPoint {
    fn from(point: crate::kafka::ThroughputPoint) -> Self {
        Self {
            timestamp: domain::unix_datetime(point.timestamp as i64),
            bytes_in: point.bytes_in,
            bytes_out: point.bytes_out,
            messages: point.messages,
        }
    }
}

#[derive(GraphQLObject, Clone)]
pub(super) struct TopicRate {
    pub name: String,
    pub messages_per_sec: f64,
    pub bytes_in_per_sec: f64,
}

impl From<crate::kafka::TopicRate> for TopicRate {
    fn from(rate: crate::kafka::TopicRate) -> Self {
        Self {
            name: rate.name,
            messages_per_sec: rate.messages_per_sec,
            bytes_in_per_sec: rate.bytes_in_per_sec,
        }
    }
}

#[derive(GraphQLEnum, Clone, Copy)]
pub(super) enum SchemaType {
    Avro,
    Json,
    Protobuf,
}

impl From<domain::SchemaType> for SchemaType {
    fn from(schema_type: domain::SchemaType) -> Self {
        match schema_type {
            domain::SchemaType::Avro => Self::Avro,
            domain::SchemaType::Json => Self::Json,
            domain::SchemaType::Protobuf => Self::Protobuf,
        }
    }
}

#[derive(GraphQLEnum, Clone, Copy)]
pub(super) enum SchemaCompatibility {
    Backward,
    Forward,
    Full,
    None,
}

impl From<domain::SchemaCompatibility> for SchemaCompatibility {
    fn from(compatibility: domain::SchemaCompatibility) -> Self {
        match compatibility {
            domain::SchemaCompatibility::Backward => Self::Backward,
            domain::SchemaCompatibility::Forward => Self::Forward,
            domain::SchemaCompatibility::Full => Self::Full,
            domain::SchemaCompatibility::None => Self::None,
        }
    }
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

impl From<domain::SchemaSubject> for SchemaSubject {
    fn from(subject: domain::SchemaSubject) -> Self {
        Self {
            subject: subject.subject,
            id: subject.id,
            schema_type: SchemaType::from(subject.schema_type),
            latest_version: subject.latest_version,
            versions: subject.versions,
            compatibility: SchemaCompatibility::from(subject.compatibility),
            schema: subject.schema,
        }
    }
}

#[derive(GraphQLEnum, Clone, Copy)]
pub(super) enum SearchResultKind {
    Topic,
    Group,
    Node,
    Subject,
}

#[derive(GraphQLObject)]
pub(super) struct SearchResult {
    pub kind: SearchResultKind,
    pub id: String,
    pub label: String,
    pub detail: String,
}

impl From<domain::SearchHit> for SearchResult {
    fn from(hit: domain::SearchHit) -> Self {
        Self {
            kind: SearchResultKind::from(hit.kind),
            id: hit.id,
            label: hit.label,
            detail: hit.detail,
        }
    }
}

impl From<domain::SearchKind> for SearchResultKind {
    fn from(kind: domain::SearchKind) -> Self {
        match kind {
            domain::SearchKind::Topic => Self::Topic,
            domain::SearchKind::Group => Self::Group,
            domain::SearchKind::Node => Self::Node,
            domain::SearchKind::Subject => Self::Subject,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn topic_record_exposes_wire_schema_id() {
        let record = domain::Record {
            topic: "orders".into(),
            partition: 0,
            offset: 1,
            timestamp: 0,
            key: Some("k".into()),
            value: Some("{}".into()),
            schema_id: Some(12),
            headers: Vec::new(),
            size_bytes: 2,
            compression: domain::Compression::None,
        };
        let mapped = TopicRecord::from(record);
        assert_eq!(mapped.schema_id, Some(12));
    }
}
