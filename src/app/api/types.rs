use std::fmt;

use jiff::Timestamp;
use serde::de::{self, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use ts_rs::TS;

use crate::app::auth::access::Privilege;
use crate::kafka::model as domain;
use crate::kafka::store::{self, projections};
use crate::kafka::{QueryError, RecordCursor};
use crate::r#macro::from_same_variants;

/// Signed 64-bit integer, serialized as a string.
///
/// Offsets, watermarks, lag and retained counts routinely pass 2^53, where a
/// JSON number silently loses precision in every JavaScript client. A string
/// crosses the wire intact. Input accepts either a string or an integer.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, TS)]
#[ts(type = "string")]
pub(crate) struct Int64(i64);

impl Serialize for Int64 {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.0.to_string())
    }
}

impl<'de> Deserialize<'de> for Int64 {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Int64Visitor;

        impl Visitor<'_> for Int64Visitor {
            type Value = Int64;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a string or integer")
            }

            fn visit_str<E: de::Error>(self, value: &str) -> Result<Int64, E> {
                value.parse().map(Int64).map_err(E::custom)
            }

            fn visit_i64<E: de::Error>(self, value: i64) -> Result<Int64, E> {
                Ok(Int64(value))
            }

            fn visit_u64<E: de::Error>(self, value: u64) -> Result<Int64, E> {
                i64::try_from(value).map(Int64).map_err(E::custom)
            }
        }

        deserializer.deserialize_any(Int64Visitor)
    }
}

impl From<i64> for Int64 {
    fn from(value: i64) -> Self {
        Self(value)
    }
}

impl From<u64> for Int64 {
    fn from(value: u64) -> Self {
        Self(value as i64)
    }
}

impl From<i32> for Int64 {
    fn from(value: i32) -> Self {
        Self(i64::from(value))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum PrivilegeName {
    Records,
    Configs,
    SchemaText,
    Acls,
}

from_same_variants!(Privilege => PrivilegeName { Records, Configs, SchemaText, Acls });

/// What the session may do on one cluster. Pairwise: a wider grant elsewhere
/// does not raise this one.
#[derive(Debug, Clone, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ClusterGrant {
    pub cluster: String,
    /// Names of the roles that granted this access, for tracing a privilege
    /// back to an IdP group mapping. Empty when no role table applies.
    pub roles: Vec<String>,
    pub privileges: Vec<PrivilegeName>,
}

#[derive(Debug, Clone, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub(crate) struct Identity {
    /// `null` when authentication is disabled.
    pub subject: Option<String>,
    pub clusters: Vec<ClusterGrant>,
}

#[derive(Debug, Clone, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub(crate) struct LaneHealth {
    pub updated_at: Option<Timestamp>,
    pub checked_at: Option<Timestamp>,
    pub last_error: Option<String>,
    pub last_poll_ms: Option<Int64>,
    /// False once a lane has failed since its last successful commit.
    pub healthy: bool,
}

impl From<store::LaneHealth> for LaneHealth {
    fn from(health: store::LaneHealth) -> Self {
        Self {
            healthy: health.healthy(),
            updated_at: health.updated_at,
            checked_at: health.checked_at,
            last_error: health.last_error,
            last_poll_ms: health.last_poll_ms.map(Int64::from),
        }
    }
}

/// Per-lane freshness and the counts a dashboard header needs.
#[derive(Debug, Clone, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ClusterHealth {
    pub cluster: String,
    pub ready: bool,
    pub topology: LaneHealth,
    pub watermarks: LaneHealth,
    pub offsets: LaneHealth,
    pub configs: LaneHealth,
    pub subjects: LaneHealth,
    pub topic_count: i32,
    pub partition_count: i32,
    pub group_count: i32,
    pub broker_count: i32,
    pub subject_count: i32,
    pub under_replicated_partitions: i32,
    pub offline_partitions: i32,
}

impl From<projections::ClusterHealthView> for ClusterHealth {
    fn from(health: projections::ClusterHealthView) -> Self {
        Self {
            ready: health.topology.updated_at.is_some(),
            cluster: health.cluster,
            topology: health.topology.into(),
            watermarks: health.watermarks.into(),
            offsets: health.offsets.into(),
            configs: health.configs.into(),
            subjects: health.subjects.into(),
            topic_count: health.topic_count,
            partition_count: health.partition_count,
            group_count: health.group_count,
            broker_count: health.broker_count,
            subject_count: health.subject_count,
            under_replicated_partitions: health.under_replicated_partitions,
            offline_partitions: health.offline_partitions,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum CleanupPolicy {
    Delete,
    Compact,
    CompactDelete,
}

from_same_variants!(domain::CleanupPolicy => CleanupPolicy { Delete, Compact, CompactDelete });

#[derive(Debug, Clone, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub(crate) struct TopicRow {
    pub name: String,
    pub internal: bool,
    pub partition_count: i32,
    pub replication_factor: i32,
    /// Messages currently in the log (`Σ high − low`).
    pub retained_messages: Int64,
    /// Messages ever produced (`Σ high`). Overstates a retention-truncated
    /// topic, so it is not the display default.
    pub produced_total: Int64,
    pub rate: f64,
    pub retention_ms: Int64,
    pub cleanup_policy: CleanupPolicy,
    pub group_count: i32,
    pub under_replicated: bool,
}

impl From<projections::TopicRow> for TopicRow {
    fn from(row: projections::TopicRow) -> Self {
        Self {
            name: row.name.to_string(),
            internal: row.internal,
            partition_count: row.partition_count,
            replication_factor: row.replication_factor,
            retained_messages: row.retained_messages.into(),
            produced_total: row.produced_total.into(),
            rate: row.rate,
            retention_ms: row.retention_ms.into(),
            cleanup_policy: row.cleanup_policy.into(),
            group_count: row.group_count,
            under_replicated: row.under_replicated,
        }
    }
}

#[derive(Debug, Clone, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub(crate) struct TopicRowPage {
    pub rows: Vec<TopicRow>,
    /// Rows matching the filter before paging, so a client can size its
    /// scrollbar without walking every page.
    pub total: i32,
    pub next_cursor: Option<String>,
}

#[derive(Debug, Clone, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub(crate) struct PartitionRow {
    pub id: i32,
    pub leader: i32,
    pub replicas: Vec<i32>,
    pub isr: Vec<i32>,
    pub low_watermark: Int64,
    pub high_watermark: Int64,
    pub retained: Int64,
    pub under_replicated: bool,
}

impl From<projections::PartitionRow> for PartitionRow {
    fn from(row: projections::PartitionRow) -> Self {
        Self {
            under_replicated: row.under_replicated(),
            retained: row.retained().into(),
            id: row.id,
            leader: row.leader,
            replicas: row.replicas,
            isr: row.isr,
            low_watermark: row.low_watermark.into(),
            high_watermark: row.high_watermark.into(),
        }
    }
}

#[derive(Debug, Clone, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub(crate) struct TopicDetail {
    pub name: String,
    pub internal: bool,
    pub partitions: Vec<PartitionRow>,
    pub replication_factor: i32,
    pub retained_messages: Int64,
    pub produced_total: Int64,
    pub rate: f64,
    pub retention_ms: Int64,
    pub cleanup_policy: CleanupPolicy,
    pub group_count: i32,
    pub under_replicated: bool,
}

impl From<projections::TopicDetail> for TopicDetail {
    fn from(detail: projections::TopicDetail) -> Self {
        Self {
            name: detail.name.to_string(),
            internal: detail.internal,
            partitions: detail.partitions.into_iter().map(Into::into).collect(),
            replication_factor: detail.replication_factor,
            retained_messages: detail.retained_messages.into(),
            produced_total: detail.produced_total.into(),
            rate: detail.rate,
            retention_ms: detail.retention_ms.into(),
            cleanup_policy: detail.cleanup_policy.into(),
            group_count: detail.group_count,
            under_replicated: detail.under_replicated,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum GroupState {
    Stable,
    Empty,
    PreparingRebalance,
    CompletingRebalance,
    Dead,
}

from_same_variants!(domain::GroupState => GroupState {
    Stable,
    Empty,
    PreparingRebalance,
    CompletingRebalance,
    Dead,
});

#[derive(Debug, Clone, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub(crate) struct MemberAssignment {
    pub topic: String,
    pub partitions: Vec<i32>,
}

impl From<domain::MemberAssignment> for MemberAssignment {
    fn from(assignment: domain::MemberAssignment) -> Self {
        Self {
            topic: assignment.topic,
            partitions: assignment.partitions,
        }
    }
}

#[derive(Debug, Clone, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub(crate) struct GroupMember {
    pub id: String,
    pub client_id: String,
    pub host: String,
    pub assignments: Vec<MemberAssignment>,
}

impl From<domain::GroupMember> for GroupMember {
    fn from(member: domain::GroupMember) -> Self {
        Self {
            id: member.id,
            client_id: member.client_id,
            host: member.host,
            assignments: member.assignments.into_iter().map(Into::into).collect(),
        }
    }
}

#[derive(Debug, Clone, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub(crate) struct GroupOffset {
    pub topic: String,
    pub partition: i32,
    pub current_offset: Int64,
    pub end_offset: Int64,
    pub lag: Int64,
    pub member_id: Option<String>,
}

impl From<domain::GroupOffset> for GroupOffset {
    fn from(offset: domain::GroupOffset) -> Self {
        Self {
            topic: offset.topic,
            partition: offset.partition,
            current_offset: offset.current_offset.into(),
            end_offset: offset.end_offset.into(),
            lag: offset.lag.into(),
            member_id: offset.member_id,
        }
    }
}

#[derive(Debug, Clone, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub(crate) struct GroupRow {
    pub id: String,
    pub state: GroupState,
    pub member_count: i32,
    pub topic_names: Vec<String>,
    pub total_lag: Int64,
    /// False when a committed partition had no watermark to join against, so
    /// the total understates the real lag.
    pub lag_complete: bool,
    pub coordinator_id: i32,
}

impl From<projections::GroupRow> for GroupRow {
    fn from(row: projections::GroupRow) -> Self {
        Self {
            id: row.id.to_string(),
            state: row.state.into(),
            member_count: row.member_count,
            topic_names: row.topic_names,
            total_lag: row.total_lag.into(),
            lag_complete: row.lag_complete,
            coordinator_id: row.coordinator_id,
        }
    }
}

#[derive(Debug, Clone, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub(crate) struct GroupRowPage {
    pub rows: Vec<GroupRow>,
    pub total: i32,
    pub next_cursor: Option<String>,
}

#[derive(Debug, Clone, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub(crate) struct GroupDetail {
    pub id: String,
    pub state: GroupState,
    pub protocol: String,
    pub coordinator_id: i32,
    pub members: Vec<GroupMember>,
    pub offsets: Vec<GroupOffset>,
    pub total_lag: Int64,
    pub lag_complete: bool,
}

impl From<projections::GroupDetail> for GroupDetail {
    fn from(detail: projections::GroupDetail) -> Self {
        Self {
            id: detail.id.to_string(),
            state: detail.state.into(),
            protocol: detail.protocol,
            coordinator_id: detail.coordinator_id,
            members: detail.members.into_iter().map(Into::into).collect(),
            offsets: detail.offsets.into_iter().map(Into::into).collect(),
            total_lag: detail.total_lag.into(),
            lag_complete: detail.lag_complete,
        }
    }
}

/// Which groups read this topic, and how far behind they are on it alone.
#[derive(Debug, Clone, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub(crate) struct TopicGroupRow {
    pub id: String,
    pub state: GroupState,
    pub member_count: i32,
    pub lag_on_topic: Int64,
}

impl From<projections::TopicGroupRow> for TopicGroupRow {
    fn from(row: projections::TopicGroupRow) -> Self {
        Self {
            id: row.id.to_string(),
            state: row.state.into(),
            member_count: row.member_count,
            lag_on_topic: row.lag_on_topic.into(),
        }
    }
}

#[derive(Debug, Clone, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub(crate) struct BrokerRow {
    pub id: i32,
    pub host: String,
    pub port: i32,
    pub rack: Option<String>,
    pub controller: bool,
    pub partition_count: i32,
    pub leader_count: i32,
}

impl From<projections::BrokerRow> for BrokerRow {
    fn from(row: projections::BrokerRow) -> Self {
        Self {
            id: row.id,
            host: row.host,
            port: row.port,
            rack: row.rack,
            controller: row.controller,
            partition_count: row.partition_count,
            leader_count: row.leader_count,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
#[allow(clippy::enum_variant_names)]
pub(crate) enum ConfigSource {
    DynamicTopicConfig,
    DynamicBrokerConfig,
    StaticBrokerConfig,
    DefaultConfig,
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

#[derive(Debug, Clone, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ConfigEntry {
    pub name: String,
    pub value: Option<String>,
    pub source: ConfigSource,
    pub read_only: bool,
    pub sensitive: bool,
}

impl From<domain::ConfigEntry> for ConfigEntry {
    fn from(entry: domain::ConfigEntry) -> Self {
        Self {
            name: entry.name,
            value: entry.value,
            source: entry.source.into(),
            read_only: entry.read_only,
            sensitive: entry.sensitive,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum SchemaType {
    Avro,
    Json,
    Protobuf,
}

from_same_variants!(domain::SchemaType => SchemaType { Avro, Json, Protobuf });

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum SchemaCompatibility {
    Backward,
    Forward,
    Full,
    None,
}

from_same_variants!(domain::SchemaCompatibility => SchemaCompatibility {
    Backward,
    Forward,
    Full,
    None,
});

#[derive(Debug, Clone, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub(crate) struct SubjectRow {
    pub subject: String,
    pub id: i32,
    #[serde(rename = "type")]
    pub schema_type: SchemaType,
    pub latest_version: i32,
    pub versions: Vec<i32>,
    pub compatibility: SchemaCompatibility,
}

impl From<projections::SubjectRow> for SubjectRow {
    fn from(row: projections::SubjectRow) -> Self {
        Self {
            subject: row.subject.to_string(),
            id: row.info.id,
            schema_type: row.info.schema_type.into(),
            latest_version: row.info.latest_version,
            versions: row.info.versions,
            compatibility: row.info.compatibility.into(),
        }
    }
}

/// An empty `rows` with an unhealthy `sourceHealth` is a registry outage, not
/// a registry with no subjects.
#[derive(Debug, Clone, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub(crate) struct SubjectRowsResult {
    pub rows: Vec<SubjectRow>,
    pub source_health: LaneHealth,
}

#[derive(Debug, Clone, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub(crate) struct SchemaReference {
    pub name: String,
    pub subject: String,
    pub version: i32,
}

impl From<domain::SchemaReference> for SchemaReference {
    fn from(reference: domain::SchemaReference) -> Self {
        Self {
            name: reference.name,
            subject: reference.subject,
            version: reference.version,
        }
    }
}

#[derive(Debug, Clone, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub(crate) struct SubjectDetail {
    pub subject: String,
    pub version: i32,
    pub id: i32,
    #[serde(rename = "type")]
    pub schema_type: SchemaType,
    pub schema: String,
    pub references: Vec<SchemaReference>,
}

impl SubjectDetail {
    pub(crate) fn new(subject: String, version: i32, schema: domain::RegisteredSchema) -> Self {
        Self {
            subject,
            version,
            id: schema.id,
            schema_type: schema.schema_type.into(),
            schema: schema.schema,
            references: schema.references.into_iter().map(Into::into).collect(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum AclAuthorizer {
    Enabled,
    Disabled,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum AclResourceType {
    Topic,
    Group,
    Cluster,
    TransactionalId,
    DelegationToken,
}

from_same_variants!(domain::AclResourceType => AclResourceType {
    Topic,
    Group,
    Cluster,
    TransactionalId,
    DelegationToken,
});

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum AclPatternType {
    Literal,
    Prefixed,
}

from_same_variants!(domain::AclPatternType => AclPatternType { Literal, Prefixed });

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum AclOperation {
    All,
    Read,
    Write,
    Create,
    Delete,
    Alter,
    Describe,
    ClusterAction,
    DescribeConfigs,
    AlterConfigs,
    IdempotentWrite,
}

from_same_variants!(domain::AclOperation => AclOperation {
    All,
    Read,
    Write,
    Create,
    Delete,
    Alter,
    Describe,
    ClusterAction,
    DescribeConfigs,
    AlterConfigs,
    IdempotentWrite,
});

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum AclPermission {
    Allow,
    Deny,
}

from_same_variants!(domain::AclPermission => AclPermission { Allow, Deny });

#[derive(Debug, Clone, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub(crate) struct Acl {
    pub resource_type: AclResourceType,
    pub resource_name: String,
    pub pattern_type: AclPatternType,
    pub principal: String,
    pub host: String,
    pub operation: AclOperation,
    pub permission: AclPermission,
}

impl From<domain::Acl> for Acl {
    fn from(acl: domain::Acl) -> Self {
        Self {
            resource_type: acl.resource_type.into(),
            resource_name: acl.resource_name,
            pattern_type: acl.pattern_type.into(),
            principal: acl.principal,
            host: acl.host,
            operation: acl.operation.into(),
            permission: acl.permission.into(),
        }
    }
}

#[derive(Debug, Clone, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub(crate) struct AclListing {
    pub authorizer: AclAuthorizer,
    pub bindings: Vec<Acl>,
}

impl From<domain::AclListing> for AclListing {
    fn from(listing: domain::AclListing) -> Self {
        match listing {
            domain::AclListing::Enabled(rows) => Self {
                authorizer: AclAuthorizer::Enabled,
                bindings: rows.into_iter().map(Acl::from).collect(),
            },
            domain::AclListing::Disabled => Self {
                authorizer: AclAuthorizer::Disabled,
                bindings: Vec::new(),
            },
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum Compression {
    None,
    Gzip,
    Snappy,
    Lz4,
    Zstd,
}

from_same_variants!(domain::Compression => Compression { None, Gzip, Snappy, Lz4, Zstd });

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum RecordOrder {
    Newest,
    Oldest,
}

from_same_variants!(RecordOrder => domain::RecordOrder { Newest, Oldest });

#[derive(Debug, Clone, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RecordHeader {
    pub key: String,
    pub value: String,
}

impl From<domain::RecordHeader> for RecordHeader {
    fn from(header: domain::RecordHeader) -> Self {
        Self {
            key: header.key,
            value: header.value,
        }
    }
}

#[derive(Debug, Clone, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub(crate) struct Record {
    pub topic: String,
    pub partition: i32,
    pub offset: Int64,
    pub timestamp: Timestamp,
    pub key: Option<String>,
    pub value: Option<String>,
    pub schema_id: Option<i32>,
    pub headers: Vec<RecordHeader>,
    pub size_bytes: Int64,
    pub compression: Compression,
}

impl From<domain::Record> for Record {
    fn from(record: domain::Record) -> Self {
        Self {
            topic: record.topic,
            partition: record.partition,
            offset: record.offset.into(),
            timestamp: Timestamp::from_millisecond(record.timestamp)
                .unwrap_or(Timestamp::UNIX_EPOCH),
            key: record.key,
            value: record.value,
            schema_id: record.schema_id,
            headers: record.headers.into_iter().map(Into::into).collect(),
            size_bytes: record.size_bytes.into(),
            compression: record.compression.into(),
        }
    }
}

#[derive(Debug, Clone, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RecordPage {
    pub records: Vec<Record>,
    /// False when the scan hit its deadline with windows still unread: the
    /// records are real, but the page is not everything the query matched.
    /// `nextCursor` resumes where the scan stopped.
    pub complete: bool,
    /// True when an obfuscation rule covers this topic. Keys, values, and
    /// headers are then a view of the records: protected fields render as
    /// `***` or as `kx:` tokens, a value that never decoded may be masked
    /// whole, and filters match that view rather than the wire record.
    pub obfuscated: bool,
    pub next_cursor: Option<String>,
    pub prev_cursor: Option<String>,
}

impl From<domain::RecordPage> for RecordPage {
    fn from(page: domain::RecordPage) -> Self {
        Self {
            records: page.records.into_iter().map(Into::into).collect(),
            complete: page.complete,
            obfuscated: page.obfuscated,
            next_cursor: page.next_cursor,
            prev_cursor: page.prev_cursor,
        }
    }
}

pub(crate) fn record_query(
    topic: String,
    params: RecordParams,
) -> Result<domain::RecordQuery, QueryError> {
    Ok(domain::RecordQuery {
        timestamps: domain::TimestampRange::new(params.from, params.to)?,
        filter: params
            .contains
            .as_deref()
            .and_then(crate::kafka::compile_contains_filter),
        cursor: match params.cursor.as_deref().map(str::trim) {
            None | Some("") => None,
            Some(cursor) => Some(RecordCursor::parse(cursor)?),
        },
        topic,
        partition: params.partition,
        limit: params.limit,
        order: params.order.unwrap_or(RecordOrder::Newest).into(),
        schema_id: params.schema_id,
    })
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RecordParams {
    pub partition: Option<i32>,
    pub order: Option<RecordOrder>,
    pub from: Option<Timestamp>,
    pub to: Option<Timestamp>,
    #[serde(default = "default_record_limit")]
    pub limit: i32,
    pub contains: Option<String>,
    pub schema_id: Option<i32>,
    pub cursor: Option<String>,
}

fn default_record_limit() -> i32 {
    50
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum SearchKind {
    Topic,
    Group,
    Node,
    Subject,
}

from_same_variants!(domain::SearchKind => SearchKind { Topic, Group, Node, Subject });

#[derive(Debug, Clone, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub(crate) struct SearchHit {
    pub kind: SearchKind,
    pub id: String,
    pub label: String,
    pub detail: String,
}

impl From<domain::SearchHit> for SearchHit {
    fn from(hit: domain::SearchHit) -> Self {
        Self {
            kind: hit.kind.into(),
            id: hit.id,
            label: hit.label,
            detail: hit.detail,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum TopicSortField {
    Name,
    Rate,
    RetainedMessages,
    Partitions,
    Groups,
}

#[derive(Debug, Clone, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub(crate) struct TopicRate {
    pub topic: String,
    pub rate: f64,
}

impl From<&store::TopicRate> for TopicRate {
    fn from(rate: &store::TopicRate) -> Self {
        Self {
            topic: rate.topic.to_string(),
            rate: rate.rate,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum ResyncReason {
    /// The client fell behind the change bus and missed events.
    Lagged,
}

/// One lane delta. `type` is the discriminant the client switches on.
#[derive(Debug, Clone, Serialize, TS)]
#[serde(
    tag = "type",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub(crate) enum Update {
    Watermarks {
        at: Timestamp,
        /// One `{topic, rate}` pair per topic, never catalog objects. A scoped
        /// subscriber gets only its topic.
        topics: Vec<TopicRate>,
    },
    GroupLag {
        at: Timestamp,
        group: String,
        lag: Int64,
        lag_complete: bool,
        offsets: Vec<GroupOffset>,
    },
    Topology {
        version: Int64,
        added_topics: Vec<String>,
        removed_topics: Vec<String>,
        changed_topics: Vec<String>,
        added_groups: Vec<String>,
        removed_groups: Vec<String>,
        changed_groups: Vec<String>,
        brokers_changed: bool,
    },
    Configs {
        version: Int64,
        topics: Vec<String>,
    },
    Subjects {
        version: Int64,
        added: Vec<String>,
        removed: Vec<String>,
        changed: Vec<String>,
    },
    Resync {
        reason: ResyncReason,
    },
}

impl Update {
    pub(crate) fn event(&self) -> &'static str {
        match self {
            Self::Watermarks { .. } => "watermarks",
            Self::GroupLag { .. } => "groupLag",
            Self::Topology { .. } => "topology",
            Self::Configs { .. } => "configs",
            Self::Subjects { .. } => "subjects",
            Self::Resync { .. } => "resync",
        }
    }
}

pub(crate) fn names(values: &[std::sync::Arc<str>]) -> Vec<String> {
    values.iter().map(|value| value.to_string()).collect()
}

pub fn typescript() -> String {
    let cfg = ts_rs::Config::from_env();
    let mut out = String::from("// Generated by `cargo xtask types`. Do not edit.\n\n");
    macro_rules! emit {
        ($($ty:ty),+ $(,)?) => {
            $(
                out.push_str("export ");
                out.push_str(&<$ty as TS>::decl(&cfg));
                out.push_str("\n\n");
            )+
        };
    }
    emit!(
        Int64,
        PrivilegeName,
        ClusterGrant,
        Identity,
        LaneHealth,
        ClusterHealth,
        CleanupPolicy,
        TopicRow,
        TopicRowPage,
        PartitionRow,
        TopicDetail,
        GroupState,
        MemberAssignment,
        GroupMember,
        GroupOffset,
        GroupRow,
        GroupRowPage,
        GroupDetail,
        TopicGroupRow,
        BrokerRow,
        ConfigSource,
        ConfigEntry,
        SchemaType,
        SchemaCompatibility,
        SubjectRow,
        SubjectRowsResult,
        SchemaReference,
        SubjectDetail,
        AclAuthorizer,
        AclResourceType,
        AclPatternType,
        AclOperation,
        AclPermission,
        Acl,
        AclListing,
        Compression,
        RecordOrder,
        RecordHeader,
        Record,
        RecordPage,
        SearchKind,
        SearchHit,
        TopicSortField,
        TopicRate,
        ResyncReason,
        Update,
    );
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn large_values_round_trip_as_strings() {
        let offset = Int64::from(9_007_199_254_740_993_i64);
        let json = serde_json::to_value(offset).expect("json");

        assert_eq!(json, serde_json::json!("9007199254740993"));
        assert_eq!(
            serde_json::from_value::<Int64>(json).expect("parse"),
            offset
        );
    }

    #[test]
    fn small_values_may_arrive_as_numbers() {
        let parsed = serde_json::from_value::<Int64>(serde_json::json!(42)).expect("parse");

        assert_eq!(parsed, Int64::from(42));
    }

    #[test]
    fn a_record_keeps_its_wire_schema_id() {
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

        assert_eq!(Record::from(record).schema_id, Some(12));
    }

    #[test]
    fn generated_typescript_is_checked_in() {
        let path =
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("web/src/api/types.gen.ts");
        let actual = std::fs::read_to_string(&path).unwrap_or_default();

        assert_eq!(
            actual,
            typescript(),
            "frontend types are stale; run `cargo xtask types`"
        );
    }
}
