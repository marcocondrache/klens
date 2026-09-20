use chrono::{DateTime, Utc};
use juniper::{GraphQLEnum, GraphQLInputObject, GraphQLObject, GraphQLUnion};

use crate::app::auth::access::Privilege;
use crate::kafka::model as domain;
use crate::kafka::store::{self, projections};
use crate::kafka::{CompiledFilter, QueryError, RecordCursor};
use crate::r#macro::from_same_variants;
use crate::utils::datetime_from_unix_millis;

use super::scalars::Int64;

#[derive(GraphQLEnum, Clone, Copy, PartialEq, Eq)]
pub(super) enum PrivilegeName {
    Records,
    Configs,
    SchemaText,
    Acls,
}

from_same_variants!(Privilege => PrivilegeName { Records, Configs, SchemaText, Acls });

/// What the session may do on one cluster. Pairwise: a wider grant elsewhere
/// does not raise this one.
#[derive(GraphQLObject)]
pub(super) struct ClusterGrant {
    pub cluster: String,
    /// Names of the roles that granted this access, for tracing a privilege
    /// back to an IdP group mapping. Empty when no role table applies.
    pub roles: Vec<String>,
    pub privileges: Vec<PrivilegeName>,
}

#[derive(GraphQLObject)]
pub(super) struct Identity {
    /// `null` when authentication is disabled.
    pub subject: Option<String>,
    pub clusters: Vec<ClusterGrant>,
}

#[derive(GraphQLObject)]
pub(super) struct LaneHealth {
    pub updated_at: Option<DateTime<Utc>>,
    pub checked_at: Option<DateTime<Utc>>,
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

/// Per-lane freshness and the counts a dashboard header needs. Replaces
/// polling a catalog just to learn how stale it is.
#[derive(GraphQLObject)]
pub(super) struct ClusterHealth {
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

#[derive(GraphQLEnum, Clone, Copy)]
pub(super) enum CleanupPolicy {
    Delete,
    Compact,
    CompactDelete,
}

from_same_variants!(domain::CleanupPolicy => CleanupPolicy { Delete, Compact, CompactDelete });

#[derive(GraphQLObject)]
pub(super) struct TopicRow {
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

#[derive(GraphQLObject)]
pub(super) struct TopicRowPage {
    pub rows: Vec<TopicRow>,
    /// Rows matching the filter before paging, so a client can size its
    /// scrollbar without walking every page.
    pub total: i32,
    pub next_cursor: Option<String>,
}

#[derive(GraphQLObject)]
pub(super) struct PartitionRow {
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

#[derive(GraphQLObject)]
pub(super) struct TopicDetail {
    pub name: String,
    pub internal: bool,
    pub partitions: Vec<PartitionRow>,
    pub replication_factor: i32,
    pub retained_messages: Int64,
    pub produced_total: Int64,
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
            group_count: detail.group_count,
            under_replicated: detail.under_replicated,
        }
    }
}

#[derive(GraphQLEnum, Clone, Copy)]
pub(super) enum GroupState {
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

#[derive(GraphQLObject, Clone)]
pub(super) struct MemberAssignment {
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

#[derive(GraphQLObject, Clone)]
pub(super) struct GroupMember {
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

#[derive(GraphQLObject, Clone)]
pub(super) struct GroupOffset {
    pub topic: String,
    pub partition: i32,
    /// `null` when this assigned partition has no committed offset yet.
    pub current_offset: Option<Int64>,
    pub end_offset: Option<Int64>,
    /// `null` when lag cannot be computed because there is no commit.
    pub lag: Option<Int64>,
    pub member_id: Option<String>,
}

impl From<domain::GroupOffset> for GroupOffset {
    fn from(offset: domain::GroupOffset) -> Self {
        Self {
            topic: offset.topic,
            partition: offset.partition,
            current_offset: offset.current_offset.map(Int64::from),
            end_offset: offset.end_offset.map(Int64::from),
            lag: offset.lag.map(Int64::from),
            member_id: offset.member_id,
        }
    }
}

#[derive(GraphQLObject)]
pub(super) struct GroupRow {
    pub id: String,
    pub state: GroupState,
    pub member_count: i32,
    pub topic_names: Vec<String>,
    /// `null` until committed offsets have been loaded. A group with
    /// assignments but no commits is still loading, not at the log start.
    pub total_lag: Option<Int64>,
    /// False when a partition is missing a commit or a watermark, so a
    /// present total may understate real lag.
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
            total_lag: row.total_lag.map(Int64::from),
            lag_complete: row.lag_complete,
            coordinator_id: row.coordinator_id,
        }
    }
}

#[derive(GraphQLObject)]
pub(super) struct GroupRowPage {
    pub rows: Vec<GroupRow>,
    pub total: i32,
    pub next_cursor: Option<String>,
}

#[derive(GraphQLObject)]
pub(super) struct GroupDetail {
    pub id: String,
    pub state: GroupState,
    pub protocol: String,
    pub coordinator_id: i32,
    pub members: Vec<GroupMember>,
    pub offsets: Vec<GroupOffset>,
    /// `null` until committed offsets have been loaded.
    pub total_lag: Option<Int64>,
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
            total_lag: detail.total_lag.map(Int64::from),
            lag_complete: detail.lag_complete,
        }
    }
}

/// The projection a topic page needs: which groups read this topic and how
/// far behind they are on it alone.
#[derive(GraphQLObject)]
pub(super) struct TopicGroupRow {
    pub id: String,
    pub state: GroupState,
    pub member_count: i32,
    /// `null` until a committed offset exists on this topic.
    pub lag_on_topic: Option<Int64>,
}

impl From<projections::TopicGroupRow> for TopicGroupRow {
    fn from(row: projections::TopicGroupRow) -> Self {
        Self {
            id: row.id.to_string(),
            state: row.state.into(),
            member_count: row.member_count,
            lag_on_topic: row.lag_on_topic.map(Int64::from),
        }
    }
}

#[derive(GraphQLObject)]
pub(super) struct BrokerRow {
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

#[derive(GraphQLEnum, Clone, Copy)]
#[allow(clippy::enum_variant_names)]
pub(super) enum ConfigSource {
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

#[derive(GraphQLObject)]
pub(super) struct ConfigEntry {
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

#[derive(GraphQLEnum, Clone, Copy)]
pub(super) enum SchemaType {
    Avro,
    Json,
    Protobuf,
}

from_same_variants!(domain::SchemaType => SchemaType { Avro, Json, Protobuf });

#[derive(GraphQLEnum, Clone, Copy)]
pub(super) enum SchemaCompatibility {
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

#[derive(GraphQLObject)]
pub(super) struct SubjectRow {
    pub subject: String,
    pub id: i32,
    #[graphql(name = "type")]
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

/// Registry degradation is typed rather than hidden: an empty `rows` with an
/// unhealthy `sourceHealth` is a registry outage, not a registry with no
/// subjects.
#[derive(GraphQLObject)]
pub(super) struct SubjectRowsResult {
    pub rows: Vec<SubjectRow>,
    pub source_health: LaneHealth,
}

#[derive(GraphQLObject)]
pub(super) struct SchemaReference {
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

#[derive(GraphQLObject)]
pub(super) struct SubjectDetail {
    pub subject: String,
    pub version: i32,
    pub id: i32,
    #[graphql(name = "type")]
    pub schema_type: SchemaType,
    pub schema: String,
    pub references: Vec<SchemaReference>,
}

impl SubjectDetail {
    pub(super) fn new(subject: String, version: i32, schema: domain::RegisteredSchema) -> Self {
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

#[derive(GraphQLEnum, Clone, Copy)]
pub(super) enum AclAuthorizer {
    Enabled,
    Disabled,
}

#[derive(GraphQLEnum, Clone, Copy)]
pub(super) enum AclResourceType {
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

#[derive(GraphQLEnum, Clone, Copy)]
pub(super) enum AclPatternType {
    Literal,
    Prefixed,
}

from_same_variants!(domain::AclPatternType => AclPatternType { Literal, Prefixed });

#[derive(GraphQLEnum, Clone, Copy)]
pub(super) enum AclOperation {
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

#[derive(GraphQLEnum, Clone, Copy)]
pub(super) enum AclPermission {
    Allow,
    Deny,
}

from_same_variants!(domain::AclPermission => AclPermission { Allow, Deny });

#[derive(GraphQLObject)]
pub(super) struct Acl {
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

#[derive(GraphQLObject)]
pub(super) struct AclListing {
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

#[derive(GraphQLEnum, Clone, Copy)]
pub(super) enum Compression {
    None,
    Gzip,
    Snappy,
    Lz4,
    Zstd,
}

from_same_variants!(domain::Compression => Compression { None, Gzip, Snappy, Lz4, Zstd });

#[derive(GraphQLEnum, Clone, Copy)]
pub(super) enum RecordOrder {
    Newest,
    Oldest,
}

from_same_variants!(RecordOrder => domain::RecordOrder { Newest, Oldest });

#[derive(GraphQLObject)]
pub(super) struct RecordHeader {
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

#[derive(GraphQLObject)]
pub(super) struct Record {
    pub topic: String,
    pub partition: i32,
    pub offset: Int64,
    pub timestamp: DateTime<Utc>,
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
            timestamp: datetime_from_unix_millis(record.timestamp),
            key: record.key,
            value: record.value,
            schema_id: record.schema_id,
            headers: record.headers.into_iter().map(Into::into).collect(),
            size_bytes: record.size_bytes.into(),
            compression: record.compression.into(),
        }
    }
}

#[derive(GraphQLObject)]
pub(super) struct RecordPage {
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

/// A substring match or a CEL expression, never both.
#[derive(GraphQLInputObject)]
pub(super) struct RecordFilterInput {
    pub contains: Option<String>,
    pub cel: Option<String>,
}

impl RecordFilterInput {
    fn compile(self) -> Result<Option<CompiledFilter>, QueryError> {
        match (
            self.contains.as_deref().map(str::trim).filter(non_empty),
            self.cel.as_deref().map(str::trim).filter(non_empty),
        ) {
            (Some(_), Some(_)) => Err(QueryError::InvalidFilter(
                "set either 'contains' or 'cel', not both".to_owned(),
            )),
            (Some(needle), None) => Ok(crate::kafka::compile_contains_filter(needle)),
            (None, Some(source)) => crate::kafka::compile_cel_filter(source),
            (None, None) => Ok(None),
        }
    }
}

fn non_empty(value: &&str) -> bool {
    !value.is_empty()
}

#[derive(GraphQLInputObject)]
pub(super) struct RecordQueryInput {
    pub topic: String,
    pub partition: Option<i32>,
    /// Nullable rather than defaulted because juniper renders an enum
    /// default as a quoted string, which is not valid SDL.
    pub order: Option<RecordOrder>,
    pub from: Option<DateTime<Utc>>,
    pub to: Option<DateTime<Utc>>,
    #[graphql(default = 50)]
    pub limit: i32,
    pub filter: Option<RecordFilterInput>,
    pub schema_id: Option<i32>,
    pub cursor: Option<String>,
}

impl TryFrom<RecordQueryInput> for domain::RecordQuery {
    type Error = QueryError;

    fn try_from(query: RecordQueryInput) -> Result<Self, Self::Error> {
        Ok(Self {
            timestamps: domain::TimestampRange::new(query.from, query.to)?,
            filter: query
                .filter
                .map(RecordFilterInput::compile)
                .transpose()?
                .flatten(),
            cursor: match query.cursor.as_deref().map(str::trim) {
                None | Some("") => None,
                Some(cursor) => Some(RecordCursor::parse(cursor)?),
            },
            topic: query.topic,
            partition: query.partition,
            limit: query.limit,
            order: query.order.unwrap_or(RecordOrder::Newest).into(),
            schema_id: query.schema_id,
        })
    }
}

#[derive(GraphQLEnum, Clone, Copy)]
pub(super) enum SearchKind {
    Topic,
    Group,
    Node,
    Subject,
}

from_same_variants!(domain::SearchKind => SearchKind { Topic, Group, Node, Subject });

#[derive(GraphQLObject)]
pub(super) struct SearchHit {
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

/// Server-side name filtering so a 10k-topic cluster's list page does not
/// need the full set client-side.
#[derive(GraphQLInputObject, Default)]
pub(super) struct RowFilter {
    pub contains: Option<String>,
}

impl RowFilter {
    pub(super) fn matches(&self, name: &str) -> bool {
        match self.contains.as_deref().map(str::trim).filter(non_empty) {
            None => true,
            Some(needle) => name.to_lowercase().contains(&needle.to_lowercase()),
        }
    }
}

#[derive(GraphQLEnum, Clone, Copy, PartialEq, Eq)]
pub(super) enum TopicSortField {
    Name,
    Rate,
    RetainedMessages,
    Partitions,
    Groups,
}

#[derive(GraphQLInputObject, Default)]
pub(super) struct TopicSort {
    /// Nullable rather than defaulted because juniper renders an enum
    /// default as a quoted string, which is not valid SDL.
    pub field: Option<TopicSortField>,
    #[graphql(default = false)]
    pub desc: bool,
}

#[derive(GraphQLInputObject, Default, Clone)]
pub(super) struct UpdateScope {
    pub topic: Option<String>,
    pub group: Option<String>,
}

#[derive(GraphQLObject, Clone)]
pub(super) struct TopicRate {
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

#[derive(GraphQLObject, Clone)]
pub(super) struct WatermarksTick {
    pub at: DateTime<Utc>,
    /// One `{topic, rate}` pair per topic, never catalog objects. A scoped
    /// subscriber gets only its topic.
    pub topics: Vec<TopicRate>,
}

#[derive(GraphQLObject, Clone)]
pub(super) struct GroupLagUpdate {
    pub at: DateTime<Utc>,
    pub group: String,
    pub lag: Option<Int64>,
    pub lag_complete: bool,
    pub offsets: Vec<GroupOffset>,
}

#[derive(GraphQLObject, Clone)]
pub(super) struct TopologyDelta {
    pub version: Int64,
    pub added_topics: Vec<String>,
    pub removed_topics: Vec<String>,
    pub changed_topics: Vec<String>,
    pub added_groups: Vec<String>,
    pub removed_groups: Vec<String>,
    pub changed_groups: Vec<String>,
    pub brokers_changed: bool,
}

#[derive(GraphQLObject, Clone)]
pub(super) struct ConfigsChanged {
    pub version: Int64,
    pub topics: Vec<String>,
}

#[derive(GraphQLObject, Clone)]
pub(super) struct SubjectsChanged {
    pub version: Int64,
    pub added: Vec<String>,
    pub removed: Vec<String>,
    pub changed: Vec<String>,
}

#[derive(GraphQLEnum, Clone, Copy)]
pub(super) enum ResyncReason {
    /// The client fell behind the change bus and missed events.
    Lagged,
}

/// Refetch the projections and keep the stream: the deltas in between are
/// gone, but the connection is still good.
#[derive(GraphQLObject, Clone)]
pub(super) struct Resync {
    pub reason: ResyncReason,
}

#[derive(GraphQLUnion, Clone)]
#[graphql(context = super::context::GraphQlContext)]
pub(super) enum Update {
    Watermarks(WatermarksTick),
    GroupLag(GroupLagUpdate),
    Topology(TopologyDelta),
    Configs(ConfigsChanged),
    Subjects(SubjectsChanged),
    Resync(Resync),
}

pub(super) fn names(values: &[std::sync::Arc<str>]) -> Vec<String> {
    values.iter().map(|value| value.to_string()).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn a_filter_may_not_be_both_substring_and_cel() {
        let filter = RecordFilterInput {
            contains: Some("boom".into()),
            cel: Some("record.key == 'k'".into()),
        };

        assert!(filter.compile().is_err());
    }

    #[test]
    fn an_empty_filter_compiles_to_no_filter() {
        let filter = RecordFilterInput {
            contains: Some("   ".into()),
            cel: None,
        };

        assert!(filter.compile().unwrap().is_none());
    }

    #[test]
    fn row_filters_are_case_insensitive_substrings() {
        let filter = RowFilter {
            contains: Some("ORDERS".into()),
        };

        assert!(filter.matches("orders.created"));
        assert!(!filter.matches("payments"));
        assert!(RowFilter::default().matches("anything"));
    }
}
