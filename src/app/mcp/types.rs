use std::borrow::Cow;

use jiff::Timestamp;
use serde::Serialize;

use crate::app::acls::Acl;
use crate::app::acls::types::AclStatus;
use crate::app::brokers::types::LogDir;
use crate::app::configs::ConfigEntry;
use crate::app::groups::types::GroupState;
use crate::app::records::types::Record;
use crate::app::search::SearchHit;
use crate::app::subjects::types::{
    SchemaCompatibility, SchemaReference, SchemaType, SubjectVersion,
};
use crate::app::topics::types::{CleanupPolicy, TopicGroupRow};
use crate::app::whoami::types::PrivilegeName;
use crate::kafka::model as domain;
use crate::kafka::store::LaneHealth;
use crate::kafka::store::projections::{self, ClusterHealthView};
use crate::kafka::store::tables::SubjectInfo;

use super::findings::Finding;
use super::untrusted::{Boundary, clip};
use super::{MAX_CONFIG_CHARS, MAX_VERSIONS};
use crate::app::mcp::fit::{first, left_out};
use crate::app::mcp::lanes::lane_error;
use crate::app::mcp::view::shortened;

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ClusterList<'a> {
    pub clusters: &'a [ClusterRow],
    #[serde(skip_serializing_if = "Option::is_none")]
    pub notice: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub truncated: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ClusterRow {
    pub cluster: String,
    pub ready: bool,
    pub broker_count: Option<i32>,
    pub topic_count: Option<i32>,
    pub partition_count: Option<i32>,
    pub group_count: Option<i32>,
    pub subject_count: Option<i32>,
    pub under_replicated_partitions: Option<i32>,
    pub offline_partitions: Option<i32>,
    pub unhealthy_lanes: Vec<UnhealthyLane>,
}

impl ClusterRow {
    pub fn new(health: ClusterHealthView, boundary: &Boundary) -> Self {
        let ready = health.topology.updated_at.is_some();
        let measured = |count| ready.then_some(count);
        let subjects_read = health.subjects.updated_at.is_some();
        let lanes = [
            ("topology", health.topology),
            ("watermarks", health.watermarks),
            ("offsets", health.offsets),
            ("configs", health.configs),
            ("subjects", health.subjects),
            ("log_dirs", health.log_dirs),
            ("acls", health.acls),
            ("quotas", health.quotas),
            ("scram_users", health.scram_users),
        ];
        Self {
            ready,
            cluster: health.cluster,
            broker_count: measured(health.broker_count),
            topic_count: measured(health.topic_count),
            partition_count: measured(health.partition_count),
            group_count: measured(health.group_count),
            subject_count: subjects_read.then_some(health.subject_count),
            under_replicated_partitions: measured(health.under_replicated_partitions),
            offline_partitions: measured(health.offline_partitions),
            unhealthy_lanes: lanes
                .into_iter()
                .filter(|(_, lane)| !lane.healthy())
                .map(|(lane, health)| UnhealthyLane::new(lane, health, boundary))
                .collect(),
        }
    }

    pub fn has_lane_error(&self) -> bool {
        self.unhealthy_lanes
            .iter()
            .any(|lane| lane.last_error.is_some())
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct UnhealthyLane {
    pub lane: &'static str,
    pub last_error: Option<String>,
    pub updated_at: Option<Timestamp>,
}

impl UnhealthyLane {
    fn new(lane: &'static str, health: LaneHealth, boundary: &Boundary) -> Self {
        Self {
            lane,
            last_error: lane_error(boundary, health.last_error),
            updated_at: health.updated_at,
        }
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ClusterDetail<'a> {
    #[serde(flatten)]
    pub row: &'a ClusterRow,
    pub unhealthy_partitions: &'a [UnhealthyPartition],
    #[serde(skip_serializing_if = "Option::is_none")]
    pub notice: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub truncated: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct UnhealthyPartition {
    pub topic: String,
    pub partition: i32,
    pub leader: Option<i32>,
    pub replicas: Vec<i32>,
    pub isr: Vec<i32>,
    pub offline: bool,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AccessList<'a> {
    pub clusters: &'a [ClusterRights],
    #[serde(skip_serializing_if = "Option::is_none")]
    pub truncated: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ClusterRights {
    pub cluster: String,
    pub writable: bool,
    pub privileges: Vec<PrivilegeName>,
    pub tools: Vec<ToolRights>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ToolRights {
    pub name: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub section: Option<Section>,
    pub available: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub needs: Option<PrivilegeName>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum Section {
    Configs,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Omitted {
    pub section: Section,
    #[serde(flatten)]
    pub reason: Reason,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase", rename_all_fields = "camelCase")]
pub enum Reason {
    Needs(PrivilegeName),
    NotRead { last_error: Option<String> },
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ConfigRow {
    #[serde(flatten)]
    pub entry: ConfigEntry,
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub cut: bool,
}

impl ConfigRow {
    pub fn new(entry: domain::ConfigEntry) -> Self {
        let mut entry = ConfigEntry::from(entry);
        let cut = match &mut entry.value {
            Some(value) => {
                let (kept, cut) = clip(value, MAX_CONFIG_CHARS);
                value.truncate(kept.len());
                cut
            }
            None => false,
        };
        Self { entry, cut }
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SearchResult<'a> {
    pub hits: &'a [ClusterHit],
    #[serde(skip_serializing_if = "<[_]>::is_empty")]
    pub not_ready: &'a [UnreadLane],
    #[serde(skip_serializing_if = "Option::is_none")]
    pub notice: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub truncated: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct UnreadLane {
    pub cluster: String,
    pub lane: &'static str,
    pub last_error: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ClusterHit {
    pub cluster: String,
    #[serde(flatten)]
    pub hit: SearchHit,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BrokerList<'a> {
    pub brokers: &'a [BrokerRow],
    pub showing: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BrokerDetail<'a> {
    #[serde(flatten)]
    pub broker: &'a BrokerRow,
    pub configs: Option<&'a [ConfigRow]>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub omitted: Option<&'a Omitted>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub truncated: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BrokerRow {
    pub id: i32,
    pub host: String,
    pub port: i32,
    pub rack: Option<String>,
    pub controller: Option<bool>,
    pub partition_count: i32,
    pub leader_count: i32,
    pub size_bytes: Option<i64>,
    pub log_dirs: Option<Vec<LogDir>>,
}

impl BrokerRow {
    pub fn new(row: projections::BrokerRow, controller_known: bool, log_dirs_read: bool) -> Self {
        Self {
            id: row.id,
            host: row.host,
            port: row.port,
            rack: row.rack,
            controller: controller_known.then_some(row.controller),
            partition_count: row.partition_count,
            leader_count: row.leader_count,
            size_bytes: row.size_bytes,
            log_dirs: log_dirs_read.then(|| row.log_dirs.into_iter().map(LogDir::from).collect()),
        }
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AclList<'a> {
    pub status: AclStatus,
    pub bindings: &'a [Acl],
    pub showing: String,
    pub notice: &'static str,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SubjectList<'a> {
    pub subjects: &'a [SubjectRow],
    pub showing: String,
    pub notice: &'static str,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SubjectRow {
    pub subject: String,
    pub latest_version: i32,
    #[serde(rename = "type")]
    pub schema_type: SchemaType,
    pub compatibility: SchemaCompatibility,
    #[serde(flatten)]
    pub versions: Option<SubjectVersions>,
}

impl SubjectRow {
    pub fn new(row: projections::SubjectRow, detailed: bool) -> Self {
        let older = row.versions.len().saturating_sub(MAX_VERSIONS);
        Self {
            versions: detailed.then(|| SubjectVersions {
                latest_schema_id: row.info.id,
                versions: row.versions[older..]
                    .iter()
                    .copied()
                    .map(SubjectVersion::from)
                    .collect(),
                versions_left_out: (older > 0).then_some(older),
            }),
            ..Self::concise(row.subject.to_string(), &row.info)
        }
    }

    pub fn concise(subject: String, info: &SubjectInfo) -> Self {
        Self {
            subject,
            latest_version: info.latest_version,
            schema_type: info.schema_type.into(),
            compatibility: info.compatibility.into(),
            versions: None,
        }
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SubjectVersions {
    pub latest_schema_id: i32,
    pub versions: Vec<SubjectVersion>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub versions_left_out: Option<usize>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TopicList<'a> {
    pub topics: &'a [TopicRow],
    pub showing: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub unmeasured: Option<usize>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TopicRow {
    pub name: String,
    pub partition_count: i32,
    pub retained_messages: Option<i64>,
    pub size_bytes: Option<i64>,
    pub rate: Option<f64>,
    pub group_count: i32,
    pub under_replicated: bool,
    #[serde(flatten)]
    pub detail: Option<TopicRowDetail>,
}

impl TopicRow {
    pub fn new(
        row: projections::TopicRow,
        counted: bool,
        rate: Option<f64>,
        detailed: bool,
    ) -> Self {
        Self {
            detail: detailed.then(|| TopicRowDetail {
                internal: row.internal,
                replication_factor: row.replication_factor,
                produced_total: counted.then_some(row.produced_total),
                retention_ms: row.retention_ms,
                cleanup_policy: row.cleanup_policy.into(),
            }),
            name: row.name.to_string(),
            partition_count: row.partition_count,
            retained_messages: counted.then_some(row.retained_messages),
            size_bytes: row.size_bytes,
            rate,
            group_count: row.group_count,
            under_replicated: row.under_replicated,
        }
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TopicRowDetail {
    pub internal: bool,
    pub replication_factor: i32,
    pub produced_total: Option<i64>,
    pub retention_ms: Option<i64>,
    pub cleanup_policy: CleanupPolicy,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CreatedTopic<'a> {
    pub topic: &'a str,
    pub partitions: Option<usize>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TopicDescription<'a> {
    #[serde(flatten)]
    pub topic: &'a TopicSummary,
    pub configs: Option<&'a [ConfigRow]>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub omitted: Option<&'a Omitted>,
    pub groups: &'a [TopicGroupRow],
    pub subjects: Option<&'a [SubjectRow]>,
    pub partitions: &'a [PartitionRow],
    pub notice: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub truncated: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TopicSummary {
    pub name: String,
    pub internal: bool,
    pub partition_count: usize,
    pub replication_factor: i32,
    pub retained_messages: Option<i64>,
    pub produced_total: Option<i64>,
    pub size_bytes: Option<i64>,
    pub disk_bytes: Option<i64>,
    pub rate: Option<f64>,
    pub retention_ms: Option<i64>,
    pub cleanup_policy: CleanupPolicy,
    pub under_replicated_partitions: usize,
    pub offline_partitions: usize,
}

impl TopicSummary {
    pub fn new(detail: &projections::TopicDetail, counted: bool, rate: Option<f64>) -> Self {
        let count = |test: fn(&projections::PartitionRow) -> bool| {
            detail
                .partitions
                .iter()
                .filter(|partition| test(partition))
                .count()
        };
        Self {
            name: detail.name.to_string(),
            internal: detail.internal,
            partition_count: detail.partitions.len(),
            replication_factor: detail.replication_factor,
            retained_messages: counted.then_some(detail.retained_messages),
            produced_total: counted.then_some(detail.produced_total),
            size_bytes: detail.size_bytes,
            disk_bytes: detail.disk_bytes,
            rate,
            retention_ms: detail.retention_ms,
            cleanup_policy: detail.cleanup_policy.into(),
            under_replicated_partitions: count(projections::PartitionRow::under_replicated),
            offline_partitions: count(projections::PartitionRow::offline),
        }
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PartitionRow {
    pub partition: i32,
    pub leader: Option<i32>,
    pub replicas: Vec<i32>,
    pub isr: Vec<i32>,
    pub under_replicated: bool,
    pub offline: bool,
    pub low_watermark: Option<i64>,
    pub high_watermark: Option<i64>,
    pub retained_messages: Option<i64>,
    pub size_bytes: Option<i64>,
}

impl PartitionRow {
    pub fn new(row: &projections::PartitionRow, counted: bool) -> Self {
        Self {
            partition: row.id,
            leader: (!row.offline()).then_some(row.leader),
            replicas: row.replicas.clone(),
            isr: row.isr.clone(),
            under_replicated: row.under_replicated(),
            offline: row.offline(),
            low_watermark: counted.then_some(row.low_watermark),
            high_watermark: counted.then_some(row.high_watermark),
            retained_messages: counted.then(|| row.retained()),
            size_bytes: row.size_bytes,
        }
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GroupList<'a> {
    pub groups: &'a [GroupRow],
    pub showing: String,
    pub notice: &'static str,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GroupRow {
    pub id: String,
    pub state: GroupState,
    pub member_count: i32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub topic_names: Option<Vec<String>>,
    pub total_lag: Option<i64>,
    pub lag_complete: bool,
}

impl GroupRow {
    pub fn new(row: projections::GroupRow, detailed: bool) -> Self {
        Self {
            id: row.id.to_string(),
            state: row.state.into(),
            member_count: row.member_count,
            topic_names: detailed.then_some(row.topic_names),
            total_lag: row.total_lag,
            lag_complete: row.lag_complete,
        }
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GroupDescription<'a> {
    pub group: &'a str,
    pub state: GroupState,
    pub protocol: &'a str,
    pub total_lag: Option<i64>,
    pub lag_complete: bool,
    pub findings: &'a [Finding],
    pub members: &'a [MemberRow<'a>],
    pub partitions: &'a [GroupPartitionRow],
    pub notice: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub truncated: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MemberRow<'a> {
    pub member_id: Cow<'a, str>,
    pub client_id: Cow<'a, str>,
    pub host: Cow<'a, str>,
    pub assignments: Vec<AssignmentRow<'a>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub topics_left_out: Option<usize>,
    pub lag: Option<i64>,
}

impl<'a> MemberRow<'a> {
    pub fn new(member: &'a domain::GroupMember, lag: Option<i64>, max_per_list: usize) -> Self {
        Self {
            member_id: shortened(&member.id),
            client_id: shortened(&member.client_id),
            host: shortened(&member.host),
            assignments: member
                .assignments
                .iter()
                .take(max_per_list)
                .map(|assignment| AssignmentRow {
                    topic: shortened(&assignment.topic),
                    partitions: first(&assignment.partitions, max_per_list),
                    partitions_left_out: left_out(assignment.partitions.len(), max_per_list),
                })
                .collect(),
            topics_left_out: left_out(member.assignments.len(), max_per_list),
            lag,
        }
    }

    pub fn widest(member: &domain::GroupMember) -> usize {
        member
            .assignments
            .iter()
            .map(|assignment| assignment.partitions.len())
            .fold(member.assignments.len(), usize::max)
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AssignmentRow<'a> {
    pub topic: Cow<'a, str>,
    pub partitions: &'a [i32],
    #[serde(skip_serializing_if = "Option::is_none")]
    pub partitions_left_out: Option<usize>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GroupPartitionRow {
    pub topic: String,
    pub partition: i32,
    pub committed_offset: Option<i64>,
    pub end_offset: Option<i64>,
    pub lag: Option<i64>,
}

impl From<domain::GroupOffset> for GroupPartitionRow {
    fn from(offset: domain::GroupOffset) -> Self {
        Self {
            topic: offset.topic,
            partition: offset.partition,
            committed_offset: offset.current_offset,
            end_offset: offset.end_offset,
            lag: offset.lag,
        }
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RecordFacts {
    pub partition: i32,
    pub offset: i64,
    pub timestamp: Timestamp,
    pub size_bytes: u64,
    pub schema_id: Option<i32>,
    pub verbatim: bool,
    pub cut: bool,
    pub headers_left_out: usize,
}

impl RecordFacts {
    pub fn new(record: &Record, cut: bool, headers_left_out: usize) -> Self {
        Self {
            partition: record.partition,
            offset: record.offset,
            timestamp: record.timestamp,
            size_bytes: record.size_bytes,
            schema_id: record.schema_id,
            verbatim: record.verbatim,
            cut,
            headers_left_out,
        }
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SchemaFacts {
    pub version: i32,
    pub id: i32,
    #[serde(rename = "type")]
    pub schema_type: SchemaType,
    pub cut: bool,
    pub references_left_out: usize,
}

#[derive(Debug, Serialize)]
pub struct SchemaText<'a> {
    pub schema: &'a str,
    pub references: &'a [SchemaReference],
}

#[derive(Debug, Serialize)]
pub struct RecordText<'a> {
    pub key: Option<&'a str>,
    pub headers: Vec<HeaderText<'a>>,
    pub value: Option<&'a str>,
}

#[derive(Debug, Serialize)]
pub struct HeaderText<'a> {
    pub key: &'a str,
    pub value: &'a str,
}
