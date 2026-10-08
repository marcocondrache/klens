use jiff::Timestamp;
use serde::Serialize;

use crate::app::brokers::types::LogDir;
use crate::app::search::SearchHit;
use crate::app::subjects::types::{SchemaCompatibility, SchemaType, SubjectVersion};
use crate::app::whoami::types::PrivilegeName;
use crate::kafka::store::LaneHealth;
use crate::kafka::store::projections::{self, ClusterHealthView};

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ClusterList<'a> {
    pub clusters: &'a [ClusterRow],
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

impl From<ClusterHealthView> for ClusterRow {
    fn from(health: ClusterHealthView) -> Self {
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
                .map(|(lane, health)| UnhealthyLane::new(lane, health))
                .collect(),
        }
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
    fn new(lane: &'static str, health: LaneHealth) -> Self {
        Self {
            lane,
            last_error: health.last_error,
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
    pub available: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub needs: Option<PrivilegeName>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SearchResult<'a> {
    pub hits: &'a [ClusterHit],
    #[serde(skip_serializing_if = "<[_]>::is_empty")]
    pub not_ready: &'a [UnreadLane],
    #[serde(skip_serializing_if = "Option::is_none")]
    pub notice: Option<&'static str>,
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
        Self {
            subject: row.subject.to_string(),
            latest_version: row.info.latest_version,
            schema_type: row.info.schema_type.into(),
            compatibility: row.info.compatibility.into(),
            versions: detailed.then(|| SubjectVersions {
                latest_schema_id: row.info.id,
                versions: row.versions.into_iter().map(SubjectVersion::from).collect(),
            }),
        }
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SubjectVersions {
    pub latest_schema_id: i32,
    pub versions: Vec<SubjectVersion>,
}
