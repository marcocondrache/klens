use jiff::Timestamp;
use serde::Serialize;

use crate::app::search::SearchHit;
use crate::app::whoami::types::PrivilegeName;
use crate::kafka::store::{LaneHealth, projections::ClusterHealthView};

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
