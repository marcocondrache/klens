use jiff::Timestamp;
use serde::Serialize;
use ts_rs::TS;

use crate::kafka::store::{self, projections};

use super::super::int64::Int64;

#[derive(Debug, Clone, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct LaneHealth {
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

#[derive(Debug, Clone, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct ClusterHealth {
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
