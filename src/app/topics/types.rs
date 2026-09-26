use serde::{Deserialize, Serialize};
use ts_rs::TS;

use crate::kafka::model as domain;
use crate::kafka::store::projections;
use crate::r#macro::from_same_variants;

use super::super::groups::GroupState;
use super::super::int64::Int64;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum CleanupPolicy {
    Delete,
    Compact,
    CompactDelete,
}

from_same_variants!(domain::CleanupPolicy => CleanupPolicy { Delete, Compact, CompactDelete });

#[derive(Debug, Clone, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct TopicRow {
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
    /// Null until the topic's configs report a `retention.ms` value.
    pub retention_ms: Option<Int64>,
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
            retention_ms: row.retention_ms.map(Into::into),
            cleanup_policy: row.cleanup_policy.into(),
            group_count: row.group_count,
            under_replicated: row.under_replicated,
        }
    }
}

#[derive(Debug, Clone, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct TopicRowPage {
    pub rows: Vec<TopicRow>,
    /// Rows matching the filter before paging, so a client can size its
    /// scrollbar without walking every page.
    pub total: i32,
    pub next_cursor: Option<String>,
}

#[derive(Debug, Clone, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct PartitionRow {
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
pub struct TopicDetail {
    pub name: String,
    pub internal: bool,
    pub partitions: Vec<PartitionRow>,
    pub replication_factor: i32,
    pub retained_messages: Int64,
    pub produced_total: Int64,
    pub rate: f64,
    /// Null until the topic's configs report a `retention.ms` value.
    pub retention_ms: Option<Int64>,
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
            retention_ms: detail.retention_ms.map(Into::into),
            cleanup_policy: detail.cleanup_policy.into(),
            group_count: detail.group_count,
            under_replicated: detail.under_replicated,
        }
    }
}

/// Which groups read this topic, and how far behind they are on it alone.
#[derive(Debug, Clone, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct TopicGroupRow {
    pub id: String,
    pub state: GroupState,
    pub member_count: i32,
    pub lag_on_topic: Option<Int64>,
}

impl From<projections::TopicGroupRow> for TopicGroupRow {
    fn from(row: projections::TopicGroupRow) -> Self {
        Self {
            id: row.id.to_string(),
            state: row.state.into(),
            member_count: row.member_count,
            lag_on_topic: row.lag_on_topic.map(Into::into),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TopicSortField {
    Name,
    Rate,
    RetainedMessages,
    Partitions,
    Groups,
}
