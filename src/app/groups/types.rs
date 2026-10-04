use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};
use ts_rs::TS;

use crate::kafka::model as domain;
use crate::kafka::store::projections;
use crate::r#macro::from_same_variants;

use super::super::error::ApiError;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum GroupState {
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
pub struct MemberAssignment {
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
pub struct GroupMember {
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
pub struct GroupOffset {
    pub topic: String,
    pub partition: i32,
    pub current_offset: Option<i64>,
    pub end_offset: Option<i64>,
    pub lag: Option<i64>,
    pub member_id: Option<String>,
}

impl From<domain::GroupOffset> for GroupOffset {
    fn from(offset: domain::GroupOffset) -> Self {
        Self {
            topic: offset.topic,
            partition: offset.partition,
            current_offset: offset.current_offset,
            end_offset: offset.end_offset,
            lag: offset.lag,
            member_id: offset.member_id,
        }
    }
}

#[derive(Debug, Clone, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct GroupRow {
    pub id: String,
    pub state: GroupState,
    pub member_count: i32,
    pub topic_names: Vec<String>,
    /// Null until the group's committed offsets are first fetched.
    pub total_lag: Option<i64>,
    /// False when a committed partition had no watermark to join against, so
    /// the total understates the real lag.
    pub lag_complete: bool,
}

impl From<projections::GroupRow> for GroupRow {
    fn from(row: projections::GroupRow) -> Self {
        Self {
            id: row.id.to_string(),
            state: row.state.into(),
            member_count: row.member_count,
            topic_names: row.topic_names,
            total_lag: row.total_lag,
            lag_complete: row.lag_complete,
        }
    }
}

#[derive(Debug, Clone, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct GroupDetail {
    pub id: String,
    pub state: GroupState,
    pub protocol: String,
    pub members: Vec<GroupMember>,
    pub offsets: Vec<GroupOffset>,
    pub total_lag: Option<i64>,
    pub lag_complete: bool,
}

impl From<projections::GroupDetail> for GroupDetail {
    fn from(detail: projections::GroupDetail) -> Self {
        Self {
            id: detail.id.to_string(),
            state: detail.state.into(),
            protocol: detail.protocol,
            members: detail.members.into_iter().map(Into::into).collect(),
            offsets: detail.offsets.into_iter().map(Into::into).collect(),
            total_lag: detail.total_lag,
            lag_complete: detail.lag_complete,
        }
    }
}

#[derive(Debug, Clone, Copy, Deserialize, TS)]
#[serde(
    tag = "kind",
    rename_all = "SCREAMING_SNAKE_CASE",
    rename_all_fields = "camelCase"
)]
pub enum ResetTarget {
    Earliest,
    Latest,
    /// Clamped into the partition's log.
    Offset {
        offset: i64,
    },
    /// Relative to the committed offset, clamped into the partition's log.
    Shift {
        by: i64,
    },
    /// The first record at or after this time, in milliseconds since the
    /// epoch, or the end of the log when no record is that recent.
    Timestamp {
        timestamp: i64,
    },
}

impl From<ResetTarget> for domain::ResetTarget {
    fn from(target: ResetTarget) -> Self {
        match target {
            ResetTarget::Earliest => Self::Earliest,
            ResetTarget::Latest => Self::Latest,
            ResetTarget::Offset { offset } => Self::Offset(offset),
            ResetTarget::Shift { by } => Self::Shift(by),
            ResetTarget::Timestamp { timestamp } => Self::Timestamp(timestamp),
        }
    }
}

#[derive(Debug, Clone, Deserialize, TS)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ResetOffsets {
    /// When omitted, the reset covers every partition the group has committed
    /// an offset for.
    #[ts(optional)]
    pub topic: Option<String>,
    /// Partitions of `topic`, or every partition of it when empty.
    #[serde(default)]
    pub partitions: BTreeSet<i32>,
    pub to: ResetTarget,
    /// Answers the plan without committing it.
    #[serde(default)]
    pub dry_run: bool,
}

impl ResetOffsets {
    pub(crate) fn into_reset(self, group: String) -> Result<domain::OffsetReset, ApiError> {
        if self.topic.is_none() && !self.partitions.is_empty() {
            return Err(ApiError::unprocessable("partitions need a topic"));
        }
        Ok(domain::OffsetReset {
            group,
            topic: self.topic,
            partitions: self.partitions.into_iter().collect(),
            to: self.to.into(),
        })
    }
}

#[derive(Debug, Clone, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct OffsetMove {
    pub topic: String,
    pub partition: i32,
    /// Null when the group has no committed offset on the partition.
    pub current_offset: Option<i64>,
    pub new_offset: i64,
    pub end_offset: i64,
}

impl From<domain::OffsetMove> for OffsetMove {
    fn from(moved: domain::OffsetMove) -> Self {
        Self {
            topic: moved.topic,
            partition: moved.partition,
            current_offset: moved.from,
            new_offset: moved.to,
            end_offset: moved.end,
        }
    }
}
