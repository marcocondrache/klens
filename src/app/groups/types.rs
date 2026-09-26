use serde::{Deserialize, Serialize};
use ts_rs::TS;

use crate::kafka::model as domain;
use crate::kafka::store::projections;
use crate::r#macro::from_same_variants;

use super::super::int64::Int64;

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
    pub current_offset: Option<Int64>,
    pub end_offset: Option<Int64>,
    pub lag: Option<Int64>,
    pub member_id: Option<String>,
}

impl From<domain::GroupOffset> for GroupOffset {
    fn from(offset: domain::GroupOffset) -> Self {
        Self {
            topic: offset.topic,
            partition: offset.partition,
            current_offset: offset.current_offset.map(Into::into),
            end_offset: offset.end_offset.map(Into::into),
            lag: offset.lag.map(Into::into),
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
    pub total_lag: Option<Int64>,
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
            total_lag: row.total_lag.map(Into::into),
            lag_complete: row.lag_complete,
            coordinator_id: row.coordinator_id,
        }
    }
}

#[derive(Debug, Clone, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct GroupRowPage {
    pub rows: Vec<GroupRow>,
    pub total: i32,
    pub next_cursor: Option<String>,
}

#[derive(Debug, Clone, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct GroupDetail {
    pub id: String,
    pub state: GroupState,
    pub protocol: String,
    pub coordinator_id: i32,
    pub members: Vec<GroupMember>,
    pub offsets: Vec<GroupOffset>,
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
            total_lag: detail.total_lag.map(Into::into),
            lag_complete: detail.lag_complete,
        }
    }
}
