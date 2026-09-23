use serde::{Deserialize, Serialize};
use ts_rs::TS;

use crate::app::int64::Int64;
use crate::kafka::{OffsetMove, ResetTarget};

/// Moves a consumer group's committed offsets. The group must have no live
/// members unless `dryRun` is set.
#[derive(Debug, Clone, Deserialize, TS)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ResetOffsetsRequest {
    pub group: String,
    /// Omitted, the reset covers every partition the group has committed on.
    #[serde(default)]
    #[ts(optional)]
    pub topic: Option<String>,
    /// Partitions of `topic`. Omitted, every partition of the topic.
    #[serde(default)]
    #[ts(optional)]
    pub partitions: Option<Vec<i32>>,
    pub to: ResetTo,
    /// Plan the reset without committing anything.
    #[serde(default)]
    pub dry_run: bool,
}

/// Where each partition moves. Targets are clamped to the partition's log.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, TS)]
#[serde(
    tag = "kind",
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub enum ResetTo {
    Earliest,
    Latest,
    /// First record at or after this unix-ms time, or the log end.
    Timestamp {
        timestamp: Int64,
    },
    Offset {
        offset: Int64,
    },
    /// Relative to the committed offset. Negative moves back.
    Shift {
        by: Int64,
    },
}

impl From<ResetTo> for ResetTarget {
    fn from(to: ResetTo) -> Self {
        match to {
            ResetTo::Earliest => Self::Earliest,
            ResetTo::Latest => Self::Latest,
            ResetTo::Timestamp { timestamp } => Self::Timestamp(timestamp.into()),
            ResetTo::Offset { offset } => Self::Offset(offset.into()),
            ResetTo::Shift { by } => Self::Shift(by.into()),
        }
    }
}

#[derive(Debug, Clone, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct OffsetReset {
    pub group: String,
    /// False for a dry run.
    pub applied: bool,
    pub partitions: Vec<OffsetChange>,
}

#[derive(Debug, Clone, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct OffsetChange {
    pub topic: String,
    pub partition: i32,
    /// `null` when the group had never committed on this partition.
    pub current: Option<Int64>,
    pub target: Int64,
}

impl From<OffsetMove> for OffsetChange {
    fn from(step: OffsetMove) -> Self {
        Self {
            topic: step.topic,
            partition: step.partition,
            current: step.current.map(Int64::from),
            target: step.target.into(),
        }
    }
}

/// Forgets a group's committed offsets on one topic. Irreversible, so
/// `confirm` must repeat the group id.
#[derive(Debug, Clone, Deserialize, TS)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DeleteOffsetsRequest {
    pub group: String,
    pub topic: String,
    /// Omitted, every partition the group has committed on in `topic`.
    #[serde(default)]
    #[ts(optional)]
    pub partitions: Option<Vec<i32>>,
    pub confirm: String,
}

#[derive(Debug, Clone, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct DeletedOffsets {
    pub group: String,
    pub topic: String,
    pub partitions: Vec<i32>,
}
