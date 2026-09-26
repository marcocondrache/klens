use serde::{Deserialize, Serialize};
use ts_rs::TS;

use crate::app::int64::Int64;
use crate::kafka::{OffsetMove, ResetTarget};

#[derive(Debug, Clone, Deserialize, TS)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ResetOffsetsRequest {
    pub group: String,
    #[serde(default)]
    #[ts(optional)]
    pub topic: Option<String>,
    #[serde(default)]
    #[ts(optional)]
    pub partitions: Option<Vec<i32>>,
    pub to: ResetTo,
    #[serde(default)]
    pub dry_run: bool,
}

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
    Timestamp { timestamp: Int64 },
    Offset { offset: Int64 },
    Shift { by: Int64 },
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
    pub applied: bool,
    pub partitions: Vec<OffsetChange>,
}

#[derive(Debug, Clone, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct OffsetChange {
    pub topic: String,
    pub partition: i32,
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

#[derive(Debug, Clone, Deserialize, TS)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DeleteOffsetsRequest {
    pub group: String,
    pub topic: String,
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
