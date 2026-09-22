use jiff::Timestamp;
use serde::{Deserialize, Serialize};
use ts_rs::TS;

use crate::kafka::store;

use super::super::groups::GroupOffset;
use super::super::int64::Int64;

#[derive(Debug, Clone, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct TopicRate {
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ResyncReason {
    /// The client fell behind the change bus and missed events.
    Lagged,
}

/// One lane delta. `type` is the discriminant the client switches on.
#[derive(Debug, Clone, Serialize, TS)]
#[serde(
    tag = "type",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum Update {
    Watermarks {
        at: Timestamp,
        /// One `{topic, rate}` pair per topic, never catalog objects. A scoped
        /// subscriber gets only its topic.
        topics: Vec<TopicRate>,
    },
    GroupLag {
        at: Timestamp,
        group: String,
        lag: Int64,
        lag_complete: bool,
        offsets: Vec<GroupOffset>,
    },
    Topology {
        version: Int64,
        added_topics: Vec<String>,
        removed_topics: Vec<String>,
        changed_topics: Vec<String>,
        added_groups: Vec<String>,
        removed_groups: Vec<String>,
        changed_groups: Vec<String>,
        brokers_changed: bool,
    },
    Configs {
        version: Int64,
        topics: Vec<String>,
    },
    Subjects {
        version: Int64,
        added: Vec<String>,
        removed: Vec<String>,
        changed: Vec<String>,
    },
    Resync {
        reason: ResyncReason,
    },
}

impl Update {
    pub(crate) fn event(&self) -> &'static str {
        match self {
            Self::Watermarks { .. } => "watermarks",
            Self::GroupLag { .. } => "groupLag",
            Self::Topology { .. } => "topology",
            Self::Configs { .. } => "configs",
            Self::Subjects { .. } => "subjects",
            Self::Resync { .. } => "resync",
        }
    }
}
