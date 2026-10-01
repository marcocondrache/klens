use serde::Serialize;
use ts_rs::TS;

use crate::kafka::store;

use super::super::groups::GroupOffset;

#[derive(Debug, Clone, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct TopicRate {
    pub topic: String,
    pub rate: f64,
}

impl From<&store::TopicRate> for TopicRate {
    fn from(rate: &store::TopicRate) -> Self {
        Self {
            topic: String::from(&*rate.topic),
            rate: rate.rate,
        }
    }
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
        /// One `{topic, rate}` pair per topic, never catalog objects. A scoped
        /// subscriber gets only its topic.
        topics: Vec<TopicRate>,
    },
    GroupLag {
        group: String,
        lag: i64,
        lag_complete: bool,
        offsets: Vec<GroupOffset>,
    },
    Topology {
        added_topics: Vec<String>,
        removed_topics: Vec<String>,
        changed_topics: Vec<String>,
        added_groups: Vec<String>,
        removed_groups: Vec<String>,
        changed_groups: Vec<String>,
        brokers_changed: bool,
    },
    Configs {
        topics: Vec<String>,
    },
    /// Sent when any subject is added, removed or changed. Added subjects are
    /// not listed: the client refetches the subject list either way.
    Subjects {
        removed: Vec<String>,
        changed: Vec<String>,
    },
    /// Sizes on disk moved. A scoped subscriber gets only its topic and never
    /// the broker flag.
    LogDirs {
        topics: Vec<String>,
        brokers_changed: bool,
    },
    Acls,
    Quotas,
    Transactions,
    /// The client fell behind the change bus and missed events.
    Resync,
}

impl Update {
    pub(crate) fn event(&self) -> &'static str {
        match self {
            Self::Watermarks { .. } => "watermarks",
            Self::GroupLag { .. } => "groupLag",
            Self::Topology { .. } => "topology",
            Self::Configs { .. } => "configs",
            Self::Subjects { .. } => "subjects",
            Self::LogDirs { .. } => "logDirs",
            Self::Acls => "acls",
            Self::Quotas => "quotas",
            Self::Transactions => "transactions",
            Self::Resync => "resync",
        }
    }
}
