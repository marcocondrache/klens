use std::sync::Arc;

use axum::response::sse::Event;
use serde::Serialize;
use ts_rs::TS;

use crate::kafka::store::{self, Change};

use super::super::groups::GroupOffset;

#[derive(Debug, Clone, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct TopicRate {
    pub topic: Arc<str>,
    pub rate: f64,
}

impl From<&store::TopicRate> for TopicRate {
    fn from(rate: &store::TopicRate) -> Self {
        Self {
            topic: Arc::clone(&rate.topic),
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
    ScramUsers,
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
            Self::ScramUsers => "scramUsers",
            Self::Resync => "resync",
        }
    }

    pub(crate) fn sse(&self) -> Event {
        Event::default()
            .event(self.event())
            .json_data(self)
            .expect("update is serializable")
    }

    /// Every update a change makes, as a stream with no scope sends it.
    pub(crate) fn firehose(change: &Change) -> Vec<Self> {
        match change {
            Change::Watermarks(tick) => vec![Self::Watermarks {
                topics: tick.rates.iter().map(TopicRate::from).collect(),
            }],
            Change::GroupOffsets(wave) => wave
                .groups
                .iter()
                .map(|update| Self::group_lag(update, false))
                .collect(),
            Change::Topology(delta) => vec![Self::Topology {
                added_topics: names(&delta.added_topics),
                removed_topics: names(&delta.removed_topics),
                changed_topics: names(&delta.changed_topics),
                added_groups: names(&delta.added_groups),
                removed_groups: names(&delta.removed_groups),
                changed_groups: names(&delta.changed_groups),
                brokers_changed: delta.brokers_changed,
            }],
            Change::Configs(delta) => vec![Self::Configs {
                topics: names(&delta.topics),
            }],
            Change::Subjects(delta) => vec![Self::Subjects {
                removed: names(&delta.removed),
                changed: names(&delta.changed),
            }],
            Change::LogDirs(delta) => vec![Self::LogDirs {
                topics: names(&delta.topics),
                brokers_changed: delta.brokers_changed,
            }],
            Change::Acls => vec![Self::Acls],
            Change::Quotas => vec![Self::Quotas],
            Change::ScramUsers => vec![Self::ScramUsers],
        }
    }

    /// Only a subscriber scoped to the group gets its offsets.
    pub(crate) fn group_lag(update: &store::GroupLagUpdate, with_offsets: bool) -> Self {
        Self::GroupLag {
            group: String::from(&*update.group),
            lag: update.total_lag,
            lag_complete: update.lag_complete,
            offsets: if with_offsets {
                update
                    .offsets
                    .iter()
                    .cloned()
                    .map(GroupOffset::from)
                    .collect()
            } else {
                Vec::new()
            },
        }
    }
}

fn names(values: &[Arc<str>]) -> Vec<String> {
    values.iter().map(|value| String::from(&**value)).collect()
}
