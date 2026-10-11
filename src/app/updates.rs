use std::convert::Infallible;
use std::sync::Arc;

use axum::Router;
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::routing::get;
use futures::stream::{BoxStream, StreamExt as _};
use serde::Deserialize;
use tokio::sync::broadcast::Receiver;
use tokio::sync::broadcast::error::RecvError;

use crate::AppState;
use crate::app::SSE_KEEP_ALIVE;
use crate::app::auth::SessionGuard;
use crate::kafka::store::{Change, InterestLease};

use super::context::Session;
use super::error::ApiError;
use super::extract::{Path, Query};

pub mod types;

#[cfg(test)]
mod tests;

pub(crate) use types::{TopicRate, Update};

pub(crate) fn router() -> Router<AppState> {
    Router::new().route("/", get(updates))
}

#[derive(Debug, Default, Deserialize)]
pub(crate) struct UpdateQuery {
    topic: Option<String>,
    group: Option<String>,
}

#[derive(Debug, Clone, Default)]
struct Scope {
    topic: Option<String>,
    group: Option<String>,
}

impl From<UpdateQuery> for Scope {
    fn from(query: UpdateQuery) -> Self {
        let named = |value: Option<String>| {
            value
                .map(|value| value.trim().to_owned())
                .filter(|value| !value.is_empty())
        };
        Self {
            topic: named(query.topic),
            group: named(query.group),
        }
    }
}

impl Scope {
    fn updates(&self, change: &Change) -> Vec<Update> {
        match change {
            Change::Watermarks(tick) => {
                let topics = match self.topic.as_deref() {
                    None => tick.rates.iter().map(TopicRate::from).collect(),
                    Some(topic) => match tick.rate(topic) {
                        None => return Vec::new(),
                        Some(rate) => vec![TopicRate {
                            topic: topic.to_owned(),
                            rate,
                        }],
                    },
                };
                vec![Update::Watermarks { topics }]
            }

            Change::GroupOffsets(wave) => match self.group.as_deref() {
                Some(group) => wave
                    .group(group)
                    .map(|update| Update::group_lag(update, true))
                    .into_iter()
                    .collect(),
                None => wave
                    .groups
                    .iter()
                    .map(|update| Update::group_lag(update, false))
                    .collect(),
            },

            Change::Topology(delta) => {
                let relevant = match (self.topic.as_deref(), self.group.as_deref()) {
                    (None, None) => true,
                    (topic, group) => {
                        topic.is_some_and(|topic| delta.touches_topic(topic))
                            || group.is_some_and(|group| delta.touches_group(group))
                    }
                };
                if !relevant {
                    return Vec::new();
                }
                vec![Update::Topology {
                    added_topics: names(&delta.added_topics),
                    removed_topics: names(&delta.removed_topics),
                    changed_topics: names(&delta.changed_topics),
                    added_groups: names(&delta.added_groups),
                    removed_groups: names(&delta.removed_groups),
                    changed_groups: names(&delta.changed_groups),
                    brokers_changed: delta.brokers_changed,
                }]
            }

            Change::Configs(delta) => {
                let topics = match self.topic.as_deref() {
                    None => names(&delta.topics),
                    Some(topic) => {
                        if !delta.topics.iter().any(|changed| changed.as_ref() == topic) {
                            return Vec::new();
                        }
                        vec![topic.to_owned()]
                    }
                };
                vec![Update::Configs { topics }]
            }

            Change::Subjects(delta) => vec![Update::Subjects {
                removed: names(&delta.removed),
                changed: names(&delta.changed),
            }],

            Change::LogDirs(delta) => match self.topic.as_deref() {
                None => vec![Update::LogDirs {
                    topics: names(&delta.topics),
                    brokers_changed: delta.brokers_changed,
                }],
                Some(topic) if delta.touches_topic(topic) => vec![Update::LogDirs {
                    topics: vec![topic.to_owned()],
                    brokers_changed: false,
                }],
                Some(_) => Vec::new(),
            },

            Change::Acls => vec![Update::Acls],
            Change::Quotas => vec![Update::Quotas],
            Change::ScramUsers => vec![Update::ScramUsers],
        }
    }
}

pub(crate) async fn updates(
    session: Session,
    Path(cluster): Path<String>,
    Query(query): Query<UpdateQuery>,
) -> Result<
    Sse<axum::response::sse::KeepAliveStream<BoxStream<'static, Result<Event, Infallible>>>>,
    ApiError,
> {
    let store = Arc::clone(session.cluster(&cluster)?.store);
    let scope = Scope::from(query);
    let stream = Stream {
        _lease: scope
            .group
            .as_deref()
            .map(|group| store.interest.lease_group(group)),
        events: store.bus.subscribe(),
        guard: session.guard,
        pending: Vec::new().into_iter(),
        cluster,
        scope,
        done: false,
    }
    .into_stream();

    Ok(Sse::new(stream).keep_alive(KeepAlive::new().interval(SSE_KEEP_ALIVE).text("keep-alive")))
}

struct Stream {
    events: Receiver<Change>,
    _lease: Option<InterestLease>,
    guard: SessionGuard,
    pending: std::vec::IntoIter<Update>,
    cluster: String,
    scope: Scope,
    done: bool,
}

impl Stream {
    fn into_stream(self) -> BoxStream<'static, Result<Event, Infallible>> {
        futures::stream::unfold(self, |mut state| async move {
            loop {
                if state.done {
                    return None;
                }
                if let Some(update) = state.pending.next() {
                    return Some((Ok(update.sse()), state));
                }

                match state.events.recv().await {
                    Err(RecvError::Closed) => return None,
                    Err(RecvError::Lagged(missed)) => {
                        tracing::debug!(
                            cluster = %state.cluster,
                            missed,
                            "subscriber lagged the change bus"
                        );
                        return Some((Ok(Update::Resync.sse()), state));
                    }
                    Ok(change) => {
                        if let Some(error) = state.denied() {
                            state.done = true;
                            return Some((Ok(error.event()), state));
                        }
                        state.pending = state.scope.updates(&change).into_iter();
                    }
                }
            }
        })
        .boxed()
    }

    fn denied(&self) -> Option<ApiError> {
        let Some(access) = self.guard.revalidate() else {
            return Some(ApiError::SessionExpired);
        };
        access.cluster(&self.cluster).err().map(ApiError::from)
    }
}

fn names(values: &[Arc<str>]) -> Vec<String> {
    values.iter().map(|value| String::from(&**value)).collect()
}
