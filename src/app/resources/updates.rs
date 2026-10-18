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

use crate::app::context::Session;
use crate::app::error::ApiError;
use crate::app::extract::{Path, Query};

pub(crate) mod feed;
pub mod types;

#[cfg(test)]
mod tests;

use feed::{Frame, SharedChange};
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

/// What one change sends to one stream.
enum Delivery {
    /// The change in full, which every stream that sends it shares.
    Firehose,
    /// The one update the change makes inside the stream's scope.
    Scoped(Update),
    /// The change falls outside the stream's scope.
    Skip,
}

impl Scope {
    fn events(&self, shared: &SharedChange) -> Vec<Event> {
        match self.delivery(&shared.change) {
            Delivery::Firehose => shared.firehose().to_vec(),
            Delivery::Scoped(update) => vec![update.sse()],
            Delivery::Skip => Vec::new(),
        }
    }

    fn delivery(&self, change: &Change) -> Delivery {
        match change {
            Change::Watermarks(tick) => match self.topic.as_deref() {
                None => Delivery::Firehose,
                Some(topic) => match tick.rate(topic) {
                    None => Delivery::Skip,
                    Some(rate) => Delivery::Scoped(Update::Watermarks {
                        topics: vec![TopicRate {
                            topic: Arc::from(topic),
                            rate,
                        }],
                    }),
                },
            },

            Change::GroupOffsets(wave) => match self.group.as_deref() {
                None => Delivery::Firehose,
                Some(group) => match wave.group(group) {
                    None => Delivery::Skip,
                    Some(update) => Delivery::Scoped(Update::group_lag(update, true)),
                },
            },

            Change::Topology(delta) => {
                let relevant = match (self.topic.as_deref(), self.group.as_deref()) {
                    (None, None) => true,
                    (topic, group) => {
                        topic.is_some_and(|topic| delta.touches_topic(topic))
                            || group.is_some_and(|group| delta.touches_group(group))
                    }
                };
                if relevant {
                    Delivery::Firehose
                } else {
                    Delivery::Skip
                }
            }

            Change::Configs(delta) => match self.topic.as_deref() {
                None => Delivery::Firehose,
                Some(topic) if delta.topics.iter().any(|changed| changed.as_ref() == topic) => {
                    Delivery::Scoped(Update::Configs {
                        topics: vec![topic.to_owned()],
                    })
                }
                Some(_) => Delivery::Skip,
            },

            Change::LogDirs(delta) => match self.topic.as_deref() {
                None => Delivery::Firehose,
                Some(topic) if delta.touches_topic(topic) => Delivery::Scoped(Update::LogDirs {
                    topics: vec![topic.to_owned()],
                    brokers_changed: false,
                }),
                Some(_) => Delivery::Skip,
            },

            Change::Subjects(_) | Change::Acls | Change::Quotas | Change::ScramUsers => {
                Delivery::Firehose
            }
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
    let scope = Scope::from(query);
    let handle = session.cluster(&cluster)?;
    let frames = session.state.feed(handle.store.name())?.subscribe();
    let lease = scope
        .group
        .as_deref()
        .map(|group| handle.store.interest.lease_group(group));

    let stream = Stream {
        _lease: lease,
        frames,
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
    frames: Receiver<Frame>,
    _lease: Option<InterestLease>,
    guard: SessionGuard,
    pending: std::vec::IntoIter<Event>,
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
                if let Some(event) = state.pending.next() {
                    return Some((Ok(event), state));
                }

                let shared = match state.frames.recv().await {
                    Err(RecvError::Closed) => return None,
                    Err(RecvError::Lagged(missed)) => {
                        tracing::debug!(
                            cluster = %state.cluster,
                            missed,
                            "subscriber lagged the change bus"
                        );
                        return Some((Ok(Update::Resync.sse()), state));
                    }
                    Ok(Frame::Resync) => return Some((Ok(Update::Resync.sse()), state)),
                    Ok(Frame::Change(shared)) => shared,
                };

                if let Some(error) = state.denied() {
                    state.done = true;
                    return Some((Ok(error.event()), state));
                }
                state.pending = state.scope.events(&shared).into_iter();
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
