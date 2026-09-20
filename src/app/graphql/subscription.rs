use std::collections::VecDeque;
use std::sync::Arc;

use futures::stream::{BoxStream, StreamExt as _};
use juniper::{FieldError, IntoFieldError as _, graphql_subscription};
use tokio::sync::broadcast::Receiver;
use tokio::sync::broadcast::error::RecvError;

use crate::app::auth::SessionGuard;
use crate::kafka::store::{Change, GroupOffsetsWave, InterestLease};

use super::context::GraphQlContext;
use super::error::GqlError;
use super::types::{
    ConfigsChanged, GroupLagUpdate, Resync, ResyncReason, SubjectsChanged, TopicRate,
    TopologyDelta, Update, UpdateScope, WatermarksTick, names,
};

pub struct Subscription;

#[graphql_subscription(context = GraphQlContext)]
impl Subscription {
    /// Every lane delta for one cluster, optionally narrowed to one topic or
    /// group.
    ///
    /// A scoped subscriber pays only for its own page: the all-topics
    /// watermark firehose exists for list pages, and even that carries one
    /// `{topic, rate}` pair per topic rather than catalog objects.
    async fn updates(
        context: &GraphQlContext,
        cluster: String,
        scope: Option<UpdateScope>,
    ) -> Result<BoxStream<'static, Result<Update, FieldError>>, GqlError> {
        let store = Arc::clone(context.cluster(&cluster)?.store);
        let scope = scope.unwrap_or_default();

        Ok(Stream {
            _lease: scope
                .group
                .as_deref()
                .map(|group| store.interest.lease_group(group)),
            events: store.bus.subscribe(),
            guard: context.guard.clone(),
            pending: VecDeque::new(),
            cluster,
            scope,
            done: false,
        }
        .into_stream())
    }
}

struct Stream {
    events: Receiver<Change>,
    _lease: Option<InterestLease>,
    guard: SessionGuard,
    pending: VecDeque<Update>,
    cluster: String,
    scope: UpdateScope,
    done: bool,
}

impl Stream {
    fn into_stream(self) -> BoxStream<'static, Result<Update, FieldError>> {
        futures::stream::unfold(self, |mut state| async move {
            loop {
                if state.done {
                    return None;
                }
                if let Some(update) = state.pending.pop_front() {
                    return Some((Ok(update), state));
                }

                match state.events.recv().await {
                    Err(RecvError::Closed) => return None,
                    Err(RecvError::Lagged(missed)) => {
                        tracing::debug!(
                            cluster = %state.cluster,
                            missed,
                            "subscriber lagged the change bus"
                        );
                        return Some((
                            Ok(Update::Resync(Resync {
                                reason: ResyncReason::Lagged,
                            })),
                            state,
                        ));
                    }
                    Ok(change) => {
                        if let Some(error) = state.denied() {
                            state.done = true;
                            return Some((Err(error.into_field_error()), state));
                        }
                        state.pending.extend(project(&change, &state.scope));
                    }
                }
            }
        })
        .boxed()
    }

    fn denied(&self) -> Option<GqlError> {
        let Some(access) = self.guard.revalidate() else {
            return Some(GqlError::SessionExpired);
        };
        access.cluster(&self.cluster).err().map(GqlError::from)
    }
}

fn project(change: &Change, scope: &UpdateScope) -> Vec<Update> {
    match change {
        Change::Watermarks(tick) => {
            let topics = match scope.topic.as_deref() {
                None => tick.rates.iter().map(TopicRate::from).collect(),
                Some(topic) => match tick.rate(topic) {
                    None => return Vec::new(),
                    Some(rate) => vec![TopicRate {
                        topic: topic.to_owned(),
                        rate,
                    }],
                },
            };
            vec![Update::Watermarks(WatermarksTick {
                at: tick.at,
                topics,
            })]
        }

        Change::GroupOffsets(wave) => match scope.group.as_deref() {
            Some(group) => wave
                .group(group)
                .map(|update| Update::GroupLag(lag_update(wave, update, true)))
                .into_iter()
                .collect(),
            None => wave
                .groups
                .iter()
                .map(|update| Update::GroupLag(lag_update(wave, update, false)))
                .collect(),
        },

        Change::Topology(delta) => {
            let relevant = match (scope.topic.as_deref(), scope.group.as_deref()) {
                (None, None) => true,
                (topic, group) => {
                    topic.is_some_and(|topic| delta.touches_topic(topic))
                        || group.is_some_and(|group| delta.touches_group(group))
                }
            };
            if !relevant {
                return Vec::new();
            }
            vec![Update::Topology(TopologyDelta {
                version: delta.version.into(),
                added_topics: names(&delta.added_topics),
                removed_topics: names(&delta.removed_topics),
                changed_topics: names(&delta.changed_topics),
                added_groups: names(&delta.added_groups),
                removed_groups: names(&delta.removed_groups),
                changed_groups: names(&delta.changed_groups),
                brokers_changed: delta.brokers_changed,
            })]
        }

        Change::Configs(delta) => {
            let topics = match scope.topic.as_deref() {
                None => names(&delta.topics),
                Some(topic) => {
                    if !delta.topics.iter().any(|changed| changed.as_ref() == topic) {
                        return Vec::new();
                    }
                    vec![topic.to_owned()]
                }
            };
            vec![Update::Configs(ConfigsChanged {
                version: delta.version.into(),
                topics,
            })]
        }

        Change::Subjects(delta) => vec![Update::Subjects(SubjectsChanged {
            version: delta.version.into(),
            added: names(&delta.added),
            removed: names(&delta.removed),
            changed: names(&delta.changed),
        })],
    }
}

fn lag_update(
    wave: &GroupOffsetsWave,
    update: &crate::kafka::store::GroupLagUpdate,
    offsets: bool,
) -> GroupLagUpdate {
    GroupLagUpdate {
        at: wave.at,
        group: update.group.to_string(),
        lag: update.total_lag.into(),
        lag_complete: update.lag_complete,
        offsets: match offsets {
            true => update.offsets.iter().cloned().map(Into::into).collect(),
            false => Vec::new(),
        },
    }
}
