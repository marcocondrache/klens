use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use crate::environment::TOPOLOGY_POLL_INTERVAL;
use crate::kafka::error::KafkaError;
use crate::kafka::session::ClusterSession;
use crate::kafka::store::{ClusterStore, Topology, TopologyDelta};

use super::runner::LaneSource;

pub struct TopologySource {
    session: Arc<dyn ClusterSession>,
    store: Arc<ClusterStore>,
}

impl TopologySource {
    pub fn new(session: Arc<dyn ClusterSession>, store: Arc<ClusterStore>) -> Self {
        Self { session, store }
    }
}

impl LaneSource for TopologySource {
    type Table = Topology;
    type Delta = TopologyDelta;

    async fn fetch(&self, _prev: Option<&Arc<Topology>>) -> Result<Topology, KafkaError> {
        let (meta, groups) = tokio::try_join!(self.session.metadata(), self.session.groups())?;
        Ok(Topology::from_snapshots(meta, groups))
    }

    fn diff(&self, prev: Option<&Topology>, next: &Topology) -> Option<TopologyDelta> {
        let mut delta = match prev {
            None => TopologyDelta {
                version: self.store.topology.version() + 1,
                added_topics: next.topics.keys().cloned().collect(),
                removed_topics: Vec::new(),
                changed_topics: Vec::new(),
                added_groups: next.groups.keys().cloned().collect(),
                removed_groups: Vec::new(),
                changed_groups: Vec::new(),
                brokers_changed: true,
            },
            Some(prev) => {
                let (added_topics, removed_topics, changed_topics) =
                    map_diff(&prev.topics, &next.topics);
                let (added_groups, removed_groups, changed_groups) =
                    map_diff(&prev.groups, &next.groups);
                TopologyDelta {
                    version: self.store.topology.version() + 1,
                    added_topics,
                    removed_topics,
                    changed_topics,
                    added_groups,
                    removed_groups,
                    changed_groups,
                    brokers_changed: prev.brokers != next.brokers
                        || prev.cluster_id != next.cluster_id
                        || prev.controller != next.controller,
                }
            }
        };
        if delta.is_empty() {
            return None;
        }
        delta.added_topics.sort();
        delta.removed_topics.sort();
        delta.changed_topics.sort();
        delta.added_groups.sort();
        delta.removed_groups.sort();
        delta.changed_groups.sort();
        Some(delta)
    }

    fn interval(&self) -> Duration {
        *TOPOLOGY_POLL_INTERVAL
    }

    fn after_commit(&self, table: &Arc<Topology>, _version: u64, delta: &TopologyDelta) {
        self.store.rebuild_search();
        self.store
            .series
            .prune_topics(|name| table.topics.contains_key(name));
        self.store
            .series
            .prune_groups(|name| table.groups.contains_key(name));
        if !delta.added_topics.is_empty() || !delta.removed_topics.is_empty() {
            self.store.watermarks.kick();
            self.store.configs.kick();
        }
        if !delta.added_groups.is_empty() || !delta.removed_groups.is_empty() {
            self.store.offsets.kick();
        }
        if delta.brokers_changed && delta.added_topics.is_empty() && delta.removed_topics.is_empty()
        {
            self.store.watermarks.kick();
        }
    }
}

type KeyDiff = (Vec<Arc<str>>, Vec<Arc<str>>, Vec<Arc<str>>);

#[cfg(test)]
pub(super) fn diff_maps<V: PartialEq>(
    prev: &BTreeMap<Arc<str>, V>,
    next: &BTreeMap<Arc<str>, V>,
) -> KeyDiff {
    map_diff(prev, next)
}

fn map_diff<V: PartialEq>(prev: &BTreeMap<Arc<str>, V>, next: &BTreeMap<Arc<str>, V>) -> KeyDiff {
    let mut added = Vec::new();
    let mut removed = Vec::new();
    let mut changed = Vec::new();
    let mut prev_iter = prev.iter().peekable();
    let mut next_iter = next.iter().peekable();
    loop {
        match (prev_iter.peek(), next_iter.peek()) {
            (None, None) => break,
            (None, Some(_)) => {
                let (key, _) = next_iter.next().expect("peeked");
                added.push(Arc::clone(key));
            }
            (Some(_), None) => {
                let (key, _) = prev_iter.next().expect("peeked");
                removed.push(Arc::clone(key));
            }
            (Some((prev_key, _)), Some((next_key, _))) => match prev_key.cmp(next_key) {
                std::cmp::Ordering::Less => {
                    let (key, _) = prev_iter.next().expect("peeked");
                    removed.push(Arc::clone(key));
                }
                std::cmp::Ordering::Greater => {
                    let (key, _) = next_iter.next().expect("peeked");
                    added.push(Arc::clone(key));
                }
                std::cmp::Ordering::Equal => {
                    let (key, prev_val) = prev_iter.next().expect("peeked");
                    let (_, next_val) = next_iter.next().expect("peeked");
                    if prev_val != next_val {
                        changed.push(Arc::clone(key));
                    }
                }
            },
        }
    }
    (added, removed, changed)
}
