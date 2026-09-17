use std::collections::BTreeMap;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use tokio::sync::broadcast;

use crate::kafka::group::GroupOffset;

use super::tables::{ConfigTable, GroupInfo, SubjectTable, TopicInfo, Topology};

const BUS_CAPACITY: usize = 256;

/// A typed delta from one ingestion lane.
///
/// Granular enough for a client to apply to its cache instead of refetching
/// whole catalogs.
#[derive(Debug, Clone)]
pub enum Change {
    Topology(Arc<TopologyDelta>),
    Watermarks(Arc<WatermarksTick>),
    GroupOffsets(Arc<GroupOffsetsWave>),
    Configs(Arc<ConfigsDelta>),
    Subjects(Arc<SubjectsDelta>),
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TopologyDelta {
    pub version: u64,
    pub added_topics: Vec<Arc<str>>,
    pub removed_topics: Vec<Arc<str>>,
    pub changed_topics: Vec<Arc<str>>,
    pub added_groups: Vec<Arc<str>>,
    pub removed_groups: Vec<Arc<str>>,
    pub changed_groups: Vec<Arc<str>>,
    pub brokers_changed: bool,
}

impl TopologyDelta {
    pub fn between(previous: Option<&Topology>, next: &Topology) -> Option<Self> {
        let empty = Topology::default();
        let previous = previous.unwrap_or(&empty);
        let (added_topics, removed_topics, changed_topics) =
            diff_maps(&previous.topics, &next.topics, TopicInfo::eq);
        let (added_groups, removed_groups, changed_groups) =
            diff_maps(&previous.groups, &next.groups, GroupInfo::eq);
        let brokers_changed = previous.brokers != next.brokers
            || previous.cluster_id != next.cluster_id
            || previous.controller != next.controller;

        let delta = Self {
            version: 0,
            added_topics,
            removed_topics,
            changed_topics,
            added_groups,
            removed_groups,
            changed_groups,
            brokers_changed,
        };
        (!delta.is_empty()).then_some(delta)
    }

    pub fn is_empty(&self) -> bool {
        !self.brokers_changed
            && self.added_topics.is_empty()
            && self.removed_topics.is_empty()
            && self.changed_topics.is_empty()
            && self.added_groups.is_empty()
            && self.removed_groups.is_empty()
            && self.changed_groups.is_empty()
    }

    pub fn touches_topic(&self, topic: &str) -> bool {
        self.added_topics.iter().any(|name| &**name == topic)
            || self.removed_topics.iter().any(|name| &**name == topic)
            || self.changed_topics.iter().any(|name| &**name == topic)
    }

    pub fn touches_group(&self, group: &str) -> bool {
        self.added_groups.iter().any(|id| &**id == group)
            || self.removed_groups.iter().any(|id| &**id == group)
            || self.changed_groups.iter().any(|id| &**id == group)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct WatermarksTick {
    pub version: u64,
    pub at: DateTime<Utc>,
    /// Messages per second per topic, from the high-watermark delta.
    pub rates: Vec<TopicRate>,
    pub cluster_rate: f64,
}

impl WatermarksTick {
    pub fn rate(&self, topic: &str) -> Option<f64> {
        self.rates
            .iter()
            .find(|rate| &*rate.topic == topic)
            .map(|rate| rate.rate)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct TopicRate {
    pub topic: Arc<str>,
    pub rate: f64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupOffsetsWave {
    pub version: u64,
    pub at: DateTime<Utc>,
    pub groups: Vec<GroupLagUpdate>,
}

impl GroupOffsetsWave {
    pub fn group(&self, id: &str) -> Option<&GroupLagUpdate> {
        self.groups.iter().find(|update| &*update.group == id)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupLagUpdate {
    pub group: Arc<str>,
    pub total_lag: i64,
    /// False when a committed partition had no watermark to join against, so
    /// the total understates the real lag.
    pub lag_complete: bool,
    pub offsets: Vec<GroupOffset>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigsDelta {
    pub version: u64,
    pub topics: Vec<Arc<str>>,
}

impl ConfigsDelta {
    pub fn between(previous: Option<&ConfigTable>, next: &ConfigTable) -> Option<Self> {
        let mut topics: Vec<Arc<str>> = Vec::new();
        match previous {
            Some(previous) => {
                for (topic, entries) in &next.topics {
                    if previous.topics.get(topic) != Some(entries) {
                        topics.push(Arc::clone(topic));
                    }
                }
                for topic in previous.topics.keys() {
                    if !next.topics.contains_key(topic) {
                        topics.push(Arc::clone(topic));
                    }
                }
            }
            None => topics.extend(next.topics.keys().cloned()),
        }
        topics.sort();
        topics.dedup();
        (!topics.is_empty()).then_some(Self { version: 0, topics })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubjectsDelta {
    pub version: u64,
    pub added: Vec<Arc<str>>,
    pub removed: Vec<Arc<str>>,
    pub changed: Vec<Arc<str>>,
}

impl SubjectsDelta {
    pub fn between(previous: Option<&SubjectTable>, next: &SubjectTable) -> Option<Self> {
        let empty = SubjectTable::default();
        let previous = previous.unwrap_or(&empty);
        let (added, removed, changed) =
            diff_maps(&previous.subjects, &next.subjects, |left, right| {
                left == right
            });
        (!added.is_empty() || !removed.is_empty() || !changed.is_empty()).then_some(Self {
            version: 0,
            added,
            removed,
            changed,
        })
    }
}

/// Per-cluster broadcast of lane deltas. Cluster A's ticks never wake cluster
/// B's subscribers.
#[derive(Debug)]
pub struct ChangeBus {
    sender: broadcast::Sender<Change>,
}

impl Default for ChangeBus {
    fn default() -> Self {
        Self::with_capacity(BUS_CAPACITY)
    }
}

impl ChangeBus {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_capacity(capacity: usize) -> Self {
        let (sender, _) = broadcast::channel(capacity.max(1));
        Self { sender }
    }

    pub fn subscribe(&self) -> broadcast::Receiver<Change> {
        self.sender.subscribe()
    }

    pub fn publish(&self, change: Change) {
        let _ = self.sender.send(change);
    }

    pub fn subscriber_count(&self) -> usize {
        self.sender.receiver_count()
    }
}

type KeyDiff = (Vec<Arc<str>>, Vec<Arc<str>>, Vec<Arc<str>>);

fn diff_maps<V>(
    previous: &BTreeMap<Arc<str>, V>,
    next: &BTreeMap<Arc<str>, V>,
    same: impl Fn(&V, &V) -> bool,
) -> KeyDiff {
    let mut added = Vec::new();
    let mut removed = Vec::new();
    let mut changed = Vec::new();

    let mut left = previous.iter().peekable();
    let mut right = next.iter().peekable();
    loop {
        match (left.peek(), right.peek()) {
            (None, None) => break,
            (Some(_), None) => {
                removed.push(Arc::clone(left.next().expect("peeked").0));
            }
            (None, Some(_)) => {
                added.push(Arc::clone(right.next().expect("peeked").0));
            }
            (Some((old, _)), Some((new, _))) => match old.cmp(new) {
                std::cmp::Ordering::Less => {
                    removed.push(Arc::clone(left.next().expect("peeked").0));
                }
                std::cmp::Ordering::Greater => {
                    added.push(Arc::clone(right.next().expect("peeked").0));
                }
                std::cmp::Ordering::Equal => {
                    let (key, old) = left.next().expect("peeked");
                    let (_, new) = right.next().expect("peeked");
                    if !same(old, new) {
                        changed.push(Arc::clone(key));
                    }
                }
            },
        }
    }

    (added, removed, changed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    use crate::kafka::group::GroupState;
    use crate::kafka::store::fixtures::{config, group, partition, topic, topology};
    use crate::kafka::topic_config::ConfigEntry;

    fn configs<const N: usize>(entries: [(&str, &str); N]) -> ConfigTable {
        ConfigTable {
            topics: entries
                .into_iter()
                .map(|(name, policy)| {
                    (
                        Arc::from(name),
                        Arc::new(vec![config("cleanup.policy", policy)]),
                    )
                })
                .collect::<HashMap<Arc<str>, Arc<Vec<ConfigEntry>>>>(),
        }
    }

    #[test]
    fn an_unchanged_table_produces_no_delta() {
        let table = topology(
            vec![topic("orders", vec![partition(0, vec![1], vec![1])])],
            vec![group("billing", "orders", vec![0])],
        );
        assert_eq!(TopologyDelta::between(Some(&table), &table), None);
    }

    #[test]
    fn the_first_commit_reports_everything_as_added() {
        let table = topology(
            vec![topic("orders", vec![partition(0, vec![1], vec![1])])],
            vec![group("billing", "orders", vec![0])],
        );

        let delta = TopologyDelta::between(None, &table).expect("first commit is a change");
        assert_eq!(delta.added_topics, [Arc::from("orders")]);
        assert_eq!(delta.added_groups, [Arc::from("billing")]);
        assert!(delta.brokers_changed);
    }

    #[test]
    fn topology_diff_separates_adds_removes_and_edits() {
        let previous = topology(
            vec![
                topic("orders", vec![partition(0, vec![1], vec![1])]),
                topic("payments", vec![partition(0, vec![1], vec![1])]),
            ],
            vec![group("billing", "orders", vec![0])],
        );
        let next = topology(
            vec![
                topic(
                    "orders",
                    vec![
                        partition(0, vec![1], vec![1]),
                        partition(1, vec![1], vec![1]),
                    ],
                ),
                topic("shipments", vec![partition(0, vec![1], vec![1])]),
            ],
            vec![group("billing", "orders", vec![0, 1])],
        );

        let delta = TopologyDelta::between(Some(&previous), &next).expect("topology moved");
        assert_eq!(delta.added_topics, [Arc::from("shipments")]);
        assert_eq!(delta.removed_topics, [Arc::from("payments")]);
        assert_eq!(delta.changed_topics, [Arc::from("orders")]);
        assert_eq!(delta.changed_groups, [Arc::from("billing")]);
        assert!(delta.added_groups.is_empty());
        assert!(!delta.brokers_changed);
        assert!(delta.touches_topic("orders"));
        assert!(delta.touches_group("billing"));
        assert!(!delta.touches_topic("unrelated"));
    }

    #[test]
    fn a_group_state_change_is_a_group_change() {
        let topics = vec![topic("orders", vec![partition(0, vec![1], vec![1])])];
        let previous = topology(topics.clone(), vec![group("billing", "orders", vec![0])]);
        let mut rebalancing = group("billing", "orders", vec![0]);
        rebalancing.state = GroupState::PreparingRebalance;
        let next = topology(topics, vec![rebalancing]);

        let delta = TopologyDelta::between(Some(&previous), &next).expect("group state moved");
        assert_eq!(delta.changed_groups, [Arc::from("billing")]);
        assert!(delta.changed_topics.is_empty());
    }

    #[test]
    fn a_shrunken_isr_is_a_topic_change() {
        let previous = topology(
            vec![topic("orders", vec![partition(0, vec![1, 2], vec![1, 2])])],
            Vec::new(),
        );
        let next = topology(
            vec![topic("orders", vec![partition(0, vec![1, 2], vec![1])])],
            Vec::new(),
        );

        let delta = TopologyDelta::between(Some(&previous), &next).expect("isr moved");
        assert_eq!(delta.changed_topics, [Arc::from("orders")]);
    }

    #[test]
    fn config_diff_lists_only_the_topics_that_moved() {
        let previous = configs([("orders", "delete"), ("payments", "delete")]);
        let next = configs([("orders", "compact"), ("shipments", "delete")]);

        let delta = ConfigsDelta::between(Some(&previous), &next).expect("configs moved");
        assert_eq!(
            delta.topics,
            vec![
                Arc::from("orders"),
                Arc::from("payments"),
                Arc::from("shipments"),
            ]
        );
        assert_eq!(ConfigsDelta::between(Some(&next), &next), None);
    }

    #[test]
    fn subject_diff_reports_adds_removes_and_version_bumps() {
        let interner = &mut crate::kafka::store::tables::Interner::default();
        let previous = SubjectTable::assemble(
            &[
                crate::kafka::store::fixtures::subject("orders-value", 1, 1),
                crate::kafka::store::fixtures::subject("payments-value", 2, 1),
            ],
            interner,
        );
        let next = SubjectTable::assemble(
            &[
                crate::kafka::store::fixtures::subject("orders-value", 1, 2),
                crate::kafka::store::fixtures::subject("shipments-value", 3, 1),
            ],
            interner,
        );

        let delta = SubjectsDelta::between(Some(&previous), &next).expect("subjects moved");
        assert_eq!(delta.added, [Arc::from("shipments-value")]);
        assert_eq!(delta.removed, [Arc::from("payments-value")]);
        assert_eq!(delta.changed, [Arc::from("orders-value")]);
        assert_eq!(SubjectsDelta::between(Some(&next), &next), None);
    }

    #[tokio::test]
    async fn the_bus_fans_out_to_every_subscriber() {
        let bus = ChangeBus::new();
        let mut first = bus.subscribe();
        let mut second = bus.subscribe();
        assert_eq!(bus.subscriber_count(), 2);

        bus.publish(Change::Subjects(Arc::new(SubjectsDelta {
            version: 4,
            added: vec![Arc::from("orders-value")],
            removed: Vec::new(),
            changed: Vec::new(),
        })));

        for receiver in [&mut first, &mut second] {
            match receiver.recv().await.expect("event") {
                Change::Subjects(delta) => assert_eq!(delta.version, 4),
                other => panic!("unexpected change: {other:?}"),
            }
        }
    }

    #[tokio::test]
    async fn a_slow_subscriber_lags_instead_of_growing_the_buffer() {
        let bus = ChangeBus::with_capacity(1);
        let mut receiver = bus.subscribe();

        for version in 0..4 {
            bus.publish(Change::Subjects(Arc::new(SubjectsDelta {
                version,
                added: Vec::new(),
                removed: Vec::new(),
                changed: Vec::new(),
            })));
        }

        assert!(
            matches!(
                receiver.recv().await,
                Err(broadcast::error::RecvError::Lagged(_))
            ),
            "the API layer turns this into a resync, not an unbounded buffer"
        );
    }

    #[test]
    fn publishing_without_subscribers_is_not_an_error() {
        let bus = ChangeBus::new();
        bus.publish(Change::Subjects(Arc::new(SubjectsDelta {
            version: 1,
            added: Vec::new(),
            removed: Vec::new(),
            changed: Vec::new(),
        })));
        assert_eq!(bus.subscriber_count(), 0);
    }
}
