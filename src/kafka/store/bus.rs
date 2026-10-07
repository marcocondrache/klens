use std::collections::BTreeMap;
use std::sync::Arc;

use itertools::{EitherOrBoth, Itertools};
use tokio::sync::broadcast;

use crate::kafka::group::GroupOffset;

use super::tables::{ConfigTable, LogDirTable, SchemaIdTable, SubjectTable, Topology};

pub const BUS_CAPACITY: usize = 256;

#[derive(Debug, Clone)]
pub enum Change {
    Topology(Arc<TopologyDelta>),
    Watermarks(Arc<WatermarksTick>),
    GroupOffsets(Arc<GroupOffsetsWave>),
    Configs(Arc<ConfigsDelta>),
    Subjects(Arc<SubjectsDelta>),
    LogDirs(Arc<LogDirsDelta>),
    Acls,
    Quotas,
    ScramUsers,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TopologyDelta {
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
            diff_maps(&previous.topics, &next.topics);
        let (added_groups, removed_groups, changed_groups) =
            diff_maps(&previous.groups, &next.groups);
        let brokers_changed = previous.brokers != next.brokers
            || previous.cluster_id != next.cluster_id
            || previous.controller != next.controller;

        let delta = Self {
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
    pub rates: Vec<TopicRate>,
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
    pub groups: Vec<GroupLagUpdate>,
}

impl GroupOffsetsWave {
    pub fn group(&self, id: &str) -> Option<&GroupLagUpdate> {
        self.groups.iter().find(|update| &*update.group == id)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct GroupLagUpdate {
    pub group: Arc<str>,
    pub total_lag: i64,
    pub lag_complete: bool,
    pub offsets: Vec<GroupOffset>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigsDelta {
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
        (!topics.is_empty()).then_some(Self { topics })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogDirsDelta {
    /// Topics whose size moved, appeared, or disappeared.
    pub topics: Vec<Arc<str>>,
    pub brokers_changed: bool,
}

impl LogDirsDelta {
    pub fn between(previous: Option<&LogDirTable>, next: &LogDirTable) -> Option<Self> {
        let empty = LogDirTable::default();
        let previous = previous.unwrap_or(&empty);

        let mut topics: Vec<Arc<str>> = next
            .partitions
            .iter()
            .filter(|(topic, partitions)| previous.partitions.get(*topic) != Some(partitions))
            .map(|(topic, _)| Arc::clone(topic))
            .chain(
                previous
                    .partitions
                    .keys()
                    .filter(|topic| !next.partitions.contains_key(*topic))
                    .cloned(),
            )
            .collect();
        topics.sort();
        let brokers_changed = previous.brokers != next.brokers;

        (brokers_changed || !topics.is_empty()).then_some(Self {
            topics,
            brokers_changed,
        })
    }

    pub fn touches_topic(&self, topic: &str) -> bool {
        self.topics.iter().any(|name| &**name == topic)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubjectsDelta {
    pub added: Vec<Arc<str>>,
    pub removed: Vec<Arc<str>>,
    pub changed: Vec<Arc<str>>,
}

impl SubjectsDelta {
    pub fn between(previous: Option<&SubjectTable>, next: &SubjectTable) -> Option<Self> {
        Self::of(previous.map(|table| &table.subjects), &next.subjects)
    }

    pub fn of_ids(previous: Option<&SchemaIdTable>, next: &SchemaIdTable) -> Option<Self> {
        Self::of(previous.map(|table| &table.subjects), &next.subjects)
    }

    fn of<V: PartialEq>(
        previous: Option<&BTreeMap<Arc<str>, V>>,
        next: &BTreeMap<Arc<str>, V>,
    ) -> Option<Self> {
        let empty = BTreeMap::new();
        let (added, removed, changed) = diff_maps(previous.unwrap_or(&empty), next);
        (!added.is_empty() || !removed.is_empty() || !changed.is_empty()).then_some(Self {
            added,
            removed,
            changed,
        })
    }
}

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
}

type KeyDiff = (Vec<Arc<str>>, Vec<Arc<str>>, Vec<Arc<str>>);

fn diff_maps<V: PartialEq>(
    previous: &BTreeMap<Arc<str>, V>,
    next: &BTreeMap<Arc<str>, V>,
) -> KeyDiff {
    let mut added = Vec::new();
    let mut removed = Vec::new();
    let mut changed = Vec::new();

    for pair in previous
        .iter()
        .merge_join_by(next, |(old, _), (new, _)| old.cmp(new))
    {
        match pair {
            EitherOrBoth::Left((key, _)) => removed.push(Arc::clone(key)),
            EitherOrBoth::Right((key, _)) => added.push(Arc::clone(key)),
            EitherOrBoth::Both((key, old), (_, new)) if old != new => changed.push(Arc::clone(key)),
            EitherOrBoth::Both(..) => {}
        }
    }

    (added, removed, changed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use foldhash::HashMap;

    use crate::kafka::group::GroupState;
    use crate::kafka::topic_config::ConfigEntry;
    use crate::testing::{config_entry, group, log_dir, partition, subject, topic, topology};

    fn configs<const N: usize>(entries: [(&str, &str); N]) -> ConfigTable {
        ConfigTable {
            topics: entries
                .into_iter()
                .map(|(name, policy)| {
                    (
                        Arc::from(name),
                        Arc::from([config_entry("cleanup.policy", policy)]),
                    )
                })
                .collect::<HashMap<Arc<str>, Arc<[ConfigEntry]>>>(),
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

    fn log_dirs(dirs: Vec<crate::kafka::storage::LogDir>) -> LogDirTable {
        LogDirTable::assemble(dirs, &mut crate::kafka::store::tables::Interner::default())
    }

    #[test]
    fn log_dir_diff_lists_topics_whose_size_moved() {
        let previous = log_dirs(vec![log_dir(
            1,
            "/data",
            &[("orders", 0, 10), ("payments", 0, 5), ("audit", 0, 1)],
        )]);
        let next = log_dirs(vec![log_dir(
            1,
            "/data",
            &[("orders", 0, 12), ("shipments", 0, 5), ("audit", 0, 1)],
        )]);

        let delta = LogDirsDelta::between(Some(&previous), &next).expect("sizes moved");
        assert_eq!(
            delta.topics,
            [
                Arc::from("orders"),
                Arc::from("payments"),
                Arc::from("shipments")
            ]
        );
        assert!(delta.brokers_changed, "the directory grew");
        assert!(delta.touches_topic("payments"));
        assert!(!delta.touches_topic("audit"));
        assert_eq!(LogDirsDelta::between(Some(&next), &next), None);
    }

    #[test]
    fn a_directory_change_alone_is_a_log_dir_delta() {
        let previous = log_dirs(vec![log_dir(1, "/data", &[("orders", 0, 10)])]);
        let mut failed = log_dir(1, "/data", &[("orders", 0, 10)]);
        failed.error = Some("KafkaStorageError".into());

        let delta = LogDirsDelta::between(Some(&previous), &log_dirs(vec![failed]))
            .expect("the directory went offline");
        assert!(delta.topics.is_empty());
        assert!(delta.brokers_changed);
    }

    #[test]
    fn the_first_log_dir_commit_reports_every_topic() {
        let delta = LogDirsDelta::between(
            None,
            &log_dirs(vec![log_dir(1, "/data", &[("orders", 0, 0)])]),
        )
        .expect("first commit");
        assert_eq!(delta.topics, [Arc::from("orders")]);
        assert!(delta.brokers_changed);
    }

    #[test]
    fn subject_diff_reports_adds_removes_and_version_bumps() {
        let interner = &mut crate::kafka::store::tables::Interner::default();
        let previous = SubjectTable::assemble(
            &[
                subject("orders-value", 1, 1),
                subject("payments-value", 2, 1),
            ],
            interner,
        );
        let next = SubjectTable::assemble(
            &[
                subject("orders-value", 1, 2),
                subject("shipments-value", 3, 1),
            ],
            interner,
        );

        let delta = SubjectsDelta::between(Some(&previous), &next).expect("subjects moved");
        assert_eq!(delta.added, [Arc::from("shipments-value")]);
        assert_eq!(delta.removed, [Arc::from("payments-value")]);
        assert_eq!(delta.changed, [Arc::from("orders-value")]);
        assert_eq!(SubjectsDelta::between(Some(&next), &next), None);
    }

    #[test]
    fn a_lone_removal_or_version_bump_is_a_subject_delta() {
        let interner = &mut crate::kafka::store::tables::Interner::default();
        let previous = SubjectTable::assemble(
            &[
                subject("orders-value", 1, 1),
                subject("payments-value", 2, 1),
            ],
            interner,
        );
        let removed = SubjectTable::assemble(&[subject("orders-value", 1, 1)], interner);
        let bumped = SubjectTable::assemble(
            &[
                subject("orders-value", 1, 2),
                subject("payments-value", 2, 1),
            ],
            interner,
        );

        let delta = SubjectsDelta::between(Some(&previous), &removed).expect("a subject went");
        assert_eq!(delta.removed, [Arc::from("payments-value")]);
        let delta = SubjectsDelta::between(Some(&previous), &bumped).expect("a version landed");
        assert_eq!(delta.changed, [Arc::from("orders-value")]);
    }
}
