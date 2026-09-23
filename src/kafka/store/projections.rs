use std::collections::hash_map::Entry;
use std::sync::Arc;

use foldhash::{HashMap, HashMapExt};

use crate::kafka::group::{GroupMember, GroupOffset, GroupState};
use crate::kafka::topic_config::{CleanupPolicy, topic_config_values};
use crate::kafka::watermarks::Watermarks;

use super::lane::LaneHealth;
use super::tables::{
    ConfigTable, GroupInfo, GroupOffsets, OffsetTable, SubjectInfo, SubjectTable, TopicInfo,
    Topology, WatermarkTable,
};

#[derive(Debug, Clone, PartialEq)]
pub struct TopicRow {
    pub name: Arc<str>,
    pub internal: bool,
    pub partition_count: i32,
    pub replication_factor: i32,
    /// Messages currently in the log (`Σ high − low`).
    pub retained_messages: i64,
    /// Messages ever produced (`Σ high`). Not the display default: it
    /// overstates a retention-truncated topic.
    pub produced_total: i64,
    pub rate: f64,
    pub retention_ms: i64,
    pub cleanup_policy: CleanupPolicy,
    pub group_count: i32,
    pub under_replicated: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionRow {
    pub id: i32,
    pub leader: i32,
    pub replicas: Vec<i32>,
    pub isr: Vec<i32>,
    pub low_watermark: i64,
    pub high_watermark: i64,
}

impl PartitionRow {
    pub fn under_replicated(&self) -> bool {
        self.isr.len() < self.replicas.len()
    }

    pub fn retained(&self) -> i64 {
        (self.high_watermark - self.low_watermark).max(0)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct TopicDetail {
    pub name: Arc<str>,
    pub internal: bool,
    pub partitions: Vec<PartitionRow>,
    pub replication_factor: i32,
    pub retained_messages: i64,
    pub produced_total: i64,
    pub rate: f64,
    pub retention_ms: i64,
    pub cleanup_policy: CleanupPolicy,
    pub group_count: i32,
    pub under_replicated: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupRow {
    pub id: Arc<str>,
    pub state: GroupState,
    pub member_count: i32,
    pub topic_names: Vec<String>,
    pub total_lag: i64,
    /// False when a committed partition had no watermark to join against.
    pub lag_complete: bool,
    pub coordinator_id: i32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupDetail {
    pub id: Arc<str>,
    pub state: GroupState,
    pub protocol: String,
    pub coordinator_id: i32,
    pub members: Vec<GroupMember>,
    pub offsets: Vec<GroupOffset>,
    pub total_lag: i64,
    pub lag_complete: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicGroupRow {
    pub id: Arc<str>,
    pub state: GroupState,
    pub member_count: i32,
    pub lag_on_topic: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BrokerRow {
    pub id: i32,
    pub host: String,
    pub port: i32,
    pub rack: Option<String>,
    pub controller: bool,
    pub partition_count: i32,
    pub leader_count: i32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubjectRow {
    pub subject: Arc<str>,
    pub info: SubjectInfo,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterHealthView {
    pub cluster: String,
    pub topology: LaneHealth,
    pub watermarks: LaneHealth,
    pub offsets: LaneHealth,
    pub configs: LaneHealth,
    pub subjects: LaneHealth,
    pub topic_count: i32,
    pub partition_count: i32,
    pub group_count: i32,
    pub broker_count: i32,
    pub subject_count: i32,
    pub under_replicated_partitions: i32,
    pub offline_partitions: i32,
}

fn lag_of(committed: i64, end: Option<i64>) -> (i64, bool) {
    match end {
        Some(end) => ((end - committed).max(0), true),
        None => (0, false),
    }
}

pub fn topic_row(
    name: &Arc<str>,
    topic: &TopicInfo,
    watermarks: Option<&WatermarkTable>,
    configs: Option<&ConfigTable>,
    topology: &Topology,
    rate: f64,
) -> TopicRow {
    let (cleanup_policy, retention_ms) =
        topic_config_values(configs.and_then(|configs| configs.get(name)));

    TopicRow {
        name: Arc::clone(name),
        internal: topic.internal,
        partition_count: topic.partitions.len() as i32,
        replication_factor: topic.replication_factor(),
        retained_messages: watermarks.map(|marks| marks.retained(name)).unwrap_or(0),
        produced_total: watermarks.map(|marks| marks.produced(name)).unwrap_or(0),
        rate,
        retention_ms,
        cleanup_policy,
        group_count: topology.groups_for_topic(name).len() as i32,
        under_replicated: topic.under_replicated(),
    }
}

pub fn topic_detail(
    name: &Arc<str>,
    topic: &TopicInfo,
    watermarks: Option<&WatermarkTable>,
    configs: Option<&ConfigTable>,
    topology: &Topology,
    rate: f64,
) -> TopicDetail {
    let partitions: Vec<PartitionRow> = topic
        .partitions
        .iter()
        .map(|partition| {
            let marks = watermarks
                .and_then(|table| table.get(name, partition.id))
                .unwrap_or_default();
            PartitionRow {
                id: partition.id,
                leader: partition.leader,
                replicas: partition.replicas.clone(),
                isr: partition.isr.clone(),
                low_watermark: marks.low,
                high_watermark: marks.high,
            }
        })
        .collect();

    let (cleanup_policy, retention_ms) =
        topic_config_values(configs.and_then(|configs| configs.get(name)));

    TopicDetail {
        name: Arc::clone(name),
        internal: topic.internal,
        replication_factor: topic.replication_factor(),
        retained_messages: partitions.iter().map(PartitionRow::retained).sum(),
        produced_total: partitions
            .iter()
            .map(|partition| partition.high_watermark.max(0))
            .sum(),
        rate,
        retention_ms,
        cleanup_policy,
        group_count: topology.groups_for_topic(name).len() as i32,
        under_replicated: partitions.iter().any(PartitionRow::under_replicated),
        partitions,
    }
}

pub fn group_offsets(
    group: &GroupInfo,
    offsets: Option<&GroupOffsets>,
    watermarks: Option<&WatermarkTable>,
) -> (Vec<GroupOffset>, i64, bool) {
    let end = |topic: &str, partition: i32| {
        watermarks
            .and_then(|table| table.get(topic, partition))
            .map(|marks: Watermarks| marks.high)
    };

    let committed = offsets
        .map(|offsets| offsets.committed.as_slice())
        .unwrap_or_default();
    let mut seen: HashMap<(&str, i32), GroupOffset> = HashMap::with_capacity(committed.len());
    let mut complete = true;

    for committed in committed {
        let end = end(&committed.topic, committed.partition);
        let (lag, known) = lag_of(committed.offset, end);
        complete &= known;
        seen.insert(
            (committed.topic.as_str(), committed.partition),
            GroupOffset {
                topic: committed.topic.clone(),
                partition: committed.partition,
                current_offset: committed.offset,
                end_offset: end.unwrap_or(committed.offset),
                lag,
                member_id: group
                    .member_for(&committed.topic, committed.partition)
                    .map(ToOwned::to_owned),
            },
        );
    }

    for (topic, partition) in group.assigned_partition_refs() {
        let Entry::Vacant(slot) = seen.entry((topic, partition)) else {
            continue;
        };
        let end = end(topic, partition);
        complete &= end.is_some();
        slot.insert(GroupOffset {
            topic: topic.to_owned(),
            partition,
            current_offset: 0,
            end_offset: end.unwrap_or(0),
            lag: end.unwrap_or(0).max(0),
            member_id: group.member_for(topic, partition).map(ToOwned::to_owned),
        });
    }

    let mut offsets: Vec<GroupOffset> = seen.into_values().collect();
    offsets.sort_by(|left, right| {
        left.topic
            .cmp(&right.topic)
            .then(left.partition.cmp(&right.partition))
    });
    let total = offsets.iter().map(|offset| offset.lag).sum();
    (offsets, total, complete)
}

pub fn group_row(
    id: &Arc<str>,
    group: &GroupInfo,
    offsets: Option<&GroupOffsets>,
    watermarks: Option<&WatermarkTable>,
) -> GroupRow {
    let (offsets, total_lag, lag_complete) = group_offsets(group, offsets, watermarks);
    GroupRow {
        id: Arc::clone(id),
        state: group.state,
        member_count: group.members.len() as i32,
        topic_names: unique_topics(&offsets),
        total_lag,
        lag_complete,
        coordinator_id: group.coordinator,
    }
}

pub fn group_detail(
    id: &Arc<str>,
    group: &GroupInfo,
    offsets: Option<&GroupOffsets>,
    watermarks: Option<&WatermarkTable>,
) -> GroupDetail {
    let (offsets, total_lag, lag_complete) = group_offsets(group, offsets, watermarks);
    GroupDetail {
        id: Arc::clone(id),
        state: group.state,
        protocol: group.protocol.clone(),
        coordinator_id: group.coordinator,
        members: group.members.clone(),
        offsets,
        total_lag,
        lag_complete,
    }
}

pub fn topic_group_row(
    id: &Arc<str>,
    topic: &str,
    group: &GroupInfo,
    offsets: Option<&GroupOffsets>,
    watermarks: Option<&WatermarkTable>,
) -> TopicGroupRow {
    let (offsets, _, _) = group_offsets(group, offsets, watermarks);
    TopicGroupRow {
        id: Arc::clone(id),
        state: group.state,
        member_count: group.members.len() as i32,
        lag_on_topic: offsets
            .iter()
            .filter(|offset| offset.topic == topic)
            .map(|offset| offset.lag)
            .sum(),
    }
}

pub fn broker_rows(topology: &Topology) -> Vec<BrokerRow> {
    let mut partition_counts: HashMap<i32, i32> = HashMap::with_capacity(topology.brokers.len());
    let mut leader_counts: HashMap<i32, i32> = HashMap::with_capacity(topology.brokers.len());
    for partition in topology
        .topics
        .values()
        .flat_map(|topic| topic.partitions.iter())
    {
        for replica in &partition.replicas {
            *partition_counts.entry(*replica).or_default() += 1;
        }
        if !partition.offline() {
            *leader_counts.entry(partition.leader).or_default() += 1;
        }
    }

    topology
        .brokers
        .iter()
        .map(|(id, broker)| BrokerRow {
            id: *id,
            host: broker.host.clone(),
            port: broker.port,
            rack: broker.rack.clone(),
            controller: topology.controller == Some(*id),
            partition_count: partition_counts.get(id).copied().unwrap_or(0),
            leader_count: leader_counts.get(id).copied().unwrap_or(0),
        })
        .collect()
}

pub fn subject_rows(subjects: &SubjectTable) -> Vec<SubjectRow> {
    subjects
        .subjects
        .iter()
        .map(|(subject, info)| SubjectRow {
            subject: Arc::clone(subject),
            info: info.clone(),
        })
        .collect()
}

pub fn offsets_for<'a>(offsets: Option<&'a OffsetTable>, group: &str) -> Option<&'a GroupOffsets> {
    offsets?.get(group).map(Arc::as_ref)
}

fn unique_topics(offsets: &[GroupOffset]) -> Vec<String> {
    let mut topics: Vec<&str> = offsets.iter().map(|offset| offset.topic.as_str()).collect();
    topics.sort_unstable();
    topics.dedup();
    topics.into_iter().map(ToOwned::to_owned).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kafka::group::MemberAssignment;
    use crate::kafka::store::fixtures::{
        at, config, group as group_snapshot, offline_partition, offsets, partition, topic,
        topology as build_topology, watermarks,
    };

    fn topology() -> Topology {
        build_topology(
            vec![topic(
                "orders",
                vec![
                    partition(0, vec![1, 2], vec![1, 2]),
                    partition(1, vec![1, 2], vec![1]),
                ],
            )],
            vec![group_snapshot("billing", "orders", vec![0, 1])],
        )
    }

    fn marks() -> WatermarkTable {
        watermarks(at(1_000), &[("orders", 0, 20, 100), ("orders", 1, 0, 50)])
    }

    #[test]
    fn a_topic_row_derives_counts_without_storing_them() {
        let topology = topology();
        let (name, topic) = topology.topics.iter().next().unwrap();

        let row = topic_row(name, topic, Some(&marks()), None, &topology, 12.5);

        assert_eq!(row.partition_count, 2);
        assert_eq!(row.replication_factor, 2);
        assert_eq!(row.retained_messages, 130, "80 retained + 50 retained");
        assert_eq!(row.produced_total, 150, "high watermarks alone");
        assert_eq!(row.group_count, 1);
        assert_eq!(row.rate, 12.5);
        assert!(row.under_replicated, "partition 1 has a shrunken isr");
    }

    #[test]
    fn a_topic_with_no_watermarks_yet_reads_as_empty_rather_than_failing() {
        let topology = topology();
        let (name, topic) = topology.topics.iter().next().unwrap();

        let row = topic_row(name, topic, None, None, &topology, 0.0);
        assert_eq!(row.retained_messages, 0);
        assert_eq!(row.cleanup_policy, CleanupPolicy::Delete);

        let detail = topic_detail(name, topic, None, None, &topology, 0.0);
        assert_eq!(detail.partitions.len(), 2);
        assert_eq!(detail.partitions[0].high_watermark, 0);
    }

    #[test]
    fn topic_configs_fold_into_the_row() {
        let topology = topology();
        let (name, topic) = topology.topics.iter().next().unwrap();
        let configs = ConfigTable {
            topics: HashMap::from_iter([(
                Arc::from("orders"),
                Arc::new(vec![
                    config("cleanup.policy", "compact"),
                    config("retention.ms", "604800000"),
                ]),
            )]),
        };

        let row = topic_row(name, topic, None, Some(&configs), &topology, 0.0);
        assert_eq!(row.cleanup_policy, CleanupPolicy::Compact);
        assert_eq!(row.retention_ms, 604_800_000);

        let detail = topic_detail(name, topic, None, Some(&configs), &topology, 4.0);
        assert_eq!(detail.cleanup_policy, CleanupPolicy::Compact);
        assert_eq!(detail.retention_ms, 604_800_000);
        assert_eq!(detail.rate, 4.0);
    }

    #[test]
    fn topic_detail_carries_replicas_and_isr_per_partition() {
        let topology = topology();
        let (name, topic) = topology.topics.iter().next().unwrap();

        let detail = topic_detail(name, topic, Some(&marks()), None, &topology, 12.5);

        assert_eq!(detail.partitions[1].isr, vec![1]);
        assert_eq!(detail.partitions[1].replicas, vec![1, 2]);
        assert!(detail.partitions[1].under_replicated());
        assert_eq!(detail.retained_messages, 130);
        assert_eq!(detail.produced_total, 150);
    }

    #[test]
    fn lag_is_a_join_of_two_tables_never_a_broker_call() {
        let topology = topology();
        let (id, group) = topology.groups.iter().next().unwrap();

        let row = group_row(
            id,
            group,
            Some(&offsets(at(1_000), &[("orders", 0, 90), ("orders", 1, 20)])),
            Some(&marks()),
        );

        assert_eq!(row.total_lag, 40, "10 behind + 30 behind");
        assert!(row.lag_complete);
        assert_eq!(row.member_count, 1);
        assert_eq!(row.topic_names, vec!["orders"]);
    }

    #[test]
    fn a_committed_offset_past_the_high_watermark_does_not_go_negative() {
        let topology = topology();
        let (id, group) = topology.groups.iter().next().unwrap();

        let row = group_row(
            id,
            group,
            Some(&offsets(at(1_000), &[("orders", 0, 500)])),
            Some(&marks()),
        );

        assert_eq!(
            row.total_lag, 50,
            "partition 0 clamps to 0, partition 1 never committed"
        );
    }

    #[test]
    fn a_partition_with_no_watermark_flags_the_total_as_incomplete() {
        let topology = topology();
        let (id, group) = topology.groups.iter().next().unwrap();

        let row = group_row(
            id,
            group,
            Some(&offsets(at(1_000), &[("orders", 0, 90)])),
            Some(&watermarks(at(1_000), &[])),
        );

        assert_eq!(row.total_lag, 0);
        assert!(
            !row.lag_complete,
            "unknown partitions must not read as zero lag"
        );
    }

    #[test]
    fn an_assigned_partition_that_never_committed_lags_by_the_whole_log() {
        let topology = topology();
        let (id, group) = topology.groups.iter().next().unwrap();

        let detail = group_detail(id, group, None, Some(&marks()));

        assert_eq!(detail.total_lag, 150);
        assert_eq!(detail.offsets.len(), 2);
        assert_eq!(detail.offsets[0].current_offset, 0);
        assert_eq!(detail.offsets[0].member_id.as_deref(), Some("billing-m1"));
    }

    #[test]
    fn topic_group_rows_carry_only_the_lag_on_that_topic() {
        let mut snapshot = group_snapshot("billing", "orders", vec![0]);
        snapshot.members[0].assignments.push(MemberAssignment {
            topic: "payments".into(),
            partitions: vec![0],
        });
        let topology = build_topology(
            vec![topic("orders", vec![partition(0, vec![1], vec![1])])],
            vec![snapshot],
        );
        let (id, group) = topology.groups.iter().next().unwrap();
        let marks = watermarks(at(1_000), &[("orders", 0, 0, 10), ("payments", 0, 0, 900)]);

        let row = topic_group_row(id, "orders", group, None, Some(&marks));
        assert_eq!(row.lag_on_topic, 10);
        assert_eq!(row.member_count, 1);
    }

    #[test]
    fn broker_rows_count_replicas_and_leaders() {
        let topology = build_topology(
            vec![topic(
                "orders",
                vec![
                    partition(0, vec![1], vec![1]),
                    offline_partition(1, vec![1]),
                ],
            )],
            Vec::new(),
        );

        let rows = broker_rows(&topology);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].partition_count, 2);
        assert_eq!(
            rows[0].leader_count, 1,
            "an offline partition has no leader"
        );
        assert!(!rows[0].controller);
    }
}
