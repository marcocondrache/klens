use std::collections::BTreeMap;
use std::sync::Arc;

use foldhash::{HashMap, HashMapExt, HashSet};

use crate::kafka::group::{CommittedOffset, GroupMember, GroupSnapshot, GroupState};
use crate::kafka::metadata::{MetadataSnapshot, PartitionMetadata, Watermarks};
use crate::kafka::registry::{SchemaCompatibility, SchemaSubject, SchemaType};
use crate::kafka::storage::LogDir;
use crate::kafka::topic_config::ConfigEntry;

#[derive(Debug, Default)]
pub struct Interner {
    names: HashSet<Arc<str>>,
}

impl Interner {
    pub fn seeded<'a>(names: impl IntoIterator<Item = &'a Arc<str>>) -> Self {
        Self {
            names: names.into_iter().cloned().collect(),
        }
    }

    pub fn intern(&mut self, name: &str) -> Arc<str> {
        if let Some(existing) = self.names.get(name) {
            return Arc::clone(existing);
        }
        let interned: Arc<str> = Arc::from(name);
        self.names.insert(Arc::clone(&interned));
        interned
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BrokerInfo {
    pub host: String,
    pub port: i32,
    pub rack: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicInfo {
    pub internal: bool,
    pub partitions: Vec<PartitionMetadata>,
}

impl TopicInfo {
    pub fn partition_ids(&self) -> Vec<i32> {
        self.partitions
            .iter()
            .map(|partition| partition.id)
            .collect()
    }

    pub fn replication_factor(&self) -> i32 {
        self.partitions
            .first()
            .map(|partition| partition.replicas.len() as i32)
            .unwrap_or(0)
    }

    pub fn under_replicated(&self) -> bool {
        self.partitions
            .iter()
            .any(PartitionMetadata::under_replicated)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupInfo {
    pub state: GroupState,
    pub protocol: String,
    pub members: Vec<GroupMember>,
}

impl GroupInfo {
    pub fn assigned_partition_refs(&self) -> impl Iterator<Item = (&str, i32)> {
        self.members.iter().flat_map(|member| {
            member.assignments.iter().flat_map(|assignment| {
                assignment
                    .partitions
                    .iter()
                    .copied()
                    .map(|partition| (assignment.topic.as_str(), partition))
            })
        })
    }

    pub fn consumes(&self, topic: &str) -> bool {
        self.assigned_partition_refs()
            .any(|(assigned, _)| assigned == topic)
    }

    pub fn owners(&self) -> HashMap<(&str, i32), &str> {
        let mut owners = HashMap::new();
        for member in &self.members {
            for assignment in &member.assignments {
                for &partition in &assignment.partitions {
                    owners
                        .entry((assignment.topic.as_str(), partition))
                        .or_insert(member.id.as_str());
                }
            }
        }
        owners
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Topology {
    pub cluster_id: Option<String>,
    pub controller: Option<i32>,
    pub brokers: BTreeMap<i32, BrokerInfo>,
    pub topics: BTreeMap<Arc<str>, TopicInfo>,
    pub groups: BTreeMap<Arc<str>, GroupInfo>,
    pub topic_groups: HashMap<Arc<str>, Vec<Arc<str>>>,
}

impl Topology {
    pub fn assemble(
        meta: MetadataSnapshot,
        groups: Vec<GroupSnapshot>,
        interner: &mut Interner,
    ) -> Self {
        let brokers = meta
            .brokers
            .into_iter()
            .map(|broker| {
                (
                    broker.id,
                    BrokerInfo {
                        host: broker.host,
                        port: broker.port,
                        rack: None,
                    },
                )
            })
            .collect();

        let topics: BTreeMap<Arc<str>, TopicInfo> = meta
            .topics
            .into_iter()
            .map(|topic| {
                (
                    interner.intern(&topic.name),
                    TopicInfo {
                        internal: topic.internal,
                        partitions: topic.partitions,
                    },
                )
            })
            .collect();

        let mut topic_groups: HashMap<Arc<str>, Vec<Arc<str>>> = HashMap::new();
        let mut assembled = BTreeMap::new();
        for group in groups {
            let id = interner.intern(&group.id);
            let mut consumed: Vec<&str> = group.consumed_topics().collect();
            consumed.sort_unstable();
            consumed.dedup();
            for topic in consumed {
                topic_groups
                    .entry(interner.intern(topic))
                    .or_default()
                    .push(Arc::clone(&id));
            }
            assembled.insert(
                id,
                GroupInfo {
                    state: group.state,
                    protocol: group.protocol,
                    members: group.members,
                },
            );
        }
        for ids in topic_groups.values_mut() {
            ids.sort();
        }

        Self {
            cluster_id: meta.cluster_id,
            controller: None,
            brokers,
            topics,
            groups: assembled,
            topic_groups,
        }
    }

    pub fn group(&self, id: &str) -> Option<&GroupInfo> {
        self.groups.get(id)
    }

    pub fn groups_for_topic(&self, topic: &str) -> &[Arc<str>] {
        self.topic_groups
            .get(topic)
            .map(Vec::as_slice)
            .unwrap_or_default()
    }

    pub fn gained_topics_since(&self, earlier: &Topology) -> bool {
        self.topics
            .keys()
            .any(|name| !earlier.topics.contains_key(name))
    }

    pub fn intern_topic(&self, name: &str) -> Arc<str> {
        match self.topics.get_key_value(name) {
            Some((key, _)) => Arc::clone(key),
            None => Arc::from(name),
        }
    }

    pub fn partition_count(&self) -> i32 {
        self.topics
            .values()
            .map(|topic| topic.partitions.len() as i32)
            .sum()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WatermarkTable {
    pub marks: HashMap<Arc<str>, HashMap<i32, Watermarks>>,
}

impl WatermarkTable {
    pub fn get(&self, topic: &str, partition: i32) -> Option<Watermarks> {
        self.marks.get(topic)?.get(&partition).copied()
    }

    pub fn produced(&self, topic: &str) -> i64 {
        self.marks
            .get(topic)
            .map(|marks| marks.values().map(|mark| mark.high.max(0)).sum())
            .unwrap_or(0)
    }

    pub fn retained(&self, topic: &str) -> i64 {
        self.marks
            .get(topic)
            .map(|marks| {
                marks
                    .values()
                    .map(|mark| (mark.high - mark.low).max(0))
                    .sum()
            })
            .unwrap_or(0)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupOffsets {
    pub committed: Vec<CommittedOffset>,
}

impl GroupOffsets {
    pub fn shows(&self, offsets: &[CommittedOffset]) -> bool {
        offsets.iter().all(|offset| self.committed.contains(offset))
    }

    pub fn partitions(&self) -> impl Iterator<Item = (&str, i32)> {
        self.committed
            .iter()
            .map(|offset| (offset.topic.as_str(), offset.partition))
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct OffsetTable {
    pub groups: HashMap<Arc<str>, Arc<GroupOffsets>>,
}

impl OffsetTable {
    pub fn get(&self, group: &str) -> Option<&Arc<GroupOffsets>> {
        self.groups.get(group)
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ConfigTable {
    pub topics: HashMap<Arc<str>, Arc<[ConfigEntry]>>,
}

impl ConfigTable {
    pub fn get(&self, topic: &str) -> Option<&[ConfigEntry]> {
        self.topics.get(topic).map(|entries| &**entries)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogDirInfo {
    pub path: String,
    pub error: Option<String>,
    pub total_bytes: Option<i64>,
    pub usable_bytes: Option<i64>,
    pub cordoned: bool,
    /// Every log in the directory, future replicas included.
    pub size_bytes: i64,
    /// Future replicas are left out: their partition is served from another
    /// directory until the move completes.
    pub replica_count: i32,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PartitionStorage {
    /// The largest replica. That is the leader's log unless a follower still
    /// holds a segment the leader has already deleted.
    pub size_bytes: i64,
    /// Every replica together.
    pub disk_bytes: i64,
}

impl PartitionStorage {
    fn add(self, other: Self) -> Self {
        Self {
            size_bytes: self.size_bytes + other.size_bytes,
            disk_bytes: self.disk_bytes + other.disk_bytes,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LogDirTable {
    pub brokers: BTreeMap<i32, Vec<LogDirInfo>>,
    pub partitions: HashMap<Arc<str>, BTreeMap<i32, PartitionStorage>>,
}

impl LogDirTable {
    pub fn assemble(dirs: Vec<LogDir>, interner: &mut Interner) -> Self {
        let mut brokers: BTreeMap<i32, Vec<LogDirInfo>> = BTreeMap::new();
        let mut partitions: HashMap<Arc<str>, BTreeMap<i32, PartitionStorage>> = HashMap::new();

        for dir in dirs {
            let mut replica_count = 0;
            for replica in dir.replicas.iter().filter(|replica| !replica.future) {
                replica_count += 1;
                let storage = partitions
                    .entry(interner.intern(&replica.topic))
                    .or_default()
                    .entry(replica.partition)
                    .or_default();
                storage.size_bytes = storage.size_bytes.max(replica.size_bytes);
                storage.disk_bytes += replica.size_bytes;
            }

            brokers.entry(dir.broker).or_default().push(LogDirInfo {
                size_bytes: dir.replicas.iter().map(|replica| replica.size_bytes).sum(),
                replica_count,
                path: dir.path,
                error: dir.error,
                total_bytes: dir.total_bytes,
                usable_bytes: dir.usable_bytes,
                cordoned: dir.cordoned,
            });
        }
        for dirs in brokers.values_mut() {
            dirs.sort_by(|left, right| left.path.cmp(&right.path));
        }

        Self {
            brokers,
            partitions,
        }
    }

    pub fn broker(&self, id: i32) -> Option<&[LogDirInfo]> {
        self.brokers.get(&id).map(Vec::as_slice)
    }

    pub fn partition(&self, topic: &str, partition: i32) -> Option<PartitionStorage> {
        self.partitions.get(topic)?.get(&partition).copied()
    }

    pub fn topic(&self, topic: &str) -> Option<PartitionStorage> {
        self.partitions.get(topic).map(|partitions| {
            partitions
                .values()
                .fold(PartitionStorage::default(), |total, partition| {
                    total.add(*partition)
                })
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubjectInfo {
    pub id: i32,
    pub schema_type: SchemaType,
    pub latest_version: i32,
    pub versions: Vec<i32>,
    pub compatibility: SchemaCompatibility,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SubjectTable {
    pub subjects: BTreeMap<Arc<str>, SubjectInfo>,
}

impl SubjectTable {
    pub fn assemble(subjects: &[SchemaSubject], interner: &mut Interner) -> Self {
        Self {
            subjects: subjects
                .iter()
                .map(|subject| {
                    (
                        interner.intern(&subject.subject),
                        SubjectInfo {
                            id: subject.id,
                            schema_type: subject.schema_type,
                            latest_version: subject.latest_version,
                            versions: subject.versions.clone(),
                            compatibility: subject.compatibility,
                        },
                    )
                })
                .collect(),
        }
    }

    pub fn get(&self, subject: &str) -> Option<&SubjectInfo> {
        self.subjects.get(subject)
    }
}

/// The schema id behind each version of each subject, keyed by version.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SchemaIdTable {
    pub subjects: BTreeMap<Arc<str>, BTreeMap<i32, i32>>,
}

impl SchemaIdTable {
    pub fn get(&self, subject: &str, version: i32) -> Option<i32> {
        self.subjects.get(subject)?.get(&version).copied()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::{group, log_dir, metadata, partition, subject, topic};

    #[test]
    fn the_interner_hands_out_one_allocation_per_name() {
        let mut interner = Interner::default();
        let first = interner.intern("orders");
        let second = interner.intern("orders");
        assert!(Arc::ptr_eq(&first, &second));

        let mut reseeded = Interner::seeded([&first]);
        assert!(Arc::ptr_eq(&reseeded.intern("orders"), &first));
    }

    #[test]
    fn topology_builds_the_reverse_index_at_commit_time() {
        let meta = metadata(vec![
            topic("orders", vec![partition(0, vec![1, 2], vec![1, 2])]),
            topic("payments", vec![partition(0, vec![1], vec![1])]),
        ]);
        let groups = vec![
            group("billing", "orders", vec![0]),
            group("audit", "orders", vec![0]),
        ];

        let topology = Topology::assemble(meta, groups, &mut Interner::default());

        assert_eq!(
            topology.groups_for_topic("orders"),
            ["audit", "billing"].map(Arc::from)
        );
        assert!(topology.groups_for_topic("payments").is_empty());
        assert!(topology.groups_for_topic("ghost").is_empty());
        assert_eq!(topology.topics["orders"].replication_factor(), 2);
        assert_eq!(topology.partition_count(), 2);
    }

    #[test]
    fn topology_interns_topic_names_across_tables() {
        let meta = metadata(vec![topic("orders", vec![partition(0, vec![1], vec![1])])]);
        let topology = Topology::assemble(meta, Vec::new(), &mut Interner::default());

        let key = topology.intern_topic("orders");
        let (stored, _) = topology.topics.get_key_value("orders").unwrap();
        assert!(Arc::ptr_eq(&key, stored));
        assert!(!Arc::ptr_eq(&topology.intern_topic("ghost"), &key));
    }

    #[test]
    fn a_group_that_only_left_offsets_behind_still_indexes_its_topic() {
        let mut snapshot = group("billing", "orders", Vec::new());
        snapshot.members.clear();
        snapshot.committed = vec![CommittedOffset {
            topic: "orders".into(),
            partition: 0,
            offset: 4,
        }];

        let topology = Topology::assemble(
            metadata(vec![topic("orders", vec![partition(0, vec![1], vec![1])])]),
            vec![snapshot],
            &mut Interner::default(),
        );

        assert_eq!(topology.groups_for_topic("orders").len(), 1);
    }

    #[test]
    fn watermark_totals_split_retained_from_produced() {
        let table = WatermarkTable {
            marks: HashMap::from_iter([(
                Arc::from("orders"),
                HashMap::from_iter([
                    (0, Watermarks { low: 40, high: 100 }),
                    (1, Watermarks { low: 0, high: 10 }),
                ]),
            )]),
        };

        assert_eq!(table.produced("orders"), 110);
        assert_eq!(table.retained("orders"), 70);
        assert_eq!(
            table.get("orders", 1),
            Some(Watermarks { low: 0, high: 10 })
        );
        assert_eq!(table.get("orders", 9), None);
        assert_eq!(table.retained("ghost"), 0);
    }

    #[test]
    fn under_replicated_reads_isr_against_replicas() {
        let healthy = TopicInfo {
            internal: false,
            partitions: vec![partition(0, vec![1, 2], vec![1, 2])],
        };
        let shrunken = TopicInfo {
            internal: false,
            partitions: vec![partition(0, vec![1, 2], vec![1])],
        };
        assert!(!healthy.under_replicated());
        assert!(shrunken.under_replicated());
    }

    #[test]
    fn subject_table_drops_schema_bodies() {
        let table =
            SubjectTable::assemble(&[subject("orders-value", 7, 3)], &mut Interner::default());

        assert_eq!(
            table.get("orders-value"),
            Some(&SubjectInfo {
                id: 7,
                schema_type: SchemaType::Avro,
                latest_version: 3,
                versions: vec![1, 2, 3],
                compatibility: SchemaCompatibility::Backward,
            }),
            "the list projection carries no schema body"
        );
    }

    #[test]
    fn a_schema_id_is_found_by_subject_and_version() {
        let table = SchemaIdTable {
            subjects: BTreeMap::from([(
                Arc::from("orders-value"),
                BTreeMap::from([(1, 5), (2, 6)]),
            )]),
        };

        assert_eq!(table.get("orders-value", 2), Some(6));
        assert_eq!(table.get("orders-value", 3), None);
        assert_eq!(table.get("payments-value", 1), None);
    }

    #[test]
    fn log_dirs_sum_per_directory_and_per_partition() {
        let table = LogDirTable::assemble(
            vec![
                log_dir(1, "/data/b", &[("orders", 0, 300), ("orders", 1, 50)]),
                log_dir(1, "/data/a", &[("payments", 0, 7)]),
                log_dir(2, "/data/a", &[("orders", 0, 280)]),
            ],
            &mut Interner::default(),
        );

        let paths: Vec<&str> = table
            .broker(1)
            .unwrap()
            .iter()
            .map(|dir| dir.path.as_str())
            .collect();
        assert_eq!(paths, ["/data/a", "/data/b"], "directories sort by path");
        assert_eq!(table.broker(1).unwrap()[1].size_bytes, 350);
        assert_eq!(table.broker(1).unwrap()[1].replica_count, 2);
        assert_eq!(table.broker(3), None);

        assert_eq!(
            table.partition("orders", 0),
            Some(PartitionStorage {
                size_bytes: 300,
                disk_bytes: 580,
            }),
            "a trailing follower does not shrink the partition"
        );
        assert_eq!(
            table.topic("orders"),
            Some(PartitionStorage {
                size_bytes: 350,
                disk_bytes: 630,
            })
        );
        assert_eq!(table.partition("orders", 9), None);
        assert_eq!(table.topic("ghost"), None);
    }

    #[test]
    fn a_future_replica_fills_its_directory_but_not_its_partition() {
        let mut moving = log_dir(1, "/data/b", &[("orders", 0, 90)]);
        moving.replicas[0].future = true;

        let table = LogDirTable::assemble(
            vec![log_dir(1, "/data/a", &[("orders", 0, 100)]), moving],
            &mut Interner::default(),
        );

        let target = &table.broker(1).unwrap()[1];
        assert_eq!(target.size_bytes, 90);
        assert_eq!(target.replica_count, 0);
        assert_eq!(
            table.partition("orders", 0),
            Some(PartitionStorage {
                size_bytes: 100,
                disk_bytes: 100,
            })
        );
    }

    #[test]
    fn log_dirs_intern_topic_names_they_already_know() {
        let mut interner = Interner::default();
        let known = interner.intern("orders");

        let table = LogDirTable::assemble(
            vec![log_dir(1, "/data", &[("orders", 0, 1)])],
            &mut interner,
        );

        let (stored, _) = table.partitions.get_key_value("orders").unwrap();
        assert!(Arc::ptr_eq(stored, &known));
    }
}
