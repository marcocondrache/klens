use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use chrono::{DateTime, Utc};

use crate::kafka::group::{CommittedOffset, GroupMember, GroupSnapshot, GroupState};
use crate::kafka::metadata::{BrokerMetadata, MetadataSnapshot, PartitionMetadata, TopicMetadata};
use crate::kafka::registry::{SchemaCompatibility, SchemaType};
use crate::kafka::topic_config::ConfigEntry;
use crate::kafka::watermarks::Watermarks;

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
    pub coordinator: i32,
    pub members: Vec<GroupMember>,
}

impl GroupInfo {
    pub fn from_snapshot(group: &GroupSnapshot) -> Self {
        Self {
            state: group.state,
            protocol: group.protocol.clone(),
            coordinator: group.coordinator,
            members: group.members.clone(),
        }
    }

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

    pub fn assigned_partitions(&self) -> Vec<(String, i32)> {
        let mut partitions: Vec<(String, i32)> = self
            .assigned_partition_refs()
            .map(|(topic, partition)| (topic.to_owned(), partition))
            .collect();
        partitions.sort();
        partitions.dedup();
        partitions
    }

    pub fn member_for(&self, topic: &str, partition: i32) -> Option<&str> {
        self.members
            .iter()
            .find(|member| member.assigned_to(topic, partition))
            .map(|member| member.id.as_str())
    }

    pub fn consumed_topics(&self) -> impl Iterator<Item = &str> {
        self.members.iter().flat_map(|member| {
            member
                .assignments
                .iter()
                .map(|assignment| assignment.topic.as_str())
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Topology {
    pub cluster_id: Option<String>,
    pub controller: Option<i32>,
    pub brokers: BTreeMap<i32, BrokerInfo>,
    pub topics: BTreeMap<Arc<str>, TopicInfo>,
    pub groups: BTreeMap<Arc<str>, GroupInfo>,
    pub topic_groups: HashMap<Arc<str>, Vec<Arc<str>>>,
}

impl Topology {
    pub fn from_snapshots(meta: MetadataSnapshot, groups: Vec<GroupSnapshot>) -> Self {
        let brokers = meta
            .brokers
            .into_iter()
            .map(|broker: BrokerMetadata| {
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

        let topics = meta
            .topics
            .into_iter()
            .map(|topic: TopicMetadata| {
                (
                    Arc::<str>::from(topic.name),
                    TopicInfo {
                        internal: topic.internal,
                        partitions: topic.partitions,
                    },
                )
            })
            .collect();

        let groups = groups
            .into_iter()
            .map(|group| {
                (
                    Arc::<str>::from(group.id.as_str()),
                    GroupInfo::from_snapshot(&group),
                )
            })
            .collect();

        let topic_groups = topic_groups_index(&topics, &groups);

        Self {
            cluster_id: meta.cluster_id,
            controller: None,
            brokers,
            topics,
            groups,
            topic_groups,
        }
    }

    pub fn intern_topic(&self, name: &str) -> Arc<str> {
        intern_key(&self.topics, name)
            .or_else(|| intern_key(&self.topic_groups, name))
            .unwrap_or_else(|| Arc::from(name))
    }

    pub fn intern_group(&self, name: &str) -> Arc<str> {
        intern_key(&self.groups, name).unwrap_or_else(|| Arc::from(name))
    }

    pub fn topic_partitions(&self) -> Vec<(Arc<str>, i32)> {
        self.topics
            .iter()
            .flat_map(|(name, topic)| {
                topic
                    .partitions
                    .iter()
                    .map(|partition| (Arc::clone(name), partition.id))
            })
            .collect()
    }
}

fn intern_key<V>(map: &impl LookupArc<V>, name: &str) -> Option<Arc<str>> {
    map.arc_key(name)
}

trait LookupArc<V> {
    fn arc_key(&self, name: &str) -> Option<Arc<str>>;
}

impl<V> LookupArc<V> for BTreeMap<Arc<str>, V> {
    fn arc_key(&self, name: &str) -> Option<Arc<str>> {
        self.get_key_value(name).map(|(key, _)| Arc::clone(key))
    }
}

impl<V> LookupArc<V> for HashMap<Arc<str>, V> {
    fn arc_key(&self, name: &str) -> Option<Arc<str>> {
        self.get_key_value(name).map(|(key, _)| Arc::clone(key))
    }
}

fn topic_groups_index(
    topics: &BTreeMap<Arc<str>, TopicInfo>,
    groups: &BTreeMap<Arc<str>, GroupInfo>,
) -> HashMap<Arc<str>, Vec<Arc<str>>> {
    let mut index: HashMap<Arc<str>, Vec<Arc<str>>> = HashMap::new();
    for (group_id, group) in groups {
        for topic_name in group.consumed_topics() {
            let topic = topics
                .get_key_value(topic_name)
                .map(|(key, _)| Arc::clone(key))
                .unwrap_or_else(|| Arc::from(topic_name));
            let members = index.entry(topic).or_default();
            if members.last().map(Arc::as_ref) != Some(group_id.as_ref()) {
                members.push(Arc::clone(group_id));
            }
        }
    }
    for members in index.values_mut() {
        members.sort();
        members.dedup();
    }
    index
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WatermarkTable {
    pub sampled_at: DateTime<Utc>,
    pub marks: HashMap<Arc<str>, HashMap<i32, Watermarks>>,
}

impl WatermarkTable {
    pub fn get(&self, topic: &str, partition: i32) -> Option<Watermarks> {
        self.marks.get(topic)?.get(&partition).copied()
    }

    pub fn topic_high_sum(&self, topic: &str) -> u64 {
        self.marks
            .get(topic)
            .map(|marks| marks.values().map(|mark| mark.high.max(0) as u64).sum())
            .unwrap_or(0)
    }

    pub fn topic_retained_sum(&self, topic: &str) -> u64 {
        self.marks
            .get(topic)
            .map(|marks| {
                marks
                    .values()
                    .map(|mark| (mark.high - mark.low).max(0) as u64)
                    .sum()
            })
            .unwrap_or(0)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupOffsets {
    pub sampled_at: DateTime<Utc>,
    pub committed: Vec<CommittedOffset>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct OffsetTable {
    pub groups: HashMap<Arc<str>, Arc<GroupOffsets>>,
}

impl OffsetTable {
    pub fn patch(
        &self,
        topology: Option<&Topology>,
        updates: HashMap<Arc<str>, Arc<GroupOffsets>>,
    ) -> Self {
        let mut groups = self.groups.clone();
        if let Some(topology) = topology {
            groups.retain(|id, _| topology.groups.contains_key(id));
        }
        groups.extend(updates);
        Self { groups }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ConfigTable {
    pub topics: HashMap<Arc<str>, Arc<Vec<ConfigEntry>>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubjectInfo {
    pub id: i32,
    pub schema_type: SchemaType,
    pub latest_version: i32,
    pub versions: Vec<i32>,
    pub compatibility: SchemaCompatibility,
    pub degraded: bool,
    pub error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SubjectTable {
    pub subjects: BTreeMap<Arc<str>, SubjectInfo>,
}
