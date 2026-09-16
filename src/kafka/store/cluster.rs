use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use indexmap::IndexMap;

use crate::kafka::cluster::{ClusterHealth, ClusterIdentity};
use crate::kafka::group::{CommittedOffset, GroupOffset, GroupState};
use crate::kafka::search::SearchHit;
use crate::kafka::topic::Partition;
use crate::kafka::topic_config::{CleanupPolicy, topic_config_values};

use super::bus::{ChangeBus, GroupLagUpdate, GroupOffsetView};
use super::interest::InterestRegistry;
use super::lane::{Lane, LaneHealth};
use super::search::SearchIndex;
use super::series::SeriesStore;
use super::tables::{
    ConfigTable, GroupInfo, OffsetTable, SubjectTable, TopicInfo, Topology, WatermarkTable,
};

pub struct ClusterStore {
    pub identity: ClusterIdentity,
    pub topology: Lane<Topology>,
    pub watermarks: Lane<WatermarkTable>,
    pub offsets: Lane<OffsetTable>,
    pub configs: Lane<ConfigTable>,
    pub subjects: Lane<SubjectTable>,
    pub series: SeriesStore,
    pub bus: ChangeBus,
    pub interest: InterestRegistry,
    search: RwLock<Arc<SearchIndex>>,
}

impl ClusterStore {
    pub fn new(identity: ClusterIdentity) -> Self {
        Self {
            identity,
            topology: Lane::new(),
            watermarks: Lane::new(),
            offsets: Lane::new(),
            configs: Lane::new(),
            subjects: Lane::new(),
            series: SeriesStore::default(),
            bus: ChangeBus::new(),
            interest: InterestRegistry::default(),
            search: RwLock::new(Arc::new(SearchIndex::default())),
        }
    }

    pub fn ready(&self) -> bool {
        self.topology.load().is_some()
    }

    pub fn rebuild_search(&self) {
        let index = SearchIndex::rebuild(
            self.topology.load().as_deref(),
            self.subjects.load().as_deref(),
        );
        *self.search.write().expect("search lock") = Arc::new(index);
    }

    pub fn search(&self, term: &str) -> Vec<SearchHit> {
        self.search.read().expect("search lock").search(term)
    }

    pub fn topic_rows(&self) -> Option<Vec<TopicRow>> {
        let topology = self.topology.load()?;
        let watermarks = self.watermarks.load();
        let configs = self.configs.load();
        Some(
            topology
                .topics
                .iter()
                .map(|(name, topic)| {
                    TopicRow::from_tables(
                        name,
                        topic,
                        watermarks.as_deref(),
                        configs.as_deref(),
                        topology.topic_groups.get(name).map(Vec::len).unwrap_or(0),
                        self.series.last_topic_rate(name),
                    )
                })
                .collect(),
        )
    }

    pub fn topic_detail(&self, name: &str) -> Option<TopicDetail> {
        let topology = self.topology.load()?;
        let (name, topic) = topology.topics.get_key_value(name)?;
        let watermarks = self.watermarks.load();
        let configs = self.configs.load();
        Some(TopicDetail::from_tables(
            name,
            topic,
            watermarks.as_deref(),
            configs.as_deref(),
            topology.topic_groups.get(name).cloned().unwrap_or_default(),
            self.series.last_topic_rate(name),
        ))
    }

    pub fn group_rows(&self) -> Option<Vec<GroupRow>> {
        let topology = self.topology.load()?;
        let offsets = self.offsets.load();
        let watermarks = self.watermarks.load();
        Some(
            topology
                .groups
                .iter()
                .map(|(id, group)| {
                    GroupRow::from_tables(id, group, offsets.as_deref(), watermarks.as_deref())
                })
                .collect(),
        )
    }

    pub fn group_detail(&self, id: &str) -> Option<GroupDetail> {
        self.interest.touch_group(id);
        let topology = self.topology.load()?;
        let (id, group) = topology.groups.get_key_value(id)?;
        let offsets = self.offsets.load();
        let watermarks = self.watermarks.load();
        Some(GroupDetail::from_tables(
            id,
            group,
            offsets.as_deref(),
            watermarks.as_deref(),
        ))
    }

    pub fn topic_groups(&self, topic: &str) -> Option<Vec<TopicGroupRow>> {
        let topology = self.topology.load()?;
        let members = topology.topic_groups.get(topic)?;
        let offsets = self.offsets.load();
        let watermarks = self.watermarks.load();
        Some(
            members
                .iter()
                .filter_map(|id| {
                    let group = topology.groups.get(id)?;
                    Some(TopicGroupRow::from_tables(
                        id,
                        group,
                        topic,
                        offsets.as_deref(),
                        watermarks.as_deref(),
                    ))
                })
                .collect(),
        )
    }

    pub fn broker_rows(&self) -> Option<Vec<BrokerRow>> {
        let topology = self.topology.load()?;
        let mut partition_counts = HashMap::<i32, i32>::new();
        let mut leader_counts = HashMap::<i32, i32>::new();
        for topic in topology.topics.values() {
            for partition in &topic.partitions {
                for replica in &partition.replicas {
                    *partition_counts.entry(*replica).or_default() += 1;
                }
                if !partition.offline() {
                    *leader_counts.entry(partition.leader).or_default() += 1;
                }
            }
        }
        Some(
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
                .collect(),
        )
    }

    pub fn subject_rows(&self) -> Option<Vec<SubjectRow>> {
        let subjects = self.subjects.load()?;
        Some(
            subjects
                .subjects
                .iter()
                .map(|(name, info)| SubjectRow {
                    name: Arc::clone(name),
                    id: info.id,
                    schema_type: info.schema_type,
                    latest_version: info.latest_version,
                    versions: info.versions.clone(),
                    compatibility: info.compatibility,
                    degraded: info.degraded,
                    error: info.error.clone(),
                })
                .collect(),
        )
    }

    pub fn health(&self) -> ClusterHealthView {
        let topology = self.topology.load();
        let mut partition_count = 0;
        let mut under_replicated = 0;
        let mut offline = 0;
        if let Some(topology) = topology.as_ref() {
            for topic in topology.topics.values() {
                for partition in &topic.partitions {
                    partition_count += 1;
                    under_replicated += i32::from(partition.under_replicated());
                    offline += i32::from(partition.offline());
                }
            }
        }
        ClusterHealthView {
            cluster: self.identity.name.clone(),
            ready: topology.is_some(),
            health: topology
                .as_ref()
                .map(|topology| {
                    ClusterHealth::from_partitions(
                        topology
                            .topics
                            .values()
                            .flat_map(|topic| topic.partitions.iter()),
                    )
                })
                .unwrap_or(ClusterHealth::Offline),
            topic_count: topology
                .as_ref()
                .map(|topology| topology.topics.len() as i32)
                .unwrap_or(0),
            group_count: topology
                .as_ref()
                .map(|topology| topology.groups.len() as i32)
                .unwrap_or(0),
            broker_count: topology
                .as_ref()
                .map(|topology| topology.brokers.len() as i32)
                .unwrap_or(0),
            subject_count: self
                .subjects
                .load()
                .map(|subjects| subjects.subjects.len() as i32)
                .unwrap_or(0),
            partition_count,
            under_replicated_partitions: under_replicated,
            offline_partitions: offline,
            topology: self.topology.health(),
            watermarks: self.watermarks.health(),
            offsets: self.offsets.health(),
            configs: self.configs.health(),
            subjects: self.subjects.health(),
        }
    }
}

pub struct StoreSet {
    clusters: IndexMap<String, Arc<ClusterStore>>,
}

impl StoreSet {
    pub fn new(stores: impl IntoIterator<Item = ClusterStore>) -> Self {
        let mut clusters = IndexMap::new();
        for store in stores {
            clusters.insert(store.identity.name.clone(), Arc::new(store));
        }
        Self { clusters }
    }

    pub fn get(&self, name: &str) -> Option<&Arc<ClusterStore>> {
        self.clusters.get(name)
    }

    pub fn names(&self) -> impl Iterator<Item = &str> {
        self.clusters.keys().map(String::as_str)
    }

    pub fn iter(&self) -> impl Iterator<Item = &Arc<ClusterStore>> {
        self.clusters.values()
    }

    pub fn ready(&self) -> bool {
        self.clusters.values().all(|store| store.ready())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct TopicRow {
    pub name: Arc<str>,
    pub internal: bool,
    pub partition_count: i32,
    pub replication_factor: i32,
    pub retained_messages: u64,
    pub produced_total: u64,
    pub rate: f64,
    pub retention_ms: i64,
    pub cleanup_policy: CleanupPolicy,
    pub group_count: i32,
    pub under_replicated: bool,
}

impl TopicRow {
    fn from_tables(
        name: &Arc<str>,
        topic: &TopicInfo,
        watermarks: Option<&WatermarkTable>,
        configs: Option<&ConfigTable>,
        group_count: usize,
        rate: Option<f64>,
    ) -> Self {
        let (cleanup_policy, retention_ms) = topic_config_values(
            configs
                .and_then(|table| table.topics.get(name))
                .map(|entries| entries.as_slice()),
        );
        Self {
            name: Arc::clone(name),
            internal: topic.internal,
            partition_count: topic.partitions.len() as i32,
            replication_factor: topic.replication_factor(),
            retained_messages: watermarks
                .map(|table| table.topic_retained_sum(name))
                .unwrap_or(0),
            produced_total: watermarks
                .map(|table| table.topic_high_sum(name))
                .unwrap_or(0),
            rate: rate.unwrap_or(0.0),
            retention_ms,
            cleanup_policy,
            group_count: group_count as i32,
            under_replicated: topic.under_replicated(),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct TopicDetail {
    pub row: TopicRow,
    pub partitions: Vec<Partition>,
    pub consumer_groups: Vec<Arc<str>>,
}

impl TopicDetail {
    fn from_tables(
        name: &Arc<str>,
        topic: &TopicInfo,
        watermarks: Option<&WatermarkTable>,
        configs: Option<&ConfigTable>,
        consumer_groups: Vec<Arc<str>>,
        rate: Option<f64>,
    ) -> Self {
        let partitions = topic
            .partitions
            .iter()
            .map(|partition| {
                let marks = watermarks
                    .and_then(|table| table.get(name, partition.id))
                    .unwrap_or_default();
                partition.with_watermarks(marks)
            })
            .collect();
        Self {
            row: TopicRow::from_tables(
                name,
                topic,
                watermarks,
                configs,
                consumer_groups.len(),
                rate,
            ),
            partitions,
            consumer_groups,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupRow {
    pub id: Arc<str>,
    pub state: GroupState,
    pub member_count: i32,
    pub topic_names: Vec<String>,
    pub total_lag: i64,
    pub lag_complete: bool,
    pub coordinator_id: i32,
}

impl GroupRow {
    fn from_tables(
        id: &Arc<str>,
        group: &GroupInfo,
        offsets: Option<&OffsetTable>,
        watermarks: Option<&WatermarkTable>,
    ) -> Self {
        let lag = lag_for_group(id, group, offsets, watermarks);
        Self {
            id: Arc::clone(id),
            state: group.state,
            member_count: group.members.len() as i32,
            topic_names: lag.topic_names,
            total_lag: lag.total_lag,
            lag_complete: lag.lag_complete,
            coordinator_id: group.coordinator,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupDetail {
    pub row: GroupRow,
    pub protocol: String,
    pub members: Vec<crate::kafka::group::GroupMember>,
    pub offsets: Vec<GroupOffset>,
}

impl GroupDetail {
    fn from_tables(
        id: &Arc<str>,
        group: &GroupInfo,
        offsets: Option<&OffsetTable>,
        watermarks: Option<&WatermarkTable>,
    ) -> Self {
        let lag = lag_for_group(id, group, offsets, watermarks);
        Self {
            row: GroupRow {
                id: Arc::clone(id),
                state: group.state,
                member_count: group.members.len() as i32,
                topic_names: lag.topic_names.clone(),
                total_lag: lag.total_lag,
                lag_complete: lag.lag_complete,
                coordinator_id: group.coordinator,
            },
            protocol: group.protocol.clone(),
            members: group.members.clone(),
            offsets: lag.offsets,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicGroupRow {
    pub id: Arc<str>,
    pub state: GroupState,
    pub member_count: i32,
    pub lag_on_topic: i64,
}

impl TopicGroupRow {
    fn from_tables(
        id: &Arc<str>,
        group: &GroupInfo,
        topic: &str,
        offsets: Option<&OffsetTable>,
        watermarks: Option<&WatermarkTable>,
    ) -> Self {
        let lag = lag_for_group(id, group, offsets, watermarks);
        let lag_on_topic = lag
            .offsets
            .iter()
            .filter(|offset| offset.topic == topic)
            .map(|offset| offset.lag)
            .sum();
        Self {
            id: Arc::clone(id),
            state: group.state,
            member_count: group.members.len() as i32,
            lag_on_topic,
        }
    }
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
    pub name: Arc<str>,
    pub id: i32,
    pub schema_type: crate::kafka::registry::SchemaType,
    pub latest_version: i32,
    pub versions: Vec<i32>,
    pub compatibility: crate::kafka::registry::SchemaCompatibility,
    pub degraded: bool,
    pub error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterHealthView {
    pub cluster: String,
    pub ready: bool,
    pub health: ClusterHealth,
    pub topic_count: i32,
    pub group_count: i32,
    pub broker_count: i32,
    pub subject_count: i32,
    pub partition_count: i32,
    pub under_replicated_partitions: i32,
    pub offline_partitions: i32,
    pub topology: LaneHealth,
    pub watermarks: LaneHealth,
    pub offsets: LaneHealth,
    pub configs: LaneHealth,
    pub subjects: LaneHealth,
}

struct LagJoin {
    total_lag: i64,
    lag_complete: bool,
    topic_names: Vec<String>,
    offsets: Vec<GroupOffset>,
}

fn lag_for_group(
    id: &Arc<str>,
    group: &GroupInfo,
    offsets: Option<&OffsetTable>,
    watermarks: Option<&WatermarkTable>,
) -> LagJoin {
    let Some(table) = offsets.and_then(|table| table.groups.get(id)) else {
        let assigned: Vec<(String, i32)> = group.assigned_partitions();
        let mut views = Vec::new();
        let mut complete = true;
        for (topic, partition) in assigned {
            match watermarks.and_then(|table| table.get(&topic, partition)) {
                Some(marks) => views.push(GroupOffset {
                    topic: topic.clone(),
                    partition,
                    current_offset: 0,
                    end_offset: marks.high,
                    lag: marks.high.max(0),
                    member_id: group.member_for(&topic, partition).map(ToOwned::to_owned),
                }),
                None => complete = false,
            }
        }
        views.sort_by(|left, right| {
            left.topic
                .cmp(&right.topic)
                .then(left.partition.cmp(&right.partition))
        });
        let topic_names = unique_topics(&views);
        let total_lag = views.iter().map(|offset| offset.lag).sum();
        return LagJoin {
            total_lag,
            lag_complete: complete && watermarks.is_some(),
            topic_names,
            offsets: views,
        };
    };

    group_lag_join(group, table.committed.as_slice(), watermarks)
}

pub fn group_lag_update(
    id: Arc<str>,
    group: Option<&GroupInfo>,
    committed: &[CommittedOffset],
    watermarks: Option<&WatermarkTable>,
) -> GroupLagUpdate {
    let join = match group {
        Some(group) => group_lag_join(group, committed, watermarks),
        None => committed_only_join(committed, watermarks),
    };
    GroupLagUpdate {
        group: id,
        total_lag: join.total_lag,
        lag_complete: join.lag_complete,
        offsets: join
            .offsets
            .into_iter()
            .map(|offset| GroupOffsetView {
                topic: Arc::from(offset.topic),
                partition: offset.partition,
                committed: offset.current_offset,
                end: offset.end_offset,
                lag: offset.lag,
                member_id: offset.member_id,
            })
            .collect(),
    }
}

fn group_lag_join(
    group: &GroupInfo,
    committed: &[CommittedOffset],
    watermarks: Option<&WatermarkTable>,
) -> LagJoin {
    let mut seen = HashMap::<(String, i32), GroupOffset>::new();
    let mut complete = watermarks.is_some();

    for committed in committed {
        match watermarks.and_then(|table| table.get(&committed.topic, committed.partition)) {
            Some(marks) => {
                seen.insert(
                    (committed.topic.clone(), committed.partition),
                    GroupOffset {
                        topic: committed.topic.clone(),
                        partition: committed.partition,
                        current_offset: committed.offset,
                        end_offset: marks.high,
                        lag: (marks.high - committed.offset).max(0),
                        member_id: group
                            .member_for(&committed.topic, committed.partition)
                            .map(ToOwned::to_owned),
                    },
                );
            }
            None => complete = false,
        }
    }

    for (topic, partition) in group.assigned_partition_refs() {
        seen.entry((topic.to_owned(), partition))
            .or_insert_with(
                || match watermarks.and_then(|table| table.get(topic, partition)) {
                    Some(marks) => GroupOffset {
                        topic: topic.to_owned(),
                        partition,
                        current_offset: 0,
                        end_offset: marks.high,
                        lag: marks.high.max(0),
                        member_id: group.member_for(topic, partition).map(ToOwned::to_owned),
                    },
                    None => {
                        complete = false;
                        GroupOffset {
                            topic: topic.to_owned(),
                            partition,
                            current_offset: 0,
                            end_offset: 0,
                            lag: 0,
                            member_id: group.member_for(topic, partition).map(ToOwned::to_owned),
                        }
                    }
                },
            );
    }

    let mut offsets: Vec<GroupOffset> = seen.into_values().collect();
    offsets.sort_by(|left, right| {
        left.topic
            .cmp(&right.topic)
            .then(left.partition.cmp(&right.partition))
    });
    let topic_names = unique_topics(&offsets);
    let total_lag = offsets.iter().map(|offset| offset.lag).sum();
    LagJoin {
        total_lag,
        lag_complete: complete,
        topic_names,
        offsets,
    }
}

fn committed_only_join(
    committed: &[CommittedOffset],
    watermarks: Option<&WatermarkTable>,
) -> LagJoin {
    let mut offsets = Vec::new();
    let mut complete = watermarks.is_some();
    for committed in committed {
        match watermarks.and_then(|table| table.get(&committed.topic, committed.partition)) {
            Some(marks) => offsets.push(GroupOffset {
                topic: committed.topic.clone(),
                partition: committed.partition,
                current_offset: committed.offset,
                end_offset: marks.high,
                lag: (marks.high - committed.offset).max(0),
                member_id: None,
            }),
            None => complete = false,
        }
    }
    offsets.sort_by(|left, right| {
        left.topic
            .cmp(&right.topic)
            .then(left.partition.cmp(&right.partition))
    });
    let topic_names = unique_topics(&offsets);
    let total_lag = offsets.iter().map(|offset| offset.lag).sum();
    LagJoin {
        total_lag,
        lag_complete: complete,
        topic_names,
        offsets,
    }
}

fn unique_topics(offsets: &[GroupOffset]) -> Vec<String> {
    let mut topics = Vec::new();
    for offset in offsets {
        if topics.last() != Some(&offset.topic) {
            topics.push(offset.topic.clone());
        }
    }
    topics
}
