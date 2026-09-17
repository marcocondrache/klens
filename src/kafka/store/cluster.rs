use std::sync::{Arc, RwLock};

use indexmap::IndexMap;

use crate::kafka::cluster::ClusterIdentity;
use crate::kafka::error::KafkaError;
use crate::kafka::search::SearchHit;
use crate::kafka::topic_config::ConfigEntry;

use super::bus::ChangeBus;
use super::interest::InterestRegistry;
use super::lane::Lane;
use super::projections::{
    self, BrokerRow, ClusterHealthView, GroupDetail, GroupRow, SubjectRow, TopicDetail,
    TopicGroupRow, TopicRow,
};
use super::search::SearchIndex;
use super::series::SeriesStore;
use super::tables::{ConfigTable, OffsetTable, SubjectTable, Topology, WatermarkTable};

/// The normalized read model for one cluster.
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

impl std::fmt::Debug for ClusterStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClusterStore")
            .field("identity", &self.identity)
            .field("topology", &self.topology.version())
            .field("watermarks", &self.watermarks.version())
            .field("offsets", &self.offsets.version())
            .field("configs", &self.configs.version())
            .field("subjects", &self.subjects.version())
            .finish_non_exhaustive()
    }
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
            series: SeriesStore::new(),
            bus: ChangeBus::new(),
            interest: InterestRegistry::new(),
            search: RwLock::new(Arc::new(SearchIndex::default())),
        }
    }

    pub fn name(&self) -> &str {
        &self.identity.name
    }

    /// A cluster is ready once its topology lane has committed at least once.
    pub fn ready(&self) -> bool {
        self.topology.ready()
    }

    pub fn rebuild_search(&self) {
        let topology = self.topology.load();
        let subjects = self.subjects.load();
        let index = SearchIndex::build(topology.as_deref(), subjects.as_deref());
        *self.search.write().expect("search index lock") = Arc::new(index);
    }

    pub fn search(&self, term: &str) -> Vec<SearchHit> {
        let index = Arc::clone(&self.search.read().expect("search index lock"));
        index.search(term)
    }

    pub fn topic_rows(&self) -> Vec<TopicRow> {
        let Some(topology) = self.topology.load() else {
            return Vec::new();
        };
        let watermarks = self.watermarks.load();
        let configs = self.configs.load();

        topology
            .topics
            .iter()
            .map(|(name, topic)| {
                projections::topic_row(
                    name,
                    topic,
                    watermarks.as_deref(),
                    configs.as_deref(),
                    &topology,
                    self.series.topic_rate(name).unwrap_or(0.0),
                )
            })
            .collect()
    }

    pub fn topic_row(&self, name: &str) -> Option<TopicRow> {
        let topology = self.topology.load()?;
        let (key, topic) = topology.topics.get_key_value(name)?;
        Some(projections::topic_row(
            key,
            topic,
            self.watermarks.load().as_deref(),
            self.configs.load().as_deref(),
            &topology,
            self.series.topic_rate(name).unwrap_or(0.0),
        ))
    }

    pub fn topic_detail(&self, name: &str) -> Option<TopicDetail> {
        let topology = self.topology.load()?;
        let (key, topic) = topology.topics.get_key_value(name)?;
        Some(projections::topic_detail(
            key,
            topic,
            self.watermarks.load().as_deref(),
            &topology,
        ))
    }

    pub fn topic_configs(&self, name: &str) -> Option<Vec<ConfigEntry>> {
        let configs = self.configs.load()?;
        configs.get(name).map(<[ConfigEntry]>::to_vec)
    }

    pub fn group_rows(&self) -> Vec<GroupRow> {
        let Some(topology) = self.topology.load() else {
            return Vec::new();
        };
        let offsets = self.offsets.load();
        let watermarks = self.watermarks.load();

        topology
            .groups
            .iter()
            .map(|(id, group)| {
                projections::group_row(
                    id,
                    group,
                    projections::offsets_for(offsets.as_deref(), id),
                    watermarks.as_deref(),
                )
            })
            .collect()
    }

    pub fn group_row(&self, id: &str) -> Option<GroupRow> {
        let topology = self.topology.load()?;
        let (key, group) = topology.groups.get_key_value(id)?;
        Some(projections::group_row(
            key,
            group,
            projections::offsets_for(self.offsets.load().as_deref(), id),
            self.watermarks.load().as_deref(),
        ))
    }

    /// Registers interest so the offsets lane promotes this group to its fast
    /// tier while someone is looking at it.
    pub fn group_detail(&self, id: &str) -> Option<GroupDetail> {
        let topology = self.topology.load()?;
        let (key, group) = topology.groups.get_key_value(id)?;
        self.interest.touch_group(id);
        Some(projections::group_detail(
            key,
            group,
            projections::offsets_for(self.offsets.load().as_deref(), id),
            self.watermarks.load().as_deref(),
        ))
    }

    pub fn topic_groups(&self, topic: &str) -> Vec<TopicGroupRow> {
        let Some(topology) = self.topology.load() else {
            return Vec::new();
        };
        let offsets = self.offsets.load();
        let watermarks = self.watermarks.load();

        topology
            .groups_for_topic(topic)
            .iter()
            .filter_map(|id| {
                let (key, group) = topology.groups.get_key_value(id)?;
                Some(projections::topic_group_row(
                    key,
                    topic,
                    group,
                    projections::offsets_for(offsets.as_deref(), id),
                    watermarks.as_deref(),
                ))
            })
            .collect()
    }

    pub fn broker_rows(&self) -> Vec<BrokerRow> {
        self.topology
            .load()
            .map(|topology| projections::broker_rows(&topology))
            .unwrap_or_default()
    }

    pub fn subject_rows(&self) -> Vec<SubjectRow> {
        self.subjects
            .load()
            .map(|subjects| projections::subject_rows(&subjects))
            .unwrap_or_default()
    }

    pub fn health(&self) -> ClusterHealthView {
        let topology = self.topology.load();
        let (under_replicated, offline) = topology
            .as_ref()
            .map(|topology| {
                topology
                    .topics
                    .values()
                    .flat_map(|topic| topic.partitions.iter())
                    .fold((0, 0), |(under, offline), partition| {
                        (
                            under + i32::from(partition.under_replicated()),
                            offline + i32::from(partition.offline()),
                        )
                    })
            })
            .unwrap_or((0, 0));

        ClusterHealthView {
            cluster: self.identity.name.clone(),
            topology: self.topology.health(),
            watermarks: self.watermarks.health(),
            offsets: self.offsets.health(),
            configs: self.configs.health(),
            subjects: self.subjects.health(),
            topic_count: topology
                .as_ref()
                .map(|topology| topology.topics.len() as i32)
                .unwrap_or(0),
            partition_count: topology
                .as_ref()
                .map(|topology| topology.partition_count())
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
            under_replicated_partitions: under_replicated,
            offline_partitions: offline,
        }
    }

    /// Wakes every lane.
    pub fn kick(&self) {
        self.topology.kick();
        self.watermarks.kick();
        self.offsets.kick();
        self.configs.kick();
        self.subjects.kick();
    }
}

/// Every configured cluster's store, in config order.
#[derive(Debug, Default)]
pub struct StoreSet {
    clusters: IndexMap<String, Arc<ClusterStore>>,
}

impl StoreSet {
    pub fn new(identities: impl IntoIterator<Item = ClusterIdentity>) -> Self {
        Self {
            clusters: identities
                .into_iter()
                .map(|identity| (identity.name.clone(), Arc::new(ClusterStore::new(identity))))
                .collect(),
        }
    }

    pub fn get(&self, name: &str) -> Option<&Arc<ClusterStore>> {
        self.clusters.get(name)
    }

    pub fn cluster(&self, name: &str) -> Result<&Arc<ClusterStore>, KafkaError> {
        self.get(name)
            .ok_or_else(|| KafkaError::UnknownCluster(name.to_owned()))
    }

    pub fn names(&self) -> impl Iterator<Item = &str> {
        self.clusters.keys().map(String::as_str)
    }

    pub fn iter(&self) -> impl Iterator<Item = &Arc<ClusterStore>> {
        self.clusters.values()
    }

    pub fn len(&self) -> usize {
        self.clusters.len()
    }

    pub fn is_empty(&self) -> bool {
        self.clusters.is_empty()
    }

    /// Readiness: every configured cluster's topology lane has committed.
    pub fn ready(&self) -> bool {
        self.clusters.values().all(|store| store.ready())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    use crate::kafka::store::fixtures::{
        at, group, identity, offsets, partition, subject, topic, topology, watermarks,
    };
    use crate::kafka::store::tables::{GroupOffsets, Interner};

    fn seeded() -> ClusterStore {
        let store = ClusterStore::new(identity("local"));
        let topology = topology(
            vec![
                topic("orders", vec![partition(0, vec![1], vec![1])]),
                topic("payments", vec![partition(0, vec![1], vec![1])]),
            ],
            vec![group("billing", "orders", vec![0])],
        );
        let orders = topology.intern_topic("orders");
        store.topology.commit(Arc::new(topology));
        store
            .watermarks
            .commit(Arc::new(watermarks(at(1_000), &[("orders", 0, 10, 60)])));
        store.offsets.commit(Arc::new(OffsetTable {
            groups: HashMap::from([(
                Arc::from("billing"),
                Arc::new(offsets(at(1_000), &[("orders", 0, 45)])),
            )]),
        }));
        store.series.push_topic_rate(&orders, at(1_000), 7.5);
        store
    }

    #[test]
    fn an_empty_store_is_not_ready_and_projects_nothing() {
        let store = ClusterStore::new(identity("local"));

        assert!(!store.ready());
        assert!(store.topic_rows().is_empty());
        assert!(store.group_rows().is_empty());
        assert!(store.broker_rows().is_empty());
        assert!(store.subject_rows().is_empty());
        assert!(store.topic_detail("orders").is_none());
        assert!(store.group_detail("billing").is_none());
        assert!(store.topic_groups("orders").is_empty());
        assert!(store.topic_configs("orders").is_none());
        assert!(store.search("orders").is_empty());

        let health = store.health();
        assert_eq!(health.topic_count, 0);
        assert!(!health.topology.healthy());
    }

    #[test]
    fn rows_join_across_lanes() {
        let store = seeded();

        let rows = store.topic_rows();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].name.as_ref(), "orders");
        assert_eq!(rows[0].retained_messages, 50);
        assert_eq!(rows[0].rate, 7.5);
        assert_eq!(rows[0].group_count, 1);
        assert_eq!(rows[1].name.as_ref(), "payments");
        assert_eq!(rows[1].rate, 0.0, "a topic with no samples reads as idle");

        let groups = store.group_rows();
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].total_lag, 15);
        assert!(groups[0].lag_complete);
    }

    #[test]
    fn a_read_sees_each_lane_at_whatever_version_it_is_on() {
        let store = seeded();
        store
            .watermarks
            .commit(Arc::new(watermarks(at(2_000), &[])));

        let row = store.topic_row("orders").expect("orders exists");
        assert_eq!(row.retained_messages, 0, "the newer watermark table wins");
        assert_eq!(row.partition_count, 1, "topology is unaffected");
    }

    #[test]
    fn a_topic_missing_from_one_lane_does_not_break_the_join() {
        let store = seeded();

        let payments = store.topic_detail("payments").expect("payments exists");
        assert_eq!(payments.partitions[0].high_watermark, 0);
        assert_eq!(payments.retained_messages, 0);
        assert_eq!(payments.group_count, 0);
    }

    #[test]
    fn topic_groups_reads_the_reverse_index() {
        let store = seeded();

        let rows = store.topic_groups("orders");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].id.as_ref(), "billing");
        assert_eq!(rows[0].lag_on_topic, 15);
        assert!(store.topic_groups("payments").is_empty());
    }

    #[test]
    fn opening_a_group_detail_registers_interest() {
        let store = seeded();
        assert!(!store.interest.is_hot("billing"));

        let detail = store.group_detail("billing").expect("billing exists");

        assert_eq!(detail.total_lag, 15);
        assert!(
            store.interest.is_hot("billing"),
            "a viewed group must join the fast offset tier"
        );
    }

    #[test]
    fn the_search_index_follows_topology_and_subject_commits() {
        let store = seeded();
        store.rebuild_search();
        assert_eq!(store.search("orders").len(), 1);

        store.subjects.commit(Arc::new(SubjectTable::assemble(
            &[subject("orders-value", 1, 1)],
            &mut Interner::default(),
        )));
        store.rebuild_search();

        assert_eq!(store.search("orders").len(), 2);
        assert_eq!(store.subject_rows().len(), 1);
    }

    #[test]
    fn group_offsets_are_shared_pointers_not_copies() {
        let store = seeded();
        let first = Arc::clone(store.offsets.load().unwrap().get("billing").unwrap());

        store.offsets.commit(Arc::new(OffsetTable {
            groups: HashMap::from([
                (Arc::from("billing"), Arc::clone(&first)),
                (
                    Arc::from("audit"),
                    Arc::new(offsets(at(2_000), &[("orders", 0, 10)])),
                ),
            ]),
        }));

        let second: Arc<GroupOffsets> =
            Arc::clone(store.offsets.load().unwrap().get("billing").unwrap());
        assert!(
            Arc::ptr_eq(&first, &second),
            "a wave rebuilds the outer map of pointers, not every group"
        );
    }

    #[test]
    fn health_reports_every_lane_independently() {
        let store = seeded();
        store.subjects.record_poll(
            std::time::Duration::from_millis(9),
            Some("registry down".into()),
        );

        let health = store.health();
        assert_eq!(health.topic_count, 2);
        assert_eq!(health.partition_count, 2);
        assert_eq!(health.group_count, 1);
        assert_eq!(health.broker_count, 1);
        assert_eq!(health.subject_count, 0);
        assert!(health.topology.healthy());
        assert_eq!(health.subjects.last_error.as_deref(), Some("registry down"));
    }

    #[test]
    fn a_store_set_keeps_config_order_and_gates_readiness() {
        let stores = StoreSet::new([identity("prod"), identity("staging")]);

        assert_eq!(stores.names().collect::<Vec<_>>(), vec!["prod", "staging"]);
        assert!(stores.cluster("ghost").is_err());
        assert!(!stores.ready());

        stores
            .cluster("prod")
            .unwrap()
            .topology
            .commit(Arc::new(Topology::default()));
        assert!(!stores.ready(), "one cluster is not the whole set");

        stores
            .cluster("staging")
            .unwrap()
            .topology
            .commit(Arc::new(Topology::default()));
        assert!(stores.ready());
    }
}
