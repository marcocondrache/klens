use std::time::Duration;

use crate::kafka::acl::AclListing;
use crate::kafka::cluster::ClusterIdentity;
use crate::kafka::quota::QuotaListing;
use crate::kafka::topic_config::ConfigEntry;

use super::bus::ChangeBus;
use super::interest::InterestRegistry;
use super::lane::Lane;
use super::projections::{
    self, BrokerRow, ClusterHealthView, GroupDetail, GroupRow, SubjectRow, TopicDetail,
    TopicGroupRow, TopicRow,
};
use super::rates::RateStore;
use super::search::{self, SearchHit};
use super::tables::{
    ConfigTable, LogDirTable, OffsetTable, SubjectTable, Topology, WatermarkTable,
};

pub struct ClusterStore {
    pub identity: ClusterIdentity,
    pub topology: Lane<Topology>,
    pub watermarks: Lane<WatermarkTable>,
    pub offsets: Lane<OffsetTable>,
    pub configs: Lane<ConfigTable>,
    pub subjects: Lane<SubjectTable>,
    pub log_dirs: Lane<LogDirTable>,
    pub acls: Lane<AclListing>,
    pub quotas: Lane<QuotaListing>,
    pub rates: RateStore,
    pub bus: ChangeBus,
    pub interest: InterestRegistry,
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
            .field("log_dirs", &self.log_dirs.version())
            .field("acls", &self.acls.version())
            .field("quotas", &self.quotas.version())
            .finish_non_exhaustive()
    }
}

impl ClusterStore {
    pub fn new(identity: ClusterIdentity, interest_ttl: Duration) -> Self {
        Self {
            identity,
            topology: Lane::new(),
            watermarks: Lane::new(),
            offsets: Lane::new(),
            configs: Lane::new(),
            subjects: Lane::new(),
            log_dirs: Lane::new(),
            acls: Lane::new(),
            quotas: Lane::new(),
            rates: RateStore::new(),
            bus: ChangeBus::new(),
            interest: InterestRegistry::new(interest_ttl),
        }
    }

    pub fn name(&self) -> &str {
        &self.identity.name
    }

    pub fn ready(&self) -> bool {
        self.topology.ready()
    }

    pub fn search(&self, term: &str) -> Vec<SearchHit> {
        let topology = self.topology.load();
        let subjects = self.subjects.load();
        search::find(topology.as_deref(), subjects.as_deref(), term)
    }

    pub fn topic_rows(&self) -> Vec<TopicRow> {
        let Some(topology) = self.topology.load() else {
            return Vec::new();
        };
        let watermarks = self.watermarks.load();
        let configs = self.configs.load();
        let log_dirs = self.log_dirs.load();

        topology
            .topics
            .iter()
            .map(|(name, topic)| {
                projections::topic_row(
                    name,
                    topic,
                    watermarks.as_deref(),
                    configs.as_deref(),
                    log_dirs.as_deref(),
                    &topology,
                    self.rates.get(name).unwrap_or(0.0),
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
            self.log_dirs.load().as_deref(),
            &topology,
            self.rates.get(name).unwrap_or(0.0),
        ))
    }

    pub fn topic_detail(&self, name: &str) -> Option<TopicDetail> {
        let topology = self.topology.load()?;
        let (key, topic) = topology.topics.get_key_value(name)?;
        Some(projections::topic_detail(
            key,
            topic,
            self.watermarks.load().as_deref(),
            self.configs.load().as_deref(),
            self.log_dirs.load().as_deref(),
            &topology,
            self.rates.get(name).unwrap_or(0.0),
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

    pub fn group_detail(&self, id: &str) -> Option<GroupDetail> {
        let topology = self.topology.load()?;
        let (key, group) = topology.groups.get_key_value(id)?;
        self.interest.touch_group(key);
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
            .map(|topology| projections::broker_rows(&topology, self.log_dirs.load().as_deref()))
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
            log_dirs: self.log_dirs.health(),
            acls: self.acls.health(),
            quotas: self.quotas.health(),
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
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use foldhash::HashMap;

    use crate::testing::{group, offsets, partition, topic, topology, watermarks};

    fn seeded() -> ClusterStore {
        let store = ClusterStore::named("local");
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
            .commit(Arc::new(watermarks(&[("orders", 0, 10, 60)])));
        store.offsets.commit(Arc::new(OffsetTable {
            groups: HashMap::from_iter([(
                Arc::from("billing"),
                Arc::new(offsets(&[("orders", 0, 45)])),
            )]),
        }));
        store.rates.set(&orders, 7.5);
        store
    }

    #[test]
    fn an_empty_store_is_not_ready_and_projects_nothing() {
        let store = ClusterStore::named("local");

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

        let orders = store.topic_detail("orders").expect("orders exists");
        assert_eq!(orders.rate, 7.5);
        assert_eq!(
            orders.retention_ms, None,
            "retention is unknown before the configs lane fetches this topic"
        );
        assert_eq!(
            orders.cleanup_policy,
            crate::kafka::topic_config::CleanupPolicy::Delete
        );
        assert_eq!(rows[1].name.as_ref(), "payments");
        assert_eq!(rows[1].rate, 0.0, "a topic with no samples reads as idle");

        let groups = store.group_rows();
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].total_lag, Some(15));
        assert!(groups[0].lag_complete);
    }

    #[test]
    fn a_read_sees_each_lane_at_whatever_version_it_is_on() {
        let store = seeded();
        store.watermarks.commit(Arc::new(watermarks(&[])));

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
    fn health_reports_every_lane_independently() {
        let store = seeded();
        store.subjects.record_poll(
            std::time::Duration::from_millis(9),
            Some("registry down".into()),
        );
        store.quotas.record_poll(
            std::time::Duration::from_millis(4),
            Some("quotas unsupported".into()),
        );

        let health = store.health();
        assert_eq!(health.topic_count, 2);
        assert_eq!(health.partition_count, 2);
        assert_eq!(health.group_count, 1);
        assert_eq!(health.broker_count, 1);
        assert_eq!(health.subject_count, 0);
        assert!(health.topology.healthy());
        assert_eq!(health.subjects.last_error.as_deref(), Some("registry down"));
        assert_eq!(
            health.quotas.last_error.as_deref(),
            Some("quotas unsupported")
        );
    }
}
