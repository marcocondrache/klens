//! Live Kafka facade.
//!
//! [`QueryEngine`] looks up a [`ClusterSession`], assembles a
//! [`ClusterSnapshot`] via `catalog_from`, and serves records, configs, a
//! single consumer group, and schema subjects.

use std::collections::HashMap;

use futures::future::join_all;
use indexmap::IndexMap;

use crate::config::Config;
use crate::environment::OFFSET_FETCH_BATCH;
use crate::kafka::adapter::ClusterHandle;
use crate::kafka::broker::Broker;
use crate::kafka::catalog::{CatalogAssemble, CatalogReuse, ClusterSnapshot};
use crate::kafka::cluster::{ClusterIdentity, ClusterOverview};
use crate::kafka::error::KafkaError;
use crate::kafka::group::{ConsumerGroup, GroupSnapshot};
use crate::kafka::limits::RecordLimits;
use crate::kafka::metadata::MetadataSnapshot;
use crate::kafka::record::RecordPage;
use crate::kafka::record::page::{fetch_one_page, fill_filtered_page};
use crate::kafka::record::plan::apply_timestamp_bounds;
use crate::kafka::record::query::RecordQuery;
use crate::kafka::registry::SchemaSubject;
use crate::kafka::session::ClusterSession;
use crate::kafka::topic::{Topic, groups_for_topic};
use crate::kafka::topic_config::ConfigEntry;
use crate::kafka::watermarks::Watermarks;

/// Assembles the catalog snapshot and serves live Kafka I/O from [`ClusterSession`]s.
pub struct QueryEngine<S: ?Sized> {
    registry: IndexMap<String, Box<S>>,
    limits: RecordLimits,
}

impl QueryEngine<dyn ClusterSession> {
    pub fn from_config(config: &Config) -> Result<Self, KafkaError> {
        Ok(Self::from_sessions(
            config
                .clusters
                .iter()
                .map(ClusterHandle::from_config)
                .collect::<Result<Vec<_>, _>>()?,
        ))
    }

    pub fn from_sessions(sessions: Vec<impl ClusterSession>) -> Self {
        let mut registry = IndexMap::with_capacity(sessions.len());
        for session in sessions {
            registry.insert(
                session.identity().name.clone(),
                Box::new(session) as Box<dyn ClusterSession>,
            );
        }

        Self {
            registry,
            limits: RecordLimits::from_env(),
        }
    }
}

impl<S: ClusterSession + ?Sized> QueryEngine<S> {
    pub fn names(&self) -> Vec<&str> {
        self.registry.keys().map(String::as_str).collect()
    }

    pub fn identities(&self) -> Vec<ClusterIdentity> {
        self.registry
            .values()
            .map(|session| session.identity().clone())
            .collect()
    }

    pub fn session(&self, name: &str) -> Result<&S, KafkaError> {
        self.registry
            .get(name)
            .map(Box::as_ref)
            .ok_or_else(|| KafkaError::UnknownCluster(name.to_owned()))
    }

    pub async fn broker_configs(
        &self,
        cluster: &str,
        id: i32,
    ) -> Result<Vec<ConfigEntry>, KafkaError> {
        let session = self.session(cluster)?;
        if session.metadata().await?.broker(id).is_none() {
            return Err(KafkaError::UnknownBroker {
                cluster: cluster.to_owned(),
                id,
            });
        }
        session.broker_configs(id).await
    }

    pub async fn catalog(&self, cluster: &str) -> Result<ClusterSnapshot, KafkaError> {
        Ok(self.catalog_from(cluster, None, true).await?.snapshot)
    }

    pub async fn catalog_from(
        &self,
        cluster: &str,
        reuse: Option<&CatalogReuse>,
        fetch_configs: bool,
    ) -> Result<CatalogAssemble, KafkaError> {
        let session = self.session(cluster)?;
        let meta = session.metadata().await?;
        let names = meta.topic_names();
        let mut groups = session.consumer_groups().await?;
        let metadata_hash = metadata_lane_hash(&meta, &groups);
        let watermark_names = catalog_watermark_names(&names, &groups);
        let watermark_refs: Vec<&str> = watermark_names.iter().map(String::as_str).collect();

        let watermarks_fut = session.watermarks_many(&watermark_refs);
        let hydrate = Self::hydrate_committed_offsets(session, &mut groups);
        let (configs, fetched_configs, watermarks) = if fetch_configs {
            let configs_fut = session.topics_configs(&names);
            let (configs, watermarks, ()) = tokio::join!(configs_fut, watermarks_fut, hydrate);
            match configs {
                Ok(configs) => (configs, true, watermarks),
                Err(_) => (
                    reuse.map(|lane| lane.configs.clone()).unwrap_or_default(),
                    false,
                    watermarks,
                ),
            }
        } else {
            let (watermarks, ()) = tokio::join!(watermarks_fut, hydrate);
            (
                reuse.map(|lane| lane.configs.clone()).unwrap_or_default(),
                false,
                watermarks,
            )
        };
        let ends = ends_from_watermarks(&watermarks);

        if let Some(previous) = reuse.and_then(|lane| {
            (lane.metadata_hash == metadata_hash)
                .then_some(lane.snapshot.as_ref())
                .flatten()
        }) {
            let topics = previous
                .topics
                .iter()
                .map(|topic| {
                    topic
                        .with_watermarks(watermarks.get(&topic.name).unwrap_or(&HashMap::new()))
                        .with_config(configs.get(&topic.name).map(Vec::as_slice))
                })
                .collect();
            let groups = groups
                .iter()
                .map(|group| ConsumerGroup::assemble(group, &ends))
                .collect();
            return Ok(CatalogAssemble {
                snapshot: ClusterSnapshot::assemble(
                    topics,
                    groups,
                    previous.brokers.clone(),
                    previous.overview.clone(),
                ),
                metadata_hash,
                configs,
                fetched_configs,
                reused_topology: true,
            });
        }

        let topics = meta
            .topics
            .iter()
            .map(|topic| {
                Topic::assemble(
                    topic,
                    watermarks.get(&topic.name).unwrap_or(&HashMap::new()),
                    configs.get(&topic.name).map(Vec::as_slice),
                    groups_for_topic(&topic.name, &groups),
                )
            })
            .collect();
        let group_count = groups.len() as i32;
        let groups = groups
            .iter()
            .map(|group| ConsumerGroup::assemble(group, &ends))
            .collect();
        let brokers = Broker::assemble_all(&meta);
        let overview = ClusterOverview::assemble(session.identity().clone(), &meta, group_count);

        Ok(CatalogAssemble {
            snapshot: ClusterSnapshot::assemble(topics, groups, brokers, overview),
            metadata_hash,
            configs,
            fetched_configs,
            reused_topology: false,
        })
    }

    pub async fn topic_configs(
        &self,
        cluster: &str,
        name: &str,
    ) -> Result<Vec<ConfigEntry>, KafkaError> {
        let session = self.session(cluster)?;
        if session.metadata().await?.topic(name).is_none() {
            return Err(KafkaError::UnknownTopic {
                cluster: cluster.to_owned(),
                topic: name.to_owned(),
            });
        }

        session
            .topics_configs(&[name])
            .await
            .map(|mut configs| configs.remove(name).unwrap_or_default())
    }

    async fn hydrate_committed_offsets(session: &S, groups: &mut [GroupSnapshot]) {
        for chunk in groups.chunks_mut(*OFFSET_FETCH_BATCH) {
            let fetches = chunk.iter().enumerate().filter_map(|(offset, group)| {
                let partitions = group.assigned_partitions();
                if partitions.is_empty() {
                    return None;
                }

                let group_id = group.id.clone();
                Some(async move {
                    let committed = session
                        .committed_offsets(&group_id, &partitions)
                        .await
                        .unwrap_or_default();
                    (offset, committed)
                })
            });

            for (offset, committed) in join_all(fetches).await {
                chunk[offset].committed = committed;
            }
        }
    }

    pub async fn consumer_group(
        &self,
        cluster: &str,
        id: &str,
    ) -> Result<ConsumerGroup, KafkaError> {
        let session = self.session(cluster)?;
        let mut snapshot = session.consumer_group(id).await?;
        Self::hydrate_committed_offsets(session, std::slice::from_mut(&mut snapshot)).await;
        let ends = Self::end_offsets(session, std::slice::from_ref(&snapshot)).await;
        Ok(ConsumerGroup::assemble(&snapshot, &ends))
    }

    async fn end_offsets(session: &S, groups: &[GroupSnapshot]) -> HashMap<(String, i32), i64> {
        let names = GroupSnapshot::consumed_topic_names(groups);
        let topics: Vec<&str> = names.iter().map(String::as_str).collect();
        ends_from_watermarks(&session.watermarks_many(&topics).await)
    }

    pub async fn records(
        &self,
        cluster: &str,
        query: RecordQuery,
    ) -> Result<RecordPage, KafkaError> {
        let session = self.session(cluster)?;
        let partitions = self.resolve_partitions(cluster, session, &query).await?;

        let limit = self.limits.clamp_limit(query.limit)?;
        query.timestamps.validate()?;

        let watermarks = Self::window_watermarks(session, &query, &partitions).await?;

        if query.filter.is_some() {
            fill_filtered_page(
                session,
                &query,
                &partitions,
                &watermarks,
                limit,
                self.limits,
            )
            .await
        } else {
            fetch_one_page(
                session,
                &query,
                &partitions,
                &watermarks,
                limit,
                self.limits,
            )
            .await
        }
    }

    async fn resolve_partitions(
        &self,
        cluster: &str,
        session: &S,
        query: &RecordQuery,
    ) -> Result<Vec<i32>, KafkaError> {
        let meta = session.metadata().await?;
        let topic = meta
            .topic(&query.topic)
            .ok_or_else(|| KafkaError::UnknownTopic {
                cluster: cluster.to_owned(),
                topic: query.topic.clone(),
            })?;

        match query.partition {
            Some(id) if topic.partition(id).is_none() => Err(KafkaError::UnknownPartition {
                cluster: cluster.to_owned(),
                topic: query.topic.clone(),
                partition: id,
            }),
            Some(id) => Ok(vec![id]),
            None => Ok(topic.partition_ids()),
        }
    }

    async fn window_watermarks(
        session: &S,
        query: &RecordQuery,
        partitions: &[i32],
    ) -> Result<HashMap<i32, Watermarks>, KafkaError> {
        let mut watermarks = session.watermarks(&query.topic).await?;
        watermarks.retain(|partition, _| partitions.contains(partition));

        let start = query.timestamps.start_seek();
        let end = query.timestamps.end_seek();
        if start.is_none() && end.is_none() {
            return Ok(watermarks);
        }

        let seek = |timestamp: Option<i64>| async move {
            match timestamp {
                Some(timestamp) => session
                    .offsets_for_times(&query.topic, partitions, timestamp)
                    .await
                    .map(Some),
                None => Ok(None),
            }
        };
        let (from_offsets, to_offsets) = tokio::try_join!(seek(start), seek(end))?;

        apply_timestamp_bounds(&mut watermarks, from_offsets.as_ref(), to_offsets.as_ref());
        Ok(watermarks)
    }

    pub async fn schema_subjects(&self, cluster: &str) -> Result<Vec<SchemaSubject>, KafkaError> {
        self.session(cluster)?.schema_subjects().await
    }
}

fn metadata_lane_hash(meta: &MetadataSnapshot, groups: &[GroupSnapshot]) -> u64 {
    use std::hash::{Hash, Hasher};

    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    meta.cluster_id.hash(&mut hasher);
    for broker in &meta.brokers {
        broker.id.hash(&mut hasher);
        broker.host.hash(&mut hasher);
        broker.port.hash(&mut hasher);
    }
    for topic in &meta.topics {
        topic.name.hash(&mut hasher);
        topic.internal.hash(&mut hasher);
        for partition in &topic.partitions {
            partition.id.hash(&mut hasher);
            partition.leader.hash(&mut hasher);
            partition.replicas.hash(&mut hasher);
            partition.isr.hash(&mut hasher);
        }
    }
    for group in groups {
        group.id.hash(&mut hasher);
        std::mem::discriminant(&group.state).hash(&mut hasher);
        group.protocol.hash(&mut hasher);
        group.coordinator.hash(&mut hasher);
        for member in &group.members {
            member.id.hash(&mut hasher);
            member.client_id.hash(&mut hasher);
            member.host.hash(&mut hasher);
            for assignment in &member.assignments {
                assignment.topic.hash(&mut hasher);
                assignment.partitions.hash(&mut hasher);
            }
        }
    }
    hasher.finish()
}

fn catalog_watermark_names(topic_names: &[&str], groups: &[GroupSnapshot]) -> Vec<String> {
    let mut names: Vec<String> = topic_names.iter().map(|name| (*name).to_owned()).collect();
    names.extend(GroupSnapshot::consumed_topic_names(groups));
    names.sort();
    names.dedup();
    names
}

fn ends_from_watermarks(
    watermarks: &HashMap<String, HashMap<i32, Watermarks>>,
) -> HashMap<(String, i32), i64> {
    let mut ends = HashMap::new();
    for (topic, marks) in watermarks {
        for (partition, Watermarks { high, .. }) in marks {
            ends.insert((topic.clone(), *partition), *high);
        }
    }
    ends
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ops::Bound;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use async_trait::async_trait;

    use crate::config::{ClusterConfig, Config};
    use crate::kafka::error::KafkaError;
    use crate::kafka::model::{
        ClusterIdentity, CommittedOffset, ConfigEntry, FetchPlan, MetadataSnapshot, Record,
        RecordOrder, RecordQuery, TimestampRange, Watermarks, unix_datetime,
    };
    use crate::kafka::testing::FakeCluster;

    fn cluster_config(name: &str) -> ClusterConfig {
        ClusterConfig {
            name: name.to_owned(),
            bootstrap_servers: vec!["localhost:9092".to_owned()],
            security: None,
            schema_registry: None,
            properties: HashMap::new(),
        }
    }

    #[derive(Clone)]
    struct Probe {
        inner: FakeCluster,
        delay: Duration,
        watermark_delay: Duration,
        group_lists: Arc<AtomicUsize>,
        committed: Arc<AtomicUsize>,
    }

    impl Probe {
        fn new(inner: FakeCluster, delay: Duration) -> Self {
            Self {
                inner,
                delay,
                watermark_delay: Duration::ZERO,
                group_lists: Arc::new(AtomicUsize::new(0)),
                committed: Arc::new(AtomicUsize::new(0)),
            }
        }

        fn with_watermark_delay(inner: FakeCluster, delay: Duration) -> Self {
            Self {
                inner,
                delay: Duration::ZERO,
                watermark_delay: delay,
                group_lists: Arc::new(AtomicUsize::new(0)),
                committed: Arc::new(AtomicUsize::new(0)),
            }
        }
    }

    #[async_trait]
    impl ClusterSession for Probe {
        fn identity(&self) -> &ClusterIdentity {
            self.inner.identity()
        }

        async fn metadata(&self) -> Result<MetadataSnapshot, KafkaError> {
            if !self.delay.is_zero() {
                tokio::time::sleep(self.delay).await;
            }
            self.inner.metadata().await
        }

        async fn watermarks(&self, topic: &str) -> Result<HashMap<i32, Watermarks>, KafkaError> {
            if !self.watermark_delay.is_zero() {
                tokio::time::sleep(self.watermark_delay).await;
            }
            self.inner.watermarks(topic).await
        }

        async fn offsets_for_times(
            &self,
            topic: &str,
            partitions: &[i32],
            timestamp: i64,
        ) -> Result<HashMap<i32, Option<i64>>, KafkaError> {
            self.inner
                .offsets_for_times(topic, partitions, timestamp)
                .await
        }

        async fn topics_configs(
            &self,
            topics: &[&str],
        ) -> Result<HashMap<String, Vec<ConfigEntry>>, KafkaError> {
            self.inner.topics_configs(topics).await
        }

        async fn broker_configs(&self, broker_id: i32) -> Result<Vec<ConfigEntry>, KafkaError> {
            self.inner.broker_configs(broker_id).await
        }

        async fn consumer_groups(&self) -> Result<Vec<GroupSnapshot>, KafkaError> {
            self.group_lists.fetch_add(1, Ordering::SeqCst);
            if !self.delay.is_zero() {
                tokio::time::sleep(self.delay).await;
            }
            self.inner.consumer_groups().await
        }

        async fn committed_offsets(
            &self,
            group_id: &str,
            partitions: &[(String, i32)],
        ) -> Result<Vec<CommittedOffset>, KafkaError> {
            self.committed.fetch_add(1, Ordering::SeqCst);
            self.inner.committed_offsets(group_id, partitions).await
        }

        async fn records(&self, plan: &FetchPlan) -> Result<Vec<Record>, KafkaError> {
            self.inner.records(plan).await
        }
    }

    #[derive(Clone)]
    struct CountingMany {
        inner: FakeCluster,
        many: Arc<AtomicUsize>,
    }

    impl CountingMany {
        fn new(inner: FakeCluster) -> Self {
            Self {
                inner,
                many: Arc::new(AtomicUsize::new(0)),
            }
        }
    }

    #[async_trait]
    impl ClusterSession for CountingMany {
        fn identity(&self) -> &ClusterIdentity {
            self.inner.identity()
        }

        async fn metadata(&self) -> Result<MetadataSnapshot, KafkaError> {
            self.inner.metadata().await
        }

        async fn watermarks_many(
            &self,
            topics: &[&str],
        ) -> HashMap<String, HashMap<i32, Watermarks>> {
            self.many.fetch_add(1, Ordering::SeqCst);
            self.inner.watermarks_many(topics).await
        }

        async fn offsets_for_times(
            &self,
            topic: &str,
            partitions: &[i32],
            timestamp: i64,
        ) -> Result<HashMap<i32, Option<i64>>, KafkaError> {
            self.inner
                .offsets_for_times(topic, partitions, timestamp)
                .await
        }

        async fn topics_configs(
            &self,
            topics: &[&str],
        ) -> Result<HashMap<String, Vec<ConfigEntry>>, KafkaError> {
            self.inner.topics_configs(topics).await
        }

        async fn broker_configs(&self, broker_id: i32) -> Result<Vec<ConfigEntry>, KafkaError> {
            self.inner.broker_configs(broker_id).await
        }

        async fn consumer_groups(&self) -> Result<Vec<GroupSnapshot>, KafkaError> {
            self.inner.consumer_groups().await
        }

        async fn committed_offsets(
            &self,
            group_id: &str,
            partitions: &[(String, i32)],
        ) -> Result<Vec<CommittedOffset>, KafkaError> {
            self.inner.committed_offsets(group_id, partitions).await
        }

        async fn records(&self, plan: &FetchPlan) -> Result<Vec<Record>, KafkaError> {
            self.inner.records(plan).await
        }
    }

    #[test]
    fn from_config_keeps_cluster_order() {
        let engine = QueryEngine::from_config(&Config {
            bind: "127.0.0.1:8080".parse().unwrap(),
            log_level: "info".into(),
            clusters: vec![cluster_config("b"), cluster_config("a")],
            auth: None,
        })
        .unwrap();

        assert_eq!(engine.names(), vec!["b", "a"]);
    }

    #[test]
    fn identities_keep_config_order() {
        let engine = QueryEngine::from_sessions(vec![
            FakeCluster::named("prod"),
            FakeCluster::named("staging"),
        ]);

        let names: Vec<_> = engine
            .identities()
            .into_iter()
            .map(|identity| identity.name)
            .collect();

        assert_eq!(names, vec!["prod", "staging"]);
    }

    #[tokio::test]
    async fn catalog_includes_partition_watermarks() {
        let engine = QueryEngine::from_sessions(vec![FakeCluster::local()]);
        let topics = engine.catalog("local").await.unwrap().topics;
        assert_eq!(topics.len(), 1);
        assert_eq!(topics[0].name, "orders.created");
        assert_eq!(topics[0].message_count, 16);
        assert_eq!(topics[0].partitions[0].low_watermark, 0);
        assert_eq!(topics[0].partitions[0].high_watermark, 8);
    }

    #[tokio::test]
    async fn catalog_message_counts_sum_high_watermarks() {
        let engine = QueryEngine::from_sessions(vec![FakeCluster::local()]);
        let counts = engine.catalog("local").await.unwrap().message_counts();
        assert_eq!(counts.get("orders.created"), Some(&16));
    }

    #[tokio::test]
    async fn consumer_group_hydrates_only_the_requested_group() {
        let payments = GroupSnapshot {
            id: "payments-processor".into(),
            state: crate::kafka::group::GroupState::Stable,
            protocol: "range".into(),
            coordinator: 1,
            members: vec![crate::kafka::group::GroupMember {
                id: "m-pay".into(),
                client_id: "payments".into(),
                host: "127.0.0.1".into(),
                assignments: vec![crate::kafka::group::MemberAssignment {
                    topic: "payments.captured".into(),
                    partitions: vec![0],
                }],
            }],
            committed: Vec::new(),
        };
        let cluster = FakeCluster::local()
            .extra_topic("payments.captured", 1, 4)
            .extra_group(payments);
        let probe = Probe::new(cluster, Duration::ZERO);
        let engine = QueryEngine::from_sessions(vec![probe.clone()]);

        let group = engine
            .consumer_group("local", "order-processor")
            .await
            .unwrap();
        assert_eq!(group.id, "order-processor");
        assert_eq!(probe.committed.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn catalog_fetches_watermarks_in_parallel() {
        let cluster = FakeCluster::local().extra_topic("payments.captured", 1, 4);
        let probe = Probe::with_watermark_delay(cluster, Duration::from_secs(1));
        let engine = QueryEngine::from_sessions(vec![probe]);

        let started = tokio::time::Instant::now();
        let mut topics = engine.catalog("local").await.unwrap().topics;
        topics.sort_by(|left, right| left.name.cmp(&right.name));

        assert_eq!(started.elapsed(), Duration::from_secs(1));
        assert_eq!(
            topics
                .iter()
                .map(|topic| (topic.name.as_str(), topic.message_count))
                .collect::<Vec<_>>(),
            vec![("orders.created", 16), ("payments.captured", 4)]
        );
    }

    #[tokio::test]
    async fn schema_subjects_come_from_the_cluster_session() {
        let engine = QueryEngine::from_sessions(vec![FakeCluster::local()]);
        let subjects = engine.schema_subjects("local").await.unwrap();

        assert_eq!(subjects.len(), 1);
        assert_eq!(subjects[0].subject, "orders.created-value");
    }

    #[tokio::test]
    async fn schema_subjects_default_to_empty_when_session_does_not_override() {
        let probe = Probe::new(FakeCluster::local(), Duration::ZERO);
        let engine = QueryEngine::from_sessions(vec![probe.clone()]);

        assert!(engine.schema_subjects("local").await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn catalog_search_includes_schema_subjects() {
        let engine = QueryEngine::from_sessions(vec![FakeCluster::local()]);
        let snapshot = engine.catalog("local").await.unwrap();
        let subjects = engine.schema_subjects("local").await.unwrap();
        let hits = snapshot.search("order", &subjects);

        assert!(
            hits.iter()
                .any(|hit| hit.kind == crate::kafka::model::SearchKind::Subject
                    && hit.id == "orders.created-value")
        );
    }

    #[tokio::test]
    async fn catalog_calls_watermarks_many_once() {
        let session = CountingMany::new(FakeCluster::local());
        let engine = QueryEngine::from_sessions(vec![session.clone()]);

        let snapshot = engine.catalog("local").await.unwrap();
        assert_eq!(session.many.load(Ordering::SeqCst), 1);
        assert_eq!(snapshot.topics[0].message_count, 16);
        assert_eq!(snapshot.message_counts().get("orders.created"), Some(&16));
    }

    #[derive(Clone)]
    struct CatalogIo {
        inner: FakeCluster,
        metadata: Arc<AtomicUsize>,
        groups: Arc<AtomicUsize>,
        watermarks: Arc<AtomicUsize>,
        configs: Arc<AtomicUsize>,
        committed: Arc<AtomicUsize>,
    }

    impl CatalogIo {
        fn new(inner: FakeCluster) -> Self {
            Self {
                inner,
                metadata: Arc::new(AtomicUsize::new(0)),
                groups: Arc::new(AtomicUsize::new(0)),
                watermarks: Arc::new(AtomicUsize::new(0)),
                configs: Arc::new(AtomicUsize::new(0)),
                committed: Arc::new(AtomicUsize::new(0)),
            }
        }
    }

    #[async_trait]
    impl ClusterSession for CatalogIo {
        fn identity(&self) -> &ClusterIdentity {
            self.inner.identity()
        }

        async fn metadata(&self) -> Result<MetadataSnapshot, KafkaError> {
            self.metadata.fetch_add(1, Ordering::SeqCst);
            self.inner.metadata().await
        }

        async fn watermarks_many(
            &self,
            topics: &[&str],
        ) -> HashMap<String, HashMap<i32, Watermarks>> {
            self.watermarks.fetch_add(1, Ordering::SeqCst);
            self.inner.watermarks_many(topics).await
        }

        async fn offsets_for_times(
            &self,
            topic: &str,
            partitions: &[i32],
            timestamp: i64,
        ) -> Result<HashMap<i32, Option<i64>>, KafkaError> {
            self.inner
                .offsets_for_times(topic, partitions, timestamp)
                .await
        }

        async fn topics_configs(
            &self,
            topics: &[&str],
        ) -> Result<HashMap<String, Vec<ConfigEntry>>, KafkaError> {
            self.configs.fetch_add(1, Ordering::SeqCst);
            self.inner.topics_configs(topics).await
        }

        async fn broker_configs(&self, broker_id: i32) -> Result<Vec<ConfigEntry>, KafkaError> {
            self.inner.broker_configs(broker_id).await
        }

        async fn consumer_groups(&self) -> Result<Vec<GroupSnapshot>, KafkaError> {
            self.groups.fetch_add(1, Ordering::SeqCst);
            self.inner.consumer_groups().await
        }

        async fn committed_offsets(
            &self,
            group_id: &str,
            partitions: &[(String, i32)],
        ) -> Result<Vec<CommittedOffset>, KafkaError> {
            self.committed.fetch_add(1, Ordering::SeqCst);
            self.inner.committed_offsets(group_id, partitions).await
        }

        async fn records(&self, plan: &FetchPlan) -> Result<Vec<Record>, KafkaError> {
            self.inner.records(plan).await
        }
    }

    #[tokio::test]
    async fn catalog_assembles_topics_groups_brokers_and_overview() {
        let engine = QueryEngine::from_sessions(vec![FakeCluster::local()]);
        let snapshot = engine.catalog("local").await.unwrap();

        assert_eq!(snapshot.topics[0].name, "orders.created");
        assert_eq!(snapshot.topics[0].message_count, 16);
        assert_eq!(snapshot.groups[0].id, "order-processor");
        assert_eq!(snapshot.brokers[0].id, 1);
        assert_eq!(snapshot.overview.identity.name, "local");
        assert_eq!(snapshot.overview.topic_count, 1);
        assert_eq!(snapshot.overview.consumer_group_count, 1);
    }

    #[tokio::test]
    async fn catalog_fetches_shared_inputs_once() {
        let session = CatalogIo::new(FakeCluster::local());
        let engine = QueryEngine::from_sessions(vec![session.clone()]);
        let snapshot = engine.catalog("local").await.unwrap();

        assert_eq!(snapshot.topics[0].name, "orders.created");
        assert_eq!(snapshot.topics[0].message_count, 16);
        assert_eq!(snapshot.groups[0].id, "order-processor");
        assert_eq!(snapshot.groups[0].lag, 5);
        assert_eq!(session.metadata.load(Ordering::SeqCst), 1);
        assert_eq!(session.groups.load(Ordering::SeqCst), 1);
        assert_eq!(session.watermarks.load(Ordering::SeqCst), 1);
        assert_eq!(session.configs.load(Ordering::SeqCst), 1);
        assert_eq!(session.committed.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn catalog_from_skips_config_fetch_when_disabled() {
        let session = CatalogIo::new(FakeCluster::local());
        let engine = QueryEngine::from_sessions(vec![session.clone()]);
        let first = engine.catalog_from("local", None, true).await.unwrap();
        assert!(first.fetched_configs);
        assert!(!first.reused_topology);
        assert_eq!(session.configs.load(Ordering::SeqCst), 1);

        let reuse = CatalogReuse {
            metadata_hash: first.metadata_hash,
            configs: first.configs.clone(),
            snapshot: Some(Arc::new(first.snapshot.clone())),
        };
        let second = engine
            .catalog_from("local", Some(&reuse), false)
            .await
            .unwrap();
        assert!(!second.fetched_configs);
        assert!(second.reused_topology);
        assert_eq!(session.configs.load(Ordering::SeqCst), 1);
        assert_eq!(session.watermarks.load(Ordering::SeqCst), 2);
        assert!(second.snapshot.body_eq(&first.snapshot));
    }

    #[tokio::test]
    async fn catalog_from_keeps_reused_configs_when_fetch_fails() {
        let session = CatalogIo::new(FakeCluster::local().with_configs_error("no configs"));
        let engine = QueryEngine::from_sessions(vec![session.clone()]);
        let good = QueryEngine::from_sessions(vec![FakeCluster::local()])
            .catalog_from("local", None, true)
            .await
            .unwrap();
        let reuse = CatalogReuse {
            metadata_hash: 0,
            configs: good.configs.clone(),
            snapshot: None,
        };
        let assembled = engine
            .catalog_from("local", Some(&reuse), true)
            .await
            .unwrap();
        assert!(!assembled.fetched_configs);
        assert_eq!(assembled.configs, good.configs);
        assert_eq!(session.configs.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn catalog_from_reuses_topology_only_when_hash_matches() {
        let engine = QueryEngine::from_sessions(vec![FakeCluster::local()]);
        let first = engine.catalog_from("local", None, true).await.unwrap();
        let reuse = CatalogReuse {
            metadata_hash: first.metadata_hash,
            configs: first.configs.clone(),
            snapshot: Some(Arc::new(first.snapshot.clone())),
        };
        let second = engine
            .catalog_from("local", Some(&reuse), false)
            .await
            .unwrap();
        assert!(second.reused_topology);
        assert_eq!(second.snapshot.brokers, first.snapshot.brokers);

        let miss = CatalogReuse {
            metadata_hash: first.metadata_hash.wrapping_add(1),
            configs: first.configs.clone(),
            snapshot: Some(Arc::new(first.snapshot.clone())),
        };
        let third = engine
            .catalog_from("local", Some(&miss), false)
            .await
            .unwrap();
        assert!(!third.reused_topology);
    }

    #[tokio::test]
    async fn catalog_from_applies_new_configs_when_topology_is_reused() {
        let first = QueryEngine::from_sessions(vec![FakeCluster::local()])
            .catalog_from("local", None, true)
            .await
            .unwrap();
        assert_eq!(first.snapshot.topics[0].retention_ms, 604_800_000);

        let reuse = CatalogReuse {
            metadata_hash: first.metadata_hash,
            configs: first.configs.clone(),
            snapshot: Some(Arc::new(first.snapshot.clone())),
        };
        let second = QueryEngine::from_sessions(vec![FakeCluster::local().with_topic_configs(
            "orders.created",
            vec![
                ConfigEntry {
                    name: "cleanup.policy".into(),
                    value: Some("compact".into()),
                    source: crate::kafka::model::ConfigSource::Default,
                    read_only: false,
                    sensitive: false,
                },
                ConfigEntry {
                    name: "retention.ms".into(),
                    value: Some("1000".into()),
                    source: crate::kafka::model::ConfigSource::Default,
                    read_only: false,
                    sensitive: false,
                },
            ],
        )])
        .catalog_from("local", Some(&reuse), true)
        .await
        .unwrap();

        assert!(second.reused_topology);
        assert!(second.fetched_configs);
        assert_eq!(
            second.snapshot.topics[0].cleanup_policy,
            crate::kafka::model::CleanupPolicy::Compact
        );
        assert_eq!(second.snapshot.topics[0].retention_ms, 1000);
        assert_eq!(second.snapshot.brokers, first.snapshot.brokers);
    }

    #[tokio::test]
    async fn catalog_unknown_cluster_is_an_error() {
        let engine = QueryEngine::from_sessions(vec![FakeCluster::local()]);
        let error = engine.catalog("missing").await.unwrap_err();
        assert!(matches!(error, KafkaError::UnknownCluster(name) if name == "missing"));
    }

    fn browse_query() -> RecordQuery {
        RecordQuery {
            topic: "orders.created".into(),
            partition: None,
            filter: None,
            timestamps: TimestampRange::default(),
            limit: 50,
            order: RecordOrder::Oldest,
            cursor: None,
            schema_id: None,
        }
    }

    fn browse_record(partition: i32, offset: i64, timestamp: i64) -> Record {
        Record {
            topic: "orders.created".into(),
            partition,
            offset,
            timestamp,
            key: Some(format!("p{partition}-{offset}")),
            value: None,
            schema_id: None,
            headers: Vec::new(),
            size_bytes: 0,
            compression: crate::kafka::record::Compression::None,
        }
    }

    #[tokio::test]
    async fn all_partitions_newest_stays_timestamp_ordered_across_cursors() {
        let evening = 1_700_080_000_000;
        let morning = 1_700_037_000_000;
        let mut records = Vec::new();
        for offset in 0..10 {
            records.push(browse_record(0, offset, evening + offset * 1_000));
            records.push(browse_record(1, offset, morning + offset * 1_000));
        }

        let engine =
            QueryEngine::from_sessions(vec![FakeCluster::local().with_orders_records(records)]);
        let mut query = browse_query();
        query.limit = 5;
        query.order = RecordOrder::Newest;

        let mut seen = Vec::new();
        for _ in 0..8 {
            let page = engine.records("local", query.clone()).await.unwrap();
            assert!(!page.records.is_empty());
            seen.extend(
                page.records
                    .iter()
                    .map(|record| (record.partition, record.timestamp)),
            );
            if !page.has_more {
                break;
            }
            query.cursor = Some(
                crate::kafka::RecordCursor::parse(page.next_cursor.as_deref().unwrap()).unwrap(),
            );
        }

        let timestamps: Vec<_> = seen.iter().map(|(_, timestamp)| *timestamp).collect();
        let mut newest_first = timestamps.clone();
        newest_first.sort_by(|left, right| right.cmp(left));
        assert_eq!(timestamps, newest_first);

        let first_morning = seen.iter().position(|(partition, _)| *partition == 1);
        let last_evening = seen.iter().rposition(|(partition, _)| *partition == 0);
        assert!(first_morning.is_some() && last_evening.is_some());
        assert!(
            first_morning.unwrap() > last_evening.unwrap(),
            "partition 1 morning records must not appear before remaining partition 0 evening records"
        );
    }

    #[tokio::test]
    async fn records_filter_by_timestamp_range() {
        let engine = QueryEngine::from_sessions(vec![FakeCluster::local()]);
        let mut query = browse_query();
        query.timestamps = TimestampRange::from_bounds(
            unix_datetime(1_700_000_000_000 + 3_000)..=unix_datetime(1_700_000_000_000 + 5_000),
        );

        let page = engine.records("local", query).await.unwrap();
        let keys: Vec<_> = page
            .records
            .iter()
            .map(|record| record.key.as_deref())
            .collect();

        assert_eq!(keys, vec![Some("ord_3"), Some("ord_4"), Some("ord_5")]);
        assert!(!page.has_more);
    }

    #[tokio::test]
    async fn filtered_records_fill_the_requested_limit() {
        let mut records = Vec::new();
        for offset in 0..500 {
            let key = if offset % 40 == 0 {
                format!("hit-{offset}")
            } else {
                format!("miss-{offset}")
            };
            records.push(Record {
                topic: "orders.created".into(),
                partition: 0,
                offset,
                timestamp: offset,
                key: Some(key),
                value: None,
                schema_id: None,
                headers: Vec::new(),
                size_bytes: 0,
                compression: crate::kafka::record::Compression::None,
            });
        }

        let engine =
            QueryEngine::from_sessions(vec![FakeCluster::local().with_orders_records(records)]);
        let mut query = browse_query();
        query.limit = 10;
        query.order = RecordOrder::Newest;
        query.filter =
            crate::kafka::compile_record_filter(r#"keyText.lowerAscii().contains("hit-")"#)
                .unwrap();

        let page = engine.records("local", query.clone()).await.unwrap();
        assert_eq!(
            page.records.len(),
            10,
            "filter must keep scanning until the page limit is filled; got {:?}",
            page.records
                .iter()
                .map(|record| record.key.as_deref())
                .collect::<Vec<_>>()
        );
        assert!(page.has_more);
        let keys: Vec<_> = page
            .records
            .iter()
            .map(|record| record.key.as_deref())
            .collect();
        assert_eq!(
            keys,
            vec![
                Some("hit-480"),
                Some("hit-440"),
                Some("hit-400"),
                Some("hit-360"),
                Some("hit-320"),
                Some("hit-280"),
                Some("hit-240"),
                Some("hit-200"),
                Some("hit-160"),
                Some("hit-120"),
            ]
        );

        query.cursor =
            Some(crate::kafka::RecordCursor::parse(page.next_cursor.as_deref().unwrap()).unwrap());
        let page_two = engine.records("local", query).await.unwrap();
        let page_two_keys: Vec<_> = page_two
            .records
            .iter()
            .map(|record| record.key.as_deref())
            .collect();
        assert_eq!(
            page_two_keys,
            vec![Some("hit-80"), Some("hit-40"), Some("hit-0")]
        );
        assert!(!page_two.has_more);
    }

    #[tokio::test]
    async fn records_timestamp_from_after_the_log_is_empty() {
        let engine = QueryEngine::from_sessions(vec![FakeCluster::local()]);
        let mut query = browse_query();
        query.timestamps = TimestampRange::from_bounds(unix_datetime(1_800_000_000_000)..);

        let page = engine.records("local", query).await.unwrap();
        assert!(page.records.is_empty());
        assert!(!page.has_more);
    }

    #[tokio::test]
    async fn records_reject_timestamp_from_after_to() {
        let engine = QueryEngine::from_sessions(vec![FakeCluster::local()]);
        let mut query = browse_query();
        query.timestamps = TimestampRange::from_bounds((
            Bound::Included(unix_datetime(2)),
            Bound::Included(unix_datetime(1)),
        ));

        let error = engine.records("local", query).await.unwrap_err();
        assert!(error.to_string().contains("timestampFrom"));
    }
}
