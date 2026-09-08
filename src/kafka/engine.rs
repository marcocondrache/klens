use std::collections::HashMap;

use futures::future::join_all;
use indexmap::IndexMap;
use tokio::time::timeout;

use crate::config::Config;
use crate::environment::{OFFSET_FETCH_BATCH, OVERVIEW_BUDGET};
use crate::kafka::catalog::{
    apply_timestamp_bounds, assemble_brokers, assemble_group, assemble_overview, assemble_topic,
    clamp_record_limit, clamp_record_page, groups_for_topic, plan_records, search_catalog,
};
use crate::kafka::error::KafkaError;
use crate::kafka::handle::ClusterHandle;
use crate::kafka::model::{
    Broker, ClusterOverview, ConfigEntry, ConsumerGroup, GroupSnapshot, RecordPage, RecordQuery,
    SchemaSubject, SearchHit, Topic, Watermarks,
};
use crate::kafka::session::ClusterSession;

/// Answers GraphQL catalog and browse queries from [`ClusterSession`]s.
pub struct QueryEngine<S: ?Sized> {
    registry: IndexMap<String, Box<S>>,
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

        Self { registry }
    }
}

impl<S: ClusterSession + ?Sized> QueryEngine<S> {
    pub fn names(&self) -> Vec<&str> {
        self.registry.keys().map(String::as_str).collect()
    }

    pub fn session(&self, name: &str) -> Result<&S, KafkaError> {
        self.registry
            .get(name)
            .map(Box::as_ref)
            .ok_or_else(|| KafkaError::UnknownCluster(name.to_owned()))
    }

    pub async fn clusters(&self) -> Vec<ClusterOverview> {
        join_all(
            self.registry
                .values()
                .map(|session| Self::overview_for(session.as_ref())),
        )
        .await
    }

    pub async fn overview(&self, cluster: &str) -> Result<ClusterOverview, KafkaError> {
        Ok(Self::overview_for(self.session(cluster)?).await)
    }

    async fn overview_for(session: &S) -> ClusterOverview {
        match timeout(*OVERVIEW_BUDGET, Self::load_overview(session)).await {
            Ok(overview) => overview,
            Err(_) => {
                tracing::warn!(cluster = %session.identity().name, "cluster overview timed out");
                ClusterOverview::offline(session.identity().clone())
            }
        }
    }

    async fn load_overview(session: &S) -> ClusterOverview {
        let metadata = session.metadata();
        let groups = session.consumer_groups();
        match tokio::join!(metadata, groups) {
            (Ok(meta), groups) => {
                let group_count = groups.map(|groups| groups.len() as i32).unwrap_or(0);
                assemble_overview(session.identity().clone(), &meta, group_count)
            }
            (Err(error), _) => {
                tracing::warn!(cluster = %session.identity().name, %error, "cluster metadata failed");
                ClusterOverview::offline(session.identity().clone())
            }
        }
    }

    pub async fn brokers(&self, cluster: &str) -> Result<Vec<Broker>, KafkaError> {
        Ok(assemble_brokers(&self.session(cluster)?.metadata().await?))
    }

    pub async fn broker(&self, cluster: &str, id: i32) -> Result<Broker, KafkaError> {
        self.brokers(cluster)
            .await?
            .into_iter()
            .find(|broker| broker.id == id)
            .ok_or_else(|| KafkaError::UnknownBroker {
                cluster: cluster.to_owned(),
                id,
            })
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

    pub async fn topics(&self, cluster: &str) -> Result<Vec<Topic>, KafkaError> {
        let session = self.session(cluster)?;
        let meta = session.metadata().await?;
        let names = meta.topic_names();
        let configs = session.topic_configs(&names).await.unwrap_or_default();
        let groups = session.consumer_groups().await.unwrap_or_default();
        let watermarks = session.watermarks_many(&names).await;

        Ok(meta
            .topics
            .iter()
            .map(|topic| {
                assemble_topic(
                    topic,
                    watermarks.get(&topic.name).unwrap_or(&HashMap::new()),
                    configs.get(&topic.name).map(Vec::as_slice),
                    groups_for_topic(&topic.name, &groups),
                )
            })
            .collect())
    }

    pub async fn topic(&self, cluster: &str, name: &str) -> Result<Topic, KafkaError> {
        let session = self.session(cluster)?;
        let meta = session.metadata().await?;
        let topic = meta.topic(name).ok_or_else(|| KafkaError::UnknownTopic {
            cluster: cluster.to_owned(),
            topic: name.to_owned(),
        })?;
        let watermarks = session.watermarks(name).await.unwrap_or_default();
        let configs = session.topic_configs(&[name]).await.unwrap_or_default();
        let groups = session.consumer_groups().await.unwrap_or_default();

        Ok(assemble_topic(
            topic,
            &watermarks,
            configs.get(name).map(Vec::as_slice),
            groups_for_topic(name, &groups),
        ))
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
            .topic_configs(&[name])
            .await
            .map(|mut configs| configs.remove(name).unwrap_or_default())
    }

    pub async fn consumer_groups(&self, cluster: &str) -> Result<Vec<ConsumerGroup>, KafkaError> {
        let session = self.session(cluster)?;
        let mut snapshots = session.consumer_groups().await?;
        Self::hydrate_committed_offsets(session, &mut snapshots).await;
        let ends = Self::end_offsets(session, &snapshots).await;
        Ok(snapshots
            .iter()
            .map(|group| assemble_group(group, &ends))
            .collect())
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
        self.consumer_groups(cluster)
            .await?
            .into_iter()
            .find(|group| group.id == id)
            .ok_or_else(|| KafkaError::UnknownGroup {
                cluster: cluster.to_owned(),
                id: id.to_owned(),
            })
    }

    async fn end_offsets(session: &S, groups: &[GroupSnapshot]) -> HashMap<(String, i32), i64> {
        let names = GroupSnapshot::consumed_topic_names(groups);
        let topics: Vec<&str> = names.iter().map(String::as_str).collect();
        let watermarks = session.watermarks_many(&topics).await;

        let mut ends = HashMap::new();
        for (topic, marks) in watermarks {
            for (partition, Watermarks { high, .. }) in marks {
                ends.insert((topic.clone(), partition), high);
            }
        }
        ends
    }

    pub async fn topic_message_counts(
        &self,
        cluster: &str,
    ) -> Result<HashMap<String, u64>, KafkaError> {
        let session = self.session(cluster)?;
        let meta = session.metadata().await?;
        let names = meta.topic_names();
        let watermarks = session.watermarks_many(&names).await;

        Ok(watermarks
            .into_iter()
            .map(|(name, marks)| {
                let messages = marks.values().map(Watermarks::messages).sum::<u64>();
                (name, messages)
            })
            .collect())
    }

    pub async fn records(
        &self,
        cluster: &str,
        query: RecordQuery,
    ) -> Result<RecordPage, KafkaError> {
        let session = self.session(cluster)?;
        let meta = session.metadata().await?;
        let topic = meta
            .topic(&query.topic)
            .ok_or_else(|| KafkaError::UnknownTopic {
                cluster: cluster.to_owned(),
                topic: query.topic.clone(),
            })?;

        if let Some(partition) = query.partition
            && topic.partition(partition).is_none()
        {
            return Err(KafkaError::UnknownPartition {
                cluster: cluster.to_owned(),
                topic: query.topic.clone(),
                partition,
            });
        }

        let limit = clamp_record_limit(query.limit).map_err(KafkaError::InvalidQuery)?;
        let page = clamp_record_page(query.page).map_err(KafkaError::InvalidQuery)?;
        query
            .timestamps
            .validate()
            .map_err(KafkaError::InvalidQuery)?;
        let partitions: Vec<i32> = match query.partition {
            Some(id) => vec![id],
            None => topic.partition_ids(),
        };
        let mut watermarks = session.watermarks(&query.topic).await?;
        watermarks.retain(|partition, _| partitions.contains(partition));
        let start_time = query.timestamps.start_seek();
        let end_time = query.timestamps.end_seek();
        if start_time.is_some() || end_time.is_some() {
            let topic = query.topic.as_str();
            let (from_offsets, to_offsets) = tokio::try_join!(
                async {
                    match start_time {
                        Some(timestamp) => session
                            .offsets_for_times(topic, &partitions, timestamp)
                            .await
                            .map(Some),
                        None => Ok(None),
                    }
                },
                async {
                    match end_time {
                        Some(timestamp) => session
                            .offsets_for_times(topic, &partitions, timestamp)
                            .await
                            .map(Some),
                        None => Ok(None),
                    }
                },
            )?;
            apply_timestamp_bounds(&mut watermarks, from_offsets.as_ref(), to_offsets.as_ref());
        }
        let plan = plan_records(&query, &partitions, &watermarks, limit, page);
        let records = session.records(&plan).await?;
        Ok(RecordPage {
            records,
            has_more: plan.has_more,
        })
    }

    pub async fn search(&self, cluster: &str, term: &str) -> Result<Vec<SearchHit>, KafkaError> {
        let session = self.session(cluster)?;
        let meta = session.metadata().await?;
        let groups = session.consumer_groups().await.unwrap_or_default();
        let subjects = session.schema_subjects().await.unwrap_or_default();
        Ok(search_catalog(
            term,
            &meta.topics,
            &meta.brokers,
            &groups,
            &subjects,
        ))
    }

    pub async fn schema_subjects(&self, cluster: &str) -> Result<Vec<SchemaSubject>, KafkaError> {
        self.session(cluster)?.schema_subjects().await
    }
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
        ClusterHealth, ClusterIdentity, CommittedOffset, ConfigEntry, FetchPlan, MetadataSnapshot,
        Record, RecordOrder, RecordQuery, TimestampRange, Watermarks, unix_datetime,
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

        async fn topic_configs(
            &self,
            topics: &[&str],
        ) -> Result<HashMap<String, Vec<ConfigEntry>>, KafkaError> {
            self.inner.topic_configs(topics).await
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

        async fn topic_configs(
            &self,
            topics: &[&str],
        ) -> Result<HashMap<String, Vec<ConfigEntry>>, KafkaError> {
            self.inner.topic_configs(topics).await
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

    #[tokio::test]
    async fn cluster_list_keeps_config_order() {
        let engine = QueryEngine::from_sessions(vec![
            FakeCluster::named("prod"),
            FakeCluster::named("staging"),
        ]);

        let names: Vec<_> = engine
            .clusters()
            .await
            .into_iter()
            .map(|cluster| cluster.identity.name.clone())
            .collect();

        assert_eq!(names, vec!["prod", "staging"]);
    }

    #[tokio::test]
    async fn cluster_list_does_not_fetch_committed_offsets() {
        let probe = Probe::new(FakeCluster::local(), Duration::ZERO);
        let engine = QueryEngine::from_sessions(vec![probe.clone()]);

        let overviews = engine.clusters().await;
        assert_eq!(overviews[0].consumer_group_count, 1);
        assert_eq!(probe.group_lists.load(Ordering::SeqCst), 1);
        assert_eq!(probe.committed.load(Ordering::SeqCst), 0);

        engine.consumer_groups("local").await.unwrap();
        assert_eq!(probe.committed.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn topics_include_partition_watermarks() {
        let engine = QueryEngine::from_sessions(vec![FakeCluster::local()]);
        let topics = engine.topics("local").await.unwrap();
        assert_eq!(topics.len(), 1);
        assert_eq!(topics[0].name, "orders.created");
        assert_eq!(topics[0].message_count, 16);
        assert_eq!(topics[0].partitions[0].low_watermark, 0);
        assert_eq!(topics[0].partitions[0].high_watermark, 8);
    }

    #[tokio::test]
    async fn topic_message_counts_sum_high_watermarks() {
        let engine = QueryEngine::from_sessions(vec![FakeCluster::local()]);
        let counts = engine.topic_message_counts("local").await.unwrap();
        assert_eq!(counts.get("orders.created"), Some(&16));
    }

    #[tokio::test(start_paused = true)]
    async fn topics_fetch_watermarks_in_parallel() {
        let cluster = FakeCluster::local().extra_topic("payments.captured", 1, 4);
        let probe = Probe::with_watermark_delay(cluster, Duration::from_secs(1));
        let engine = QueryEngine::from_sessions(vec![probe]);

        let started = tokio::time::Instant::now();
        let mut topics = engine.topics("local").await.unwrap();
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

    #[tokio::test(start_paused = true)]
    async fn cluster_list_marks_slow_cluster_offline_without_blocking_others() {
        let slow = Probe::new(FakeCluster::named("slow"), Duration::from_secs(60));
        let fast = FakeCluster::named("fast");
        let engine = QueryEngine::from_sessions(vec![slow, Probe::new(fast, Duration::ZERO)]);

        let overviews = engine.clusters().await;
        assert_eq!(overviews[0].identity.name, "slow");
        assert_eq!(overviews[0].health, ClusterHealth::Offline);
        assert_eq!(overviews[1].identity.name, "fast");
        assert_eq!(overviews[1].health, ClusterHealth::Healthy);
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
    async fn search_includes_schema_subjects() {
        let engine = QueryEngine::from_sessions(vec![FakeCluster::local()]);
        let hits = engine.search("local", "order").await.unwrap();

        assert!(
            hits.iter()
                .any(|hit| hit.kind == crate::kafka::model::SearchKind::Subject
                    && hit.id == "orders.created-value")
        );
    }

    #[tokio::test]
    async fn topics_and_counts_call_watermarks_many_once() {
        let session = CountingMany::new(FakeCluster::local());
        let engine = QueryEngine::from_sessions(vec![session.clone()]);

        let topics = engine.topics("local").await.unwrap();
        assert_eq!(session.many.load(Ordering::SeqCst), 1);
        assert_eq!(topics[0].message_count, 16);

        let counts = engine.topic_message_counts("local").await.unwrap();
        assert_eq!(session.many.load(Ordering::SeqCst), 2);
        assert_eq!(counts.get("orders.created"), Some(&16));
    }

    fn browse_query() -> RecordQuery {
        RecordQuery {
            topic: "orders.created".into(),
            partition: None,
            search: String::new(),
            timestamps: TimestampRange::default(),
            limit: 50,
            order: RecordOrder::Oldest,
            page: 0,
        }
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
