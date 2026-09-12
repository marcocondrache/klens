use std::collections::HashMap;
use std::time::Instant;

use futures::future::join_all;
use indexmap::IndexMap;
use tokio::time::timeout;

use crate::config::Config;
use crate::environment::{OFFSET_FETCH_BATCH, OVERVIEW_BUDGET};
use crate::kafka::adapter::ClusterHandle;
use crate::kafka::broker::Broker;
use crate::kafka::cluster::ClusterOverview;
use crate::kafka::error::KafkaError;
use crate::kafka::group::{ConsumerGroup, GroupSnapshot};
use crate::kafka::limits::RecordLimits;
use crate::kafka::record::Record;
use crate::kafka::record::RecordPage;
use crate::kafka::record::cursor::RecordCursor;
use crate::kafka::record::plan::{FetchPlan, PartitionWindow, apply_timestamp_bounds, page_cursor};
use crate::kafka::record::query::RecordQuery;
use crate::kafka::registry::SchemaSubject;
use crate::kafka::search::{SearchHit, search_catalog};
use crate::kafka::session::ClusterSession;
use crate::kafka::topic::{Topic, groups_for_topic};
use crate::kafka::topic_config::ConfigEntry;
use crate::kafka::watermarks::Watermarks;

const MAX_FILTER_PASSES: usize = 64;

/// Answers GraphQL catalog and browse queries from [`ClusterSession`]s.
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
                ClusterOverview::assemble(session.identity().clone(), &meta, group_count)
            }
            (Err(error), _) => {
                tracing::warn!(cluster = %session.identity().name, %error, "cluster metadata failed");
                ClusterOverview::offline(session.identity().clone())
            }
        }
    }

    pub async fn brokers(&self, cluster: &str) -> Result<Vec<Broker>, KafkaError> {
        Ok(Broker::assemble_all(
            &self.session(cluster)?.metadata().await?,
        ))
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
        let (configs, groups, watermarks) = Self::load_topics_state(session, &names).await;

        Ok(meta
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
            .collect())
    }

    pub async fn topic(&self, cluster: &str, name: &str) -> Result<Topic, KafkaError> {
        let session = self.session(cluster)?;
        let meta = session.metadata().await?;
        let topic = meta.topic(name).ok_or_else(|| KafkaError::UnknownTopic {
            cluster: cluster.to_owned(),
            topic: name.to_owned(),
        })?;
        let (configs, groups, watermarks) = Self::load_topics_state(session, &[name]).await;

        Ok(Topic::assemble(
            topic,
            watermarks.get(name).unwrap_or(&HashMap::new()),
            configs.get(&topic.name).map(Vec::as_slice),
            groups_for_topic(name, &groups),
        ))
    }

    async fn load_topics_state(
        session: &S,
        topics: &[&str],
    ) -> (
        HashMap<String, Vec<ConfigEntry>>,
        Vec<GroupSnapshot>,
        HashMap<String, HashMap<i32, Watermarks>>,
    ) {
        let (configs, groups, watermarks) = tokio::join!(
            session.topics_configs(topics),
            session.consumer_groups(),
            session.watermarks_many(topics),
        );
        (
            configs.unwrap_or_default(),
            groups.unwrap_or_default(),
            watermarks,
        )
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

    pub async fn consumer_groups(
        &self,
        cluster: &str,
        topic: Option<&str>,
    ) -> Result<Vec<ConsumerGroup>, KafkaError> {
        let session = self.session(cluster)?;
        let mut snapshots = session.consumer_groups().await?;
        if let Some(topic) = topic {
            snapshots.retain(|group| group.consumes_topic(topic));
        }
        Self::hydrate_committed_offsets(session, &mut snapshots).await;
        let ends = Self::end_offsets(session, &snapshots).await;
        Ok(snapshots
            .iter()
            .map(|group| ConsumerGroup::assemble(group, &ends))
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
        let session = self.session(cluster)?;
        let mut snapshot = session.consumer_group(id).await?;
        Self::hydrate_committed_offsets(session, std::slice::from_mut(&mut snapshot)).await;
        let ends = Self::end_offsets(session, std::slice::from_ref(&snapshot)).await;
        Ok(ConsumerGroup::assemble(&snapshot, &ends))
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

async fn fetch_one_page<S: ClusterSession + ?Sized>(
    session: &S,
    query: &RecordQuery,
    partitions: &[i32],
    watermarks: &HashMap<i32, Watermarks>,
    limit: usize,
    limits: RecordLimits,
) -> Result<RecordPage, KafkaError> {
    let plan = FetchPlan::build(query, partitions, watermarks, limit, limits);
    let records = session.records(&plan).await?;
    let next = crate::kafka::record::plan::next_cursor(
        plan.order,
        &plan.windows,
        watermarks,
        &records,
        plan.limit,
    );
    Ok(RecordPage {
        has_more: next.is_some(),
        next_cursor: next.map(|cursor| cursor.encode()),
        records,
    })
}

async fn fill_filtered_page<S: ClusterSession + ?Sized>(
    session: &S,
    query: &RecordQuery,
    partitions: &[i32],
    watermarks: &HashMap<i32, Watermarks>,
    limit: usize,
    limits: RecordLimits,
) -> Result<RecordPage, KafkaError> {
    let started = Instant::now();
    let budget = session.consume_timeout();
    let mut collected: Vec<Record> = Vec::new();
    let mut resume = query.cursor.clone();
    let mut last_windows: Vec<PartitionWindow> = Vec::new();
    let mut last_kept: Vec<Record> = Vec::new();

    for _ in 0..MAX_FILTER_PASSES {
        if started.elapsed() >= budget {
            break;
        }

        let plan = FetchPlan::build(
            &pass_query(query, resume.clone()),
            partitions,
            watermarks,
            limit,
            limits,
        );
        if plan.windows.is_empty() {
            last_windows.clear();
            last_kept.clear();
            break;
        }

        let batch = session.records(&plan).await?;
        last_windows = plan.windows.clone();

        let remaining = limit - collected.len();
        let kept: Vec<Record> = batch.into_iter().take(remaining).collect();
        last_kept = kept.clone();
        collected.extend(kept);

        let page_filled = collected.len() >= limit;
        let next = page_cursor(
            plan.order,
            &last_windows,
            watermarks,
            &last_kept,
            page_filled,
        );

        if page_filled {
            resume = next;
            break;
        }
        if next.is_none() {
            resume = None;
            break;
        }
        if next == resume {
            break;
        }
        resume = next;
    }

    collected.sort_by(|left, right| left.cmp_for_order(right, query.order));

    Ok(RecordPage {
        has_more: resume.is_some(),
        next_cursor: resume.map(|cursor| cursor.encode()),
        records: collected,
    })
}

fn pass_query(query: &RecordQuery, resume: Option<RecordCursor>) -> RecordQuery {
    let mut pass = query.clone();
    pass.cursor = resume;
    pass
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

        engine.consumer_groups("local", None).await.unwrap();
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

    #[tokio::test]
    async fn consumer_groups_topic_filter_skips_unrelated_offset_fetches() {
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

        let filtered = engine
            .consumer_groups("local", Some("orders.created"))
            .await
            .unwrap();
        assert_eq!(
            filtered
                .iter()
                .map(|group| group.id.as_str())
                .collect::<Vec<_>>(),
            vec!["order-processor"]
        );
        assert_eq!(probe.committed.load(Ordering::SeqCst), 1);

        let all = engine.consumer_groups("local", None).await.unwrap();
        assert_eq!(all.len(), 2);
        assert_eq!(probe.committed.load(Ordering::SeqCst), 3);
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
