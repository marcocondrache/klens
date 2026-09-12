use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use chrono::{DateTime, Utc};
use tokio::sync::Notify;
use tokio::task::JoinHandle;

use crate::config::SecurityProtocol;
use crate::kafka::QueryEngine;
use crate::kafka::broker::Broker;
use crate::kafka::cluster::{ClusterIdentity, ClusterOverview};
use crate::kafka::error::KafkaError;
use crate::kafka::group::ConsumerGroup;
use crate::kafka::rates::RateStore;
use crate::kafka::registry::SchemaSubject;
use crate::kafka::search::{SearchHit, search_snapshot};
use crate::kafka::session::ClusterSession;
use crate::kafka::topic::Topic;
use crate::kafka::topic_config::ConfigEntry;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterSnapshot {
    pub updated_at: DateTime<Utc>,
    pub topics: Vec<Topic>,
    pub groups: Vec<ConsumerGroup>,
    pub brokers: Vec<Broker>,
    pub overview: ClusterOverview,
}

impl ClusterSnapshot {
    pub fn from_topics(topics: Vec<Topic>) -> Self {
        Self::from_catalog(topics, Vec::new())
    }

    pub fn from_groups(groups: Vec<ConsumerGroup>) -> Self {
        Self::from_catalog(Vec::new(), groups)
    }

    pub fn from_catalog(topics: Vec<Topic>, groups: Vec<ConsumerGroup>) -> Self {
        Self::assemble(
            topics,
            groups,
            Vec::new(),
            ClusterOverview::offline(empty_identity()),
        )
    }

    pub fn assemble(
        topics: Vec<Topic>,
        groups: Vec<ConsumerGroup>,
        brokers: Vec<Broker>,
        overview: ClusterOverview,
    ) -> Self {
        Self {
            updated_at: wall_clock(),
            topics,
            groups,
            brokers,
            overview,
        }
    }

    pub fn topic(&self, name: &str) -> Option<&Topic> {
        self.topics.iter().find(|topic| topic.name == name)
    }

    pub fn group(&self, id: &str) -> Option<&ConsumerGroup> {
        self.groups.iter().find(|group| group.id == id)
    }

    pub fn broker(&self, id: i32) -> Option<&Broker> {
        self.brokers.iter().find(|broker| broker.id == id)
    }

    pub fn message_counts(&self) -> HashMap<String, u64> {
        self.topics
            .iter()
            .map(|topic| (topic.name.clone(), topic.message_count))
            .collect()
    }

    pub fn search(&self, term: &str, subjects: &[SchemaSubject]) -> Vec<SearchHit> {
        search_snapshot(term, &self.topics, &self.brokers, &self.groups, subjects)
    }

    pub fn body_eq(&self, other: &Self) -> bool {
        self.topics == other.topics
            && self.groups == other.groups
            && self.brokers == other.brokers
            && self.overview == other.overview
    }

    pub fn groups_for_topic(&self, topic: Option<&str>) -> Vec<ConsumerGroup> {
        match topic {
            Some(topic) => self
                .groups
                .iter()
                .filter(|group| group.topics.iter().any(|name| name == topic))
                .cloned()
                .collect(),
            None => self.groups.clone(),
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PollLane {
    pub updated_at: Option<DateTime<Utc>>,
    pub last_error: Option<String>,
    pub last_poll_duration_ms: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogHealth {
    pub cluster: String,
    pub updated_at: Option<DateTime<Utc>>,
    pub subjects_updated_at: Option<DateTime<Utc>>,
    pub last_error: Option<String>,
    pub last_poll_duration_ms: Option<u64>,
    pub topic_count: i32,
    pub group_count: i32,
    pub broker_count: i32,
    pub subject_count: i32,
}

#[derive(Clone, Default)]
pub struct CatalogCache {
    inner: Arc<RwLock<HashMap<String, Arc<ClusterSnapshot>>>>,
    polls: Arc<RwLock<HashMap<String, PollLane>>>,
}

impl CatalogCache {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn snapshot(&self, cluster: &str) -> Option<Arc<ClusterSnapshot>> {
        self.inner
            .read()
            .expect("catalog cache lock")
            .get(cluster)
            .cloned()
    }

    pub fn topic(&self, cluster: &str, name: &str) -> Option<Topic> {
        self.snapshot(cluster)?.topic(name).cloned()
    }

    pub fn group(&self, cluster: &str, id: &str) -> Option<ConsumerGroup> {
        self.snapshot(cluster)?.group(id).cloned()
    }

    pub fn broker(&self, cluster: &str, id: i32) -> Option<Broker> {
        self.snapshot(cluster)?.broker(id).cloned()
    }

    pub fn updated_at(&self, cluster: &str) -> Option<DateTime<Utc>> {
        self.snapshot(cluster).map(|snapshot| snapshot.updated_at)
    }

    pub fn store(&self, cluster: impl Into<String>, snapshot: impl Into<Arc<ClusterSnapshot>>) {
        self.inner
            .write()
            .expect("catalog cache lock")
            .insert(cluster.into(), snapshot.into());
    }

    pub fn seed(
        &self,
        cluster: impl Into<String>,
        snapshot: impl Into<Arc<ClusterSnapshot>>,
    ) -> bool {
        let mut inner = self.inner.write().expect("catalog cache lock");
        let cluster = cluster.into();
        if inner.contains_key(&cluster) {
            return false;
        }
        inner.insert(cluster, snapshot.into());
        true
    }

    pub fn invalidate(&self, cluster: &str) {
        self.inner
            .write()
            .expect("catalog cache lock")
            .remove(cluster);
    }

    pub fn record_poll(&self, cluster: &str, duration: Duration, error: Option<String>) {
        record_poll_lane(&self.polls, "catalog cache lock", cluster, duration, error);
    }

    pub fn poll_lane(&self, cluster: &str) -> PollLane {
        let mut lane = self
            .polls
            .read()
            .expect("catalog cache lock")
            .get(cluster)
            .cloned()
            .unwrap_or_default();
        lane.updated_at = self.updated_at(cluster);
        lane
    }
}

#[derive(Clone, Default)]
pub struct SubjectCache {
    inner: Arc<RwLock<HashMap<String, Arc<Vec<SchemaSubject>>>>>,
    polls: Arc<RwLock<HashMap<String, PollLane>>>,
}

impl SubjectCache {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn snapshot(&self, cluster: &str) -> Option<Arc<Vec<SchemaSubject>>> {
        self.inner
            .read()
            .expect("subject cache lock")
            .get(cluster)
            .cloned()
    }

    pub fn store(&self, cluster: impl Into<String>, subjects: impl Into<Arc<Vec<SchemaSubject>>>) {
        let cluster = cluster.into();
        self.inner
            .write()
            .expect("subject cache lock")
            .insert(cluster.clone(), subjects.into());
        touch_updated_at(&self.polls, "subject cache lock", &cluster);
    }

    pub fn seed(
        &self,
        cluster: impl Into<String>,
        subjects: impl Into<Arc<Vec<SchemaSubject>>>,
    ) -> bool {
        let mut inner = self.inner.write().expect("subject cache lock");
        let cluster = cluster.into();
        if inner.contains_key(&cluster) {
            return false;
        }
        inner.insert(cluster.clone(), subjects.into());
        drop(inner);
        touch_updated_at(&self.polls, "subject cache lock", &cluster);
        true
    }

    pub fn invalidate(&self, cluster: &str) {
        self.inner
            .write()
            .expect("subject cache lock")
            .remove(cluster);
    }

    pub fn record_poll(&self, cluster: &str, duration: Duration, error: Option<String>) {
        record_poll_lane(&self.polls, "subject cache lock", cluster, duration, error);
    }

    pub fn poll_lane(&self, cluster: &str) -> PollLane {
        self.polls
            .read()
            .expect("subject cache lock")
            .get(cluster)
            .cloned()
            .unwrap_or_default()
    }
}

#[derive(Debug, Clone, Default)]
pub struct CatalogReuse {
    pub metadata_hash: u64,
    pub configs: HashMap<String, Vec<ConfigEntry>>,
    pub snapshot: Option<Arc<ClusterSnapshot>>,
}

#[derive(Debug, Clone)]
pub struct CatalogAssemble {
    pub snapshot: ClusterSnapshot,
    pub metadata_hash: u64,
    pub configs: HashMap<String, Vec<ConfigEntry>>,
    pub fetched_configs: bool,
    pub reused_topology: bool,
}

/// Background catalog and subject tasks per configured cluster. Dropping the
/// poller aborts them.
pub struct CatalogPoller {
    tasks: Vec<JoinHandle<()>>,
    kicks: HashMap<String, Arc<Notify>>,
}

impl Drop for CatalogPoller {
    fn drop(&mut self) {
        for task in &self.tasks {
            task.abort();
        }
    }
}

impl CatalogPoller {
    pub fn start(
        catalog: CatalogCache,
        subjects: SubjectCache,
        engine: Arc<QueryEngine<dyn ClusterSession>>,
        rates: RateStore,
        catalog_interval: Duration,
        subject_interval: Duration,
        config_interval: Duration,
    ) -> Self {
        let clusters: Vec<String> = engine.names().into_iter().map(str::to_owned).collect();
        tracing::info!(
            clusters = ?clusters,
            catalog_interval_secs = catalog_interval.as_secs(),
            subject_interval_secs = subject_interval.as_secs(),
            config_interval_secs = config_interval.as_secs(),
            "starting catalog poller"
        );
        let kicks: HashMap<String, Arc<Notify>> = clusters
            .iter()
            .cloned()
            .map(|cluster| (cluster, Arc::new(Notify::new())))
            .collect();
        let catalog_engine = Arc::clone(&engine);
        let catalog_rates = rates;
        let catalog_tasks: Vec<JoinHandle<()>> = clusters
            .iter()
            .cloned()
            .map(|cluster| {
                let engine = Arc::clone(&catalog_engine);
                let rates = catalog_rates.clone();
                let cache = catalog.clone();
                let kick = Arc::clone(kicks.get(&cluster).expect("catalog kick"));
                tokio::spawn(async move {
                    let mut reuse = CatalogReuse::default();
                    let mut last_config_fetch = None;
                    loop {
                        let fetch_configs = reuse.configs.is_empty()
                            || last_config_fetch.is_none_or(|fetched_at| {
                                tokio::time::Instant::now()
                                    .saturating_duration_since(fetched_at)
                                    >= config_interval
                            });
                        let started = tokio::time::Instant::now();
                        match engine
                            .catalog_from(&cluster, Some(&reuse), fetch_configs)
                            .await
                        {
                            Ok(assembled) => {
                                if assembled.fetched_configs {
                                    last_config_fetch = Some(tokio::time::Instant::now());
                                }
                                rates.observe(&cluster, assembled.snapshot.message_counts());
                                let changed = reuse.snapshot.as_ref().is_none_or(|prev| {
                                    !prev.body_eq(&assembled.snapshot)
                                });
                                let missing = cache.snapshot(&cluster).is_none();
                                reuse.metadata_hash = assembled.metadata_hash;
                                reuse.configs = assembled.configs;
                                if changed || missing {
                                    let snapshot = Arc::new(assembled.snapshot);
                                    cache.store(cluster.clone(), Arc::clone(&snapshot));
                                    reuse.snapshot = Some(snapshot);
                                    tracing::debug!(cluster = %cluster, lane = "catalog", "poll updated");
                                }
                                cache.record_poll(&cluster, started.elapsed(), None);
                            }
                            Err(error) => {
                                cache.record_poll(
                                    &cluster,
                                    started.elapsed(),
                                    Some(error.to_string()),
                                );
                                tracing::warn!(cluster = %cluster, lane = "catalog", %error, "poll failed");
                            }
                        }

                        wait_for_kick_or_interval(catalog_interval, &kick).await;
                    }
                })
            })
            .collect();
        let subject_record = subjects.clone();
        let subject_tasks = Self::spawn_loop(
            clusters,
            subject_interval,
            "subjects",
            move |cluster| {
                let engine = Arc::clone(&engine);
                let subjects = subject_record.clone();
                async move {
                    let started = tokio::time::Instant::now();
                    let result = engine.schema_subjects(&cluster).await;
                    subjects.record_poll(
                        &cluster,
                        started.elapsed(),
                        result.as_ref().err().map(ToString::to_string),
                    );
                    result
                }
            },
            move |cluster, list| subjects.store(cluster, list),
            None,
        );

        Self {
            tasks: catalog_tasks.into_iter().chain(subject_tasks).collect(),
            kicks,
        }
    }

    pub fn start_with<F, Fut>(
        cache: CatalogCache,
        clusters: impl IntoIterator<Item = impl Into<String>>,
        interval: Duration,
        fetch: F,
    ) -> Self
    where
        F: Fn(String) -> Fut + Send + Sync + Clone + 'static,
        Fut: Future<Output = Result<ClusterSnapshot, KafkaError>> + Send + 'static,
    {
        let clusters: Vec<String> = clusters.into_iter().map(Into::into).collect();
        let kicks: HashMap<String, Arc<Notify>> = clusters
            .iter()
            .cloned()
            .map(|cluster| (cluster, Arc::new(Notify::new())))
            .collect();
        let record = cache.clone();
        Self {
            tasks: Self::spawn_loop(
                clusters,
                interval,
                "catalog",
                move |cluster| {
                    let fetch = fetch.clone();
                    let record = record.clone();
                    async move {
                        let started = tokio::time::Instant::now();
                        let result = fetch(cluster.clone()).await;
                        record.record_poll(
                            &cluster,
                            started.elapsed(),
                            result.as_ref().err().map(ToString::to_string),
                        );
                        result
                    }
                },
                move |cluster, snapshot| cache.store(cluster, snapshot),
                Some(&kicks),
            ),
            kicks,
        }
    }

    pub fn kick(&self, cluster: &str) {
        if let Some(notify) = self.kicks.get(cluster) {
            notify.notify_one();
        }
    }

    fn spawn_loop<T, F, Fut, P>(
        clusters: impl IntoIterator<Item = impl Into<String>>,
        interval: Duration,
        lane: &'static str,
        fetch: F,
        persist: P,
        kicks: Option<&HashMap<String, Arc<Notify>>>,
    ) -> Vec<JoinHandle<()>>
    where
        T: Send + 'static,
        F: Fn(String) -> Fut + Send + Sync + Clone + 'static,
        Fut: Future<Output = Result<T, KafkaError>> + Send + 'static,
        P: Fn(String, T) + Send + Sync + Clone + 'static,
    {
        clusters
            .into_iter()
            .map(Into::into)
            .map(|cluster| {
                let fetch = fetch.clone();
                let persist = persist.clone();
                let kick = kicks.and_then(|kicks| kicks.get(&cluster)).cloned();
                tokio::spawn(async move {
                    loop {
                        match fetch(cluster.clone()).await {
                            Ok(value) => {
                                persist(cluster.clone(), value);
                                tracing::debug!(cluster = %cluster, lane, "poll updated");
                            }
                            Err(error) => {
                                tracing::warn!(cluster = %cluster, lane, %error, "poll failed");
                            }
                        }

                        match &kick {
                            Some(kick) => wait_for_kick_or_interval(interval, kick).await,
                            None => tokio::time::sleep(interval).await,
                        }
                    }
                })
            })
            .collect()
    }
}

fn empty_identity() -> ClusterIdentity {
    ClusterIdentity {
        name: String::new(),
        bootstrap_servers: Vec::new(),
        security_protocol: SecurityProtocol::Plaintext,
    }
}

fn record_poll_lane(
    polls: &Arc<RwLock<HashMap<String, PollLane>>>,
    lock: &'static str,
    cluster: &str,
    duration: Duration,
    error: Option<String>,
) {
    let mut polls = polls.write().expect(lock);
    let lane = polls.entry(cluster.to_owned()).or_default();
    lane.last_poll_duration_ms = Some(duration.as_millis() as u64);
    lane.last_error = error;
}

fn touch_updated_at(
    polls: &Arc<RwLock<HashMap<String, PollLane>>>,
    lock: &'static str,
    cluster: &str,
) {
    polls
        .write()
        .expect(lock)
        .entry(cluster.to_owned())
        .or_default()
        .updated_at = Some(wall_clock());
}

async fn wait_for_kick_or_interval(interval: Duration, kick: &Notify) {
    tokio::select! {
        _ = tokio::time::sleep(interval) => {}
        _ = kick.notified() => {}
    }
}

fn wall_clock() -> DateTime<Utc> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    DateTime::<Utc>::from_timestamp(now.as_secs() as i64, now.subsec_nanos())
        .unwrap_or(DateTime::<Utc>::UNIX_EPOCH)
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;
    use crate::kafka::cluster::ClusterHealth;
    use crate::kafka::group::{ConsumerGroup, GroupState};
    use crate::kafka::registry::{SchemaCompatibility, SchemaSubject, SchemaType};
    use crate::kafka::testing::FakeCluster;
    use crate::kafka::topic_config::CleanupPolicy;

    fn test_group(id: &str) -> ConsumerGroup {
        ConsumerGroup {
            id: id.to_owned(),
            state: GroupState::Stable,
            protocol: "range".into(),
            coordinator: 1,
            members: Vec::new(),
            topics: vec!["orders".into()],
            lag: 4,
            offsets: Vec::new(),
        }
    }

    fn test_subject(name: &str) -> SchemaSubject {
        SchemaSubject {
            subject: name.to_owned(),
            id: 1,
            schema_type: SchemaType::Avro,
            latest_version: 1,
            versions: vec![1],
            compatibility: SchemaCompatibility::Backward,
            schema: "{}".into(),
        }
    }

    fn test_topic(name: &str) -> Topic {
        Topic {
            name: name.to_owned(),
            internal: false,
            partitions: Vec::new(),
            replication_factor: 1,
            message_count: 0,
            cleanup_policy: CleanupPolicy::Delete,
            retention_ms: 0,
            consumer_groups: Vec::new(),
            under_replicated: false,
        }
    }

    async fn wait_until(mut predicate: impl FnMut() -> bool) {
        for _ in 0..200 {
            if predicate() {
                return;
            }
            tokio::task::yield_now().await;
        }
        panic!("condition not met");
    }

    #[test]
    fn store_overwrites_and_seed_does_not() {
        let cache = CatalogCache::new();
        cache.store(
            "local",
            ClusterSnapshot::from_topics(vec![test_topic("first")]),
        );
        cache.store(
            "local",
            ClusterSnapshot::from_topics(vec![test_topic("second")]),
        );
        assert_eq!(cache.topic("local", "second").unwrap().name, "second");
        assert!(cache.topic("local", "first").is_none());

        assert!(!cache.seed(
            "local",
            ClusterSnapshot::from_topics(vec![test_topic("seeded")])
        ));
        assert_eq!(cache.topic("local", "second").unwrap().name, "second");

        let other = CatalogCache::new();
        assert!(other.seed(
            "staging",
            ClusterSnapshot::from_topics(vec![test_topic("seeded")])
        ));
        assert_eq!(other.topic("staging", "seeded").unwrap().name, "seeded");
        assert!(other.updated_at("staging").is_some());
        assert!(other.snapshot("missing").is_none());

        cache.invalidate("local");
        assert!(cache.snapshot("local").is_none());
        cache.invalidate("missing");
    }

    #[test]
    fn record_poll_keeps_last_error_until_a_success() {
        let cache = CatalogCache::new();
        cache.record_poll("local", Duration::from_millis(9), Some("down".into()));
        assert_eq!(cache.poll_lane("local").last_error.as_deref(), Some("down"));
        assert_eq!(cache.poll_lane("local").last_poll_duration_ms, Some(9));
        cache.record_poll("local", Duration::from_millis(4), None);
        assert_eq!(cache.poll_lane("local").last_error, None);
        assert_eq!(cache.poll_lane("local").last_poll_duration_ms, Some(4));
    }

    #[test]
    fn snapshot_reads_share_one_arc() {
        let cache = CatalogCache::new();
        cache.store(
            "local",
            ClusterSnapshot::from_topics(vec![test_topic("orders")]),
        );
        let first = cache.snapshot("local").unwrap();
        let second = cache.snapshot("local").unwrap();
        assert!(Arc::ptr_eq(&first, &second));
        assert_eq!(first.topic("orders").unwrap().name, "orders");
    }

    #[test]
    fn snapshot_looks_up_topics_and_groups_by_name() {
        let snapshot = ClusterSnapshot::from_catalog(
            vec![test_topic("orders"), test_topic("payments")],
            vec![test_group("orders-app"), test_group("payments-app")],
        );
        assert_eq!(snapshot.topic("payments").unwrap().name, "payments");
        assert!(snapshot.topic("missing").is_none());
        assert_eq!(snapshot.group("payments-app").unwrap().id, "payments-app");
        assert!(snapshot.group("missing").is_none());
        assert_eq!(
            snapshot.groups_for_topic(Some("orders"))[0].id,
            "orders-app"
        );
        assert!(snapshot.groups_for_topic(Some("missing")).is_empty());
        assert_eq!(snapshot.groups_for_topic(None).len(), 2);
        assert_eq!(
            snapshot.message_counts(),
            HashMap::from([("orders".into(), 0), ("payments".into(), 0)])
        );
        assert!(snapshot.broker(1).is_none());
        assert_eq!(snapshot.overview.health, ClusterHealth::Offline);
    }

    #[test]
    fn snapshot_looks_up_brokers_by_id() {
        let snapshot = ClusterSnapshot::assemble(
            Vec::new(),
            Vec::new(),
            vec![Broker {
                id: 3,
                host: "broker-c".into(),
                port: 9092,
                rack: None,
                controller: false,
                partition_count: 2,
                leader_count: 1,
            }],
            ClusterOverview::offline(ClusterIdentity {
                name: "local".into(),
                bootstrap_servers: vec!["localhost:9092".into()],
                security_protocol: crate::config::SecurityProtocol::Plaintext,
            }),
        );
        assert_eq!(snapshot.broker(3).unwrap().host, "broker-c");
        assert!(snapshot.broker(1).is_none());
    }

    #[test]
    fn cache_group_lookup_is_by_id() {
        let cache = CatalogCache::new();
        cache.store(
            "local",
            ClusterSnapshot::from_groups(vec![test_group("cached")]),
        );
        assert_eq!(cache.group("local", "cached").unwrap().lag, 4);
        assert!(cache.group("local", "missing").is_none());
        assert!(cache.group("other", "cached").is_none());
    }

    #[test]
    fn cache_broker_lookup_is_by_id() {
        let cache = CatalogCache::new();
        cache.store(
            "local",
            ClusterSnapshot::assemble(
                Vec::new(),
                Vec::new(),
                vec![Broker {
                    id: 7,
                    host: "cached-broker".into(),
                    port: 9093,
                    rack: None,
                    controller: false,
                    partition_count: 4,
                    leader_count: 2,
                }],
                ClusterOverview::offline(empty_identity()),
            ),
        );
        assert_eq!(cache.broker("local", 7).unwrap().host, "cached-broker");
        assert!(cache.broker("local", 1).is_none());
        assert!(cache.broker("other", 7).is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn first_poll_runs_immediately_then_respects_the_interval() {
        let polls = Arc::new(AtomicUsize::new(0));
        let cache = CatalogCache::new();
        let counter = Arc::clone(&polls);
        let _poller = CatalogPoller::start_with(
            cache.clone(),
            ["local"],
            Duration::from_secs(5),
            move |_| {
                counter.fetch_add(1, Ordering::SeqCst);
                async { Ok(ClusterSnapshot::from_topics(Vec::new())) }
            },
        );

        wait_until(|| polls.load(Ordering::SeqCst) >= 1).await;
        assert_eq!(polls.load(Ordering::SeqCst), 1);
        assert!(cache.snapshot("local").is_some());

        tokio::time::advance(Duration::from_secs(4)).await;
        tokio::task::yield_now().await;
        assert_eq!(polls.load(Ordering::SeqCst), 1);

        tokio::time::advance(Duration::from_secs(1) + Duration::from_millis(1)).await;
        wait_until(|| polls.load(Ordering::SeqCst) >= 2).await;
        assert_eq!(polls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn kick_runs_a_poll_before_the_interval() {
        let polls = Arc::new(AtomicUsize::new(0));
        let cache = CatalogCache::new();
        let counter = Arc::clone(&polls);
        let poller =
            CatalogPoller::start_with(cache, ["local"], Duration::from_secs(60), move |_| {
                counter.fetch_add(1, Ordering::SeqCst);
                async { Ok(ClusterSnapshot::from_topics(Vec::new())) }
            });

        wait_until(|| polls.load(Ordering::SeqCst) >= 1).await;
        assert_eq!(polls.load(Ordering::SeqCst), 1);

        poller.kick("local");
        wait_until(|| polls.load(Ordering::SeqCst) >= 2).await;
        assert_eq!(polls.load(Ordering::SeqCst), 2);
        poller.kick("missing");
    }

    #[tokio::test(start_paused = true)]
    async fn kick_during_fetch_runs_again_when_it_finishes() {
        let polls = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&polls);
        let poller = CatalogPoller::start_with(
            CatalogCache::new(),
            ["local"],
            Duration::from_secs(60),
            move |_| {
                let n = counter.fetch_add(1, Ordering::SeqCst);
                async move {
                    if n == 0 {
                        tokio::time::sleep(Duration::from_secs(5)).await;
                    }
                    Ok(ClusterSnapshot::from_topics(Vec::new()))
                }
            },
        );

        tokio::task::yield_now().await;
        poller.kick("local");
        tokio::time::advance(Duration::from_secs(5) + Duration::from_millis(1)).await;
        wait_until(|| polls.load(Ordering::SeqCst) >= 2).await;
        assert_eq!(polls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn invalidate_then_kick_stores_the_catalog_again() {
        let engine = Arc::new(QueryEngine::from_sessions(vec![FakeCluster::local()]));
        let cache = CatalogCache::new();
        let poller = CatalogPoller::start(
            cache.clone(),
            SubjectCache::new(),
            engine,
            RateStore::new(),
            Duration::from_secs(60),
            Duration::from_secs(60),
            Duration::from_secs(60),
        );

        wait_until(|| cache.snapshot("local").is_some()).await;
        let first = cache.snapshot("local").unwrap();
        cache.invalidate("local");
        assert!(cache.snapshot("local").is_none());

        poller.kick("local");
        wait_until(|| cache.snapshot("local").is_some()).await;
        let second = cache.snapshot("local").unwrap();
        assert!(!Arc::ptr_eq(&first, &second));
        assert!(second.body_eq(&first));
    }

    #[tokio::test(start_paused = true)]
    async fn a_failed_poll_keeps_the_previous_snapshot() {
        let polls = Arc::new(AtomicUsize::new(0));
        let cache = CatalogCache::new();
        cache.store(
            "local",
            ClusterSnapshot::from_catalog(vec![test_topic("kept")], vec![test_group("kept-group")]),
        );
        let counter = Arc::clone(&polls);
        let _poller = CatalogPoller::start_with(
            cache.clone(),
            ["local"],
            Duration::from_secs(5),
            move |_| {
                counter.fetch_add(1, Ordering::SeqCst);
                async { Err(KafkaError::UnknownCluster("local".into())) }
            },
        );

        wait_until(|| polls.load(Ordering::SeqCst) >= 1).await;
        assert_eq!(cache.topic("local", "kept").unwrap().name, "kept");
        assert_eq!(cache.group("local", "kept-group").unwrap().id, "kept-group");
    }

    #[tokio::test(start_paused = true)]
    async fn one_cluster_failing_does_not_block_others() {
        let cache = CatalogCache::new();
        let _poller = CatalogPoller::start_with(
            cache.clone(),
            ["good", "bad"],
            Duration::from_secs(5),
            |cluster| async move {
                if cluster == "bad" {
                    Err(KafkaError::Admin("broker down".into()))
                } else {
                    Ok(ClusterSnapshot::from_topics(vec![test_topic("ok")]))
                }
            },
        );

        wait_until(|| cache.snapshot("good").is_some()).await;
        assert_eq!(cache.topic("good", "ok").unwrap().name, "ok");
        assert!(cache.snapshot("bad").is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn a_slow_cluster_does_not_block_others() {
        let cache = CatalogCache::new();
        let _poller = CatalogPoller::start_with(
            cache.clone(),
            ["slow", "fast"],
            Duration::from_secs(5),
            |cluster| async move {
                if cluster == "slow" {
                    tokio::time::sleep(Duration::from_secs(30)).await;
                }
                Ok(ClusterSnapshot::from_topics(vec![test_topic(&cluster)]))
            },
        );

        wait_until(|| cache.snapshot("fast").is_some()).await;
        assert_eq!(cache.topic("fast", "fast").unwrap().name, "fast");
        assert!(cache.snapshot("slow").is_none());

        tokio::time::advance(Duration::from_secs(30)).await;
        wait_until(|| cache.snapshot("slow").is_some()).await;
        assert_eq!(cache.topic("slow", "slow").unwrap().name, "slow");
    }

    #[tokio::test(start_paused = true)]
    async fn dropping_the_poller_stops_further_polls() {
        let polls = Arc::new(AtomicUsize::new(0));
        let cache = CatalogCache::new();
        let counter = Arc::clone(&polls);
        let poller =
            CatalogPoller::start_with(cache, ["local"], Duration::from_secs(5), move |_| {
                counter.fetch_add(1, Ordering::SeqCst);
                async { Ok(ClusterSnapshot::from_topics(Vec::new())) }
            });

        wait_until(|| polls.load(Ordering::SeqCst) >= 1).await;
        drop(poller);
        tokio::time::advance(Duration::from_secs(30)).await;
        tokio::task::yield_now().await;
        assert_eq!(polls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn start_polls_query_engine_catalog() {
        let engine = Arc::new(QueryEngine::from_sessions(vec![FakeCluster::local()]));
        let cache = CatalogCache::new();
        let rates = RateStore::new();
        let subjects = SubjectCache::new();
        let _poller = CatalogPoller::start(
            cache.clone(),
            subjects.clone(),
            engine,
            rates.clone(),
            Duration::from_secs(60),
            Duration::from_secs(60),
            Duration::from_secs(60),
        );

        wait_until(|| cache.snapshot("local").is_some() && subjects.snapshot("local").is_some())
            .await;
        let snapshot = cache.snapshot("local").unwrap();
        assert_eq!(snapshot.topics[0].name, "orders.created");
        assert_eq!(snapshot.topics[0].message_count, 16);
        assert_eq!(snapshot.groups[0].id, "order-processor");
        assert_eq!(snapshot.groups[0].lag, 5);
        assert_eq!(snapshot.groups[0].topics, vec!["orders.created"]);
        assert_eq!(snapshot.brokers[0].id, 1);
        assert_eq!(snapshot.brokers[0].partition_count, 2);
        assert_eq!(snapshot.brokers[0].leader_count, 2);
        assert_eq!(snapshot.overview.cluster_id, "test-cluster");
        assert_eq!(snapshot.overview.health, ClusterHealth::Healthy);
        assert_eq!(snapshot.overview.broker_count, 1);
        assert_eq!(snapshot.overview.topic_count, 1);
        assert_eq!(snapshot.overview.consumer_group_count, 1);
        assert!(snapshot.updated_at >= DateTime::<Utc>::UNIX_EPOCH);
        assert_eq!(
            rates
                .topic_rate("local", "orders.created")
                .unwrap()
                .messages_per_sec,
            0.0
        );
        assert_eq!(
            subjects.snapshot("local").unwrap()[0].subject,
            "orders.created-value"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn poller_observes_rate_store_from_catalog_counts() {
        let engine = Arc::new(QueryEngine::from_sessions(vec![FakeCluster::local()]));
        let cache = CatalogCache::new();
        let rates = RateStore::new();
        let _poller = CatalogPoller::start(
            cache.clone(),
            SubjectCache::new(),
            engine,
            rates.clone(),
            Duration::from_secs(5),
            Duration::from_secs(60),
            Duration::from_secs(60),
        );

        wait_until(|| !rates.topic_rates("local").is_empty()).await;
        assert_eq!(rates.cluster_history("local").len(), 1);

        tokio::time::advance(Duration::from_secs(5) + Duration::from_millis(1)).await;
        wait_until(|| rates.cluster_history("local").len() >= 2).await;
        assert_eq!(
            rates
                .topic_rate("local", "orders.created")
                .unwrap()
                .messages_per_sec,
            0.0
        );
    }

    #[tokio::test(start_paused = true)]
    async fn unchanged_catalog_poll_keeps_the_same_arc() {
        let engine = Arc::new(QueryEngine::from_sessions(vec![FakeCluster::local()]));
        let cache = CatalogCache::new();
        let rates = RateStore::new();
        let _poller = CatalogPoller::start(
            cache.clone(),
            SubjectCache::new(),
            engine,
            rates.clone(),
            Duration::from_secs(5),
            Duration::from_secs(60),
            Duration::from_secs(60),
        );

        wait_until(|| cache.snapshot("local").is_some()).await;
        let first = cache.snapshot("local").unwrap();

        tokio::time::advance(Duration::from_secs(5) + Duration::from_millis(1)).await;
        wait_until(|| rates.cluster_history("local").len() >= 2).await;
        let second = cache.snapshot("local").unwrap();
        assert!(Arc::ptr_eq(&first, &second));
    }

    #[test]
    fn subject_store_overwrites_and_seed_does_not() {
        let cache = SubjectCache::new();
        cache.store("local", vec![test_subject("first")]);
        cache.store("local", vec![test_subject("second")]);
        assert_eq!(cache.snapshot("local").unwrap()[0].subject, "second");
        assert!(!cache.seed("local", vec![test_subject("seeded")]));
        assert_eq!(cache.snapshot("local").unwrap()[0].subject, "second");

        let other = SubjectCache::new();
        assert!(other.seed("staging", vec![test_subject("seeded")]));
        assert_eq!(other.snapshot("staging").unwrap()[0].subject, "seeded");
        assert!(other.snapshot("missing").is_none());

        cache.invalidate("local");
        assert!(cache.snapshot("local").is_none());
        assert!(cache.poll_lane("local").updated_at.is_some());
    }

    #[tokio::test]
    async fn failed_subject_poll_keeps_catalog_and_previous_subjects() {
        let engine = Arc::new(QueryEngine::from_sessions(vec![
            FakeCluster::local().with_subjects_error("registry down"),
        ]));
        let cache = CatalogCache::new();
        let subjects = SubjectCache::new();
        subjects.store("local", vec![test_subject("kept")]);
        let _poller = CatalogPoller::start(
            cache.clone(),
            subjects.clone(),
            engine,
            RateStore::new(),
            Duration::from_secs(60),
            Duration::from_secs(60),
            Duration::from_secs(60),
        );

        wait_until(|| cache.snapshot("local").is_some()).await;
        assert_eq!(
            cache.topic("local", "orders.created").unwrap().name,
            "orders.created"
        );
        assert_eq!(subjects.snapshot("local").unwrap()[0].subject, "kept");
    }

    #[tokio::test]
    async fn failed_catalog_poll_still_fills_subjects() {
        let engine = Arc::new(QueryEngine::from_sessions(vec![
            FakeCluster::local().unreachable(),
        ]));
        let cache = CatalogCache::new();
        cache.store(
            "local",
            ClusterSnapshot::from_topics(vec![test_topic("kept")]),
        );
        let subjects = SubjectCache::new();
        let _poller = CatalogPoller::start(
            cache.clone(),
            subjects.clone(),
            engine,
            RateStore::new(),
            Duration::from_secs(60),
            Duration::from_secs(60),
            Duration::from_secs(60),
        );

        wait_until(|| subjects.snapshot("local").is_some()).await;
        assert_eq!(cache.topic("local", "kept").unwrap().name, "kept");
        assert_eq!(
            subjects.snapshot("local").unwrap()[0].subject,
            "orders.created-value"
        );
    }
}
