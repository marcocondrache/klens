use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use chrono::{DateTime, Utc};
use tokio::task::JoinHandle;

use crate::kafka::QueryEngine;
use crate::kafka::error::KafkaError;
use crate::kafka::session::ClusterSession;
use crate::kafka::topic::Topic;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterSnapshot {
    pub updated_at: DateTime<Utc>,
    pub topics: Vec<Topic>,
}

impl ClusterSnapshot {
    pub fn from_topics(topics: Vec<Topic>) -> Self {
        Self {
            updated_at: wall_clock(),
            topics,
        }
    }

    pub fn topic(&self, name: &str) -> Option<&Topic> {
        self.topics.iter().find(|topic| topic.name == name)
    }
}

#[derive(Clone, Default)]
pub struct CatalogCache {
    inner: Arc<RwLock<HashMap<String, ClusterSnapshot>>>,
}

impl CatalogCache {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn snapshot(&self, cluster: &str) -> Option<ClusterSnapshot> {
        self.inner
            .read()
            .expect("catalog cache lock")
            .get(cluster)
            .cloned()
    }

    pub fn topic(&self, cluster: &str, name: &str) -> Option<Topic> {
        self.snapshot(cluster)?.topic(name).cloned()
    }

    pub fn updated_at(&self, cluster: &str) -> Option<DateTime<Utc>> {
        self.snapshot(cluster).map(|snapshot| snapshot.updated_at)
    }

    pub fn store(&self, cluster: impl Into<String>, snapshot: ClusterSnapshot) {
        self.inner
            .write()
            .expect("catalog cache lock")
            .insert(cluster.into(), snapshot);
    }

    pub fn seed(&self, cluster: impl Into<String>, snapshot: ClusterSnapshot) -> bool {
        let mut inner = self.inner.write().expect("catalog cache lock");
        let cluster = cluster.into();
        if inner.contains_key(&cluster) {
            return false;
        }
        inner.insert(cluster, snapshot);
        true
    }
}

/// One background task per configured cluster. Dropping the poller aborts them.
pub struct CatalogPoller {
    tasks: Vec<JoinHandle<()>>,
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
        cache: CatalogCache,
        engine: Arc<QueryEngine<dyn ClusterSession>>,
        interval: Duration,
    ) -> Self {
        let clusters: Vec<String> = engine.names().into_iter().map(str::to_owned).collect();
        tracing::info!(
            clusters = ?clusters,
            interval_secs = interval.as_secs(),
            "starting catalog poller"
        );
        Self::start_with(cache, clusters, interval, move |cluster| {
            let engine = Arc::clone(&engine);
            async move { engine.topics(&cluster).await }
        })
    }

    pub fn start_with<F, Fut>(
        cache: CatalogCache,
        clusters: impl IntoIterator<Item = impl Into<String>>,
        interval: Duration,
        fetch: F,
    ) -> Self
    where
        F: Fn(String) -> Fut + Send + Sync + Clone + 'static,
        Fut: Future<Output = Result<Vec<Topic>, KafkaError>> + Send + 'static,
    {
        let tasks = clusters
            .into_iter()
            .map(Into::into)
            .map(|cluster| {
                let cache = cache.clone();
                let fetch = fetch.clone();
                tokio::spawn(async move {
                    loop {
                        match fetch(cluster.clone()).await {
                            Ok(topics) => {
                                cache.store(cluster.clone(), ClusterSnapshot::from_topics(topics));
                                tracing::debug!(cluster = %cluster, "catalog snapshot updated");
                            }
                            Err(error) => {
                                tracing::warn!(cluster = %cluster, %error, "catalog poll failed");
                            }
                        }

                        tokio::time::sleep(interval).await;
                    }
                })
            })
            .collect();

        Self { tasks }
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
    use crate::kafka::testing::FakeCluster;
    use crate::kafka::topic_config::CleanupPolicy;

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
    }

    #[test]
    fn snapshot_topic_lookup_is_by_name() {
        let snapshot =
            ClusterSnapshot::from_topics(vec![test_topic("orders"), test_topic("payments")]);
        assert_eq!(snapshot.topic("payments").unwrap().name, "payments");
        assert!(snapshot.topic("missing").is_none());
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
                async { Ok(Vec::new()) }
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
    async fn a_failed_poll_keeps_the_previous_snapshot() {
        let polls = Arc::new(AtomicUsize::new(0));
        let cache = CatalogCache::new();
        cache.store(
            "local",
            ClusterSnapshot::from_topics(vec![test_topic("kept")]),
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
                    Ok(vec![test_topic("ok")])
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
                Ok(vec![test_topic(&cluster)])
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
                async { Ok(Vec::new()) }
            });

        wait_until(|| polls.load(Ordering::SeqCst) >= 1).await;
        drop(poller);
        tokio::time::advance(Duration::from_secs(30)).await;
        tokio::task::yield_now().await;
        assert_eq!(polls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn start_polls_query_engine_topics() {
        let engine = Arc::new(QueryEngine::from_sessions(vec![FakeCluster::local()]));
        let cache = CatalogCache::new();
        let _poller = CatalogPoller::start(cache.clone(), engine, Duration::from_secs(60));

        wait_until(|| cache.snapshot("local").is_some()).await;
        let snapshot = cache.snapshot("local").unwrap();
        assert_eq!(snapshot.topics[0].name, "orders.created");
        assert_eq!(snapshot.topics[0].message_count, 16);
        assert!(snapshot.updated_at >= DateTime::<Utc>::UNIX_EPOCH);
    }
}
