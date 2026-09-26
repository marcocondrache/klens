use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use tokio::time::Instant;

use crate::kafka::error::KafkaError;
use crate::kafka::store::{ClusterStore, Lane};

#[async_trait]
pub trait LaneSource: Send + Sync + 'static {
    type Table: Send + Sync + 'static;
    type Delta: Send + 'static;

    fn name(&self) -> &'static str;

    fn interval(&self) -> Duration;

    fn lane<'a>(&self, store: &'a ClusterStore) -> &'a Lane<Self::Table>;

    async fn fetch(
        &self,
        store: &ClusterStore,
        previous: Option<&Arc<Self::Table>>,
    ) -> Result<Option<Self::Table>, KafkaError>;

    fn diff(&self, previous: Option<&Self::Table>, next: &Self::Table) -> Option<Self::Delta>;

    fn publish(
        &self,
        store: &ClusterStore,
        version: u64,
        previous: Option<&Arc<Self::Table>>,
        next: &Arc<Self::Table>,
        delta: Self::Delta,
    );
}

pub async fn run<S: LaneSource>(store: Arc<ClusterStore>, source: S) {
    loop {
        poll(&store, &source).await;
        source.lane(&store).wait(source.interval()).await;
    }
}

async fn poll<S: LaneSource>(store: &ClusterStore, source: &S) {
    let cluster = store.name();
    let lane = source.name();
    let previous = source.lane(store).load();
    let started = Instant::now();

    match source.fetch(store, previous.as_ref()).await {
        Ok(next) => {
            if let Some(next) = next
                && let Some(delta) = source.diff(previous.as_deref(), &next)
            {
                let next = Arc::new(next);
                let version = source.lane(store).commit(Arc::clone(&next));
                source.publish(store, version, previous.as_ref(), &next, delta);
                tracing::debug!(cluster = %cluster, lane, version, "lane committed");
            }
            source.lane(store).record_poll(started.elapsed(), None);
        }
        Err(error) => {
            source
                .lane(store)
                .record_poll(started.elapsed(), Some(error.to_string()));
            tracing::warn!(cluster = %cluster, lane, %error, "lane poll failed");
        }
    }
}

/// Poll intervals below this are rejected so a misconfigured deployment
/// cannot hammer the brokers.
pub fn floor(interval: Duration) -> Duration {
    interval.max(Duration::from_secs(1))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use crate::kafka::store::Topology;
    use crate::kafka::store::fixtures::{identity, partition, topic, topology};

    struct Scripted {
        polls: AtomicUsize,
        script: Mutex<Vec<Result<Topology, String>>>,
        publishes: AtomicUsize,
    }

    impl Scripted {
        fn new(script: Vec<Result<Topology, String>>) -> Arc<Self> {
            Arc::new(Self {
                polls: AtomicUsize::new(0),
                script: Mutex::new(script),
                publishes: AtomicUsize::new(0),
            })
        }
    }

    #[async_trait]
    impl LaneSource for Arc<Scripted> {
        type Table = Topology;
        type Delta = ();

        fn name(&self) -> &'static str {
            "scripted"
        }

        fn interval(&self) -> Duration {
            Duration::from_secs(600)
        }

        fn lane<'a>(&self, store: &'a ClusterStore) -> &'a Lane<Topology> {
            &store.topology
        }

        async fn fetch(
            &self,
            _store: &ClusterStore,
            _previous: Option<&Arc<Topology>>,
        ) -> Result<Option<Topology>, KafkaError> {
            let index = self.polls.fetch_add(1, Ordering::SeqCst);
            let script = self.script.lock().expect("script");
            match script.get(index).or_else(|| script.last()) {
                Some(Ok(topology)) => Ok(Some(topology.clone())),
                Some(Err(message)) => Err(KafkaError::Admin(message.clone())),
                None => Ok(None),
            }
        }

        fn diff(&self, previous: Option<&Topology>, next: &Topology) -> Option<()> {
            (previous != Some(next)).then_some(())
        }

        fn publish(
            &self,
            _store: &ClusterStore,
            _version: u64,
            _previous: Option<&Arc<Topology>>,
            _next: &Arc<Topology>,
            (): (),
        ) {
            self.publishes.fetch_add(1, Ordering::SeqCst);
        }
    }

    fn cluster(name: &str) -> Arc<ClusterStore> {
        Arc::new(ClusterStore::new(identity(name)))
    }

    fn orders(partitions: usize) -> Topology {
        topology(
            vec![topic(
                "orders",
                (0..partitions as i32)
                    .map(|id| partition(id, vec![1], vec![1]))
                    .collect(),
            )],
            Vec::new(),
        )
    }

    async fn poll_until(source: &Arc<Scripted>, polls: usize) {
        for _ in 0..1_000 {
            if source.polls.load(Ordering::SeqCst) >= polls {
                return;
            }
            tokio::task::yield_now().await;
        }
        panic!("lane never reached {polls} polls");
    }

    #[test]
    fn the_poll_floor_rejects_sub_second_intervals() {
        assert_eq!(floor(Duration::from_millis(50)), Duration::from_secs(1));
        assert_eq!(floor(Duration::from_secs(30)), Duration::from_secs(30));
    }

    #[tokio::test(start_paused = true)]
    async fn a_no_change_poll_commits_nothing_and_publishes_nothing() {
        let store = cluster("local");
        let source = Scripted::new(vec![Ok(orders(1))]);
        let task = tokio::spawn(run(Arc::clone(&store), Arc::clone(&source)));

        poll_until(&source, 1).await;
        assert_eq!(store.topology.version(), 1);
        assert_eq!(source.publishes.load(Ordering::SeqCst), 1);

        store.topology.kick();
        poll_until(&source, 2).await;

        assert_eq!(
            store.topology.version(),
            1,
            "an unchanged table must not bump the version"
        );
        assert_eq!(source.publishes.load(Ordering::SeqCst), 1);
        assert!(store.topology.health().checked_at.is_some());
        task.abort();
    }

    #[tokio::test(start_paused = true)]
    async fn a_failed_poll_keeps_serving_the_last_table() {
        let store = cluster("local");
        let source = Scripted::new(vec![Ok(orders(1)), Err("broker down".into())]);
        let task = tokio::spawn(run(Arc::clone(&store), Arc::clone(&source)));

        poll_until(&source, 1).await;
        store.topology.kick();
        poll_until(&source, 2).await;
        tokio::task::yield_now().await;

        assert_eq!(
            store.topology.load().unwrap().topics.len(),
            1,
            "a failure never clears the table"
        );
        assert_eq!(store.topology.version(), 1);
        assert_eq!(
            store.topology.health().last_error.as_deref(),
            Some("kafka admin request failed: broker down")
        );
        task.abort();
    }

    #[tokio::test(start_paused = true)]
    async fn a_lane_that_never_succeeds_leaves_its_table_empty() {
        let store = cluster("local");
        let source = Scripted::new(vec![Err("broker down".into())]);
        let task = tokio::spawn(run(Arc::clone(&store), Arc::clone(&source)));

        poll_until(&source, 1).await;
        tokio::task::yield_now().await;

        assert!(
            store.topology.load().is_none(),
            "an unavailable source is not an empty cluster"
        );
        assert!(!store.ready());
        task.abort();
    }

    #[tokio::test(start_paused = true)]
    async fn a_change_commits_and_publishes_once() {
        let store = cluster("local");
        let source = Scripted::new(vec![Ok(orders(1)), Ok(orders(2))]);
        let task = tokio::spawn(run(Arc::clone(&store), Arc::clone(&source)));

        poll_until(&source, 1).await;
        store.topology.kick();
        poll_until(&source, 2).await;
        tokio::task::yield_now().await;

        assert_eq!(store.topology.version(), 2);
        assert_eq!(source.publishes.load(Ordering::SeqCst), 2);
        assert_eq!(
            store
                .topology
                .load()
                .unwrap()
                .topic("orders")
                .unwrap()
                .partitions
                .len(),
            2
        );
        task.abort();
    }

    #[tokio::test(start_paused = true)]
    async fn a_replaced_table_is_freed_while_the_lane_sleeps() {
        let store = cluster("local");
        let source = Scripted::new(vec![Ok(orders(1)), Ok(orders(2))]);
        let task = tokio::spawn(run(Arc::clone(&store), Arc::clone(&source)));

        poll_until(&source, 1).await;
        let first = Arc::downgrade(&store.topology.load().unwrap());
        store.topology.kick();
        poll_until(&source, 2).await;
        tokio::task::yield_now().await;

        assert_eq!(store.topology.version(), 2);
        assert!(
            first.upgrade().is_none(),
            "the replaced table outlived its commit"
        );
        task.abort();
    }

    #[tokio::test(start_paused = true)]
    async fn the_lane_sleeps_for_its_interval_between_polls() {
        let store = cluster("local");
        let source = Scripted::new(vec![Ok(orders(1))]);
        let task = tokio::spawn(run(Arc::clone(&store), Arc::clone(&source)));

        poll_until(&source, 1).await;
        tokio::time::advance(Duration::from_secs(599)).await;
        assert_eq!(source.polls.load(Ordering::SeqCst), 1);

        tokio::time::advance(Duration::from_secs(2)).await;
        poll_until(&source, 2).await;
        task.abort();
    }
}
