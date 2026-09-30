use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use tokio::time::Instant;

use crate::kafka::error::KafkaError;
use crate::kafka::store::{ClusterStore, Follower, Lane, Topology};

#[async_trait]
pub trait LaneSource: Send + Sync + 'static {
    type Upstream: Upstream;
    type Table: Send + Sync + 'static;
    type Delta: Send + 'static;

    fn name(&self) -> &'static str;

    fn interval(&self) -> Duration;

    fn lane<'a>(&self, store: &'a ClusterStore) -> &'a Lane<Self::Table>;

    async fn fetch(
        &self,
        store: &ClusterStore,
        upstream: &Self::Upstream,
        previous: Option<&Arc<Self::Table>>,
    ) -> Result<Self::Table, KafkaError>;

    fn stale(&self, _fetched: &Self::Upstream, _latest: &Self::Upstream) -> bool {
        false
    }

    fn diff(&self, previous: Option<&Self::Table>, next: &Self::Table) -> Option<Self::Delta>;

    fn publish(
        &self,
        store: &ClusterStore,
        previous: Option<&Arc<Self::Table>>,
        next: &Arc<Self::Table>,
        delta: Self::Delta,
    );
}

pub trait Upstream: Send + Sync + Sized + 'static {
    fn feed(store: &ClusterStore) -> Feed<'_, Self>;
}

impl Upstream for () {
    fn feed(_store: &ClusterStore) -> Feed<'_, Self> {
        Feed::Fixed(Arc::new(()))
    }
}

impl Upstream for Topology {
    fn feed(store: &ClusterStore) -> Feed<'_, Self> {
        Feed::Lane(store.topology.follow())
    }
}

pub enum Feed<'a, T> {
    Fixed(Arc<T>),
    Lane(Follower<'a, T>),
}

impl<T> Feed<'_, T> {
    async fn table(&mut self) -> Arc<T> {
        match self {
            Self::Fixed(table) => Arc::clone(table),
            Self::Lane(follower) => follower.table().await,
        }
    }

    async fn commit(&mut self) -> Arc<T> {
        match self {
            Self::Fixed(_) => std::future::pending().await,
            Self::Lane(follower) => follower.commit().await,
        }
    }
}

pub async fn run<S: LaneSource>(store: Arc<ClusterStore>, source: S) {
    let mut upstream = S::Upstream::feed(&store);
    loop {
        let fetched = upstream.table().await;
        poll(&store, &source, &fetched).await;
        tokio::select! {
            () = source.lane(&store).wait(source.interval()) => {}
            () = async {
                while !source.stale(&fetched, &*upstream.commit().await) {}
            } => {}
        }
    }
}

async fn poll<S: LaneSource>(store: &ClusterStore, source: &S, upstream: &S::Upstream) {
    let cluster = store.name();
    let lane = source.name();
    let previous = source.lane(store).load();
    let started = Instant::now();

    match source.fetch(store, upstream, previous.as_ref()).await {
        Ok(next) => {
            if let Some(delta) = source.diff(previous.as_deref(), &next) {
                let next = Arc::new(next);
                let version = source.lane(store).commit(Arc::clone(&next));
                source.publish(store, previous.as_ref(), &next, delta);
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use crate::config::IngestTuning;
    use crate::kafka::store::fixtures::{identity, partition, topic, topology};
    use crate::kafka::store::{ConfigTable, LaneHealth};

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
        type Upstream = ();
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
            (): &(),
            _previous: Option<&Arc<Topology>>,
        ) -> Result<Topology, KafkaError> {
            let index = self.polls.fetch_add(1, Ordering::SeqCst);
            let script = self.script.lock().expect("script");
            match script
                .get(index)
                .or_else(|| script.last())
                .expect("a scripted lane needs at least one step")
            {
                Ok(topology) => Ok(topology.clone()),
                Err(message) => Err(KafkaError::Admin(message.clone())),
            }
        }

        fn diff(&self, previous: Option<&Topology>, next: &Topology) -> Option<()> {
            (previous != Some(next)).then_some(())
        }

        fn publish(
            &self,
            _store: &ClusterStore,
            _previous: Option<&Arc<Topology>>,
            _next: &Arc<Topology>,
            (): (),
        ) {
            self.publishes.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[derive(Default)]
    struct Dependent {
        fetched: Mutex<Vec<Topology>>,
    }

    impl Dependent {
        fn polls(&self) -> usize {
            self.fetched.lock().expect("fetched").len()
        }

        fn last_fetched(&self) -> Topology {
            let fetched = self.fetched.lock().expect("fetched");
            fetched.last().expect("a poll").clone()
        }
    }

    #[async_trait]
    impl LaneSource for Arc<Dependent> {
        type Upstream = Topology;
        type Table = ConfigTable;
        type Delta = ();

        fn name(&self) -> &'static str {
            "dependent"
        }

        fn interval(&self) -> Duration {
            Duration::from_secs(600)
        }

        fn lane<'a>(&self, store: &'a ClusterStore) -> &'a Lane<ConfigTable> {
            &store.configs
        }

        async fn fetch(
            &self,
            _store: &ClusterStore,
            topology: &Topology,
            _previous: Option<&Arc<ConfigTable>>,
        ) -> Result<ConfigTable, KafkaError> {
            self.fetched.lock().expect("fetched").push(topology.clone());
            Ok(ConfigTable::default())
        }

        fn stale(&self, fetched: &Topology, latest: &Topology) -> bool {
            latest.gained_topics_since(fetched)
        }

        fn diff(&self, previous: Option<&ConfigTable>, next: &ConfigTable) -> Option<()> {
            (previous != Some(next)).then_some(())
        }

        fn publish(
            &self,
            _store: &ClusterStore,
            _previous: Option<&Arc<ConfigTable>>,
            _next: &Arc<ConfigTable>,
            (): (),
        ) {
        }
    }

    fn cluster(name: &str) -> Arc<ClusterStore> {
        Arc::new(ClusterStore::new(
            identity(name),
            IngestTuning::default().interest_ttl,
        ))
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

    fn orders_and_payments() -> Topology {
        topology(
            vec![
                topic("orders", vec![partition(0, vec![1], vec![1])]),
                topic("payments", vec![partition(0, vec![1], vec![1])]),
            ],
            Vec::new(),
        )
    }

    async fn dependent_polls(source: &Dependent, polls: usize) {
        for _ in 0..1_000 {
            if source.polls() >= polls {
                return;
            }
            tokio::task::yield_now().await;
        }
        panic!("dependent lane never reached {polls} polls");
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
            store.topology.load().unwrap().topics["orders"]
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
    async fn a_failing_lane_waits_out_its_interval() {
        let store = cluster("local");
        let source = Scripted::new(vec![Err("broker down".into())]);
        let task = tokio::spawn(run(Arc::clone(&store), Arc::clone(&source)));

        poll_until(&source, 1).await;
        tokio::time::advance(Duration::from_secs(599)).await;
        assert_eq!(source.polls.load(Ordering::SeqCst), 1);
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

    #[tokio::test(start_paused = true)]
    async fn a_dependent_lane_polls_nothing_until_its_upstream_commits() {
        let store = cluster("local");
        let source = Arc::new(Dependent::default());
        let task = tokio::spawn(run(Arc::clone(&store), Arc::clone(&source)));

        tokio::time::advance(Duration::from_secs(3_600)).await;
        assert_eq!(source.polls(), 0);
        assert_eq!(
            store.configs.health(),
            LaneHealth::default(),
            "a lane that never polled reports no check"
        );

        store.topology.commit(Arc::new(orders(1)));
        dependent_polls(&source, 1).await;
        assert!(store.configs.ready());
        task.abort();
    }

    #[tokio::test(start_paused = true)]
    async fn an_upstream_commit_that_leaves_the_lane_stale_wakes_it() {
        let store = cluster("local");
        store.topology.commit(Arc::new(orders(1)));
        let source = Arc::new(Dependent::default());
        let task = tokio::spawn(run(Arc::clone(&store), Arc::clone(&source)));
        dependent_polls(&source, 1).await;

        store.topology.commit(Arc::new(orders_and_payments()));
        dependent_polls(&source, 2).await;

        assert!(source.last_fetched().topics.contains_key("payments"));
        task.abort();
    }

    #[tokio::test(start_paused = true)]
    async fn an_upstream_commit_the_lane_does_not_need_waits_out_its_interval() {
        let store = cluster("local");
        store.topology.commit(Arc::new(orders(1)));
        let source = Arc::new(Dependent::default());
        let task = tokio::spawn(run(Arc::clone(&store), Arc::clone(&source)));
        dependent_polls(&source, 1).await;

        store.topology.commit(Arc::new(orders(2)));
        tokio::time::advance(Duration::from_secs(599)).await;
        assert_eq!(source.polls(), 1);

        tokio::time::advance(Duration::from_secs(2)).await;
        dependent_polls(&source, 2).await;
        assert_eq!(source.last_fetched().topics["orders"].partitions.len(), 2);
        task.abort();
    }
}
