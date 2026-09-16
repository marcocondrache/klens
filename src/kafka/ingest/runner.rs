use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use tokio::task::JoinHandle;

use crate::kafka::error::KafkaError;
use crate::kafka::session::ClusterSession;
use crate::kafka::store::{Change, ClusterStore, Lane, StoreSet};

use super::configs::ConfigSource;
use super::offsets::OffsetsScheduler;
use super::subjects::SubjectSource;
use super::topology::TopologySource;
use super::watermarks::WatermarkSource;

pub trait LaneSource: Send + Sync + 'static {
    type Table: Send + Sync + 'static;
    type Delta: Into<Option<Change>>;

    fn fetch(
        &self,
        prev: Option<&Arc<Self::Table>>,
    ) -> impl Future<Output = Result<Self::Table, KafkaError>> + Send;

    /// `None` means no observable change: nothing is published and
    /// [`after_commit`](Self::after_commit) is skipped. Whether the table is
    /// still committed depends on [`commit_unchanged`](Self::commit_unchanged).
    fn diff(&self, prev: Option<&Self::Table>, next: &Self::Table) -> Option<Self::Delta>;

    fn interval(&self) -> Duration;

    /// Commit the fetched table even when `diff` reports no change.
    ///
    /// Lanes whose table carries a freshness signal of its own (for example
    /// watermark `sampled_at`) should return `true` so successful polls keep
    /// the table and lane health current without emitting events.
    fn commit_unchanged(&self) -> bool {
        false
    }

    fn after_commit(&self, _table: &Arc<Self::Table>, _version: u64, _delta: &Self::Delta) {}
}

pub struct IngestSet {
    tasks: Vec<JoinHandle<()>>,
    stores: Arc<StoreSet>,
}

impl Drop for IngestSet {
    fn drop(&mut self) {
        for task in &self.tasks {
            task.abort();
        }
    }
}

impl IngestSet {
    pub fn start(sessions: Vec<Arc<dyn ClusterSession>>, stores: StoreSet) -> Self {
        let stores = Arc::new(stores);
        let mut tasks = Vec::new();
        for session in sessions {
            let name = session.identity().name.clone();
            let Some(store) = stores.get(&name).cloned() else {
                continue;
            };
            tasks.extend(spawn_cluster(session, store));
        }
        Self { tasks, stores }
    }

    pub fn stores(&self) -> &StoreSet {
        &self.stores
    }

    pub fn kick_all(&self, cluster: &str) {
        if let Some(store) = self.stores.get(cluster) {
            store.topology.kick();
            store.watermarks.kick();
            store.offsets.kick();
            store.configs.kick();
            store.subjects.kick();
        }
    }
}

pub fn spawn_lane<S>(
    source: S,
    store: Arc<ClusterStore>,
    lane: fn(&ClusterStore) -> &Lane<S::Table>,
) -> JoinHandle<()>
where
    S: LaneSource,
{
    tokio::spawn(async move {
        loop {
            let started = tokio::time::Instant::now();
            let lane = lane(&store);
            let prev = lane.load();
            match source.fetch(prev.as_ref()).await {
                Ok(next) => {
                    match source.diff(prev.as_deref(), &next) {
                        Some(delta) => {
                            let next = Arc::new(next);
                            let version = lane.commit(Arc::clone(&next));
                            source.after_commit(&next, version, &delta);
                            if let Some(change) = delta.into() {
                                store.bus.publish(change);
                            }
                        }
                        None if source.commit_unchanged() => {
                            lane.commit(Arc::new(next));
                        }
                        None => {}
                    }
                    lane.record_health(started.elapsed(), None);
                }
                Err(error) => {
                    tracing::warn!(
                        cluster = %store.identity.name,
                        error = %error,
                        "ingest lane poll failed"
                    );
                    lane.record_health(started.elapsed(), Some(error.to_string()));
                }
            }
            let interval = source.interval();
            lane.wait(interval).await;
        }
    })
}

fn spawn_cluster(
    session: Arc<dyn ClusterSession>,
    store: Arc<ClusterStore>,
) -> Vec<JoinHandle<()>> {
    vec![
        spawn_lane(
            TopologySource::new(Arc::clone(&session), Arc::clone(&store)),
            Arc::clone(&store),
            |store| &store.topology,
        ),
        spawn_lane(
            WatermarkSource::new(Arc::clone(&session), Arc::clone(&store)),
            Arc::clone(&store),
            |store| &store.watermarks,
        ),
        spawn_lane(
            ConfigSource::new(Arc::clone(&session), Arc::clone(&store)),
            Arc::clone(&store),
            |store| &store.configs,
        ),
        spawn_lane(
            SubjectSource::new(Arc::clone(&session), Arc::clone(&store)),
            Arc::clone(&store),
            |store| &store.subjects,
        ),
        OffsetsScheduler::spawn(session, store),
    ]
}
