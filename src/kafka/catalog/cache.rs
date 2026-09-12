use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use chrono::{DateTime, Utc};
use tokio::sync::watch;

use crate::kafka::broker::Broker;
use crate::kafka::group::ConsumerGroup;
use crate::kafka::registry::SchemaSubject;
use crate::kafka::topic::Topic;

use super::snapshot::{ClusterSnapshot, wall_clock};

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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogRevision {
    pub cluster: String,
    pub updated_at: DateTime<Utc>,
    pub generation: u64,
}

#[derive(Clone)]
pub struct CatalogCache {
    inner: Arc<RwLock<HashMap<String, Arc<ClusterSnapshot>>>>,
    polls: Arc<RwLock<HashMap<String, PollLane>>>,
    generations: Arc<RwLock<HashMap<String, u64>>>,
    updates: Arc<RwLock<HashMap<String, watch::Sender<Option<CatalogRevision>>>>>,
}

impl Default for CatalogCache {
    fn default() -> Self {
        Self {
            inner: Arc::new(RwLock::new(HashMap::new())),
            polls: Arc::new(RwLock::new(HashMap::new())),
            generations: Arc::new(RwLock::new(HashMap::new())),
            updates: Arc::new(RwLock::new(HashMap::new())),
        }
    }
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
        let cluster = cluster.into();
        let snapshot = snapshot.into();
        let mut inner = self.inner.write().expect("catalog cache lock");
        let roster_changed = inner
            .get(&cluster)
            .is_none_or(|previous| !previous.roster_eq(&snapshot));
        inner.insert(cluster.clone(), Arc::clone(&snapshot));
        drop(inner);
        if roster_changed {
            self.publish_revision(&cluster, &snapshot);
        }
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
        let snapshot = snapshot.into();
        inner.insert(cluster.clone(), Arc::clone(&snapshot));
        drop(inner);
        self.publish_revision(&cluster, &snapshot);
        true
    }

    pub fn subscribe_updates(&self, cluster: &str) -> watch::Receiver<Option<CatalogRevision>> {
        let mut updates = self.updates.write().expect("catalog cache lock");
        updates
            .entry(cluster.to_owned())
            .or_insert_with(|| {
                let (sender, _) = watch::channel(None);
                sender
            })
            .subscribe()
    }

    pub fn generation(&self, cluster: &str) -> Option<u64> {
        self.generations
            .read()
            .expect("catalog cache lock")
            .get(cluster)
            .copied()
    }

    fn publish_revision(&self, cluster: &str, snapshot: &ClusterSnapshot) {
        let generation = {
            let mut generations = self.generations.write().expect("catalog cache lock");
            let slot = generations.entry(cluster.to_owned()).or_insert(0);
            *slot += 1;
            *slot
        };
        let revision = CatalogRevision {
            cluster: cluster.to_owned(),
            updated_at: snapshot.updated_at,
            generation,
        };
        let mut updates = self.updates.write().expect("catalog cache lock");
        if let Some(sender) = updates.get(cluster) {
            let _ = sender.send(Some(revision));
            return;
        }
        let (sender, _) = watch::channel(Some(revision));
        updates.insert(cluster.to_owned(), sender);
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
