use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::StreamExt;
use tokio::time::Instant;

use crate::environment::{
    FAST_OFFSET_INTERVAL, OFFSET_FETCH_CONCURRENCY, OFFSET_LANE_TICK, SLOW_OFFSET_INTERVAL,
};
use crate::kafka::group::CommittedOffset;
use crate::kafka::session::ClusterSession;
use crate::kafka::store::projections::group_offsets;
use crate::kafka::store::{
    Change, ClusterStore, GroupLagUpdate, GroupOffsets, GroupOffsetsWave, OffsetTable, Topology,
};
use crate::utils::utc_now;

use super::runner::floor;

/// Committed offsets, on a tiered schedule rather than a fixed interval.
///
/// Groups someone is looking at refresh fast; everything else refreshes
/// slowly. Broker load therefore scales with viewed groups, not existing
/// groups, which is where the old per-poll fan-out fell over.
pub struct OffsetLane {
    session: Arc<dyn ClusterSession>,
    tick: Duration,
    fast: Duration,
    slow: Duration,
    concurrency: usize,
    /// When each group was last asked, on the monotonic clock. Scheduling
    /// state belongs to the scheduler, not to the table.
    attempted_at: Mutex<HashMap<Arc<str>, Instant>>,
}

/// One scheduler pass: the groups that came due, fetched together and
/// committed as a single table successor.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct Wave {
    pub refreshed: Vec<Arc<str>>,
    pub failed: Vec<Arc<str>>,
    pub dropped: Vec<Arc<str>>,
}

impl Wave {
    pub fn is_empty(&self) -> bool {
        self.refreshed.is_empty() && self.failed.is_empty() && self.dropped.is_empty()
    }
}

impl OffsetLane {
    pub fn new(session: Arc<dyn ClusterSession>) -> Self {
        Self {
            session,
            tick: floor(*OFFSET_LANE_TICK),
            fast: floor(*FAST_OFFSET_INTERVAL),
            slow: floor(*SLOW_OFFSET_INTERVAL),
            concurrency: (*OFFSET_FETCH_CONCURRENCY).max(1),
            attempted_at: Mutex::new(HashMap::new()),
        }
    }

    pub fn with_tiers(mut self, tick: Duration, fast: Duration, slow: Duration) -> Self {
        self.tick = floor(tick);
        self.fast = floor(fast);
        self.slow = floor(slow);
        self
    }

    pub fn with_concurrency(mut self, concurrency: usize) -> Self {
        self.concurrency = concurrency.max(1);
        self
    }

    /// Drives the scheduler until the task is aborted.
    pub async fn run(self, store: Arc<ClusterStore>) {
        let cluster = store.name().to_owned();
        loop {
            let started = Instant::now();
            let wave = self.sweep(&store).await;
            store.offsets.record_poll(started.elapsed(), None);
            if !wave.is_empty() {
                tracing::debug!(
                    cluster = %cluster,
                    lane = "offsets",
                    refreshed = wave.refreshed.len(),
                    failed = wave.failed.len(),
                    dropped = wave.dropped.len(),
                    "offset wave committed"
                );
            }

            store.offsets.wait(self.tick).await;
        }
    }

    /// One pass: pick what is due, fetch it concurrently, commit once,
    /// compute lag, feed the series store, publish the wave.
    pub async fn sweep(&self, store: &ClusterStore) -> Wave {
        let Some(topology) = store.topology.load() else {
            return Wave::default();
        };
        self.retain(&topology);

        let previous = store.offsets.load();
        let hot = store.interest.hot_groups();

        let due = self.due_groups(&topology, &hot, Instant::now());
        let dropped = stale_groups(&topology, previous.as_deref());
        if due.is_empty() && dropped.is_empty() {
            return Wave::default();
        }

        let fetched = self.fetch(&topology, previous.as_deref(), &due).await;
        let now = utc_now();

        let mut groups: HashMap<Arc<str>, Arc<GroupOffsets>> =
            HashMap::with_capacity(topology.groups.len());
        let mut refreshed = Vec::new();
        let mut failed = Vec::new();

        for id in topology.groups.keys() {
            match fetched.get(id) {
                // A per-group failure degrades that group: it keeps its
                // previous value, and `sampled_at` exposes the staleness.
                Some(None) => {
                    failed.push(Arc::clone(id));
                    if let Some(stale) = previous.as_ref().and_then(|table| table.get(id)) {
                        groups.insert(Arc::clone(id), Arc::clone(stale));
                    }
                }
                Some(Some(committed)) => {
                    refreshed.push(Arc::clone(id));
                    groups.insert(
                        Arc::clone(id),
                        Arc::new(GroupOffsets {
                            sampled_at: now,
                            committed: committed.clone(),
                        }),
                    );
                }
                // Untouched groups carry their pointer across, so a wave is
                // one map rebuild rather than a full refetch.
                None => {
                    if let Some(kept) = previous.as_ref().and_then(|table| table.get(id)) {
                        groups.insert(Arc::clone(id), Arc::clone(kept));
                    }
                }
            }
        }

        self.retain(&topology);
        let next = Arc::new(OffsetTable { groups });
        let version = store.offsets.commit(Arc::clone(&next));

        let updates = self.lag_updates(store, &topology, &next, &refreshed);
        for update in &updates {
            store
                .series
                .push_group_lag(&update.group, now, update.total_lag);
        }
        if !updates.is_empty() {
            store
                .bus
                .publish(Change::GroupOffsets(Arc::new(GroupOffsetsWave {
                    version,
                    at: now,
                    groups: updates,
                })));
        }

        Wave {
            refreshed,
            failed,
            dropped,
        }
    }

    fn due_groups(
        &self,
        topology: &Topology,
        hot: &HashSet<Arc<str>>,
        now: Instant,
    ) -> Vec<Arc<str>> {
        let attempted = self.attempted_at.lock().expect("offset lane clock");
        topology
            .groups
            .keys()
            .filter(|id| {
                let threshold = if hot.contains(*id) {
                    self.fast
                } else {
                    self.slow
                };
                attempted
                    .get(*id)
                    .is_none_or(|at| now.saturating_duration_since(*at) >= threshold)
            })
            .cloned()
            .collect()
    }

    fn mark_attempted(&self, ids: &[Arc<str>]) {
        let now = Instant::now();
        let mut attempted = self.attempted_at.lock().expect("offset lane clock");
        for id in ids {
            attempted.insert(Arc::clone(id), now);
        }
    }

    /// Forgets the schedule for groups the cluster no longer reports, so the
    /// bookkeeping stays the size of the roster.
    fn retain(&self, topology: &Topology) {
        self.attempted_at
            .lock()
            .expect("offset lane clock")
            .retain(|id, _| topology.groups.contains_key(id));
    }

    async fn fetch(
        &self,
        topology: &Topology,
        previous: Option<&OffsetTable>,
        due: &[Arc<str>],
    ) -> HashMap<Arc<str>, Option<Vec<CommittedOffset>>> {
        self.mark_attempted(due);
        let session = Arc::clone(&self.session);

        futures::stream::iter(due.iter().cloned().map(|id| {
            let session = Arc::clone(&session);
            let partitions = offset_fetch_partitions(topology, previous, &id);
            async move {
                if partitions.is_empty() {
                    return (id, Some(Vec::new()));
                }
                match session.committed_offsets(&id, &partitions).await {
                    Ok(committed) => (id, Some(committed)),
                    Err(error) => {
                        tracing::warn!(group = %id, %error, "offset fetch failed");
                        (id, None)
                    }
                }
            }
        }))
        .buffer_unordered(self.concurrency)
        .collect()
        .await
    }

    fn lag_updates(
        &self,
        store: &ClusterStore,
        topology: &Topology,
        offsets: &OffsetTable,
        refreshed: &[Arc<str>],
    ) -> Vec<GroupLagUpdate> {
        let watermarks = store.watermarks.load();

        refreshed
            .iter()
            .filter_map(|id| {
                let group = topology.group(id)?;
                let (offsets, total_lag, lag_complete) = group_offsets(
                    group,
                    offsets.get(id).map(Arc::as_ref),
                    watermarks.as_deref(),
                );
                Some(GroupLagUpdate {
                    group: Arc::clone(id),
                    total_lag,
                    lag_complete,
                    offsets,
                })
            })
            .collect()
    }
}

/// Assigned partitions, plus whatever the group last committed to. The union
/// keeps an empty group's offsets refreshing after its members are gone.
fn offset_fetch_partitions(
    topology: &Topology,
    previous: Option<&OffsetTable>,
    id: &str,
) -> Vec<(String, i32)> {
    let mut partitions: Vec<(String, i32)> = topology
        .group(id)
        .into_iter()
        .flat_map(|group| {
            group
                .assigned_partition_refs()
                .map(|(topic, partition)| (topic.to_owned(), partition))
        })
        .collect();

    if let Some(offsets) = previous.and_then(|table| table.get(id)) {
        partitions.extend(
            offsets
                .partitions()
                .map(|(topic, partition)| (topic.to_owned(), partition)),
        );
    }

    partitions.sort();
    partitions.dedup();
    partitions
}

/// Groups the offsets table still carries that the topology no longer
/// reports.
fn stale_groups(topology: &Topology, previous: Option<&OffsetTable>) -> Vec<Arc<str>> {
    previous
        .map(|table| {
            table
                .groups
                .keys()
                .filter(|id| !topology.groups.contains_key(*id))
                .cloned()
                .collect()
        })
        .unwrap_or_default()
}
