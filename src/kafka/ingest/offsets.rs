use std::collections::HashMap;
use std::sync::Arc;

use futures::StreamExt;
use tokio::task::JoinHandle;
use tokio::time::Instant;

use crate::environment::{
    FAST_OFFSET_INTERVAL, OFFSET_FETCH_CONCURRENCY, OFFSET_TICK_INTERVAL, SLOW_OFFSET_INTERVAL,
};
use crate::kafka::session::ClusterSession;
use crate::kafka::store::{Change, ClusterStore, GroupOffsets, GroupOffsetsWave, group_lag_update};
use crate::utils::utc_now;

pub struct OffsetsScheduler;

impl OffsetsScheduler {
    pub fn spawn(session: Arc<dyn ClusterSession>, store: Arc<ClusterStore>) -> JoinHandle<()> {
        tokio::spawn(async move {
            let mut ages: HashMap<Arc<str>, Instant> = HashMap::new();
            loop {
                let started = tokio::time::Instant::now();
                match run_tick(&session, &store, &mut ages).await {
                    Ok(()) => store.offsets.record_health(started.elapsed(), None),
                    Err(error) => {
                        tracing::warn!(
                            cluster = %store.identity.name,
                            error = %error,
                            "offsets lane tick failed"
                        );
                        store
                            .offsets
                            .record_health(started.elapsed(), Some(error.to_string()));
                    }
                }
                store.offsets.wait(*OFFSET_TICK_INTERVAL).await;
            }
        })
    }
}

async fn run_tick(
    session: &Arc<dyn ClusterSession>,
    store: &ClusterStore,
    ages: &mut HashMap<Arc<str>, Instant>,
) -> Result<(), crate::kafka::error::KafkaError> {
    let Some(topology) = store.topology.load() else {
        return Ok(());
    };
    let hot = store.interest.hot_groups();
    let now = Instant::now();
    let fast = *FAST_OFFSET_INTERVAL;
    let slow = *SLOW_OFFSET_INTERVAL;

    let due: Vec<Arc<str>> = topology
        .groups
        .keys()
        .filter(|id| {
            let age = ages.get(*id).map(|at| now.saturating_duration_since(*at));
            let interval = if hot.contains(*id) { fast } else { slow };
            age.is_none_or(|age| age >= interval)
        })
        .cloned()
        .collect();

    ages.retain(|id, _| topology.groups.contains_key(id));

    if due.is_empty() {
        if let Some(prev) = store.offsets.load() {
            let pruned = prev.patch(Some(&topology), HashMap::new());
            if pruned != *prev {
                store.offsets.commit(Arc::new(pruned));
            }
        }
        return Ok(());
    }

    let concurrency = *OFFSET_FETCH_CONCURRENCY;
    let fetches = futures::stream::iter(due.into_iter().map(|id| {
        let session = Arc::clone(session);
        let partitions = topology
            .groups
            .get(&id)
            .map(|group| group.assigned_partitions())
            .unwrap_or_default();
        async move {
            let result = if partitions.is_empty() {
                Ok(Vec::new())
            } else {
                session.committed_offsets(&id, &partitions).await
            };
            (id, result)
        }
    }))
    .buffer_unordered(concurrency);

    let sampled_at = utc_now();
    let mut updates = HashMap::new();
    let mut wave = Vec::new();
    let mut fetches = std::pin::pin!(fetches);
    while let Some((id, result)) = fetches.next().await {
        match result {
            Ok(committed) => {
                ages.insert(Arc::clone(&id), now);
                let offsets = Arc::new(GroupOffsets {
                    sampled_at,
                    committed,
                });
                let group = topology.groups.get(&id);
                let watermarks = store.watermarks.load();
                let update = group_lag_update(
                    Arc::clone(&id),
                    group,
                    &offsets.committed,
                    watermarks.as_deref(),
                );
                store
                    .series
                    .push_group_lag(Arc::clone(&id), sampled_at, update.total_lag);
                wave.push(update);
                updates.insert(id, offsets);
            }
            Err(error) => {
                tracing::debug!(
                    cluster = %store.identity.name,
                    group = %id,
                    error = %error,
                    "offset fetch failed for group"
                );
            }
        }
    }

    if updates.is_empty() {
        return Ok(());
    }

    let prev = store.offsets.load().unwrap_or_default();
    let next = prev.patch(Some(&topology), updates);
    store.offsets.commit(Arc::new(next));
    store
        .bus
        .publish(Change::GroupOffsets(Arc::new(GroupOffsetsWave {
            at: sampled_at,
            groups: wave,
        })));
    Ok(())
}
