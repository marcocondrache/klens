use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};

use crate::environment::{IDLE_HEARTBEAT, MAX_SAMPLE_GAP, WATERMARK_POLL_INTERVAL};
use crate::kafka::error::KafkaError;
use crate::kafka::session::ClusterSession;
use crate::kafka::store::{ClusterStore, WatermarkTable, WatermarksTick};
use crate::kafka::watermarks::Watermarks;
use crate::utils::utc_now;

use super::runner::LaneSource;

pub struct WatermarkSource {
    session: Arc<dyn ClusterSession>,
    store: Arc<ClusterStore>,
}

impl WatermarkSource {
    pub fn new(session: Arc<dyn ClusterSession>, store: Arc<ClusterStore>) -> Self {
        Self { session, store }
    }
}

impl LaneSource for WatermarkSource {
    type Table = WatermarkTable;
    type Delta = WatermarksTick;

    async fn fetch(
        &self,
        _prev: Option<&Arc<WatermarkTable>>,
    ) -> Result<WatermarkTable, KafkaError> {
        let topology = self.store.topology.load();
        let offsets = self.store.offsets.load();
        let mut partitions: Vec<(String, i32)> = Vec::new();
        if let Some(topology) = topology.as_ref() {
            partitions.extend(
                topology
                    .topic_partitions()
                    .into_iter()
                    .map(|(topic, partition)| (topic.to_string(), partition)),
            );
        }
        if let Some(offsets) = offsets.as_ref() {
            for group in offsets.groups.values() {
                for committed in &group.committed {
                    partitions.push((committed.topic.clone(), committed.partition));
                }
            }
        }
        partitions.sort();
        partitions.dedup();

        let nested = self.session.watermarks(&partitions).await?;
        let intern = |name: &str| {
            topology
                .as_ref()
                .map(|topology| topology.intern_topic(name))
                .unwrap_or_else(|| Arc::from(name))
        };
        let mut marks = HashMap::new();
        for (topic, partitions) in nested {
            marks.insert(intern(&topic), partitions);
        }
        Ok(WatermarkTable {
            sampled_at: utc_now(),
            marks,
        })
    }

    fn diff(&self, prev: Option<&WatermarkTable>, next: &WatermarkTable) -> Option<WatermarksTick> {
        let idle = *IDLE_HEARTBEAT;
        let (rates, cluster_rate, moved) = compute_rates(prev, next);
        if !moved {
            let prev = prev?;
            let gap = next
                .sampled_at
                .signed_duration_since(prev.sampled_at)
                .to_std()
                .unwrap_or(Duration::ZERO);
            if gap < idle {
                return None;
            }
        }
        Some(WatermarksTick {
            at: next.sampled_at,
            rates,
            cluster_rate,
        })
    }

    fn interval(&self) -> Duration {
        *WATERMARK_POLL_INTERVAL
    }

    fn after_commit(&self, _table: &Arc<WatermarkTable>, _version: u64, delta: &WatermarksTick) {
        for (topic, rate) in &delta.rates {
            self.store
                .series
                .push_topic_rate(Arc::clone(topic), delta.at, *rate);
        }
        self.store
            .series
            .push_cluster_rate(delta.at, delta.cluster_rate);
    }
}

pub fn compute_rates(
    prev: Option<&WatermarkTable>,
    next: &WatermarkTable,
) -> (HashMap<Arc<str>, f64>, f64, bool) {
    let mut rates = HashMap::new();
    let mut cluster_rate = 0.0;
    let mut moved = prev.is_none();
    let elapsed = prev.and_then(|prev| duration_between(prev.sampled_at, next.sampled_at));
    let stale = elapsed.is_none_or(|elapsed| elapsed.is_zero() || elapsed > *MAX_SAMPLE_GAP);

    for (topic, partitions) in &next.marks {
        let rate = if stale {
            0.0
        } else if let (Some(prev), Some(elapsed)) = (prev, elapsed) {
            let previous = prev.topic_high_sum(topic);
            let current = partitions
                .values()
                .map(|mark| mark.high.max(0) as u64)
                .sum::<u64>();
            if current != previous || prev.marks.get(topic) != Some(partitions) {
                moved = true;
            }
            messages_per_sec(previous, current, elapsed)
        } else {
            0.0
        };
        cluster_rate += rate;
        rates.insert(Arc::clone(topic), rate);
    }

    if let Some(prev) = prev {
        if prev.marks.len() != next.marks.len()
            || prev.marks.keys().any(|key| !next.marks.contains_key(key))
        {
            moved = true;
        }
        if watermarks_moved(&prev.marks, &next.marks) {
            moved = true;
        }
    }

    (rates, round_rate(cluster_rate), moved)
}

fn watermarks_moved(
    prev: &HashMap<Arc<str>, HashMap<i32, Watermarks>>,
    next: &HashMap<Arc<str>, HashMap<i32, Watermarks>>,
) -> bool {
    prev != next
}

fn duration_between(prev: DateTime<Utc>, next: DateTime<Utc>) -> Option<Duration> {
    next.signed_duration_since(prev).to_std().ok()
}

fn messages_per_sec(previous: u64, current: u64, elapsed: Duration) -> f64 {
    if elapsed.is_zero() {
        return 0.0;
    }
    round_rate(current.saturating_sub(previous) as f64 / elapsed.as_secs_f64())
}

fn round_rate(value: f64) -> f64 {
    (value * 1000.0).round() / 1000.0
}
