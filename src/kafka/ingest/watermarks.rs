use foldhash::{HashMap, HashMapExt};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use jiff::Timestamp;
use tokio::time::Instant;

use crate::environment::{IDLE_HEARTBEAT, MAX_SAMPLE_GAP};
use crate::kafka::error::KafkaError;
use crate::kafka::session::ClusterSession;
use crate::kafka::store::{
    Change, ClusterStore, Lane, TopicRate, Topology, WatermarkTable, WatermarksTick,
};
use crate::kafka::watermarks::Watermarks;

use super::runner::{LaneSource, floor};

pub struct WatermarkLane {
    session: Arc<dyn ClusterSession>,
    interval: Duration,
    idle_heartbeat: Duration,
    max_sample_gap: Duration,
    committed_at: Mutex<Option<Instant>>,
}

impl WatermarkLane {
    pub fn with_interval(session: Arc<dyn ClusterSession>, interval: Duration) -> Self {
        Self {
            session,
            interval: floor(interval),
            idle_heartbeat: *IDLE_HEARTBEAT,
            max_sample_gap: *MAX_SAMPLE_GAP,
            committed_at: Mutex::new(None),
        }
    }

    fn wanted_partitions(
        &self,
        store: &ClusterStore,
        topology: &Topology,
    ) -> HashMap<String, Vec<i32>> {
        let mut wanted: HashMap<String, Vec<i32>> = HashMap::with_capacity(topology.topics.len());
        for (name, topic) in &topology.topics {
            wanted.insert(name.to_string(), topic.partition_ids());
        }

        if let Some(offsets) = store.offsets.load() {
            for group in offsets.groups.values() {
                for (topic, partition) in group.partitions() {
                    match wanted.get_mut(topic) {
                        Some(partitions) => partitions.push(partition),
                        None => {
                            wanted.insert(topic.to_owned(), vec![partition]);
                        }
                    }
                }
            }
        }

        for partitions in wanted.values_mut() {
            partitions.sort_unstable();
            partitions.dedup();
        }
        wanted
    }

    fn since_last_commit(&self, now: Instant) -> Option<Duration> {
        (*self.committed_at.lock().expect("watermark lane clock"))
            .map(|at| now.saturating_duration_since(at))
    }

    fn mark_committed(&self, now: Instant) {
        *self.committed_at.lock().expect("watermark lane clock") = Some(now);
    }
}

#[async_trait]
impl LaneSource for WatermarkLane {
    type Table = WatermarkTable;
    type Delta = ();

    fn name(&self) -> &'static str {
        "watermarks"
    }

    fn interval(&self) -> Duration {
        self.interval
    }

    fn lane<'a>(&self, store: &'a ClusterStore) -> &'a Lane<WatermarkTable> {
        &store.watermarks
    }

    async fn fetch(
        &self,
        store: &ClusterStore,
        _previous: Option<&Arc<WatermarkTable>>,
    ) -> Result<Option<WatermarkTable>, KafkaError> {
        let Some(topology) = store.topology.load() else {
            return Ok(None);
        };

        let wanted = self.wanted_partitions(store, &topology);
        let fetched = self.session.watermarks(&wanted).await?;

        let marks = fetched
            .into_iter()
            .map(|(topic, partitions)| (topology.intern_topic(&topic), partitions))
            .collect();
        Ok(Some(WatermarkTable::new(Timestamp::now(), marks)))
    }

    fn diff(&self, previous: Option<&WatermarkTable>, next: &WatermarkTable) -> Option<()> {
        if previous.is_none_or(|previous| previous.marks != next.marks) {
            return Some(());
        }
        self.since_last_commit(Instant::now())
            .is_none_or(|since| since >= self.idle_heartbeat)
            .then_some(())
    }

    fn publish(
        &self,
        store: &ClusterStore,
        version: u64,
        previous: Option<&Arc<WatermarkTable>>,
        next: &Arc<WatermarkTable>,
        (): (),
    ) {
        let now = Instant::now();
        let elapsed = self
            .since_last_commit(now)
            .filter(|gap| !gap.is_zero() && *gap <= self.max_sample_gap);
        self.mark_committed(now);

        let rates = rates_between(previous.map(Arc::as_ref), next, elapsed);

        for rate in &rates {
            store.rates.set(&rate.topic, rate.rate);
        }

        store
            .bus
            .publish(Change::Watermarks(Arc::new(WatermarksTick {
                version,
                at: next.sampled_at,
                rates,
            })));
    }
}

/// Produce rate per topic from the high-watermark delta.
///
/// `elapsed` is `None` when there is no usable baseline: the first sample, or
/// a gap too long to divide by. A shrinking high watermark means the log was
/// truncated; that clamps to zero rather than reporting a negative rate.
fn rates_between(
    previous: Option<&WatermarkTable>,
    next: &WatermarkTable,
    elapsed: Option<Duration>,
) -> Vec<TopicRate> {
    let baseline = elapsed.and_then(|elapsed| Some((previous?, elapsed.as_secs_f64())));

    let mut rates: Vec<TopicRate> = next
        .marks
        .iter()
        .map(|(topic, partitions)| {
            let rate = match baseline {
                Some((previous, seconds)) => {
                    let produced: i64 = partitions
                        .iter()
                        .map(|(partition, marks)| {
                            let before = previous
                                .get(topic, *partition)
                                .map(|marks: Watermarks| marks.high)
                                .unwrap_or(marks.high);
                            (marks.high - before).max(0)
                        })
                        .sum();
                    round_rate(produced as f64 / seconds)
                }
                None => 0.0,
            };
            TopicRate {
                topic: Arc::clone(topic),
                rate,
            }
        })
        .collect();

    rates.sort_by(|left, right| left.topic.cmp(&right.topic));
    rates
}

fn round_rate(value: f64) -> f64 {
    (value * 1000.0).round() / 1000.0
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kafka::store::fixtures::{at, watermarks};

    fn rates(
        previous: Option<&WatermarkTable>,
        next: &WatermarkTable,
        elapsed: Option<Duration>,
    ) -> Vec<(String, f64)> {
        rates_between(previous, next, elapsed)
            .into_iter()
            .map(|rate| (rate.topic.to_string(), rate.rate))
            .collect()
    }

    fn secs(seconds: u64) -> Option<Duration> {
        Some(Duration::from_secs(seconds))
    }

    #[test]
    fn the_first_sample_has_no_rate_to_report() {
        let next = watermarks(at(1_000), &[("orders", 0, 0, 100)]);
        assert_eq!(rates(None, &next, None), vec![("orders".into(), 0.0)]);
    }

    #[test]
    fn the_rate_is_the_high_watermark_delta_over_elapsed_time() {
        let previous = watermarks(at(1_000), &[("orders", 0, 0, 10), ("orders", 1, 0, 10)]);
        let next = watermarks(at(3_000), &[("orders", 0, 0, 30), ("orders", 1, 0, 20)]);

        assert_eq!(
            rates(Some(&previous), &next, secs(2)),
            vec![("orders".into(), 15.0)]
        );
    }

    #[test]
    fn a_truncated_log_reports_zero_rather_than_a_negative_rate() {
        let previous = watermarks(at(1_000), &[("orders", 0, 0, 800)]);
        let next = watermarks(at(3_000), &[("orders", 0, 700, 800)]);

        assert_eq!(
            rates(Some(&previous), &next, secs(2)),
            vec![("orders".into(), 0.0)]
        );
    }

    #[test]
    fn a_stale_previous_sample_is_not_divided_by() {
        let previous = watermarks(at(1_000), &[("orders", 0, 0, 10)]);
        let next = watermarks(at(61_000), &[("orders", 0, 0, 100_000)]);

        assert_eq!(
            rates(Some(&previous), &next, None),
            vec![("orders".into(), 0.0)],
            "the caller drops a gap longer than MAX_SAMPLE_GAP"
        );
    }

    #[test]
    fn a_new_partition_contributes_nothing_on_its_first_appearance() {
        let previous = watermarks(at(1_000), &[("orders", 0, 0, 10)]);
        let next = watermarks(at(3_000), &[("orders", 0, 0, 10), ("orders", 1, 0, 5_000)]);

        assert_eq!(
            rates(Some(&previous), &next, secs(2)),
            vec![("orders".into(), 0.0)],
            "a partition first seen now has no baseline to measure against"
        );
    }

    #[test]
    fn rates_are_reported_per_topic_in_name_order() {
        let previous = watermarks(at(1_000), &[("payments", 0, 0, 0), ("orders", 0, 0, 0)]);
        let next = watermarks(at(2_000), &[("payments", 0, 0, 4), ("orders", 0, 0, 2)]);

        assert_eq!(
            rates(Some(&previous), &next, secs(1)),
            vec![("orders".into(), 2.0), ("payments".into(), 4.0)]
        );
    }

    #[test]
    fn rates_round_to_thousandths() {
        let previous = watermarks(at(0), &[("orders", 0, 0, 0)]);
        let next = watermarks(at(3_000), &[("orders", 0, 0, 1)]);

        assert_eq!(
            rates(Some(&previous), &next, secs(3)),
            vec![("orders".into(), 0.333)]
        );
    }
}
