use foldhash::{HashMap, HashMapExt};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use tokio::time::Instant;

use crate::config::IngestTuning;
use crate::kafka::error::KafkaError;
use crate::kafka::metadata::Watermarks;
use crate::kafka::session::{ClusterSession, merge_watermarks, watermarks};
use crate::kafka::store::{
    Change, ClusterStore, Lane, TopicRate, Topology, WatermarkTable, WatermarksTick,
};

use super::runner::LaneSource;

pub struct WatermarkLane {
    session: Arc<dyn ClusterSession>,
    high_interval: Duration,
    low_interval: Duration,
    low_read_at: Mutex<Option<Instant>>,
    idle_heartbeat: Duration,
    max_sample_gap: Duration,
    committed_at: Mutex<Option<Instant>>,
}

impl WatermarkLane {
    pub fn with_interval(
        session: Arc<dyn ClusterSession>,
        high_interval: Duration,
        tuning: &IngestTuning,
    ) -> Self {
        Self {
            session,
            high_interval,
            low_interval: tuning.low_watermark,
            low_read_at: Mutex::new(None),
            idle_heartbeat: tuning.idle_heartbeat,
            max_sample_gap: tuning.max_sample_gap,
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

    fn low_due(&self, now: Instant) -> bool {
        (*self.low_read_at.lock().expect("watermark lane clock"))
            .is_none_or(|at| now.saturating_duration_since(at) >= self.low_interval)
    }

    async fn read_low_and_high(
        &self,
        wanted: &HashMap<String, Vec<i32>>,
        now: Instant,
    ) -> Result<HashMap<String, HashMap<i32, Watermarks>>, KafkaError> {
        let fetched = watermarks(self.session.as_ref(), wanted).await?;
        *self.low_read_at.lock().expect("watermark lane clock") = Some(now);
        Ok(fetched)
    }

    async fn read_high(
        &self,
        wanted: &HashMap<String, Vec<i32>>,
        previous: &WatermarkTable,
    ) -> Result<HashMap<String, HashMap<i32, Watermarks>>, KafkaError> {
        let highs = self.session.high_watermarks(wanted).await?;

        let mut lows: HashMap<String, HashMap<i32, i64>> = HashMap::with_capacity(highs.len());
        let mut uncached: HashMap<String, Vec<i32>> = HashMap::new();
        for (topic, partitions) in &highs {
            let mut known = HashMap::with_capacity(partitions.len());
            let mut missing = Vec::new();
            for (&partition, &high) in partitions {
                match previous.get(topic, partition) {
                    Some(cached) if cached.low <= high => {
                        known.insert(partition, cached.low);
                    }
                    _ => missing.push(partition),
                }
            }
            if !missing.is_empty() {
                uncached.insert(topic.clone(), missing);
            }
            lows.insert(topic.clone(), known);
        }

        if !uncached.is_empty() {
            for (topic, partitions) in self.session.low_watermarks(&uncached).await? {
                lows.entry(topic).or_default().extend(partitions);
            }
        }
        Ok(merge_watermarks(&lows, highs))
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
    type Upstream = Topology;
    type Table = WatermarkTable;
    type Delta = ();

    fn name(&self) -> &'static str {
        "watermarks"
    }

    fn interval(&self) -> Duration {
        self.high_interval
    }

    fn lane<'a>(&self, store: &'a ClusterStore) -> &'a Lane<WatermarkTable> {
        &store.watermarks
    }

    async fn fetch(
        &self,
        store: &ClusterStore,
        topology: &Topology,
        previous: Option<&Arc<WatermarkTable>>,
    ) -> Result<WatermarkTable, KafkaError> {
        let wanted = self.wanted_partitions(store, topology);
        let now = Instant::now();
        let fetched = match previous {
            Some(previous) if !self.low_due(now) => self.read_high(&wanted, previous).await?,
            _ => self.read_low_and_high(&wanted, now).await?,
        };

        let marks = fetched
            .into_iter()
            .map(|(topic, partitions)| (topology.intern_topic(&topic), partitions))
            .collect();
        Ok(WatermarkTable { marks })
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
        previous: Option<&Arc<WatermarkTable>>,
        next: &Arc<WatermarkTable>,
        (): (),
    ) {
        let now = Instant::now();
        let elapsed = self
            .since_last_commit(now)
            .filter(|gap| !gap.is_zero() && *gap <= self.max_sample_gap);
        self.mark_committed(now);

        let mut rates = rates_between(previous.map(Arc::as_ref), next, elapsed);
        rates.retain(|rate| store.rates.set(&rate.topic, rate.rate));
        if rates.is_empty() {
            return;
        }

        store
            .bus
            .publish(Change::Watermarks(Arc::new(WatermarksTick { rates })));
    }
}

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
    use crate::kafka::store::fixtures::{identity, watermarks};
    use crate::kafka::testing::FakeCluster;
    use tokio::sync::broadcast::Receiver;
    use tokio::sync::broadcast::error::TryRecvError;

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
        let next = watermarks(&[("orders", 0, 0, 100)]);
        assert_eq!(rates(None, &next, None), vec![("orders".into(), 0.0)]);
    }

    #[test]
    fn the_rate_is_the_high_watermark_delta_over_elapsed_time() {
        let previous = watermarks(&[("orders", 0, 0, 10), ("orders", 1, 0, 10)]);
        let next = watermarks(&[("orders", 0, 0, 30), ("orders", 1, 0, 20)]);

        assert_eq!(
            rates(Some(&previous), &next, secs(2)),
            vec![("orders".into(), 15.0)]
        );
    }

    #[test]
    fn a_truncated_log_reports_zero_rather_than_a_negative_rate() {
        let previous = watermarks(&[("orders", 0, 0, 800)]);
        let next = watermarks(&[("orders", 0, 700, 800)]);

        assert_eq!(
            rates(Some(&previous), &next, secs(2)),
            vec![("orders".into(), 0.0)]
        );
    }

    #[test]
    fn a_stale_previous_sample_is_not_divided_by() {
        let previous = watermarks(&[("orders", 0, 0, 10)]);
        let next = watermarks(&[("orders", 0, 0, 100_000)]);

        assert_eq!(
            rates(Some(&previous), &next, None),
            vec![("orders".into(), 0.0)],
            "the caller drops a gap longer than MAX_SAMPLE_GAP"
        );
    }

    #[test]
    fn a_new_partition_contributes_nothing_on_its_first_appearance() {
        let previous = watermarks(&[("orders", 0, 0, 10)]);
        let next = watermarks(&[("orders", 0, 0, 10), ("orders", 1, 0, 5_000)]);

        assert_eq!(
            rates(Some(&previous), &next, secs(2)),
            vec![("orders".into(), 0.0)],
            "a partition first seen now has no baseline to measure against"
        );
    }

    #[test]
    fn rates_are_reported_per_topic_in_name_order() {
        let previous = watermarks(&[("payments", 0, 0, 0), ("orders", 0, 0, 0)]);
        let next = watermarks(&[("payments", 0, 0, 4), ("orders", 0, 0, 2)]);

        assert_eq!(
            rates(Some(&previous), &next, secs(1)),
            vec![("orders".into(), 2.0), ("payments".into(), 4.0)]
        );
    }

    #[test]
    fn rates_round_to_thousandths() {
        let previous = watermarks(&[("orders", 0, 0, 0)]);
        let next = watermarks(&[("orders", 0, 0, 1)]);

        assert_eq!(
            rates(Some(&previous), &next, secs(3)),
            vec![("orders".into(), 0.333)]
        );
    }

    fn ticked(events: &mut Receiver<Change>) -> Vec<(String, f64)> {
        match events.try_recv() {
            Ok(Change::Watermarks(tick)) => tick
                .rates
                .iter()
                .map(|rate| (rate.topic.to_string(), rate.rate))
                .collect(),
            other => panic!("expected a watermark tick, got {other:?}"),
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_tick_carries_only_the_rates_that_changed() {
        let tuning = IngestTuning::default();
        let lane = WatermarkLane::with_interval(
            Arc::new(FakeCluster::local()),
            tuning.high_watermark,
            &tuning,
        );
        let store = ClusterStore::new(identity("local"), tuning.interest_ttl);
        let mut events = store.bus.subscribe();
        let step = |marks: &[(&str, i32, i64, i64)]| Arc::new(watermarks(marks));

        let first = step(&[("orders", 0, 0, 10), ("payments", 0, 0, 10)]);
        lane.publish(&store, None, &first, ());
        assert_eq!(
            ticked(&mut events),
            vec![("orders".into(), 0.0), ("payments".into(), 0.0)],
            "the first tick seeds every topic"
        );

        tokio::time::advance(Duration::from_secs(1)).await;
        let busy = step(&[("orders", 0, 0, 14), ("payments", 0, 0, 10)]);
        lane.publish(&store, Some(&first), &busy, ());
        assert_eq!(ticked(&mut events), vec![("orders".into(), 4.0)]);

        tokio::time::advance(Duration::from_secs(1)).await;
        lane.publish(&store, Some(&busy), &busy, ());
        assert_eq!(
            ticked(&mut events),
            vec![("orders".into(), 0.0)],
            "a rate that falls to zero is still sent"
        );
        assert_eq!(store.rates.get("orders"), Some(0.0));

        tokio::time::advance(Duration::from_secs(1)).await;
        lane.publish(&store, Some(&busy), &busy, ());
        assert_eq!(
            events.try_recv().map(|_| ()),
            Err(TryRecvError::Empty),
            "a tick with no changed rate is not published"
        );
    }
}
