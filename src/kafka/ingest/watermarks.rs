use foldhash::{HashMap, HashMapExt};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use futures::future::join_all;
use itertools::Itertools as _;
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
    partial_error: Mutex<Option<String>>,
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
            partial_error: Mutex::new(None),
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

    async fn read(
        &self,
        wanted: &HashMap<String, Vec<i32>>,
        cached: Option<&WatermarkTable>,
    ) -> Result<HashMap<String, HashMap<i32, Watermarks>>, KafkaError> {
        match cached {
            Some(previous) => self.read_high(wanted, previous).await,
            None => watermarks(self.session.as_ref(), wanted).await,
        }
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
        let now = Instant::now();
        let previous = previous.map(Arc::as_ref);
        let reread_lows = store.watermarks.take_refresh() || self.low_due(now);
        let cached = previous.filter(|_| !reread_lows);
        let leaders = by_leader(topology, self.wanted_partitions(store, topology));
        let reads = join_all(leaders.iter().map(|(&leader, wanted)| async move {
            (leader, wanted, self.read(wanted, cached).await)
        }))
        .await;

        let mut marks: HashMap<Arc<str>, HashMap<i32, Watermarks>> = HashMap::new();
        let mut failures = Vec::new();
        for (leader, wanted, read) in reads {
            match read {
                Ok(fetched) => {
                    for (topic, partitions) in fetched {
                        marks
                            .entry(topology.intern_topic(&topic))
                            .or_default()
                            .extend(partitions);
                    }
                }
                Err(error) => {
                    keep_previous(&mut marks, previous, wanted, topology);
                    failures.push((leader, error));
                }
            }
        }

        if failures.len() == leaders.len()
            && let Some((_, error)) = failures.pop()
        {
            return Err(error);
        }
        if cached.is_none() {
            *self.low_read_at.lock().expect("watermark lane clock") = Some(now);
        }
        *self.partial_error.lock().expect("watermark lane error") =
            (!failures.is_empty()).then(|| {
                failures
                    .iter()
                    .map(|(leader, error)| format!("leader {leader}: {error}"))
                    .join("; ")
            });
        Ok(WatermarkTable { marks })
    }

    fn take_partial_error(&self) -> Option<String> {
        self.partial_error
            .lock()
            .expect("watermark lane error")
            .take()
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

fn by_leader(
    topology: &Topology,
    wanted: HashMap<String, Vec<i32>>,
) -> BTreeMap<i32, HashMap<String, Vec<i32>>> {
    let mut leaders: BTreeMap<i32, HashMap<String, Vec<i32>>> = BTreeMap::new();
    for (topic, partitions) in wanted {
        let info = topology.topics.get(topic.as_str());
        let led = partitions.into_iter().into_group_map_by(|&partition| {
            info.and_then(|info| info.leader(partition)).unwrap_or(-1)
        });
        for (leader, partitions) in led {
            leaders
                .entry(leader)
                .or_default()
                .insert(topic.clone(), partitions);
        }
    }
    leaders
}

fn keep_previous(
    marks: &mut HashMap<Arc<str>, HashMap<i32, Watermarks>>,
    previous: Option<&WatermarkTable>,
    wanted: &HashMap<String, Vec<i32>>,
    topology: &Topology,
) {
    let Some(previous) = previous else {
        return;
    };
    for (topic, partitions) in wanted {
        for &partition in partitions {
            if let Some(kept) = previous.get(topic, partition) {
                marks
                    .entry(topology.intern_topic(topic))
                    .or_default()
                    .insert(partition, kept);
            }
        }
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
mod tests;
