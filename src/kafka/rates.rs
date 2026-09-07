use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio::time::Instant;

/// How often a live subscription samples topic high watermarks.
pub const SAMPLE_INTERVAL: Duration = Duration::from_secs(2);

/// Ignore a previous snapshot older than this when computing a rate.
const MAX_SAMPLE_GAP: Duration = Duration::from_secs(15);

const HISTORY_LEN: usize = 60;

/// Produce rate for one topic, derived from high-watermark deltas.
#[derive(Debug, Clone, PartialEq)]
pub struct TopicRate {
    pub name: String,
    pub messages_per_sec: f64,
    pub bytes_in_per_sec: f64,
}

/// One sampled throughput observation.
#[derive(Debug, Clone, PartialEq)]
pub struct ThroughputPoint {
    pub timestamp: f64,
    pub bytes_in: f64,
    pub bytes_out: f64,
    pub messages: f64,
}

#[derive(Debug, Clone)]
struct Sample {
    at: Instant,
    unix_ms: f64,
    counts: HashMap<String, u64>,
}

#[derive(Debug, Default)]
struct ClusterSamples {
    current: Option<Sample>,
    rates: HashMap<String, TopicRate>,
    topic_history: HashMap<String, VecDeque<ThroughputPoint>>,
    cluster_history: VecDeque<ThroughputPoint>,
}

/// Remembers watermark snapshots so GraphQL queries and subscriptions can
/// share the latest produce rates.
#[derive(Clone, Debug, Default)]
pub struct RateStore {
    inner: Arc<RwLock<HashMap<String, ClusterSamples>>>,
}

impl RateStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn observe(&self, cluster: &str, counts: HashMap<String, u64>) {
        self.observe_at(cluster, counts, Instant::now(), unix_ms_now());
    }

    pub fn observe_at(
        &self,
        cluster: &str,
        counts: HashMap<String, u64>,
        at: Instant,
        unix_ms: f64,
    ) {
        let sample = Sample {
            at,
            unix_ms,
            counts,
        };

        let mut clusters = self.inner.write().expect("rate store lock");
        let cluster = clusters.entry(cluster.to_owned()).or_default();
        cluster.record(sample);
    }

    pub fn topic_rates(&self, cluster: &str) -> Vec<TopicRate> {
        let clusters = self.inner.read().expect("rate store lock");
        let Some(cluster) = clusters.get(cluster) else {
            return Vec::new();
        };

        let mut rates: Vec<TopicRate> = cluster.rates.values().cloned().collect();
        rates.sort_by(|left, right| left.name.cmp(&right.name));
        rates
    }

    pub fn topic_rate(&self, cluster: &str, topic: &str) -> Option<TopicRate> {
        let clusters = self.inner.read().expect("rate store lock");
        clusters.get(cluster)?.rates.get(topic).cloned()
    }

    pub fn topic_history(&self, cluster: &str, topic: &str) -> Vec<ThroughputPoint> {
        let clusters = self.inner.read().expect("rate store lock");
        clusters
            .get(cluster)
            .and_then(|cluster| cluster.topic_history.get(topic))
            .map(|history| history.iter().cloned().collect())
            .unwrap_or_default()
    }

    pub fn cluster_history(&self, cluster: &str) -> Vec<ThroughputPoint> {
        let clusters = self.inner.read().expect("rate store lock");
        clusters
            .get(cluster)
            .map(|cluster| cluster.cluster_history.iter().cloned().collect())
            .unwrap_or_default()
    }
}

impl ClusterSamples {
    fn record(&mut self, sample: Sample) {
        let previous = self.current.replace(sample);
        let current = self.current.as_ref().expect("sample just stored");

        let mut rates = HashMap::with_capacity(current.counts.len());
        let mut cluster_messages = 0.0;

        for name in current.counts.keys() {
            let messages_per_sec = previous
                .as_ref()
                .and_then(|previous| rate_between(previous, current, name))
                .unwrap_or(0.0);
            cluster_messages += messages_per_sec;
            rates.insert(
                name.clone(),
                TopicRate {
                    name: name.clone(),
                    messages_per_sec,
                    bytes_in_per_sec: 0.0,
                },
            );

            push_history(
                self.topic_history.entry(name.clone()).or_default(),
                ThroughputPoint {
                    timestamp: current.unix_ms,
                    bytes_in: 0.0,
                    bytes_out: 0.0,
                    messages: messages_per_sec,
                },
            );
        }

        self.topic_history
            .retain(|topic, _| current.counts.contains_key(topic));
        self.rates = rates;

        push_history(
            &mut self.cluster_history,
            ThroughputPoint {
                timestamp: current.unix_ms,
                bytes_in: 0.0,
                bytes_out: 0.0,
                messages: round_rate(cluster_messages),
            },
        );
    }
}

fn rate_between(previous: &Sample, current: &Sample, topic: &str) -> Option<f64> {
    let elapsed = current.at.saturating_duration_since(previous.at);
    if elapsed.is_zero() || elapsed > MAX_SAMPLE_GAP {
        return None;
    }

    let previous_count = previous.counts.get(topic).copied()?;
    let current_count = current.counts.get(topic).copied()?;
    Some(messages_per_sec(previous_count, current_count, elapsed))
}

pub fn messages_per_sec(previous: u64, current: u64, elapsed: Duration) -> f64 {
    if elapsed.is_zero() {
        return 0.0;
    }

    round_rate(current.saturating_sub(previous) as f64 / elapsed.as_secs_f64())
}

fn round_rate(value: f64) -> f64 {
    (value * 1000.0).round() / 1000.0
}

fn push_history(history: &mut VecDeque<ThroughputPoint>, point: ThroughputPoint) {
    if history.len() == HISTORY_LEN {
        history.pop_front();
    }
    history.push_back(point);
}

fn unix_ms_now() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis() as f64)
        .unwrap_or(0.0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn counts(pairs: &[(&str, u64)]) -> HashMap<String, u64> {
        pairs
            .iter()
            .map(|(name, count)| ((*name).to_owned(), *count))
            .collect()
    }

    #[test]
    fn first_sample_has_zero_rate() {
        let store = RateStore::new();
        let start = Instant::now();
        store.observe_at("local", counts(&[("orders", 10)]), start, 1_000.0);

        assert_eq!(
            store.topic_rates("local"),
            vec![TopicRate {
                name: "orders".into(),
                messages_per_sec: 0.0,
                bytes_in_per_sec: 0.0,
            }]
        );
    }

    #[test]
    fn second_sample_uses_high_watermark_delta() {
        let store = RateStore::new();
        let start = Instant::now();
        store.observe_at("local", counts(&[("orders", 10)]), start, 1_000.0);
        store.observe_at(
            "local",
            counts(&[("orders", 30)]),
            start + Duration::from_secs(2),
            3_000.0,
        );

        assert_eq!(
            store
                .topic_rate("local", "orders")
                .unwrap()
                .messages_per_sec,
            10.0
        );
        assert_eq!(store.topic_history("local", "orders").len(), 2);
        assert_eq!(store.topic_history("local", "orders")[1].messages, 10.0);
        assert_eq!(store.cluster_history("local")[1].messages, 10.0);
    }

    #[test]
    fn truncated_logs_do_not_produce_negative_rates() {
        let store = RateStore::new();
        let start = Instant::now();
        store.observe_at("local", counts(&[("orders", 80)]), start, 1_000.0);
        store.observe_at(
            "local",
            counts(&[("orders", 12)]),
            start + Duration::from_secs(2),
            3_000.0,
        );

        assert_eq!(
            store
                .topic_rate("local", "orders")
                .unwrap()
                .messages_per_sec,
            0.0
        );
    }

    #[test]
    fn stale_previous_sample_is_ignored() {
        let store = RateStore::new();
        let start = Instant::now();
        store.observe_at("local", counts(&[("orders", 10)]), start, 1_000.0);
        store.observe_at(
            "local",
            counts(&[("orders", 1_010)]),
            start + Duration::from_secs(30),
            31_000.0,
        );

        assert_eq!(
            store
                .topic_rate("local", "orders")
                .unwrap()
                .messages_per_sec,
            0.0
        );
    }

    #[test]
    fn dropped_topics_leave_the_rate_list() {
        let store = RateStore::new();
        let start = Instant::now();
        store.observe_at(
            "local",
            counts(&[("orders", 10), ("payments", 4)]),
            start,
            1_000.0,
        );
        store.observe_at(
            "local",
            counts(&[("orders", 12)]),
            start + Duration::from_secs(2),
            3_000.0,
        );

        let names: Vec<_> = store
            .topic_rates("local")
            .into_iter()
            .map(|rate| rate.name)
            .collect();
        assert_eq!(names, vec!["orders"]);
        assert!(store.topic_history("local", "payments").is_empty());
    }

    #[test]
    fn history_is_capped() {
        let store = RateStore::new();
        let start = Instant::now();
        for index in 0..=HISTORY_LEN {
            store.observe_at(
                "local",
                counts(&[("orders", index as u64)]),
                start + Duration::from_millis(index as u64 * 500),
                index as f64,
            );
        }

        assert_eq!(store.topic_history("local", "orders").len(), HISTORY_LEN);
        assert_eq!(store.cluster_history("local").len(), HISTORY_LEN);
    }

    #[test]
    fn messages_per_sec_rounds_to_millis() {
        assert_eq!(messages_per_sec(0, 1, Duration::from_secs(3)), 0.333);
    }
}
