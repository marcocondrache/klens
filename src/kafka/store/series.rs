use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, RwLock};

use chrono::{DateTime, Utc};

use crate::environment::HISTORY_LEN;

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Point<V> {
    pub at: DateTime<Utc>,
    pub value: V,
}

#[derive(Debug, Clone, PartialEq)]
struct Ring<V> {
    points: VecDeque<Point<V>>,
}

impl<V> Default for Ring<V> {
    fn default() -> Self {
        Self {
            points: VecDeque::new(),
        }
    }
}

impl<V: Clone> Ring<V> {
    fn push(&mut self, point: Point<V>, cap: usize) {
        while self.points.len() >= cap {
            self.points.pop_front();
        }
        self.points.push_back(point);
    }

    fn to_vec(&self) -> Vec<Point<V>> {
        self.points.iter().cloned().collect()
    }

    fn last(&self) -> Option<&Point<V>> {
        self.points.back()
    }
}

/// Server-timestamped sparkline history, fed only by the watermark and
/// offsets lanes.
///
/// History exists whether or not anyone is subscribed, and the API serves the
/// same points it streams, so seeding a chart and then following the stream
/// produces one coherent series.
#[derive(Debug)]
pub struct SeriesStore {
    topic_rates: RwLock<HashMap<Arc<str>, Ring<f64>>>,
    group_lag: RwLock<HashMap<Arc<str>, Ring<i64>>>,
    cluster_rate: RwLock<Ring<f64>>,
    cap: usize,
}

impl Default for SeriesStore {
    fn default() -> Self {
        Self::with_capacity(*HISTORY_LEN)
    }
}

impl SeriesStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_capacity(cap: usize) -> Self {
        Self {
            topic_rates: RwLock::new(HashMap::new()),
            group_lag: RwLock::new(HashMap::new()),
            cluster_rate: RwLock::new(Ring::default()),
            cap: cap.max(1),
        }
    }

    pub fn push_topic_rate(&self, topic: &Arc<str>, at: DateTime<Utc>, rate: f64) {
        let mut rates = self.topic_rates.write().expect("series store lock");
        match rates.get_mut(topic) {
            Some(ring) => ring.push(Point { at, value: rate }, self.cap),
            None => {
                let mut ring = Ring::default();
                ring.push(Point { at, value: rate }, self.cap);
                rates.insert(Arc::clone(topic), ring);
            }
        }
    }

    pub fn push_cluster_rate(&self, at: DateTime<Utc>, rate: f64) {
        self.cluster_rate
            .write()
            .expect("series store lock")
            .push(Point { at, value: rate }, self.cap);
    }

    pub fn push_group_lag(&self, group: &Arc<str>, at: DateTime<Utc>, lag: i64) {
        let value = lag.max(0);
        let mut lags = self.group_lag.write().expect("series store lock");
        match lags.get_mut(group) {
            Some(ring) => ring.push(Point { at, value }, self.cap),
            None => {
                let mut ring = Ring::default();
                ring.push(Point { at, value }, self.cap);
                lags.insert(Arc::clone(group), ring);
            }
        }
    }

    pub fn topic_history(&self, topic: &str) -> Vec<Point<f64>> {
        self.topic_rates
            .read()
            .expect("series store lock")
            .get(topic)
            .map(Ring::to_vec)
            .unwrap_or_default()
    }

    pub fn group_history(&self, group: &str) -> Vec<Point<i64>> {
        self.group_lag
            .read()
            .expect("series store lock")
            .get(group)
            .map(Ring::to_vec)
            .unwrap_or_default()
    }

    pub fn cluster_history(&self) -> Vec<Point<f64>> {
        self.cluster_rate
            .read()
            .expect("series store lock")
            .to_vec()
    }

    pub fn topic_rate(&self, topic: &str) -> Option<f64> {
        self.topic_rates
            .read()
            .expect("series store lock")
            .get(topic)
            .and_then(Ring::last)
            .map(|point| point.value)
    }

    pub fn group_lag(&self, group: &str) -> Option<i64> {
        self.group_lag
            .read()
            .expect("series store lock")
            .get(group)
            .and_then(Ring::last)
            .map(|point| point.value)
    }

    /// Membership in the topology is the retention policy: a topic or group
    /// the cluster no longer reports loses its ring on the next commit.
    pub fn retain_topics(&self, live: impl Fn(&str) -> bool) {
        self.topic_rates
            .write()
            .expect("series store lock")
            .retain(|topic, _| live(topic));
    }

    pub fn retain_groups(&self, live: impl Fn(&str) -> bool) {
        self.group_lag
            .write()
            .expect("series store lock")
            .retain(|group, _| live(group));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::datetime_from_unix_millis;

    fn at(millis: i64) -> DateTime<Utc> {
        datetime_from_unix_millis(millis)
    }

    #[test]
    fn history_starts_empty() {
        let series = SeriesStore::new();
        assert!(series.topic_history("orders").is_empty());
        assert!(series.group_history("billing").is_empty());
        assert!(series.cluster_history().is_empty());
        assert_eq!(series.topic_rate("orders"), None);
    }

    #[test]
    fn points_keep_the_lane_timestamp() {
        let series = SeriesStore::new();
        series.push_topic_rate(&Arc::from("orders"), at(1_000), 12.5);
        series.push_topic_rate(&Arc::from("orders"), at(4_000), 9.0);

        assert_eq!(
            series.topic_history("orders"),
            vec![
                Point {
                    at: at(1_000),
                    value: 12.5,
                },
                Point {
                    at: at(4_000),
                    value: 9.0,
                },
            ]
        );
        assert_eq!(series.topic_rate("orders"), Some(9.0));
    }

    #[test]
    fn negative_lag_is_clamped() {
        let series = SeriesStore::new();
        series.push_group_lag(&Arc::from("billing"), at(1_000), -4);
        assert_eq!(series.group_lag("billing"), Some(0));
    }

    #[test]
    fn rings_are_capped_and_keep_the_newest_window() {
        let series = SeriesStore::with_capacity(3);
        let topic: Arc<str> = Arc::from("orders");
        for index in 0..5 {
            series.push_topic_rate(&topic, at(index), index as f64);
            series.push_cluster_rate(at(index), index as f64);
            series.push_group_lag(&Arc::from("billing"), at(index), index);
        }

        let values: Vec<f64> = series
            .topic_history("orders")
            .into_iter()
            .map(|point| point.value)
            .collect();
        assert_eq!(values, vec![2.0, 3.0, 4.0]);
        assert_eq!(series.cluster_history().len(), 3);
        assert_eq!(series.group_history("billing").len(), 3);
    }

    #[test]
    fn rings_follow_table_membership() {
        let series = SeriesStore::new();
        series.push_topic_rate(&Arc::from("orders"), at(1_000), 1.0);
        series.push_topic_rate(&Arc::from("payments"), at(1_000), 2.0);
        series.push_group_lag(&Arc::from("billing"), at(1_000), 5);
        series.push_group_lag(&Arc::from("audit"), at(1_000), 6);

        series.retain_topics(|topic| topic == "orders");
        series.retain_groups(|group| group == "billing");

        assert_eq!(series.topic_history("orders").len(), 1);
        assert!(series.topic_history("payments").is_empty());
        assert_eq!(series.group_history("billing").len(), 1);
        assert!(series.group_history("audit").is_empty());
    }

    #[test]
    fn keys_share_the_interned_allocation() {
        let series = SeriesStore::new();
        let topic: Arc<str> = Arc::from("orders");
        series.push_topic_rate(&topic, at(1_000), 1.0);
        series.push_topic_rate(&topic, at(2_000), 2.0);

        let rates = series.topic_rates.read().unwrap();
        let (key, _) = rates.get_key_value("orders").unwrap();
        assert!(Arc::ptr_eq(key, &topic));
    }
}
