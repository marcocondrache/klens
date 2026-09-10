use std::collections::{HashMap, VecDeque};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::environment::HISTORY_LEN;

#[derive(Debug, Clone, PartialEq)]
pub struct ThroughputPoint {
    pub timestamp: f64,
    pub bytes_in: f64,
    pub bytes_out: f64,
    pub messages: f64,
}

impl ThroughputPoint {
    pub fn messages(timestamp: f64, messages: f64) -> Self {
        Self {
            timestamp,
            bytes_in: 0.0,
            bytes_out: 0.0,
            messages,
        }
    }
}

#[derive(Debug)]
pub struct Series {
    points: VecDeque<ThroughputPoint>,
}

impl Series {
    pub fn push(&mut self, point: ThroughputPoint) {
        if self.points.len() >= *HISTORY_LEN {
            self.points.pop_front();
        }
        self.points.push_back(point);
    }

    pub fn to_vec(&self) -> Vec<ThroughputPoint> {
        self.points.iter().cloned().collect()
    }

    fn last_sampled_at(&self) -> f64 {
        self.points.back().map_or(f64::MIN, |point| point.timestamp)
    }
}

impl Default for Series {
    fn default() -> Self {
        Self {
            points: VecDeque::with_capacity(*HISTORY_LEN),
        }
    }
}

#[derive(Debug, Default)]
pub struct SeriesMap {
    series: HashMap<String, Series>,
    capacity: Option<usize>,
}

impl SeriesMap {
    pub fn bounded(capacity: usize) -> Self {
        Self {
            series: HashMap::new(),
            capacity: Some(capacity),
        }
    }

    pub fn push(&mut self, key: &str, point: ThroughputPoint) {
        if let Some(series) = self.series.get_mut(key) {
            series.push(point);
            return;
        }

        if self.capacity.is_some_and(|cap| self.series.len() >= cap) {
            self.evict_stalest();
        }

        self.series.entry(key.to_owned()).or_default().push(point);
    }

    pub fn history(&self, key: &str) -> Vec<ThroughputPoint> {
        self.series.get(key).map(Series::to_vec).unwrap_or_default()
    }

    pub fn retain(&mut self, keep: impl Fn(&str) -> bool) {
        self.series.retain(|key, _| keep(key));
    }

    fn evict_stalest(&mut self) {
        let stalest = self
            .series
            .iter()
            .min_by(|(_, left), (_, right)| {
                left.last_sampled_at().total_cmp(&right.last_sampled_at())
            })
            .map(|(key, _)| key.clone());

        if let Some(key) = stalest {
            self.series.remove(&key);
        }
    }
}

pub fn unix_ms_now() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis() as f64)
        .unwrap_or(0.0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn series_keeps_the_most_recent_window() {
        let mut series = Series::default();
        for index in 0..=*HISTORY_LEN {
            series.push(ThroughputPoint::messages(index as f64, index as f64));
        }

        let points = series.to_vec();
        assert_eq!(points.len(), *HISTORY_LEN);
        assert_eq!(points[0].messages, 1.0);
        assert_eq!(points.last().unwrap().messages, *HISTORY_LEN as f64);
    }

    #[test]
    fn missing_key_has_no_history() {
        let map = SeriesMap::default();
        assert!(map.history("orders").is_empty());
    }

    #[test]
    fn keys_hold_independent_series() {
        let mut map = SeriesMap::default();
        map.push("orders", ThroughputPoint::messages(1_000.0, 10.0));
        map.push("payments", ThroughputPoint::messages(1_000.0, 20.0));

        assert_eq!(map.history("orders")[0].messages, 10.0);
        assert_eq!(map.history("payments")[0].messages, 20.0);
    }

    #[test]
    fn retain_drops_keys_that_are_no_longer_live() {
        let mut map = SeriesMap::default();
        map.push("orders", ThroughputPoint::messages(1_000.0, 10.0));
        map.push("payments", ThroughputPoint::messages(1_000.0, 20.0));

        map.retain(|key| key == "orders");

        assert_eq!(map.history("orders").len(), 1);
        assert!(map.history("payments").is_empty());
    }

    #[test]
    fn bounded_map_evicts_the_least_recently_sampled_series() {
        let mut map = SeriesMap::bounded(2);
        map.push("orders", ThroughputPoint::messages(1_000.0, 1.0));
        map.push("payments", ThroughputPoint::messages(2_000.0, 2.0));
        map.push("orders", ThroughputPoint::messages(3_000.0, 3.0));

        map.push("shipments", ThroughputPoint::messages(4_000.0, 4.0));

        assert!(map.history("payments").is_empty());
        assert_eq!(map.history("orders").len(), 2);
        assert_eq!(map.history("shipments").len(), 1);
    }

    #[test]
    fn resampling_an_existing_key_does_not_evict() {
        let mut map = SeriesMap::bounded(1);
        map.push("orders", ThroughputPoint::messages(1_000.0, 1.0));
        map.push("orders", ThroughputPoint::messages(2_000.0, 2.0));

        assert_eq!(map.history("orders").len(), 2);
    }
}
