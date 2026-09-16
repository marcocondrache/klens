use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, RwLock};

use chrono::{DateTime, Utc};

use crate::environment::HISTORY_LEN;

#[derive(Debug, Clone, PartialEq)]
pub struct Point<V> {
    pub at: DateTime<Utc>,
    pub value: V,
}

#[derive(Debug, Clone)]
pub struct Ring<V> {
    points: VecDeque<Point<V>>,
    cap: usize,
}

impl<V: Clone> Ring<V> {
    pub fn new(cap: usize) -> Self {
        Self {
            points: VecDeque::with_capacity(cap),
            cap,
        }
    }

    pub fn push(&mut self, point: Point<V>) {
        if self.points.len() >= self.cap {
            self.points.pop_front();
        }
        self.points.push_back(point);
    }

    pub fn to_vec(&self) -> Vec<Point<V>> {
        self.points.iter().cloned().collect()
    }

    pub fn last(&self) -> Option<&Point<V>> {
        self.points.back()
    }
}

pub struct SeriesStore {
    topic_rates: RwLock<HashMap<Arc<str>, Ring<f64>>>,
    group_lag: RwLock<HashMap<Arc<str>, Ring<i64>>>,
    cluster_rate: RwLock<Ring<f64>>,
    cap: usize,
}

impl Default for SeriesStore {
    fn default() -> Self {
        Self::new(*HISTORY_LEN)
    }
}

impl SeriesStore {
    pub fn new(cap: usize) -> Self {
        Self {
            topic_rates: RwLock::new(HashMap::new()),
            group_lag: RwLock::new(HashMap::new()),
            cluster_rate: RwLock::new(Ring::new(cap)),
            cap,
        }
    }

    pub fn push_topic_rate(&self, topic: Arc<str>, at: DateTime<Utc>, value: f64) {
        let mut rates = self.topic_rates.write().expect("series lock");
        rates
            .entry(topic)
            .or_insert_with(|| Ring::new(self.cap))
            .push(Point { at, value });
    }

    pub fn push_cluster_rate(&self, at: DateTime<Utc>, value: f64) {
        self.cluster_rate
            .write()
            .expect("series lock")
            .push(Point { at, value });
    }

    pub fn push_group_lag(&self, group: Arc<str>, at: DateTime<Utc>, value: i64) {
        let mut lags = self.group_lag.write().expect("series lock");
        lags.entry(group)
            .or_insert_with(|| Ring::new(self.cap))
            .push(Point { at, value });
    }

    pub fn topic_history(&self, topic: &str) -> Vec<Point<f64>> {
        self.topic_rates
            .read()
            .expect("series lock")
            .get(topic)
            .map(Ring::to_vec)
            .unwrap_or_default()
    }

    pub fn group_history(&self, group: &str) -> Vec<Point<i64>> {
        self.group_lag
            .read()
            .expect("series lock")
            .get(group)
            .map(Ring::to_vec)
            .unwrap_or_default()
    }

    pub fn cluster_history(&self) -> Vec<Point<f64>> {
        self.cluster_rate.read().expect("series lock").to_vec()
    }

    pub fn last_topic_rate(&self, topic: &str) -> Option<f64> {
        self.topic_rates
            .read()
            .expect("series lock")
            .get(topic)
            .and_then(Ring::last)
            .map(|point| point.value)
    }

    pub fn prune_topics(&self, keep: impl Fn(&str) -> bool) {
        self.topic_rates
            .write()
            .expect("series lock")
            .retain(|key, _| keep(key));
    }

    pub fn prune_groups(&self, keep: impl Fn(&str) -> bool) {
        self.group_lag
            .write()
            .expect("series lock")
            .retain(|key, _| keep(key));
    }
}
