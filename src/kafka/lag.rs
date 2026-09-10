use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use super::series::{SeriesMap, ThroughputPoint, unix_ms_now};

const MAX_GROUPS: usize = 1_024;

/// Remembers consumer-group lag samples so GraphQL queries can seed sparkline
/// history.
#[derive(Clone, Debug, Default)]
pub struct LagStore {
    inner: Arc<RwLock<HashMap<String, SeriesMap>>>,
}

impl LagStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn observe(&self, cluster: &str, group: &str, lag: i64) {
        self.observe_at(cluster, group, lag, unix_ms_now());
    }

    pub fn observe_at(&self, cluster: &str, group: &str, lag: i64, unix_ms: f64) {
        let mut clusters = self.inner.write().expect("lag store lock");
        clusters
            .entry(cluster.to_owned())
            .or_insert_with(|| SeriesMap::bounded(MAX_GROUPS))
            .push(group, ThroughputPoint::messages(unix_ms, lag.max(0) as f64));
    }

    pub fn history(&self, cluster: &str, group: &str) -> Vec<ThroughputPoint> {
        let clusters = self.inner.read().expect("lag store lock");
        clusters
            .get(cluster)
            .map(|groups| groups.history(group))
            .unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::environment::HISTORY_LEN;

    #[test]
    fn history_starts_empty() {
        let store = LagStore::new();
        assert!(store.history("local", "orders").is_empty());
    }

    #[test]
    fn observe_appends_lag_points() {
        let store = LagStore::new();
        store.observe_at("local", "orders", 100, 1_000.0);
        store.observe_at("local", "orders", 80, 3_000.0);

        let history = store.history("local", "orders");
        assert_eq!(history.len(), 2);
        assert_eq!(history[0].messages, 100.0);
        assert_eq!(history[1].messages, 80.0);
        assert_eq!(history[1].timestamp, 3_000.0);
    }

    #[test]
    fn negative_lag_is_clamped() {
        let store = LagStore::new();
        store.observe_at("local", "orders", -5, 1_000.0);

        assert_eq!(store.history("local", "orders")[0].messages, 0.0);
    }

    #[test]
    fn history_is_capped() {
        let store = LagStore::new();
        for index in 0..=*HISTORY_LEN {
            store.observe_at("local", "orders", index as i64, index as f64);
        }

        let history = store.history("local", "orders");
        assert_eq!(history.len(), *HISTORY_LEN);
        assert_eq!(history[0].messages, 1.0);
        assert_eq!(history.last().unwrap().messages, *HISTORY_LEN as f64);
    }

    #[test]
    fn groups_are_isolated() {
        let store = LagStore::new();
        store.observe_at("local", "orders", 10, 1_000.0);
        store.observe_at("local", "payments", 20, 1_000.0);

        assert_eq!(store.history("local", "orders")[0].messages, 10.0);
        assert_eq!(store.history("local", "payments")[0].messages, 20.0);
    }

    #[test]
    fn clusters_are_isolated() {
        let store = LagStore::new();
        store.observe_at("local", "orders", 10, 1_000.0);
        store.observe_at("staging", "orders", 20, 1_000.0);

        assert_eq!(store.history("local", "orders")[0].messages, 10.0);
        assert_eq!(store.history("staging", "orders")[0].messages, 20.0);
    }
}
