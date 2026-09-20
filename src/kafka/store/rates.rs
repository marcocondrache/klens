use std::sync::{Arc, RwLock};

use foldhash::{HashMap, HashMapExt};

/// Latest produce rate per topic, written by the watermark lane.
#[derive(Debug)]
pub struct RateStore {
    topic_rates: RwLock<HashMap<Arc<str>, f64>>,
}

impl Default for RateStore {
    fn default() -> Self {
        Self {
            topic_rates: RwLock::new(HashMap::new()),
        }
    }
}

impl RateStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn set(&self, topic: &Arc<str>, rate: f64) {
        let mut rates = self.topic_rates.write().expect("rate store lock");
        match rates.get_mut(topic) {
            Some(slot) => *slot = rate,
            None => {
                rates.insert(Arc::clone(topic), rate);
            }
        }
    }

    pub fn get(&self, topic: &str) -> Option<f64> {
        self.topic_rates
            .read()
            .expect("rate store lock")
            .get(topic)
            .copied()
    }

    pub fn retain(&self, live: impl Fn(&str) -> bool) {
        self.topic_rates
            .write()
            .expect("rate store lock")
            .retain(|topic, _| live(topic));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn missing_topics_have_no_rate() {
        let rates = RateStore::new();
        assert_eq!(rates.get("orders"), None);
    }

    #[test]
    fn a_later_sample_replaces_the_rate() {
        let rates = RateStore::new();
        let topic: Arc<str> = Arc::from("orders");
        rates.set(&topic, 12.5);
        rates.set(&topic, 9.0);
        assert_eq!(rates.get("orders"), Some(9.0));
    }

    #[test]
    fn retain_follows_table_membership() {
        let rates = RateStore::new();
        rates.set(&Arc::from("orders"), 1.0);
        rates.set(&Arc::from("payments"), 2.0);

        rates.retain(|topic| topic == "orders");

        assert_eq!(rates.get("orders"), Some(1.0));
        assert_eq!(rates.get("payments"), None);
    }

    #[test]
    fn keys_share_the_interned_allocation() {
        let rates = RateStore::new();
        let topic: Arc<str> = Arc::from("orders");
        rates.set(&topic, 1.0);
        rates.set(&topic, 2.0);

        let map = rates.topic_rates.read().unwrap();
        let (key, _) = map.get_key_value("orders").unwrap();
        assert!(Arc::ptr_eq(key, &topic));
    }
}
