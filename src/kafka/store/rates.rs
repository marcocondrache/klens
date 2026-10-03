use std::sync::{Arc, RwLock};

use foldhash::{HashMap, HashMapExt};

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

    /// Returns whether the stored rate changed.
    pub fn set(&self, topic: &Arc<str>, rate: f64) -> bool {
        let mut rates = self.topic_rates.write().expect("rate store lock");
        match rates.get_mut(topic) {
            Some(slot) => std::mem::replace(slot, rate) != rate,
            None => {
                rates.insert(Arc::clone(topic), rate);
                true
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
    fn set_reports_whether_the_rate_changed() {
        let rates = RateStore::new();
        let topic: Arc<str> = Arc::from("orders");
        assert!(rates.set(&topic, 0.0), "a first rate is a change");
        assert!(!rates.set(&topic, 0.0));
        assert!(rates.set(&topic, 4.5));
        assert!(
            rates.set(&topic, 0.0),
            "a rate that falls to zero is a change"
        );
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
