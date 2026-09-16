use std::sync::Arc;
use std::time::Duration;

use crate::environment::CONFIG_LANE_INTERVAL;
use crate::kafka::error::KafkaError;
use crate::kafka::session::ClusterSession;
use crate::kafka::store::{ClusterStore, ConfigTable, ConfigsDelta};

use super::runner::LaneSource;

pub struct ConfigSource {
    session: Arc<dyn ClusterSession>,
    store: Arc<ClusterStore>,
}

impl ConfigSource {
    pub fn new(session: Arc<dyn ClusterSession>, store: Arc<ClusterStore>) -> Self {
        Self { session, store }
    }
}

impl LaneSource for ConfigSource {
    type Table = ConfigTable;
    type Delta = ConfigsDelta;

    async fn fetch(&self, _prev: Option<&Arc<ConfigTable>>) -> Result<ConfigTable, KafkaError> {
        let Some(topology) = self.store.topology.load() else {
            return Ok(ConfigTable::default());
        };
        let names: Vec<&str> = topology.topics.keys().map(Arc::as_ref).collect();
        let fetched = self.session.topic_configs(&names).await?;
        let mut topics = std::collections::HashMap::new();
        for (name, entries) in fetched {
            let key = topology.intern_topic(&name);
            topics.insert(key, Arc::new(entries));
        }
        Ok(ConfigTable { topics })
    }

    fn diff(&self, prev: Option<&ConfigTable>, next: &ConfigTable) -> Option<ConfigsDelta> {
        let mut changed = Vec::new();
        match prev {
            None => {
                if next.topics.is_empty() {
                    return None;
                }
                changed.extend(next.topics.keys().cloned());
            }
            Some(prev) => {
                for (name, entries) in &next.topics {
                    if prev.topics.get(name) != Some(entries) {
                        changed.push(Arc::clone(name));
                    }
                }
                for name in prev.topics.keys() {
                    if !next.topics.contains_key(name) {
                        changed.push(Arc::clone(name));
                    }
                }
            }
        }
        if changed.is_empty() {
            return None;
        }
        changed.sort();
        changed.dedup();
        Some(ConfigsDelta { topics: changed })
    }

    fn interval(&self) -> Duration {
        *CONFIG_LANE_INTERVAL
    }
}
