use std::sync::Arc;
use std::time::Duration;

use foldhash::HashMap;

use async_trait::async_trait;

use crate::kafka::error::KafkaError;
use crate::kafka::session::ClusterSession;
use crate::kafka::store::{Change, ClusterStore, ConfigTable, ConfigsDelta, Lane};

use super::runner::{LaneSource, floor};

pub struct ConfigLane {
    session: Arc<dyn ClusterSession>,
    interval: Duration,
}

impl ConfigLane {
    pub fn with_interval(session: Arc<dyn ClusterSession>, interval: Duration) -> Self {
        Self {
            session,
            interval: floor(interval),
        }
    }
}

#[async_trait]
impl LaneSource for ConfigLane {
    type Table = ConfigTable;
    type Delta = ConfigsDelta;

    fn name(&self) -> &'static str {
        "configs"
    }

    fn interval(&self) -> Duration {
        self.interval
    }

    fn lane<'a>(&self, store: &'a ClusterStore) -> &'a Lane<ConfigTable> {
        &store.configs
    }

    async fn fetch(
        &self,
        store: &ClusterStore,
        previous: Option<&Arc<ConfigTable>>,
    ) -> Result<Option<ConfigTable>, KafkaError> {
        let Some(topology) = store.topology.load() else {
            return Ok(None);
        };

        let names: Vec<&str> = topology.topics.keys().map(AsRef::as_ref).collect();
        let mut fetched = self.session.topic_configs(&names).await?;

        let topics: HashMap<Arc<str>, Arc<[crate::kafka::topic_config::ConfigEntry]>> = topology
            .topics
            .keys()
            .filter_map(|name| {
                let entries = fetched.remove(name.as_ref())?;
                let entries = match previous.and_then(|table| table.topics.get(name)) {
                    Some(existing) if **existing == *entries => Arc::clone(existing),
                    _ => Arc::from(entries),
                };
                Some((Arc::clone(name), entries))
            })
            .collect();

        Ok(Some(ConfigTable { topics }))
    }

    fn diff(&self, previous: Option<&ConfigTable>, next: &ConfigTable) -> Option<ConfigsDelta> {
        ConfigsDelta::between(previous, next)
    }

    fn publish(
        &self,
        store: &ClusterStore,
        version: u64,
        _previous: Option<&Arc<ConfigTable>>,
        _next: &Arc<ConfigTable>,
        mut delta: ConfigsDelta,
    ) {
        delta.version = version;
        store.bus.publish(Change::Configs(Arc::new(delta)));
    }
}
