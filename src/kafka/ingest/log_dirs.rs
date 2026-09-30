use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;

use crate::kafka::error::KafkaError;
use crate::kafka::session::ClusterSession;
use crate::kafka::store::{Change, ClusterStore, Interner, Lane, LogDirTable, LogDirsDelta};

use super::runner::{Fetch, LaneSource};

pub struct LogDirLane {
    session: Arc<dyn ClusterSession>,
    interval: Duration,
}

impl LogDirLane {
    pub fn with_interval(session: Arc<dyn ClusterSession>, interval: Duration) -> Self {
        Self { session, interval }
    }
}

#[async_trait]
impl LaneSource for LogDirLane {
    type Table = LogDirTable;
    type Delta = LogDirsDelta;

    fn name(&self) -> &'static str {
        "log_dirs"
    }

    fn interval(&self) -> Duration {
        self.interval
    }

    fn lane<'a>(&self, store: &'a ClusterStore) -> &'a Lane<LogDirTable> {
        &store.log_dirs
    }

    async fn fetch(
        &self,
        store: &ClusterStore,
        _previous: Option<&Arc<LogDirTable>>,
    ) -> Result<Fetch<LogDirTable>, KafkaError> {
        // The admin client learns the brokers it asks from the metadata the
        // topology lane refreshes.
        let Some(topology) = store.topology.load() else {
            return Ok(Fetch::Awaiting);
        };

        let dirs = self.session.log_dirs().await?;
        Ok(Fetch::Ready(LogDirTable::assemble(
            dirs,
            &mut Interner::seeded(topology.topics.keys()),
        )))
    }

    fn diff(&self, previous: Option<&LogDirTable>, next: &LogDirTable) -> Option<LogDirsDelta> {
        LogDirsDelta::between(previous, next)
    }

    fn publish(
        &self,
        store: &ClusterStore,
        _previous: Option<&Arc<LogDirTable>>,
        _next: &Arc<LogDirTable>,
        delta: LogDirsDelta,
    ) {
        store.bus.publish(Change::LogDirs(Arc::new(delta)));
    }
}
