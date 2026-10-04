use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;

use crate::kafka::error::KafkaError;
use crate::kafka::scram::ScramListing;
use crate::kafka::session::ClusterSession;
use crate::kafka::store::{Change, ClusterStore, Lane};

use super::runner::LaneSource;

pub struct ScramUserLane {
    session: Arc<dyn ClusterSession>,
    interval: Duration,
}

impl ScramUserLane {
    pub fn with_interval(session: Arc<dyn ClusterSession>, interval: Duration) -> Self {
        Self { session, interval }
    }
}

#[async_trait]
impl LaneSource for ScramUserLane {
    type Upstream = ();
    type Table = ScramListing;
    type Delta = ();

    fn name(&self) -> &'static str {
        "scram_users"
    }

    fn interval(&self) -> Duration {
        self.interval
    }

    fn lane<'a>(&self, store: &'a ClusterStore) -> &'a Lane<ScramListing> {
        &store.scram_users
    }

    async fn fetch(
        &self,
        _store: &ClusterStore,
        (): &(),
        _previous: Option<&Arc<ScramListing>>,
    ) -> Result<ScramListing, KafkaError> {
        self.session.scram_users().await
    }

    fn diff(&self, previous: Option<&ScramListing>, next: &ScramListing) -> Option<()> {
        (previous != Some(next)).then_some(())
    }

    fn publish(
        &self,
        store: &ClusterStore,
        _previous: Option<&Arc<ScramListing>>,
        _next: &Arc<ScramListing>,
        (): (),
    ) {
        store.bus.publish(Change::ScramUsers);
    }
}

#[cfg(test)]
mod tests;
