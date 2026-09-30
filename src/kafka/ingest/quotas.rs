use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;

use crate::kafka::error::KafkaError;
use crate::kafka::quota::QuotaListing;
use crate::kafka::session::ClusterSession;
use crate::kafka::store::{Change, ClusterStore, Lane};

use super::runner::LaneSource;

pub struct QuotaLane {
    session: Arc<dyn ClusterSession>,
    interval: Duration,
}

impl QuotaLane {
    pub fn with_interval(session: Arc<dyn ClusterSession>, interval: Duration) -> Self {
        Self { session, interval }
    }
}

#[async_trait]
impl LaneSource for QuotaLane {
    type Upstream = ();
    type Table = QuotaListing;
    type Delta = ();

    fn name(&self) -> &'static str {
        "quotas"
    }

    fn interval(&self) -> Duration {
        self.interval
    }

    fn lane<'a>(&self, store: &'a ClusterStore) -> &'a Lane<QuotaListing> {
        &store.quotas
    }

    async fn fetch(
        &self,
        _store: &ClusterStore,
        (): &(),
        _previous: Option<&Arc<QuotaListing>>,
    ) -> Result<QuotaListing, KafkaError> {
        self.session.client_quotas().await
    }

    fn diff(&self, previous: Option<&QuotaListing>, next: &QuotaListing) -> Option<()> {
        (previous != Some(next)).then_some(())
    }

    fn publish(
        &self,
        store: &ClusterStore,
        _previous: Option<&Arc<QuotaListing>>,
        _next: &Arc<QuotaListing>,
        (): (),
    ) {
        store.bus.publish(Change::Quotas);
    }
}
