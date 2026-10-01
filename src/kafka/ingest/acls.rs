use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;

use crate::kafka::acl::AclListing;
use crate::kafka::error::KafkaError;
use crate::kafka::session::ClusterSession;
use crate::kafka::store::{Change, ClusterStore, Lane};

use super::runner::LaneSource;

pub struct AclLane {
    session: Arc<dyn ClusterSession>,
    interval: Duration,
}

impl AclLane {
    pub fn with_interval(session: Arc<dyn ClusterSession>, interval: Duration) -> Self {
        Self { session, interval }
    }
}

#[async_trait]
impl LaneSource for AclLane {
    type Upstream = ();
    type Table = AclListing;
    type Delta = ();

    fn name(&self) -> &'static str {
        "acls"
    }

    fn interval(&self) -> Duration {
        self.interval
    }

    fn lane<'a>(&self, store: &'a ClusterStore) -> &'a Lane<AclListing> {
        &store.acls
    }

    async fn fetch(
        &self,
        _store: &ClusterStore,
        (): &(),
        _previous: Option<&Arc<AclListing>>,
    ) -> Result<AclListing, KafkaError> {
        self.session.acls().await
    }

    fn diff(&self, previous: Option<&AclListing>, next: &AclListing) -> Option<()> {
        (previous != Some(next)).then_some(())
    }

    fn publish(
        &self,
        store: &ClusterStore,
        _previous: Option<&Arc<AclListing>>,
        _next: &Arc<AclListing>,
        (): (),
    ) {
        store.bus.publish(Change::Acls);
    }
}
