use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;

use crate::environment::TOPOLOGY_LANE_INTERVAL;
use crate::kafka::error::KafkaError;
use crate::kafka::session::ClusterSession;
use crate::kafka::store::{Change, ClusterStore, Interner, Lane, Topology, TopologyDelta};

use super::runner::{LaneSource, floor};

/// Metadata plus consumer-group membership, the slowest-moving shape in the
/// cluster.
///
/// Diffing the sorted tables replaces the old metadata hash: "nothing
/// changed" costs one lockstep walk and no downstream work, and when
/// something did change the granular delta falls out for free.
pub struct TopologyLane {
    session: Arc<dyn ClusterSession>,
    interval: Duration,
}

impl TopologyLane {
    pub fn new(session: Arc<dyn ClusterSession>) -> Self {
        Self::with_interval(session, *TOPOLOGY_LANE_INTERVAL)
    }

    pub fn with_interval(session: Arc<dyn ClusterSession>, interval: Duration) -> Self {
        Self {
            session,
            interval: floor(interval),
        }
    }
}

#[async_trait]
impl LaneSource for TopologyLane {
    type Table = Topology;
    type Delta = TopologyDelta;

    fn name(&self) -> &'static str {
        "topology"
    }

    fn interval(&self) -> Duration {
        self.interval
    }

    fn lane<'a>(&self, store: &'a ClusterStore) -> &'a Lane<Topology> {
        &store.topology
    }

    async fn fetch(
        &self,
        _store: &ClusterStore,
        previous: Option<&Arc<Topology>>,
    ) -> Result<Option<Topology>, KafkaError> {
        let (meta, groups) = tokio::try_join!(self.session.metadata(), self.session.groups())?;

        let mut interner = match previous {
            Some(previous) => {
                Interner::seeded(previous.topics.keys().chain(previous.groups.keys()))
            }
            None => Interner::default(),
        };
        Ok(Some(Topology::assemble(&meta, &groups, &mut interner)))
    }

    fn diff(&self, previous: Option<&Topology>, next: &Topology) -> Option<TopologyDelta> {
        TopologyDelta::between(previous, next)
    }

    fn publish(
        &self,
        store: &ClusterStore,
        version: u64,
        _previous: Option<&Arc<Topology>>,
        next: &Arc<Topology>,
        mut delta: TopologyDelta,
    ) {
        delta.version = version;

        // Membership in the topology is the series retention policy: a topic
        // or group the cluster no longer reports loses its ring here.
        if !delta.removed_topics.is_empty() {
            store
                .series
                .retain_topics(|topic| next.topics.contains_key(topic));
        }
        if !delta.removed_groups.is_empty() {
            store
                .series
                .retain_groups(|group| next.groups.contains_key(group));
        }

        store.rebuild_search();
        store.bus.publish(Change::Topology(Arc::new(delta)));
    }
}
