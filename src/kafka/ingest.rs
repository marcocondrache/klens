//! The ingestion lanes that fill the [store](crate::kafka::store).
//!
//! Five independent per-cluster loops, each fetching at its own cadence,
//! diffing against the previous table, swapping it, and publishing a typed
//! delta.
//!
//! | Lane       | Cadence  | Emits                                     |
//! | ---------- | -------- | ----------------------------------------- |
//! | Topology   | ~10s     | added / removed / changed topics & groups |
//! | Watermarks | ~3s      | per-topic produce rates                   |
//! | Offsets    | adaptive | per-group committed offsets and lag       |
//! | Configs    | ~60s     | changed topic configs                     |
//! | Subjects   | ~30s     | registry listing changes                  |

pub mod configs;
pub mod offsets;
pub mod runner;
pub mod subjects;
pub mod topology;
pub mod watermarks;

use std::sync::Arc;

use tokio::task::JoinSet;

use crate::kafka::session::ClusterSession;
use crate::kafka::store::{ClusterStore, StoreSet};

pub use configs::ConfigLane;
pub use offsets::{OffsetLane, Wave};
pub use runner::{LaneSource, run};
pub use subjects::SubjectLane;
pub use topology::TopologyLane;
pub use watermarks::WatermarkLane;

/// Owns every lane task. Dropping it aborts them, so a store and its
/// ingestion have the same lifetime.
pub struct Ingest {
    tasks: JoinSet<()>,
}

impl Ingest {
    pub fn start(
        clusters: impl IntoIterator<Item = (Arc<ClusterStore>, Arc<dyn ClusterSession>)>,
    ) -> Self {
        use crate::environment::{
            CONFIG_LANE_INTERVAL, SUBJECT_LANE_INTERVAL, TOPOLOGY_LANE_INTERVAL,
            WATERMARK_LANE_INTERVAL,
        };

        let mut tasks = JoinSet::new();

        for (store, session) in clusters {
            tracing::info!(
                cluster = %store.name(),
                topology_secs = TOPOLOGY_LANE_INTERVAL.as_secs(),
                watermark_secs = WATERMARK_LANE_INTERVAL.as_secs(),
                config_secs = CONFIG_LANE_INTERVAL.as_secs(),
                subject_secs = SUBJECT_LANE_INTERVAL.as_secs(),
                "starting ingestion lanes"
            );

            tasks.spawn(run(
                Arc::clone(&store),
                TopologyLane::new(Arc::clone(&session)),
            ));
            tasks.spawn(run(
                Arc::clone(&store),
                WatermarkLane::new(Arc::clone(&session)),
            ));
            tasks.spawn(run(
                Arc::clone(&store),
                ConfigLane::new(Arc::clone(&session)),
            ));
            tasks.spawn(run(
                Arc::clone(&store),
                SubjectLane::new(Arc::clone(&session)),
            ));
            tasks.spawn(OffsetLane::new(Arc::clone(&session)).run(Arc::clone(&store)));
        }

        Self { tasks }
    }

    /// Builds a store per session and starts every lane against it.
    pub fn bootstrap(sessions: Vec<Arc<dyn ClusterSession>>) -> (Arc<StoreSet>, Self) {
        let stores = Arc::new(StoreSet::new(
            sessions
                .iter()
                .map(|session| session.identity().clone())
                .collect::<Vec<_>>(),
        ));
        let clusters: Vec<(Arc<ClusterStore>, Arc<dyn ClusterSession>)> = sessions
            .into_iter()
            .filter_map(|session| {
                let store = stores.get(&session.identity().name)?;
                Some((Arc::clone(store), session))
            })
            .collect();

        let ingest = Self::start(clusters);
        (stores, ingest)
    }

    pub fn lane_count(&self) -> usize {
        self.tasks.len()
    }
}

#[cfg(test)]
mod tests;
