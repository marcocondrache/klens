//! The ingestion lanes that fill the [store](crate::kafka::store).
//!
//! Five independent per-cluster loops, each fetching at its own cadence,
//! diffing against the previous table, swapping it, and publishing a typed
//! delta.
//!
//! Cadences default to [`crate::config::ClusterIngestConfig`] and can be
//! overridden per cluster.
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
use std::time::Duration;

use tokio::task::JoinSet;

use crate::config::ClusterIngestConfig;
use crate::kafka::session::ClusterSession;
use crate::kafka::store::{ClusterStore, StoreSet};

pub use configs::ConfigLane;
pub use offsets::{OffsetLane, Wave};
pub use runner::{LaneSource, run};
pub use subjects::SubjectLane;
pub use topology::TopologyLane;
pub use watermarks::WatermarkLane;

/// Per-lane cadences. Defaults match [`ClusterIngestConfig`]; tests override
/// them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LaneIntervals {
    pub topology: Duration,
    pub watermarks: Duration,
    pub offsets_tick: Duration,
    pub fast_offsets: Duration,
    pub slow_offsets: Duration,
    pub configs: Duration,
    pub subjects: Duration,
}

impl From<&ClusterIngestConfig> for LaneIntervals {
    fn from(config: &ClusterIngestConfig) -> Self {
        Self {
            topology: Duration::from_secs(config.topology_secs),
            watermarks: Duration::from_secs(config.watermark_secs),
            offsets_tick: Duration::from_secs(config.offset_tick_secs),
            fast_offsets: Duration::from_secs(config.fast_offset_secs),
            slow_offsets: Duration::from_secs(config.slow_offset_secs),
            configs: Duration::from_secs(config.config_secs),
            subjects: Duration::from_secs(config.subjects_secs),
        }
    }
}

impl Default for LaneIntervals {
    fn default() -> Self {
        Self::from(&ClusterIngestConfig::default())
    }
}

/// Owns every lane task. Dropping it aborts them, so a store and its
/// ingestion have the same lifetime.
pub struct Ingest {
    tasks: JoinSet<()>,
}

impl Ingest {
    pub fn start(
        clusters: impl IntoIterator<Item = (Arc<ClusterStore>, Arc<dyn ClusterSession>, LaneIntervals)>,
    ) -> Self {
        let mut tasks = JoinSet::new();

        for (store, session, intervals) in clusters {
            tracing::info!(
                cluster = %store.name(),
                topology_secs = intervals.topology.as_secs(),
                watermark_secs = intervals.watermarks.as_secs(),
                config_secs = intervals.configs.as_secs(),
                subject_secs = intervals.subjects.as_secs(),
                "starting ingestion lanes"
            );

            tasks.spawn(run(
                Arc::clone(&store),
                TopologyLane::with_interval(Arc::clone(&session), intervals.topology),
            ));
            tasks.spawn(run(
                Arc::clone(&store),
                WatermarkLane::with_interval(Arc::clone(&session), intervals.watermarks),
            ));
            tasks.spawn(run(
                Arc::clone(&store),
                ConfigLane::with_interval(Arc::clone(&session), intervals.configs),
            ));
            tasks.spawn(run(
                Arc::clone(&store),
                SubjectLane::with_interval(Arc::clone(&session), intervals.subjects),
            ));
            tasks.spawn(
                OffsetLane::new(Arc::clone(&session))
                    .with_tiers(
                        intervals.offsets_tick,
                        intervals.fast_offsets,
                        intervals.slow_offsets,
                    )
                    .run(Arc::clone(&store)),
            );
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
        let clusters: Vec<(Arc<ClusterStore>, Arc<dyn ClusterSession>, LaneIntervals)> = sessions
            .into_iter()
            .filter_map(|session| {
                let store = stores.get(&session.identity().name)?;
                Some((Arc::clone(store), session, LaneIntervals::default()))
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
