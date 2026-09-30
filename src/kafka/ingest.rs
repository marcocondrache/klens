pub mod configs;
pub mod log_dirs;
pub mod offsets;
pub mod runner;
pub mod subjects;
pub mod topology;
pub mod watermarks;

use std::sync::Arc;

use tokio::task::JoinSet;

use crate::config::IngestTuning;
use crate::kafka::cluster::{Cluster, Clusters};

pub use configs::ConfigLane;
pub use log_dirs::LogDirLane;
pub use offsets::{OffsetLane, Wave};
pub use runner::{LaneSource, run};
pub use subjects::SubjectLane;
pub use topology::TopologyLane;
pub use watermarks::WatermarkLane;

#[must_use = "dropping an Ingest aborts its lanes"]
pub struct Ingest {
    tasks: JoinSet<()>,
}

impl Ingest {
    pub fn start(clusters: &Clusters, tuning: &IngestTuning) -> Self {
        let mut tasks = JoinSet::new();

        for Cluster { session, store, .. } in clusters.iter() {
            tracing::info!(cluster = %store.name(), "starting ingestion lanes");

            tasks.spawn(run(
                Arc::clone(store),
                TopologyLane::with_interval(Arc::clone(session), tuning.topology),
            ));
            tasks.spawn(run(
                Arc::clone(store),
                WatermarkLane::with_interval(Arc::clone(session), tuning.watermark, tuning),
            ));
            tasks.spawn(run(
                Arc::clone(store),
                ConfigLane::with_interval(Arc::clone(session), tuning.config),
            ));
            tasks.spawn(run(
                Arc::clone(store),
                SubjectLane::with_interval(Arc::clone(session), tuning.subjects),
            ));
            tasks.spawn(run(
                Arc::clone(store),
                LogDirLane::with_interval(Arc::clone(session), tuning.log_dirs),
            ));
            tasks.spawn(
                OffsetLane::new(Arc::clone(session))
                    .with_tiers(tuning.offset_tick, tuning.fast_offset, tuning.slow_offset)
                    .with_concurrency(tuning.offset_fetch_concurrency)
                    .run(Arc::clone(store)),
            );
        }

        Self { tasks }
    }

    pub fn lane_count(&self) -> usize {
        self.tasks.len()
    }
}

#[cfg(test)]
mod tests;
