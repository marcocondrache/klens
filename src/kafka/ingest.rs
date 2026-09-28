pub mod configs;
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
pub use offsets::{OffsetLane, Wave};
pub use runner::{Fetch, LaneSource, run};
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

        for Cluster {
            session,
            store,
            ingest,
        } in clusters.iter()
        {
            tracing::info!(
                cluster = %store.name(),
                topology = ?ingest.topology,
                watermark = ?ingest.watermark,
                config = ?ingest.config,
                subjects = ?ingest.subjects,
                "starting ingestion lanes"
            );

            tasks.spawn(run(
                Arc::clone(store),
                TopologyLane::with_interval(Arc::clone(session), ingest.topology),
            ));
            tasks.spawn(run(
                Arc::clone(store),
                WatermarkLane::with_interval(Arc::clone(session), ingest.watermark, tuning),
            ));
            tasks.spawn(run(
                Arc::clone(store),
                ConfigLane::with_interval(Arc::clone(session), ingest.config),
            ));
            tasks.spawn(run(
                Arc::clone(store),
                SubjectLane::with_interval(Arc::clone(session), ingest.subjects),
            ));
            tasks.spawn(
                OffsetLane::new(Arc::clone(session))
                    .with_tiers(ingest.offset_tick, ingest.fast_offset, ingest.slow_offset)
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
