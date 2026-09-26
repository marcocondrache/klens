pub mod configs;
pub mod offsets;
pub mod runner;
pub mod subjects;
pub mod topology;
pub mod watermarks;

use std::sync::Arc;
use std::time::Duration;

use tokio::task::JoinSet;

use crate::kafka::cluster::{Cluster, Clusters};

pub use configs::ConfigLane;
pub use offsets::{OffsetLane, Wave};
pub use runner::{Fetch, LaneSource, run};
pub use subjects::SubjectLane;
pub use topology::TopologyLane;
pub use watermarks::WatermarkLane;

pub struct Ingest {
    tasks: JoinSet<()>,
}

impl Ingest {
    pub fn start(clusters: &Clusters) -> Self {
        let mut tasks = JoinSet::new();

        for Cluster {
            session,
            store,
            ingest,
        } in clusters.iter()
        {
            tracing::info!(
                cluster = %store.name(),
                topology_secs = ingest.topology_secs,
                watermark_secs = ingest.watermark_secs,
                config_secs = ingest.config_secs,
                subject_secs = ingest.subjects_secs,
                "starting ingestion lanes"
            );

            tasks.spawn(run(
                Arc::clone(store),
                TopologyLane::with_interval(
                    Arc::clone(session),
                    Duration::from_secs(ingest.topology_secs),
                ),
            ));
            tasks.spawn(run(
                Arc::clone(store),
                WatermarkLane::with_interval(
                    Arc::clone(session),
                    Duration::from_secs(ingest.watermark_secs),
                ),
            ));
            tasks.spawn(run(
                Arc::clone(store),
                ConfigLane::with_interval(
                    Arc::clone(session),
                    Duration::from_secs(ingest.config_secs),
                ),
            ));
            tasks.spawn(run(
                Arc::clone(store),
                SubjectLane::with_interval(
                    Arc::clone(session),
                    Duration::from_secs(ingest.subjects_secs),
                ),
            ));
            tasks.spawn(
                OffsetLane::new(Arc::clone(session))
                    .with_tiers(
                        Duration::from_secs(ingest.offset_tick_secs),
                        Duration::from_secs(ingest.fast_offset_secs),
                        Duration::from_secs(ingest.slow_offset_secs),
                    )
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
