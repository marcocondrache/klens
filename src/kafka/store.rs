//! Normalized, versioned, in-memory read model.
//!
//! Phase 1 of the v2 architecture. Each cluster owns independently swappable
//! tables, a series store, a change bus, and an interest registry. Ingestion
//! lanes in [`super::ingest`] feed this store. The existing GraphQL API is
//! unchanged.

mod bus;
mod cluster;
mod interest;
mod lane;
mod search;
mod series;
mod tables;

pub use bus::{
    Change, ConfigsDelta, GroupOffsetsWave, SubjectsChanged, TopologyDelta, WatermarksTick,
};
pub use cluster::{ClusterStore, StoreSet, group_lag_update};
pub use lane::Lane;
pub use tables::{ConfigTable, GroupOffsets, SubjectInfo, SubjectTable, Topology, WatermarkTable};

#[cfg(test)]
mod tests;
