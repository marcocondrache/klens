//! The normalized, versioned read model.
//!
//! One [`ClusterStore`] per cluster holds five independently swappable
//! tables, a server-timestamped series store, a per-cluster change bus, and
//! the interest registry that drives the offsets lane's fast tier.
//!
//! - [`Lane`] is the swap/version/health container every table lives in.
//! - `tables` holds the tables themselves. Nothing derived is stored: lag,
//!   counts, rates and under-replicated flags are [`projections`] computed at
//!   read time.
//! - [`ChangeBus`] carries the typed deltas the lanes publish.
//! - [`crate::kafka::ingest`] fills all of it.

pub mod bus;
pub mod cluster;
pub mod interest;
pub mod lane;
pub mod projections;
pub mod search;
pub mod series;
pub mod tables;

#[cfg(test)]
pub mod fixtures;

pub use bus::{
    Change, ChangeBus, ConfigsDelta, GroupLagUpdate, GroupOffsetsWave, SubjectsDelta, TopicRate,
    TopologyDelta, WatermarksTick,
};
pub use cluster::{ClusterStore, StoreSet};
pub use interest::{InterestLease, InterestRegistry};
pub use lane::{Lane, LaneHealth};
pub use projections::{
    BrokerRow, ClusterHealthView, GroupDetail, GroupRow, PartitionRow, SubjectRow, TopicDetail,
    TopicGroupRow, TopicRow,
};
pub use search::SearchIndex;
pub use series::{Point, SeriesStore};
pub use tables::{
    BrokerInfo, ConfigTable, GroupInfo, GroupOffsets, Interner, OffsetTable, SubjectInfo,
    SubjectTable, TopicInfo, Topology, WatermarkTable,
};
