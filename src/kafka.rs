//! Kafka I/O, the versioned read model, assembled domain types, and live
//! record reads.
//!
//! Open these first.
//!
//! - [`ClusterSession`] is the per-cluster I/O port. `client` is the broker
//!   adapter ([`KafkaClient`]). `registry` is Schema Registry. `testing` is
//!   the in-memory session.
//! - Raw broker snapshots live in `metadata`, `group` (`GroupSnapshot`),
//!   `watermarks`, and `topic_config`.
//! - [`store`] is the v2 read model: normalized tables behind versioned
//!   lanes, read-time projections, a server-timestamped series store, and a
//!   per-cluster change bus. [`ingest`] fills it from five independent
//!   per-cluster loops.
//! - Assembled types live in `topic`, `broker`, `cluster`, `group`
//!   (`ConsumerGroup`), `search`, and `scan`. `model` re-exports them.
//! - `catalog` stores and polls the v1 product snapshot ([`ClusterSnapshot`],
//!   [`CatalogPoller`]). [`QueryEngine`] builds that snapshot (`assemble_catalog`)
//!   and serves live records, configs, one group, and subjects. `rates`,
//!   `lag`, and `series` are its time series.

mod client;
mod registry;
mod session;

mod metadata;
mod topic_config;
mod watermarks;

mod acl;
mod broker;
mod cluster;
mod group;
mod limits;
mod scan;
mod search;
mod topic;

mod catalog;
mod engine;
pub mod ingest;
mod lag;
mod rates;
mod series;
pub mod store;

mod error;
pub(crate) mod model;

#[cfg(test)]
mod testing;

pub use catalog::{
    CatalogAssemble, CatalogCache, CatalogHealth, CatalogPoller, CatalogPollerIntervals,
    CatalogPollerIo, CatalogReuse, CatalogRevision, ClusterSnapshot, PollLane, SubjectCache,
};
pub use client::KafkaClient;
pub use engine::QueryEngine;
pub use error::{KafkaError, QueryError};
pub use lag::LagStore;
pub use limits::RecordLimits;
pub use model::{
    Broker, ClusterHealth, ClusterIdentity, ClusterOverview, ConfigEntry, ConsumerGroup, Record,
    RecordPage, RecordQuery, SearchHit, TimestampRange, Topic,
};
pub use rates::{RateStore, TopicRate};
pub use scan::cursor::{CursorDirection, RecordCursor};
pub use scan::filter::{
    CompiledFilter, cel as compile_cel_filter, contains as compile_contains_filter,
};
pub use series::ThroughputPoint;
pub use session::ClusterSession;

#[cfg(test)]
pub use testing::FakeCluster;
