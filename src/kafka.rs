//! Kafka I/O, assembled domain types, the catalog snapshot, and live record reads.
//!
//! Open these first.
//!
//! - [`ClusterSession`] is the per-cluster I/O port. `client` is the rdkafka
//!   wrapper ([`KafkaClient`]). `registry` is Schema Registry. `testing` is
//!   the in-memory session.
//! - Raw broker snapshots live in `metadata`, `group` (`GroupSnapshot`),
//!   `watermarks`, and `topic_config`.
//! - Assembled types live in `topic`, `broker`, `cluster`, `group`
//!   (`ConsumerGroup`), `search`, and `record`. `model` re-exports them.
//! - `catalog` stores and polls the product snapshot ([`ClusterSnapshot`],
//!   [`CatalogPoller`]). [`QueryEngine`] builds that snapshot (`assemble_catalog`)
//!   and serves live records, configs, one group, and subjects. `rates`,
//!   `lag`, and `series` are time series.

mod client;
mod registry;
mod session;

mod metadata;
mod topic_config;
mod watermarks;

mod broker;
mod cluster;
mod group;
mod limits;
mod record;
mod search;
mod topic;

mod catalog;
mod engine;
mod lag;
mod rates;
mod series;

mod error;
pub(crate) mod model;

#[cfg(test)]
mod testing;

pub use catalog::{
    CatalogAssemble, CatalogCache, CatalogHealth, CatalogPoller, CatalogPollerIntervals,
    CatalogPollerIo, CatalogReuse, CatalogRevision, ClusterSnapshot, PollLane, SubjectCache,
};
pub use client::{KafkaClient, KafkaClusterConfig};
pub use engine::QueryEngine;
pub use error::{KafkaError, QueryError};
pub use lag::LagStore;
pub use limits::RecordLimits;
pub use model::{
    Broker, ClusterHealth, ClusterIdentity, ClusterOverview, ConfigEntry, ConsumerGroup, Record,
    RecordPage, RecordQuery, SearchHit, TimestampRange, Topic,
};
pub use rates::{RateStore, TopicRate};
pub use record::cursor::RecordCursor;
pub use record::filter::{RecordFilter, compile as compile_record_filter};
pub use series::ThroughputPoint;
pub use session::ClusterSession;

#[cfg(test)]
pub use testing::{CountingSession, FakeCluster};
