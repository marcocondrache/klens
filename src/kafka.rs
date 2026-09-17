//! Kafka I/O, the versioned read model, and live record reads.
//!
//! Open these first.
//!
//! - [`ClusterSession`] is the per-cluster I/O port and [`SessionSet`] holds
//!   one per configured cluster. `client` is the broker adapter
//!   ([`KafkaClient`]). `registry` is Schema Registry. `testing` is the
//!   in-memory session.
//! - Raw broker snapshots live in `metadata`, `group` (`GroupSnapshot`),
//!   `watermarks`, and `topic_config`.
//! - [`store`] is the read model: normalized tables behind versioned lanes,
//!   read-time projections, a server-timestamped series store, and a
//!   per-cluster change bus. [`ingest`] fills it from five independent
//!   per-cluster loops.

mod client;
mod registry;
mod session;

mod metadata;
mod topic_config;
mod watermarks;

mod acl;
mod cluster;
mod group;
mod limits;
mod scan;

pub mod ingest;
pub mod store;

mod error;
pub(crate) mod model;

#[cfg(test)]
mod testing;

pub use client::KafkaClient;
pub use error::{KafkaError, QueryError};
pub use limits::RecordLimits;
pub use model::{ConfigEntry, RecordPage, RecordQuery};
pub use scan::cursor::RecordCursor;
pub use scan::filter::{
    CompiledFilter, cel as compile_cel_filter, contains as compile_contains_filter,
};
pub use scan::read::read_page;
pub use session::{ClusterSession, SessionSet};

#[cfg(test)]
pub use testing::{FakeCluster, card_record};
