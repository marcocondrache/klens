mod client;
mod registry;
mod session;

mod metadata;
mod topic_config;

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

pub use cluster::{Cluster, Clusters};
pub use error::{KafkaError, QueryError};
pub use limits::{RecordLimits, TailLimits};
pub use model::{ConfigEntry, RecordPage, RecordQuery};
pub use scan::cursor::RecordCursor;
pub use scan::filter::{CompiledFilter, contains as compile_contains_filter};
pub use scan::tail::{Tail, TailBatch, TailPosition, TailQuery};
pub use session::ClusterSession;

#[cfg(test)]
pub use testing::{FakeCluster, FixtureRecord, card_record};
