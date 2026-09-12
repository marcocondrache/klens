//! Product snapshot cache and poller.
//!
//! [`ClusterSnapshot`] is the assembled catalog. [`CatalogCache`] and
//! [`SubjectCache`] hold the last good poll. [`CatalogPoller`] refreshes them.
//! [`crate::kafka::QueryEngine::assemble_catalog`] builds a snapshot. This module
//! does not.

mod cache;
mod poller;
mod snapshot;

pub use cache::{CatalogCache, CatalogHealth, CatalogRevision, PollLane, SubjectCache};
pub use poller::{
    CatalogAssemble, CatalogPoller, CatalogPollerIntervals, CatalogPollerIo, CatalogReuse,
};
pub use snapshot::ClusterSnapshot;

#[cfg(test)]
mod tests;
