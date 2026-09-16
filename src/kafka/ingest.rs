//! Independent ingestion lanes that feed [`super::store`].
//!
//! Each lane fetches, diffs, swaps its table, and publishes typed deltas.
//! The existing catalog poller is unchanged; these lanes land dark until
//! the GraphQL cutover.

mod configs;
mod offsets;
mod runner;
mod subjects;
mod topology;
mod watermarks;

pub use runner::IngestSet;

#[cfg(test)]
mod tests;
