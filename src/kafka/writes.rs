//! Per-cluster Kafka writes.
//!
//! [`ClusterWrites`] is the write half of the I/O port: every
//! [`super::ClusterSession`] implements it. Nothing here decides who may
//! write; the app layer checks privileges and the cluster's opt-in before it
//! calls in.

mod reset;

use async_trait::async_trait;

use crate::kafka::error::KafkaError;
use crate::kafka::group::CommittedOffset;

pub use reset::{OffsetMove, ResetScope, ResetTarget, plan_reset};

#[async_trait]
pub trait ClusterWrites: Send + Sync {
    /// Commits `offsets` on behalf of a group klens is not a member of.
    ///
    /// The broker refuses while the group has live members.
    async fn commit_group_offsets(
        &self,
        group: &str,
        offsets: &[CommittedOffset],
    ) -> Result<(), KafkaError>;

    /// Forgets the group's committed offsets on `partitions`.
    ///
    /// The broker refuses a partition whose topic a live member is still
    /// subscribed to.
    async fn delete_group_offsets(
        &self,
        group: &str,
        partitions: &[(String, i32)],
    ) -> Result<(), KafkaError>;
}
