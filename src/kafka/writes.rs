mod reset;

use async_trait::async_trait;

use crate::kafka::error::KafkaError;
use crate::kafka::group::CommittedOffset;

pub use reset::{OffsetMove, ResetScope, ResetTarget, plan_reset};

#[async_trait]
pub trait ClusterWrites: Send + Sync {
    async fn commit_group_offsets(
        &self,
        group: &str,
        offsets: &[CommittedOffset],
    ) -> Result<(), KafkaError>;

    async fn delete_group_offsets(
        &self,
        group: &str,
        partitions: &[(String, i32)],
    ) -> Result<(), KafkaError>;
}
