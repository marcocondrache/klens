use std::collections::HashMap;
use std::time::Duration;

use async_trait::async_trait;
use futures::future::join_all;

use crate::kafka::error::KafkaError;
use crate::kafka::model::{
    ClusterIdentity, CommittedOffset, ConfigEntry, FetchPlan, GroupSnapshot, MetadataSnapshot,
    Record, SchemaSubject, Watermarks,
};

/// Per-cluster Kafka I/O. The query engine talks only to this port.
#[async_trait]
pub trait ClusterSession: Send + Sync + 'static {
    fn identity(&self) -> &ClusterIdentity;

    async fn metadata(&self) -> Result<MetadataSnapshot, KafkaError>;

    async fn watermarks(&self, topic: &str) -> Result<HashMap<i32, Watermarks>, KafkaError> {
        Ok(self
            .watermarks_many(&[topic])
            .await
            .remove(topic)
            .unwrap_or_default())
    }

    /// Low/high watermarks for many topics in one sweep.
    ///
    /// The default joins per-topic [`watermarks`](Self::watermarks) calls.
    /// Live clusters override this with batched `ListOffsets`.
    async fn watermarks_many(&self, topics: &[&str]) -> HashMap<String, HashMap<i32, Watermarks>> {
        join_all(topics.iter().map(|name| async move {
            let watermarks = self.watermarks(name).await.unwrap_or_default();
            ((*name).to_owned(), watermarks)
        }))
        .await
        .into_iter()
        .collect()
    }

    /// Earliest offset at or after `timestamp` (unix ms) for each partition.
    ///
    /// `None` means the broker has no message at or after that time (the Kafka
    /// `ListOffsets` invalid offset).
    async fn offsets_for_times(
        &self,
        topic: &str,
        partitions: &[i32],
        timestamp: i64,
    ) -> Result<HashMap<i32, Option<i64>>, KafkaError>;

    async fn topic_configs(
        &self,
        topics: &[&str],
    ) -> Result<HashMap<String, Vec<ConfigEntry>>, KafkaError>;

    async fn broker_configs(&self, broker_id: i32) -> Result<Vec<ConfigEntry>, KafkaError>;

    async fn consumer_groups(&self) -> Result<Vec<GroupSnapshot>, KafkaError>;

    async fn committed_offsets(
        &self,
        group_id: &str,
        partitions: &[(String, i32)],
    ) -> Result<Vec<CommittedOffset>, KafkaError>;

    async fn records(&self, plan: &FetchPlan) -> Result<Vec<Record>, KafkaError>;

    async fn schema_subjects(&self) -> Result<Vec<SchemaSubject>, KafkaError> {
        Ok(Vec::new())
    }

    fn consume_timeout(&self) -> Duration {
        *crate::environment::CONSUME_TIMEOUT
    }
}
