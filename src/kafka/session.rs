//! Per-cluster Kafka I/O port.
//!
//! The query engine talks only to [`ClusterSession`]. Production is
//! [`super::client::KafkaClient`]. Tests use [`super::testing::FakeCluster`].

use std::collections::HashMap;
use std::time::Duration;

use async_trait::async_trait;

use crate::kafka::error::KafkaError;
use crate::kafka::model::{
    ClusterIdentity, CommittedOffset, ConfigEntry, FetchPlan, GroupSnapshot, MetadataSnapshot,
    Record, SchemaSubject, Watermarks,
};

/// Per-cluster Kafka I/O. Matches [`super::client::KafkaClient`].
#[async_trait]
pub trait ClusterSession: Send + Sync + 'static {
    fn identity(&self) -> &ClusterIdentity;

    async fn metadata(&self) -> Result<MetadataSnapshot, KafkaError>;

    /// Low and high watermarks for the given partitions.
    ///
    /// The caller supplies partitions from a metadata snapshot it already
    /// has. This method does not refetch cluster metadata.
    async fn watermarks(
        &self,
        partitions: &[(String, i32)],
    ) -> Result<HashMap<String, HashMap<i32, Watermarks>>, KafkaError>;

    /// Earliest offset at or after `timestamp` (unix ms) for each answered
    /// partition.
    ///
    /// `None` is Kafka's invalid offset (nothing at or after that time). A
    /// missing key means the broker omitted that partition.
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

    async fn groups(&self) -> Result<Vec<GroupSnapshot>, KafkaError>;

    /// Snapshot for one consumer group.
    ///
    /// The default scans [`groups`](Self::groups). Live clusters override
    /// this with a single-group broker fetch.
    async fn group(&self, id: &str) -> Result<GroupSnapshot, KafkaError> {
        self.groups()
            .await?
            .into_iter()
            .find(|group| group.id == id)
            .ok_or_else(|| KafkaError::UnknownGroup {
                cluster: self.identity().name.clone(),
                id: id.to_owned(),
            })
    }

    async fn committed_offsets(
        &self,
        group_id: &str,
        partitions: &[(String, i32)],
    ) -> Result<Vec<CommittedOffset>, KafkaError>;

    /// Fully scan the plan's half-open windows, then return at most `limit`
    /// matching records sorted by `order`. An incomplete scan must return an
    /// error, not a partial batch: pagination advances past underfilled windows.
    async fn records(&self, plan: &FetchPlan) -> Result<Vec<Record>, KafkaError>;

    async fn schema_subjects(&self) -> Result<Vec<SchemaSubject>, KafkaError> {
        Ok(Vec::new())
    }

    fn consume_timeout(&self) -> Duration {
        *crate::environment::CONSUME_TIMEOUT
    }
}
