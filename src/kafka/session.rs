use std::collections::HashMap;
use std::time::Duration;

use async_trait::async_trait;

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

    async fn watermarks(
        &self,
        topic: &str,
        partitions: &[i32],
    ) -> Result<HashMap<i32, Watermarks>, KafkaError>;

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
