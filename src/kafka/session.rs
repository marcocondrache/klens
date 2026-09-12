//! Per-cluster Kafka I/O port.
//!
//! The query engine talks only to [`ClusterSession`]. The production impl is
//! `ClusterHandle` in [`super::adapter`].

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
        let partitions = self.metadata().await?.topic_partition_pairs(&[topic]);
        Ok(self
            .watermarks_many(&partitions)
            .await
            .remove(topic)
            .unwrap_or_default())
    }

    /// Low and high watermarks for the given partitions in one sweep.
    ///
    /// The caller supplies partitions from a metadata snapshot it already
    /// has. This method does not refetch cluster metadata.
    ///
    /// The default groups those partitions by topic and calls
    /// [`watermarks`](Self::watermarks). Live clusters override this with
    /// batched `ListOffsets`.
    async fn watermarks_many(
        &self,
        partitions: &[(String, i32)],
    ) -> HashMap<String, HashMap<i32, Watermarks>> {
        let mut topics: Vec<String> = partitions.iter().map(|(topic, _)| topic.clone()).collect();
        topics.sort();
        topics.dedup();
        join_all(topics.iter().map(|name| async move {
            let marks = self.watermarks(name).await.unwrap_or_default();
            let wanted: HashMap<i32, Watermarks> = partitions
                .iter()
                .filter(|(topic, _)| topic == name)
                .filter_map(|(_, partition)| {
                    marks.get(partition).copied().map(|mark| (*partition, mark))
                })
                .collect();
            (name.clone(), wanted)
        }))
        .await
        .into_iter()
        .collect()
    }

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

    async fn topics_configs(
        &self,
        topics: &[&str],
    ) -> Result<HashMap<String, Vec<ConfigEntry>>, KafkaError>;

    async fn broker_configs(&self, broker_id: i32) -> Result<Vec<ConfigEntry>, KafkaError>;

    async fn consumer_groups(&self) -> Result<Vec<GroupSnapshot>, KafkaError>;

    /// Snapshot for one consumer group.
    ///
    /// The default scans [`consumer_groups`](Self::consumer_groups). Live
    /// clusters override this with a single-group broker fetch.
    async fn consumer_group(&self, id: &str) -> Result<GroupSnapshot, KafkaError> {
        self.consumer_groups()
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

    async fn records(&self, plan: &FetchPlan) -> Result<Vec<Record>, KafkaError>;

    async fn schema_subjects(&self) -> Result<Vec<SchemaSubject>, KafkaError> {
        Ok(Vec::new())
    }

    fn consume_timeout(&self) -> Duration {
        *crate::environment::CONSUME_TIMEOUT
    }
}
