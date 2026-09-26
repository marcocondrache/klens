use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use foldhash::HashMap;

use crate::kafka::error::KafkaError;
use crate::kafka::model::{
    AclListing, ClusterIdentity, CommittedOffset, ConfigEntry, GroupSnapshot, MetadataSnapshot,
    PartitionWindow, RegisteredSchema, ScanConsumer, SchemaSubject, TailConsumer, TailPosition,
    TopicMetadata, Watermarks,
};
use crate::kafka::scan::obfuscate::ObfuscationPolicy;
use crate::kafka::scan::payload::PayloadCodec;
use crate::kafka::writes::ClusterWrites;

#[async_trait]
pub trait ClusterSession: ClusterWrites + Send + Sync + 'static {
    fn identity(&self) -> &ClusterIdentity;

    async fn metadata(&self) -> Result<MetadataSnapshot, KafkaError>;

    async fn topic_metadata(&self, topic: &str) -> Result<TopicMetadata, KafkaError>;

    async fn watermarks(
        &self,
        topics: &HashMap<String, Vec<i32>>,
    ) -> Result<HashMap<String, HashMap<i32, Watermarks>>, KafkaError>;

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

    async fn committed_offsets(
        &self,
        group_id: &str,
        partitions: Option<&[(String, i32)]>,
    ) -> Result<Vec<CommittedOffset>, KafkaError>;

    async fn open_scan(
        &self,
        topic: &str,
        windows: &[PartitionWindow],
    ) -> Result<Box<dyn ScanConsumer>, KafkaError>;

    async fn open_tail(
        &self,
        topic: &str,
        start: &[TailPosition],
    ) -> Result<Box<dyn TailConsumer>, KafkaError>;

    fn payload_codec(&self) -> Option<Arc<dyn PayloadCodec>> {
        None
    }

    fn obfuscation(&self) -> Option<Arc<ObfuscationPolicy>> {
        None
    }

    async fn schema_subjects(&self) -> Result<Vec<SchemaSubject>, KafkaError> {
        Ok(Vec::new())
    }

    async fn subject_schema(
        &self,
        subject: &str,
        version: i32,
    ) -> Result<RegisteredSchema, KafkaError> {
        Err(KafkaError::UnknownSubject {
            cluster: self.identity().name.clone(),
            subject: subject.to_owned(),
            version,
        })
    }

    async fn acls(&self) -> Result<AclListing, KafkaError>;

    fn consume_timeout(&self) -> Duration {
        *crate::environment::CONSUME_TIMEOUT
    }
}
