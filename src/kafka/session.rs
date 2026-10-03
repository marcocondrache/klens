use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use foldhash::{HashMap, HashMapExt};

use crate::kafka::error::KafkaError;
use crate::kafka::model::{
    AclListing, ClusterIdentity, CommittedOffset, ConfigEntry, GroupSnapshot, LogDir,
    MetadataSnapshot, NewTopic, PartitionWindow, QuotaListing, RegisteredSchema, ScanConsumer,
    SchemaSubject, TailConsumer, TailPosition, TopicMetadata, Watermarks,
};
use crate::kafka::scan::obfuscate::ObfuscationPolicy;
use crate::kafka::scan::payload::PayloadCodec;

#[async_trait]
pub trait ClusterSession: Send + Sync + 'static {
    fn identity(&self) -> &ClusterIdentity;

    async fn metadata(&self) -> Result<MetadataSnapshot, KafkaError>;

    async fn topic_metadata(&self, topic: &str) -> Result<TopicMetadata, KafkaError>;

    async fn low_watermarks(
        &self,
        topics: &HashMap<String, Vec<i32>>,
    ) -> Result<HashMap<String, HashMap<i32, i64>>, KafkaError>;

    async fn high_watermarks(
        &self,
        topics: &HashMap<String, Vec<i32>>,
    ) -> Result<HashMap<String, HashMap<i32, i64>>, KafkaError>;

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

    async fn log_dirs(&self) -> Result<Vec<LogDir>, KafkaError>;

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

    fn payload_codec(&self) -> Option<Arc<dyn PayloadCodec>>;

    fn obfuscation(&self) -> Option<Arc<ObfuscationPolicy>>;

    async fn schema_subjects(&self) -> Result<Vec<SchemaSubject>, KafkaError>;

    async fn subject_schema(
        &self,
        subject: &str,
        version: i32,
    ) -> Result<RegisteredSchema, KafkaError>;

    async fn acls(&self) -> Result<AclListing, KafkaError>;

    async fn client_quotas(&self) -> Result<QuotaListing, KafkaError>;

    async fn create_topic(&self, topic: &NewTopic) -> Result<(), KafkaError>;

    fn consume_timeout(&self) -> Duration;

    fn scan_poll_wait(&self) -> Duration;
}

pub async fn watermarks<S: ClusterSession + ?Sized>(
    session: &S,
    topics: &HashMap<String, Vec<i32>>,
) -> Result<HashMap<String, HashMap<i32, Watermarks>>, KafkaError> {
    let (lows, highs) = tokio::try_join!(
        session.low_watermarks(topics),
        session.high_watermarks(topics)
    )?;
    Ok(merge_watermarks(&lows, highs))
}

pub fn merge_watermarks(
    lows: &HashMap<String, HashMap<i32, i64>>,
    highs: HashMap<String, HashMap<i32, i64>>,
) -> HashMap<String, HashMap<i32, Watermarks>> {
    let mut out = HashMap::with_capacity(highs.len());
    for (topic, partitions) in highs {
        let topic_lows = lows.get(&topic);
        let marks: HashMap<i32, Watermarks> = partitions
            .into_iter()
            .filter_map(|(partition, high)| {
                let low = topic_lows
                    .and_then(|lows| lows.get(&partition))
                    .copied()
                    .unwrap_or(high);
                (low <= high).then_some((partition, Watermarks { low, high }))
            })
            .collect();
        if !marks.is_empty() {
            out.insert(topic, marks);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn merge_watermarks_keeps_empty_skips_inverted_and_falls_back_to_the_high() {
        let lows = HashMap::from_iter([
            (
                "orders".to_owned(),
                HashMap::from_iter([(0, 0), (1, 10), (2, 4)]),
            ),
            ("payments".to_owned(), HashMap::from_iter([(0, 1)])),
            ("logs".to_owned(), HashMap::from_iter([(0, 3)])),
            ("audit".to_owned(), HashMap::from_iter([(0, 8)])),
        ]);
        let highs = HashMap::from_iter([
            (
                "orders".to_owned(),
                HashMap::from_iter([(0, 0), (1, 5), (2, 12)]),
            ),
            ("payments".to_owned(), HashMap::from_iter([(1, 9)])),
            ("logs".to_owned(), HashMap::from_iter([(0, 9)])),
            ("audit".to_owned(), HashMap::from_iter([(0, 7)])),
        ]);

        assert_eq!(
            merge_watermarks(&lows, highs),
            HashMap::from_iter([
                (
                    "orders".to_owned(),
                    HashMap::from_iter([
                        (0, Watermarks { low: 0, high: 0 }),
                        (2, Watermarks { low: 4, high: 12 }),
                    ]),
                ),
                (
                    "payments".to_owned(),
                    HashMap::from_iter([(1, Watermarks { low: 9, high: 9 })]),
                ),
                (
                    "logs".to_owned(),
                    HashMap::from_iter([(0, Watermarks { low: 3, high: 9 })]),
                ),
            ])
        );
    }
}
