use bytes::Bytes;

use crate::app::auth::access::{DeleteRecordsCap, ProduceCap, RecordsCap};
use crate::kafka::model::{FoundRecord, NewRecord, ProducedRecord, RecordAt, RecordDeletion};
use crate::kafka::store::TopicInfo;
use crate::kafka::{Export, KafkaError, RecordPage, RecordQuery, Tail, TailQuery};

use super::Granted;

impl Granted<'_, RecordsCap> {
    pub(crate) async fn read(&self, query: RecordQuery) -> Result<RecordPage, KafkaError> {
        self.cluster.records(query, self.limits.records).await
    }

    pub(crate) async fn record(&self, at: RecordAt) -> Result<FoundRecord, KafkaError> {
        self.cluster.record(at, self.limits.records).await
    }

    pub(crate) async fn export(&self, query: RecordQuery) -> Result<Export, KafkaError> {
        self.cluster.export(query, self.limits.records).await
    }

    pub(crate) async fn tail(&self, query: TailQuery) -> Result<Tail, KafkaError> {
        self.cluster.tail(query, self.limits).await
    }
}

impl Granted<'_, DeleteRecordsCap> {
    pub(crate) async fn delete_records(
        &self,
        topic: &str,
        partitions: &[i32],
        before: Option<i64>,
    ) -> Result<(), KafkaError> {
        let known = self.writable_topic(topic, TopicInfo::partition_ids)?;
        if let Some(&partition) = partitions
            .iter()
            .find(|partition| !known.contains(partition))
        {
            return Err(self.unknown_partition(topic, partition));
        }
        let deletion = RecordDeletion {
            topic: topic.to_owned(),
            before: if partitions.is_empty() {
                &known
            } else {
                partitions
            }
            .iter()
            .map(|&partition| (partition, before))
            .collect(),
        };
        let lows = self.cluster.session.delete_records(&deletion).await?;
        tracing::info!(
            cluster = %self.cluster.store.name(),
            topic,
            partitions = ?deletion.before.keys().collect::<Vec<_>>(),
            before,
            "deleted records"
        );
        self.settle(&self.cluster.store.watermarks, |marks| {
            lows.iter().all(|(&partition, &low)| {
                marks
                    .get(topic, partition)
                    .is_some_and(|mark| mark.low >= low)
            })
        })
        .await;
        Ok(())
    }
}

impl Granted<'_, ProduceCap> {
    pub(crate) async fn encode(&self, schema_id: i32, json: &str) -> Result<Bytes, KafkaError> {
        self.cluster.session.encode_payload(schema_id, json).await
    }

    pub(crate) fn writable_partition(
        &self,
        topic: &str,
        partition: Option<i32>,
    ) -> Result<(), KafkaError> {
        let partitions = self.writable_topic(topic, TopicInfo::partition_ids)?;
        if let Some(partition) = partition
            && !partitions.contains(&partition)
        {
            return Err(self.unknown_partition(topic, partition));
        }
        Ok(())
    }

    pub(crate) async fn produce(&self, record: &NewRecord) -> Result<ProducedRecord, KafkaError> {
        let topic = record.topic.as_str();
        self.writable_partition(topic, record.partition)?;
        let produced = self.cluster.session.produce(record).await?;
        tracing::info!(
            cluster = %self.cluster.store.name(),
            topic,
            partition = produced.partition,
            offset = produced.offset,
            "produced record"
        );
        Ok(produced)
    }
}
