use std::collections::HashMap;
use std::ops::Range;
use std::time::Duration;

use async_trait::async_trait;
use rdkafka::Message;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::error::KafkaError as RdKafkaError;
use rdkafka::message::{Headers, Timestamp};
use rdkafka::topic_partition_list::Offset;
use rdkafka::topic_partition_list::TopicPartitionList;
use tokio::time::{Instant, timeout_at};

use crate::kafka::error::KafkaError;
use crate::kafka::model::{Compression, FetchPlan, Record, RecordHeader, decode_bytes};
use crate::kafka::record::batch::RecordBatch;
use crate::kafka::registry::decode::{PayloadDecoder, decode_field};
use crate::kafka::session::RecordBrowse;

pub(super) struct KafkaBrowse<'a> {
    consumer: Option<StreamConsumer>,
    timeout: Duration,
    decoder: Option<&'a PayloadDecoder>,
}

impl<'a> KafkaBrowse<'a> {
    pub(super) fn new(
        consumer: StreamConsumer,
        timeout: Duration,
        decoder: Option<&'a PayloadDecoder>,
    ) -> Self {
        Self {
            consumer: Some(consumer),
            timeout,
            decoder,
        }
    }

    fn consumer(&self) -> &StreamConsumer {
        self.consumer
            .as_ref()
            .expect("consumer is taken only when closing")
    }

    pub(super) async fn fetch(&self, plan: &FetchPlan) -> Result<Vec<Record>, KafkaError> {
        let deadline = Instant::now() + self.timeout;
        timeout_at(
            deadline,
            consume_windows(self.consumer(), plan, self.decoder, deadline),
        )
        .await
        .map_err(|_| KafkaError::Timeout)?
    }

    pub(super) async fn close(mut self) {
        offload_close(self.consumer.take()).await;
    }
}

impl Drop for KafkaBrowse<'_> {
    fn drop(&mut self) {
        if let Some(consumer) = self.consumer.take() {
            match tokio::runtime::Handle::try_current() {
                Ok(handle) => {
                    let _ = handle.spawn_blocking(move || drop(consumer));
                }
                Err(_) => drop(consumer),
            }
        }
    }
}

#[async_trait]
impl<'a> RecordBrowse for KafkaBrowse<'a> {
    async fn fetch(&self, plan: &FetchPlan) -> Result<Vec<Record>, KafkaError> {
        KafkaBrowse::fetch(self, plan).await
    }

    async fn close(self: Box<Self>) {
        KafkaBrowse::close(*self).await
    }
}

async fn offload_close(consumer: Option<StreamConsumer>) {
    if let Some(consumer) = consumer {
        let _ = tokio::task::spawn_blocking(move || drop(consumer)).await;
    }
}

async fn consume_windows(
    consumer: &StreamConsumer,
    plan: &FetchPlan,
    decoder: Option<&PayloadDecoder>,
    deadline: Instant,
) -> Result<Vec<Record>, KafkaError> {
    if plan.windows.is_empty() || plan.limit == 0 {
        return Ok(Vec::new());
    }

    let mut tpl = TopicPartitionList::new();

    for window in &plan.windows {
        if window.is_empty() {
            continue;
        }
        tpl.add_partition_offset(&plan.topic, window.partition, Offset::Offset(window.start))?;
    }

    if tpl.count() == 0 {
        return Ok(Vec::new());
    }

    consumer.assign(&tpl)?;
    // A reused consumer may still have these partitions paused from a
    // previous pass over this handle: pausing is tracked per partition, not
    // reset by `assign`.
    consumer.resume(&tpl)?;

    let mut scan = WindowScan::new(plan);
    let mut records = RecordBatch::new(plan.limit, plan.order);

    while !scan.remaining.is_empty() {
        // Buffered messages and cached decoding may never yield to the timer.
        if Instant::now() >= deadline {
            return Err(KafkaError::Timeout);
        }
        match consumer.recv().await {
            Err(RdKafkaError::PartitionEOF(partition)) => {
                scan.remaining.remove(&partition);
                let mut completed = TopicPartitionList::new();
                completed.add_partition(&plan.topic, partition);
                consumer.pause(&completed)?;
            }
            Err(error) => return Err(error.into()),
            Ok(message) => {
                let partition = message.partition();
                if !scan.remaining.contains_key(&partition) {
                    continue;
                }
                let accepted = scan.accept(partition, message.offset());
                if !scan.remaining.contains_key(&partition) {
                    let mut completed = TopicPartitionList::new();
                    completed.add_partition(&plan.topic, partition);
                    consumer.pause(&completed)?;
                }
                if !accepted {
                    continue;
                }

                let record = record_from_message(&message, decoder, plan).await;
                if record.matches(plan.filter.as_ref()) {
                    records.push(record);
                }
            }
        }
    }

    if Instant::now() >= deadline {
        return Err(KafkaError::Timeout);
    }
    Ok(records.into_records())
}

/// Active half-open offset ranges. Kafka can jump over offsets in compacted logs.
struct WindowScan {
    remaining: HashMap<i32, Range<i64>>,
}

impl WindowScan {
    fn new(plan: &FetchPlan) -> Self {
        Self {
            remaining: plan
                .windows
                .iter()
                .filter(|window| !window.is_empty())
                .map(|window| (window.partition, window.start..window.end))
                .collect(),
        }
    }

    fn accept(&mut self, partition: i32, offset: i64) -> bool {
        let Some(window) = self.remaining.get(&partition) else {
            return false;
        };
        let accepted = window.contains(&offset);
        if offset >= window.end - 1 {
            self.remaining.remove(&partition);
        }
        accepted
    }
}

async fn record_from_message(
    message: &rdkafka::message::BorrowedMessage<'_>,
    decoder: Option<&PayloadDecoder>,
    plan: &FetchPlan,
) -> Record {
    let headers = message
        .headers()
        .map(|headers| {
            (0..headers.count())
                .filter_map(|index| {
                    let header = headers.try_get(index)?;
                    Some(RecordHeader {
                        key: header.key.to_owned(),
                        value: header.value.map(decode_bytes).unwrap_or_default(),
                    })
                })
                .collect()
        })
        .unwrap_or_default();

    let timestamp = match message.timestamp() {
        Timestamp::NotAvailable => 0,
        Timestamp::CreateTime(ms) | Timestamp::LogAppendTime(ms) => ms,
    };

    let size_bytes = message.key().map(|key| key.len()).unwrap_or(0)
        + message.payload().map(|payload| payload.len()).unwrap_or(0);

    let (key, value) = tokio::join!(
        decode_field(decoder, message.key(), None),
        decode_field(decoder, message.payload(), plan.schema_id),
    );
    let key = key.map(|field| field.text);
    let (value, schema_id) = match value {
        Some(decoded) => (Some(decoded.text), decoded.schema_id),
        None => (None, None),
    };

    Record {
        topic: message.topic().to_owned(),
        partition: message.partition(),
        offset: message.offset(),
        timestamp,
        key,
        value,
        schema_id,
        headers,
        size_bytes: size_bytes as u64,
        compression: Compression::None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scan_enforces_half_open_windows_across_interleaved_partitions() {
        let mut scan = WindowScan {
            remaining: HashMap::from([(0, 10..12), (1, 20..23)]),
        };

        assert!(!scan.accept(0, 9));
        assert!(scan.accept(0, 10));
        assert!(scan.accept(1, 20));
        assert!(scan.accept(0, 11));
        assert!(!scan.remaining.contains_key(&0));
        assert!(!scan.accept(0, 12));
        assert!(!scan.accept(0, 13));
        assert!(scan.accept(1, 22));
        assert!(scan.remaining.is_empty());
    }

    #[test]
    fn compacted_gap_completes_window_without_accepting_outside_record() {
        let mut scan = WindowScan {
            remaining: HashMap::from([(0, 10..20)]),
        };

        assert!(!scan.accept(1, 10));
        assert!(scan.accept(0, 12));
        assert!(!scan.accept(0, 25));
        assert!(scan.remaining.is_empty());
    }
}
