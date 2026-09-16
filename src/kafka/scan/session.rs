use std::collections::HashMap;
use std::ops::Range;
use std::time::Duration;

use async_trait::async_trait;
use tokio::time::Instant;

use crate::kafka::error::KafkaError;
use crate::kafka::record::batch::RecordBatch;
use crate::kafka::record::plan::{FetchPlan, PartitionWindow};
use crate::kafka::record::{Compression, Record, RecordHeader, decode_bytes};
use crate::kafka::registry::decode::{DecodeValue, is_confluent_framed, wire_schema_id};
use crate::kafka::scan::filter::{CompiledFilter, MetaVerdict, RecordMeta};
use crate::kafka::scan::payload::DecodedPayload;

#[async_trait]
pub(crate) trait ScanConsumer: Send + Sync {
    async fn assign(&self, topic: &str, partitions: Vec<i32>) -> Result<(), KafkaError>;
    async fn seek(&self, topic: &str, partition: i32, offset: i64) -> Result<(), KafkaError>;
    async fn poll(&self, timeout: Duration) -> Result<Vec<RawRecord>, KafkaError>;
    async fn position(&self, topic: &str, partition: i32) -> Option<i64>;
    async fn current_lag(&self, topic: &str, partition: i32) -> Option<u64>;
    async fn pause(&self, topic: &str, partitions: &[i32]);
    async fn resume(&self, topic: &str, partitions: &[i32]);
    async fn decode(
        &self,
        bytes: Option<&[u8]>,
        override_id: Option<i32>,
    ) -> Option<DecodedPayload>;
    async fn close(&self);
    async fn observe_plan(&self, _plan: &FetchPlan) {}
}

pub(crate) struct RawRecord {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub timestamp: i64,
    pub key: Option<Vec<u8>>,
    pub value: Option<Vec<u8>>,
    pub headers: Vec<(Vec<u8>, Option<Vec<u8>>)>,
}

pub struct ScanSession {
    consumer: Box<dyn ScanConsumer>,
    deadline: Instant,
    topic: String,
}

pub(crate) struct ScanOutcome {
    pub completed: bool,
}

impl ScanSession {
    pub(crate) fn new(consumer: Box<dyn ScanConsumer>, topic: String, deadline: Instant) -> Self {
        Self {
            consumer,
            deadline,
            topic,
        }
    }

    pub(crate) async fn scan(
        &mut self,
        plan: &FetchPlan,
        filter: Option<&CompiledFilter>,
        batch: &mut RecordBatch,
        pass: &mut Vec<Record>,
    ) -> Result<ScanOutcome, KafkaError> {
        let windows: Vec<PartitionWindow> = plan
            .windows
            .iter()
            .filter(|window| !window.is_empty())
            .cloned()
            .collect();
        if windows.is_empty() || plan.limit == 0 {
            return Ok(ScanOutcome { completed: true });
        }

        self.consumer.observe_plan(plan).await;

        let partitions: Vec<i32> = windows.iter().map(|window| window.partition).collect();
        self.consumer
            .assign(&self.topic, partitions.clone())
            .await?;
        self.consumer.resume(&self.topic, &partitions).await;
        for window in &windows {
            self.consumer
                .seek(&self.topic, window.partition, window.start)
                .await?;
        }

        let mut scan = WindowScan::new(&windows);
        while !scan.remaining.is_empty() {
            if Instant::now() >= self.deadline {
                return Ok(ScanOutcome {
                    completed: scan.remaining.is_empty(),
                });
            }
            let budget = self
                .deadline
                .saturating_duration_since(Instant::now())
                .min(Duration::from_millis(100));
            let messages =
                match tokio::time::timeout_at(self.deadline, self.consumer.poll(budget)).await {
                    Ok(result) => result?,
                    Err(_) => {
                        return Ok(ScanOutcome {
                            completed: scan.remaining.is_empty(),
                        });
                    }
                };

            if messages.is_empty() {
                complete_idle_partitions(self.consumer.as_ref(), &self.topic, &mut scan).await;
                continue;
            }

            for message in messages {
                if Instant::now() >= self.deadline {
                    return Ok(ScanOutcome {
                        completed: scan.remaining.is_empty(),
                    });
                }
                let partition = message.partition;
                if !scan.remaining.contains_key(&partition) {
                    continue;
                }
                let accepted = scan.accept(partition, message.offset);
                if !scan.remaining.contains_key(&partition) {
                    self.consumer.pause(&self.topic, &[partition]).await;
                }
                if !accepted {
                    continue;
                }
                if let Some(record) =
                    accept_message(self.consumer.as_ref(), &message, plan, filter, batch).await
                {
                    pass.push(record.clone());
                    batch.push(record);
                }
            }
        }

        Ok(ScanOutcome { completed: true })
    }
}

impl Drop for ScanSession {
    fn drop(&mut self) {
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            let consumer = std::mem::replace(&mut self.consumer, Box::new(ClosedConsumer));
            drop(handle.spawn(async move {
                consumer.close().await;
            }));
        }
    }
}

struct ClosedConsumer;

#[async_trait]
impl ScanConsumer for ClosedConsumer {
    async fn assign(&self, _topic: &str, _partitions: Vec<i32>) -> Result<(), KafkaError> {
        Ok(())
    }
    async fn seek(&self, _topic: &str, _partition: i32, _offset: i64) -> Result<(), KafkaError> {
        Ok(())
    }
    async fn poll(&self, _timeout: Duration) -> Result<Vec<RawRecord>, KafkaError> {
        Ok(Vec::new())
    }
    async fn position(&self, _topic: &str, _partition: i32) -> Option<i64> {
        None
    }
    async fn current_lag(&self, _topic: &str, _partition: i32) -> Option<u64> {
        None
    }
    async fn pause(&self, _topic: &str, _partitions: &[i32]) {}
    async fn resume(&self, _topic: &str, _partitions: &[i32]) {}
    async fn decode(
        &self,
        _bytes: Option<&[u8]>,
        _override_id: Option<i32>,
    ) -> Option<DecodedPayload> {
        None
    }
    async fn close(&self) {}
}

async fn complete_idle_partitions(consumer: &dyn ScanConsumer, topic: &str, scan: &mut WindowScan) {
    let mut done = Vec::new();
    for (&partition, window) in &scan.remaining {
        let Some(position) = consumer.position(topic, partition).await else {
            continue;
        };
        let lag = consumer.current_lag(topic, partition).await;
        if position >= window.end || lag == Some(0) {
            done.push(partition);
        }
    }
    for partition in done {
        scan.remaining.remove(&partition);
        consumer.pause(topic, &[partition]).await;
    }
}

pub(crate) struct WindowScan {
    remaining: HashMap<i32, Range<i64>>,
}

impl WindowScan {
    fn new(windows: &[PartitionWindow]) -> Self {
        Self {
            remaining: windows
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

async fn accept_message(
    consumer: &dyn ScanConsumer,
    message: &RawRecord,
    plan: &FetchPlan,
    filter: Option<&CompiledFilter>,
    batch: &RecordBatch,
) -> Option<Record> {
    let headers: Vec<RecordHeader> = message
        .headers
        .iter()
        .map(|(key, value)| RecordHeader {
            key: String::from_utf8_lossy(key).into_owned(),
            value: value.as_deref().map(decode_bytes).unwrap_or_default(),
        })
        .collect();
    let timestamp = message.timestamp.max(0);
    let size_bytes = (message.key.as_ref().map(Vec::len).unwrap_or(0)
        + message.value.as_ref().map(Vec::len).unwrap_or(0)) as u64;
    let meta = RecordMeta {
        partition: message.partition,
        offset: message.offset,
        timestamp,
        size_bytes,
        topic: &message.topic,
        schema_id: message.value.as_deref().and_then(wire_schema_id),
        compression: Compression::None,
        headers: &headers,
    };

    match filter {
        None => {}
        Some(filter) => match filter.on_meta(&meta) {
            MetaVerdict::Fail => return None,
            MetaVerdict::Pass => {
                let (key, value) = decode_pair(consumer, message, plan.schema_id).await;
                return Some(render(message, key, value, headers, timestamp, size_bytes));
            }
            MetaVerdict::NeedsPayload => {
                if !batch.would_keep(timestamp, message.partition, message.offset) {
                    return None;
                }
            }
        },
    }

    let (key, value) = decode_pair(consumer, message, plan.schema_id).await;
    if let Some(filter) = filter
        && !filter.on_payload(&meta, &key, &value)
    {
        return None;
    }
    Some(render(message, key, value, headers, timestamp, size_bytes))
}

async fn decode_pair(
    consumer: &dyn ScanConsumer,
    message: &RawRecord,
    override_id: Option<i32>,
) -> (DecodedPayload, DecodedPayload) {
    let key = consumer
        .decode(message.key.as_deref(), None)
        .await
        .unwrap_or_else(DecodedPayload::absent);
    let value = consumer
        .decode(message.value.as_deref(), override_id)
        .await
        .unwrap_or_else(DecodedPayload::absent);
    (key, value)
}

fn render(
    message: &RawRecord,
    key: DecodedPayload,
    value: DecodedPayload,
    headers: Vec<RecordHeader>,
    timestamp: i64,
    size_bytes: u64,
) -> Record {
    let schema_id = value.schema_id().or(key.schema_id());
    let key_text = if key.is_absent() {
        None
    } else {
        Some(key.text().to_owned())
    };
    let value_text = if value.is_absent() {
        None
    } else {
        Some(value.text().to_owned())
    };
    Record {
        topic: message.topic.clone(),
        partition: message.partition,
        offset: message.offset,
        timestamp,
        key: key_text,
        value: value_text,
        schema_id,
        headers,
        size_bytes,
        compression: Compression::None,
    }
}

pub(crate) fn decoded_from_bytes(
    bytes: Option<&[u8]>,
    decoded: Option<DecodeValue>,
) -> Option<DecodedPayload> {
    match (bytes, decoded) {
        (None, _) => None,
        (Some(bytes), Some(decoded)) => Some(DecodedPayload::from_decode(bytes.to_vec(), decoded)),
        (Some(bytes), None) => Some(DecodedPayload::from_raw(
            bytes.to_vec(),
            wire_schema_id(bytes),
            is_confluent_framed(bytes),
        )),
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
