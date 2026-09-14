use std::collections::HashMap;
use std::ops::Range;
use std::time::Duration;

use async_trait::async_trait;
use krafka::consumer::{Consumer, ConsumerRecord};
use tokio::time::{Instant, timeout_at};

use crate::kafka::error::KafkaError;
use crate::kafka::model::{Compression, FetchPlan, Record, RecordHeader, decode_bytes};
use crate::kafka::record::batch::RecordBatch;
use crate::kafka::record::plan::PartitionWindow;
use crate::kafka::registry::decode::{PayloadDecoder, decode_field};
use crate::kafka::session::RecordBrowse;

pub(super) struct KafkaBrowse<'a> {
    consumer: Option<Consumer>,
    timeout: Duration,
    decoder: Option<&'a PayloadDecoder>,
}

impl<'a> KafkaBrowse<'a> {
    pub(super) fn new(
        consumer: Consumer,
        timeout: Duration,
        decoder: Option<&'a PayloadDecoder>,
    ) -> Self {
        Self {
            consumer: Some(consumer),
            timeout,
            decoder,
        }
    }

    fn consumer(&self) -> &Consumer {
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
        if let Some(consumer) = self.consumer.take() {
            let _ = consumer.close().await;
        }
    }
}

impl Drop for KafkaBrowse<'_> {
    fn drop(&mut self) {
        if let Some(consumer) = self.consumer.take()
            && let Ok(handle) = tokio::runtime::Handle::try_current()
        {
            drop(handle.spawn(async move {
                let _ = consumer.close().await;
            }));
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

async fn consume_windows(
    consumer: &Consumer,
    plan: &FetchPlan,
    decoder: Option<&PayloadDecoder>,
    deadline: Instant,
) -> Result<Vec<Record>, KafkaError> {
    if plan.windows.is_empty() || plan.limit == 0 {
        return Ok(Vec::new());
    }

    let windows = clamp_windows(consumer, plan).await?;
    if windows.is_empty() {
        return Ok(Vec::new());
    }

    let partitions: Vec<i32> = windows.iter().map(|window| window.partition).collect();
    consumer.assign(&plan.topic, partitions.clone()).await?;
    // A reused consumer may still have these partitions paused from a
    // previous pass over this handle. Pause is tracked per partition, not
    // reset by `assign`.
    consumer.resume(&plan.topic, &partitions).await;
    for window in &windows {
        consumer
            .seek(&plan.topic, window.partition, window.start)
            .await?;
    }

    let mut scan = WindowScan::new(&windows);
    let mut records = RecordBatch::new(plan.limit, plan.order);

    while !scan.remaining.is_empty() {
        if Instant::now() >= deadline {
            return Err(KafkaError::Timeout);
        }
        let budget = deadline.saturating_duration_since(Instant::now());
        let batch = timeout_at(deadline, consumer.poll(budget))
            .await
            .map_err(|_| KafkaError::Timeout)??;

        if batch.is_empty() {
            complete_idle_partitions(consumer, &plan.topic, &mut scan).await;
            continue;
        }

        for message in batch {
            let partition = message.partition;
            if !scan.remaining.contains_key(&partition) {
                continue;
            }
            let accepted = scan.accept(partition, message.offset);
            if !scan.remaining.contains_key(&partition) {
                consumer.pause(&plan.topic, &[partition]).await;
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

    if Instant::now() >= deadline {
        return Err(KafkaError::Timeout);
    }
    Ok(records.into_records())
}

async fn clamp_windows(
    consumer: &Consumer,
    plan: &FetchPlan,
) -> Result<Vec<PartitionWindow>, KafkaError> {
    let mut windows = Vec::new();
    for window in &plan.windows {
        if window.is_empty() {
            continue;
        }
        let high = consumer
            .fetch_end_offset(&plan.topic, window.partition)
            .await?;
        let end = window.end.min(high);
        if window.start >= end {
            continue;
        }
        windows.push(PartitionWindow {
            partition: window.partition,
            start: window.start,
            end,
        });
    }
    Ok(windows)
}

/// krafka has no PartitionEOF. An empty poll after the consumer has a
/// watermark means this window has no more records.
async fn complete_idle_partitions(consumer: &Consumer, topic: &str, scan: &mut WindowScan) {
    let mut done = Vec::new();
    for (&partition, window) in &scan.remaining {
        let Some(position) = consumer.position(topic, partition).await else {
            continue;
        };
        let lag = consumer.current_lag(topic, partition).await;
        if position >= window.end || lag == Some(0) || lag.is_some() {
            done.push(partition);
        }
    }
    for partition in done {
        scan.remaining.remove(&partition);
        consumer.pause(topic, &[partition]).await;
    }
}

/// Active half-open offset ranges. Kafka can jump over offsets in compacted logs.
struct WindowScan {
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

async fn record_from_message(
    message: &ConsumerRecord,
    decoder: Option<&PayloadDecoder>,
    plan: &FetchPlan,
) -> Record {
    let headers = message
        .headers
        .iter()
        .map(|(key, value)| RecordHeader {
            key: String::from_utf8_lossy(key).into_owned(),
            value: value.as_deref().map(decode_bytes).unwrap_or_default(),
        })
        .collect();

    let timestamp = message.timestamp.max(0);
    let size_bytes = message.key.as_ref().map(|key| key.len()).unwrap_or(0)
        + message.value.as_ref().map(|value| value.len()).unwrap_or(0);

    let (key, value) = tokio::join!(
        decode_field(decoder, message.key.as_deref(), None),
        decode_field(decoder, message.value.as_deref(), plan.schema_id),
    );
    let key = key.map(|field| field.text);
    let (value, schema_id) = match value {
        Some(decoded) => (Some(decoded.text), decoded.schema_id),
        None => (None, None),
    };

    Record {
        topic: message.topic.clone(),
        partition: message.partition,
        offset: message.offset,
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
