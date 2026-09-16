use std::collections::HashMap;
use std::ops::Range;
use std::time::Duration;

use krafka::client::KrafkaClient;
use krafka::consumer::{AutoOffsetReset, Consumer, ConsumerRecord};
use tokio::time::Instant;

use crate::kafka::error::KafkaError;
use crate::kafka::model::{Compression, FetchPlan, Record, RecordHeader, decode_bytes};
use crate::kafka::record::batch::RecordBatch;
use crate::kafka::record::plan::PartitionWindow;
use schemreg::{KEY_SCHEMA_ID_HEADER, VALUE_SCHEMA_ID_HEADER};

use crate::kafka::registry::decode::{PayloadDecoder, decode_field};

pub(super) async fn fetch(
    client: &KrafkaClient,
    plan: &FetchPlan,
    decoder: Option<&PayloadDecoder>,
    deadline: Instant,
) -> Result<Vec<Record>, KafkaError> {
    // Only scan state is new: connections and metadata belong to the cluster.
    // Starting at the plan's offsets avoids reset lookups followed by seeks.
    let consumer = Consumer::builder()
        .with_client(client)
        .enable_auto_commit(false)
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .initial_offsets(
            plan.windows
                .iter()
                .map(|window| ((plan.topic.clone(), window.partition), window.start))
                .collect(),
        )
        .build()
        .await?;
    let mut guard = CloseOnDrop(Some(consumer));
    let consumer = guard.0.as_ref().expect("consumer is open");
    let result = consume_windows(consumer, plan, decoder, deadline).await;
    let _ = consumer.close().await;
    guard.0.take();
    result
}

/// Also close on cancellation; the consumer borrows the cluster's pool.
struct CloseOnDrop(Option<Consumer>);

impl Drop for CloseOnDrop {
    fn drop(&mut self) {
        if let Some(consumer) = self.0.take()
            && let Ok(handle) = tokio::runtime::Handle::try_current()
        {
            drop(handle.spawn(async move {
                let _ = consumer.close().await;
            }));
        }
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

    consumer
        .assign(
            &plan.topic,
            windows.iter().map(|window| window.partition).collect(),
        )
        .await?;

    let mut scan = WindowScan::new(&windows);
    let mut records = RecordBatch::new(plan.limit, plan.order);

    while !scan.remaining.is_empty() {
        if Instant::now() >= deadline {
            return Err(KafkaError::Timeout);
        }
        // Leave time to inspect positions after empty polls (e.g. compacted batches).
        let budget = deadline
            .saturating_duration_since(Instant::now())
            .min(Duration::from_millis(100));
        let batch = consumer.poll(budget).await?;

        if batch.is_empty() {
            complete_idle_partitions(consumer, &plan.topic, &mut scan).await;
            continue;
        }

        for message in batch {
            if Instant::now() >= deadline {
                return Err(KafkaError::Timeout);
            }
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

/// An empty poll alone is not EOF: it can also follow a retriable broker error.
async fn complete_idle_partitions(consumer: &Consumer, topic: &str, scan: &mut WindowScan) {
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
        decode_field(
            decoder,
            message.key.as_deref(),
            None,
            message
                .header_value(KEY_SCHEMA_ID_HEADER.as_bytes())
                .map(|value| value.as_ref()),
        ),
        decode_field(
            decoder,
            message.value.as_deref(),
            plan.schema_id,
            message
                .header_value(VALUE_SCHEMA_ID_HEADER.as_bytes())
                .map(|value| value.as_ref()),
        ),
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

    #[tokio::test]
    async fn known_positive_lag_does_not_complete_a_window() {
        let broker = krafka::testing::FakeBroker::start().await.unwrap();
        assert!(broker.create_topic("orders", 1));
        super::super::tests::produce_krafka(&broker.bootstrap_servers(), "orders", 2).await;
        let consumer = Consumer::builder()
            .bootstrap_servers(broker.bootstrap_servers())
            .enable_auto_commit(false)
            .auto_offset_reset(AutoOffsetReset::Earliest)
            .max_poll_records(1)
            .build()
            .await
            .unwrap();
        consumer.assign("orders", vec![0]).await.unwrap();
        assert_eq!(
            consumer.poll(Duration::from_secs(1)).await.unwrap().len(),
            1
        );
        let mut scan = WindowScan {
            remaining: HashMap::from([(0, 0..2)]),
        };
        complete_idle_partitions(&consumer, "orders", &mut scan).await;
        assert!(scan.remaining.contains_key(&0));
        assert_eq!(
            consumer.poll(Duration::from_secs(1)).await.unwrap().len(),
            1
        );
        complete_idle_partitions(&consumer, "orders", &mut scan).await;
        assert!(scan.remaining.is_empty());
        consumer.close().await.unwrap();
    }

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
