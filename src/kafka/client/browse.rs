use std::collections::HashMap;
use std::ops::Range;
use std::time::Duration;

use async_trait::async_trait;
use krafka::consumer::{Consumer, ConsumerRecord};
use tokio::time::{Instant, timeout_at};

use crate::kafka::error::KafkaError;
use crate::kafka::model::{Compression, FetchPlan, Record, RecordHeader, decode_bytes};
use crate::kafka::record::batch::RecordBatch;
use crate::kafka::registry::decode::{PayloadDecoder, decode_field};
use crate::kafka::session::RecordBrowse;

pub(super) struct KafkaBrowse {
    consumer: Option<Consumer>,
    timeout: Duration,
    decoder: Option<PayloadDecoder>,
}

impl KafkaBrowse {
    pub(super) fn new(
        consumer: Consumer,
        timeout: Duration,
        decoder: Option<PayloadDecoder>,
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
            consume_windows(self.consumer(), plan, self.decoder.as_ref(), deadline),
        )
        .await
        .map_err(|_| KafkaError::Timeout)?
    }

    /// Drops the consumer rather than calling `Consumer::close`.
    ///
    /// This consumer never joins a group and never auto-commits, so
    /// `close`'s group-leave and offset-commit work has nothing to do here
    /// — and, measured against a live broker, `close` on a pool shared via
    /// `.with_client(..)` took a consistent ~10s to return where a plain
    /// drop returns in microseconds, which is not a cost worth paying per
    /// browse call for a no-op. `Consumer`'s own `Drop` still logs if this
    /// somehow gets skipped, so nothing is silently lost either way.
    pub(super) async fn close(mut self) {
        drop(self.consumer.take());
    }
}

#[async_trait]
impl RecordBrowse for KafkaBrowse {
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

    let partitions: Vec<i32> = plan
        .windows
        .iter()
        .filter(|window| !window.is_empty())
        .map(|window| window.partition)
        .collect();
    if partitions.is_empty() {
        return Ok(Vec::new());
    }

    consumer.assign(&plan.topic, partitions.clone()).await?;
    // A reused consumer may still have these partitions paused from a
    // previous pass over this handle: pausing is tracked per partition, not
    // reset by `assign`.
    consumer.resume(&plan.topic, &partitions).await;

    let seeks = plan
        .windows
        .iter()
        .filter(|window| !window.is_empty())
        .map(|window| ((plan.topic.clone(), window.partition), window.start))
        .collect();
    consumer.seek_many(&seeks).await?;

    let mut scan = WindowScan::new(plan);
    let mut records = RecordBatch::new(plan.limit, plan.order);

    while !scan.remaining.is_empty() {
        let now = Instant::now();
        if now >= deadline {
            return Err(KafkaError::Timeout);
        }
        // Kafka's own poll contract: returns as soon as data is available,
        // or empty once `deadline - now` elapses with nothing new — the loop
        // re-checks the deadline on the next pass rather than busy-waiting.
        let batch = consumer.poll(deadline - now).await?;

        for message in batch {
            let partition = message.partition;
            let offset = message.offset;
            if !scan.remaining.contains_key(&partition) {
                continue;
            }

            let accepted = scan.accept(partition, offset);
            if !scan.remaining.contains_key(&partition) {
                consumer.pause(&plan.topic, &[partition]).await;
            }
            if !accepted {
                continue;
            }

            let record = record_from_message(message, decoder, plan).await;
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
    message: ConsumerRecord,
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
        topic: message.topic,
        partition: message.partition,
        offset: message.offset,
        timestamp: message.timestamp,
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
