use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use krafka::Kafka;
use krafka::consumer::{
    AutoOffsetReset, Consumer, ConsumerBuilder, ConsumerRecord, TopicPartition,
};

use crate::config::Tuning;
use crate::kafka::error::KafkaError;
use crate::kafka::model::{PartitionWindow, RawRecord, ScanConsumer};

use super::pool::{ScanPool, assign};

const MIN_FETCH_WAIT: Duration = Duration::from_millis(1);

#[derive(Debug, Clone, Copy)]
pub(super) struct ReaderConfig {
    fetch_wait: Duration,
    page_limit: i32,
    fetch_max_bytes: i32,
}

impl ReaderConfig {
    pub(super) fn new(tuning: &Tuning, fetch_wait: Duration) -> Self {
        Self {
            fetch_wait: fetch_wait.max(MIN_FETCH_WAIT),
            page_limit: i32::try_from(tuning.records.max_limit.get()).unwrap_or(i32::MAX),
            // krafka's 50 MB default is wider than the frame the connection
            // now accepts, which would make a busy fetch unreadable.
            fetch_max_bytes: i32::try_from(tuning.kafka.max_response_bytes() / 2)
                .unwrap_or(i32::MAX),
        }
    }
}

pub(super) fn reader(
    kafka: &Kafka,
    topic: &str,
    start: impl IntoIterator<Item = (i32, i64)>,
    config: ReaderConfig,
) -> ConsumerBuilder {
    kafka
        .consumer_without_group()
        .enable_auto_commit(false)
        .metrics_push(false)
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .fetch_max_wait(config.fetch_wait)
        .max_poll_records(config.page_limit)
        .max_buffered_records(config.page_limit.saturating_mul(2))
        .fetch_max_bytes(config.fetch_max_bytes)
        .initial_offsets(
            start
                .into_iter()
                .map(|(partition, offset)| (TopicPartition::new(topic, partition), offset)),
        )
}

pub(super) fn raw_record(message: ConsumerRecord) -> RawRecord {
    RawRecord {
        partition: message.partition,
        offset: message.offset,
        timestamp: message.timestamp,
        key: message.key,
        value: message.value,
        headers: message.headers,
    }
}

pub(super) struct ScanLease {
    pool: Arc<ScanPool>,
    topic: String,
    consumer: Arc<Consumer>,
    reusable: AtomicBool,
    released: AtomicBool,
}

impl ScanLease {
    pub(super) fn new(pool: Arc<ScanPool>, topic: &str, consumer: Arc<Consumer>) -> Self {
        Self {
            pool,
            topic: topic.to_owned(),
            consumer,
            reusable: AtomicBool::new(true),
            released: AtomicBool::new(false),
        }
    }

    fn poison<T>(&self, result: Result<T, KafkaError>) -> Result<T, KafkaError> {
        if result.is_err() {
            self.reusable.store(false, Ordering::SeqCst);
        }
        result
    }

    fn release(&self) {
        if self.released.swap(true, Ordering::SeqCst) {
            return;
        }
        self.pool.release(
            &self.topic,
            Arc::clone(&self.consumer),
            self.reusable.load(Ordering::SeqCst),
        );
    }
}

#[async_trait]
impl ScanConsumer for ScanLease {
    async fn reassign(&self, windows: &[PartitionWindow]) -> Result<(), KafkaError> {
        self.poison(assign(&self.consumer, &self.topic, windows).await)
    }

    async fn poll(&self, max_wait: Duration) -> Result<Vec<RawRecord>, KafkaError> {
        let polled = self.poison(self.consumer.poll(max_wait).await.map_err(KafkaError::from))?;

        Ok(polled.into_iter().map(raw_record).collect())
    }

    async fn pause(&self, partitions: &[i32]) {
        self.consumer.pause(&self.topic, partitions).await;
    }

    async fn seek_to_end(&self, windows: &[PartitionWindow]) {
        let ends = windows.iter().map(|window| {
            (
                TopicPartition::new(self.topic.as_str(), window.partition),
                window.end,
            )
        });
        let _ = self.poison(
            self.consumer
                .seek_many(ends)
                .await
                .map_err(KafkaError::from),
        );
    }

    async fn position(&self, partition: i32) -> Option<i64> {
        self.consumer.position(&self.topic, partition).await
    }

    async fn lag(&self, partition: i32) -> Option<u64> {
        self.consumer
            .lag()
            .await
            .get(&TopicPartition::new(self.topic.as_str(), partition))?
            .lag
    }

    async fn close(&self) {
        self.release();
    }
}

impl Drop for ScanLease {
    fn drop(&mut self) {
        self.release();
    }
}

#[cfg(test)]
mod tests {
    use krafka::admin::{ListOffsetsOptions, OffsetSpec};
    use krafka::testing::ApiKey;
    use krafka::testing::Control;

    use super::*;
    use crate::kafka::client::testing::Broker;
    use crate::testing::eventually;

    fn window(partition: i32, start: i64, end: i64) -> PartitionWindow {
        PartitionWindow {
            partition,
            start,
            end,
        }
    }

    async fn read(scan: &ScanLease, window: PartitionWindow) -> Vec<i64> {
        let mut offsets = Vec::new();
        while (offsets.len() as i64) < window.end - window.start {
            let polled = scan.poll(Duration::from_secs(1)).await.expect("poll");
            if polled.is_empty() {
                break;
            }
            offsets.extend(
                polled
                    .into_iter()
                    .map(|record| record.offset)
                    .filter(|offset| (window.start..window.end).contains(offset)),
            );
        }
        offsets
    }

    #[test]
    fn a_reader_asks_for_at_most_half_a_response_frame() {
        let config = ReaderConfig::new(&Tuning::default(), Duration::from_millis(250));

        assert_eq!(
            config.fetch_max_bytes,
            16 * 1024 * 1024,
            "a fetch wider than the frame limit would be unreadable"
        );
        assert_eq!(config.fetch_wait, Duration::from_millis(250));
    }

    #[test]
    fn a_reader_never_asks_the_broker_for_a_zero_wait() {
        let config = ReaderConfig::new(&Tuning::default(), Duration::ZERO);

        assert_eq!(
            config.fetch_wait, MIN_FETCH_WAIT,
            "Redpanda never answers a fetch-session close with max_wait_ms 0"
        );
    }

    #[tokio::test]
    async fn a_released_consumer_serves_the_next_page_of_the_same_topic() {
        let broker = Broker::orders(4).await;
        let client = broker.client().await;

        let first = client
            .scans
            .acquire("orders", &[window(0, 0, 2)])
            .await
            .unwrap();
        assert_eq!(read(&first, window(0, 0, 2)).await, vec![0, 1]);
        first.close().await;
        assert_eq!(client.scans.parked("orders"), 1);
        let lookups = broker.request_count(ApiKey::ListOffsets);

        let second = client
            .scans
            .acquire("orders", &[window(0, 2, 4)])
            .await
            .unwrap();

        assert_eq!(
            client.scans.parked("orders"),
            0,
            "the next page of a topic takes the consumer it left behind"
        );
        assert_eq!(read(&second, window(0, 2, 4)).await, vec![2, 3]);
        second.close().await;
        assert_eq!(
            broker.request_count(ApiKey::ListOffsets),
            lookups,
            "the pooled consumer already knows where it is"
        );
    }

    #[tokio::test]
    async fn a_waiting_fetch_does_not_hold_up_admin_calls() {
        let broker = Broker::orders(2).await;
        let client = broker.client().await;
        let scan = client
            .scans
            .acquire("orders", &[window(0, 0, 2)])
            .await
            .unwrap();
        broker.on(ApiKey::Fetch, |_| Control::Delay(Duration::from_secs(3)));
        broker.clear_requests();

        let polling = tokio::spawn(async move { scan.poll(Duration::from_secs(5)).await });
        assert!(
            broker
                .wait_for_requests(ApiKey::Fetch, 1, Duration::from_secs(2))
                .await
        );
        let listed = tokio::time::timeout(
            Duration::from_secs(1),
            client.transport.admin.list_offsets(
                [(TopicPartition::new("orders", 0), OffsetSpec::Latest)],
                ListOffsetsOptions::default(),
            ),
        )
        .await;

        assert!(
            listed.is_ok_and(|listed| listed.is_ok()),
            "an admin call must not queue behind a scan's fetch"
        );
        polling.abort();
    }

    #[tokio::test]
    async fn a_consumer_that_failed_mid_page_is_closed_instead_of_pooled() {
        let broker = Broker::orders(2).await;
        let client = broker.client().await;
        let scan = client
            .scans
            .acquire("orders", &[window(0, 0, 2)])
            .await
            .unwrap();
        let consumer = Arc::clone(&scan.consumer);
        // krafka retries a failed fetch inside poll and accepts any partition
        // on assign, so no broker answer fails the lease on cue.
        scan.reusable.store(false, Ordering::SeqCst);
        scan.close().await;

        assert_eq!(
            client.scans.parked("orders"),
            0,
            "a consumer that failed mid-page must not be handed out again"
        );
        eventually("a consumer the pool does not keep closing", || {
            consumer.is_closed()
        })
        .await;
    }

    #[tokio::test]
    async fn a_window_read_to_its_end_reports_its_position_and_no_lag() {
        let broker = Broker::orders(2).await;
        let client = broker.client().await;

        let scan = client
            .scans
            .acquire("orders", &[window(0, 0, 2)])
            .await
            .unwrap();

        assert_eq!(scan.position(0).await, Some(0));
        assert_eq!(
            scan.lag(0).await,
            None,
            "nothing fetched yet, so the high watermark is unknown"
        );
        assert_eq!(read(&scan, window(0, 0, 2)).await, vec![0, 1]);
        assert_eq!(scan.position(0).await, Some(2));
        assert_eq!(scan.lag(0).await, Some(0));

        scan.close().await;
    }

    #[tokio::test]
    async fn reseeking_between_passes_does_not_look_offsets_up_again() {
        let broker = Broker::orders(4).await;
        let client = broker.client().await;

        let scan = client
            .scans
            .acquire("orders", &[window(0, 2, 4)])
            .await
            .unwrap();
        assert_eq!(read(&scan, window(0, 2, 4)).await, vec![2, 3]);
        let after_first = broker.request_count(ApiKey::ListOffsets);

        scan.reassign(&[window(0, 0, 2)]).await.unwrap();

        assert_eq!(read(&scan, window(0, 0, 2)).await, vec![0, 1]);
        assert_eq!(
            broker.request_count(ApiKey::ListOffsets),
            after_first,
            "a second pass reuses the consumer's known position"
        );
        scan.close().await;
    }
}
