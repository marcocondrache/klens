use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use krafka::consumer::Consumer;

use crate::kafka::error::KafkaError;
use crate::kafka::model::{Compression, PartitionWindow, RawRecord, ScanConsumer};

use super::pool::{ScanPool, assign};

/// One page's hold on a pooled consumer.
///
/// Closing it releases the consumer back to the pool with its fetch session,
/// positions and read-ahead buffer intact. Only an error the consumer may not
/// have recovered from gives it up.
pub(super) struct ScanLease {
    pool: Arc<ScanPool>,
    topic: String,
    consumer: Arc<Consumer>,
    healthy: AtomicBool,
    released: AtomicBool,
}

impl ScanLease {
    pub(super) fn new(pool: Arc<ScanPool>, topic: &str, consumer: Arc<Consumer>) -> Self {
        Self {
            pool,
            topic: topic.to_owned(),
            consumer,
            healthy: AtomicBool::new(true),
            released: AtomicBool::new(false),
        }
    }

    /// A consumer that failed mid-page may hold a broken fetch session or a
    /// position nobody can account for; the next page starts clean instead.
    fn watch<T>(&self, result: Result<T, KafkaError>) -> Result<T, KafkaError> {
        if result.is_err() {
            self.healthy.store(false, Ordering::SeqCst);
        }
        result
    }

    fn release(&self) -> bool {
        if self.released.swap(true, Ordering::SeqCst) {
            return false;
        }
        self.pool.release(
            &self.topic,
            Arc::clone(&self.consumer),
            self.healthy.load(Ordering::SeqCst),
        );
        true
    }
}

#[async_trait]
impl ScanConsumer for ScanLease {
    async fn reassign(&self, windows: &[PartitionWindow]) -> Result<(), KafkaError> {
        self.watch(assign(&self.consumer, &self.topic, windows).await)
    }

    async fn poll(&self, budget: Duration) -> Result<Vec<RawRecord>, KafkaError> {
        // The budget is how long the broker may park the fetch, not a cap on
        // the round trip that carries it back: on a link slower than the
        // budget, cutting the poll off here would discard every response.
        // The scan bounds the page by its own deadline instead.
        let polled = self.watch(self.consumer.poll(budget).await.map_err(KafkaError::from))?;

        Ok(polled
            .into_iter()
            .map(|message| RawRecord {
                partition: message.partition,
                offset: message.offset,
                timestamp: message.timestamp,
                key: message.key,
                value: message.value,
                headers: message.headers,
                // krafka decodes batches before the consumer sees them and
                // does not carry the batch's codec on the record.
                compression: Compression::None,
            })
            .collect())
    }

    async fn pause(&self, partitions: &[i32]) {
        self.consumer.pause(&self.topic, partitions).await;
    }

    async fn position(&self, partition: i32) -> Option<i64> {
        self.consumer.position(&self.topic, partition).await
    }

    async fn lag(&self, partition: i32) -> Option<u64> {
        self.consumer.current_lag(&self.topic, partition).await
    }

    async fn close(&self) {
        if self.release() {
            self.pool.sweep().await;
        }
    }
}

impl Drop for ScanLease {
    fn drop(&mut self) {
        // Releasing is synchronous even here: whatever the pool declines is
        // closed by its janitor, not by a task spawned from a destructor.
        self.release();
    }
}

#[cfg(test)]
mod tests {
    use krafka::protocol::ApiKey;
    use krafka::testing::FakeBroker;

    use super::*;
    use crate::kafka::client::KafkaClient;
    use crate::kafka::session::ClusterSession;

    fn window(partition: i32, start: i64, end: i64) -> PartitionWindow {
        PartitionWindow {
            partition,
            start,
            end,
        }
    }

    /// Read a window to its end, discarding what the consumer reads past it —
    /// which is what the scan loop does with the same surplus.
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

    async fn client(broker: &FakeBroker) -> KafkaClient {
        KafkaClient::new(&crate::config::ClusterConfig {
            name: "test".into(),
            bootstrap_servers: vec![broker.bootstrap_servers()],
            security: None,
            schema_registry: None,
            obfuscation: None,
            properties: Default::default(),
        })
        .await
        .expect("kafka client")
    }

    #[tokio::test]
    async fn a_released_consumer_serves_the_next_page_of_the_same_topic() {
        let broker = FakeBroker::start().await.unwrap();
        assert!(broker.create_topic("orders", 1));
        super::super::tests::produce_krafka(&broker.bootstrap_servers(), "orders", 4).await;
        let client = client(&broker).await;

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
    async fn a_consumer_that_failed_mid_page_is_not_pooled() {
        let broker = FakeBroker::start().await.unwrap();
        assert!(broker.create_topic("orders", 1));
        super::super::tests::produce_krafka(&broker.bootstrap_servers(), "orders", 2).await;
        let client = client(&broker).await;

        let poisoned = client
            .scans
            .acquire("orders", &[window(0, 0, 2)])
            .await
            .unwrap();
        poisoned.healthy.store(false, Ordering::SeqCst);
        poisoned.close().await;

        assert_eq!(
            client.scans.parked("orders"),
            0,
            "a consumer that failed mid-page must not be handed out again"
        );
    }

    #[tokio::test]
    async fn a_window_read_to_its_end_reports_its_position_and_no_lag() {
        let broker = FakeBroker::start().await.unwrap();
        assert!(broker.create_topic("orders", 1));
        super::super::tests::produce_krafka(&broker.bootstrap_servers(), "orders", 2).await;
        let client = client(&broker).await;

        let scan = client
            .scans
            .acquire("orders", &[window(0, 0, 2)])
            .await
            .unwrap();

        assert_eq!(scan.position(0).await, Some(0));
        assert_eq!(read(&scan, window(0, 0, 2)).await, vec![0, 1]);
        assert_eq!(scan.position(0).await, Some(2));
        assert_eq!(scan.lag(0).await, Some(0));

        scan.close().await;
    }

    #[tokio::test]
    async fn reseeking_between_passes_does_not_look_offsets_up_again() {
        let broker = FakeBroker::start().await.unwrap();
        assert!(broker.create_topic("orders", 1));
        super::super::tests::produce_krafka(&broker.bootstrap_servers(), "orders", 4).await;
        let client = client(&broker).await;

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

    #[tokio::test]
    async fn an_opened_scan_resolves_no_offsets_of_its_own() {
        let broker = FakeBroker::start().await.unwrap();
        assert!(broker.create_topic("orders", 1));
        super::super::tests::produce_krafka(&broker.bootstrap_servers(), "orders", 2).await;
        let client = client(&broker).await;
        broker.clear_requests();

        let scan = client
            .open_scan("orders", &[window(0, 0, 2)])
            .await
            .unwrap();

        assert_eq!(
            broker.request_count(ApiKey::ListOffsets),
            0,
            "pre-seeded window starts leave nothing for auto-offset-reset to ask"
        );
        scan.close().await;
    }
}
