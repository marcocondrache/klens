use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use krafka::client::KrafkaClient;
use krafka::consumer::{AutoOffsetReset, Consumer};

use crate::kafka::error::KafkaError;
use crate::kafka::model::{Compression, PartitionWindow, RawRecord, ScanConsumer};

pub(super) struct KrafkaScan {
    topic: String,
    consumer: Arc<Consumer>,
    closed: AtomicBool,
}

impl KrafkaScan {
    pub(super) async fn open(client: &KrafkaClient, topic: &str) -> Result<Self, KafkaError> {
        let consumer = Consumer::builder()
            .with_client(client)
            .enable_auto_commit(false)
            .auto_offset_reset(AutoOffsetReset::Earliest)
            .build()
            .await?;

        Ok(Self {
            topic: topic.to_owned(),
            consumer: Arc::new(consumer),
            closed: AtomicBool::new(false),
        })
    }
}

#[async_trait]
impl ScanConsumer for KrafkaScan {
    async fn assign(&self, windows: &[PartitionWindow]) -> Result<(), KafkaError> {
        let partitions: Vec<i32> = windows.iter().map(|window| window.partition).collect();
        self.consumer
            .assign(&self.topic, partitions.clone())
            .await?;
        self.consumer.resume(&self.topic, &partitions).await;

        // Seeking before the first poll also avoids an auto-offset-reset
        // lookup whose answer would immediately be overwritten.
        for window in windows {
            self.consumer
                .seek(&self.topic, window.partition, window.start)
                .await?;
        }
        Ok(())
    }

    async fn poll(&self, budget: Duration) -> Result<Vec<RawRecord>, KafkaError> {
        Ok(self
            .consumer
            .poll(budget)
            .await?
            .into_iter()
            .map(|message| RawRecord {
                partition: message.partition,
                offset: message.offset,
                timestamp: message.timestamp,
                key: message.key,
                value: message.value,
                headers: message.headers,
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
        if !self.closed.swap(true, Ordering::SeqCst) {
            let _ = self.consumer.close().await;
        }
    }
}

impl Drop for KrafkaScan {
    fn drop(&mut self) {
        if self.closed.swap(true, Ordering::SeqCst) {
            return;
        }

        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            let consumer = Arc::clone(&self.consumer);
            drop(handle.spawn(async move {
                let _ = consumer.close().await;
            }));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn position_and_lag_track_a_partially_read_window() {
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
        let scan = KrafkaScan {
            topic: "orders".to_owned(),
            consumer: Arc::new(consumer),
            closed: AtomicBool::new(false),
        };
        scan.assign(&[PartitionWindow {
            partition: 0,
            start: 0,
            end: 2,
        }])
        .await
        .unwrap();

        assert_eq!(scan.poll(Duration::from_secs(1)).await.unwrap().len(), 1);
        assert!(scan.position(0).await.is_some_and(|position| position < 2));
        assert_ne!(scan.lag(0).await, Some(0));

        assert_eq!(scan.poll(Duration::from_secs(1)).await.unwrap().len(), 1);
        assert_eq!(scan.position(0).await, Some(2));
        assert_eq!(scan.lag(0).await, Some(0));

        scan.close().await;
    }

    #[tokio::test]
    async fn reseeking_between_passes_does_not_look_offsets_up_again() {
        let broker = krafka::testing::FakeBroker::start().await.unwrap();
        assert!(broker.create_topic("orders", 1));
        super::super::tests::produce_krafka(&broker.bootstrap_servers(), "orders", 4).await;

        let consumer = Consumer::builder()
            .bootstrap_servers(broker.bootstrap_servers())
            .enable_auto_commit(false)
            .auto_offset_reset(AutoOffsetReset::Earliest)
            .build()
            .await
            .unwrap();
        let scan = KrafkaScan {
            topic: "orders".to_owned(),
            consumer: Arc::new(consumer),
            closed: AtomicBool::new(false),
        };

        let pass = async |start, end| {
            scan.assign(&[PartitionWindow {
                partition: 0,
                start,
                end,
            }])
            .await
            .unwrap();
            // A poll can overshoot the window from the fetch buffer; the
            // scan discards those, so the test does too.
            let mut offsets = Vec::new();
            while offsets.len() < (end - start) as usize {
                let polled = scan.poll(Duration::from_secs(1)).await.unwrap();
                offsets.extend(
                    polled
                        .into_iter()
                        .map(|record| record.offset)
                        .filter(|offset| (start..end).contains(offset)),
                );
            }
            offsets
        };

        assert_eq!(pass(2, 4).await, vec![2, 3]);
        let after_first = broker.request_count(krafka::protocol::ApiKey::ListOffsets);

        assert_eq!(pass(0, 2).await, vec![0, 1]);
        assert_eq!(
            broker.request_count(krafka::protocol::ApiKey::ListOffsets),
            after_first,
            "a second pass reuses the consumer's known position"
        );

        scan.close().await;
    }
}
