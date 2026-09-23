use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use krafka::client::KrafkaClient as KrafkaSharedClient;
use krafka::consumer::{AutoOffsetReset, Consumer};

use crate::environment::TAIL_POLL_WAIT;
use crate::kafka::error::KafkaError;
use crate::kafka::model::{RawRecord, TailConsumer, TailPosition};

use super::pool::retire;
use super::scan::{raw_record, reader};

pub(super) struct TailLease {
    topic: String,
    consumer: Arc<Consumer>,
}

impl TailLease {
    pub(super) async fn open(
        client: &KrafkaSharedClient,
        topic: &str,
        start: &[TailPosition],
    ) -> Result<Self, KafkaError> {
        let offsets = start
            .iter()
            .map(|position| (position.partition, position.offset));
        let consumer = reader(client, topic, offsets, *TAIL_POLL_WAIT)
            .auto_offset_reset(AutoOffsetReset::Latest)
            .build()
            .await?;
        let consumer = Arc::new(consumer);

        let partitions = start.iter().map(|position| position.partition).collect();
        if let Err(error) = consumer.assign(topic, partitions).await {
            retire(consumer);
            return Err(error.into());
        }

        Ok(Self {
            topic: topic.to_owned(),
            consumer,
        })
    }
}

#[async_trait]
impl TailConsumer for TailLease {
    async fn poll(&self, budget: Duration) -> Result<Vec<RawRecord>, KafkaError> {
        let polled = self.consumer.poll(budget).await?;
        Ok(polled.into_iter().map(raw_record).collect())
    }

    async fn position(&self, partition: i32) -> Option<i64> {
        self.consumer.position(&self.topic, partition).await
    }

    async fn lag(&self, partition: i32) -> Option<u64> {
        self.consumer.current_lag(&self.topic, partition).await
    }

    async fn seek(&self, positions: &[TailPosition]) -> Result<(), KafkaError> {
        let offsets = positions
            .iter()
            .map(|position| ((self.topic.clone(), position.partition), position.offset))
            .collect();
        Ok(self.consumer.seek_many(&offsets).await?)
    }
}

impl Drop for TailLease {
    fn drop(&mut self) {
        retire(Arc::clone(&self.consumer));
    }
}

#[cfg(test)]
mod tests {
    use krafka::protocol::ApiKey;
    use krafka::testing::FakeBroker;

    use super::*;
    use crate::kafka::client::tests::{kafka_client, produce_krafka};

    fn at(partition: i32, offset: i64) -> TailPosition {
        TailPosition { partition, offset }
    }

    async fn read(tail: &TailLease, count: usize) -> Vec<i64> {
        let mut offsets = Vec::new();
        while offsets.len() < count {
            let polled = tail.poll(Duration::from_secs(1)).await.expect("poll");
            if polled.is_empty() {
                break;
            }
            offsets.extend(polled.into_iter().map(|record| record.offset));
        }
        offsets
    }

    async fn orders(count: usize) -> FakeBroker {
        let broker = FakeBroker::start().await.unwrap();
        assert!(broker.create_topic("orders", 1));
        produce_krafka(&broker.bootstrap_servers(), "orders", count).await;
        broker
    }

    #[tokio::test]
    async fn a_tail_reads_from_its_start_without_looking_offsets_up() {
        let broker = orders(4).await;
        let client = kafka_client(&broker.bootstrap_servers()).await;
        broker.clear_requests();

        let tail = TailLease::open(&client.transport.client, "orders", &[at(0, 2)])
            .await
            .unwrap();

        assert_eq!(tail.position(0).await, Some(2));
        assert_eq!(read(&tail, 2).await, vec![2, 3]);
        assert_eq!(tail.position(0).await, Some(4));
        assert_eq!(tail.lag(0).await, Some(0));
        assert_eq!(
            broker.request_count(ApiKey::ListOffsets),
            0,
            "the start offsets came from the caller"
        );
    }

    #[tokio::test]
    async fn a_seek_moves_where_the_next_poll_reads() {
        let broker = orders(4).await;
        let client = kafka_client(&broker.bootstrap_servers()).await;
        let tail = TailLease::open(&client.transport.client, "orders", &[at(0, 0)])
            .await
            .unwrap();
        assert_eq!(read(&tail, 4).await, vec![0, 1, 2, 3]);

        tail.seek(&[at(0, 1)]).await.unwrap();

        assert_eq!(tail.position(0).await, Some(1));
        assert_eq!(
            tail.lag(0).await,
            Some(3),
            "the high watermark is still known"
        );
        assert_eq!(read(&tail, 3).await, vec![1, 2, 3]);
    }

    #[tokio::test]
    async fn a_dropped_tail_closes_its_consumer() {
        let broker = orders(1).await;
        let client = kafka_client(&broker.bootstrap_servers()).await;
        let tail = TailLease::open(&client.transport.client, "orders", &[at(0, 0)])
            .await
            .unwrap();
        let consumer = Arc::clone(&tail.consumer);

        drop(tail);

        for _ in 0..1_000 {
            if consumer.is_closed() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        panic!("a tail nobody holds must not keep its consumer open");
    }
}
