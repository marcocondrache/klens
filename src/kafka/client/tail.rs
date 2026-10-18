use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use foldhash::HashMap;
use krafka::consumer::{AutoOffsetReset, Consumer, TopicPartition};

use crate::kafka::error::KafkaError;
use crate::kafka::model::{RawRecord, TailConsumer, TailPosition};

use super::scan::{ReaderConfig, raw_record, reader};
use super::transport::Connector;

const RETIRE_GRACE: Duration = Duration::from_secs(1);

/// Each tail reads over a connection pool of its own, so a fetch the broker
/// holds open never queues the cluster's other requests behind it.
pub(super) struct TailLease {
    topic: String,
    consumer: Arc<Consumer>,
}

impl TailLease {
    pub(super) async fn open(
        connector: &Connector,
        topic: &str,
        start: &[TailPosition],
        config: ReaderConfig,
    ) -> Result<Self, KafkaError> {
        let offsets = start
            .iter()
            .map(|position| (position.partition, position.offset));
        let kafka = connector.connect().await?;
        let consumer = reader(&kafka, topic, offsets, config)
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

fn retire(consumer: Arc<Consumer>) {
    tokio::spawn(async move {
        let _ = tokio::time::timeout(RETIRE_GRACE, consumer.close()).await;
    });
}

#[async_trait]
impl TailConsumer for TailLease {
    async fn poll(&self, max_wait: Duration) -> Result<Vec<RawRecord>, KafkaError> {
        let polled = self.consumer.poll(max_wait).await?;
        Ok(polled.into_iter().map(raw_record).collect())
    }

    async fn position(&self, partition: i32) -> Option<i64> {
        self.consumer.position(&self.topic, partition).await
    }

    async fn lags(&self) -> HashMap<i32, u64> {
        self.consumer
            .lag()
            .await
            .into_iter()
            .filter(|(partition, _)| partition.topic == self.topic)
            .filter_map(|(partition, lag)| Some((partition.partition, lag.lag?)))
            .collect()
    }

    async fn seek(&self, positions: &[TailPosition]) -> Result<(), KafkaError> {
        let offsets = positions.iter().map(|position| {
            (
                TopicPartition::new(self.topic.as_str(), position.partition),
                position.offset,
            )
        });
        Ok(self.consumer.seek_many(offsets).await?)
    }
}

impl Drop for TailLease {
    fn drop(&mut self) {
        retire(Arc::clone(&self.consumer));
    }
}

#[cfg(test)]
mod tests {
    use krafka::testing::ApiKey;
    use krafka::testing::Control;

    use super::*;
    use crate::kafka::client::KafkaClient;
    use crate::kafka::client::testing::Broker;
    use crate::kafka::session::ClusterSession;
    use crate::testing::eventually;

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

    async fn answers_promptly(client: &KafkaClient) {
        let wanted = HashMap::from_iter([("orders".to_owned(), vec![0])]);
        tokio::time::timeout(Duration::from_secs(2), client.high_watermarks(&wanted))
            .await
            .expect("the cluster's requests are not queued behind the tail's")
            .expect("high watermarks");
    }

    #[tokio::test]
    async fn a_tail_reads_from_its_start_without_looking_offsets_up() {
        let broker = Broker::orders(4).await;
        let client = broker.client().await;
        broker.clear_requests();

        let tail = TailLease::open(&client.connector, "orders", &[at(0, 2)], client.tail_reader)
            .await
            .unwrap();

        assert_eq!(tail.position(0).await, Some(2));
        assert_eq!(read(&tail, 2).await, vec![2, 3]);
        assert_eq!(tail.position(0).await, Some(4));
        assert_eq!(tail.lags().await, HashMap::from_iter([(0, 0)]));
        assert_eq!(
            broker.request_count(ApiKey::ListOffsets),
            0,
            "the start offsets came from the caller"
        );
    }

    #[tokio::test]
    async fn a_seek_moves_where_the_next_poll_reads() {
        let broker = Broker::orders(4).await;
        let client = broker.client().await;
        let tail = TailLease::open(&client.connector, "orders", &[at(0, 0)], client.tail_reader)
            .await
            .unwrap();
        assert_eq!(read(&tail, 4).await, vec![0, 1, 2, 3]);

        tail.seek(&[at(0, 1)]).await.unwrap();

        assert_eq!(tail.position(0).await, Some(1));
        assert_eq!(
            tail.lags().await,
            HashMap::from_iter([(0, 3)]),
            "the high watermark is still known"
        );
        assert_eq!(read(&tail, 3).await, vec![1, 2, 3]);
    }

    #[tokio::test]
    async fn a_dropped_tail_closes_its_consumer() {
        let broker = Broker::orders(1).await;
        let client = broker.client().await;
        let tail = TailLease::open(&client.connector, "orders", &[at(0, 0)], client.tail_reader)
            .await
            .unwrap();
        let consumer = Arc::clone(&tail.consumer);

        drop(tail);

        eventually("a tail nobody holds closing its consumer", || {
            consumer.is_closed()
        })
        .await;
    }

    #[tokio::test]
    async fn a_tail_the_broker_stops_answering_holds_up_nothing_else() {
        let broker = Broker::orders(1).await;
        let client = broker.client().await;
        let tail = TailLease::open(&client.connector, "orders", &[at(0, 1)], client.tail_reader)
            .await
            .unwrap();

        let fetches = broker.request_count(ApiKey::Fetch);
        broker.on_once(ApiKey::Fetch, |_| Control::Silence);
        let _ = tokio::time::timeout(
            Duration::from_millis(50),
            tail.poll(Duration::from_millis(10)),
        )
        .await;
        assert!(
            broker
                .wait_for_requests(ApiKey::Fetch, fetches + 1, Duration::from_secs(1))
                .await
        );

        answers_promptly(&client).await;
        drop(tail);
        answers_promptly(&client).await;
    }
}
