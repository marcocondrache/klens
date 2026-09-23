use std::ops::Deref;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use krafka::client::KrafkaClient as KrafkaSharedClient;
use krafka::consumer::{AutoOffsetReset, Consumer, ConsumerBuilder};

use crate::environment::TAIL_POLL_WAIT;
use crate::kafka::error::KafkaError;
use crate::kafka::model::{RawRecord, TailConsumer, TailPosition};

use super::scan::{raw_record, reader};
use super::transport::Connector;

struct Reader {
    consumer: Consumer,
    client: KrafkaSharedClient,
}

impl Reader {
    async fn open(
        connector: &Connector,
        configure: impl FnOnce(&KrafkaSharedClient) -> ConsumerBuilder,
    ) -> Result<Self, KafkaError> {
        let client = connector.connect().await?;
        match configure(&client).build().await {
            Ok(consumer) => Ok(Self { consumer, client }),
            Err(error) => {
                client.pool().close_all().await;
                Err(error.into())
            }
        }
    }
}

impl Deref for Reader {
    type Target = Consumer;

    fn deref(&self) -> &Consumer {
        &self.consumer
    }
}

const RETIRE_GRACE: Duration = Duration::from_secs(1);

/// Nothing waits on a retired consumer, so closing it is fire-and-forget.
fn retire(reader: Arc<Reader>) {
    tokio::spawn(async move {
        let _ = tokio::time::timeout(RETIRE_GRACE, reader.consumer.close()).await;
        reader.client.pool().close_all().await;
    });
}

pub(super) struct TailLease {
    topic: String,
    consumer: Arc<Reader>,
}

impl TailLease {
    pub(super) async fn open(
        connector: &Connector,
        topic: &str,
        start: &[TailPosition],
    ) -> Result<Self, KafkaError> {
        let offsets = start
            .iter()
            .map(|position| (position.partition, position.offset));
        let consumer = Reader::open(connector, |client| {
            reader(client, topic, offsets, *TAIL_POLL_WAIT)
                .auto_offset_reset(AutoOffsetReset::Latest)
        })
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
    use foldhash::HashMap;
    use krafka::protocol::ApiKey;
    use krafka::testing::{Control, FakeBroker};

    use super::*;
    use crate::kafka::client::KafkaClient;
    use crate::kafka::client::tests::{kafka_client, produce_krafka};
    use crate::kafka::session::ClusterSession;

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
        tokio::time::timeout(Duration::from_secs(2), client.watermarks(&wanted))
            .await
            .expect("the cluster's requests are not queued behind the tail's")
            .expect("watermarks");
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

        let tail = TailLease::open(&client.transport.connector, "orders", &[at(0, 2)])
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
        let tail = TailLease::open(&client.transport.connector, "orders", &[at(0, 0)])
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
        let tail = TailLease::open(&client.transport.connector, "orders", &[at(0, 0)])
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

    #[tokio::test]
    async fn a_tail_the_broker_stops_answering_holds_up_nothing_else() {
        let broker = orders(1).await;
        let client = kafka_client(&broker.bootstrap_servers()).await;
        let tail = TailLease::open(&client.transport.connector, "orders", &[at(0, 1)])
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
