use std::ops::Deref;

use futures::StreamExt as _;
use futures::stream::FuturesUnordered;
use krafka::producer::{Producer, ProducerRecord};
use krafka::testing::FakeBroker;

use super::KafkaClient;
use crate::config::{self, Tuning};
use crate::testing::yaml;

pub struct Broker(FakeBroker);

impl Broker {
    pub async fn start() -> Self {
        Self(FakeBroker::start().await.expect("fake broker"))
    }

    pub async fn orders(records: usize) -> Self {
        let broker = Self::start().await;
        broker.topic("orders", 1);
        broker.produce("orders", records).await;
        broker
    }

    pub fn topic(&self, name: &str, partitions: i32) {
        assert!(
            self.0.create_topic(name, partitions),
            "topic {name} already exists"
        );
    }

    pub async fn produce(&self, topic: &str, count: usize) {
        let producer = self.producer().await;
        for _ in 0..count {
            let _metadata = producer
                .send(topic, Some(b"k"), Some(b"hello"))
                .await
                .expect("produce");
        }
    }

    pub async fn produce_to_each(&self, topic: &str, partitions: i32, count: usize) {
        let producer = self.producer().await;
        let mut acks = FuturesUnordered::new();
        for partition in 0..partitions {
            for _ in 0..count {
                let record = ProducerRecord::new(topic, &b"hello"[..]).with_partition(partition);
                acks.push(producer.enqueue(record).await.expect("enqueue"));
            }
        }
        while let Some(ack) = acks.next().await {
            let _metadata = ack.expect("produce");
        }
    }

    pub fn config(&self) -> config::Cluster {
        yaml(&format!(
            "bootstrap_servers: ['{}']",
            self.bootstrap_servers()
        ))
    }

    pub async fn client(&self) -> KafkaClient {
        self.client_with(&Tuning::default()).await
    }

    pub async fn client_with(&self, tuning: &Tuning) -> KafkaClient {
        KafkaClient::new("test", &self.config(), tuning)
            .await
            .expect("kafka client")
    }

    async fn producer(&self) -> Producer {
        Producer::builder()
            .bootstrap_servers(self.bootstrap_servers())
            .build()
            .await
            .expect("producer")
    }
}

impl Deref for Broker {
    type Target = FakeBroker;

    fn deref(&self) -> &FakeBroker {
        &self.0
    }
}
