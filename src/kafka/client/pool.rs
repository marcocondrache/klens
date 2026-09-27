use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use krafka::client::KrafkaClient as KrafkaSharedClient;
use krafka::consumer::Consumer;

use crate::config::ScanTuning;
use crate::kafka::client::transport;
use crate::kafka::error::KafkaError;
use crate::kafka::model::PartitionWindow;

use super::scan::{ReaderConfig, ScanLease, reader};

pub(super) struct ScanPool {
    client: KrafkaSharedClient,
    reader: ReaderConfig,
    inner: Mutex<ScanPoolInner>,
}

struct ScanPoolInner {
    parked: VecDeque<Parked>,
    max_per_topic: usize,
    max_total: usize,
    ttl: Duration,
}

struct Parked {
    topic: String,
    consumer: Arc<Consumer>,
    since: Instant,
}

impl ScanPoolInner {
    fn new(tuning: &ScanTuning) -> Self {
        Self {
            parked: VecDeque::new(),
            max_per_topic: tuning.pool_per_topic.get(),
            max_total: tuning.pool_total.get(),
            ttl: tuning.pool_idle_ttl.get(),
        }
    }

    fn take(&mut self, topic: &str, evicted: &mut Vec<Arc<Consumer>>) -> Option<Arc<Consumer>> {
        self.expire(evicted);

        while let Some(index) = self.parked.iter().rposition(|parked| parked.topic == topic) {
            let candidate = self.parked.remove(index).expect("index from rposition");
            if !candidate.consumer.is_closed() {
                return Some(candidate.consumer);
            }
            evicted.push(candidate.consumer);
        }
        None
    }

    fn park(&mut self, topic: &str, consumer: Arc<Consumer>, evicted: &mut Vec<Arc<Consumer>>) {
        self.expire(evicted);

        let same_topic = self
            .parked
            .iter()
            .filter(|parked| parked.topic == topic)
            .count();
        if self.parked.len() >= self.max_total || same_topic >= self.max_per_topic {
            evicted.push(consumer);
            return;
        }

        self.parked.push_back(Parked {
            topic: topic.to_owned(),
            consumer,
            since: Instant::now(),
        });
    }

    fn expire(&mut self, evicted: &mut Vec<Arc<Consumer>>) {
        while self
            .parked
            .front()
            .is_some_and(|parked| parked.since.elapsed() > self.ttl)
        {
            let expired = self.parked.pop_front().expect("non-empty front");
            evicted.push(expired.consumer);
        }
    }
}

pub(super) fn retire(consumer: Arc<Consumer>) {
    tokio::spawn(async move {
        let _ = consumer.close().await;
    });
}

impl ScanPool {
    pub(super) fn spawn(
        transport: &transport::Transport,
        tuning: &ScanTuning,
        reader: ReaderConfig,
    ) -> Arc<Self> {
        let pool = Arc::new(Self {
            client: transport.client.clone(),
            reader,
            inner: Mutex::new(ScanPoolInner::new(tuning)),
        });

        let weak = Arc::downgrade(&pool);
        let period = pool.inner.lock().expect("scan pool").ttl;
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(period);
            ticker.tick().await;
            loop {
                ticker.tick().await;
                let Some(pool) = weak.upgrade() else {
                    return;
                };
                pool.with_idle(|idle, evicted| idle.expire(evicted));
            }
        });

        pool
    }

    pub(super) async fn acquire(
        self: &Arc<Self>,
        topic: &str,
        windows: &[PartitionWindow],
    ) -> Result<ScanLease, KafkaError> {
        let taken = self.with_idle(|idle, evicted| idle.take(topic, evicted));
        let consumer = match taken {
            Some(consumer) => consumer,
            None => Arc::new(self.build(topic, windows).await?),
        };
        if let Err(error) = assign(&consumer, topic, windows).await {
            retire(consumer);
            return Err(error);
        }

        Ok(ScanLease::new(Arc::clone(self), topic, consumer))
    }

    pub(super) fn release(&self, topic: &str, consumer: Arc<Consumer>, reusable: bool) {
        if !reusable {
            retire(consumer);
            return;
        }
        self.with_idle(|idle, evicted| idle.park(topic, consumer, evicted));
    }

    #[cfg(test)]
    pub(super) fn parked(&self, topic: &str) -> usize {
        self.inner
            .lock()
            .expect("scan pool")
            .parked
            .iter()
            .filter(|parked| parked.topic == topic)
            .count()
    }

    fn with_idle<T>(&self, op: impl FnOnce(&mut ScanPoolInner, &mut Vec<Arc<Consumer>>) -> T) -> T {
        let mut evicted = Vec::new();
        let result = op(&mut self.inner.lock().expect("scan pool"), &mut evicted);
        for consumer in evicted {
            retire(consumer);
        }
        result
    }

    async fn build(
        &self,
        topic: &str,
        windows: &[PartitionWindow],
    ) -> Result<Consumer, KafkaError> {
        let start = windows
            .iter()
            .map(|window| (window.partition, window.start));

        Ok(reader(&self.client, topic, start, self.reader)
            .build()
            .await?)
    }
}

/// A partition already sitting at its window start is left alone: seeking
/// there would discard the records the last poll read ahead, which for a
/// continuing page are the ones it is about to ask for.
pub(super) async fn assign(
    consumer: &Consumer,
    topic: &str,
    windows: &[PartitionWindow],
) -> Result<(), KafkaError> {
    let partitions: Vec<i32> = windows.iter().map(|window| window.partition).collect();
    consumer.assign(topic, partitions.clone()).await?;
    consumer.resume(topic, &partitions).await;

    for window in windows {
        if consumer.position(topic, window.partition).await == Some(window.start) {
            continue;
        }
        consumer.seek(topic, window.partition, window.start).await?;
    }
    Ok(())
}
