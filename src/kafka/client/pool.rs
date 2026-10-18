use std::collections::VecDeque;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

use krafka::Kafka;
use krafka::consumer::Consumer;

use crate::config::ScanTuning;
use crate::kafka::error::KafkaError;
use crate::kafka::model::PartitionWindow;

use super::scan::{ReaderConfig, ScanLease, reader, retire};

pub(super) struct ScanPool {
    kafka: Kafka,
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
            ttl: tuning.pool_idle_ttl,
        }
    }

    fn take(&mut self, topic: &str, evicted: &mut Vec<Arc<Consumer>>) -> Option<Arc<Consumer>> {
        self.expire(Instant::now(), evicted);

        while let Some(candidate) = self.remove_newest(topic) {
            if !candidate.consumer.is_closed() {
                return Some(candidate.consumer);
            }
            evicted.push(candidate.consumer);
        }
        None
    }

    fn remove_newest(&mut self, topic: &str) -> Option<Parked> {
        let index = self
            .parked
            .iter()
            .rposition(|parked| parked.topic == topic)?;
        self.parked.remove(index)
    }

    fn park(&mut self, topic: &str, consumer: Arc<Consumer>, evicted: &mut Vec<Arc<Consumer>>) {
        self.expire(Instant::now(), evicted);

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

    fn expire(&mut self, now: Instant, evicted: &mut Vec<Arc<Consumer>>) {
        while let Some(expired) = self
            .parked
            .pop_front_if(|parked| now.saturating_duration_since(parked.since) > self.ttl)
        {
            evicted.push(expired.consumer);
        }
    }
}

impl ScanPool {
    pub(super) fn spawn(kafka: Kafka, tuning: &ScanTuning, reader: ReaderConfig) -> Arc<Self> {
        let pool = Arc::new(Self {
            kafka,
            reader,
            inner: Mutex::new(ScanPoolInner::new(tuning)),
        });

        let weak = Arc::downgrade(&pool);
        let period = tuning.pool_idle_ttl;
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(period);
            ticker.tick().await;
            loop {
                ticker.tick().await;
                let Some(pool) = weak.upgrade() else {
                    return;
                };
                pool.with_idle(|idle, evicted| idle.expire(Instant::now(), evicted));
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
        self.idle()
            .parked
            .iter()
            .filter(|parked| parked.topic == topic)
            .count()
    }

    // Every update leaves the queue whole, so a guard a panic poisoned still
    // holds a usable pool.
    fn idle(&self) -> MutexGuard<'_, ScanPoolInner> {
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn with_idle<T>(&self, op: impl FnOnce(&mut ScanPoolInner, &mut Vec<Arc<Consumer>>) -> T) -> T {
        let mut evicted = Vec::new();
        let result = op(&mut self.idle(), &mut evicted);
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

        Ok(reader(&self.kafka, topic, start, self.reader)
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kafka::client::testing::Broker;

    #[tokio::test]
    async fn a_parked_consumer_is_retired_once_it_idles_past_the_ttl() {
        let broker = Broker::orders(1).await;
        let client = broker.client().await;
        let window = PartitionWindow {
            partition: 0,
            start: 0,
            end: 1,
        };
        let consumer = client.scans.build("orders", &[window]).await.unwrap();
        let mut idle = ScanPoolInner::new(&ScanTuning::default());
        let mut evicted = Vec::new();

        idle.park("orders", Arc::new(consumer), &mut evicted);
        let parked_at = idle.parked[0].since;

        idle.expire(parked_at + idle.ttl, &mut evicted);
        assert_eq!(
            idle.parked.len(),
            1,
            "a consumer idle for the ttl stays parked"
        );

        idle.expire(
            parked_at + idle.ttl + Duration::from_millis(1),
            &mut evicted,
        );
        assert!(idle.parked.is_empty());
        assert_eq!(evicted.len(), 1);
    }
}
