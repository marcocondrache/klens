use std::collections::VecDeque;
use std::ops::Deref;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use krafka::client::KrafkaClient as KrafkaSharedClient;
use krafka::consumer::{Consumer, ConsumerBuilder};

use crate::environment::{
    SCAN_PACE_BOUND, SCAN_POOL_IDLE_TTL, SCAN_POOL_PER_TOPIC, SCAN_POOL_TOTAL,
};
use crate::kafka::client::transport::{self, Connector};
use crate::kafka::error::KafkaError;
use crate::kafka::model::PartitionWindow;

use super::scan::{ScanLease, reader};

pub(super) struct ScanPool {
    connector: Connector,
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
    consumer: Arc<Reader>,
    since: Instant,
}

impl ScanPoolInner {
    fn new() -> Self {
        Self {
            parked: VecDeque::new(),
            max_per_topic: (*SCAN_POOL_PER_TOPIC).max(1),
            max_total: (*SCAN_POOL_TOTAL).max(1),
            ttl: *SCAN_POOL_IDLE_TTL,
        }
    }

    /// Newest match first: the most recently parked consumer has the
    /// freshest positions and metadata. Closed consumers found along the
    /// way are evicted.
    fn take(&mut self, topic: &str, evicted: &mut Vec<Arc<Reader>>) -> Option<Arc<Reader>> {
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

    /// A consumer over either cap is evicted rather than parked.
    fn park(&mut self, topic: &str, consumer: Arc<Reader>, evicted: &mut Vec<Arc<Reader>>) {
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

    /// The queue is sorted by age, so everything expired sits at the front.
    fn expire(&mut self, evicted: &mut Vec<Arc<Reader>>) {
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

pub(super) struct Reader {
    consumer: Consumer,
    client: KrafkaSharedClient,
}

impl Reader {
    pub(super) async fn open(
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
pub(super) fn retire(reader: Arc<Reader>) {
    tokio::spawn(async move {
        let _ = tokio::time::timeout(RETIRE_GRACE, reader.consumer.close()).await;
        reader.client.pool().close_all().await;
    });
}

impl ScanPool {
    pub(super) fn spawn(transport: &transport::Transport) -> Arc<Self> {
        let pool = Arc::new(Self {
            connector: transport.connector.clone(),
            inner: Mutex::new(ScanPoolInner::new()),
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

    pub(super) fn release(&self, topic: &str, consumer: Arc<Reader>, reusable: bool) {
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

    fn with_idle<T>(&self, op: impl FnOnce(&mut ScanPoolInner, &mut Vec<Arc<Reader>>) -> T) -> T {
        let mut evicted = Vec::new();
        let result = op(&mut self.inner.lock().expect("scan pool"), &mut evicted);
        for consumer in evicted {
            retire(consumer);
        }
        result
    }

    async fn build(&self, topic: &str, windows: &[PartitionWindow]) -> Result<Reader, KafkaError> {
        // The broker releases the long poll exactly when the scan stops
        // waiting for it, instead of holding a fetch nobody will read.
        let start = windows
            .iter()
            .map(|window| (window.partition, window.start));

        Reader::open(&self.connector, |client| {
            reader(client, topic, start, *SCAN_PACE_BOUND)
        })
        .await
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
