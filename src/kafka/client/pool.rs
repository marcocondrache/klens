use std::collections::HashMap;
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant};

use krafka::client::KrafkaClient as KrafkaSharedClient;
use krafka::consumer::{AutoOffsetReset, Consumer};

use crate::environment::{
    MAX_RECORD_LIMIT, SCAN_POOL_IDLE_TTL, SCAN_POOL_PER_TOPIC, SCAN_POOL_TOTAL,
};
use crate::kafka::error::KafkaError;
use crate::kafka::model::PartitionWindow;
use crate::kafka::scan::session::ScanPace;

use super::budget::ConnectionBudget;
use super::scan::ScanHold;

pub(super) struct ScanPool {
    client: KrafkaSharedClient,
    idle: Mutex<Idle>,
    graveyard: Graveyard,
    budget: ConnectionBudget,
    max_per_topic: usize,
    max_total: usize,
    idle_ttl: Duration,
}

#[derive(Default)]
struct Idle {
    topics: HashMap<String, Vec<Parked>>,
    total: usize,
}

struct Parked {
    consumer: Arc<Consumer>,
    since: Instant,
}

struct Graveyard {
    waiting: Mutex<Vec<Arc<Consumer>>>,
}

impl Graveyard {
    fn new() -> Self {
        Self {
            waiting: Mutex::new(Vec::new()),
        }
    }

    fn push(&self, consumer: Arc<Consumer>) {
        self.waiting
            .lock()
            .expect("scan pool graveyard")
            .push(consumer);
    }

    fn extend(&self, consumers: Vec<Arc<Consumer>>) {
        self.waiting
            .lock()
            .expect("scan pool graveyard")
            .extend(consumers);
    }

    fn drain(&self) -> Vec<Arc<Consumer>> {
        std::mem::take(&mut *self.waiting.lock().expect("scan pool graveyard"))
    }
}

struct Janitor {
    pool: Weak<ScanPool>,
    period: Duration,
}

impl Janitor {
    fn spawn(self) {
        tokio::spawn(async move { self.run().await });
    }

    async fn run(self) {
        let mut ticker = tokio::time::interval(self.period);
        ticker.tick().await;
        loop {
            ticker.tick().await;
            let Some(pool) = self.pool.upgrade() else {
                return;
            };
            pool.expire_idle();
            pool.sweep().await;
        }
    }
}

impl ScanPool {
    pub(super) fn spawn(client: KrafkaSharedClient, budget: ConnectionBudget) -> Arc<Self> {
        let pool = Arc::new(Self {
            client,
            idle: Mutex::new(Idle::default()),
            graveyard: Graveyard::new(),
            budget,
            max_per_topic: (*SCAN_POOL_PER_TOPIC).max(1),
            max_total: (*SCAN_POOL_TOTAL).max(1),
            idle_ttl: *SCAN_POOL_IDLE_TTL,
        });
        Janitor {
            pool: Arc::downgrade(&pool),
            period: pool.idle_ttl,
        }
        .spawn();
        pool
    }

    pub(super) async fn acquire(
        self: &Arc<Self>,
        topic: &str,
        windows: &[PartitionWindow],
    ) -> Result<ScanHold, KafkaError> {
        let consumer = match self.take(topic) {
            Some(consumer) => consumer,
            None => Arc::new(self.build(topic, windows).await?),
        };
        if let Err(error) = assign(&consumer, topic, windows).await {
            self.discard(consumer);
            self.sweep().await;
            return Err(error);
        }

        Ok(ScanHold::new(Arc::clone(self), topic, consumer))
    }

    #[cfg(test)]
    pub(super) fn parked(&self, topic: &str) -> usize {
        self.idle
            .lock()
            .expect("scan pool")
            .topics
            .get(topic)
            .map_or(0, Vec::len)
    }

    pub(super) fn release(&self, topic: &str, consumer: Arc<Consumer>, reusable: bool) {
        if !reusable {
            self.discard(consumer);
            return;
        }

        let mut idle = self.idle.lock().expect("scan pool");
        let full = idle.total >= self.max_total
            || idle
                .topics
                .get(topic)
                .is_some_and(|parked| parked.len() >= self.max_per_topic);
        if full {
            drop(idle);
            self.discard(consumer);
            return;
        }

        idle.topics
            .entry(topic.to_owned())
            .or_default()
            .push(Parked {
                consumer,
                since: Instant::now(),
            });
        idle.total += 1;
    }

    pub(super) async fn sweep(&self) {
        for consumer in self.graveyard.drain() {
            let _ = consumer.close().await;
        }
    }

    fn take(&self, topic: &str) -> Option<Arc<Consumer>> {
        let mut stale = Vec::new();
        let mut taken = None;

        {
            let mut idle = self.idle.lock().expect("scan pool");
            if let Some(parked) = idle.topics.get_mut(topic) {
                while let Some(candidate) = parked.pop() {
                    if candidate.since.elapsed() <= self.idle_ttl && !candidate.consumer.is_closed()
                    {
                        taken = Some(candidate.consumer);
                        break;
                    }
                    stale.push(candidate.consumer);
                }
            }
            idle.total -= stale.len() + usize::from(taken.is_some());
        }

        if !stale.is_empty() {
            self.graveyard.extend(stale);
        }
        taken
    }

    fn expire_idle(&self) {
        let mut expired = Vec::new();
        {
            let mut idle = self.idle.lock().expect("scan pool");
            let ttl = self.idle_ttl;
            idle.topics.retain(|_, parked| {
                parked.retain(|candidate| {
                    if candidate.since.elapsed() <= ttl {
                        return true;
                    }
                    expired.push(Arc::clone(&candidate.consumer));
                    false
                });
                !parked.is_empty()
            });
            idle.total -= expired.len();
        }

        self.graveyard.extend(expired);
    }

    fn discard(&self, consumer: Arc<Consumer>) {
        self.graveyard.push(consumer);
    }

    async fn build(
        &self,
        topic: &str,
        windows: &[PartitionWindow],
    ) -> Result<Consumer, KafkaError> {
        let page_limit = i32::try_from(*MAX_RECORD_LIMIT).unwrap_or(i32::MAX);

        Ok(Consumer::builder()
            .with_client(&self.client)
            .enable_auto_commit(false)
            .auto_offset_reset(AutoOffsetReset::Earliest)
            // The broker releases the long poll exactly when the scan stops
            // waiting for it, instead of holding a fetch nobody will read.
            .fetch_max_wait(ScanPace::ALIGNED.park())
            .max_poll_records(page_limit)
            .max_buffered_records(page_limit.saturating_mul(2))
            // krafka's 50 MB default is wider than the frame the connection
            // now accepts, which would make a busy fetch unreadable.
            .fetch_max_bytes(self.budget.fetch_max_bytes())
            // With the window starts already known, the first assignment
            // resolves no offsets.
            .initial_offsets(
                windows
                    .iter()
                    .map(|window| ((topic.to_owned(), window.partition), window.start))
                    .collect(),
            )
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
