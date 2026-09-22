use std::time::Duration;

use async_trait::async_trait;
use foldhash::HashMap;
use tokio::time::{Instant, sleep_until, timeout};

use crate::environment::TAIL_POLL_WAIT;
use crate::kafka::error::KafkaError;
use crate::kafka::limits::TailLimits;
use crate::kafka::session::ClusterSession;
use crate::kafka::store::ClusterStore;

use super::Record;
use super::batch::RecordBatch;
use super::filter::CompiledFilter;
use super::pipeline::{Kept, RecordPipeline, Screen};
use super::query::RecordOrder;
use super::read::resolve_partitions;
use super::session::{RawRecord, topic_obfuscator};

#[async_trait]
pub trait TailConsumer: Send + Sync {
    async fn poll(&self, budget: Duration) -> Result<Vec<RawRecord>, KafkaError>;

    async fn position(&self, partition: i32) -> Option<i64>;

    async fn lag(&self, partition: i32) -> Option<u64>;

    async fn seek(&self, positions: &[TailPosition]) -> Result<(), KafkaError>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TailPosition {
    pub partition: i32,
    pub offset: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TailQuery {
    pub topic: String,
    /// Partitions to follow; empty follows every partition of the topic.
    pub partitions: Vec<i32>,
    pub filter: Option<CompiledFilter>,
    pub schema_id: Option<i32>,
}

impl TailQuery {
    fn searching(&self) -> bool {
        self.filter.is_some() || self.schema_id.is_some()
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TailBatch {
    pub records: Vec<Record>,
    pub skipped: u64,
}

impl TailBatch {
    pub fn is_empty(&self) -> bool {
        self.records.is_empty() && self.skipped == 0
    }
}

pub struct Tail {
    consumer: Box<dyn TailConsumer>,
    pipeline: RecordPipeline,
    topic: String,
    start: Vec<TailPosition>,
    backlog: u64,
    limits: TailLimits,
    flushed: Option<Instant>,
}

impl Tail {
    pub async fn open<S: ClusterSession + ?Sized>(
        session: &S,
        store: &ClusterStore,
        query: TailQuery,
        limits: TailLimits,
    ) -> Result<Self, KafkaError> {
        let partitions =
            resolve_partitions(session, store, &query.topic, &query.partitions).await?;
        let wanted = HashMap::from_iter([(query.topic.clone(), partitions)]);
        let mut start: Vec<TailPosition> = session
            .watermarks(&wanted)
            .await?
            .remove(&query.topic)
            .unwrap_or_default()
            .into_iter()
            .map(|(partition, marks)| TailPosition {
                partition,
                offset: marks.high,
            })
            .collect();
        start.sort_unstable_by_key(|position| position.partition);

        let consumer = timeout(
            session.consume_timeout(),
            session.open_tail(&query.topic, &start),
        )
        .await
        .map_err(|_| KafkaError::Timeout)??;

        Ok(Self {
            consumer,
            pipeline: RecordPipeline::new(
                session.payload_codec(),
                query.filter.clone(),
                topic_obfuscator(session, &query.topic),
                query.schema_id,
            ),
            backlog: limits.backlog(query.searching()),
            topic: query.topic,
            start,
            limits,
            flushed: None,
        })
    }

    pub fn start(&self) -> &[TailPosition] {
        &self.start
    }

    pub fn obfuscated(&self) -> bool {
        self.pipeline.obfuscated()
    }

    pub async fn next(&mut self) -> Result<TailBatch, KafkaError> {
        let called = Instant::now();
        let quiet_until = called + self.limits.heartbeat;
        let ready_at = self
            .flushed
            .map_or(called, |flushed| flushed + self.limits.interval);

        let mut batch = RecordBatch::new(self.limits.batch, RecordOrder::Newest);
        let mut skipped = 0;
        while batch.is_empty() {
            if Instant::now() >= quiet_until {
                return Ok(TailBatch {
                    records: Vec::new(),
                    skipped,
                });
            }
            let polled_at = Instant::now();
            let polled = self.consumer.poll(*TAIL_POLL_WAIT).await?;
            if polled.is_empty() {
                sleep_until(polled_at + *TAIL_POLL_WAIT).await;
            }
            skipped += self.ingest(polled, &mut batch).await;
            skipped += self.skip_ahead().await?;
        }

        sleep_until(ready_at).await;
        self.flushed = Some(Instant::now());
        skipped += batch.displaced() as u64;

        let mut records = self.pipeline.decode_deferred(batch.into_sorted()).await;
        records.sort_by(|left, right| {
            left.raw()
                .sort_key()
                .cmp_for_order(&right.raw().sort_key(), RecordOrder::Oldest)
        });
        Ok(TailBatch {
            records: records
                .into_iter()
                .map(|record| record.into_record(&self.topic))
                .collect(),
            skipped,
        })
    }

    async fn ingest(&self, polled: Vec<RawRecord>, batch: &mut RecordBatch<Kept>) -> u64 {
        let mut rejected = 0;
        let mut candidates = Vec::new();

        for mut raw in polled {
            let sort = raw.sort_key();
            if !batch.admits(&sort) {
                rejected += 1;
                continue;
            }

            match self.pipeline.screen(&mut raw) {
                None => continue,
                Some(Screen::Deferred) => batch.push(sort, Kept::pending(raw)),
                Some(Screen::NeedsPayload) => candidates.push(raw),
            }
        }

        for kept in self.pipeline.decode_and_filter(candidates).await {
            batch.push(kept.raw().sort_key(), kept);
        }
        rejected
    }

    async fn skip_ahead(&self) -> Result<u64, KafkaError> {
        let mut seeks = Vec::new();
        let mut skipped = 0;

        for &TailPosition { partition, .. } in &self.start {
            let Some(lag) = self.consumer.lag(partition).await else {
                continue;
            };
            if lag <= self.backlog {
                continue;
            }
            let Some(position) = self.consumer.position(partition).await else {
                continue;
            };

            let jump = lag - self.backlog;
            skipped += jump;
            seeks.push(TailPosition {
                partition,
                offset: position.saturating_add(jump as i64),
            });
        }

        if !seeks.is_empty() {
            self.consumer.seek(&seeks).await?;
        }
        Ok(skipped)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::kafka::limits::RecordLimits;
    use crate::kafka::scan::Compression;
    use crate::kafka::scan::filter::contains;
    use crate::kafka::store::fixtures::{identity, partition, topic, topology};
    use crate::kafka::testing::{FAKE_TAIL_POLL_RECORDS, FakeCluster, card_record};

    const LIMITS: TailLimits = TailLimits {
        batch: 3,
        interval: Duration::from_secs(1),
        heartbeat: Duration::from_secs(15),
        records: RecordLimits {
            max_limit: 500,
            min_window: 1,
            window_multiplier: 2,
            search_window_multiplier: 4,
        },
    };

    fn record(partition: i32, offset: i64, timestamp: i64, key: &str) -> Record {
        Record {
            topic: "orders.created".into(),
            partition,
            offset,
            timestamp,
            key: Some(key.to_owned()),
            value: None,
            schema_id: None,
            headers: Vec::new(),
            size_bytes: key.len() as u64,
            compression: Compression::None,
        }
    }

    fn query() -> TailQuery {
        TailQuery {
            topic: "orders.created".into(),
            partitions: Vec::new(),
            filter: None,
            schema_id: None,
        }
    }

    async fn open(session: &FakeCluster, query: TailQuery) -> Tail {
        let store = ClusterStore::new(identity("local"));
        Tail::open(session, &store, query, LIMITS).await.unwrap()
    }

    fn produce_run(session: &FakeCluster, offsets: std::ops::Range<i64>) {
        for offset in offsets {
            session.produce(record(0, offset, offset, &format!("ord_{offset}")));
        }
    }

    fn keys(batch: &TailBatch) -> Vec<(i32, i64)> {
        batch
            .records
            .iter()
            .map(|record| (record.partition, record.offset))
            .collect()
    }

    #[test]
    fn a_batch_with_nothing_to_say_is_empty() {
        assert!(TailBatch::default().is_empty());
        assert!(
            !TailBatch {
                records: Vec::new(),
                skipped: 3,
            }
            .is_empty(),
            "skipping is news even without records"
        );
        assert!(
            !TailBatch {
                records: vec![record(0, 0, 0, "k")],
                skipped: 0,
            }
            .is_empty()
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_tail_starts_at_the_end_of_the_log() {
        let session = FakeCluster::local();
        let mut tail = open(&session, query()).await;

        assert_eq!(
            tail.start(),
            [
                TailPosition {
                    partition: 0,
                    offset: 8,
                },
                TailPosition {
                    partition: 1,
                    offset: 8,
                },
            ]
        );

        session.produce(record(0, 8, 8, "new"));
        let batch = tail.next().await.unwrap();

        assert_eq!(keys(&batch), vec![(0, 8)], "nothing already in the log");
        assert_eq!(batch.records[0].key.as_deref(), Some("new"));
        assert_eq!(batch.skipped, 0);
        assert!(session.tail_seeks().is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn a_tail_on_one_partition_follows_only_that_partition() {
        let session = FakeCluster::local();
        let mut named = query();
        named.partitions = vec![1];
        let mut tail = open(&session, named).await;

        assert_eq!(
            tail.start(),
            [TailPosition {
                partition: 1,
                offset: 8,
            }]
        );

        session.produce(record(0, 8, 8, "elsewhere"));
        session.produce(record(1, 8, 9, "here"));

        assert_eq!(keys(&tail.next().await.unwrap()), vec![(1, 8)]);
    }

    #[tokio::test(start_paused = true)]
    async fn a_batch_reads_oldest_first_across_partitions() {
        let session = FakeCluster::local();
        let mut tail = open(&session, query()).await;

        session.produce(record(1, 8, 1_000, "a"));
        session.produce(record(0, 8, 3_000, "c"));
        session.produce(record(0, 9, 2_000, "b"));

        assert_eq!(
            keys(&tail.next().await.unwrap()),
            vec![(1, 8), (0, 9), (0, 8)]
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_filter_sends_only_matching_records() {
        let session = FakeCluster::local();
        let mut filtered = query();
        filtered.filter = contains("hit");
        let mut tail = open(&session, filtered).await;

        session.produce(record(0, 8, 8, "miss"));
        session.produce(record(0, 9, 9, "hit"));
        let batch = tail.next().await.unwrap();

        assert_eq!(keys(&batch), vec![(0, 9)]);
        assert_eq!(batch.skipped, 0, "a record the filter drops is not skipped");
    }

    #[tokio::test(start_paused = true)]
    async fn a_full_batch_keeps_the_newest_and_counts_what_it_let_go() {
        let session = FakeCluster::local();
        let mut tail = open(&session, query()).await;

        produce_run(&session, 8..12);
        let batch = tail.next().await.unwrap();

        assert_eq!(keys(&batch), vec![(0, 9), (0, 10), (0, 11)]);
        assert_eq!(batch.skipped, 1);
    }

    #[tokio::test(start_paused = true)]
    async fn a_full_batch_rejects_older_records_before_screening_them() {
        let session = FakeCluster::local();
        let mut tail = open(&session, query()).await;

        for (offset, timestamp) in [(8, 40), (9, 30), (10, 20), (11, 10)] {
            session.produce(record(0, offset, timestamp, "k"));
        }
        let batch = tail.next().await.unwrap();

        assert_eq!(keys(&batch), vec![(0, 10), (0, 9), (0, 8)]);
        assert_eq!(batch.skipped, 1, "the oldest never made it in");
    }

    #[tokio::test(start_paused = true)]
    async fn batches_are_at_least_an_interval_apart() {
        let session = FakeCluster::local();
        let mut tail = open(&session, query()).await;

        session.produce(record(0, 8, 8, "first"));
        let started = Instant::now();
        tail.next().await.unwrap();
        assert_eq!(
            started.elapsed(),
            Duration::ZERO,
            "the first batch never waits"
        );

        session.produce(record(0, 9, 9, "second"));
        let started = Instant::now();
        assert_eq!(keys(&tail.next().await.unwrap()), vec![(0, 9)]);
        assert_eq!(started.elapsed(), LIMITS.interval);

        tokio::time::sleep(LIMITS.interval * 5).await;
        session.produce(record(0, 10, 10, "third"));
        let started = Instant::now();
        tail.next().await.unwrap();
        assert_eq!(
            started.elapsed(),
            Duration::ZERO,
            "an interval that already passed costs nothing"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_quiet_topic_hands_back_an_empty_batch_every_heartbeat() {
        let session = FakeCluster::local();
        let mut tail = open(&session, query()).await;
        let started = Instant::now();

        let batch = tail.next().await.unwrap();

        assert!(batch.is_empty());
        assert_eq!(started.elapsed(), LIMITS.heartbeat);
    }

    #[tokio::test(start_paused = true)]
    async fn a_partition_far_behind_skips_ahead_to_one_backlog() {
        let session = FakeCluster::local();
        let mut tail = open(&session, query()).await;

        produce_run(&session, 8..41);
        let first = tail.next().await.unwrap();

        assert_eq!(FAKE_TAIL_POLL_RECORDS, 4);
        assert_eq!(session.tail_seeks(), vec![vec![(0, 35)]]);
        assert_eq!(keys(&first), vec![(0, 9), (0, 10), (0, 11)]);
        assert_eq!(
            first.skipped,
            23 + 1,
            "the jump and the record the batch let go"
        );

        let second = tail.next().await.unwrap();
        assert_eq!(keys(&second), vec![(0, 36), (0, 37), (0, 38)]);
        assert_eq!(second.skipped, 1);
        assert_eq!(
            session.tail_seeks().len(),
            1,
            "caught up, so no second jump"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_partition_exactly_one_backlog_behind_stays_put() {
        let session = FakeCluster::local();
        let mut tail = open(&session, query()).await;

        produce_run(&session, 8..18);
        tail.next().await.unwrap();

        assert!(session.tail_seeks().is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn a_filtered_tail_reads_a_longer_backlog_before_skipping() {
        let session = FakeCluster::local();
        let mut filtered = query();
        filtered.filter = contains("ord");
        let mut tail = open(&session, filtered).await;

        produce_run(&session, 8..41);
        tail.next().await.unwrap();

        assert_eq!(session.tail_seeks(), vec![vec![(0, 29)]]);
    }

    #[tokio::test]
    async fn a_tail_with_nothing_assigned_waits_out_each_poll() {
        let session = FakeCluster::local();
        let store = ClusterStore::new(identity("local"));
        store.topology.commit(Arc::new(topology(
            vec![topic("unwritten", vec![partition(0, vec![1], vec![1])])],
            Vec::new(),
        )));
        let mut unwritten = query();
        unwritten.topic = "unwritten".into();
        let limits = TailLimits {
            heartbeat: Duration::from_millis(50),
            ..LIMITS
        };
        let mut tail = Tail::open(&session, &store, unwritten, limits)
            .await
            .unwrap();
        assert!(tail.start().is_empty(), "no partition has a high watermark");

        let batch = tail.next().await.unwrap();

        assert!(batch.is_empty());
        assert_eq!(
            session.tail_polls(),
            1,
            "an empty answer that did not wait must not be retried at once"
        );
    }

    const PAN: &str = "4111111111111111";

    #[tokio::test(start_paused = true)]
    async fn an_obfuscated_topic_is_tailed_as_tokens() {
        let session = FakeCluster::local().with_obfuscation(
            "
            secret: 0123456789abcdef0123456789abcdef
            rules:
              - topics: ['orders.*']
                fields:
                  - path: card.number
                    strategy: hash
            ",
        );
        let mut tail = open(&session, query()).await;
        assert!(tail.obfuscated());

        session.produce(card_record(8, PAN));
        let batch = tail.next().await.unwrap();

        let value = batch.records[0].value.as_deref().expect("value");
        assert!(value.contains("\"kx:"), "{value}");
        assert!(!value.contains(PAN), "{value}");
    }

    #[tokio::test(start_paused = true)]
    async fn a_plain_topic_is_not_obfuscated() {
        assert!(!open(&FakeCluster::local(), query()).await.obfuscated());
    }
}
