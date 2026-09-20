use std::ops::Range;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use foldhash::{HashMap, HashSet, HashSetExt};

use async_trait::async_trait;
use bytes::Bytes;
use tokio::time::{Instant, timeout_at};

use crate::environment::SCAN_PACE_BOUND;
use crate::kafka::error::KafkaError;
use crate::kafka::limits::RecordLimits;
use crate::kafka::session::ClusterSession;
use crate::kafka::watermarks::Watermarks;

use super::batch::{RecordBatch, SortKey};
use super::cursor::CursorDirection;
use super::filter::{CompiledFilter, RawField, RecordMeta, Verdict};
use super::obfuscate::{Field, TopicObfuscator};
use super::payload::{DecodedPayload, PayloadCodec, PayloadSlot, framed_schema_id, needs_decode};
use super::plan::{PartitionWindow, advance_cursor, plan_windows, rewind_cursor};
use super::query::{RecordOrder, RecordQuery};
use super::{Compression, Record, RecordHeader, RecordPage};

const MAX_FILTER_PASSES: usize = 64;

/// A record exactly as it came off the wire.
#[derive(Debug, Clone)]
pub struct RawRecord {
    pub partition: i32,
    pub offset: i64,
    pub timestamp: i64,
    pub key: Option<Bytes>,
    pub value: Option<Bytes>,
    pub headers: Vec<(Bytes, Option<Bytes>)>,
    pub compression: Compression,
}

impl RawRecord {
    fn size_bytes(&self) -> u64 {
        let key = self.key.as_ref().map_or(0, Bytes::len);
        let value = self.value.as_ref().map_or(0, Bytes::len);
        (key + value) as u64
    }

    fn sort_key(&self) -> SortKey {
        SortKey {
            timestamp: self.timestamp,
            partition: self.partition,
            offset: self.offset,
        }
    }
}

/// A consumer scoped to a single page request.
///
/// It arrives already assigned to the page's first windows; a filter scan
/// that needs another pass re-points it with [`reassign`](Self::reassign).
#[async_trait]
pub trait ScanConsumer: Send + Sync {
    /// Point the consumer at these windows' start offsets, replacing the
    /// previous assignment and resuming anything paused by an earlier pass.
    async fn reassign(&self, windows: &[PartitionWindow]) -> Result<(), KafkaError>;

    async fn poll(&self, budget: Duration) -> Result<Vec<RawRecord>, KafkaError>;

    /// Stop fetching a partition whose window is finished.
    async fn pause(&self, partitions: &[i32]);

    /// Next offset the consumer would read, if it knows one.
    async fn position(&self, partition: i32) -> Option<i64>;

    /// Records left between the position and the end of the log, if known.
    async fn lag(&self, partition: i32) -> Option<u64>;

    /// Give the consumer up. Whether that closes it or returns it to a pool
    /// is the session's business.
    async fn close(&self);
}

/// What one pass actually managed to read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScanOutcome {
    /// Per planned window, the range the cursor may advance over. A window
    /// the deadline cut short collapses to its pre-pass boundary so the next
    /// page re-reads it rather than skipping it.
    pub covered: Vec<PartitionWindow>,
    /// Whether every planned window was read to its end.
    pub complete: bool,
    /// Whether at least one window was read to its end.
    pub scanned: bool,
}

struct Kept {
    raw: RawRecord,
    key: Option<DecodedPayload>,
    value: Option<DecodedPayload>,
    decoded: bool,
}

impl Kept {
    fn pending(raw: RawRecord) -> Self {
        Self {
            raw,
            key: None,
            value: None,
            decoded: false,
        }
    }

    fn into_record(self, topic: &str) -> Record {
        let schema_id = self.value.as_ref().and_then(DecodedPayload::schema_id);
        let headers = self
            .raw
            .headers
            .iter()
            .map(|(key, value)| RecordHeader {
                key: String::from_utf8_lossy(key).into_owned(),
                value: value
                    .as_deref()
                    .map(|value| String::from_utf8_lossy(value).into_owned())
                    .unwrap_or_default(),
            })
            .collect();

        Record {
            topic: topic.to_owned(),
            partition: self.raw.partition,
            offset: self.raw.offset,
            timestamp: self.raw.timestamp.max(0),
            size_bytes: self.raw.size_bytes(),
            compression: self.raw.compression,
            key: self.key.map(DecodedPayload::into_text),
            value: self.value.map(DecodedPayload::into_text),
            schema_id,
            headers,
        }
    }
}

pub struct ScanSession {
    consumer: Box<dyn ScanConsumer>,
    codec: Option<Arc<dyn PayloadCodec>>,
    topic: String,
    filter: Option<CompiledFilter>,
    /// This topic's obfuscation rules, resolved once per page.
    obfuscator: Option<Arc<TopicObfuscator>>,
    schema_id: Option<i32>,
    walk: RecordOrder,
    assigned: Mutex<Assignment>,
}

struct Assignment(Vec<PartitionWindow>);

impl Assignment {
    fn from_open(windows: &[PartitionWindow]) -> Self {
        Self(windows.to_vec())
    }

    fn needs_reassign(&self, windows: &[PartitionWindow]) -> bool {
        self.0 != windows
    }

    fn retarget(&mut self, windows: &[PartitionWindow]) {
        self.0 = windows.to_vec();
    }
}

impl ScanSession {
    /// Open a consumer already assigned to `windows`.
    pub async fn open<S: ClusterSession + ?Sized>(
        session: &S,
        query: &RecordQuery,
        walk: RecordOrder,
        deadline: Instant,
        windows: &[PartitionWindow],
    ) -> Result<Self, KafkaError> {
        let consumer = timeout_at(deadline, session.open_scan(&query.topic, windows))
            .await
            .map_err(|_| KafkaError::Timeout)??;

        Ok(Self {
            consumer,
            codec: session.payload_codec(),
            obfuscator: topic_obfuscator(session, &query.topic),
            topic: query.topic.clone(),
            filter: query.filter.clone(),
            schema_id: query.schema_id,
            walk,
            assigned: Mutex::new(Assignment::from_open(windows)),
        })
    }

    fn obfuscated(&self) -> bool {
        self.obfuscator.is_some()
    }

    pub async fn close(self) {
        self.consumer.close().await;
    }

    /// Read one plan's windows, keeping matches in `batch`.
    ///
    /// Returns when every window is finished or the deadline passes; a
    /// deadline is not an error here, it is reported through the outcome.
    async fn run(
        &self,
        windows: &[PartitionWindow],
        batch: &mut RecordBatch<Kept>,
        deadline: Instant,
    ) -> Result<ScanOutcome, KafkaError> {
        let mut scan = WindowScan::new(windows);
        if scan.planned == 0 {
            return Ok(scan.outcome(windows, self.walk));
        }

        if self
            .assigned
            .lock()
            .expect("scan assignment")
            .needs_reassign(windows)
        {
            match timeout_at(deadline, self.consumer.reassign(windows)).await {
                Ok(assigned) => assigned?,
                Err(_) => return Ok(scan.outcome(windows, self.walk)),
            }
            self.assigned
                .lock()
                .expect("scan assignment")
                .retarget(windows);
        }

        while !scan.remaining.is_empty() {
            let now = Instant::now();
            if now >= deadline {
                break;
            }

            let budget = deadline
                .saturating_duration_since(now)
                .min(*SCAN_PACE_BOUND);
            let Ok(polled) = timeout_at(deadline, self.consumer.poll(budget)).await else {
                break;
            };

            let polled = polled?;
            if polled.is_empty() {
                self.settle_idle(&mut scan, deadline).await;
                continue;
            }

            self.ingest(polled, &mut scan, batch).await;
        }

        Ok(scan.outcome(windows, self.walk))
    }

    async fn ingest(
        &self,
        polled: Vec<RawRecord>,
        scan: &mut WindowScan,
        batch: &mut RecordBatch<Kept>,
    ) {
        let mut candidates: Vec<Candidate> = Vec::new();
        let mut slots: Vec<PayloadSlot> = Vec::new();

        for mut raw in polled {
            let partition = raw.partition;
            if !scan.remaining.contains_key(&partition) {
                continue;
            }

            let accepted = scan.accept(partition, raw.offset);
            if !scan.remaining.contains_key(&partition) {
                self.consumer.pause(&[partition]).await;
            }
            if !accepted {
                continue;
            }

            let sort = raw.sort_key();
            if !batch.admits(&sort) {
                continue;
            }

            // Before `meta()` exists, so CEL over `headers` and the rendered
            // headers both see the masked value.
            if let Some(obfuscator) = &self.obfuscator {
                obfuscator.mask_headers(&mut raw.headers);
            }

            let Some(verdict) = self.screen(&raw) else {
                continue;
            };
            if verdict != Verdict::NeedsPayload {
                batch.push(sort, Kept::pending(raw));
                continue;
            }

            let key = raw.key.clone().map(|bytes| {
                slots.push(PayloadSlot::new(bytes, None));
                slots.len() - 1
            });
            let value = raw.value.clone().map(|bytes| {
                slots.push(PayloadSlot::new(bytes, self.schema_id));
                slots.len() - 1
            });

            candidates.push(Candidate {
                raw,
                sort,
                key,
                value,
            });
        }

        if candidates.is_empty() {
            return;
        }

        let mut decoded = self.decode(slots).await;

        for candidate in candidates {
            let mut key = candidate.key.and_then(|index| decoded[index].take());
            let mut value = candidate.value.and_then(|index| decoded[index].take());
            self.obfuscate(&mut key, &mut value);

            if let Some(filter) = &self.filter
                && !filter.on_payload(
                    &self.meta(&candidate.raw, value.as_ref()),
                    key.as_ref(),
                    value.as_ref(),
                )
            {
                continue;
            }

            batch.push(
                candidate.sort,
                Kept {
                    raw: candidate.raw,
                    key,
                    value,
                    decoded: true,
                },
            );
        }
    }

    async fn decode_page(&self, page: &mut [Kept]) {
        let mut slots = Vec::new();
        let mut indices = Vec::with_capacity(page.len());
        for record in page.iter() {
            if record.decoded {
                indices.push((None, None));
                continue;
            }
            let key = record.raw.key.clone().map(|bytes| {
                slots.push(PayloadSlot::new(bytes, None));
                slots.len() - 1
            });
            let value = record.raw.value.clone().map(|bytes| {
                slots.push(PayloadSlot::new(bytes, self.schema_id));
                slots.len() - 1
            });
            indices.push((key, value));
        }

        let mut decoded = self.decode(slots).await;
        for (record, (key, value)) in page.iter_mut().zip(indices) {
            if record.decoded {
                continue;
            }
            record.key = key.and_then(|index| decoded[index].take());
            record.value = value.and_then(|index| decoded[index].take());
            self.obfuscate(&mut record.key, &mut record.value);
            record.decoded = true;
        }
    }

    /// Apply this topic's rules to a decoded pair, before anything filters or
    /// renders it. Unconfigured topics pay one branch.
    fn obfuscate(&self, key: &mut Option<DecodedPayload>, value: &mut Option<DecodedPayload>) {
        let Some(obfuscator) = &self.obfuscator else {
            return;
        };

        obfuscator.apply(Field::Key, key);
        obfuscator.apply(Field::Value, value);
    }

    async fn decode(&self, mut slots: Vec<PayloadSlot>) -> Vec<Option<DecodedPayload>> {
        if let Some(codec) = &self.codec
            && !slots.is_empty()
        {
            codec.decode_batch(&mut slots).await;
        }
        slots.into_iter().map(|slot| Some(slot.take())).collect()
    }

    fn screen(&self, raw: &RawRecord) -> Option<Verdict> {
        let Some(filter) = &self.filter else {
            return Some(Verdict::Pass);
        };

        let verdict = match filter.on_meta(&self.meta(raw, None)) {
            Verdict::Fail => return None,
            verdict => verdict,
        };
        if verdict != Verdict::NeedsPayload {
            return Some(verdict);
        }

        // Answering from raw bytes would let a filter match cleartext this
        // topic never shows, which is an oracle for the hidden value. Decode
        // first and filter the obfuscated view instead.
        if self
            .obfuscator
            .as_ref()
            .is_some_and(|obfuscator| obfuscator.hides_payload())
        {
            return Some(Verdict::NeedsPayload);
        }

        match filter.on_raw(
            self.field(raw.key.as_deref(), None),
            self.field(raw.value.as_deref(), self.schema_id),
        ) {
            Verdict::Fail => None,
            verdict => Some(verdict),
        }
    }

    fn field<'a>(&self, bytes: Option<&'a [u8]>, override_id: Option<i32>) -> Option<RawField<'a>> {
        bytes.map(|bytes| RawField {
            bytes,
            framed: self.codec.is_some() && needs_decode(bytes, override_id),
        })
    }

    fn meta<'a>(&'a self, raw: &'a RawRecord, value: Option<&DecodedPayload>) -> RecordMeta<'a> {
        let schema_id = match value {
            Some(value) => value.schema_id(),
            None => raw.value.as_deref().and_then(framed_schema_id),
        };

        RecordMeta {
            topic: &self.topic,
            partition: raw.partition,
            offset: raw.offset,
            timestamp: raw.timestamp.max(0),
            size_bytes: raw.size_bytes(),
            compression: raw.compression,
            schema_id,
            headers: &raw.headers,
        }
    }

    /// An empty poll alone is not EOF: it can also follow a retriable broker
    /// error. A partition is only finished once its position or lag says so.
    async fn settle_idle(&self, scan: &mut WindowScan, deadline: Instant) {
        let mut done = Vec::new();
        for (&partition, window) in &scan.remaining {
            let Ok(Some(position)) = timeout_at(deadline, self.consumer.position(partition)).await
            else {
                continue;
            };
            if position >= window.end
                || matches!(
                    timeout_at(deadline, self.consumer.lag(partition)).await,
                    Ok(Some(0))
                )
            {
                done.push(partition);
            }
        }

        for partition in done {
            scan.finish(partition);
            self.consumer.pause(&[partition]).await;
        }
    }
}

struct Candidate {
    raw: RawRecord,
    sort: SortKey,
    key: Option<usize>,
    value: Option<usize>,
}

fn topic_obfuscator<S: ClusterSession + ?Sized>(
    session: &S,
    topic: &str,
) -> Option<Arc<TopicObfuscator>> {
    session
        .obfuscation()
        .and_then(|policy| policy.for_topic(topic))
}

/// Active half-open offset ranges. Kafka can jump over offsets in compacted logs.
struct WindowScan {
    remaining: HashMap<i32, Range<i64>>,
    completed: HashSet<i32>,
    planned: usize,
}

impl WindowScan {
    fn new(windows: &[PartitionWindow]) -> Self {
        let remaining: HashMap<i32, Range<i64>> = windows
            .iter()
            .filter(|window| !window.is_empty())
            .map(|window| (window.partition, window.start..window.end))
            .collect();

        Self {
            planned: remaining.len(),
            remaining,
            completed: HashSet::new(),
        }
    }

    fn accept(&mut self, partition: i32, offset: i64) -> bool {
        let Some(window) = self.remaining.get(&partition) else {
            return false;
        };

        let accepted = window.contains(&offset);
        if offset >= window.end - 1 {
            self.finish(partition);
        }
        accepted
    }

    fn finish(&mut self, partition: i32) {
        if self.remaining.remove(&partition).is_some() {
            self.completed.insert(partition);
        }
    }

    fn outcome(&self, windows: &[PartitionWindow], walk: RecordOrder) -> ScanOutcome {
        let covered = windows
            .iter()
            .map(|window| {
                if self.completed.contains(&window.partition) {
                    *window
                } else {
                    abandoned(window, walk)
                }
            })
            .collect();

        ScanOutcome {
            covered,
            complete: self.completed.len() == self.planned,
            scanned: !self.completed.is_empty(),
        }
    }
}

fn abandoned(window: &PartitionWindow, walk: RecordOrder) -> PartitionWindow {
    let boundary = match walk {
        RecordOrder::Newest => window.end,
        RecordOrder::Oldest => window.start,
    };

    PartitionWindow {
        partition: window.partition,
        start: boundary,
        end: boundary,
    }
}

/// Fetch one page.
///
/// Running out of time is not fatal: whatever the heap holds is returned
/// with `complete: false` and a cursor that resumes at the last fully
/// scanned window edge.
pub async fn fetch_page<S: ClusterSession + ?Sized>(
    session: &S,
    query: &RecordQuery,
    partitions: &[i32],
    watermarks: &HashMap<i32, Watermarks>,
    limit: usize,
    limits: RecordLimits,
) -> Result<RecordPage, KafkaError> {
    let walk = query.walk();
    let order = query.order;
    let direction = query.direction();
    let searching = query.searching();

    let plan = |cursor: Option<&_>| {
        plan_windows(
            partitions, watermarks, walk, limit, searching, cursor, limits,
        )
    };

    let mut cursor = query.cursor.clone();
    let mut windows = plan(cursor.as_ref());
    if windows.is_empty() || limit == 0 {
        return Ok(RecordPage {
            obfuscated: topic_obfuscator(session, &query.topic).is_some(),
            ..RecordPage::empty()
        });
    }

    let deadline = Instant::now() + session.consume_timeout();
    let scan = ScanSession::open(session, query, walk, deadline, &windows).await?;

    let max_passes = if searching { MAX_FILTER_PASSES } else { 1 };
    let mut kept: Vec<Kept> = Vec::with_capacity(limit);
    let mut complete = true;
    let mut scanned = false;

    for pass in 0..max_passes {
        if pass > 0 {
            windows = plan(cursor.as_ref());
            if windows.is_empty() {
                cursor = None;
                break;
            }
        }

        let remaining = limit - kept.len();
        let mut batch = RecordBatch::new(remaining, walk);
        let outcome = scan.run(&windows, &mut batch, deadline).await?;
        complete &= outcome.complete;
        scanned |= outcome.scanned;

        let found = batch.into_sorted();
        let filled = found.len() >= remaining;
        let next = advance_cursor(
            walk,
            &outcome.covered,
            watermarks,
            &edges(&found),
            remaining,
            order,
            direction,
        );
        kept.extend(found);

        let stalled = next == cursor;
        cursor = next;
        if filled || cursor.is_none() || stalled || Instant::now() >= deadline {
            break;
        }
    }

    if !scanned && kept.is_empty() {
        scan.close().await;
        return Err(KafkaError::Timeout);
    }

    kept.sort_by(|left, right| {
        left.raw
            .sort_key()
            .cmp_for_order(&right.raw.sort_key(), order)
    });
    scan.decode_page(&mut kept).await;
    let obfuscated = scan.obfuscated();
    scan.close().await;

    let near = rewind_cursor(walk, watermarks, &edges(&kept), order, direction.flipped());
    let (next_cursor, prev_cursor) = match direction {
        CursorDirection::Forward => (cursor, query.cursor.as_ref().and(near)),
        CursorDirection::Backward => (near, cursor),
    };

    Ok(RecordPage {
        records: kept
            .into_iter()
            .map(|record| record.into_record(&query.topic))
            .collect(),
        complete,
        obfuscated,
        next_cursor: next_cursor.map(|cursor| cursor.encode()),
        prev_cursor: prev_cursor.map(|cursor| cursor.encode()),
    })
}

fn edges(kept: &[Kept]) -> Vec<(i32, i64)> {
    kept.iter()
        .map(|record| (record.raw.partition, record.raw.offset))
        .collect()
}

#[cfg(test)]
pub async fn scan_once<S: ClusterSession + ?Sized>(
    session: &S,
    topic: &str,
    windows: &[PartitionWindow],
    limit: usize,
    order: RecordOrder,
) -> Result<Vec<Record>, KafkaError> {
    use super::query::TimestampRange;

    let query = RecordQuery {
        topic: topic.to_owned(),
        partition: None,
        filter: None,
        timestamps: TimestampRange::UNBOUNDED,
        limit: limit as i32,
        order,
        cursor: None,
        schema_id: None,
    };

    let deadline = Instant::now() + session.consume_timeout();
    let scan = ScanSession::open(session, &query, order, deadline, windows).await?;
    let mut batch = RecordBatch::new(limit, order);
    let outcome = scan.run(windows, &mut batch, deadline).await;
    let mut page = batch.into_sorted();
    scan.decode_page(&mut page).await;
    scan.close().await;

    if !outcome?.complete {
        return Err(KafkaError::Timeout);
    }
    Ok(page
        .into_iter()
        .map(|record| record.into_record(topic))
        .collect())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use crate::kafka::scan::cursor::RecordCursor;
    use crate::kafka::scan::filter::{cel, contains};
    use crate::kafka::scan::query::TimestampRange;
    use crate::kafka::testing::{FakeCluster, card_record};

    const LIMITS: RecordLimits = RecordLimits {
        max_limit: 500,
        min_window: 1,
        window_multiplier: 2,
        search_window_multiplier: 2,
    };

    fn stored(partition: i32, offset: i64, key: &str) -> Record {
        Record {
            topic: "orders.created".into(),
            partition,
            offset,
            timestamp: offset,
            key: Some(key.to_owned()),
            value: None,
            headers: Vec::new(),
            schema_id: None,
            size_bytes: key.len() as u64,
            compression: Compression::None,
        }
    }

    fn delayed(offsets: &[i64], delay: Duration) -> FakeCluster {
        FakeCluster::local()
            .with_orders_records(
                offsets
                    .iter()
                    .map(|&offset| stored(0, offset, "hit"))
                    .collect(),
            )
            .with_records_delay(delay)
            .with_consume_timeout(Duration::from_secs(10))
    }

    fn query() -> RecordQuery {
        RecordQuery {
            topic: "orders.created".into(),
            partition: Some(0),
            filter: contains("hit"),
            timestamps: TimestampRange::UNBOUNDED,
            limit: 2,
            order: RecordOrder::Oldest,
            cursor: None,
            schema_id: None,
        }
    }

    fn marks(low: i64, high: i64) -> HashMap<i32, Watermarks> {
        HashMap::from_iter([(0, Watermarks { low, high })])
    }

    fn offsets(page: &RecordPage) -> Vec<i64> {
        page.records.iter().map(|record| record.offset).collect()
    }

    #[test]
    fn a_scan_tracks_half_open_windows_across_interleaved_partitions() {
        let mut scan = WindowScan::new(&[
            PartitionWindow {
                partition: 0,
                start: 10,
                end: 12,
            },
            PartitionWindow {
                partition: 1,
                start: 20,
                end: 23,
            },
        ]);

        assert!(!scan.accept(0, 9));
        assert!(scan.accept(0, 10));
        assert!(scan.accept(1, 20));
        assert!(scan.accept(0, 11));
        assert!(!scan.remaining.contains_key(&0));
        assert!(!scan.accept(0, 12));
        assert!(scan.accept(1, 22));
        assert!(scan.remaining.is_empty());
        assert_eq!(scan.completed.len(), 2);
    }

    #[test]
    fn a_compacted_gap_completes_a_window_without_accepting_an_outside_record() {
        let window = PartitionWindow {
            partition: 0,
            start: 10,
            end: 20,
        };
        let mut scan = WindowScan::new(&[window]);

        assert!(!scan.accept(1, 10));
        assert!(scan.accept(0, 12));
        assert!(!scan.accept(0, 25));
        assert!(scan.remaining.is_empty());
        assert!(scan.outcome(&[window], RecordOrder::Oldest).complete);
    }

    #[test]
    fn an_abandoned_window_does_not_advance_its_cursor() {
        let window = PartitionWindow {
            partition: 0,
            start: 10,
            end: 20,
        };
        let scan = WindowScan::new(&[window]);

        let newest = scan.outcome(&[window], RecordOrder::Newest);
        assert!(!newest.complete);
        assert!(!newest.scanned);
        assert_eq!(newest.covered[0].start, 20);
        assert_eq!(newest.covered[0].end, 20);

        let oldest = scan.outcome(&[window], RecordOrder::Oldest);
        assert_eq!(oldest.covered[0].start, 10);
        assert_eq!(oldest.covered[0].end, 10);
    }

    #[tokio::test(start_paused = true)]
    async fn one_consumer_serves_every_pass_of_a_page() {
        let session = delayed(&[0, 6, 12, 13], Duration::from_secs(1));
        let mut query = query();
        query.limit = 3;

        let page = fetch_page(&session, &query, &[0], &marks(0, 24), 3, LIMITS)
            .await
            .unwrap();

        assert_eq!(offsets(&page), vec![0, 6, 12]);
        assert_eq!(session.consumers_opened(), 1);
        assert_eq!(
            session.assigned_windows(),
            vec![vec![(0, 0, 6)], vec![(0, 6, 12)], vec![(0, 12, 18)]]
        );
    }

    #[tokio::test(start_paused = true)]
    async fn remaining_limit_shrinks_without_narrowing_scan_windows() {
        let session = delayed(&[0, 6, 12, 13], Duration::from_secs(1));
        let mut query = query();
        query.limit = 3;
        let started = Instant::now();

        let page = fetch_page(&session, &query, &[0], &marks(0, 24), 3, LIMITS)
            .await
            .unwrap();

        assert_eq!(started.elapsed(), Duration::from_secs(3));
        assert_eq!(offsets(&page), vec![0, 6, 12]);
        assert!(page.complete);
        assert!(page.has_more());
        assert_eq!(
            RecordCursor::parse(page.next_cursor.as_deref().unwrap())
                .unwrap()
                .offsets,
            BTreeMap::from([(0, 13)])
        );
    }

    #[tokio::test(start_paused = true)]
    async fn empty_scan_results_advance_until_exhaustion() {
        let session = delayed(&[], Duration::from_secs(1));

        let page = fetch_page(&session, &query(), &[0], &marks(0, 8), 2, LIMITS)
            .await
            .unwrap();

        assert!(page.records.is_empty());
        assert!(page.complete);
        assert!(!page.has_more());
        assert_eq!(
            session.assigned_windows(),
            vec![vec![(0, 0, 4)], vec![(0, 4, 8)]]
        );
    }

    #[tokio::test(start_paused = true)]
    async fn an_exhausted_cursor_never_opens_a_consumer() {
        let session = delayed(&[], Duration::from_secs(11));
        let mut query = query();
        query.cursor = Some(RecordCursor::new(
            RecordOrder::Oldest,
            CursorDirection::Forward,
            BTreeMap::from([(0, 8)]),
        ));
        let started = Instant::now();

        let page = fetch_page(&session, &query, &[0], &marks(0, 8), 2, LIMITS)
            .await
            .unwrap();

        assert!(page.records.is_empty());
        assert!(!page.has_more());
        assert_eq!(session.consumers_opened(), 0);
        assert_eq!(started.elapsed(), Duration::ZERO);
    }

    #[tokio::test(start_paused = true)]
    async fn the_deadline_returns_what_the_scan_already_found() {
        let session = delayed(&[0, 4], Duration::from_secs(6));
        let started = Instant::now();

        let page = fetch_page(&session, &query(), &[0], &marks(0, 12), 2, LIMITS)
            .await
            .unwrap();

        assert_eq!(started.elapsed(), Duration::from_secs(10));
        assert_eq!(offsets(&page), vec![0]);
        assert!(!page.complete, "the second pass never finished its window");
        assert!(page.has_more());
        assert_eq!(
            RecordCursor::parse(page.next_cursor.as_deref().unwrap())
                .unwrap()
                .offsets,
            BTreeMap::from([(0, 4)]),
            "the abandoned window is re-read, not skipped"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_deadline_before_any_window_completes_is_a_timeout() {
        let session = delayed(&[0], Duration::from_secs(11));
        let mut query = query();
        query.filter = None;

        let error = fetch_page(&session, &query, &[0], &marks(0, 12), 2, LIMITS)
            .await
            .unwrap_err();

        assert!(matches!(error, KafkaError::Timeout));
        assert_eq!(error.code(), "TIMEOUT");
        assert_eq!(session.consumers_opened(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn the_filter_pass_cap_returns_a_resumable_cursor() {
        let session = delayed(&[256], Duration::from_millis(1));
        let mut query = query();

        let page = fetch_page(&session, &query, &[0], &marks(0, 260), 2, LIMITS)
            .await
            .unwrap();

        assert!(page.records.is_empty());
        assert!(page.has_more());
        assert_eq!(session.assigned_windows().len(), MAX_FILTER_PASSES);
        let cursor = RecordCursor::parse(page.next_cursor.as_deref().unwrap()).unwrap();
        assert_eq!(cursor.offsets, BTreeMap::from([(0, 256)]));

        query.cursor = Some(cursor);
        let next = fetch_page(&session, &query, &[0], &marks(0, 260), 2, LIMITS)
            .await
            .unwrap();

        assert_eq!(offsets(&next), vec![256]);
        assert!(!next.has_more());
    }

    #[tokio::test(start_paused = true)]
    async fn a_page_reached_by_cursor_carries_an_edge_back() {
        let session = delayed(&[0, 1, 2, 3], Duration::from_millis(1));
        let mut query = query();
        query.filter = None;
        query.cursor = Some(RecordCursor::new(
            RecordOrder::Oldest,
            CursorDirection::Forward,
            BTreeMap::from([(0, 2)]),
        ));

        let page = fetch_page(&session, &query, &[0], &marks(0, 8), 2, LIMITS)
            .await
            .unwrap();

        assert_eq!(offsets(&page), vec![2, 3]);
        let previous = RecordCursor::parse(page.prev_cursor.as_deref().unwrap()).unwrap();
        assert_eq!(previous.direction, CursorDirection::Backward);
        assert_eq!(previous.offsets, BTreeMap::from([(0, 2)]));
    }

    #[tokio::test(start_paused = true)]
    async fn the_first_page_has_no_previous_edge() {
        let session = delayed(&[0, 1, 2, 3], Duration::from_millis(1));
        let mut query = query();
        query.filter = None;

        let page = fetch_page(&session, &query, &[0], &marks(0, 8), 2, LIMITS)
            .await
            .unwrap();

        assert!(page.prev_cursor.is_none());
        assert!(page.has_more());
    }

    #[tokio::test(start_paused = true)]
    async fn a_backward_page_walks_the_other_way_and_returns_query_order() {
        let session = delayed(&[0, 1, 2, 3, 4, 5], Duration::from_millis(1));
        let mut query = query();
        query.filter = None;
        query.order = RecordOrder::Newest;
        query.cursor = Some(RecordCursor::new(
            RecordOrder::Newest,
            CursorDirection::Backward,
            BTreeMap::from([(0, 2)]),
        ));

        let page = fetch_page(&session, &query, &[0], &marks(0, 6), 2, LIMITS)
            .await
            .unwrap();

        assert_eq!(
            session.assigned_windows(),
            vec![vec![(0, 2, 6)]],
            "a backward newest page reads forward from its near edge"
        );
        assert_eq!(offsets(&page), vec![3, 2], "still newest-first");
        let next = RecordCursor::parse(page.next_cursor.as_deref().unwrap()).unwrap();
        assert_eq!(next.direction, CursorDirection::Forward);
        assert_eq!(next.offsets, BTreeMap::from([(0, 2)]));
    }

    #[tokio::test(start_paused = true)]
    async fn a_filter_skips_decoding_records_that_cannot_reach_the_page() {
        let records: Vec<Record> = (0..8).map(|offset| stored(0, offset, "hit")).collect();
        let session = FakeCluster::local()
            .with_orders_records(records)
            .with_consume_timeout(Duration::from_secs(10));
        let mut query = query();
        query.limit = 2;

        let page = fetch_page(&session, &query, &[0], &marks(0, 8), 2, LIMITS)
            .await
            .unwrap();

        assert_eq!(offsets(&page), vec![0, 1]);
        assert_eq!(
            session.decoded_payloads(),
            2,
            "the heap fills after two records and rejects the rest before decoding"
        );
    }

    const PAN: &str = "4111111111111111";

    const RULES: &str = "
        secret: 0123456789abcdef0123456789abcdef
        rules:
          - topics: ['orders.*']
            headers: ['x-user-id']
            fields:
              - path: card.number
                strategy: hash
    ";

    fn cards() -> FakeCluster {
        let records = (0..4).map(|offset| card_record(offset, PAN)).collect();

        FakeCluster::local()
            .with_orders_records(records)
            .with_obfuscation(RULES)
            .with_consume_timeout(Duration::from_secs(10))
    }

    async fn card_page(session: &FakeCluster, filter: Option<CompiledFilter>) -> RecordPage {
        let mut query = query();
        query.filter = filter;
        query.limit = 4;

        fetch_page(session, &query, &[0], &marks(0, 4), 4, LIMITS)
            .await
            .unwrap()
    }

    #[tokio::test(start_paused = true)]
    async fn an_obfuscated_field_leaves_as_a_token_and_never_as_cleartext() {
        let page = card_page(&cards(), None).await;

        let value = page.records[0].value.as_deref().expect("value");
        assert!(value.contains("\"kx:"), "{value}");
        assert!(!value.contains(PAN), "{value}");
        assert!(
            value.contains("ord_0"),
            "fields no rule names still render: {value}"
        );
        assert_eq!(page.records[0].key.as_deref(), Some("ord_0"));
    }

    #[tokio::test(start_paused = true)]
    async fn a_contains_filter_cannot_be_an_oracle_for_an_obfuscated_field() {
        let session = cards();

        assert!(
            card_page(&session, contains(PAN)).await.records.is_empty(),
            "the cleartext the page never shows must not be searchable"
        );
        assert!(
            card_page(&session, contains("4111"))
                .await
                .records
                .is_empty(),
            "nor may a prefix of it be, one digit at a time"
        );
        assert_eq!(
            card_page(&session, contains("ord_2")).await.records.len(),
            1,
            "everything else still filters"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_filter_matches_the_token_the_page_shows() {
        let page = card_page(&cards(), contains("kx:")).await;

        assert_eq!(offsets(&page), vec![0, 1, 2, 3]);
    }

    #[tokio::test(start_paused = true)]
    async fn configured_headers_are_masked_before_filters_and_before_rendering() {
        let session = cards();

        let page = card_page(&session, None).await;
        assert_eq!(page.records[0].headers[0].value, "***");

        let matched = card_page(&session, cel(r#"headers["x-user-id"] == "ada""#).unwrap()).await;
        assert!(
            matched.records.is_empty(),
            "a header expression sees the masked value too"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_value_the_codec_declined_is_masked_rather_than_served_raw() {
        let mut unframed = card_record(0, PAN);
        unframed.value = Some(format!(r#"{{"card":{{"number":"{PAN}"}}}}"#));

        let session = FakeCluster::local()
            .with_orders_records(vec![unframed])
            .with_obfuscation(RULES)
            .with_consume_timeout(Duration::from_secs(10));

        let page = card_page(&session, None).await;

        assert_eq!(
            page.records[0].value.as_deref(),
            Some("***"),
            "an unframed value cannot be walked, so the whole value fails closed"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn metadata_filters_still_answer_before_anything_is_decoded() {
        let session = cards();

        let page = card_page(&session, cel("offset >= 2").unwrap()).await;

        assert_eq!(offsets(&page), vec![2, 3]);
        assert_eq!(
            session.decoded_payloads(),
            4,
            "only the two records that reach the page decode, key and value each"
        );
    }

    const PATTERNS: &str = r"
        secret: 0123456789abcdef0123456789abcdef
        rules:
          - topics: ['orders.*']
            patterns:
              - regex: '\d{13,19}'
                strategy: hash
    ";

    fn text_orders() -> FakeCluster {
        let records = (0..4)
            .map(|offset| {
                let mut record = card_record(offset, PAN);
                record.value = Some(format!("charged {PAN} on order {offset}"));
                record
            })
            .collect();

        FakeCluster::local()
            .with_orders_records(records)
            .with_obfuscation(PATTERNS)
            .with_consume_timeout(Duration::from_secs(10))
    }

    #[tokio::test(start_paused = true)]
    async fn a_pattern_rule_tokens_text_no_field_rule_could_have_reached() {
        let page = card_page(&text_orders(), None).await;

        let value = page.records[0].value.as_deref().expect("value");
        assert!(value.starts_with("charged kx:"), "{value}");
        assert!(!value.contains(PAN), "{value}");
        assert!(value.ends_with("on order 0"), "{value}");
    }

    #[tokio::test(start_paused = true)]
    async fn a_contains_filter_cannot_be_an_oracle_for_a_pattern_rule_either() {
        let session = text_orders();

        assert!(
            card_page(&session, contains("4111"))
                .await
                .records
                .is_empty(),
            "raw bytes must stay out of reach of a filter on a rewritten topic"
        );
        assert_eq!(
            card_page(&session, contains("on order 2"))
                .await
                .records
                .len(),
            1,
            "text no pattern matches still filters"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_page_says_whether_its_topic_is_obfuscated() {
        assert!(card_page(&cards(), None).await.obfuscated);
        assert!(card_page(&text_orders(), None).await.obfuscated);

        let plain = FakeCluster::local().with_consume_timeout(Duration::from_secs(10));
        assert!(!card_page(&plain, None).await.obfuscated);
    }

    #[tokio::test(start_paused = true)]
    async fn an_empty_page_still_says_its_topic_is_obfuscated() {
        let session = cards();
        let query = query();

        let page = fetch_page(&session, &query, &[0], &marks(0, 0), 4, LIMITS)
            .await
            .unwrap();

        assert!(page.records.is_empty());
        assert!(
            page.obfuscated,
            "a page with nothing on it still describes its topic"
        );
    }
}
