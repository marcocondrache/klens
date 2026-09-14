use std::collections::HashMap;

use tokio::time::{Instant, timeout_at};

use crate::kafka::error::KafkaError;
use crate::kafka::limits::RecordLimits;
use crate::kafka::record::RecordPage;
use crate::kafka::record::plan::{FetchPlan, next_cursor};
use crate::kafka::record::query::RecordQuery;
use crate::kafka::session::ClusterSession;
use crate::kafka::watermarks::Watermarks;

const MAX_FILTER_PASSES: usize = 64;

pub async fn fetch_page<S: ClusterSession + ?Sized>(
    session: &S,
    query: &RecordQuery,
    partitions: &[i32],
    watermarks: &HashMap<i32, Watermarks>,
    limit: usize,
    limits: RecordLimits,
) -> Result<RecordPage, KafkaError> {
    let deadline = Instant::now() + session.consume_timeout();
    // One consumer serves every retry pass below; opening it once keeps a
    // filtered search from tearing a consumer down and recreating it per pass.
    let browse = session.open_browse().await?;
    let mut records = Vec::with_capacity(limit);
    let mut pass = query.clone();
    let max_passes = if query.filter.is_some() {
        MAX_FILTER_PASSES
    } else {
        1
    };

    let outcome: Result<(), KafkaError> = async {
        for _ in 0..max_passes {
            let remaining = limit - records.len();
            let mut plan = FetchPlan::build(&pass, partitions, watermarks, limit, limits);
            if plan.windows.is_empty() {
                pass.cursor = None;
                break;
            }
            // Keep the scan window wide for sparse filters, but retain only the
            // records still needed by this page.
            plan.limit = remaining;
            let batch = timeout_at(deadline, browse.fetch(&plan))
                .await
                .map_err(|_| KafkaError::Timeout)??;
            if Instant::now() > deadline {
                return Err(KafkaError::Timeout);
            }
            let filled = batch.len() >= remaining;
            let next = next_cursor(plan.order, &plan.windows, watermarks, &batch, remaining);
            records.extend(batch);

            let stalled = next == pass.cursor;
            pass.cursor = next;
            if filled || pass.cursor.is_none() || stalled || Instant::now() >= deadline {
                break;
            }
        }
        Ok(())
    }
    .await;

    browse.close().await;
    outcome?;

    records.sort_by(|left, right| left.cmp_for_order(right, query.order));
    Ok(RecordPage {
        has_more: pass.cursor.is_some(),
        next_cursor: pass.cursor.map(|cursor| cursor.encode()),
        records,
    })
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Mutex;
    use std::time::Duration;

    use async_trait::async_trait;

    use super::*;
    use crate::kafka::model::{
        ClusterIdentity, CommittedOffset, ConfigEntry, GroupSnapshot, MetadataSnapshot,
    };
    use crate::kafka::record::cursor::RecordCursor;
    use crate::kafka::record::query::{RecordOrder, TimestampRange};
    use crate::kafka::record::{Compression, Record};
    use crate::kafka::testing::FakeCluster;

    const LIMITS: RecordLimits = RecordLimits {
        max_limit: 500,
        min_window: 1,
        window_multiplier: 2,
        search_window_multiplier: 2,
    };

    struct DelayedSession {
        inner: FakeCluster,
        delay: Duration,
        plans: Mutex<Vec<FetchPlan>>,
    }

    impl DelayedSession {
        fn new(offsets: &[i64], delay: Duration) -> Self {
            let records = offsets
                .iter()
                .map(|&offset| Record {
                    topic: "orders.created".into(),
                    partition: 0,
                    offset,
                    timestamp: offset,
                    key: Some("hit".into()),
                    value: None,
                    schema_id: None,
                    headers: Vec::new(),
                    size_bytes: 0,
                    compression: Compression::None,
                })
                .collect();
            Self {
                inner: FakeCluster::local().with_orders_records(records),
                delay,
                plans: Mutex::new(Vec::new()),
            }
        }
    }

    #[async_trait]
    impl ClusterSession for DelayedSession {
        fn identity(&self) -> &ClusterIdentity {
            self.inner.identity()
        }

        async fn metadata(&self) -> Result<MetadataSnapshot, KafkaError> {
            unimplemented!("page fetching must not request metadata")
        }

        async fn offsets_for_times(
            &self,
            _topic: &str,
            _partitions: &[i32],
            _timestamp: i64,
        ) -> Result<HashMap<i32, Option<i64>>, KafkaError> {
            unimplemented!()
        }

        async fn watermarks(
            &self,
            _partitions: &[(String, i32)],
        ) -> Result<HashMap<String, HashMap<i32, Watermarks>>, KafkaError> {
            unimplemented!("page fetching must not request watermarks")
        }

        async fn topic_configs(
            &self,
            _topics: &[&str],
        ) -> Result<HashMap<String, Vec<ConfigEntry>>, KafkaError> {
            unimplemented!()
        }

        async fn broker_configs(&self, _broker_id: i32) -> Result<Vec<ConfigEntry>, KafkaError> {
            unimplemented!()
        }

        async fn groups(&self) -> Result<Vec<GroupSnapshot>, KafkaError> {
            unimplemented!()
        }

        async fn committed_offsets(
            &self,
            _group_id: &str,
            _partitions: &[(String, i32)],
        ) -> Result<Vec<CommittedOffset>, KafkaError> {
            unimplemented!()
        }

        async fn records(&self, plan: &FetchPlan) -> Result<Vec<Record>, KafkaError> {
            self.plans.lock().unwrap().push(plan.clone());
            tokio::time::sleep(self.delay).await;
            self.inner.records(plan).await
        }

        fn consume_timeout(&self) -> Duration {
            Duration::from_secs(10)
        }
    }

    fn query() -> RecordQuery {
        RecordQuery {
            topic: "orders.created".into(),
            partition: Some(0),
            filter: crate::kafka::compile_record_filter(r#"key == "hit""#).unwrap(),
            timestamps: TimestampRange::UNBOUNDED,
            limit: 2,
            order: RecordOrder::Oldest,
            cursor: None,
            schema_id: None,
        }
    }

    #[tokio::test(start_paused = true)]
    async fn filtered_passes_share_one_deadline_and_discard_partial_page_on_timeout() {
        let session = DelayedSession::new(&[0, 4], Duration::from_secs(6));
        let marks = HashMap::from([(0, Watermarks { low: 0, high: 12 })]);
        let started = Instant::now();

        let error = fetch_page(&session, &query(), &[0], &marks, 2, LIMITS)
            .await
            .unwrap_err();

        assert!(matches!(error, KafkaError::Timeout));
        assert_eq!(error.code(), "TIMEOUT");
        assert_eq!(started.elapsed(), Duration::from_secs(10));
        let plans = session.plans.lock().unwrap();
        assert_eq!(plans.len(), 2);
        assert_eq!(plans[0].limit, 2);
        // The first pass already returned a match, but the second scan is incomplete.
        assert_eq!(plans[1].limit, 1);
        assert_eq!(plans[1].windows[0].start, 4);
    }

    #[tokio::test(start_paused = true)]
    async fn plain_fetch_is_also_bounded_by_the_consume_deadline() {
        let session = DelayedSession::new(&[0], Duration::from_secs(11));
        let marks = HashMap::from([(0, Watermarks { low: 0, high: 12 })]);
        let mut query = query();
        query.filter = None;
        let started = Instant::now();

        let error = fetch_page(&session, &query, &[0], &marks, 2, LIMITS)
            .await
            .unwrap_err();

        assert!(matches!(error, KafkaError::Timeout));
        assert_eq!(started.elapsed(), Duration::from_secs(10));
        assert_eq!(session.plans.lock().unwrap().len(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn remaining_limit_shrinks_without_narrowing_scan_windows() {
        let session = DelayedSession::new(&[0, 6, 12, 13], Duration::from_secs(1));
        let marks = HashMap::from([(0, Watermarks { low: 0, high: 24 })]);
        let mut query = query();
        query.limit = 3;
        let started = Instant::now();

        let page = fetch_page(&session, &query, &[0], &marks, 3, LIMITS)
            .await
            .unwrap();

        assert_eq!(started.elapsed(), Duration::from_secs(3));
        assert_eq!(
            page.records
                .iter()
                .map(|record| record.offset)
                .collect::<Vec<_>>(),
            vec![0, 6, 12]
        );
        assert!(page.has_more);
        assert_eq!(
            RecordCursor::parse(page.next_cursor.as_deref().unwrap())
                .unwrap()
                .offsets,
            BTreeMap::from([(0, 13)])
        );
        let plans = session.plans.lock().unwrap();
        assert_eq!(
            plans
                .iter()
                .map(|plan| (plan.limit, plan.windows[0].start, plan.windows[0].end))
                .collect::<Vec<_>>(),
            vec![(3, 0, 6), (2, 6, 12), (1, 12, 18)]
        );
    }

    #[tokio::test(start_paused = true)]
    async fn empty_scan_results_advance_until_exhaustion() {
        let session = DelayedSession::new(&[], Duration::from_secs(1));
        let marks = HashMap::from([(0, Watermarks { low: 0, high: 8 })]);

        let page = fetch_page(&session, &query(), &[0], &marks, 2, LIMITS)
            .await
            .unwrap();

        assert!(page.records.is_empty());
        assert!(!page.has_more);
        assert!(page.next_cursor.is_none());
        let plans = session.plans.lock().unwrap();
        assert_eq!(plans.len(), 2);
        assert_eq!(plans[0].windows[0].start, 0);
        assert_eq!(plans[1].windows[0].start, 4);
        assert_eq!(plans[1].windows[0].end, 8);
    }

    #[tokio::test(start_paused = true)]
    async fn exhausted_cursor_clears_continuation_without_fetching_empty_windows() {
        for filtered in [false, true] {
            let session = DelayedSession::new(&[], Duration::from_secs(11));
            let marks = HashMap::from([(0, Watermarks { low: 0, high: 8 })]);
            let mut query = query();
            if !filtered {
                query.filter = None;
            }
            query.cursor = Some(RecordCursor {
                offsets: BTreeMap::from([(0, 8)]),
            });
            let started = Instant::now();

            let page = fetch_page(&session, &query, &[0], &marks, 2, LIMITS)
                .await
                .unwrap();

            assert!(page.records.is_empty());
            assert!(!page.has_more);
            assert!(page.next_cursor.is_none());
            assert!(session.plans.lock().unwrap().is_empty());
            assert_eq!(started.elapsed(), Duration::ZERO);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn filter_pass_cap_returns_a_resumable_cursor() {
        let session = DelayedSession::new(&[256], Duration::from_millis(1));
        let marks = HashMap::from([(0, Watermarks { low: 0, high: 260 })]);
        let mut query = query();

        let page = fetch_page(&session, &query, &[0], &marks, 2, LIMITS)
            .await
            .unwrap();

        assert!(page.records.is_empty());
        assert!(page.has_more);
        assert_eq!(session.plans.lock().unwrap().len(), 64);
        let cursor = RecordCursor::parse(page.next_cursor.as_deref().unwrap()).unwrap();
        assert_eq!(cursor.offsets, BTreeMap::from([(0, 256)]));
        query.cursor = Some(cursor);

        let next = fetch_page(&session, &query, &[0], &marks, 2, LIMITS)
            .await
            .unwrap();

        assert_eq!(next.records.len(), 1);
        assert_eq!(next.records[0].offset, 256);
        assert!(!next.has_more);
        assert!(next.next_cursor.is_none());
        let plans = session.plans.lock().unwrap();
        assert_eq!(plans.len(), 65);
        assert_eq!(plans[64].windows[0].start, 256);
    }
}
