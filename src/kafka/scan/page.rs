use std::collections::HashMap;

use tokio::time::{Instant, timeout_at};

use crate::kafka::error::KafkaError;
use crate::kafka::limits::RecordLimits;
use crate::kafka::record::Record;
use crate::kafka::record::RecordPage;
use crate::kafka::record::batch::RecordBatch;
use crate::kafka::record::plan::{FetchPlan, next_cursor};
use crate::kafka::record::query::RecordQuery;
use crate::kafka::scan::filter::CompiledFilter;
use crate::kafka::scan::session::ScanSession;
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
    let mut pass = query.clone();
    let max_passes = if query.filter.is_some() {
        MAX_FILTER_PASSES
    } else {
        1
    };
    let filter = query.filter.as_ref().map(CompiledFilter::compile);
    let mut batch = RecordBatch::new(limit, query.order);
    let mut next = None;
    let mut opened: Option<ScanSession> = None;
    let mut any_completed = false;
    let mut timed_out = false;

    for _ in 0..max_passes {
        let remaining = limit.saturating_sub(batch.len());
        if remaining == 0 {
            break;
        }
        let mut plan = FetchPlan::build(&pass, partitions, watermarks, limit, limits);
        if plan.windows.is_empty() {
            next = None;
            break;
        }
        plan.limit = remaining;

        if Instant::now() >= deadline {
            timed_out = true;
            break;
        }

        if opened.is_none() {
            opened = Some(
                timeout_at(deadline, session.open_scan(&query.topic, deadline))
                    .await
                    .map_err(|_| KafkaError::Timeout)??,
            );
        }
        let scan = opened.as_mut().expect("scan session");
        let mut kept = Vec::new();
        match timeout_at(
            deadline,
            scan.scan(&plan, filter.as_ref(), &mut batch, &mut kept),
        )
        .await
        {
            Ok(outcome) => {
                let outcome = outcome?;
                timed_out = Instant::now() >= deadline;
                if outcome.completed {
                    any_completed = true;
                    next = next_cursor(
                        plan.order,
                        &plan.windows,
                        watermarks,
                        &pass_records(&kept, remaining, query.order),
                        remaining,
                    );
                    let stalled = next == pass.cursor;
                    pass.cursor = next.clone();
                    if batch.len() >= limit || pass.cursor.is_none() || stalled {
                        break;
                    }
                }
                if timed_out {
                    break;
                }
            }
            Err(_) => {
                timed_out = true;
                break;
            }
        }
    }

    if timed_out && !any_completed {
        return Err(KafkaError::Timeout);
    }

    Ok(RecordPage {
        has_more: next.is_some(),
        next_cursor: next.map(|cursor| cursor.encode()),
        prev_cursor: prev_cursor(query),
        complete: !timed_out,
        records: batch.into_records(),
    })
}

fn pass_records(
    kept: &[Record],
    remaining: usize,
    order: crate::kafka::record::query::RecordOrder,
) -> Vec<Record> {
    let mut pass = RecordBatch::new(remaining, order);
    for record in kept {
        pass.push(record.clone());
    }
    pass.into_records()
}

fn prev_cursor(query: &RecordQuery) -> Option<String> {
    query.cursor.as_ref().map(|cursor| cursor.encode())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::time::Duration;

    use super::*;
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

    fn delayed(offsets: &[i64], delay: Duration) -> FakeCluster {
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
        FakeCluster::local()
            .with_orders_records(records)
            .with_records_delay(delay)
            .with_consume_timeout(Duration::from_secs(10))
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
    async fn filtered_passes_share_one_deadline_and_keep_partial_page_on_timeout() {
        let session = delayed(&[0, 4], Duration::from_secs(6));
        let marks = HashMap::from([(0, Watermarks { low: 0, high: 12 })]);
        let started = Instant::now();

        let page = fetch_page(&session, &query(), &[0], &marks, 2, LIMITS)
            .await
            .unwrap();

        assert_eq!(started.elapsed(), Duration::from_secs(10));
        assert_eq!(
            page.records
                .iter()
                .map(|record| record.offset)
                .collect::<Vec<_>>(),
            vec![0]
        );
        assert!(!page.complete);
        assert!(page.has_more);
        assert_eq!(session.scan_opens(), 1);
        let plans = session.recorded_plans();
        assert_eq!(plans.len(), 2);
        assert_eq!(plans[0].limit, 2);
        assert_eq!(plans[1].limit, 1);
        assert_eq!(plans[1].windows[0].start, 4);
    }

    #[tokio::test(start_paused = true)]
    async fn plain_fetch_is_also_bounded_by_the_consume_deadline() {
        let session = delayed(&[0], Duration::from_secs(11));
        let marks = HashMap::from([(0, Watermarks { low: 0, high: 12 })]);
        let mut query = query();
        query.filter = None;
        let started = Instant::now();

        let error = fetch_page(&session, &query, &[0], &marks, 2, LIMITS)
            .await
            .unwrap_err();

        assert!(matches!(error, KafkaError::Timeout));
        assert_eq!(started.elapsed(), Duration::from_secs(10));
        assert_eq!(session.recorded_plans().len(), 1);
        assert_eq!(session.scan_opens(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn remaining_limit_shrinks_without_narrowing_scan_windows() {
        let session = delayed(&[0, 6, 12, 13], Duration::from_secs(1));
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
        assert!(page.complete);
        assert_eq!(session.scan_opens(), 1);
        assert_eq!(
            RecordCursor::parse(page.next_cursor.as_deref().unwrap())
                .unwrap()
                .offsets,
            BTreeMap::from([(0, 13)])
        );
        let plans = session.recorded_plans();
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
        let session = delayed(&[], Duration::from_secs(1));
        let marks = HashMap::from([(0, Watermarks { low: 0, high: 8 })]);

        let page = fetch_page(&session, &query(), &[0], &marks, 2, LIMITS)
            .await
            .unwrap();

        assert!(page.records.is_empty());
        assert!(!page.has_more);
        assert!(page.next_cursor.is_none());
        assert!(page.complete);
        assert_eq!(session.scan_opens(), 1);
        let plans = session.recorded_plans();
        assert_eq!(plans.len(), 2);
        assert_eq!(plans[0].windows[0].start, 0);
        assert_eq!(plans[1].windows[0].start, 4);
        assert_eq!(plans[1].windows[0].end, 8);
    }

    #[tokio::test(start_paused = true)]
    async fn exhausted_cursor_clears_continuation_without_fetching_empty_windows() {
        for filtered in [false, true] {
            let session = delayed(&[], Duration::from_secs(11));
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
            assert!(session.recorded_plans().is_empty());
            assert_eq!(session.scan_opens(), 0);
            assert_eq!(started.elapsed(), Duration::ZERO);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn filter_pass_cap_returns_a_resumable_cursor() {
        let session = delayed(&[256], Duration::from_millis(1));
        let marks = HashMap::from([(0, Watermarks { low: 0, high: 260 })]);
        let mut query = query();

        let page = fetch_page(&session, &query, &[0], &marks, 2, LIMITS)
            .await
            .unwrap();

        assert!(page.records.is_empty());
        assert!(page.has_more);
        assert_eq!(session.recorded_plans().len(), 64);
        assert_eq!(session.scan_opens(), 1);
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
        let plans = session.recorded_plans();
        assert_eq!(plans.len(), 65);
        assert_eq!(plans[64].windows[0].start, 256);
    }
}
