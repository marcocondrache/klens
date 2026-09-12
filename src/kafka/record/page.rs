use std::collections::HashMap;
use std::time::Instant;

use crate::kafka::error::KafkaError;
use crate::kafka::limits::RecordLimits;
use crate::kafka::record::cursor::RecordCursor;
use crate::kafka::record::plan::{FetchPlan, PartitionWindow, page_cursor};
use crate::kafka::record::query::RecordQuery;
use crate::kafka::record::{Record, RecordPage};
use crate::kafka::session::ClusterSession;
use crate::kafka::watermarks::Watermarks;

const MAX_FILTER_PASSES: usize = 64;

pub async fn fetch_one_page<S: ClusterSession + ?Sized>(
    session: &S,
    query: &RecordQuery,
    partitions: &[i32],
    watermarks: &HashMap<i32, Watermarks>,
    limit: usize,
    limits: RecordLimits,
) -> Result<RecordPage, KafkaError> {
    let plan = FetchPlan::build(query, partitions, watermarks, limit, limits);
    let records = session.records(&plan).await?;
    let next = crate::kafka::record::plan::next_cursor(
        plan.order,
        &plan.windows,
        watermarks,
        &records,
        plan.limit,
    );
    Ok(RecordPage {
        has_more: next.is_some(),
        next_cursor: next.map(|cursor| cursor.encode()),
        records,
    })
}

pub async fn fill_filtered_page<S: ClusterSession + ?Sized>(
    session: &S,
    query: &RecordQuery,
    partitions: &[i32],
    watermarks: &HashMap<i32, Watermarks>,
    limit: usize,
    limits: RecordLimits,
) -> Result<RecordPage, KafkaError> {
    let started = Instant::now();
    let budget = session.consume_timeout();
    let mut collected: Vec<Record> = Vec::new();
    let mut resume = query.cursor.clone();
    let mut last_windows: Vec<PartitionWindow> = Vec::new();
    let mut last_kept: Vec<Record> = Vec::new();

    for _ in 0..MAX_FILTER_PASSES {
        if started.elapsed() >= budget {
            break;
        }

        let plan = FetchPlan::build(
            &pass_query(query, resume.clone()),
            partitions,
            watermarks,
            limit,
            limits,
        );
        if plan.windows.is_empty() {
            last_windows.clear();
            last_kept.clear();
            break;
        }

        let batch = session.records(&plan).await?;
        last_windows = plan.windows.clone();

        let remaining = limit - collected.len();
        let kept: Vec<Record> = batch.into_iter().take(remaining).collect();
        last_kept = kept.clone();
        collected.extend(kept);

        let page_filled = collected.len() >= limit;
        let next = page_cursor(
            plan.order,
            &last_windows,
            watermarks,
            &last_kept,
            page_filled,
        );

        if page_filled {
            resume = next;
            break;
        }
        if next.is_none() {
            resume = None;
            break;
        }
        if next == resume {
            break;
        }
        resume = next;
    }

    collected.sort_by(|left, right| left.cmp_for_order(right, query.order));

    Ok(RecordPage {
        has_more: resume.is_some(),
        next_cursor: resume.map(|cursor| cursor.encode()),
        records: collected,
    })
}

fn pass_query(query: &RecordQuery, resume: Option<RecordCursor>) -> RecordQuery {
    let mut pass = query.clone();
    pass.cursor = resume;
    pass
}
