use std::collections::{BTreeMap, HashMap};

use crate::kafka::limits::RecordLimits;
use crate::kafka::record::Record;
use crate::kafka::record::cursor::RecordCursor;
use crate::kafka::record::filter::RecordFilter;
use crate::kafka::record::query::{RecordOrder, RecordQuery};
use crate::kafka::watermarks::Watermarks;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionWindow {
    pub partition: i32,
    pub start: i64,
    pub end: i64,
}

impl PartitionWindow {
    pub fn is_empty(&self) -> bool {
        self.start >= self.end
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchPlan {
    pub topic: String,
    pub windows: Vec<PartitionWindow>,
    pub filter: Option<RecordFilter>,
    pub limit: usize,
    pub order: RecordOrder,
    pub schema_id: Option<i32>,
}

impl FetchPlan {
    pub fn build(
        query: &RecordQuery,
        partitions: &[i32],
        watermarks: &HashMap<i32, Watermarks>,
        limit: usize,
        limits: RecordLimits,
    ) -> Self {
        let searching = query.filter.is_some();
        let cursor = query.cursor.as_ref();

        Self {
            topic: query.topic.clone(),
            windows: plan_windows(
                partitions,
                watermarks,
                query.order,
                limit,
                searching,
                cursor,
                limits,
            ),
            filter: query.filter.clone(),
            limit,
            order: query.order,
            schema_id: query.schema_id,
        }
    }
}

/// Ordering decides which end of the log the window starts from: newest walks
/// back from the high watermark, oldest forward from the low watermark.
pub fn plan_windows(
    partitions: &[i32],
    watermarks: &HashMap<i32, Watermarks>,
    order: RecordOrder,
    limit: usize,
    searching: bool,
    cursor: Option<&RecordCursor>,
    limits: RecordLimits,
) -> Vec<PartitionWindow> {
    let take = limits.window_take(limit, searching);

    partitions
        .iter()
        .filter_map(|partition| {
            let marks = watermarks.get(partition)?;
            let resume = resume_offset(cursor, *partition, marks, order);

            let (start, end) = match order {
                RecordOrder::Newest => {
                    let end = resume.min(marks.high).max(marks.low);
                    let remaining = (end - marks.low).max(0);
                    let take = take.min(remaining);
                    if take == 0 {
                        return None;
                    }
                    (end - take, end)
                }
                RecordOrder::Oldest => {
                    let start = resume.max(marks.low).min(marks.high);
                    let remaining = (marks.high - start).max(0);
                    let take = take.min(remaining);
                    if take == 0 {
                        return None;
                    }
                    (start, start + take)
                }
            };

            Some(PartitionWindow {
                partition: *partition,
                start,
                end,
            })
        })
        .collect()
}

/// Where the next window for `partition` should start (oldest) or exclusively
/// end (newest).
///
/// A missing cursor means the first page. A cursor that omits a partition means
/// that partition is exhausted — not that it should restart from the log end.
fn resume_offset(
    cursor: Option<&RecordCursor>,
    partition: i32,
    marks: &Watermarks,
    order: RecordOrder,
) -> i64 {
    match (cursor, order) {
        (None, RecordOrder::Newest) => marks.high,
        (None, RecordOrder::Oldest) => marks.low,
        (Some(cursor), RecordOrder::Newest) => {
            cursor.offsets.get(&partition).copied().unwrap_or(marks.low)
        }
        (Some(cursor), RecordOrder::Oldest) => cursor
            .offsets
            .get(&partition)
            .copied()
            .unwrap_or(marks.high),
    }
}

/// Resume point after this page. `None` means the log (within the current
/// watermark bounds) is exhausted.
pub fn next_cursor(
    order: RecordOrder,
    windows: &[PartitionWindow],
    watermarks: &HashMap<i32, Watermarks>,
    records: &[Record],
    limit: usize,
) -> Option<RecordCursor> {
    let filled = records.len() >= limit;
    let mut offsets = BTreeMap::new();

    for window in windows {
        let Some(marks) = watermarks.get(&window.partition) else {
            continue;
        };

        match order {
            RecordOrder::Oldest => {
                let next = if filled {
                    records
                        .iter()
                        .filter(|record| record.partition == window.partition)
                        .map(|record| record.offset + 1)
                        .max()
                        .unwrap_or(window.start)
                } else {
                    window.end
                };
                if next < marks.high {
                    offsets.insert(window.partition, next);
                }
            }
            RecordOrder::Newest => {
                let next = if filled {
                    records
                        .iter()
                        .filter(|record| record.partition == window.partition)
                        .map(|record| record.offset)
                        .min()
                        .unwrap_or(window.end)
                } else {
                    window.start
                };
                if next > marks.low {
                    offsets.insert(window.partition, next);
                }
            }
        }
    }

    if offsets.is_empty() {
        None
    } else {
        Some(RecordCursor { offsets })
    }
}

/// Page cursor after a filtered fill loop.
///
/// `windows` and `last_kept` are the **final** `session.records` pass only.
/// `page_filled` is whether the **merged** page reached `limit`.
pub fn page_cursor(
    order: RecordOrder,
    windows: &[PartitionWindow],
    watermarks: &HashMap<i32, Watermarks>,
    last_kept: &[Record],
    page_filled: bool,
) -> Option<RecordCursor> {
    next_cursor(
        order,
        windows,
        watermarks,
        last_kept,
        cursor_limit(page_filled, last_kept.len()),
    )
}

fn cursor_limit(page_filled: bool, last_kept: usize) -> usize {
    if page_filled {
        last_kept
    } else {
        last_kept.saturating_add(1)
    }
}

/// A missing `from` offset means nothing was written at or after that time, so
/// the partition collapses to empty.
pub fn apply_timestamp_bounds(
    watermarks: &mut HashMap<i32, Watermarks>,
    from_offsets: Option<&HashMap<i32, Option<i64>>>,
    to_offsets: Option<&HashMap<i32, Option<i64>>>,
) {
    for (partition, marks) in watermarks {
        if let Some(from_offsets) = from_offsets {
            match from_offsets.get(partition).copied().flatten() {
                Some(offset) => marks.low = marks.low.max(offset),
                None => marks.low = marks.high,
            }
        }

        if let Some(to_offsets) = to_offsets
            && let Some(offset) = to_offsets.get(partition).copied().flatten()
        {
            marks.high = marks.high.min(offset);
        }

        if marks.low > marks.high {
            marks.low = marks.high;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The `environment.rs` defaults, pinned so the window assertions below do not
    /// depend on the ambient environment.
    fn limits() -> RecordLimits {
        RecordLimits {
            max_limit: 500,
            min_window: 4,
            window_multiplier: 2,
            search_window_multiplier: 8,
        }
    }

    fn marks(low: i64, high: i64) -> HashMap<i32, Watermarks> {
        HashMap::from([(0, Watermarks { low, high })])
    }

    fn cursor(partition: i32, offset: i64) -> RecordCursor {
        RecordCursor {
            offsets: std::collections::BTreeMap::from([(partition, offset)]),
        }
    }

    fn record(partition: i32, offset: i64) -> Record {
        Record {
            topic: "orders".into(),
            partition,
            offset,
            timestamp: offset,
            key: None,
            value: None,
            schema_id: None,
            headers: Vec::new(),
            size_bytes: 0,
            compression: crate::kafka::record::Compression::None,
        }
    }

    #[test]
    fn newest_window_reads_from_the_high_watermark() {
        let watermarks = marks(10, 40);

        let windows = plan_windows(
            &[0],
            &watermarks,
            RecordOrder::Newest,
            5,
            false,
            None,
            limits(),
        );
        assert_eq!(
            windows,
            vec![PartitionWindow {
                partition: 0,
                start: 30,
                end: 40,
            }]
        );
    }

    #[test]
    fn oldest_window_reads_from_the_low_watermark() {
        let watermarks = marks(10, 40);

        let windows = plan_windows(
            &[0],
            &watermarks,
            RecordOrder::Oldest,
            5,
            false,
            None,
            limits(),
        );
        assert_eq!(
            windows,
            vec![PartitionWindow {
                partition: 0,
                start: 10,
                end: 20,
            }]
        );
    }

    #[test]
    fn newest_cursor_ends_the_next_window() {
        let watermarks = marks(10, 40);
        let resume = cursor(0, 35);

        let windows = plan_windows(
            &[0],
            &watermarks,
            RecordOrder::Newest,
            5,
            false,
            Some(&resume),
            limits(),
        );
        assert_eq!(
            windows,
            vec![PartitionWindow {
                partition: 0,
                start: 25,
                end: 35,
            }]
        );
    }

    #[test]
    fn oldest_cursor_starts_the_next_window() {
        let watermarks = marks(10, 40);
        let resume = cursor(0, 15);

        let windows = plan_windows(
            &[0],
            &watermarks,
            RecordOrder::Oldest,
            5,
            false,
            Some(&resume),
            limits(),
        );
        assert_eq!(
            windows,
            vec![PartitionWindow {
                partition: 0,
                start: 15,
                end: 25,
            }]
        );
    }

    #[test]
    fn cursor_past_available_records_yields_no_windows() {
        let watermarks = marks(10, 40);

        let newest = plan_windows(
            &[0],
            &watermarks,
            RecordOrder::Newest,
            5,
            false,
            Some(&cursor(0, 10)),
            limits(),
        );
        let oldest = plan_windows(
            &[0],
            &watermarks,
            RecordOrder::Oldest,
            5,
            false,
            Some(&cursor(0, 40)),
            limits(),
        );
        assert!(newest.is_empty());
        assert!(oldest.is_empty());
    }

    #[test]
    fn next_cursor_advances_oldest_past_returned_offsets() {
        let watermarks = marks(10, 40);
        let windows = vec![PartitionWindow {
            partition: 0,
            start: 10,
            end: 20,
        }];
        let records = vec![record(0, 10), record(0, 14)];

        let cursor = next_cursor(RecordOrder::Oldest, &windows, &watermarks, &records, 2).unwrap();
        assert_eq!(cursor.offsets[&0], 15);
    }

    #[test]
    fn next_cursor_walks_newest_back_from_returned_offsets() {
        let watermarks = marks(10, 40);
        let windows = vec![PartitionWindow {
            partition: 0,
            start: 30,
            end: 40,
        }];
        let records = vec![record(0, 39), record(0, 35)];

        let cursor = next_cursor(RecordOrder::Newest, &windows, &watermarks, &records, 2).unwrap();
        assert_eq!(cursor.offsets[&0], 35);
    }

    #[test]
    fn next_cursor_is_none_when_the_log_is_exhausted() {
        let watermarks = marks(10, 20);
        let windows = vec![PartitionWindow {
            partition: 0,
            start: 10,
            end: 20,
        }];
        let records = vec![record(0, 10), record(0, 19)];

        assert!(next_cursor(RecordOrder::Oldest, &windows, &watermarks, &records, 50).is_none());
    }

    #[test]
    fn each_partition_keeps_a_full_page_window() {
        let watermarks = HashMap::from([
            (0, Watermarks { low: 0, high: 100 }),
            (1, Watermarks { low: 0, high: 100 }),
        ]);

        let windows = plan_windows(
            &[0, 1],
            &watermarks,
            RecordOrder::Newest,
            5,
            false,
            None,
            limits(),
        );

        assert_eq!(
            windows
                .iter()
                .map(|window| window.end - window.start)
                .collect::<Vec<_>>(),
            vec![10, 10]
        );
    }

    #[test]
    fn omitted_cursor_partition_stays_exhausted() {
        let watermarks = HashMap::from([
            (0, Watermarks { low: 0, high: 40 }),
            (1, Watermarks { low: 0, high: 40 }),
        ]);
        let resume = cursor(0, 20);

        let windows = plan_windows(
            &[0, 1],
            &watermarks,
            RecordOrder::Newest,
            5,
            false,
            Some(&resume),
            limits(),
        );

        assert_eq!(
            windows,
            vec![PartitionWindow {
                partition: 0,
                start: 10,
                end: 20,
            }]
        );
    }

    #[test]
    fn next_cursor_holds_unreturned_partition_at_window_end() {
        let watermarks = HashMap::from([
            (0, Watermarks { low: 10, high: 40 }),
            (1, Watermarks { low: 10, high: 40 }),
        ]);
        let windows = vec![
            PartitionWindow {
                partition: 0,
                start: 30,
                end: 40,
            },
            PartitionWindow {
                partition: 1,
                start: 30,
                end: 40,
            },
        ];
        let records = vec![record(0, 39), record(0, 35)];

        let cursor = next_cursor(RecordOrder::Newest, &windows, &watermarks, &records, 2).unwrap();
        assert_eq!(cursor.offsets[&0], 35);
        assert_eq!(cursor.offsets[&1], 40);
    }

    #[test]
    fn searching_widens_the_window() {
        let watermarks = marks(0, 1_000);

        let plain = plan_windows(
            &[0],
            &watermarks,
            RecordOrder::Newest,
            5,
            false,
            None,
            limits(),
        );
        let searching = plan_windows(
            &[0],
            &watermarks,
            RecordOrder::Newest,
            5,
            true,
            None,
            limits(),
        );

        assert_eq!(plain[0].end - plain[0].start, 10);
        assert_eq!(searching[0].end - searching[0].start, 40);
    }

    #[test]
    fn timestamp_from_raises_the_low_watermark() {
        let mut watermarks = marks(10, 40);
        let from = HashMap::from([(0, Some(25))]);

        apply_timestamp_bounds(&mut watermarks, Some(&from), None);
        assert_eq!(watermarks[&0], Watermarks { low: 25, high: 40 });
    }

    #[test]
    fn timestamp_to_lowers_the_high_watermark() {
        let mut watermarks = marks(10, 40);
        let to = HashMap::from([(0, Some(22))]);

        apply_timestamp_bounds(&mut watermarks, None, Some(&to));
        assert_eq!(watermarks[&0], Watermarks { low: 10, high: 22 });
    }

    #[test]
    fn missing_from_offset_empties_the_partition() {
        let mut watermarks = marks(10, 40);
        let from = HashMap::from([(0, None)]);

        apply_timestamp_bounds(&mut watermarks, Some(&from), None);
        assert_eq!(watermarks[&0], Watermarks { low: 40, high: 40 });
    }

    #[test]
    fn missing_to_offset_keeps_the_high_watermark() {
        let mut watermarks = marks(10, 40);
        let to = HashMap::from([(0, None)]);

        apply_timestamp_bounds(&mut watermarks, None, Some(&to));
        assert_eq!(watermarks[&0], Watermarks { low: 10, high: 40 });
    }

    #[test]
    fn inverted_timestamp_bounds_collapse_to_empty() {
        let mut watermarks = marks(10, 40);
        let from = HashMap::from([(0, Some(30))]);
        let to = HashMap::from([(0, Some(20))]);

        apply_timestamp_bounds(&mut watermarks, Some(&from), Some(&to));
        assert_eq!(watermarks[&0], Watermarks { low: 20, high: 20 });
    }

    #[test]
    fn build_keeps_the_compiled_filter() {
        let query = RecordQuery {
            topic: "orders".into(),
            partition: None,
            filter: crate::kafka::compile_record_filter(r#"key == "ord_1""#).unwrap(),
            timestamps: crate::kafka::record::query::TimestampRange::UNBOUNDED,
            limit: 5,
            order: RecordOrder::Newest,
            cursor: None,
            schema_id: None,
        };

        let plan = FetchPlan::build(&query, &[0], &marks(0, 100), 5, limits());
        assert_eq!(
            plan.filter
                .as_ref()
                .map(crate::kafka::record::filter::RecordFilter::source),
            Some(r#"key == "ord_1""#)
        );
        assert_eq!(plan.topic, "orders");
        assert_eq!(plan.windows[0].end - plan.windows[0].start, 40);
    }

    #[test]
    fn page_cursor_uses_last_kept_when_the_merged_page_is_full() {
        let watermarks = marks(0, 500);
        let windows = vec![PartitionWindow {
            partition: 0,
            start: 100,
            end: 180,
        }];
        let last_kept = vec![record(0, 160), record(0, 120)];

        let cursor =
            page_cursor(RecordOrder::Newest, &windows, &watermarks, &last_kept, true).unwrap();
        assert_eq!(cursor.offsets[&0], 120);
    }

    #[test]
    fn page_cursor_advances_past_the_window_when_still_underfilled() {
        let watermarks = marks(0, 500);
        let windows = vec![PartitionWindow {
            partition: 0,
            start: 420,
            end: 500,
        }];
        let last_kept = vec![record(0, 480), record(0, 440)];

        let cursor = page_cursor(
            RecordOrder::Newest,
            &windows,
            &watermarks,
            &last_kept,
            false,
        )
        .unwrap();
        assert_eq!(cursor.offsets[&0], 420);
    }
}
