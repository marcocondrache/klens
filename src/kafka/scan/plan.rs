use std::collections::BTreeMap;

use foldhash::{HashMap, HashMapExt};

use crate::kafka::limits::RecordLimits;
use crate::kafka::metadata::Watermarks;

use super::cursor::{CursorDirection, RecordCursor};
use super::query::RecordOrder;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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

pub fn plan_windows(
    partitions: &[i32],
    watermarks: &HashMap<i32, Watermarks>,
    walk: RecordOrder,
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
            let resume = resume_offset(cursor, *partition, marks, walk);

            let (start, end) = match walk {
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

fn resume_offset(
    cursor: Option<&RecordCursor>,
    partition: i32,
    marks: &Watermarks,
    walk: RecordOrder,
) -> i64 {
    match (cursor, walk) {
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

pub fn advance_cursor(
    walk: RecordOrder,
    covered: &[PartitionWindow],
    watermarks: &HashMap<i32, Watermarks>,
    kept: &[(i32, i64)],
    limit: usize,
    order: RecordOrder,
    direction: CursorDirection,
) -> Option<RecordCursor> {
    let filled = kept.len() >= limit;
    let mut returned: HashMap<i32, i64> = HashMap::new();
    if filled {
        for (partition, offset) in kept {
            let offset = match walk {
                RecordOrder::Oldest => offset + 1,
                RecordOrder::Newest => *offset,
            };
            returned
                .entry(*partition)
                .and_modify(|current| {
                    *current = match walk {
                        RecordOrder::Oldest => (*current).max(offset),
                        RecordOrder::Newest => (*current).min(offset),
                    };
                })
                .or_insert(offset);
        }
    }

    let mut offsets = BTreeMap::new();
    for window in covered {
        let Some(marks) = watermarks.get(&window.partition) else {
            continue;
        };

        match walk {
            RecordOrder::Oldest => {
                let next = if filled {
                    returned
                        .get(&window.partition)
                        .copied()
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
                    returned
                        .get(&window.partition)
                        .copied()
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

    cursor_from(offsets, order, direction)
}

pub fn rewind_cursor(
    walk: RecordOrder,
    watermarks: &HashMap<i32, Watermarks>,
    kept: &[(i32, i64)],
    order: RecordOrder,
    direction: CursorDirection,
) -> Option<RecordCursor> {
    let mut edges: HashMap<i32, i64> = HashMap::new();
    for (partition, offset) in kept {
        edges
            .entry(*partition)
            .and_modify(|current| {
                *current = match walk {
                    RecordOrder::Newest => (*current).max(*offset),
                    RecordOrder::Oldest => (*current).min(*offset),
                };
            })
            .or_insert(*offset);
    }

    let mut offsets = BTreeMap::new();
    for (partition, offset) in edges {
        let Some(marks) = watermarks.get(&partition) else {
            continue;
        };
        match walk {
            RecordOrder::Newest => {
                let next = offset + 1;
                if next < marks.high {
                    offsets.insert(partition, next);
                }
            }
            RecordOrder::Oldest => {
                if offset > marks.low {
                    offsets.insert(partition, offset);
                }
            }
        }
    }

    cursor_from(offsets, order, direction)
}

fn cursor_from(
    offsets: BTreeMap<i32, i64>,
    order: RecordOrder,
    direction: CursorDirection,
) -> Option<RecordCursor> {
    if offsets.is_empty() {
        None
    } else {
        Some(RecordCursor::new(order, direction, offsets))
    }
}

pub fn apply_timestamp_bounds(
    watermarks: &mut HashMap<i32, Watermarks>,
    from_offsets: Option<&HashMap<i32, Option<i64>>>,
    to_offsets: Option<&HashMap<i32, Option<i64>>>,
) {
    for (partition, marks) in watermarks {
        if let Some(from_offsets) = from_offsets {
            match from_offsets.get(partition) {
                Some(Some(offset)) => marks.low = marks.low.max(*offset),
                Some(None) => marks.low = marks.high,
                None => {}
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

    fn limits() -> RecordLimits {
        RecordLimits {
            max_limit: 500,
            min_window: 4,
            window_multiplier: 2,
            search_window_multiplier: 8,
        }
    }

    fn marks(low: i64, high: i64) -> HashMap<i32, Watermarks> {
        HashMap::from_iter([(0, Watermarks { low, high })])
    }

    fn cursor(partition: i32, offset: i64) -> RecordCursor {
        RecordCursor::new(
            RecordOrder::Newest,
            CursorDirection::Forward,
            BTreeMap::from([(partition, offset)]),
        )
    }

    fn window(partition: i32, start: i64, end: i64) -> PartitionWindow {
        PartitionWindow {
            partition,
            start,
            end,
        }
    }

    fn advance(
        walk: RecordOrder,
        covered: &[PartitionWindow],
        watermarks: &HashMap<i32, Watermarks>,
        kept: &[(i32, i64)],
        limit: usize,
    ) -> Option<RecordCursor> {
        advance_cursor(
            walk,
            covered,
            watermarks,
            kept,
            limit,
            walk,
            CursorDirection::Forward,
        )
    }

    #[test]
    fn newest_window_reads_from_the_high_watermark() {
        let windows = plan_windows(
            &[0],
            &marks(10, 40),
            RecordOrder::Newest,
            5,
            false,
            None,
            limits(),
        );
        assert_eq!(windows, vec![window(0, 30, 40)]);
    }

    #[test]
    fn oldest_window_reads_from_the_low_watermark() {
        let windows = plan_windows(
            &[0],
            &marks(10, 40),
            RecordOrder::Oldest,
            5,
            false,
            None,
            limits(),
        );
        assert_eq!(windows, vec![window(0, 10, 20)]);
    }

    #[test]
    fn newest_cursor_ends_the_next_window() {
        let windows = plan_windows(
            &[0],
            &marks(10, 40),
            RecordOrder::Newest,
            5,
            false,
            Some(&cursor(0, 35)),
            limits(),
        );
        assert_eq!(windows, vec![window(0, 25, 35)]);
    }

    #[test]
    fn oldest_cursor_starts_the_next_window() {
        let windows = plan_windows(
            &[0],
            &marks(10, 40),
            RecordOrder::Oldest,
            5,
            false,
            Some(&cursor(0, 15)),
            limits(),
        );
        assert_eq!(windows, vec![window(0, 15, 25)]);
    }

    #[test]
    fn cursor_past_available_records_yields_no_windows() {
        let newest = plan_windows(
            &[0],
            &marks(10, 40),
            RecordOrder::Newest,
            5,
            false,
            Some(&cursor(0, 10)),
            limits(),
        );
        let oldest = plan_windows(
            &[0],
            &marks(10, 40),
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
        let cursor = advance(
            RecordOrder::Oldest,
            &[window(0, 10, 20)],
            &marks(10, 40),
            &[(0, 10), (0, 14)],
            2,
        )
        .unwrap();
        assert_eq!(cursor.offsets[&0], 15);
    }

    #[test]
    fn next_cursor_walks_newest_back_from_returned_offsets() {
        let cursor = advance(
            RecordOrder::Newest,
            &[window(0, 30, 40)],
            &marks(10, 40),
            &[(0, 39), (0, 35)],
            2,
        )
        .unwrap();
        assert_eq!(cursor.offsets[&0], 35);
    }

    #[test]
    fn next_cursor_is_none_when_the_log_is_exhausted() {
        assert!(
            advance(
                RecordOrder::Oldest,
                &[window(0, 10, 20)],
                &marks(10, 20),
                &[(0, 10), (0, 19)],
                50,
            )
            .is_none()
        );
    }

    #[test]
    fn each_partition_keeps_a_full_page_window() {
        let watermarks = HashMap::from_iter([
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
        let watermarks = HashMap::from_iter([
            (0, Watermarks { low: 0, high: 40 }),
            (1, Watermarks { low: 0, high: 40 }),
        ]);

        let windows = plan_windows(
            &[0, 1],
            &watermarks,
            RecordOrder::Newest,
            5,
            false,
            Some(&cursor(0, 20)),
            limits(),
        );

        assert_eq!(windows, vec![window(0, 10, 20)]);
    }

    #[test]
    fn next_cursor_holds_unreturned_partition_at_window_end() {
        let watermarks = HashMap::from_iter([
            (0, Watermarks { low: 10, high: 40 }),
            (1, Watermarks { low: 10, high: 40 }),
        ]);

        let cursor = advance(
            RecordOrder::Newest,
            &[window(0, 30, 40), window(1, 30, 40)],
            &watermarks,
            &[(0, 39), (0, 35)],
            2,
        )
        .unwrap();
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
    fn next_cursor_uses_last_kept_when_the_remaining_limit_is_filled() {
        let cursor = advance(
            RecordOrder::Newest,
            &[window(0, 100, 180)],
            &marks(0, 500),
            &[(0, 160), (0, 120)],
            2,
        )
        .unwrap();
        assert_eq!(cursor.offsets[&0], 120);
    }

    #[test]
    fn next_cursor_advances_past_the_window_when_still_underfilled() {
        let cursor = advance(
            RecordOrder::Newest,
            &[window(0, 420, 500)],
            &marks(0, 500),
            &[(0, 480), (0, 440)],
            3,
        )
        .unwrap();
        assert_eq!(cursor.offsets[&0], 420);
    }

    #[test]
    fn a_partially_covered_window_only_advances_over_what_was_read() {
        let cursor = advance(
            RecordOrder::Newest,
            &[window(0, 36, 40)],
            &marks(10, 40),
            &[],
            2,
        )
        .unwrap();
        assert_eq!(
            cursor.offsets[&0], 36,
            "the unread older half is not skipped"
        );
    }

    #[test]
    fn the_near_edge_of_a_newest_page_points_at_the_newer_side() {
        let cursor = rewind_cursor(
            RecordOrder::Newest,
            &marks(10, 100),
            &[(0, 39), (0, 35)],
            RecordOrder::Newest,
            CursorDirection::Backward,
        )
        .unwrap();

        assert_eq!(cursor.offsets[&0], 40);
        assert_eq!(cursor.walk(), RecordOrder::Oldest);
    }

    #[test]
    fn the_near_edge_of_an_oldest_page_points_at_the_older_side() {
        let cursor = rewind_cursor(
            RecordOrder::Oldest,
            &marks(10, 100),
            &[(0, 14), (0, 20)],
            RecordOrder::Oldest,
            CursorDirection::Backward,
        )
        .unwrap();

        assert_eq!(cursor.offsets[&0], 14);
        assert_eq!(cursor.walk(), RecordOrder::Newest);
    }

    #[test]
    fn there_is_no_near_edge_at_the_end_of_the_log() {
        assert!(
            rewind_cursor(
                RecordOrder::Newest,
                &marks(10, 40),
                &[(0, 39)],
                RecordOrder::Newest,
                CursorDirection::Backward,
            )
            .is_none()
        );
        assert!(
            rewind_cursor(
                RecordOrder::Oldest,
                &marks(10, 40),
                &[(0, 10)],
                RecordOrder::Oldest,
                CursorDirection::Backward,
            )
            .is_none()
        );
    }

    #[test]
    fn an_empty_page_has_no_near_edge() {
        assert!(
            rewind_cursor(
                RecordOrder::Newest,
                &marks(10, 40),
                &[],
                RecordOrder::Newest,
                CursorDirection::Backward,
            )
            .is_none()
        );
    }

    #[test]
    fn timestamp_from_raises_the_low_watermark() {
        let mut watermarks = marks(10, 40);
        let from = HashMap::from_iter([(0, Some(25))]);

        apply_timestamp_bounds(&mut watermarks, Some(&from), None);
        assert_eq!(watermarks[&0], Watermarks { low: 25, high: 40 });
    }

    #[test]
    fn timestamp_to_lowers_the_high_watermark() {
        let mut watermarks = marks(10, 40);
        let to = HashMap::from_iter([(0, Some(22))]);

        apply_timestamp_bounds(&mut watermarks, None, Some(&to));
        assert_eq!(watermarks[&0], Watermarks { low: 10, high: 22 });
    }

    #[test]
    fn invalid_from_offset_empties_the_partition() {
        let mut watermarks = marks(10, 40);
        let from = HashMap::from_iter([(0, None)]);

        apply_timestamp_bounds(&mut watermarks, Some(&from), None);
        assert_eq!(watermarks[&0], Watermarks { low: 40, high: 40 });
    }

    #[test]
    fn omitted_from_offset_keeps_the_watermark() {
        let mut watermarks = marks(10, 40);
        let from = HashMap::from_iter([(1, Some(25))]);

        apply_timestamp_bounds(&mut watermarks, Some(&from), None);
        assert_eq!(watermarks[&0], Watermarks { low: 10, high: 40 });
    }

    #[test]
    fn invalid_to_offset_keeps_the_high_watermark() {
        let mut watermarks = marks(10, 40);
        let to = HashMap::from_iter([(0, None)]);

        apply_timestamp_bounds(&mut watermarks, None, Some(&to));
        assert_eq!(watermarks[&0], Watermarks { low: 10, high: 40 });
    }

    #[test]
    fn inverted_timestamp_bounds_collapse_to_empty() {
        let mut watermarks = marks(10, 40);
        let from = HashMap::from_iter([(0, Some(30))]);
        let to = HashMap::from_iter([(0, Some(20))]);

        apply_timestamp_bounds(&mut watermarks, Some(&from), Some(&to));
        assert_eq!(watermarks[&0], Watermarks { low: 20, high: 20 });
    }
}
