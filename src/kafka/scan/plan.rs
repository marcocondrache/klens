use std::collections::BTreeMap;

use foldhash::{HashMap, HashMapExt};

use crate::kafka::limits::RecordLimits;
use crate::kafka::metadata::Watermarks;

use super::cursor::{RecordCursor, Remaining};
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
    let remaining = cursor.map(|cursor| &cursor.remaining);

    partitions
        .iter()
        .filter_map(|&partition| {
            let marks = watermarks.get(&partition)?;
            let ending_at = |end: i64| {
                let end = end.min(marks.high).max(marks.low);
                (end - take.min(end - marks.low), end)
            };
            let starting_at = |start: i64| {
                let start = start.max(marks.low).min(marks.high);
                (start, start + take.min(marks.high - start))
            };

            let (start, end) = match (remaining, walk) {
                (None, RecordOrder::Newest) => ending_at(marks.high),
                (None, RecordOrder::Oldest) => starting_at(marks.low),
                (Some(Remaining::Before(ends)), _) => ending_at(*ends.get(&partition)?),
                (Some(Remaining::From(starts)), _) => starting_at(*starts.get(&partition)?),
            };
            let window = PartitionWindow {
                partition,
                start,
                end,
            };
            (!window.is_empty()).then_some(window)
        })
        .collect()
}

pub fn advance_cursor(
    walk: RecordOrder,
    covered: &[PartitionWindow],
    watermarks: &HashMap<i32, Watermarks>,
    kept: &[(i32, i64)],
    limit: usize,
    order: RecordOrder,
) -> Option<RecordCursor> {
    let returned = (kept.len() >= limit).then(|| past_furthest(walk, kept));
    let next = covered.iter().map(|window| {
        let (near, far) = match walk {
            RecordOrder::Oldest => (window.start, window.end),
            RecordOrder::Newest => (window.end, window.start),
        };
        let next = match &returned {
            Some(returned) => returned.get(&window.partition).copied().unwrap_or(near),
            None => far,
        };
        (window.partition, next)
    });

    cursor_from(order, walk, next, watermarks)
}

/// The page `cursor` opened starts where it points, so walking back resumes
/// every partition there. A partition it no longer lists was fully shown
/// before the page, so the walk back starts from that partition's far end.
pub fn rewind_cursor(
    cursor: &RecordCursor,
    partitions: &[i32],
    watermarks: &HashMap<i32, Watermarks>,
) -> Option<RecordCursor> {
    let walk = cursor.walk();
    let boundaries = cursor.remaining.offsets();
    let back = partitions.iter().filter_map(|&partition| {
        let marks = watermarks.get(&partition)?;
        let far_end = match walk {
            RecordOrder::Oldest => marks.high,
            RecordOrder::Newest => marks.low,
        };
        let boundary = boundaries.get(&partition).copied().unwrap_or(far_end);
        Some((partition, boundary))
    });

    cursor_from(cursor.order, walk.flipped(), back, watermarks)
}

fn past_furthest(walk: RecordOrder, kept: &[(i32, i64)]) -> HashMap<i32, i64> {
    let mut edges = HashMap::new();
    for &(partition, offset) in kept {
        let (past, further): (i64, fn(i64, i64) -> i64) = match walk {
            RecordOrder::Oldest => (offset + 1, i64::max),
            RecordOrder::Newest => (offset, i64::min),
        };
        let edge = edges.entry(partition).or_insert(past);
        *edge = further(*edge, past);
    }
    edges
}

fn cursor_from(
    order: RecordOrder,
    walk: RecordOrder,
    offsets: impl IntoIterator<Item = (i32, i64)>,
    watermarks: &HashMap<i32, Watermarks>,
) -> Option<RecordCursor> {
    let offsets: BTreeMap<i32, i64> = offsets
        .into_iter()
        .filter(|&(partition, offset)| {
            watermarks.get(&partition).is_some_and(|marks| match walk {
                RecordOrder::Oldest => offset < marks.high,
                RecordOrder::Newest => offset > marks.low,
            })
        })
        .collect();

    (!offsets.is_empty()).then(|| RecordCursor {
        order,
        remaining: Remaining::walking(walk, offsets),
    })
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
    use crate::kafka::scan::cursor::CursorDirection;

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

    fn cursor(remaining: fn(BTreeMap<i32, i64>) -> Remaining, offset: i64) -> RecordCursor {
        let remaining = remaining(BTreeMap::from([(0, offset)]));
        RecordCursor {
            order: remaining.walk(),
            remaining,
        }
    }

    fn ending(offsets: &[(i32, i64)]) -> Remaining {
        Remaining::Before(BTreeMap::from_iter(offsets.iter().copied()))
    }

    fn starting(offsets: &[(i32, i64)]) -> Remaining {
        Remaining::From(BTreeMap::from_iter(offsets.iter().copied()))
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
        advance_cursor(walk, covered, watermarks, kept, limit, walk)
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
            Some(&cursor(Remaining::Before, 35)),
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
            Some(&cursor(Remaining::From, 15)),
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
            Some(&cursor(Remaining::Before, 10)),
            limits(),
        );
        let oldest = plan_windows(
            &[0],
            &marks(10, 40),
            RecordOrder::Oldest,
            5,
            false,
            Some(&cursor(Remaining::From, 40)),
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
        assert_eq!(cursor.remaining, starting(&[(0, 15)]));
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
        assert_eq!(cursor.remaining, ending(&[(0, 35)]));
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
            Some(&cursor(Remaining::Before, 20)),
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
        assert_eq!(cursor.remaining, ending(&[(0, 35), (1, 40)]));
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
        assert_eq!(cursor.remaining, ending(&[(0, 120)]));
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
        assert_eq!(cursor.remaining, ending(&[(0, 420)]));
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
            cursor.remaining,
            ending(&[(0, 36)]),
            "the unread older half is not skipped"
        );
    }

    #[test]
    fn the_near_edge_of_a_newest_page_points_at_the_newer_side() {
        let cursor = rewind_cursor(&cursor(Remaining::Before, 40), &[0], &marks(10, 100)).unwrap();

        assert_eq!(cursor.remaining, starting(&[(0, 40)]));
        assert_eq!(cursor.direction(), CursorDirection::Backward);
    }

    #[test]
    fn the_near_edge_of_an_oldest_page_points_at_the_older_side() {
        let cursor = rewind_cursor(&cursor(Remaining::From, 14), &[0], &marks(10, 100)).unwrap();

        assert_eq!(cursor.remaining, ending(&[(0, 14)]));
        assert_eq!(cursor.direction(), CursorDirection::Backward);
    }

    #[test]
    fn the_near_edge_of_a_backward_page_points_forward() {
        let backward = RecordCursor {
            order: RecordOrder::Newest,
            remaining: starting(&[(0, 20)]),
        };

        let cursor = rewind_cursor(&backward, &[0], &marks(10, 100)).unwrap();

        assert_eq!(cursor.remaining, ending(&[(0, 20)]));
        assert_eq!(cursor.direction(), CursorDirection::Forward);
    }

    #[test]
    fn a_partition_the_cursor_exhausted_walks_back_from_its_far_end() {
        let watermarks = HashMap::from_iter([
            (0, Watermarks { low: 10, high: 40 }),
            (1, Watermarks { low: 10, high: 40 }),
        ]);

        let newest = rewind_cursor(&cursor(Remaining::Before, 20), &[0, 1], &watermarks).unwrap();
        let oldest = rewind_cursor(&cursor(Remaining::From, 20), &[0, 1], &watermarks).unwrap();

        assert_eq!(newest.remaining, starting(&[(0, 20), (1, 10)]));
        assert_eq!(oldest.remaining, ending(&[(0, 20), (1, 40)]));
    }

    #[test]
    fn there_is_no_near_edge_at_the_start_of_the_log() {
        assert!(rewind_cursor(&cursor(Remaining::Before, 40), &[0], &marks(10, 40)).is_none());
        assert!(rewind_cursor(&cursor(Remaining::From, 10), &[0], &marks(10, 40)).is_none());
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
