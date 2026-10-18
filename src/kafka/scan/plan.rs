use std::collections::BTreeMap;

use foldhash::{HashMap, HashMapExt};

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

/// The partitions a read covers and the offsets each one held when the read
/// opened.
#[derive(Debug)]
pub struct LogSpan {
    partitions: Vec<i32>,
    watermarks: HashMap<i32, Watermarks>,
}

impl LogSpan {
    pub fn new(partitions: Vec<i32>, watermarks: HashMap<i32, Watermarks>) -> Self {
        Self {
            partitions,
            watermarks,
        }
    }

    pub fn windows(
        &self,
        walk: RecordOrder,
        take: i64,
        cursor: Option<&RecordCursor>,
    ) -> Vec<PartitionWindow> {
        let remaining = cursor.map(|cursor| &cursor.remaining);

        self.partitions
            .iter()
            .filter_map(|&partition| {
                let marks = self.watermarks.get(&partition)?;
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

    pub fn start(&self, walk: RecordOrder) -> Remaining {
        let offsets = self
            .partitions
            .iter()
            .filter_map(|&partition| {
                let marks = self.watermarks.get(&partition)?;
                let near_end = match walk {
                    RecordOrder::Oldest => marks.low,
                    RecordOrder::Newest => marks.high,
                };
                Some((partition, near_end))
            })
            .collect();

        Remaining::walking(walk, offsets)
    }

    pub fn advance(
        &self,
        from: &RecordCursor,
        covered: &[PartitionWindow],
        kept: &[(i32, i64)],
        limit: usize,
    ) -> Option<RecordCursor> {
        let walk = from.walk();
        let returned = (kept.len() >= limit).then(|| past_furthest(walk, kept));
        let mut next = from.remaining.offsets().clone();
        for window in covered {
            let (near, far) = match walk {
                RecordOrder::Oldest => (window.start, window.end),
                RecordOrder::Newest => (window.end, window.start),
            };
            let offset = match &returned {
                Some(returned) => returned.get(&window.partition).copied().unwrap_or(near),
                None => far,
            };
            next.insert(window.partition, offset);
        }

        self.cursor(from.order, walk, next)
    }

    pub fn rewind(&self, opened: &RecordCursor) -> Option<RecordCursor> {
        let walk = opened.walk();
        let boundaries = opened.remaining.offsets();
        let back = self.partitions.iter().filter_map(|&partition| {
            let marks = self.watermarks.get(&partition)?;
            let far_end = match walk {
                RecordOrder::Oldest => marks.high,
                RecordOrder::Newest => marks.low,
            };
            let boundary = boundaries.get(&partition).copied().unwrap_or(far_end);
            Some((partition, boundary))
        });

        self.cursor(opened.order, walk.flipped(), back)
    }

    fn cursor(
        &self,
        order: RecordOrder,
        walk: RecordOrder,
        offsets: impl IntoIterator<Item = (i32, i64)>,
    ) -> Option<RecordCursor> {
        let offsets: BTreeMap<i32, i64> = offsets
            .into_iter()
            .filter(|(partition, _)| self.watermarks.contains_key(partition))
            .collect();
        let unread = offsets.iter().any(|(partition, &offset)| {
            let marks = &self.watermarks[partition];
            match walk {
                RecordOrder::Oldest => offset.max(marks.low) < marks.high,
                RecordOrder::Newest => offset.min(marks.high) > marks.low,
            }
        });

        unread.then(|| RecordCursor {
            order,
            remaining: Remaining::walking(walk, offsets),
        })
    }
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
    use std::num::NonZeroUsize;

    use super::*;
    use crate::kafka::limits::RecordLimits;
    use crate::kafka::scan::cursor::CursorDirection;

    fn limits() -> RecordLimits {
        RecordLimits {
            max_limit: NonZeroUsize::new(500).unwrap(),
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

    fn span(watermarks: HashMap<i32, Watermarks>) -> LogSpan {
        let mut partitions: Vec<i32> = watermarks.keys().copied().collect();
        partitions.sort_unstable();
        LogSpan::new(partitions, watermarks)
    }

    fn advance(
        walk: RecordOrder,
        covered: &[PartitionWindow],
        watermarks: HashMap<i32, Watermarks>,
        kept: &[(i32, i64)],
        limit: usize,
    ) -> Option<RecordCursor> {
        let from = covered
            .iter()
            .map(|window| match walk {
                RecordOrder::Oldest => (window.partition, window.start),
                RecordOrder::Newest => (window.partition, window.end),
            })
            .collect();
        let from = RecordCursor {
            order: walk,
            remaining: Remaining::walking(walk, from),
        };
        span(watermarks).advance(&from, covered, kept, limit)
    }

    #[test]
    fn newest_window_reads_from_the_high_watermark() {
        let windows = LogSpan::new(vec![0], marks(10, 40)).windows(
            RecordOrder::Newest,
            limits().window_take(5, false),
            None,
        );
        assert_eq!(windows, vec![window(0, 30, 40)]);
    }

    #[test]
    fn oldest_window_reads_from_the_low_watermark() {
        let windows = LogSpan::new(vec![0], marks(10, 40)).windows(
            RecordOrder::Oldest,
            limits().window_take(5, false),
            None,
        );
        assert_eq!(windows, vec![window(0, 10, 20)]);
    }

    #[test]
    fn newest_cursor_ends_the_next_window() {
        let windows = LogSpan::new(vec![0], marks(10, 40)).windows(
            RecordOrder::Newest,
            limits().window_take(5, false),
            Some(&cursor(Remaining::Before, 35)),
        );
        assert_eq!(windows, vec![window(0, 25, 35)]);
    }

    #[test]
    fn oldest_cursor_starts_the_next_window() {
        let windows = LogSpan::new(vec![0], marks(10, 40)).windows(
            RecordOrder::Oldest,
            limits().window_take(5, false),
            Some(&cursor(Remaining::From, 15)),
        );
        assert_eq!(windows, vec![window(0, 15, 25)]);
    }

    #[test]
    fn cursor_past_available_records_yields_no_windows() {
        let newest = LogSpan::new(vec![0], marks(10, 40)).windows(
            RecordOrder::Newest,
            limits().window_take(5, false),
            Some(&cursor(Remaining::Before, 10)),
        );
        let oldest = LogSpan::new(vec![0], marks(10, 40)).windows(
            RecordOrder::Oldest,
            limits().window_take(5, false),
            Some(&cursor(Remaining::From, 40)),
        );
        assert!(newest.is_empty());
        assert!(oldest.is_empty());
    }

    #[test]
    fn next_cursor_advances_oldest_past_returned_offsets() {
        let cursor = advance(
            RecordOrder::Oldest,
            &[window(0, 10, 20)],
            marks(10, 40),
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
            marks(10, 40),
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
                marks(10, 20),
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

        let windows = LogSpan::new(vec![0, 1], watermarks).windows(
            RecordOrder::Newest,
            limits().window_take(5, false),
            None,
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

        let windows = LogSpan::new(vec![0, 1], watermarks).windows(
            RecordOrder::Newest,
            limits().window_take(5, false),
            Some(&cursor(Remaining::Before, 20)),
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
            watermarks,
            &[(0, 39), (0, 35)],
            2,
        )
        .unwrap();
        assert_eq!(cursor.remaining, ending(&[(0, 35), (1, 40)]));
    }

    #[test]
    fn searching_widens_the_window() {
        let span = LogSpan::new(vec![0], marks(0, 1_000));

        let plain = span.windows(RecordOrder::Newest, limits().window_take(5, false), None);
        let searching = span.windows(RecordOrder::Newest, limits().window_take(5, true), None);

        assert_eq!(plain[0].end - plain[0].start, 10);
        assert_eq!(searching[0].end - searching[0].start, 40);
    }

    #[test]
    fn next_cursor_uses_last_kept_when_the_remaining_limit_is_filled() {
        let cursor = advance(
            RecordOrder::Newest,
            &[window(0, 100, 180)],
            marks(0, 500),
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
            marks(0, 500),
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
            marks(10, 40),
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
        let cursor = LogSpan::new(vec![0], marks(10, 100))
            .rewind(&cursor(Remaining::Before, 40))
            .unwrap();

        assert_eq!(cursor.remaining, starting(&[(0, 40)]));
        assert_eq!(cursor.direction(), CursorDirection::Backward);
    }

    #[test]
    fn the_near_edge_of_an_oldest_page_points_at_the_older_side() {
        let cursor = LogSpan::new(vec![0], marks(10, 100))
            .rewind(&cursor(Remaining::From, 14))
            .unwrap();

        assert_eq!(cursor.remaining, ending(&[(0, 14)]));
        assert_eq!(cursor.direction(), CursorDirection::Backward);
    }

    #[test]
    fn the_near_edge_of_a_backward_page_points_forward() {
        let backward = RecordCursor {
            order: RecordOrder::Newest,
            remaining: starting(&[(0, 20)]),
        };

        let cursor = LogSpan::new(vec![0], marks(10, 100))
            .rewind(&backward)
            .unwrap();

        assert_eq!(cursor.remaining, ending(&[(0, 20)]));
        assert_eq!(cursor.direction(), CursorDirection::Forward);
    }

    #[test]
    fn a_fresh_walk_starts_every_partition_at_its_near_end() {
        let watermarks = HashMap::from_iter([
            (0, Watermarks { low: 10, high: 40 }),
            (1, Watermarks { low: 0, high: 0 }),
        ]);

        let span = LogSpan::new(vec![0, 1, 2], watermarks);

        assert_eq!(
            span.start(RecordOrder::Oldest),
            starting(&[(0, 10), (1, 0)])
        );
        assert_eq!(span.start(RecordOrder::Newest), ending(&[(0, 40), (1, 0)]));
    }

    #[test]
    fn a_partition_the_walk_ran_out_of_stays_where_it_ran_out() {
        let watermarks = HashMap::from_iter([
            (0, Watermarks { low: 0, high: 40 }),
            (1, Watermarks { low: 0, high: 20 }),
        ]);

        let cursor = advance(
            RecordOrder::Oldest,
            &[window(0, 10, 20), window(1, 10, 20)],
            watermarks,
            &[],
            2,
        )
        .unwrap();

        assert_eq!(cursor.remaining, starting(&[(0, 20), (1, 20)]));
    }

    #[test]
    fn a_partition_without_a_window_keeps_its_place() {
        let watermarks = HashMap::from_iter([
            (0, Watermarks { low: 0, high: 40 }),
            (1, Watermarks { low: 0, high: 20 }),
        ]);

        let from = RecordCursor {
            order: RecordOrder::Oldest,
            remaining: starting(&[(0, 10), (1, 20)]),
        };

        let cursor = span(watermarks)
            .advance(&from, &[window(0, 10, 20)], &[], 2)
            .unwrap();

        assert_eq!(cursor.remaining, starting(&[(0, 20), (1, 20)]));
    }

    #[test]
    fn an_offset_the_low_watermark_passed_has_nothing_left_to_read() {
        let from = RecordCursor {
            order: RecordOrder::Oldest,
            remaining: starting(&[(0, 2)]),
        };

        assert!(span(marks(5, 5)).advance(&from, &[], &[], 2).is_none());
    }

    #[test]
    fn a_partition_the_cursor_ran_out_of_walks_back_from_where_it_ran_out() {
        let watermarks = HashMap::from_iter([
            (0, Watermarks { low: 10, high: 40 }),
            (1, Watermarks { low: 10, high: 40 }),
        ]);
        let opened = RecordCursor {
            order: RecordOrder::Oldest,
            remaining: starting(&[(0, 20), (1, 25)]),
        };

        let cursor = LogSpan::new(vec![0, 1], watermarks)
            .rewind(&opened)
            .unwrap();

        assert_eq!(cursor.remaining, ending(&[(0, 20), (1, 25)]));
    }

    #[test]
    fn a_partition_missing_from_the_cursor_walks_back_from_its_far_end() {
        let watermarks = HashMap::from_iter([
            (0, Watermarks { low: 10, high: 40 }),
            (1, Watermarks { low: 10, high: 40 }),
        ]);

        let span = LogSpan::new(vec![0, 1], watermarks);

        let newest = span.rewind(&cursor(Remaining::Before, 20)).unwrap();
        let oldest = span.rewind(&cursor(Remaining::From, 20)).unwrap();

        assert_eq!(newest.remaining, starting(&[(0, 20), (1, 10)]));
        assert_eq!(oldest.remaining, ending(&[(0, 20), (1, 40)]));
    }

    #[test]
    fn there_is_no_near_edge_at_the_start_of_the_log() {
        assert!(
            LogSpan::new(vec![0], marks(10, 40))
                .rewind(&cursor(Remaining::Before, 40))
                .is_none()
        );
        assert!(
            LogSpan::new(vec![0], marks(10, 40))
                .rewind(&cursor(Remaining::From, 10))
                .is_none()
        );
    }

    #[test]
    fn timestamp_offsets_narrow_the_watermarks() {
        let bound =
            |partition: i32, offset: Option<i64>| Some(HashMap::from_iter([(partition, offset)]));
        let cases = [
            (
                "a start offset raises the low watermark",
                bound(0, Some(25)),
                None,
                (25, 40),
            ),
            (
                "an end offset lowers the high watermark",
                None,
                bound(0, Some(22)),
                (10, 22),
            ),
            (
                "no start offset empties the partition",
                bound(0, None),
                None,
                (40, 40),
            ),
            (
                "another partition's start changes nothing",
                bound(1, Some(25)),
                None,
                (10, 40),
            ),
            (
                "no end offset keeps the high watermark",
                None,
                bound(0, None),
                (10, 40),
            ),
            (
                "inverted bounds collapse to empty",
                bound(0, Some(30)),
                bound(0, Some(20)),
                (20, 20),
            ),
        ];

        for (case, from, to, (low, high)) in cases {
            let mut watermarks = marks(10, 40);
            apply_timestamp_bounds(&mut watermarks, from.as_ref(), to.as_ref());
            assert_eq!(watermarks[&0], Watermarks { low, high }, "{case}");
        }
    }
}
