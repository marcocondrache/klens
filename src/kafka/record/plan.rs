use std::collections::HashMap;

use crate::kafka::limits::RecordLimits;
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
    pub search: String,
    pub limit: usize,
    pub order: RecordOrder,
    pub has_more: bool,
}

impl FetchPlan {
    pub fn build(
        query: &RecordQuery,
        partitions: &[i32],
        watermarks: &HashMap<i32, Watermarks>,
        limit: usize,
        page: usize,
        limits: RecordLimits,
    ) -> Self {
        let searching = !query.search.trim().is_empty();

        Self {
            topic: query.topic.clone(),
            windows: plan_windows(
                partitions,
                watermarks,
                query.order,
                limit,
                searching,
                page,
                limits,
            ),
            search: query.search.trim().to_ascii_lowercase(),
            limit,
            order: query.order,
            has_more: has_more(partitions, watermarks, limit, page),
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
    page: usize,
    limits: RecordLimits,
) -> Vec<PartitionWindow> {
    let (skip, window) = limits.window_span(partitions.len(), limit, searching, page);

    partitions
        .iter()
        .filter_map(|partition| {
            let marks = watermarks.get(partition)?;
            let remaining = (marks.available() - skip).max(0);
            let take = window.min(remaining);
            if take == 0 {
                return None;
            }

            let (start, end) = match order {
                RecordOrder::Newest => {
                    let end = marks.high - skip;
                    (end - take, end)
                }
                RecordOrder::Oldest => {
                    let start = marks.low + skip;
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

pub fn has_more(
    partitions: &[i32],
    watermarks: &HashMap<i32, Watermarks>,
    limit: usize,
    page: usize,
) -> bool {
    let available: i64 = partitions
        .iter()
        .filter_map(|partition| watermarks.get(partition).map(|marks| marks.available()))
        .sum();
    ((page + 1).saturating_mul(limit) as i64) < available
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

    #[test]
    fn newest_window_reads_from_the_high_watermark() {
        let watermarks = marks(10, 40);

        let windows = plan_windows(
            &[0],
            &watermarks,
            RecordOrder::Newest,
            5,
            false,
            0,
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
        assert!(has_more(&[0], &watermarks, 5, 0));
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
            0,
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
        assert!(has_more(&[0], &watermarks, 5, 0));
    }

    #[test]
    fn newest_window_on_later_page_moves_back_from_the_high_watermark() {
        let watermarks = marks(10, 40);

        let windows = plan_windows(
            &[0],
            &watermarks,
            RecordOrder::Newest,
            5,
            false,
            1,
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
        assert!(has_more(&[0], &watermarks, 5, 1));
    }

    #[test]
    fn oldest_window_on_later_page_moves_forward_from_the_low_watermark() {
        let watermarks = marks(10, 40);

        let windows = plan_windows(
            &[0],
            &watermarks,
            RecordOrder::Oldest,
            5,
            false,
            1,
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
        assert!(has_more(&[0], &watermarks, 5, 1));
    }

    #[test]
    fn page_past_available_records_yields_no_windows() {
        let watermarks = marks(10, 40);

        let windows = plan_windows(
            &[0],
            &watermarks,
            RecordOrder::Newest,
            5,
            false,
            6,
            limits(),
        );
        assert!(windows.is_empty());
        assert!(!has_more(&[0], &watermarks, 5, 6));
    }

    #[test]
    fn first_page_has_more_when_returned_limit_is_below_log_size() {
        let watermarks = marks(0, 10);

        assert!(has_more(&[0], &watermarks, 5, 0));
        assert!(!has_more(&[0], &watermarks, 5, 1));
        assert_eq!(
            plan_windows(
                &[0],
                &watermarks,
                RecordOrder::Oldest,
                5,
                false,
                1,
                limits()
            ),
            vec![PartitionWindow {
                partition: 0,
                start: 5,
                end: 10,
            }]
        );
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
            0,
            limits(),
        );
        let searching = plan_windows(&[0], &watermarks, RecordOrder::Newest, 5, true, 0, limits());

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
    fn build_normalises_the_search_term() {
        let query = RecordQuery {
            topic: "orders".into(),
            partition: None,
            search: "  OrderId  ".into(),
            timestamps: crate::kafka::record::query::TimestampRange::UNBOUNDED,
            limit: 5,
            order: RecordOrder::Newest,
            page: 0,
        };

        let plan = FetchPlan::build(&query, &[0], &marks(0, 100), 5, 0, limits());
        assert_eq!(plan.search, "orderid");
        assert_eq!(plan.topic, "orders");
    }
}
