use foldhash::{HashMap, HashMapExt};

use crate::kafka::watermarks::Watermarks;

/// `None` means the broker returned Kafka's invalid-offset sentinel.
pub fn from_list_offsets(
    results: impl IntoIterator<Item = (String, i32, i64)>,
) -> HashMap<(String, i32), Option<i64>> {
    results
        .into_iter()
        .map(|(topic, partition, offset)| {
            (
                (topic, partition),
                if offset >= 0 { Some(offset) } else { None },
            )
        })
        .collect()
}

pub fn partition_time_offsets(
    listed: impl IntoIterator<Item = (String, i32, i64)>,
) -> HashMap<i32, Option<i64>> {
    listed
        .into_iter()
        .map(|(_, partition, offset)| (partition, (offset >= 0).then_some(offset)))
        .collect()
}

/// Partitions missing a high offset are dropped, and inverted pairs are
/// skipped rather than reported as negative message counts.
pub fn merge_watermark_offsets(
    beginning: &HashMap<(String, i32), Option<i64>>,
    end: impl IntoIterator<Item = (String, i32, i64)>,
) -> HashMap<String, HashMap<i32, Watermarks>> {
    let mut out: HashMap<String, HashMap<i32, Watermarks>> = HashMap::new();
    for (topic, partition, high) in end {
        if high < 0 {
            continue;
        }
        let key = (topic, partition);
        // Empty partitions often return only the last offset.
        let low = beginning.get(&key).copied().flatten().unwrap_or(high);
        if high < low {
            continue;
        }
        let (topic, partition) = key;
        out.entry(topic)
            .or_default()
            .insert(partition, Watermarks { low, high });
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_list_offsets_keeps_concrete_offsets() {
        let listed = from_list_offsets([
            ("orders".into(), 0, 12),
            ("orders".into(), 1, -1),
            ("orders".into(), 2, -2),
        ]);
        assert_eq!(listed.get(&("orders".into(), 0)), Some(&Some(12)));
        assert_eq!(listed.get(&("orders".into(), 1)), Some(&None));
        assert_eq!(listed.get(&("orders".into(), 2)), Some(&None));
    }

    #[test]
    fn partition_time_offsets_keeps_only_what_the_broker_returned() {
        let offsets = partition_time_offsets([("orders".into(), 0, 12), ("orders".into(), 2, -1)]);
        assert_eq!(offsets.get(&0), Some(&Some(12)));
        assert_eq!(offsets.get(&2), Some(&None));
        assert!(!offsets.contains_key(&1));
    }

    #[test]
    fn merge_watermark_offsets_keeps_empty_skips_inverted_and_partial() {
        let beginning = HashMap::from_iter([
            (("orders".into(), 0), Some(0)),
            (("orders".into(), 1), Some(10)),
            (("orders".into(), 2), Some(4)),
            (("payments".into(), 0), Some(1)),
            (("payments".into(), 1), None),
            (("logs".into(), 0), Some(3)),
        ]);
        let end = [
            ("orders".into(), 0, 0),
            ("orders".into(), 1, 5),
            ("orders".into(), 2, 12),
            ("payments".into(), 0, -1),
            ("payments".into(), 1, 9),
            ("logs".into(), 0, 9),
        ];

        assert_eq!(
            merge_watermark_offsets(&beginning, end),
            HashMap::from_iter([
                (
                    "orders".into(),
                    HashMap::from_iter([
                        (0, Watermarks { low: 0, high: 0 }),
                        (2, Watermarks { low: 4, high: 12 }),
                    ]),
                ),
                (
                    "payments".into(),
                    HashMap::from_iter([(1, Watermarks { low: 9, high: 9 })]),
                ),
                (
                    "logs".into(),
                    HashMap::from_iter([(0, Watermarks { low: 3, high: 9 })]),
                ),
            ])
        );
    }
}
