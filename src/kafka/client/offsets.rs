use std::collections::HashMap;

use rdkafka::admin::ListOffsetsResultInfo;
use rdkafka::topic_partition_list::Offset;

use crate::kafka::watermarks::Watermarks;

/// `None` means the broker returned Kafka's invalid-offset sentinel.
pub fn from_list_infos(
    infos: impl IntoIterator<Item = ListOffsetsResultInfo>,
) -> HashMap<(String, i32), Option<i64>> {
    infos
        .into_iter()
        .map(|info| {
            let offset = match info.offset {
                Offset::Offset(offset) if offset >= 0 => Some(offset),
                _ => None,
            };
            ((info.topic, info.partition), offset)
        })
        .collect()
}

pub fn partition_time_offsets(
    listed: HashMap<(String, i32), Option<i64>>,
) -> HashMap<i32, Option<i64>> {
    listed
        .into_iter()
        .map(|((_, partition), offset)| (partition, offset))
        .collect()
}

/// Partitions missing a high offset are dropped, and inverted pairs are
/// skipped rather than reported as negative message counts.
pub fn merge_watermark_offsets(
    beginning: &HashMap<(String, i32), Option<i64>>,
    end: &HashMap<(String, i32), Option<i64>>,
) -> HashMap<String, HashMap<i32, Watermarks>> {
    let mut out: HashMap<String, HashMap<i32, Watermarks>> = HashMap::new();
    for (key, high) in end {
        let Some(high) = *high else {
            continue;
        };
        // Empty partitions often return only the last offset.
        let low = beginning.get(key).copied().flatten().unwrap_or(high);
        if high < low {
            continue;
        }
        out.entry(key.0.clone())
            .or_default()
            .insert(key.1, Watermarks { low, high });
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_list_infos_keeps_concrete_offsets() {
        let listed = from_list_infos([
            ListOffsetsResultInfo {
                topic: "orders".into(),
                partition: 0,
                offset: Offset::Offset(12),
                timestamp: -1,
            },
            ListOffsetsResultInfo {
                topic: "orders".into(),
                partition: 1,
                offset: Offset::Invalid,
                timestamp: -1,
            },
            ListOffsetsResultInfo {
                topic: "orders".into(),
                partition: 2,
                offset: Offset::End,
                timestamp: -1,
            },
        ]);
        assert_eq!(listed.get(&("orders".into(), 0)), Some(&Some(12)));
        assert_eq!(listed.get(&("orders".into(), 1)), Some(&None));
        assert_eq!(listed.get(&("orders".into(), 2)), Some(&None));
    }

    #[test]
    fn partition_time_offsets_keeps_only_what_the_broker_returned() {
        let listed = HashMap::from([
            (("orders".into(), 0), Some(12)),
            (("orders".into(), 2), None),
        ]);

        let offsets = partition_time_offsets(listed);
        assert_eq!(offsets.get(&0), Some(&Some(12)));
        assert_eq!(offsets.get(&2), Some(&None));
        assert!(!offsets.contains_key(&1));
    }

    #[test]
    fn merge_watermark_offsets_keeps_empty_skips_inverted_and_partial() {
        let beginning = HashMap::from([
            (("orders".into(), 0), Some(0)),
            (("orders".into(), 1), Some(10)),
            (("orders".into(), 2), Some(4)),
            (("payments".into(), 0), Some(1)),
            (("payments".into(), 1), None),
            (("logs".into(), 0), Some(3)),
        ]);
        let end = HashMap::from([
            (("orders".into(), 0), Some(0)),
            (("orders".into(), 1), Some(5)),
            (("orders".into(), 2), Some(12)),
            (("payments".into(), 0), None),
            (("payments".into(), 1), Some(9)),
            (("logs".into(), 0), Some(9)),
        ]);

        assert_eq!(
            merge_watermark_offsets(&beginning, &end),
            HashMap::from([
                (
                    "orders".into(),
                    HashMap::from([
                        (0, Watermarks { low: 0, high: 0 }),
                        (2, Watermarks { low: 4, high: 12 }),
                    ]),
                ),
                (
                    "payments".into(),
                    HashMap::from([(1, Watermarks { low: 9, high: 9 })]),
                ),
                (
                    "logs".into(),
                    HashMap::from([(0, Watermarks { low: 3, high: 9 })]),
                ),
            ])
        );
    }
}
