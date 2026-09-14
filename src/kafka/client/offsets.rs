use std::collections::HashMap;

use krafka::admin::ListOffsetResult;

use crate::kafka::watermarks::Watermarks;

/// `None` means the broker returned Kafka's invalid-offset sentinel, or the
/// partition's entry carried a per-partition error.
pub fn from_krafka_offsets(
    results: impl IntoIterator<Item = ListOffsetResult>,
) -> HashMap<(String, i32), Option<i64>> {
    results
        .into_iter()
        .map(|result| {
            let offset = (result.error.is_none() && result.offset >= 0).then_some(result.offset);
            ((result.topic, result.partition), offset)
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

    // `ListOffsetResult` is `#[non_exhaustive]` in krafka, so it cannot be
    // built with a struct literal here; `from_krafka_offsets` is covered by
    // `KafkaClient::watermarks`'s and `offsets_for_times`'s `FakeBroker`
    // integration tests in `client.rs` instead.

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
