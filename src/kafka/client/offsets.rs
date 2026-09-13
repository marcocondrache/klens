use std::collections::HashMap;
use std::time::Duration;

use rdkafka::consumer::Consumer;
use rdkafka::topic_partition_list::{Offset, TopicPartitionList};

use crate::kafka::error::KafkaError;
use crate::kafka::watermarks::Watermarks;

/// `timestamp` selects what to look up: [`Offset::Beginning`],
/// [`Offset::End`], or [`Offset::Offset`] with a unix-millis value to resolve a
/// time. `None` in the result means the broker returned Kafka's invalid-offset
/// sentinel for that partition.
pub fn list_offsets<S: AsRef<str>>(
    consumer: &impl Consumer,
    partitions: &[(S, i32)],
    timestamp: Offset,
    timeout: Duration,
) -> Result<HashMap<(String, i32), Option<i64>>, KafkaError> {
    let mut tpl = TopicPartitionList::new();
    for (topic, partition) in partitions {
        tpl.add_partition_offset(topic.as_ref(), *partition, timestamp)?;
    }

    let listed = consumer.offsets_for_times(tpl, timeout)?;
    Ok(listed
        .elements()
        .into_iter()
        .map(|element| {
            let offset = match element.offset() {
                Offset::Offset(offset) if offset >= 0 => Some(offset),
                _ => None,
            };
            ((element.topic().to_owned(), element.partition()), offset)
        })
        .collect())
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
