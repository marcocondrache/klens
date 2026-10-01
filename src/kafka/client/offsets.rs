use foldhash::{HashMap, HashMapExt};

pub fn partition_time_offsets(
    listed: impl IntoIterator<Item = (String, i32, i64)>,
) -> HashMap<i32, Option<i64>> {
    listed
        .into_iter()
        .map(|(_, partition, offset)| (partition, (offset >= 0).then_some(offset)))
        .collect()
}

pub fn known_offsets(
    listed: impl IntoIterator<Item = (String, i32, i64)>,
) -> HashMap<String, HashMap<i32, i64>> {
    let mut out: HashMap<String, HashMap<i32, i64>> = HashMap::new();
    for (topic, partition, offset) in listed {
        if offset >= 0 {
            out.entry(topic).or_default().insert(partition, offset);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn partition_time_offsets_keeps_only_what_the_broker_returned() {
        let offsets = partition_time_offsets([("orders".into(), 0, 12), ("orders".into(), 2, -1)]);
        assert_eq!(offsets.get(&0), Some(&Some(12)));
        assert_eq!(offsets.get(&2), Some(&None));
        assert!(!offsets.contains_key(&1));
    }

    #[test]
    fn known_offsets_skip_partitions_without_an_offset() {
        let offsets = known_offsets([
            ("orders".into(), 0, 0),
            ("orders".into(), 1, 12),
            ("orders".into(), 2, -1),
            ("payments".into(), 0, -1),
        ]);

        assert_eq!(
            offsets,
            HashMap::from_iter([("orders".into(), HashMap::from_iter([(0, 0), (1, 12)]))])
        );
    }
}
