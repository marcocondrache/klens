use std::cmp::Ordering;
use std::collections::BinaryHeap;

use super::Record;
use super::query::RecordOrder;

pub(crate) struct RecordBatch {
    records: BinaryHeap<Entry>,
    limit: usize,
    order: RecordOrder,
    seen: usize,
}

impl RecordBatch {
    pub(crate) fn new(limit: usize, order: RecordOrder) -> Self {
        Self {
            records: BinaryHeap::new(),
            limit,
            order,
            seen: 0,
        }
    }

    pub(crate) fn push(&mut self, record: Record) {
        if self.limit == 0 {
            return;
        }

        let entry = Entry {
            record,
            order: self.order,
            sequence: self.seen,
        };
        self.seen += 1;

        if self.records.len() < self.limit {
            self.records.push(entry);
        } else if let Some(mut worst) = self.records.peek_mut()
            && entry < *worst
        {
            *worst = entry;
        }
    }

    pub(crate) fn into_records(self) -> Vec<Record> {
        self.records
            .into_sorted_vec()
            .into_iter()
            .map(|entry| entry.record)
            .collect()
    }
}

struct Entry {
    record: Record,
    order: RecordOrder,
    sequence: usize,
}

impl Ord for Entry {
    fn cmp(&self, other: &Self) -> Ordering {
        // Better records compare less, so the max-heap keeps the worst at its root.
        // Every entry uses the batch's order; sequence preserves stable sort ties.
        self.record
            .cmp_for_order(&other.record, self.order)
            .then(self.sequence.cmp(&other.sequence))
    }
}

impl PartialOrd for Entry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for Entry {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for Entry {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kafka::record::Compression;

    fn record(timestamp: i64, partition: i32, offset: i64) -> Record {
        Record {
            topic: "orders.created".into(),
            partition,
            offset,
            timestamp,
            key: None,
            value: None,
            schema_id: None,
            headers: Vec::new(),
            size_bytes: 0,
            compression: Compression::None,
        }
    }

    #[test]
    fn retains_best_records_in_both_orders() {
        for (order, expected) in [
            (RecordOrder::Newest, vec![50, 40, 30]),
            (RecordOrder::Oldest, vec![10, 20, 30]),
        ] {
            let mut batch = RecordBatch::new(3, order);
            for timestamp in [30, 10, 50, 20, 40] {
                batch.push(record(timestamp, 0, timestamp));
            }
            assert_eq!(
                batch
                    .into_records()
                    .iter()
                    .map(|record| record.timestamp)
                    .collect::<Vec<_>>(),
                expected
            );
        }
    }

    #[test]
    fn timestamp_ties_use_partition_then_offset() {
        for (order, expected) in [
            (RecordOrder::Newest, vec![(0, 3), (0, 1), (1, 4)]),
            (RecordOrder::Oldest, vec![(0, 1), (0, 3), (1, 2)]),
        ] {
            let mut batch = RecordBatch::new(3, order);
            for (partition, offset) in [(1, 2), (0, 1), (1, 4), (0, 3), (2, 0)] {
                batch.push(record(100, partition, offset));
            }
            assert_eq!(
                batch
                    .into_records()
                    .iter()
                    .map(|record| (record.partition, record.offset))
                    .collect::<Vec<_>>(),
                expected
            );
        }
    }

    #[test]
    fn exact_ties_preserve_input_order() {
        for order in [RecordOrder::Newest, RecordOrder::Oldest] {
            let mut batch = RecordBatch::new(3, order);
            for value in ["first", "second", "third", "fourth"] {
                let mut record = record(100, 0, 1);
                record.value = Some(value.into());
                batch.push(record);
            }
            // Replacing the worst tied record must retain the earliest two ties.
            batch.push(record(
                if order == RecordOrder::Newest { 200 } else { 0 },
                0,
                2,
            ));
            assert_eq!(
                batch
                    .into_records()
                    .iter()
                    .map(|record| record.value.as_deref())
                    .collect::<Vec<_>>(),
                vec![None, Some("first"), Some("second")]
            );
        }
    }

    #[test]
    fn empty_and_zero_capacity_batches_are_empty() {
        for order in [RecordOrder::Newest, RecordOrder::Oldest] {
            assert!(RecordBatch::new(10, order).into_records().is_empty());
            let mut batch = RecordBatch::new(0, order);
            for offset in 0..100 {
                batch.push(record(offset, 0, offset));
                assert!(batch.records.is_empty());
            }
            assert!(batch.into_records().is_empty());
        }
    }

    #[test]
    fn large_input_matches_stable_sort_and_stays_bounded() {
        let records: Vec<_> = (0..10_000)
            .map(|index| {
                let mut record = record((index * 7919) % 101, (index % 7) as i32, index % 13);
                record.value = Some(index.to_string());
                record
            })
            .collect();

        for order in [RecordOrder::Newest, RecordOrder::Oldest] {
            let mut sorted = records.clone();
            sorted.sort_by(|left, right| left.cmp_for_order(right, order));
            for limit in [0, 1, 7, 128, records.len(), records.len() + 1] {
                let mut batch = RecordBatch::new(limit, order);
                for (index, record) in records.iter().enumerate() {
                    batch.push(record.clone());
                    assert_eq!(batch.records.len(), limit.min(index + 1));
                }
                assert_eq!(batch.into_records(), sorted[..limit.min(sorted.len())]);
            }
        }
    }
}
