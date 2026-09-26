use std::cmp::Ordering;
use std::collections::BinaryHeap;

use super::query::RecordOrder;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SortKey {
    pub timestamp: i64,
    pub partition: i32,
    pub offset: i64,
}

impl SortKey {
    pub fn cmp_for_order(&self, other: &Self, order: RecordOrder) -> Ordering {
        match order {
            RecordOrder::Newest => self
                .timestamp
                .cmp(&other.timestamp)
                .reverse()
                .then(self.partition.cmp(&other.partition))
                .then(self.offset.cmp(&other.offset).reverse()),
            RecordOrder::Oldest => self
                .timestamp
                .cmp(&other.timestamp)
                .then(self.partition.cmp(&other.partition))
                .then(self.offset.cmp(&other.offset)),
        }
    }
}

pub struct RecordBatch<T> {
    entries: BinaryHeap<Entry<T>>,
    limit: usize,
    order: RecordOrder,
    seen: usize,
}

impl<T> RecordBatch<T> {
    pub fn new(limit: usize, order: RecordOrder) -> Self {
        Self {
            entries: BinaryHeap::new(),
            limit,
            order,
            seen: 0,
        }
    }

    pub fn is_full(&self) -> bool {
        self.entries.len() >= self.limit
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn displaced(&self) -> usize {
        self.seen - self.entries.len()
    }

    pub fn admits(&self, key: &SortKey) -> bool {
        if self.limit == 0 {
            return false;
        }
        if !self.is_full() {
            return true;
        }
        self.entries
            .peek()
            .is_none_or(|worst| key.cmp_for_order(&worst.key, self.order) == Ordering::Less)
    }

    pub fn push(&mut self, key: SortKey, value: T) {
        if self.limit == 0 {
            return;
        }

        let entry = Entry {
            key,
            value,
            order: self.order,
            sequence: self.seen,
        };
        self.seen += 1;

        if self.entries.len() < self.limit {
            self.entries.push(entry);
        } else if let Some(mut worst) = self.entries.peek_mut()
            && entry < *worst
        {
            *worst = entry;
        }
    }

    pub fn into_sorted(self) -> Vec<T> {
        self.entries
            .into_sorted_vec()
            .into_iter()
            .map(|entry| entry.value)
            .collect()
    }
}

struct Entry<T> {
    key: SortKey,
    value: T,
    order: RecordOrder,
    sequence: usize,
}

impl<T> Ord for Entry<T> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.key
            .cmp_for_order(&other.key, self.order)
            .then(self.sequence.cmp(&other.sequence))
    }
}

impl<T> PartialOrd for Entry<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<T> PartialEq for Entry<T> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl<T> Eq for Entry<T> {}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(timestamp: i64, partition: i32, offset: i64) -> SortKey {
        SortKey {
            timestamp,
            partition,
            offset,
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
                batch.push(key(timestamp, 0, timestamp), timestamp);
            }
            assert_eq!(batch.into_sorted(), expected);
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
                batch.push(key(100, partition, offset), (partition, offset));
            }
            assert_eq!(batch.into_sorted(), expected);
        }
    }

    #[test]
    fn exact_ties_preserve_input_order() {
        for order in [RecordOrder::Newest, RecordOrder::Oldest] {
            let mut batch = RecordBatch::new(3, order);
            for value in ["first", "second", "third", "fourth"] {
                batch.push(key(100, 0, 1), value);
            }
            let better = if order == RecordOrder::Newest { 200 } else { 0 };
            batch.push(key(better, 0, 2), "better");
            assert_eq!(batch.into_sorted(), vec!["better", "first", "second"]);
        }
    }

    #[test]
    fn empty_and_zero_capacity_batches_are_empty() {
        for order in [RecordOrder::Newest, RecordOrder::Oldest] {
            assert!(RecordBatch::<i64>::new(10, order).into_sorted().is_empty());
            let mut batch = RecordBatch::new(0, order);
            for offset in 0..100 {
                batch.push(key(offset, 0, offset), offset);
                assert!(batch.entries.is_empty());
            }
            assert!(!batch.admits(&key(0, 0, 0)));
            assert!(batch.into_sorted().is_empty());
        }
    }

    #[test]
    fn a_full_batch_counts_what_it_let_go() {
        let mut batch = RecordBatch::new(2, RecordOrder::Newest);
        assert!(batch.is_empty());

        batch.push(key(10, 0, 1), "a");
        assert!(!batch.is_empty());
        assert_eq!(batch.displaced(), 0);

        batch.push(key(20, 0, 2), "b");
        batch.push(key(30, 0, 3), "c");
        batch.push(key(5, 0, 4), "d");

        assert_eq!(batch.displaced(), 2, "one evicted, one never kept");
        assert_eq!(batch.into_sorted(), vec!["c", "b"]);
    }

    #[test]
    fn an_underfilled_batch_admits_everything() {
        let mut batch = RecordBatch::new(2, RecordOrder::Newest);
        assert!(batch.admits(&key(0, 0, 0)));
        batch.push(key(100, 0, 1), "a");
        assert!(batch.admits(&key(0, 0, 0)));
        assert!(!batch.is_full());
    }

    #[test]
    fn a_full_batch_only_admits_records_better_than_its_worst() {
        let mut batch = RecordBatch::new(2, RecordOrder::Newest);
        batch.push(key(100, 0, 1), "a");
        batch.push(key(90, 0, 2), "b");

        assert!(batch.is_full());
        assert!(batch.admits(&key(95, 0, 3)), "newer than the worst kept");
        assert!(!batch.admits(&key(80, 0, 4)), "older than the worst kept");
        assert!(
            !batch.admits(&key(90, 0, 2)),
            "a tie loses to the record already held"
        );
    }

    #[test]
    fn admits_follows_the_batch_order() {
        let mut batch = RecordBatch::new(1, RecordOrder::Oldest);
        batch.push(key(100, 0, 1), "a");

        assert!(batch.admits(&key(50, 0, 2)));
        assert!(!batch.admits(&key(150, 0, 2)));
    }

    #[test]
    fn large_input_matches_stable_sort_and_stays_bounded() {
        let records: Vec<(SortKey, usize)> = (0..10_000)
            .map(|index| {
                (
                    key((index * 7919) % 101, (index % 7) as i32, index % 13),
                    index as usize,
                )
            })
            .collect();

        for order in [RecordOrder::Newest, RecordOrder::Oldest] {
            let mut sorted = records.clone();
            sorted.sort_by(|left, right| left.0.cmp_for_order(&right.0, order));
            let expected: Vec<usize> = sorted.iter().map(|(_, index)| *index).collect();

            for limit in [0, 1, 7, 128, records.len(), records.len() + 1] {
                let mut batch = RecordBatch::new(limit, order);
                for (index, (key, value)) in records.iter().enumerate() {
                    batch.push(*key, *value);
                    assert_eq!(batch.entries.len(), limit.min(index + 1));
                }
                assert_eq!(batch.into_sorted(), expected[..limit.min(expected.len())]);
            }
        }
    }
}
