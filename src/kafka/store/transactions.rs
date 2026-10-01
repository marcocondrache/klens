use std::collections::BTreeSet;
use std::sync::Arc;

use foldhash::{HashMap, HashSet};

use crate::kafka::transaction::{
    PartitionProducers, TransactionDescription, is_authorization_error,
};

use super::tables::Topology;

pub const DEFAULT_MAX_TIMEOUT_MS: i64 = 15 * 60 * 1000;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpenPartition {
    pub topic: Arc<str>,
    pub partition: i32,
    /// Unset as -1 when topology does not know the partition.
    pub leader: i32,
    pub producer_id: i64,
    pub producer_epoch: i32,
    pub open_offset: i64,
    pub last_timestamp_ms: Option<i64>,
    pub max_timeout_ms: i64,
    pub seen_before: bool,
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct LeaderScan {
    pub partitions: usize,
    pub open: Vec<OpenPartition>,
    pub denied: BTreeSet<Arc<str>>,
    pub unchecked: usize,
}

impl LeaderScan {
    pub fn of(topology: &Topology, described: &[PartitionProducers]) -> Self {
        let partitions = topology.partition_count() as usize;
        let mut scan = Self {
            partitions,
            unchecked: partitions.saturating_sub(described.len()),
            ..Self::default()
        };

        for entry in described {
            let topic = topology.intern_topic(&entry.topic);
            match &entry.error {
                Some(error) if is_authorization_error(error) => {
                    scan.denied.insert(topic);
                }
                Some(_) => scan.unchecked += 1,
                None => {
                    let leader = leader(topology, &entry.topic, entry.partition);
                    scan.open
                        .extend(entry.producers.iter().filter_map(|producer| {
                            Some(OpenPartition {
                                topic: Arc::clone(&topic),
                                partition: entry.partition,
                                leader,
                                producer_id: producer.producer_id,
                                producer_epoch: producer.producer_epoch,
                                open_offset: producer.open_offset?,
                                last_timestamp_ms: producer.last_timestamp_ms,
                                max_timeout_ms: DEFAULT_MAX_TIMEOUT_MS,
                                seen_before: false,
                            })
                        }));
                }
            }
        }
        scan.open.sort_by(|left, right| {
            (&left.topic, left.partition, left.producer_id).cmp(&(
                &right.topic,
                right.partition,
                right.producer_id,
            ))
        });
        scan
    }

    pub fn producer_ids(&self) -> BTreeSet<i64> {
        self.open.iter().map(|open| open.producer_id).collect()
    }

    pub fn leaders(&self) -> BTreeSet<i32> {
        self.open.iter().map(|open| open.leader).collect()
    }
}

fn leader(topology: &Topology, topic: &str, partition: i32) -> i32 {
    topology
        .topics
        .get(topic)
        .and_then(|topic| topic.partitions.iter().find(|meta| meta.id == partition))
        .map_or(-1, |meta| meta.leader)
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TransactionTable {
    pub transactions: Vec<TransactionDescription>,
    pub open_partitions: Vec<OpenPartition>,
    pub partition_count: usize,
    pub denied_topics: Vec<Arc<str>>,
    pub unchecked_partitions: usize,
}

impl TransactionTable {
    pub fn assemble(
        described: Vec<TransactionDescription>,
        scan: LeaderScan,
        max_timeouts: &HashMap<i32, i64>,
        previous: Option<&TransactionTable>,
    ) -> Self {
        let mut transactions: Vec<TransactionDescription> = described
            .into_iter()
            .filter(|transaction| transaction.error.is_none())
            .collect();
        transactions.sort_by(|left, right| left.transactional_id.cmp(&right.transactional_id));
        transactions.dedup_by(|left, right| left.transactional_id == right.transactional_id);

        let seen: HashSet<(&str, i32, i64, i64)> = previous
            .map(|previous| {
                previous
                    .open_partitions
                    .iter()
                    .map(|open| {
                        (
                            &*open.topic,
                            open.partition,
                            open.producer_id,
                            open.open_offset,
                        )
                    })
                    .collect()
            })
            .unwrap_or_default();
        let open_partitions = scan
            .open
            .into_iter()
            .map(|open| OpenPartition {
                max_timeout_ms: max_timeouts
                    .get(&open.leader)
                    .copied()
                    .unwrap_or(DEFAULT_MAX_TIMEOUT_MS),
                seen_before: seen.contains(&(
                    &*open.topic,
                    open.partition,
                    open.producer_id,
                    open.open_offset,
                )),
                ..open
            })
            .collect();

        Self {
            transactions,
            open_partitions,
            partition_count: scan.partitions,
            denied_topics: scan.denied.into_iter().collect(),
            unchecked_partitions: scan.unchecked,
        }
    }

    pub fn open(&self) -> impl Iterator<Item = &TransactionDescription> {
        self.transactions
            .iter()
            .filter(|transaction| transaction.state.is_open())
    }

    pub fn coordinator(&self, producer_id: i64) -> Option<&TransactionDescription> {
        self.transactions
            .iter()
            .find(|transaction| transaction.producer_id == producer_id)
    }

    pub fn open_partition_count(&self) -> usize {
        self.open_partitions
            .iter()
            .map(|open| (&open.topic, open.partition))
            .collect::<HashSet<_>>()
            .len()
    }

    pub fn past_timeout_on(&self, topic: &str, partition: i32, now_ms: i64) -> bool {
        self.open().any(|transaction| {
            transaction.includes(topic, partition) && transaction.past_timeout(now_ms)
        })
    }
}

#[cfg(test)]
mod tests;
