use std::collections::BTreeSet;
use std::sync::Arc;

use foldhash::HashSet;

use crate::kafka::store::{OpenPartition, TransactionTable};
use crate::kafka::transaction::TransactionDescription;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HangingReason {
    /// Open for longer than the leader's `transaction.max.timeout.ms`.
    PastTimeout,
    /// The producer's coordinator holds no open transaction on the partition.
    UnknownToCoordinator,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HangingPartition {
    pub topic: Arc<str>,
    pub partition: i32,
    pub producer_id: i64,
    pub producer_epoch: i32,
    /// `read_committed` consumers stop here.
    pub open_offset: i64,
    pub open_since_ms: Option<i64>,
    pub transactional_id: Option<String>,
    pub reason: HangingReason,
}

/// The partitions whose open transaction will never finish, the way
/// `kafka-transactions.sh find-hanging` finds them.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Hanging {
    /// Sorted by topic, partition and producer.
    pub partitions: Vec<HangingPartition>,
    /// Producers with an open transaction that no coordinator lists. klens
    /// cannot tell an unknown producer from one whose transactional id it may
    /// not describe, so these count only once past the timeout.
    pub unlisted_producers: usize,
}

impl Hanging {
    pub fn of(table: &TransactionTable, now_ms: i64) -> Self {
        let mut hanging = Self::default();
        let mut unlisted = BTreeSet::new();
        for open in &table.open_partitions {
            match classify(open, table.coordinator(open.producer_id), now_ms) {
                Verdict::Healthy => {}
                Verdict::Unlisted => {
                    unlisted.insert(open.producer_id);
                }
                Verdict::Hanging(partition) => hanging.partitions.push(partition),
            }
        }
        hanging.unlisted_producers = unlisted.len();
        hanging
    }

    pub fn partition_count(&self) -> usize {
        self.partitions
            .iter()
            .map(|hanging| (&hanging.topic, hanging.partition))
            .collect::<HashSet<_>>()
            .len()
    }

    pub fn on(&self, topic: &str, partition: i32) -> bool {
        self.partitions
            .iter()
            .any(|hanging| &*hanging.topic == topic && hanging.partition == partition)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Verdict {
    Healthy,
    Unlisted,
    Hanging(HangingPartition),
}

fn classify(
    open: &OpenPartition,
    coordinator: Option<&TransactionDescription>,
    now_ms: i64,
) -> Verdict {
    let known = coordinator.filter(|description| {
        description.state.is_open() && description.includes(&open.topic, open.partition)
    });
    let open_since_ms = match known {
        Some(description) => description.started_at_ms,
        None => open.last_timestamp_ms,
    };
    let past_timeout = open_since_ms.is_some_and(|since| now_ms - since > open.max_timeout_ms);

    let reason = match (past_timeout, known, coordinator) {
        (true, _, _) => HangingReason::PastTimeout,
        (false, Some(_), _) => return Verdict::Healthy,
        (false, None, Some(_)) => HangingReason::UnknownToCoordinator,
        (false, None, None) => return Verdict::Unlisted,
    };
    // A transaction that finished between the leader's answer and the
    // coordinator's reads as unknown to the coordinator, so only one still
    // open from the same offset on the next look counts.
    if !open.seen_before {
        return Verdict::Healthy;
    }
    Verdict::Hanging(HangingPartition {
        topic: Arc::clone(&open.topic),
        partition: open.partition,
        producer_id: open.producer_id,
        producer_epoch: open.producer_epoch,
        open_offset: open.open_offset,
        open_since_ms,
        transactional_id: coordinator.map(|description| description.transactional_id.clone()),
        reason,
    })
}

#[cfg(test)]
mod tests;
