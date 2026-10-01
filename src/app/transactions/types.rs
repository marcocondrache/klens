use jiff::Timestamp;
use serde::Serialize;
use ts_rs::TS;

use crate::kafka::hanging::{self, Hanging};
use crate::kafka::model as domain;
use crate::kafka::store::{Topology, TransactionTable};
use crate::r#macro::from_same_variants;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, TS)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TransactionState {
    Empty,
    Ongoing,
    PrepareCommit,
    PrepareAbort,
    CompleteCommit,
    CompleteAbort,
    Dead,
    PrepareEpochFence,
    Unknown,
}

from_same_variants!(domain::TransactionState => TransactionState {
    Empty,
    Ongoing,
    PrepareCommit,
    PrepareAbort,
    CompleteCommit,
    CompleteAbort,
    Dead,
    PrepareEpochFence,
    Unknown,
});

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, TS)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum HangingReason {
    PastTimeout,
    UnknownToCoordinator,
}

from_same_variants!(hanging::HangingReason => HangingReason {
    PastTimeout,
    UnknownToCoordinator,
});

#[derive(Debug, Clone, PartialEq, Eq, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct TopicPartition {
    pub topic: String,
    pub partition: i32,
}

#[derive(Debug, Clone, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct OpenTransaction {
    pub transactional_id: String,
    pub producer_id: i64,
    pub producer_epoch: i16,
    pub state: TransactionState,
    pub started_at: Option<Timestamp>,
    pub timeout_ms: i32,
    pub past_timeout: bool,
    pub partitions: Vec<TopicPartition>,
}

impl OpenTransaction {
    fn new(transaction: &domain::TransactionDescription, now_ms: i64) -> Self {
        Self {
            transactional_id: transaction.transactional_id.clone(),
            producer_id: transaction.producer_id,
            producer_epoch: transaction.producer_epoch,
            state: transaction.state.into(),
            started_at: transaction.started_at_ms.and_then(timestamp),
            timeout_ms: transaction.timeout_ms,
            past_timeout: transaction.past_timeout(now_ms),
            partitions: transaction
                .partitions
                .iter()
                .map(|(topic, partition)| TopicPartition {
                    topic: topic.clone(),
                    partition: *partition,
                })
                .collect(),
        }
    }
}

#[derive(Debug, Clone, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct HangingPartition {
    pub topic: String,
    pub partition: i32,
    pub producer_id: i64,
    pub producer_epoch: i32,
    pub offset: i64,
    pub open_since: Option<Timestamp>,
    pub transactional_id: Option<String>,
    pub reason: HangingReason,
    pub groups: Vec<String>,
}

impl HangingPartition {
    fn new(hanging: &hanging::HangingPartition, topology: Option<&Topology>) -> Self {
        Self {
            topic: hanging.topic.to_string(),
            partition: hanging.partition,
            producer_id: hanging.producer_id,
            producer_epoch: hanging.producer_epoch,
            offset: hanging.open_offset,
            open_since: hanging.open_since_ms.and_then(timestamp),
            transactional_id: hanging.transactional_id.clone(),
            reason: hanging.reason.into(),
            groups: topology
                .map(|topology| {
                    topology
                        .groups_for_topic(&hanging.topic)
                        .iter()
                        .map(|group| group.to_string())
                        .collect()
                })
                .unwrap_or_default(),
        }
    }
}

#[derive(Debug, Clone, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct TransactionCoverage {
    pub partition_count: i32,
    pub open_partition_count: i32,
    pub denied_topics: Vec<String>,
    pub unchecked_partitions: i32,
    pub unlisted_producers: i32,
}

impl TransactionCoverage {
    fn new(table: &TransactionTable, hanging: &Hanging) -> Self {
        Self {
            partition_count: table.partition_count as i32,
            open_partition_count: table.open_partition_count() as i32,
            denied_topics: table
                .denied_topics
                .iter()
                .map(|topic| topic.to_string())
                .collect(),
            unchecked_partitions: table.unchecked_partitions as i32,
            unlisted_producers: hanging.unlisted_producers as i32,
        }
    }
}

#[derive(Debug, Clone, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct Transactions {
    pub open: Vec<OpenTransaction>,
    pub hanging: Vec<HangingPartition>,
    pub coverage: Option<TransactionCoverage>,
}

impl Transactions {
    pub(crate) fn assemble(
        table: Option<&TransactionTable>,
        topology: Option<&Topology>,
        now: Timestamp,
    ) -> Self {
        let Some(table) = table else {
            return Self {
                open: Vec::new(),
                hanging: Vec::new(),
                coverage: None,
            };
        };
        let now_ms = now.as_millisecond();
        let hanging = Hanging::of(table, now_ms);
        Self {
            open: table
                .open()
                .map(|transaction| OpenTransaction::new(transaction, now_ms))
                .collect(),
            hanging: hanging
                .partitions
                .iter()
                .map(|partition| HangingPartition::new(partition, topology))
                .collect(),
            coverage: Some(TransactionCoverage::new(table, &hanging)),
        }
    }
}

fn timestamp(ms: i64) -> Option<Timestamp> {
    Timestamp::from_millisecond(ms).ok()
}
