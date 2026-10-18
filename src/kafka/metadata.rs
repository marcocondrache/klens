use std::collections::BTreeMap;
use std::num::{NonZeroU8, NonZeroU16};

use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataSnapshot {
    pub cluster_id: Option<String>,
    pub brokers: Vec<BrokerMetadata>,
    pub topics: Vec<TopicMetadata>,
}

impl MetadataSnapshot {
    #[cfg(test)]
    pub(crate) fn topic(&self, name: &str) -> Option<&TopicMetadata> {
        self.topics.iter().find(|topic| topic.name == name)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BrokerMetadata {
    pub id: i32,
    pub host: String,
    pub port: i32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicMetadata {
    pub name: String,
    pub internal: bool,
    pub partitions: Vec<PartitionMetadata>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionMetadata {
    pub id: i32,
    pub leader: i32,
    pub replicas: Vec<i32>,
    pub isr: Vec<i32>,
}

impl PartitionMetadata {
    pub fn under_replicated(&self) -> bool {
        self.isr.len() < self.replicas.len()
    }

    pub fn offline(&self) -> bool {
        self.leader < 0
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewTopic {
    pub name: String,
    pub partitions: Option<NonZeroU16>,
    pub replication_factor: Option<NonZeroU8>,
    pub configs: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordDeletion {
    pub topic: String,
    pub before: BTreeMap<i32, Option<i64>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Watermarks {
    pub low: i64,
    pub high: i64,
}

pub fn is_internal_topic(name: &str) -> bool {
    name.starts_with('_') || name.starts_with('.')
}

const MAX_TOPIC_NAME_LEN: usize = 249;

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum InvalidTopicName {
    #[error("topic name cannot be empty")]
    Empty,

    #[error("topic name length {0} exceeds maximum of {MAX_TOPIC_NAME_LEN}")]
    TooLong(usize),

    #[error("topic name contains illegal character {0:?}; only [a-zA-Z0-9._-] is allowed")]
    IllegalCharacter(char),
}

/// Kafka's own rule for a topic name. krafka rejects a whole request that
/// names one topic outside it.
pub fn validate_topic_name(name: &str) -> Result<(), InvalidTopicName> {
    if name.is_empty() {
        return Err(InvalidTopicName::Empty);
    }
    if name.len() > MAX_TOPIC_NAME_LEN {
        return Err(InvalidTopicName::TooLong(name.len()));
    }
    match name
        .chars()
        .find(|c| !(c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-')))
    {
        Some(illegal) => Err(InvalidTopicName::IllegalCharacter(illegal)),
        None => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_topic_name_is_checked_against_kafkas_rule() {
        assert_eq!(validate_topic_name("orders.v2_eu-1"), Ok(()));
        assert_eq!(validate_topic_name(""), Err(InvalidTopicName::Empty));
        assert_eq!(validate_topic_name(&"a".repeat(MAX_TOPIC_NAME_LEN)), Ok(()));
        assert_eq!(
            validate_topic_name(&"a".repeat(MAX_TOPIC_NAME_LEN + 1)),
            Err(InvalidTopicName::TooLong(MAX_TOPIC_NAME_LEN + 1))
        );
        assert_eq!(
            validate_topic_name("orders/eu"),
            Err(InvalidTopicName::IllegalCharacter('/'))
        );
        assert_eq!(
            validate_topic_name("örders"),
            Err(InvalidTopicName::IllegalCharacter('ö'))
        );
    }
}
