use std::collections::BTreeMap;
use std::num::{NonZeroU8, NonZeroU16};

use crate::kafka::error::KafkaError;

const KAFKA_MAX_TOPIC_NAME_LEN: usize = 249;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewTopic {
    name: String,
    pub partitions: Option<NonZeroU16>,
    pub replication_factor: Option<NonZeroU8>,
    pub configs: BTreeMap<String, String>,
}

impl NewTopic {
    pub fn new(name: String) -> Result<Self, KafkaError> {
        if !is_legal_kafka_topic_name(&name) {
            return Err(KafkaError::InvalidTopicName(name));
        }

        Ok(Self {
            name,
            partitions: None,
            replication_factor: None,
            configs: BTreeMap::new(),
        })
    }

    pub fn name(&self) -> &str {
        &self.name
    }
}

fn is_legal_kafka_topic_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= KAFKA_MAX_TOPIC_NAME_LEN
        && name != "."
        && name != ".."
        && name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rejected(name: &str) -> bool {
        matches!(
            NewTopic::new(name.to_owned()),
            Err(KafkaError::InvalidTopicName(rejected)) if rejected == name
        )
    }

    #[test]
    fn a_topic_name_follows_the_broker_rule() {
        assert_eq!(
            NewTopic::new("orders.v2_eu-1".into()).unwrap().name(),
            "orders.v2_eu-1"
        );
        assert_eq!(NewTopic::new("a".repeat(249)).unwrap().name().len(), 249);

        assert!(rejected(""));
        assert!(rejected("."));
        assert!(rejected(".."));
        assert!(rejected("orders created"));
        assert!(rejected("orders/created"));
        assert!(rejected("ordèrs"));
        assert!(rejected(&"a".repeat(250)));
    }
}
