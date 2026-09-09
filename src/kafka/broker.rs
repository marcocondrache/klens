use std::collections::HashMap;

use crate::kafka::metadata::{BrokerMetadata, MetadataSnapshot};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Broker {
    pub id: i32,
    pub host: String,
    pub port: i32,
    pub rack: Option<String>,
    pub controller: bool,
    pub partition_count: i32,
    pub leader_count: i32,
}

impl Broker {
    pub fn assemble(
        broker: &BrokerMetadata,
        partition_counts: &HashMap<i32, i32>,
        leader_counts: &HashMap<i32, i32>,
    ) -> Self {
        Self {
            id: broker.id,
            host: broker.host.clone(),
            port: broker.port,
            rack: None,
            controller: false,
            partition_count: partition_counts.get(&broker.id).copied().unwrap_or(0),
            leader_count: leader_counts.get(&broker.id).copied().unwrap_or(0),
        }
    }

    pub fn assemble_all(meta: &MetadataSnapshot) -> Vec<Self> {
        let mut partition_counts = HashMap::<i32, i32>::new();
        let mut leader_counts = HashMap::<i32, i32>::new();

        for partition in meta.partitions() {
            for replica in &partition.replicas {
                *partition_counts.entry(*replica).or_default() += 1;
            }
            if !partition.offline() {
                *leader_counts.entry(partition.leader).or_default() += 1;
            }
        }

        meta.brokers
            .iter()
            .map(|broker| Self::assemble(broker, &partition_counts, &leader_counts))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kafka::metadata::{MetadataSnapshot, PartitionMetadata, TopicMetadata};

    #[test]
    fn counts_replicas_and_leaders_per_broker() {
        let meta = MetadataSnapshot {
            cluster_id: None,
            brokers: vec![
                BrokerMetadata {
                    id: 1,
                    host: "broker-a".into(),
                    port: 9092,
                },
                BrokerMetadata {
                    id: 2,
                    host: "broker-b".into(),
                    port: 9092,
                },
            ],
            topics: vec![TopicMetadata {
                name: "orders".into(),
                internal: false,
                partitions: vec![
                    PartitionMetadata {
                        id: 0,
                        leader: 1,
                        replicas: vec![1, 2],
                        isr: vec![1, 2],
                    },
                    // Offline: contributes replicas but no leader.
                    PartitionMetadata {
                        id: 1,
                        leader: -1,
                        replicas: vec![2],
                        isr: vec![],
                    },
                ],
            }],
        };

        let brokers = Broker::assemble_all(&meta);
        assert_eq!(brokers[0].partition_count, 1);
        assert_eq!(brokers[0].leader_count, 1);
        assert_eq!(brokers[1].partition_count, 2);
        assert_eq!(brokers[1].leader_count, 0);
    }

    #[test]
    fn unknown_broker_counts_default_to_zero() {
        let broker = Broker::assemble(
            &BrokerMetadata {
                id: 9,
                host: "broker-z".into(),
                port: 9092,
            },
            &HashMap::new(),
            &HashMap::new(),
        );
        assert_eq!(broker.partition_count, 0);
        assert_eq!(broker.leader_count, 0);
    }
}
