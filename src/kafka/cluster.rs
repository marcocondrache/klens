use crate::config::{ClusterConfig, SecurityProtocol};
use crate::kafka::metadata::{MetadataSnapshot, PartitionMetadata};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterIdentity {
    pub name: String,
    pub bootstrap_servers: Vec<String>,
    pub security_protocol: SecurityProtocol,
}

impl From<&ClusterConfig> for ClusterIdentity {
    fn from(config: &ClusterConfig) -> Self {
        Self {
            name: config.name.trim().to_owned(),
            bootstrap_servers: config.bootstrap_servers.clone(),
            security_protocol: config
                .security
                .as_ref()
                .map(|security| security.protocol)
                .unwrap_or(SecurityProtocol::Plaintext),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClusterHealth {
    Healthy,
    Degraded,
    Offline,
}

impl ClusterHealth {
    pub fn from_partitions<'a>(
        partitions: impl IntoIterator<Item = &'a PartitionMetadata>,
    ) -> Self {
        if partitions
            .into_iter()
            .any(|partition| partition.under_replicated() || partition.offline())
        {
            Self::Degraded
        } else {
            Self::Healthy
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterOverview {
    pub identity: ClusterIdentity,
    pub cluster_id: String,
    pub health: ClusterHealth,
    pub broker_count: i32,
    pub topic_count: i32,
    pub partition_count: i32,
    pub consumer_group_count: i32,
    pub under_replicated_partitions: i32,
    pub offline_partitions: i32,
    pub message_count: u64,
}

impl ClusterOverview {
    pub fn assemble(identity: ClusterIdentity, meta: &MetadataSnapshot, group_count: i32) -> Self {
        let mut partition_count = 0;
        let mut under_replicated = 0;
        let mut offline = 0;
        for partition in meta.partitions() {
            partition_count += 1;
            under_replicated += i32::from(partition.under_replicated());
            offline += i32::from(partition.offline());
        }

        Self {
            identity,
            cluster_id: meta.cluster_id.clone().unwrap_or_default(),
            health: ClusterHealth::from_partitions(meta.partitions()),
            broker_count: meta.brokers.len() as i32,
            topic_count: meta.topics.len() as i32,
            partition_count,
            consumer_group_count: group_count,
            under_replicated_partitions: under_replicated,
            offline_partitions: offline,
            message_count: 0,
        }
    }

    pub fn offline(identity: ClusterIdentity) -> Self {
        Self {
            identity,
            cluster_id: String::new(),
            health: ClusterHealth::Offline,
            broker_count: 0,
            topic_count: 0,
            partition_count: 0,
            consumer_group_count: 0,
            under_replicated_partitions: 0,
            offline_partitions: 0,
            message_count: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::SecurityProtocol;
    use crate::kafka::metadata::{
        BrokerMetadata, MetadataSnapshot, PartitionMetadata, TopicMetadata,
    };

    fn identity() -> ClusterIdentity {
        ClusterIdentity {
            name: "local".into(),
            bootstrap_servers: vec!["localhost:9092".into()],
            security_protocol: SecurityProtocol::Plaintext,
        }
    }

    fn topic(name: &str, partitions: Vec<PartitionMetadata>) -> TopicMetadata {
        TopicMetadata {
            name: name.into(),
            internal: name.starts_with('_'),
            partitions,
        }
    }

    fn partition(id: i32, leader: i32, replicas: Vec<i32>, isr: Vec<i32>) -> PartitionMetadata {
        PartitionMetadata {
            id,
            leader,
            replicas,
            isr,
        }
    }

    #[test]
    fn health_is_degraded_when_partitions_are_unhealthy() {
        assert_eq!(
            ClusterHealth::from_partitions(&[partition(0, 1, vec![1], vec![1])]),
            ClusterHealth::Healthy
        );
        assert_eq!(
            ClusterHealth::from_partitions(&[partition(0, 1, vec![1, 2], vec![1])]),
            ClusterHealth::Degraded
        );
        assert_eq!(
            ClusterHealth::from_partitions(&[partition(0, -1, vec![1], vec![])]),
            ClusterHealth::Degraded
        );
    }

    #[test]
    fn overview_counts_partition_problems() {
        let meta = MetadataSnapshot {
            cluster_id: Some("abc".into()),
            brokers: vec![BrokerMetadata {
                id: 1,
                host: "localhost".into(),
                port: 9092,
            }],
            topics: vec![topic(
                "orders",
                vec![
                    partition(0, 1, vec![1, 2], vec![1]),
                    partition(1, -1, vec![1], vec![]),
                ],
            )],
        };

        let overview = ClusterOverview::assemble(identity(), &meta, 3);
        assert_eq!(overview.cluster_id, "abc");
        assert_eq!(overview.health, ClusterHealth::Degraded);
        assert_eq!(overview.partition_count, 2);
        assert_eq!(overview.under_replicated_partitions, 2);
        assert_eq!(overview.offline_partitions, 1);
        assert_eq!(overview.consumer_group_count, 3);
    }
}
