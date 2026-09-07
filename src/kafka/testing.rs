use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;

use crate::config::SecurityProtocol;
use crate::kafka::error::KafkaError;
use crate::kafka::model::{
    BrokerMetadata, ClusterIdentity, CommittedOffset, Compression, ConfigEntry, ConfigSource,
    FetchPlan, GroupMember, GroupSnapshot, GroupState, MemberAssignment, MetadataSnapshot,
    PartitionMetadata, Record, RecordHeader, TopicMetadata, Watermarks,
};
use crate::kafka::session::ClusterSession;

#[derive(Debug, Clone)]
pub struct FakeCluster {
    identity: ClusterIdentity,
    metadata: MetadataSnapshot,
    watermarks: HashMap<String, HashMap<i32, Watermarks>>,
    topic_configs: HashMap<String, Vec<ConfigEntry>>,
    broker_configs: HashMap<i32, Vec<ConfigEntry>>,
    groups: Vec<GroupSnapshot>,
    records: Vec<Record>,
}

impl FakeCluster {
    pub fn local() -> Arc<Self> {
        let identity = ClusterIdentity {
            name: "local".into(),
            bootstrap_servers: vec!["localhost:9092".into()],
            security_protocol: SecurityProtocol::Plaintext,
        };

        let metadata = MetadataSnapshot {
            cluster_id: Some("test-cluster".into()),
            brokers: vec![BrokerMetadata {
                id: 1,
                host: "localhost".into(),
                port: 9092,
            }],
            topics: vec![TopicMetadata {
                name: "orders.created".into(),
                internal: false,
                partitions: vec![
                    PartitionMetadata {
                        id: 0,
                        leader: 1,
                        replicas: vec![1],
                        isr: vec![1],
                    },
                    PartitionMetadata {
                        id: 1,
                        leader: 1,
                        replicas: vec![1],
                        isr: vec![1],
                    },
                ],
            }],
        };

        let watermarks = HashMap::from([(
            "orders.created".into(),
            HashMap::from([
                (0, Watermarks { low: 0, high: 8 }),
                (1, Watermarks { low: 0, high: 8 }),
            ]),
        )]);

        let topic_configs = HashMap::from([(
            "orders.created".into(),
            vec![
                ConfigEntry {
                    name: "cleanup.policy".into(),
                    value: Some("delete".into()),
                    source: ConfigSource::Default,
                    read_only: false,
                    sensitive: false,
                },
                ConfigEntry {
                    name: "retention.ms".into(),
                    value: Some("604800000".into()),
                    source: ConfigSource::Default,
                    read_only: false,
                    sensitive: false,
                },
            ],
        )]);

        let broker_configs = HashMap::from([(
            1,
            vec![ConfigEntry {
                name: "log.retention.hours".into(),
                value: Some("168".into()),
                source: ConfigSource::Default,
                read_only: false,
                sensitive: false,
            }],
        )]);

        let groups = vec![GroupSnapshot {
            id: "order-processor".into(),
            state: GroupState::Stable,
            protocol: "range".into(),
            coordinator: 1,
            members: vec![GroupMember {
                id: "member-1".into(),
                client_id: "orders".into(),
                host: "127.0.0.1".into(),
                assignments: vec![MemberAssignment {
                    topic: "orders.created".into(),
                    partitions: vec![0, 1],
                }],
            }],
            committed: vec![
                CommittedOffset {
                    topic: "orders.created".into(),
                    partition: 0,
                    offset: 6,
                },
                CommittedOffset {
                    topic: "orders.created".into(),
                    partition: 1,
                    offset: 5,
                },
            ],
        }];

        let records = (0..8)
            .map(|offset| Record {
                topic: "orders.created".into(),
                partition: i32::from(offset % 2 == 0),
                offset: i64::from(offset),
                timestamp: 1_700_000_000_000 + i64::from(offset) * 1_000,
                key: Some(format!("ord_{offset}")),
                value: Some(format!(r#"{{"orderId":"ord_{offset}"}}"#)),
                headers: vec![RecordHeader {
                    key: "source".into(),
                    value: "checkout".into(),
                }],
                size_bytes: 24,
                compression: Compression::None,
            })
            .collect();

        Arc::new(Self {
            identity,
            metadata,
            watermarks,
            topic_configs,
            broker_configs,
            groups,
            records,
        })
    }

    pub fn named(name: &str) -> Arc<Self> {
        let mut cluster = (*Self::local()).clone();
        cluster.identity.name = name.to_owned();
        Arc::new(cluster)
    }

    pub fn extra_topic(self: &Arc<Self>, name: &str, partitions: i32, high: i64) -> Arc<Self> {
        let mut cluster = (**self).clone();
        cluster.metadata.topics.push(TopicMetadata {
            name: name.to_owned(),
            internal: false,
            partitions: (0..partitions)
                .map(|id| PartitionMetadata {
                    id,
                    leader: 1,
                    replicas: vec![1],
                    isr: vec![1],
                })
                .collect(),
        });
        cluster.watermarks.insert(
            name.to_owned(),
            (0..partitions)
                .map(|id| (id, Watermarks { low: 0, high }))
                .collect(),
        );
        Arc::new(cluster)
    }
}

#[async_trait]
impl ClusterSession for FakeCluster {
    fn identity(&self) -> &ClusterIdentity {
        &self.identity
    }

    async fn metadata(&self) -> Result<MetadataSnapshot, KafkaError> {
        Ok(self.metadata.clone())
    }

    async fn watermarks(
        &self,
        topic: &str,
        partitions: &[i32],
    ) -> Result<HashMap<i32, Watermarks>, KafkaError> {
        Ok(partitions
            .iter()
            .filter_map(|partition| {
                self.watermarks
                    .get(topic)
                    .and_then(|marks| marks.get(partition).copied())
                    .map(|marks| (*partition, marks))
            })
            .collect())
    }

    async fn topic_configs(
        &self,
        topics: &[&str],
    ) -> Result<HashMap<String, Vec<ConfigEntry>>, KafkaError> {
        Ok(topics
            .iter()
            .filter_map(|topic| {
                self.topic_configs
                    .get(*topic)
                    .cloned()
                    .map(|entries| ((*topic).to_owned(), entries))
            })
            .collect())
    }

    async fn broker_configs(&self, broker_id: i32) -> Result<Vec<ConfigEntry>, KafkaError> {
        Ok(self
            .broker_configs
            .get(&broker_id)
            .cloned()
            .unwrap_or_default())
    }

    async fn consumer_groups(&self) -> Result<Vec<GroupSnapshot>, KafkaError> {
        Ok(self.groups.clone())
    }

    async fn committed_offsets(
        &self,
        group_id: &str,
        _partitions: &[(String, i32)],
    ) -> Result<Vec<CommittedOffset>, KafkaError> {
        Ok(self
            .groups
            .iter()
            .find(|group| group.id == group_id)
            .map(|group| group.committed.clone())
            .unwrap_or_default())
    }

    async fn records(&self, plan: &FetchPlan) -> Result<Vec<Record>, KafkaError> {
        let mut records: Vec<Record> = self
            .records
            .iter()
            .filter(|record| {
                record.topic == plan.topic
                    && plan.windows.iter().any(|window| {
                        window.partition == record.partition
                            && record.offset >= window.start
                            && record.offset < window.end
                    })
                    && record.matches(&plan.search)
            })
            .cloned()
            .collect();

        records.sort_by(|left, right| match plan.order {
            crate::kafka::model::RecordOrder::Newest => left
                .timestamp
                .cmp(&right.timestamp)
                .reverse()
                .then(left.offset.cmp(&right.offset).reverse()),
            crate::kafka::model::RecordOrder::Oldest => left
                .timestamp
                .cmp(&right.timestamp)
                .then(left.offset.cmp(&right.offset)),
        });
        records.truncate(plan.limit);
        Ok(records)
    }
}
