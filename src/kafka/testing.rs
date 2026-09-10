use std::collections::HashMap;

use async_trait::async_trait;

use crate::config::SecurityProtocol;
use crate::kafka::cluster::ClusterIdentity;
use crate::kafka::error::KafkaError;
use crate::kafka::group::{
    CommittedOffset, GroupMember, GroupSnapshot, GroupState, MemberAssignment,
};
use crate::kafka::metadata::{BrokerMetadata, MetadataSnapshot, PartitionMetadata, TopicMetadata};
use crate::kafka::record::plan::FetchPlan;
use crate::kafka::record::{Compression, Record, RecordHeader};
use crate::kafka::registry::{SchemaCompatibility, SchemaSubject, SchemaType};
use crate::kafka::session::ClusterSession;
use crate::kafka::topic_config::{ConfigEntry, ConfigSource};
use crate::kafka::watermarks::Watermarks;

#[derive(Debug, Clone)]
pub struct FakeCluster {
    identity: ClusterIdentity,
    metadata: MetadataSnapshot,
    watermarks: HashMap<String, HashMap<i32, Watermarks>>,
    topic_configs: HashMap<String, Vec<ConfigEntry>>,
    broker_configs: HashMap<i32, Vec<ConfigEntry>>,
    groups: Vec<GroupSnapshot>,
    records: Vec<Record>,
    subjects: Vec<SchemaSubject>,
}

impl FakeCluster {
    pub fn local() -> Self {
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
                key_schema_id: None,
                value_schema_id: None,
                headers: vec![RecordHeader {
                    key: "source".into(),
                    value: "checkout".into(),
                }],
                size_bytes: 24,
                compression: Compression::None,
            })
            .collect();

        let subjects = vec![SchemaSubject {
            subject: "orders.created-value".into(),
            id: 1,
            schema_type: SchemaType::Avro,
            latest_version: 2,
            versions: vec![1, 2],
            compatibility: SchemaCompatibility::Backward,
            schema:
                r#"{"type":"record","name":"Order","fields":[{"name":"orderId","type":"string"}]}"#
                    .into(),
        }];

        Self {
            identity,
            metadata,
            watermarks,
            topic_configs,
            broker_configs,
            groups,
            records,
            subjects,
        }
    }

    pub fn named(name: &str) -> Self {
        let mut cluster = Self::local();
        cluster.identity.name = name.to_owned();
        cluster
    }

    pub fn extra_topic(&self, name: &str, partitions: i32, high: i64) -> Self {
        let mut cluster = self.clone();
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
        cluster
    }

    pub fn extra_group(&self, group: GroupSnapshot) -> Self {
        let mut cluster = self.clone();
        cluster.groups.push(group);
        cluster
    }

    pub fn with_orders_records(mut self, records: Vec<Record>) -> Self {
        let mut highs = HashMap::<i32, i64>::new();
        for record in &records {
            let high = highs.entry(record.partition).or_insert(0);
            *high = (*high).max(record.offset + 1);
        }

        let mut ids: Vec<i32> = highs.keys().copied().collect();
        ids.sort_unstable();

        if let Some(topic) = self
            .metadata
            .topics
            .iter_mut()
            .find(|topic| topic.name == "orders.created")
        {
            topic.partitions = ids
                .iter()
                .map(|id| PartitionMetadata {
                    id: *id,
                    leader: 1,
                    replicas: vec![1],
                    isr: vec![1],
                })
                .collect();
        }

        self.watermarks.insert(
            "orders.created".into(),
            ids.into_iter()
                .map(|id| {
                    (
                        id,
                        Watermarks {
                            low: 0,
                            high: highs[&id],
                        },
                    )
                })
                .collect(),
        );
        self.records = records;
        self
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

    async fn watermarks(&self, topic: &str) -> Result<HashMap<i32, Watermarks>, KafkaError> {
        Ok(self.watermarks.get(topic).cloned().unwrap_or_default())
    }

    async fn offsets_for_times(
        &self,
        topic: &str,
        partitions: &[i32],
        timestamp: i64,
    ) -> Result<HashMap<i32, Option<i64>>, KafkaError> {
        Ok(partitions
            .iter()
            .map(|partition| {
                let offset = self
                    .records
                    .iter()
                    .filter(|record| {
                        record.topic == topic
                            && record.partition == *partition
                            && record.timestamp >= timestamp
                    })
                    .map(|record| record.offset)
                    .min();
                (*partition, offset)
            })
            .collect())
    }

    async fn topics_configs(
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

        records.sort_by(|left, right| left.cmp_for_order(right, plan.order));
        records.truncate(plan.limit);
        Ok(records)
    }

    async fn schema_subjects(&self) -> Result<Vec<SchemaSubject>, KafkaError> {
        Ok(self.subjects.clone())
    }
}
