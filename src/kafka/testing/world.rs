use foldhash::HashMap;

use super::fixtures::{metadata, partition, subject, topic};
use super::records::{FixtureRecord, ORDERS};
use crate::kafka::acl::{
    Acl, AclListing, AclOperation, AclPatternType, AclPermission, AclResourceType,
};
use crate::kafka::group::{
    CommittedOffset, GroupMember, GroupSnapshot, GroupState, MemberAssignment,
};
use crate::kafka::metadata::{MetadataSnapshot, Watermarks};
use crate::kafka::quota::{ClientQuota, QuotaEntity, QuotaEntityType, QuotaListing, QuotaValues};
use crate::kafka::registry::SchemaSubject;
use crate::kafka::scram::{ScramCredential, ScramListing, ScramMechanism, ScramUser};
use crate::kafka::storage::{LogDir, ReplicaLog};
use crate::kafka::topic_config::{ConfigEntry, ConfigSource};

pub(super) struct LocalWorld {
    pub metadata: MetadataSnapshot,
    pub watermarks: HashMap<String, HashMap<i32, Watermarks>>,
    pub topic_configs: HashMap<String, Vec<ConfigEntry>>,
    pub broker_configs: HashMap<i32, Vec<ConfigEntry>>,
    pub log_dirs: Vec<LogDir>,
    pub groups: Vec<GroupSnapshot>,
    pub records: Vec<FixtureRecord>,
    pub subjects: Vec<SchemaSubject>,
    pub acls: AclListing,
    pub quotas: QuotaListing,
    pub scram_users: ScramListing,
}

pub(super) fn local() -> LocalWorld {
    let partitions = || {
        vec![
            partition(0, vec![1], vec![1]),
            partition(1, vec![1], vec![1]),
        ]
    };
    let marks = Watermarks { low: 0, high: 8 };

    LocalWorld {
        metadata: metadata(vec![topic(ORDERS, partitions())]),
        watermarks: HashMap::from_iter([(
            ORDERS.into(),
            HashMap::from_iter([(0, marks), (1, marks)]),
        )]),
        topic_configs: HashMap::from_iter([(
            ORDERS.into(),
            vec![
                default_config("cleanup.policy", "delete"),
                default_config("retention.ms", "604800000"),
            ],
        )]),
        broker_configs: HashMap::from_iter([(
            1,
            vec![default_config("log.retention.hours", "168")],
        )]),
        log_dirs: vec![LogDir {
            broker: 1,
            path: "/var/lib/kafka/data".into(),
            error: None,
            total_bytes: Some(1_000_000),
            usable_bytes: Some(750_000),
            cordoned: false,
            replicas: vec![replica(0, 4_096), replica(1, 2_048)],
        }],
        groups: vec![GroupSnapshot {
            id: "order-processor".into(),
            state: GroupState::Stable,
            protocol: "range".into(),
            members: vec![GroupMember {
                id: "member-1".into(),
                client_id: "orders".into(),
                host: "127.0.0.1".into(),
                assignments: vec![MemberAssignment {
                    topic: ORDERS.into(),
                    partitions: vec![0, 1],
                }],
            }],
            committed: vec![committed(0, 6), committed(1, 5)],
        }],
        records: (0..8)
            .map(|offset| {
                FixtureRecord::order(i32::from(offset % 2 == 0), i64::from(offset))
                    .at(1_700_000_000_000 + i64::from(offset) * 1_000)
                    .key(format!("ord_{offset}"))
                    .value(format!(r#"{{"orderId":"ord_{offset}"}}"#))
                    .header("source", "checkout")
            })
            .collect(),
        subjects: vec![subject("orders.created-value", 1, 2)],
        acls: AclListing::Enabled(local_acls()),
        quotas: QuotaListing::Described(local_quotas()),
        scram_users: ScramListing::Described(local_scram_users()),
    }
}

fn default_config(name: &str, value: &str) -> ConfigEntry {
    ConfigEntry {
        name: name.into(),
        value: Some(value.into()),
        source: ConfigSource::Default,
        read_only: false,
        sensitive: false,
    }
}

fn replica(partition: i32, size_bytes: i64) -> ReplicaLog {
    ReplicaLog {
        topic: ORDERS.into(),
        partition,
        size_bytes,
        future: false,
    }
}

fn committed(partition: i32, offset: i64) -> CommittedOffset {
    CommittedOffset {
        topic: ORDERS.into(),
        partition,
        offset,
    }
}

fn local_acls() -> Vec<Acl> {
    vec![
        Acl {
            resource_type: AclResourceType::Topic,
            resource_name: ORDERS.into(),
            pattern_type: AclPatternType::Literal,
            principal: "User:alice".into(),
            host: "*".into(),
            operation: AclOperation::Read,
            permission: AclPermission::Allow,
        },
        Acl {
            resource_type: AclResourceType::Topic,
            resource_name: "orders.".into(),
            pattern_type: AclPatternType::Prefixed,
            principal: "User:eve".into(),
            host: "10.0.0.1".into(),
            operation: AclOperation::Write,
            permission: AclPermission::Deny,
        },
        Acl {
            resource_type: AclResourceType::Group,
            resource_name: "order-processor".into(),
            pattern_type: AclPatternType::Literal,
            principal: "User:order-processor".into(),
            host: "*".into(),
            operation: AclOperation::Read,
            permission: AclPermission::Allow,
        },
    ]
}

fn local_quotas() -> Vec<ClientQuota> {
    vec![
        quota(
            &[(QuotaEntityType::User, Some("alice"))],
            QuotaValues {
                producer_byte_rate: Some(1_048_576.0),
                consumer_byte_rate: Some(2_097_152.0),
                ..QuotaValues::default()
            },
        ),
        quota(
            &[
                (QuotaEntityType::User, Some("alice")),
                (QuotaEntityType::ClientId, Some("checkout")),
            ],
            QuotaValues {
                producer_byte_rate: Some(524_288.0),
                ..QuotaValues::default()
            },
        ),
        quota(
            &[(QuotaEntityType::User, None)],
            QuotaValues {
                request_percentage: Some(50.0),
                controller_mutation_rate: Some(10.0),
                ..QuotaValues::default()
            },
        ),
        quota(
            &[(QuotaEntityType::ClientId, None)],
            QuotaValues {
                consumer_byte_rate: Some(1_048_576.0),
                ..QuotaValues::default()
            },
        ),
        quota(
            &[(QuotaEntityType::Ip, Some("10.0.0.7"))],
            QuotaValues {
                connection_creation_rate: Some(20.0),
                ..QuotaValues::default()
            },
        ),
    ]
}

fn quota(entity: &[(QuotaEntityType, Option<&str>)], values: QuotaValues) -> ClientQuota {
    ClientQuota {
        entity: entity
            .iter()
            .map(|(entity_type, name)| QuotaEntity {
                entity_type: *entity_type,
                name: name.map(str::to_owned),
            })
            .collect(),
        values,
    }
}

fn local_scram_users() -> Vec<ScramUser> {
    let credential = |mechanism, iterations| ScramCredential {
        mechanism,
        iterations,
    };
    vec![
        ScramUser {
            name: "alice".into(),
            credentials: vec![
                credential(ScramMechanism::Sha256, 4096),
                credential(ScramMechanism::Sha512, 8192),
            ],
        },
        ScramUser {
            name: "bob".into(),
            credentials: vec![credential(ScramMechanism::Sha512, 4096)],
        },
    ]
}
