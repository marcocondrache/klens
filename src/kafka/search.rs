use crate::kafka::broker::Broker;
use crate::kafka::group::{ConsumerGroup, GroupSnapshot};
use crate::kafka::metadata::{BrokerMetadata, TopicMetadata};
use crate::kafka::registry::SchemaSubject;
use crate::kafka::topic::Topic;

const MAX_HITS: usize = 20;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SearchKind {
    Topic,
    Group,
    Node,
    Subject,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchHit {
    pub kind: SearchKind,
    pub id: String,
    pub label: String,
    pub detail: String,
}

pub fn search_catalog(
    term: &str,
    topics: &[TopicMetadata],
    brokers: &[BrokerMetadata],
    groups: &[GroupSnapshot],
    subjects: &[SchemaSubject],
) -> Vec<SearchHit> {
    search_hits(
        term,
        topics
            .iter()
            .map(|topic| (topic.name.as_str(), topic.partitions.len())),
        groups
            .iter()
            .map(|group| (group.id.as_str(), group.state.to_string())),
        brokers
            .iter()
            .map(|broker| (broker.id, broker.host.as_str())),
        subjects,
    )
}

pub fn search_snapshot(
    term: &str,
    topics: &[Topic],
    brokers: &[Broker],
    groups: &[ConsumerGroup],
    subjects: &[SchemaSubject],
) -> Vec<SearchHit> {
    search_hits(
        term,
        topics
            .iter()
            .map(|topic| (topic.name.as_str(), topic.partitions.len())),
        groups
            .iter()
            .map(|group| (group.id.as_str(), group.state.to_string())),
        brokers
            .iter()
            .map(|broker| (broker.id, broker.host.as_str())),
        subjects,
    )
}

fn search_hits<'a>(
    term: &str,
    topics: impl IntoIterator<Item = (&'a str, usize)>,
    groups: impl IntoIterator<Item = (&'a str, String)>,
    brokers: impl IntoIterator<Item = (i32, &'a str)>,
    subjects: &[SchemaSubject],
) -> Vec<SearchHit> {
    let needle = term.trim().to_ascii_lowercase();
    if needle.is_empty() {
        return Vec::new();
    }

    let mut hits = Vec::new();

    for (name, partitions) in topics {
        if name.to_ascii_lowercase().contains(&needle) {
            hits.push(SearchHit {
                kind: SearchKind::Topic,
                id: name.to_owned(),
                label: name.to_owned(),
                detail: format!("{partitions} partitions"),
            });
        }
    }

    for (id, state) in groups {
        if id.to_ascii_lowercase().contains(&needle) {
            hits.push(SearchHit {
                kind: SearchKind::Group,
                id: id.to_owned(),
                label: id.to_owned(),
                detail: state,
            });
        }
    }

    for (id, host) in brokers {
        let haystack = format!("{id} {host}").to_ascii_lowercase();
        if haystack.contains(&needle) {
            hits.push(SearchHit {
                kind: SearchKind::Node,
                id: id.to_string(),
                label: format!("Broker {id}"),
                detail: host.to_owned(),
            });
        }
    }

    for subject in subjects {
        if subject.subject.to_ascii_lowercase().contains(&needle) {
            hits.push(SearchHit {
                kind: SearchKind::Subject,
                id: subject.subject.clone(),
                label: subject.subject.clone(),
                detail: format!("{} · v{}", subject.schema_type, subject.latest_version),
            });
        }
    }

    hits.truncate(MAX_HITS);
    hits
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kafka::group::{GroupMember, GroupState, MemberAssignment};
    use crate::kafka::metadata::PartitionMetadata;
    use crate::kafka::registry::{SchemaCompatibility, SchemaType};

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
    fn filters_topics_groups_and_brokers() {
        let topics = vec![topic(
            "orders.created",
            vec![partition(0, 1, vec![1], vec![1])],
        )];
        let brokers = vec![BrokerMetadata {
            id: 7,
            host: "broker-a".into(),
            port: 9092,
        }];
        let groups = vec![GroupSnapshot {
            id: "order-processor".into(),
            state: GroupState::Stable,
            protocol: "range".into(),
            coordinator: 0,
            members: vec![GroupMember {
                id: "m1".into(),
                client_id: "c1".into(),
                host: "127.0.0.1".into(),
                assignments: vec![MemberAssignment {
                    topic: "orders.created".into(),
                    partitions: vec![0],
                }],
            }],
            committed: Vec::new(),
        }];

        let subjects = vec![SchemaSubject {
            subject: "orders.created-value".into(),
            id: 1,
            schema_type: SchemaType::Avro,
            latest_version: 2,
            versions: vec![1, 2],
            compatibility: SchemaCompatibility::Backward,
            schema: "{}".into(),
        }];

        let hits = search_catalog("order", &topics, &brokers, &groups, &subjects);
        assert_eq!(hits.len(), 3);
        assert!(hits.iter().any(|hit| hit.kind == SearchKind::Topic));
        assert!(hits.iter().any(|hit| hit.kind == SearchKind::Group));
        assert!(hits.iter().any(|hit| hit.kind == SearchKind::Subject));

        let nodes = search_catalog("broker-a", &topics, &brokers, &groups, &[]);
        assert_eq!(nodes[0].kind, SearchKind::Node);

        let assembled_topics = vec![crate::kafka::topic::Topic {
            name: "orders.created".into(),
            internal: false,
            partitions: vec![crate::kafka::topic::Partition {
                id: 0,
                leader: 1,
                replicas: vec![1],
                isr: vec![1],
                low_watermark: 0,
                high_watermark: 1,
            }],
            replication_factor: 1,
            message_count: 1,
            cleanup_policy: crate::kafka::topic_config::CleanupPolicy::Delete,
            retention_ms: 0,
            consumer_groups: Vec::new(),
            under_replicated: false,
        }];
        let assembled_brokers = vec![crate::kafka::broker::Broker {
            id: 7,
            host: "broker-a".into(),
            port: 9092,
            rack: None,
            controller: false,
            partition_count: 1,
            leader_count: 1,
        }];
        let assembled_groups = vec![crate::kafka::group::ConsumerGroup {
            id: "order-processor".into(),
            state: GroupState::Stable,
            protocol: "range".into(),
            coordinator: 0,
            members: Vec::new(),
            topics: vec!["orders.created".into()],
            lag: 0,
            offsets: Vec::new(),
        }];
        assert_eq!(
            search_snapshot(
                "order",
                &assembled_topics,
                &assembled_brokers,
                &assembled_groups,
                &subjects,
            ),
            hits
        );
        assert_eq!(
            search_snapshot(
                "broker-a",
                &assembled_topics,
                &assembled_brokers,
                &assembled_groups,
                &[]
            ),
            nodes
        );
    }

    #[test]
    fn blank_term_matches_nothing() {
        let topics = vec![topic("orders", vec![partition(0, 1, vec![1], vec![1])])];
        assert!(search_catalog("   ", &topics, &[], &[], &[]).is_empty());
        assert!(search_snapshot("   ", &[], &[], &[], &[]).is_empty());
    }
}
