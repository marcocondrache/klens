use crate::kafka::group::GroupSnapshot;
use crate::kafka::metadata::{BrokerMetadata, TopicMetadata};
use crate::kafka::registry::SchemaSubject;

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

/// Kinds are searched in a fixed order, so the cap favours topics over
/// groups, brokers and subjects.
pub fn search_catalog(
    term: &str,
    topics: &[TopicMetadata],
    brokers: &[BrokerMetadata],
    groups: &[GroupSnapshot],
    subjects: &[SchemaSubject],
) -> Vec<SearchHit> {
    let needle = term.trim().to_ascii_lowercase();
    if needle.is_empty() {
        return Vec::new();
    }

    let mut hits = Vec::new();

    for topic in topics {
        if topic.name.to_ascii_lowercase().contains(&needle) {
            hits.push(SearchHit {
                kind: SearchKind::Topic,
                id: topic.name.clone(),
                label: topic.name.clone(),
                detail: format!("{} partitions", topic.partitions.len()),
            });
        }
    }

    for group in groups {
        if group.id.to_ascii_lowercase().contains(&needle) {
            hits.push(SearchHit {
                kind: SearchKind::Group,
                id: group.id.clone(),
                label: group.id.clone(),
                detail: group.state.to_string(),
            });
        }
    }

    for broker in brokers {
        let haystack = format!("{} {}", broker.id, broker.host).to_ascii_lowercase();
        if haystack.contains(&needle) {
            hits.push(SearchHit {
                kind: SearchKind::Node,
                id: broker.id.to_string(),
                label: format!("Broker {}", broker.id),
                detail: broker.host.clone(),
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
    }

    #[test]
    fn blank_term_matches_nothing() {
        let topics = vec![topic("orders", vec![partition(0, 1, vec![1], vec![1])])];
        assert!(search_catalog("   ", &topics, &[], &[], &[]).is_empty());
    }
}
