use crate::kafka::broker::Broker;
use crate::kafka::group::ConsumerGroup;
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
    use crate::kafka::group::GroupState;
    use crate::kafka::registry::{SchemaCompatibility, SchemaType};
    use crate::kafka::topic::Partition;
    use crate::kafka::topic_config::CleanupPolicy;

    fn topic(name: &str) -> Topic {
        Topic {
            name: name.into(),
            internal: false,
            partitions: vec![Partition {
                id: 0,
                leader: 1,
                replicas: vec![1],
                isr: vec![1],
                low_watermark: 0,
                high_watermark: 1,
            }],
            replication_factor: 1,
            message_count: 1,
            cleanup_policy: CleanupPolicy::Delete,
            retention_ms: 0,
            consumer_groups: Vec::new(),
            under_replicated: false,
        }
    }

    fn broker(id: i32, host: &str) -> Broker {
        Broker {
            id,
            host: host.into(),
            port: 9092,
            rack: None,
            controller: false,
            partition_count: 1,
            leader_count: 1,
        }
    }

    fn group(id: &str) -> ConsumerGroup {
        ConsumerGroup {
            id: id.into(),
            state: GroupState::Stable,
            protocol: "range".into(),
            coordinator: 0,
            members: Vec::new(),
            topics: vec!["orders.created".into()],
            lag: 0,
            offsets: Vec::new(),
        }
    }

    #[test]
    fn filters_topics_groups_and_brokers() {
        let topics = vec![topic("orders.created")];
        let brokers = vec![broker(7, "broker-a")];
        let groups = vec![group("order-processor")];
        let subjects = vec![SchemaSubject {
            subject: "orders.created-value".into(),
            id: 1,
            schema_type: SchemaType::Avro,
            latest_version: 2,
            versions: vec![1, 2],
            compatibility: SchemaCompatibility::Backward,
            schema: "{}".into(),
        }];

        let hits = search_snapshot("order", &topics, &brokers, &groups, &subjects);
        assert_eq!(hits.len(), 3);
        assert!(hits.iter().any(|hit| hit.kind == SearchKind::Topic));
        assert!(hits.iter().any(|hit| hit.kind == SearchKind::Group));
        assert!(hits.iter().any(|hit| hit.kind == SearchKind::Subject));

        let nodes = search_snapshot("broker-a", &topics, &brokers, &groups, &[]);
        assert_eq!(nodes[0].kind, SearchKind::Node);
    }

    #[test]
    fn blank_term_matches_nothing() {
        assert!(search_snapshot("   ", &[topic("orders")], &[], &[], &[]).is_empty());
        assert!(search_snapshot("   ", &[], &[], &[], &[]).is_empty());
    }
}
