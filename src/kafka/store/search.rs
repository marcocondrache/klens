use std::sync::Arc;

use crate::kafka::search::{SearchHit, SearchKind};

use super::tables::{SubjectTable, Topology};

const MAX_HITS: usize = 20;

#[derive(Debug, Clone, Default)]
pub struct SearchIndex {
    topics: Vec<(Arc<str>, usize)>,
    groups: Vec<(Arc<str>, String)>,
    brokers: Vec<(i32, String)>,
    subjects: Vec<(Arc<str>, String)>,
}

impl SearchIndex {
    pub fn rebuild(topology: Option<&Topology>, subjects: Option<&SubjectTable>) -> Self {
        let mut index = Self::default();
        if let Some(topology) = topology {
            index.topics = topology
                .topics
                .iter()
                .map(|(name, topic)| (Arc::clone(name), topic.partitions.len()))
                .collect();
            index.groups = topology
                .groups
                .iter()
                .map(|(id, group)| (Arc::clone(id), group.state.to_string()))
                .collect();
            index.brokers = topology
                .brokers
                .iter()
                .map(|(id, broker)| (*id, broker.host.clone()))
                .collect();
        }
        if let Some(subjects) = subjects {
            index.subjects = subjects
                .subjects
                .iter()
                .map(|(name, info)| {
                    (
                        Arc::clone(name),
                        format!("{} · v{}", info.schema_type, info.latest_version),
                    )
                })
                .collect();
        }
        index
    }

    pub fn search(&self, term: &str) -> Vec<SearchHit> {
        let needle = term.trim().to_ascii_lowercase();
        if needle.is_empty() {
            return Vec::new();
        }

        let mut hits = Vec::new();
        for (name, partitions) in &self.topics {
            if name.to_ascii_lowercase().contains(&needle) {
                hits.push(SearchHit {
                    kind: SearchKind::Topic,
                    id: name.to_string(),
                    label: name.to_string(),
                    detail: format!("{partitions} partitions"),
                });
            }
        }
        for (id, state) in &self.groups {
            if id.to_ascii_lowercase().contains(&needle) {
                hits.push(SearchHit {
                    kind: SearchKind::Group,
                    id: id.to_string(),
                    label: id.to_string(),
                    detail: state.clone(),
                });
            }
        }
        for (id, host) in &self.brokers {
            let haystack = format!("{id} {host}").to_ascii_lowercase();
            if haystack.contains(&needle) {
                hits.push(SearchHit {
                    kind: SearchKind::Node,
                    id: id.to_string(),
                    label: format!("Broker {id}"),
                    detail: host.clone(),
                });
            }
        }
        for (name, detail) in &self.subjects {
            if name.to_ascii_lowercase().contains(&needle) {
                hits.push(SearchHit {
                    kind: SearchKind::Subject,
                    id: name.to_string(),
                    label: name.to_string(),
                    detail: detail.clone(),
                });
            }
        }
        hits.truncate(MAX_HITS);
        hits
    }
}
