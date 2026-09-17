use super::tables::{SubjectTable, Topology};

const MAX_HITS: usize = 20;

/// What a [`SearchHit`] points at.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SearchKind {
    Topic,
    Group,
    Node,
    Subject,
}

/// One typeahead result: what it is, what to navigate to, and a one-line
/// detail the UI shows next to the name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchHit {
    pub kind: SearchKind,
    pub id: String,
    pub label: String,
    pub detail: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Entry {
    kind: SearchKind,
    id: String,
    label: String,
    detail: String,
    haystack: String,
}

/// Lowercased name index over topics, groups, brokers and subjects, rebuilt
/// on every topology or subject commit.
///
/// Typeahead runs per keystroke, so the per-query cost has to be a scan of
/// pre-lowercased strings, not a walk over the catalog.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SearchIndex {
    entries: Vec<Entry>,
}

impl SearchIndex {
    pub fn build(topology: Option<&Topology>, subjects: Option<&SubjectTable>) -> Self {
        let mut entries = Vec::new();

        if let Some(topology) = topology {
            for (name, topic) in &topology.topics {
                entries.push(Entry {
                    kind: SearchKind::Topic,
                    id: name.to_string(),
                    label: name.to_string(),
                    detail: format!("{} partitions", topic.partitions.len()),
                    haystack: name.to_lowercase(),
                });
            }
            for (id, group) in &topology.groups {
                entries.push(Entry {
                    kind: SearchKind::Group,
                    id: id.to_string(),
                    label: id.to_string(),
                    detail: group.state.to_string(),
                    haystack: id.to_lowercase(),
                });
            }
            for (id, broker) in &topology.brokers {
                entries.push(Entry {
                    kind: SearchKind::Node,
                    id: id.to_string(),
                    label: format!("Broker {id}"),
                    detail: broker.host.clone(),
                    haystack: format!("{id} {}", broker.host).to_lowercase(),
                });
            }
        }

        if let Some(subjects) = subjects {
            for (name, subject) in &subjects.subjects {
                entries.push(Entry {
                    kind: SearchKind::Subject,
                    id: name.to_string(),
                    label: name.to_string(),
                    detail: format!("{} · v{}", subject.schema_type, subject.latest_version),
                    haystack: name.to_lowercase(),
                });
            }
        }

        Self { entries }
    }

    pub fn search(&self, term: &str) -> Vec<SearchHit> {
        let needle = term.trim().to_lowercase();
        if needle.is_empty() {
            return Vec::new();
        }

        self.entries
            .iter()
            .filter(|entry| entry.haystack.contains(&needle))
            .take(MAX_HITS)
            .map(|entry| SearchHit {
                kind: entry.kind,
                id: entry.id.clone(),
                label: entry.label.clone(),
                detail: entry.detail.clone(),
            })
            .collect()
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kafka::store::fixtures::{group, metadata, partition, subject, topic, topology};
    use crate::kafka::store::tables::Interner;

    fn index() -> SearchIndex {
        let topology = topology(
            vec![
                topic(
                    "orders.created",
                    vec![
                        partition(0, vec![1], vec![1]),
                        partition(1, vec![1], vec![1]),
                    ],
                ),
                topic("payments", vec![partition(0, vec![1], vec![1])]),
            ],
            vec![group("order-processor", "orders.created", vec![0])],
        );
        let subjects = SubjectTable::assemble(
            &[subject("orders.created-value", 1, 2)],
            &mut Interner::default(),
        );
        SearchIndex::build(Some(&topology), Some(&subjects))
    }

    #[test]
    fn an_unbuilt_index_matches_nothing() {
        let empty = SearchIndex::build(None, None);
        assert!(empty.is_empty());
        assert!(empty.search("orders").is_empty());
    }

    #[test]
    fn matches_across_every_entity_kind() {
        let hits = index().search("order");
        let kinds: Vec<SearchKind> = hits.iter().map(|hit| hit.kind).collect();

        assert_eq!(
            kinds,
            vec![SearchKind::Topic, SearchKind::Group, SearchKind::Subject]
        );
        assert_eq!(hits[0].detail, "2 partitions");
        assert_eq!(hits[1].detail, "Stable");
        assert_eq!(hits[2].detail, "AVRO \u{b7} v2");
    }

    #[test]
    fn brokers_match_on_id_or_host() {
        assert_eq!(index().search("localhost")[0].kind, SearchKind::Node);
        assert_eq!(index().search("1")[0].label, "Broker 1");
    }

    #[test]
    fn matching_is_case_insensitive_and_blank_terms_match_nothing() {
        assert_eq!(index().search("ORDERS.CREATED-VALUE").len(), 1);
        assert!(index().search("   ").is_empty());
        assert!(index().search("").is_empty());
    }

    #[test]
    fn results_are_capped() {
        let topics = (0..50)
            .map(|id| {
                topic(
                    &format!("orders-{id:02}"),
                    vec![partition(0, vec![1], vec![1])],
                )
            })
            .collect();
        let topology = Topology::assemble(&metadata(topics), &[], &mut Interner::default());

        assert_eq!(
            SearchIndex::build(Some(&topology), None)
                .search("orders")
                .len(),
            MAX_HITS
        );
    }
}
