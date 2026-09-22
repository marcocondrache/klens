use frizbee::{CaseMatching, Config, Matcher, Pattern};

use super::tables::{SubjectTable, Topology};

const MAX_HITS: usize = 20;

const CHARS_PER_TYPO: usize = 4;

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
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SearchIndex {
    entries: Vec<Entry>,
    haystacks: Vec<String>,
}

impl SearchIndex {
    pub fn build(topology: Option<&Topology>, subjects: Option<&SubjectTable>) -> Self {
        let mut entries = Vec::new();
        let mut haystacks = Vec::new();

        if let Some(topology) = topology {
            for (name, topic) in &topology.topics {
                entries.push(Entry {
                    kind: SearchKind::Topic,
                    id: name.to_string(),
                    label: name.to_string(),
                    detail: format!("{} partitions", topic.partitions.len()),
                });
                haystacks.push(name.to_string());
            }
            for (id, group) in &topology.groups {
                entries.push(Entry {
                    kind: SearchKind::Group,
                    id: id.to_string(),
                    label: id.to_string(),
                    detail: group.state.to_string(),
                });
                haystacks.push(id.to_string());
            }
            for (id, broker) in &topology.brokers {
                entries.push(Entry {
                    kind: SearchKind::Node,
                    id: id.to_string(),
                    label: format!("Broker {id}"),
                    detail: broker.host.clone(),
                });
                haystacks.push(format!("{id} {}", broker.host));
            }
        }

        if let Some(subjects) = subjects {
            for (name, subject) in &subjects.subjects {
                entries.push(Entry {
                    kind: SearchKind::Subject,
                    id: name.to_string(),
                    label: name.to_string(),
                    detail: format!("{} · v{}", subject.schema_type, subject.latest_version),
                });
                haystacks.push(name.to_string());
            }
        }

        Self { entries, haystacks }
    }

    pub fn search(&self, term: &str) -> Vec<SearchHit> {
        let patterns: Vec<Pattern> = Pattern::parse_query(term)
            .into_iter()
            .map(|pattern| {
                let typos = pattern.needle.chars().count() / CHARS_PER_TYPO;
                pattern.max_typos(Some(u16::try_from(typos).unwrap_or(u16::MAX)))
            })
            .collect();
        if patterns.iter().all(|pattern| pattern.negated) {
            return Vec::new();
        }

        let config = Config::default().casing(CaseMatching::Ignore);
        Matcher::from_patterns(&patterns, &config)
            .match_list(&self.haystacks)
            .into_iter()
            .take(MAX_HITS)
            .map(|found| &self.entries[found.index as usize])
            .map(|entry| SearchHit {
                kind: entry.kind,
                id: entry.id.clone(),
                label: entry.label.clone(),
                detail: entry.detail.clone(),
            })
            .collect()
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

    fn topics(names: &[&str]) -> SearchIndex {
        let topics = names
            .iter()
            .map(|name| topic(name, vec![partition(0, vec![1], vec![1])]))
            .collect();
        let topology = Topology::assemble(&metadata(topics), &[], &mut Interner::default());
        SearchIndex::build(Some(&topology), None)
    }

    fn labels(index: &SearchIndex, term: &str) -> Vec<String> {
        index
            .search(term)
            .into_iter()
            .map(|hit| hit.label)
            .collect()
    }

    #[test]
    fn closer_matches_rank_first() {
        let index = topics(&["audit.orders", "order-events", "orders", "orders.created"]);
        let hits = labels(&index, "orders");

        assert_eq!(hits[0], "orders");
        assert_eq!(hits[1], "orders.created");
        assert_eq!(hits.last().map(String::as_str), Some("order-events"));
    }

    #[test]
    fn matches_are_fuzzy_and_forgive_typos_in_longer_needles() {
        let index = topics(&["billing.invoices", "payments", "user.profile.updated"]);

        assert_eq!(labels(&index, "upu"), ["user.profile.updated"]);
        assert_eq!(labels(&index, "invoces"), ["billing.invoices"]);
        assert!(labels(&index, "pmx").is_empty());
    }

    #[test]
    fn queries_combine_atoms_and_support_exclusions() {
        let index = topics(&["orders", "payments.refunds", "prod.orders"]);

        assert_eq!(labels(&index, "pay ref"), ["payments.refunds"]);
        assert_eq!(labels(&index, "orders !prod"), ["orders"]);
        assert_eq!(labels(&index, "^prod"), ["prod.orders"]);
        assert!(labels(&index, "!orders").is_empty());
    }

    #[test]
    fn results_are_capped_to_the_best_matches() {
        let names: Vec<String> = (0..50)
            .map(|id| format!("analytics.orders-{id:02}"))
            .chain(["orders".to_owned()])
            .collect();
        let names: Vec<&str> = names.iter().map(String::as_str).collect();
        let hits = labels(&topics(&names), "orders");

        assert_eq!(hits.len(), MAX_HITS);
        assert_eq!(hits[0], "orders");
    }
}
