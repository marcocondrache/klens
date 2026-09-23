use std::borrow::Cow;

use frizbee::{CaseMatching, Config, Matcher, Pattern};

use super::tables::{BrokerInfo, GroupInfo, SubjectInfo, SubjectTable, TopicInfo, Topology};

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

enum Candidate<'a> {
    Topic(&'a str, &'a TopicInfo),
    Group(&'a str, &'a GroupInfo),
    Node(i32, &'a BrokerInfo),
    Subject(&'a str, &'a SubjectInfo),
}

impl Candidate<'_> {
    fn haystack(&self) -> Cow<'_, str> {
        match self {
            Self::Topic(name, _) | Self::Group(name, _) | Self::Subject(name, _) => {
                Cow::Borrowed(name)
            }
            Self::Node(id, broker) => Cow::Owned(format!("{id} {}", broker.host)),
        }
    }

    fn hit(&self) -> SearchHit {
        match self {
            Self::Topic(name, topic) => SearchHit {
                kind: SearchKind::Topic,
                id: name.to_string(),
                label: name.to_string(),
                detail: format!("{} partitions", topic.partitions.len()),
            },
            Self::Group(id, group) => SearchHit {
                kind: SearchKind::Group,
                id: id.to_string(),
                label: id.to_string(),
                detail: group.state.to_string(),
            },
            Self::Node(id, broker) => SearchHit {
                kind: SearchKind::Node,
                id: id.to_string(),
                label: format!("Broker {id}"),
                detail: broker.host.clone(),
            },
            Self::Subject(name, subject) => SearchHit {
                kind: SearchKind::Subject,
                id: name.to_string(),
                label: name.to_string(),
                detail: format!("{} · v{}", subject.schema_type, subject.latest_version),
            },
        }
    }
}

fn candidates<'a>(
    topology: Option<&'a Topology>,
    subjects: Option<&'a SubjectTable>,
) -> Vec<Candidate<'a>> {
    let capacity = topology.map_or(0, |topology| {
        topology.topics.len() + topology.groups.len() + topology.brokers.len()
    }) + subjects.map_or(0, |subjects| subjects.subjects.len());
    let mut candidates = Vec::with_capacity(capacity);

    if let Some(topology) = topology {
        candidates.extend(
            topology
                .topics
                .iter()
                .map(|(name, topic)| Candidate::Topic(name, topic)),
        );
        candidates.extend(
            topology
                .groups
                .iter()
                .map(|(id, group)| Candidate::Group(id, group)),
        );
        candidates.extend(
            topology
                .brokers
                .iter()
                .map(|(id, broker)| Candidate::Node(*id, broker)),
        );
    }

    if let Some(subjects) = subjects {
        candidates.extend(
            subjects
                .subjects
                .iter()
                .map(|(name, subject)| Candidate::Subject(name, subject)),
        );
    }

    candidates
}

pub fn find(
    topology: Option<&Topology>,
    subjects: Option<&SubjectTable>,
    term: &str,
) -> Vec<SearchHit> {
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

    let candidates = candidates(topology, subjects);
    let haystacks: Vec<Cow<'_, str>> = candidates.iter().map(Candidate::haystack).collect();

    let config = Config::default().casing(CaseMatching::Ignore);
    Matcher::from_patterns(&patterns, &config)
        .match_list(&haystacks)
        .into_iter()
        .take(MAX_HITS)
        .map(|found| candidates[found.index as usize].hit())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kafka::store::fixtures::{group, metadata, partition, subject, topic, topology};
    use crate::kafka::store::tables::Interner;

    fn cluster() -> (Topology, SubjectTable) {
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
        (topology, subjects)
    }

    fn search(term: &str) -> Vec<SearchHit> {
        let (topology, subjects) = cluster();
        find(Some(&topology), Some(&subjects), term)
    }

    #[test]
    fn a_cluster_without_tables_matches_nothing() {
        assert!(find(None, None, "orders").is_empty());
    }

    #[test]
    fn matches_across_every_entity_kind() {
        let hits = search("order");
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
        assert_eq!(search("localhost")[0].kind, SearchKind::Node);
        assert_eq!(search("1")[0].label, "Broker 1");
    }

    #[test]
    fn matching_is_case_insensitive_and_blank_terms_match_nothing() {
        assert_eq!(search("ORDERS.CREATED-VALUE").len(), 1);
        assert!(search("   ").is_empty());
        assert!(search("").is_empty());
    }

    fn topics(names: &[&str]) -> Topology {
        let topics = names
            .iter()
            .map(|name| topic(name, vec![partition(0, vec![1], vec![1])]))
            .collect();
        Topology::assemble(metadata(topics), Vec::new(), &mut Interner::default())
    }

    fn labels(topology: &Topology, term: &str) -> Vec<String> {
        find(Some(topology), None, term)
            .into_iter()
            .map(|hit| hit.label)
            .collect()
    }

    #[test]
    fn closer_matches_rank_first() {
        let topology = topics(&["audit.orders", "order-events", "orders", "orders.created"]);
        let hits = labels(&topology, "orders");

        assert_eq!(hits[0], "orders");
        assert_eq!(hits[1], "orders.created");
        assert_eq!(hits.last().map(String::as_str), Some("order-events"));
    }

    #[test]
    fn matches_are_fuzzy_and_forgive_typos_in_longer_needles() {
        let topology = topics(&["billing.invoices", "payments", "user.profile.updated"]);

        assert_eq!(labels(&topology, "upu"), ["user.profile.updated"]);
        assert_eq!(labels(&topology, "invoces"), ["billing.invoices"]);
        assert!(labels(&topology, "pmx").is_empty());
    }

    #[test]
    fn queries_combine_atoms_and_support_exclusions() {
        let topology = topics(&["orders", "payments.refunds", "prod.orders"]);

        assert_eq!(labels(&topology, "pay ref"), ["payments.refunds"]);
        assert_eq!(labels(&topology, "orders !prod"), ["orders"]);
        assert_eq!(labels(&topology, "^prod"), ["prod.orders"]);
        assert!(labels(&topology, "!orders").is_empty());
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
