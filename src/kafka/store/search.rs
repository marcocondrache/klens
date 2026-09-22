use frizbee::{CaseMatching, Config, Match, Matcher, Pattern};

use super::tables::{SubjectTable, Topology};

const MAX_HITS: usize = 20;

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
                    haystack: name.to_string(),
                });
            }
            for (id, group) in &topology.groups {
                entries.push(Entry {
                    kind: SearchKind::Group,
                    id: id.to_string(),
                    label: id.to_string(),
                    detail: group.state.to_string(),
                    haystack: id.to_string(),
                });
            }
            for (id, broker) in &topology.brokers {
                entries.push(Entry {
                    kind: SearchKind::Node,
                    id: id.to_string(),
                    label: format!("Broker {id}"),
                    detail: broker.host.clone(),
                    haystack: format!("{id} {}", broker.host),
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
                    haystack: name.to_string(),
                });
            }
        }

        Self { entries }
    }

    /// Best matches first, keeping one row for each kind that matched.
    /// A space means every word must match. Every four typed characters
    /// allow one unmatched character.
    pub fn search(&self, term: &str) -> Vec<SearchHit> {
        let term = term.trim();
        if term.is_empty() || self.entries.is_empty() {
            return Vec::new();
        }

        let patterns = rank_patterns(term);
        if patterns.is_empty() {
            return Vec::new();
        }

        let haystacks: Vec<&str> = self
            .entries
            .iter()
            .map(|entry| entry.haystack.as_str())
            .collect();
        let mut matcher =
            Matcher::from_patterns(&patterns, &Config::default().casing(CaseMatching::Ignore));
        self.rank(&matcher.match_list(&haystacks))
    }

    /// The best hit of each kind is reserved, then the rest of the list is
    /// filled by score. Topics are indexed first, so a plain top-20 cut
    /// hides groups, brokers, and schemas whenever a prefix is common.
    fn rank(&self, matched: &[Match]) -> Vec<SearchHit> {
        let mut selected = vec![false; self.entries.len()];
        let mut picked = Vec::with_capacity(MAX_HITS.min(matched.len()));
        let mut seen = [false; 4];

        for hit in matched {
            if picked.len() == MAX_HITS {
                break;
            }
            let index = hit.index as usize;
            let kind = kind_slot(self.entries[index].kind);
            if seen[kind] {
                continue;
            }
            seen[kind] = true;
            selected[index] = true;
            picked.push(hit.index);
        }

        for hit in matched {
            if picked.len() == MAX_HITS {
                break;
            }
            let index = hit.index as usize;
            if selected[index] {
                continue;
            }
            selected[index] = true;
            picked.push(hit.index);
        }

        picked.sort_by_key(|index| {
            matched
                .iter()
                .position(|hit| hit.index == *index)
                .unwrap_or(usize::MAX)
        });

        picked
            .into_iter()
            .map(|index| {
                let entry = &self.entries[index as usize];
                SearchHit {
                    kind: entry.kind,
                    id: entry.id.clone(),
                    label: entry.label.clone(),
                    detail: entry.detail.clone(),
                }
            })
            .collect()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

fn rank_patterns(term: &str) -> Vec<Pattern> {
    Pattern::parse_query(term)
        .into_iter()
        .map(|pattern| {
            let budget = typo_budget(&pattern.needle);
            pattern.max_typos(Some(budget))
        })
        .collect()
}

/// One unmatched character for every four typed. Short queries stay exact
/// so a single digit still means broker 1.
fn typo_budget(needle: &str) -> u16 {
    let chars = needle.chars().count() / 4;
    u16::try_from(chars).unwrap_or(u16::MAX)
}

fn kind_slot(kind: SearchKind) -> usize {
    match kind {
        SearchKind::Topic => 0,
        SearchKind::Group => 1,
        SearchKind::Node => 2,
        SearchKind::Subject => 3,
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
        assert_eq!(index().search("LOCALHOST")[0].kind, SearchKind::Node);
        assert_eq!(index().search("1")[0].label, "Broker 1");
    }

    #[test]
    fn separate_words_must_all_match() {
        let hits = index().search("orders created");
        let labels: Vec<&str> = hits.iter().map(|hit| hit.label.as_str()).collect();

        assert_eq!(labels, vec!["orders.created", "orders.created-value"]);
    }

    #[test]
    fn a_typo_still_finds_the_topic() {
        let hits = index().search("ordres");
        assert!(
            hits.iter().any(|hit| hit.label == "orders.created"),
            "{hits:?}"
        );
    }

    #[test]
    fn an_exact_name_ranks_ahead_of_a_longer_one() {
        let topology = topology(
            vec![
                topic("alpha-zeta", vec![partition(0, vec![1], vec![1])]),
                topic("zeta-extra", vec![partition(0, vec![1], vec![1])]),
                topic("zeta", vec![partition(0, vec![1], vec![1])]),
            ],
            vec![],
        );

        assert_eq!(
            SearchIndex::build(Some(&topology), None).search("zeta")[0].label,
            "zeta"
        );
    }

    #[test]
    fn every_matching_kind_survives_the_cap() {
        let topics = (0..30)
            .map(|id| {
                topic(
                    &format!("alpha-{id:02}"),
                    vec![partition(0, vec![1], vec![1])],
                )
            })
            .collect();
        let topology = Topology::assemble(
            &metadata(topics),
            &[group("alpha-worker", "alpha-00", vec![0])],
            &mut Interner::default(),
        );
        let subjects =
            SubjectTable::assemble(&[subject("alpha-value", 1, 1)], &mut Interner::default());
        let hits = SearchIndex::build(Some(&topology), Some(&subjects)).search("alpha");
        let kinds: Vec<SearchKind> = hits.iter().map(|hit| hit.kind).collect();

        assert_eq!(hits.len(), MAX_HITS);
        assert!(kinds.contains(&SearchKind::Group), "{kinds:?}");
        assert!(kinds.contains(&SearchKind::Subject), "{kinds:?}");
    }

    #[test]
    fn a_query_with_no_text_matches_nothing() {
        assert!(index().search("!").is_empty());
        assert!(index().search("^").is_empty());
        assert!(index().search("$").is_empty());
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
