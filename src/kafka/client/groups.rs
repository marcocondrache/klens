use foldhash::HashSet;
use krafka::admin::ConsumerGroupDescription;

use crate::kafka::group::{GroupSnapshot, GroupState, is_internal_group};

pub(super) const LISTED_GROUP_TYPES: [&str; 2] = ["classic", "consumer"];

pub(super) const ACTIVE_GROUP_STATES: [&str; 5] = [
    "Stable",
    "PreparingRebalance",
    "CompletingRebalance",
    "Assigning",
    "Reconciling",
];

pub(super) fn snapshots_from_descriptions(
    descriptions: Vec<ConsumerGroupDescription>,
) -> Vec<GroupSnapshot> {
    descriptions
        .into_iter()
        .filter(|description| {
            description.error.is_none()
                && !description.state.eq_ignore_ascii_case("dead")
                && !is_internal_group(&description.group_id)
        })
        .map(GroupSnapshot::from_krafka)
        .collect()
}

pub(super) fn split_empty_groups(
    listed: impl IntoIterator<Item = String>,
    active: impl IntoIterator<Item = String>,
) -> (Vec<String>, Vec<GroupSnapshot>) {
    let active: HashSet<String> = active
        .into_iter()
        .filter(|id| !is_internal_group(id))
        .collect();
    let empty = listed
        .into_iter()
        .filter(|id| !is_internal_group(id) && !active.contains(id))
        .map(empty_snapshot)
        .collect();
    (active.into_iter().collect(), empty)
}

fn empty_snapshot(id: String) -> GroupSnapshot {
    GroupSnapshot {
        id,
        state: GroupState::Empty,
        protocol: String::new(),
        members: Vec::new(),
        committed: Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ids(raw: &[&str]) -> Vec<String> {
        raw.iter().map(|id| (*id).to_owned()).collect()
    }

    fn sorted(mut ids: Vec<String>) -> Vec<String> {
        ids.sort_unstable();
        ids
    }

    #[test]
    fn groups_missing_from_the_active_listing_are_empty() {
        let (active, empty) = split_empty_groups(
            ids(&["orders", "audit", "billing"]),
            ids(&["orders", "billing"]),
        );

        assert_eq!(sorted(active), ids(&["billing", "orders"]));
        assert_eq!(empty, vec![empty_snapshot("audit".to_owned())]);
        assert_eq!(empty[0].state, GroupState::Empty);
        assert!(empty[0].protocol.is_empty());
    }

    #[test]
    fn a_group_that_became_active_after_the_full_listing_is_still_described() {
        let (active, empty) = split_empty_groups(ids(&["orders"]), ids(&["orders", "fresh"]));

        assert_eq!(sorted(active), ids(&["fresh", "orders"]));
        assert!(empty.is_empty());
    }

    #[test]
    fn internal_groups_are_neither_described_nor_listed_as_empty() {
        let (active, empty) = split_empty_groups(
            ids(&["klens.internal.idle", "klens.internal.live", "orders"]),
            ids(&["klens.internal.live"]),
        );

        assert!(active.is_empty());
        assert_eq!(empty, vec![empty_snapshot("orders".to_owned())]);
    }
}
