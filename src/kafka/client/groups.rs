use std::collections::HashMap;

use krafka::admin::{ConsumerGroupDescription, ConsumerGroupListing};
use krafka::error::KrafkaError;

use crate::kafka::group::{GroupSnapshot, GroupState, is_internal_group};

pub(super) const LISTED_GROUP_TYPES: [&str; 2] = ["classic", "consumer"];

pub(super) struct ListedGroup {
    id: String,
    state: Option<GroupState>,
}

impl ListedGroup {
    pub(super) fn from_krafka(listing: ConsumerGroupListing) -> Self {
        Self {
            id: listing.group_id,
            state: listing.state.as_deref().map(GroupState::parse),
        }
    }

    /// A broker below ListGroups v4 reports no state, so its groups are
    /// described to learn whether they have members.
    fn needs_description(&self) -> bool {
        self.state.is_none_or(GroupState::has_members)
    }
}

pub(super) fn snapshots_from_descriptions(
    descriptions: HashMap<String, Result<ConsumerGroupDescription, KrafkaError>>,
) -> Vec<GroupSnapshot> {
    descriptions
        .into_values()
        .filter_map(Result::ok)
        .filter(|description| {
            !description.state.eq_ignore_ascii_case("dead")
                && !is_internal_group(&description.group_id)
        })
        .map(GroupSnapshot::from_krafka)
        .collect()
}

pub(super) fn split_empty_groups(
    listed: impl IntoIterator<Item = ListedGroup>,
) -> (Vec<String>, Vec<GroupSnapshot>) {
    let mut active = Vec::new();
    let mut empty = Vec::new();

    for group in listed {
        if is_internal_group(&group.id) {
            continue;
        }
        if group.needs_description() {
            active.push(group.id);
        } else {
            empty.push(empty_snapshot(group.id));
        }
    }

    (active, empty)
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

    fn listed(id: &str, state: Option<GroupState>) -> ListedGroup {
        ListedGroup {
            id: id.to_owned(),
            state,
        }
    }

    #[test]
    fn groups_listed_without_members_are_empty_and_the_rest_are_described() {
        let (active, empty) = split_empty_groups([
            listed("orders", Some(GroupState::Stable)),
            listed("audit", Some(GroupState::Empty)),
            listed("billing", Some(GroupState::PreparingRebalance)),
            listed("retired", Some(GroupState::Dead)),
        ]);

        assert_eq!(active, ["orders", "billing"]);
        assert_eq!(
            empty,
            [
                empty_snapshot("audit".to_owned()),
                empty_snapshot("retired".to_owned())
            ]
        );
        assert_eq!(empty[0].state, GroupState::Empty);
        assert!(empty[0].protocol.is_empty());
    }

    #[test]
    fn a_group_listed_without_a_state_is_described() {
        let (active, empty) = split_empty_groups([listed("orders", None)]);

        assert_eq!(active, ["orders"]);
        assert!(empty.is_empty());
    }

    #[test]
    fn internal_groups_are_neither_described_nor_listed_as_empty() {
        let (active, empty) = split_empty_groups([
            listed("klens.internal.idle", Some(GroupState::Empty)),
            listed("klens.internal.live", Some(GroupState::Stable)),
            listed("orders", Some(GroupState::Empty)),
        ]);

        assert!(active.is_empty());
        assert_eq!(empty, [empty_snapshot("orders".to_owned())]);
    }
}
