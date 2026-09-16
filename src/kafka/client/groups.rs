use krafka::admin::ConsumerGroupDescription;

use crate::kafka::group::{GroupSnapshot, is_internal_group};

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
