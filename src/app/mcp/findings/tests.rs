use super::*;
use crate::kafka::model::{GroupMember, GroupSnapshot, MemberAssignment};
use crate::kafka::store::projections::group_detail;
use crate::testing::{offsets, partition, topic, topology, watermarks};

fn member(id: &str, partitions: Vec<i32>) -> GroupMember {
    GroupMember {
        id: id.into(),
        client_id: format!("client-{id}"),
        host: format!("host-{id}"),
        assignments: vec![MemberAssignment {
            topic: "orders".into(),
            partitions,
        }],
    }
}

fn idle(id: &str) -> GroupMember {
    GroupMember {
        assignments: Vec::new(),
        ..member(id, Vec::new())
    }
}

fn on_orders(state: GroupState, members: Vec<GroupMember>, partitions: i32) -> Topology {
    let group = GroupSnapshot {
        id: "billing".into(),
        state,
        protocol: "range".into(),
        members,
        committed: Vec::new(),
    };
    topology(
        vec![topic(
            "orders",
            (0..partitions)
                .map(|id| partition(id, vec![1], vec![1]))
                .collect(),
        )],
        vec![group],
    )
}

fn examine(
    state: GroupState,
    members: Vec<GroupMember>,
    partitions: i32,
    committed: Option<&[(&str, i32, i64)]>,
) -> Vec<Finding> {
    let topology = on_orders(state, members, partitions);
    let marks: Vec<_> = (0..partitions).map(|id| ("orders", id, 0, 1000)).collect();
    let (id, info) = topology.groups.iter().next().expect("the group");
    let detail = group_detail(
        id,
        info,
        committed.map(offsets).as_ref(),
        Some(&watermarks(&marks)),
    );
    findings(&detail, &topology)
}

#[test]
fn a_balanced_group_finds_nothing() {
    let members = vec![member("a", vec![0]), member("b", vec![1])];

    let found = examine(
        GroupState::Stable,
        members,
        2,
        Some(&[("orders", 0, 1000), ("orders", 1, 1000)]),
    );

    assert_eq!(found, []);
}

#[test]
fn a_group_without_members_finds_only_that() {
    let found = examine(GroupState::Empty, Vec::new(), 2, Some(&[("orders", 0, 0)]));

    assert_eq!(found, [Finding::NoMembers]);
}

#[test]
fn a_rebalancing_group_finds_only_that() {
    for state in [
        GroupState::PreparingRebalance,
        GroupState::CompletingRebalance,
    ] {
        let found = examine(state, vec![idle("a"), idle("b"), idle("c")], 2, None);

        assert_eq!(found, [Finding::Rebalancing], "{state}");
    }
}

#[test]
fn members_past_the_partition_count_are_named_with_both_counts() {
    let members = vec![member("a", vec![0]), member("b", vec![1]), idle("c")];

    let found = examine(GroupState::Stable, members, 2, None);

    assert_eq!(
        found,
        [Finding::MoreMembersThanPartitions {
            members: 3,
            partitions: 2,
        }]
    );
}

#[test]
fn members_on_a_topic_klens_does_not_know_claim_no_partition_count() {
    let mut stray = member("b", vec![0]);
    stray.assignments[0].topic = "deleted".into();
    let members = vec![member("a", vec![0]), stray, idle("c")];

    let found = examine(GroupState::Stable, members, 1, None);

    assert_eq!(found, []);
}

#[test]
fn a_group_with_no_decoded_assignment_claims_no_partition_count() {
    let found = examine(GroupState::Stable, vec![idle("a"), idle("b")], 1, None);

    assert_eq!(found, []);
}

#[test]
fn partitions_no_member_holds_are_named_per_topic() {
    let members = vec![member("a", vec![0]), member("b", vec![2])];

    let found = examine(GroupState::Stable, members, 4, None);

    assert_eq!(
        found,
        [Finding::UnassignedPartitions {
            topic: "orders".into(),
            partitions: vec![1, 3],
        }]
    );
}

#[test]
fn one_member_holding_four_fifths_of_a_large_lag_is_named() {
    let members = vec![member("a", vec![0]), member("b", vec![1, 2])];

    let found = examine(
        GroupState::Stable,
        members,
        3,
        Some(&[("orders", 0, 800), ("orders", 1, 200), ("orders", 2, 0)]),
    );

    assert_eq!(
        found,
        [Finding::LagOnOneMember {
            member_id: "b".into(),
            client_id: "client-b".into(),
            host: "host-b".into(),
            lag: 800 + 1000,
            total_lag: 200 + 800 + 1000,
        }]
    );
}

#[test]
fn lag_on_one_member_starts_at_its_thresholds() {
    let lagging = |committed: &[(&str, i32, i64)], members: Vec<GroupMember>| {
        examine(GroupState::Stable, members, 2, Some(committed))
    };
    let pair = || vec![member("a", vec![0]), member("b", vec![1])];

    let at_both = lagging(&[("orders", 0, 800), ("orders", 1, 200)], pair());
    let short_share = lagging(&[("orders", 0, 799), ("orders", 1, 200)], pair());
    let short_total = lagging(&[("orders", 0, 1000), ("orders", 1, 201)], pair());
    let alone = lagging(
        &[("orders", 0, 0), ("orders", 1, 1000)],
        vec![member("a", vec![0, 1])],
    );

    assert_eq!(
        at_both,
        [Finding::LagOnOneMember {
            member_id: "b".into(),
            client_id: "client-b".into(),
            host: "host-b".into(),
            lag: 800,
            total_lag: 1000,
        }]
    );
    assert_eq!(short_share, [], "800 of 1001 is under 80%");
    assert_eq!(short_total, [], "a total of 799 is too small to judge");
    assert_eq!(alone, [], "one member always holds all the lag");
}

#[test]
fn lag_on_one_member_is_not_judged_on_an_incomplete_total() {
    let members = vec![member("a", vec![0]), member("b", vec![1])];
    let topology = on_orders(GroupState::Stable, members, 2);
    let (id, info) = topology.groups.iter().next().expect("the group");
    let committed = offsets(&[("orders", 0, 0), ("orders", 1, 0)]);
    let marks = watermarks(&[("orders", 0, 0, 5000)]);

    let detail = group_detail(id, info, Some(&committed), Some(&marks));

    assert_eq!(detail.total_lag, Some(5000));
    assert!(!detail.lag_complete);
    assert_eq!(findings(&detail, &topology), []);
}

#[test]
fn lag_is_not_judged_before_klens_reads_the_offsets() {
    let members = vec![member("a", vec![0]), member("b", vec![1])];

    let found = examine(GroupState::Stable, members, 2, None);

    assert_eq!(found, []);
}

#[test]
fn a_member_lag_sums_the_partitions_it_holds() {
    let members = vec![member("a", vec![0, 1]), member("b", vec![2])];
    let topology = on_orders(GroupState::Stable, members, 4);
    let (id, info) = topology.groups.iter().next().expect("the group");
    let committed = offsets(&[("orders", 0, 7), ("orders", 1, 5), ("orders", 3, 1)]);
    let marks = watermarks(&[
        ("orders", 0, 0, 10),
        ("orders", 1, 0, 10),
        ("orders", 3, 0, 10),
    ]);

    let detail = group_detail(id, info, Some(&committed), Some(&marks));

    assert_eq!(
        member_lags(&detail),
        [Some(3 + 5), None],
        "b's partition has no watermark, and no member holds partition 3"
    );
}
