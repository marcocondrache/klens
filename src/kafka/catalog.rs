use std::collections::HashMap;

use crate::environment::{
    MAX_RECORD_LIMIT, RECORD_MIN_WINDOW, RECORD_SEARCH_WINDOW_MULTIPLIER, RECORD_WINDOW_MULTIPLIER,
};
use crate::kafka::model::{
    Broker, BrokerMetadata, CleanupPolicy, ClusterHealth, ClusterIdentity, ClusterOverview,
    ConfigEntry, ConsumerGroup, FetchPlan, GroupOffset, GroupSnapshot, MetadataSnapshot, Partition,
    PartitionWindow, RecordOrder, RecordQuery, SchemaSubject, SearchHit, SearchKind, Topic,
    TopicMetadata, Watermarks,
};

pub fn partition_health(meta: &MetadataSnapshot) -> (i32, i32, i32) {
    let mut partitions = 0;
    let mut under_replicated = 0;
    let mut offline = 0;

    for topic in &meta.topics {
        for partition in &topic.partitions {
            partitions += 1;
            if partition.under_replicated() {
                under_replicated += 1;
            }
            if partition.offline() {
                offline += 1;
            }
        }
    }

    (partitions, under_replicated, offline)
}

pub fn cluster_health(under_replicated: i32, offline: i32) -> ClusterHealth {
    if under_replicated > 0 || offline > 0 {
        ClusterHealth::Degraded
    } else {
        ClusterHealth::Healthy
    }
}

pub fn assemble_overview(
    identity: ClusterIdentity,
    meta: &MetadataSnapshot,
    group_count: i32,
) -> ClusterOverview {
    let (partition_count, under_replicated, offline) = partition_health(meta);

    ClusterOverview {
        identity,
        cluster_id: meta.cluster_id.clone().unwrap_or_default(),
        health: cluster_health(under_replicated, offline),
        broker_count: meta.brokers.len() as i32,
        topic_count: meta.topics.len() as i32,
        partition_count,
        consumer_group_count: group_count,
        under_replicated_partitions: under_replicated,
        offline_partitions: offline,
        message_count: 0,
    }
}

pub fn assemble_brokers(meta: &MetadataSnapshot) -> Vec<Broker> {
    let mut partition_counts = HashMap::<i32, i32>::new();
    let mut leader_counts = HashMap::<i32, i32>::new();

    for topic in &meta.topics {
        for partition in &topic.partitions {
            for replica in &partition.replicas {
                *partition_counts.entry(*replica).or_default() += 1;
            }
            if partition.leader >= 0 {
                *leader_counts.entry(partition.leader).or_default() += 1;
            }
        }
    }

    meta.brokers
        .iter()
        .map(|broker| assemble_broker(broker, &partition_counts, &leader_counts))
        .collect()
}

pub fn assemble_broker(
    broker: &BrokerMetadata,
    partition_counts: &HashMap<i32, i32>,
    leader_counts: &HashMap<i32, i32>,
) -> Broker {
    Broker {
        id: broker.id,
        host: broker.host.clone(),
        port: broker.port,
        rack: None,
        controller: false,
        partition_count: partition_counts.get(&broker.id).copied().unwrap_or(0),
        leader_count: leader_counts.get(&broker.id).copied().unwrap_or(0),
    }
}

pub fn topic_config_values(entries: Option<&[ConfigEntry]>) -> (CleanupPolicy, i64) {
    let mut cleanup_policy = CleanupPolicy::Delete;
    let mut retention_ms = 0;

    if let Some(entries) = entries {
        for entry in entries {
            match entry.name.as_str() {
                "cleanup.policy" => {
                    if let Some(value) = &entry.value {
                        cleanup_policy = CleanupPolicy::parse(value);
                    }
                }
                "retention.ms" => {
                    if let Some(value) = &entry.value {
                        retention_ms = value.parse().unwrap_or(0);
                    }
                }
                _ => {}
            }
        }
    }

    (cleanup_policy, retention_ms)
}

pub fn groups_for_topic(topic: &str, groups: &[GroupSnapshot]) -> Vec<String> {
    groups
        .iter()
        .filter(|group| group.consumes_topic(topic))
        .map(|group| group.id.clone())
        .collect()
}

pub fn assemble_topic(
    topic: &TopicMetadata,
    watermarks: &HashMap<i32, Watermarks>,
    config: Option<&[ConfigEntry]>,
    consumer_groups: Vec<String>,
) -> Topic {
    let partitions: Vec<Partition> = topic
        .partitions
        .iter()
        .map(|partition| {
            let marks = watermarks.get(&partition.id).copied().unwrap_or_default();
            Partition {
                id: partition.id,
                leader: partition.leader,
                replicas: partition.replicas.clone(),
                isr: partition.isr.clone(),
                low_watermark: marks.low,
                high_watermark: marks.high,
            }
        })
        .collect();

    let replication_factor = partitions
        .first()
        .map(|partition| partition.replicas.len() as i32)
        .unwrap_or(0);
    let message_count = partitions
        .iter()
        .map(|partition| (partition.high_watermark - partition.low_watermark).max(0) as u64)
        .sum();
    let under_replicated = partitions
        .iter()
        .any(|partition| partition.isr.len() < partition.replicas.len());
    let (cleanup_policy, retention_ms) = topic_config_values(config);

    Topic {
        name: topic.name.clone(),
        internal: topic.internal,
        partitions,
        replication_factor,
        message_count,
        cleanup_policy,
        retention_ms,
        consumer_groups,
        under_replicated,
    }
}

pub fn clamp_record_limit(limit: i32) -> Result<usize, String> {
    if limit < 1 {
        return Err("limit must be at least 1".into());
    }

    Ok((limit as usize).min(*MAX_RECORD_LIMIT))
}

pub fn clamp_record_page(page: i32) -> Result<usize, String> {
    if page < 0 {
        return Err("page must be at least 0".into());
    }

    Ok(page as usize)
}

pub fn apply_timestamp_bounds(
    watermarks: &mut HashMap<i32, Watermarks>,
    from_offsets: Option<&HashMap<i32, Option<i64>>>,
    to_offsets: Option<&HashMap<i32, Option<i64>>>,
) {
    for (partition, marks) in watermarks {
        if let Some(from_offsets) = from_offsets {
            match from_offsets.get(partition).copied().flatten() {
                Some(offset) => marks.low = marks.low.max(offset),
                None => marks.low = marks.high,
            }
        }

        if let Some(to_offsets) = to_offsets
            && let Some(offset) = to_offsets.get(partition).copied().flatten()
        {
            marks.high = marks.high.min(offset);
        }

        if marks.low > marks.high {
            marks.low = marks.high;
        }
    }
}

fn window_span(partition_count: usize, limit: usize, searching: bool, page: usize) -> (i64, i64) {
    let n = partition_count.max(1);
    let multiplier = if searching {
        *RECORD_SEARCH_WINDOW_MULTIPLIER
    } else {
        *RECORD_WINDOW_MULTIPLIER
    };
    let take = (limit.saturating_mul(multiplier))
        .div_ceil(n)
        .max(*RECORD_MIN_WINDOW) as i64;
    let skip = (page.saturating_mul(limit)).div_ceil(n) as i64;
    (skip, take)
}

pub fn plan_windows(
    partitions: &[i32],
    watermarks: &HashMap<i32, Watermarks>,
    order: RecordOrder,
    limit: usize,
    searching: bool,
    page: usize,
) -> Vec<PartitionWindow> {
    let (skip, window) = window_span(partitions.len(), limit, searching, page);

    partitions
        .iter()
        .filter_map(|partition| {
            let marks = watermarks.get(partition)?;
            let remaining = (marks.available() - skip).max(0);
            let take = window.min(remaining);
            if take == 0 {
                return None;
            }

            let (start, end) = match order {
                RecordOrder::Newest => {
                    let end = marks.high - skip;
                    (end - take, end)
                }
                RecordOrder::Oldest => {
                    let start = marks.low + skip;
                    (start, start + take)
                }
            };

            Some(PartitionWindow {
                partition: *partition,
                start,
                end,
            })
        })
        .collect()
}

pub fn plan_has_more(
    partitions: &[i32],
    watermarks: &HashMap<i32, Watermarks>,
    limit: usize,
    page: usize,
) -> bool {
    let available: i64 = partitions
        .iter()
        .filter_map(|partition| watermarks.get(partition).map(|marks| marks.available()))
        .sum();
    ((page + 1).saturating_mul(limit) as i64) < available
}

pub fn plan_records(
    query: &RecordQuery,
    partitions: &[i32],
    watermarks: &HashMap<i32, Watermarks>,
    limit: usize,
    page: usize,
) -> FetchPlan {
    let searching = !query.search.trim().is_empty();

    FetchPlan {
        topic: query.topic.clone(),
        windows: plan_windows(partitions, watermarks, query.order, limit, searching, page),
        search: query.search.trim().to_ascii_lowercase(),
        limit,
        order: query.order,
        has_more: plan_has_more(partitions, watermarks, limit, page),
    }
}

pub fn member_for_partition<'a>(
    members: &'a [crate::kafka::model::GroupMember],
    topic: &str,
    partition: i32,
) -> Option<&'a str> {
    members.iter().find_map(|member| {
        member
            .assignments
            .iter()
            .any(|assignment| {
                assignment.topic == topic && assignment.partitions.contains(&partition)
            })
            .then_some(member.id.as_str())
    })
}

pub fn assemble_group(group: &GroupSnapshot, ends: &HashMap<(String, i32), i64>) -> ConsumerGroup {
    let mut seen = HashMap::<(String, i32), GroupOffset>::new();

    for committed in &group.committed {
        let key = (committed.topic.clone(), committed.partition);
        let end = ends.get(&key).copied().unwrap_or(committed.offset);
        seen.insert(
            key.clone(),
            GroupOffset {
                topic: committed.topic.clone(),
                partition: committed.partition,
                current_offset: committed.offset,
                end_offset: end,
                lag: (end - committed.offset).max(0),
                member_id: member_for_partition(
                    &group.members,
                    &committed.topic,
                    committed.partition,
                )
                .map(ToOwned::to_owned),
            },
        );
    }

    for (topic, partition) in group.assigned_partition_refs() {
        seen.entry((topic.to_owned(), partition))
            .or_insert_with(|| {
                let end = ends
                    .get(&(topic.to_owned(), partition))
                    .copied()
                    .unwrap_or(0);
                GroupOffset {
                    topic: topic.to_owned(),
                    partition,
                    current_offset: 0,
                    end_offset: end,
                    lag: end,
                    member_id: member_for_partition(&group.members, topic, partition)
                        .map(ToOwned::to_owned),
                }
            });
    }

    let mut offsets: Vec<GroupOffset> = seen.into_values().collect();
    offsets.sort_by(|left, right| {
        left.topic
            .cmp(&right.topic)
            .then(left.partition.cmp(&right.partition))
    });
    let lag = offsets.iter().map(|offset| offset.lag).sum();

    ConsumerGroup {
        id: group.id.clone(),
        state: group.state,
        protocol: group.protocol.clone(),
        coordinator: group.coordinator,
        members: group.members.clone(),
        topics: unique_offset_topics(&offsets),
        lag,
        offsets,
    }
}

fn unique_offset_topics(offsets: &[GroupOffset]) -> Vec<String> {
    let mut topics = Vec::new();
    for offset in offsets {
        if topics.last() != Some(&offset.topic) {
            topics.push(offset.topic.clone());
        }
    }
    topics
}

pub fn search_catalog(
    term: &str,
    topics: &[TopicMetadata],
    brokers: &[BrokerMetadata],
    groups: &[GroupSnapshot],
    subjects: &[SchemaSubject],
) -> Vec<SearchHit> {
    let needle = term.trim().to_ascii_lowercase();
    if needle.is_empty() {
        return Vec::new();
    }

    let mut hits = Vec::new();

    for topic in topics {
        if topic.name.to_ascii_lowercase().contains(&needle) {
            hits.push(SearchHit {
                kind: SearchKind::Topic,
                id: topic.name.clone(),
                label: topic.name.clone(),
                detail: format!("{} partitions", topic.partitions.len()),
            });
        }
    }

    for group in groups {
        if group.id.to_ascii_lowercase().contains(&needle) {
            hits.push(SearchHit {
                kind: SearchKind::Group,
                id: group.id.clone(),
                label: group.id.clone(),
                detail: group.state.to_string(),
            });
        }
    }

    for broker in brokers {
        let haystack = format!("{} {}", broker.id, broker.host).to_ascii_lowercase();
        if haystack.contains(&needle) {
            hits.push(SearchHit {
                kind: SearchKind::Node,
                id: broker.id.to_string(),
                label: format!("Broker {}", broker.id),
                detail: broker.host.clone(),
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

    hits.truncate(20);
    hits
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::SecurityProtocol;
    use crate::kafka::model::{
        BrokerMetadata, ClusterIdentity, GroupMember, GroupState, MemberAssignment,
        PartitionMetadata, TopicMetadata,
    };

    fn identity() -> ClusterIdentity {
        ClusterIdentity {
            name: "local".into(),
            bootstrap_servers: vec!["localhost:9092".into()],
            security_protocol: SecurityProtocol::Plaintext,
        }
    }

    fn topic(name: &str, partitions: Vec<PartitionMetadata>) -> TopicMetadata {
        TopicMetadata {
            name: name.into(),
            internal: name.starts_with('_'),
            partitions,
        }
    }

    fn partition(id: i32, leader: i32, replicas: Vec<i32>, isr: Vec<i32>) -> PartitionMetadata {
        PartitionMetadata {
            id,
            leader,
            replicas,
            isr,
        }
    }

    #[test]
    fn health_is_degraded_when_partitions_are_unhealthy() {
        assert_eq!(cluster_health(0, 0), ClusterHealth::Healthy);
        assert_eq!(cluster_health(1, 0), ClusterHealth::Degraded);
        assert_eq!(cluster_health(0, 2), ClusterHealth::Degraded);
    }

    #[test]
    fn overview_counts_partition_problems() {
        let meta = MetadataSnapshot {
            cluster_id: Some("abc".into()),
            brokers: vec![BrokerMetadata {
                id: 1,
                host: "localhost".into(),
                port: 9092,
            }],
            topics: vec![topic(
                "orders",
                vec![
                    partition(0, 1, vec![1, 2], vec![1]),
                    partition(1, -1, vec![1], vec![]),
                ],
            )],
        };

        let overview = assemble_overview(identity(), &meta, 3);
        assert_eq!(overview.cluster_id, "abc");
        assert_eq!(overview.health, ClusterHealth::Degraded);
        assert_eq!(overview.partition_count, 2);
        assert_eq!(overview.under_replicated_partitions, 2);
        assert_eq!(overview.offline_partitions, 1);
        assert_eq!(overview.consumer_group_count, 3);
    }

    #[test]
    fn newest_window_reads_from_the_high_watermark() {
        let mut watermarks = HashMap::new();
        watermarks.insert(0, Watermarks { low: 10, high: 40 });

        let windows = plan_windows(&[0], &watermarks, RecordOrder::Newest, 5, false, 0);
        assert_eq!(
            windows,
            vec![PartitionWindow {
                partition: 0,
                start: 30,
                end: 40,
            }]
        );
        assert!(plan_has_more(&[0], &watermarks, 5, 0));
    }

    #[test]
    fn oldest_window_reads_from_the_low_watermark() {
        let mut watermarks = HashMap::new();
        watermarks.insert(0, Watermarks { low: 10, high: 40 });

        let windows = plan_windows(&[0], &watermarks, RecordOrder::Oldest, 5, false, 0);
        assert_eq!(
            windows,
            vec![PartitionWindow {
                partition: 0,
                start: 10,
                end: 20,
            }]
        );
        assert!(plan_has_more(&[0], &watermarks, 5, 0));
    }

    #[test]
    fn newest_window_on_later_page_moves_back_from_the_high_watermark() {
        let mut watermarks = HashMap::new();
        watermarks.insert(0, Watermarks { low: 10, high: 40 });

        let windows = plan_windows(&[0], &watermarks, RecordOrder::Newest, 5, false, 1);
        assert_eq!(
            windows,
            vec![PartitionWindow {
                partition: 0,
                start: 25,
                end: 35,
            }]
        );
        assert!(plan_has_more(&[0], &watermarks, 5, 1));
    }

    #[test]
    fn oldest_window_on_later_page_moves_forward_from_the_low_watermark() {
        let mut watermarks = HashMap::new();
        watermarks.insert(0, Watermarks { low: 10, high: 40 });

        let windows = plan_windows(&[0], &watermarks, RecordOrder::Oldest, 5, false, 1);
        assert_eq!(
            windows,
            vec![PartitionWindow {
                partition: 0,
                start: 15,
                end: 25,
            }]
        );
        assert!(plan_has_more(&[0], &watermarks, 5, 1));
    }

    #[test]
    fn timestamp_from_raises_the_low_watermark() {
        let mut watermarks = HashMap::new();
        watermarks.insert(0, Watermarks { low: 10, high: 40 });
        let from = HashMap::from([(0, Some(25))]);

        apply_timestamp_bounds(&mut watermarks, Some(&from), None);
        assert_eq!(watermarks[&0], Watermarks { low: 25, high: 40 });
    }

    #[test]
    fn timestamp_to_lowers_the_high_watermark() {
        let mut watermarks = HashMap::new();
        watermarks.insert(0, Watermarks { low: 10, high: 40 });
        let to = HashMap::from([(0, Some(22))]);

        apply_timestamp_bounds(&mut watermarks, None, Some(&to));
        assert_eq!(watermarks[&0], Watermarks { low: 10, high: 22 });
    }

    #[test]
    fn missing_from_offset_empties_the_partition() {
        let mut watermarks = HashMap::new();
        watermarks.insert(0, Watermarks { low: 10, high: 40 });
        let from = HashMap::from([(0, None)]);

        apply_timestamp_bounds(&mut watermarks, Some(&from), None);
        assert_eq!(watermarks[&0], Watermarks { low: 40, high: 40 });
    }

    #[test]
    fn missing_to_offset_keeps_the_high_watermark() {
        let mut watermarks = HashMap::new();
        watermarks.insert(0, Watermarks { low: 10, high: 40 });
        let to = HashMap::from([(0, None)]);

        apply_timestamp_bounds(&mut watermarks, None, Some(&to));
        assert_eq!(watermarks[&0], Watermarks { low: 10, high: 40 });
    }

    #[test]
    fn inverted_timestamp_bounds_collapse_to_empty() {
        let mut watermarks = HashMap::new();
        watermarks.insert(0, Watermarks { low: 10, high: 40 });
        let from = HashMap::from([(0, Some(30))]);
        let to = HashMap::from([(0, Some(20))]);

        apply_timestamp_bounds(&mut watermarks, Some(&from), Some(&to));
        assert_eq!(watermarks[&0], Watermarks { low: 20, high: 20 });
    }

    #[test]
    fn unix_millis_truncates_and_rejects_non_finite_values() {
        use crate::kafka::model::unix_millis;

        assert!(unix_millis(f64::NAN).is_err());
        assert!(unix_millis(f64::INFINITY).is_err());
        assert_eq!(unix_millis(1_700_000_000_000.9).unwrap(), 1_700_000_000_000);
    }

    #[test]
    fn timestamp_range_rejects_from_after_to() {
        use crate::kafka::model::validate_timestamp_range;

        assert!(validate_timestamp_range(Some(2), Some(1)).is_err());
        assert!(validate_timestamp_range(Some(1), Some(1)).is_ok());
        assert!(validate_timestamp_range(Some(1), None).is_ok());
    }

    #[test]
    fn page_past_available_records_yields_no_windows() {
        let mut watermarks = HashMap::new();
        watermarks.insert(0, Watermarks { low: 10, high: 40 });

        let windows = plan_windows(&[0], &watermarks, RecordOrder::Newest, 5, false, 6);
        assert!(windows.is_empty());
        assert!(!plan_has_more(&[0], &watermarks, 5, 6));
    }

    #[test]
    fn first_page_has_more_when_returned_limit_is_below_log_size() {
        let mut watermarks = HashMap::new();
        watermarks.insert(0, Watermarks { low: 0, high: 10 });

        assert!(plan_has_more(&[0], &watermarks, 5, 0));
        assert!(!plan_has_more(&[0], &watermarks, 5, 1));
        assert_eq!(
            plan_windows(&[0], &watermarks, RecordOrder::Oldest, 5, false, 1),
            vec![PartitionWindow {
                partition: 0,
                start: 5,
                end: 10,
            }]
        );
    }

    #[test]
    fn cleanup_policy_parses_combined_values() {
        assert_eq!(
            CleanupPolicy::parse("compact,delete"),
            CleanupPolicy::CompactDelete
        );
        assert_eq!(CleanupPolicy::parse("compact"), CleanupPolicy::Compact);
        assert_eq!(CleanupPolicy::parse("delete"), CleanupPolicy::Delete);
    }

    #[test]
    fn search_filters_topics_groups_and_brokers() {
        let topics = vec![topic(
            "orders.created",
            vec![partition(0, 1, vec![1], vec![1])],
        )];
        let brokers = vec![BrokerMetadata {
            id: 7,
            host: "broker-a".into(),
            port: 9092,
        }];
        let groups = vec![GroupSnapshot {
            id: "order-processor".into(),
            state: GroupState::Stable,
            protocol: "range".into(),
            coordinator: 0,
            members: vec![GroupMember {
                id: "m1".into(),
                client_id: "c1".into(),
                host: "127.0.0.1".into(),
                assignments: vec![MemberAssignment {
                    topic: "orders.created".into(),
                    partitions: vec![0],
                }],
            }],
            committed: Vec::new(),
        }];

        let subjects = vec![crate::kafka::model::SchemaSubject {
            subject: "orders.created-value".into(),
            id: 1,
            schema_type: crate::kafka::model::SchemaType::Avro,
            latest_version: 2,
            versions: vec![1, 2],
            compatibility: crate::kafka::model::SchemaCompatibility::Backward,
            schema: "{}".into(),
        }];

        let hits = search_catalog("order", &topics, &brokers, &groups, &subjects);
        assert_eq!(hits.len(), 3);
        assert!(hits.iter().any(|hit| hit.kind == SearchKind::Topic));
        assert!(hits.iter().any(|hit| hit.kind == SearchKind::Group));
        assert!(hits.iter().any(|hit| hit.kind == SearchKind::Subject));

        let nodes = search_catalog("broker-a", &topics, &brokers, &groups, &[]);
        assert_eq!(nodes[0].kind, SearchKind::Node);
    }

    #[test]
    fn groups_for_topic_does_not_need_a_materialized_topic_list() {
        let assigned = GroupSnapshot {
            id: "assigned".into(),
            state: GroupState::Stable,
            protocol: "range".into(),
            coordinator: 1,
            members: vec![GroupMember {
                id: "m1".into(),
                client_id: "c1".into(),
                host: "127.0.0.1".into(),
                assignments: vec![MemberAssignment {
                    topic: "orders".into(),
                    partitions: vec![0],
                }],
            }],
            committed: Vec::new(),
        };
        let committed = GroupSnapshot {
            id: "committed".into(),
            state: GroupState::Stable,
            protocol: "range".into(),
            coordinator: 1,
            members: Vec::new(),
            committed: vec![crate::kafka::model::CommittedOffset {
                topic: "orders".into(),
                partition: 0,
                offset: 1,
            }],
        };
        let other = GroupSnapshot {
            id: "other".into(),
            state: GroupState::Empty,
            protocol: String::new(),
            coordinator: 1,
            members: vec![GroupMember {
                id: "m2".into(),
                client_id: "c2".into(),
                host: "127.0.0.1".into(),
                assignments: vec![MemberAssignment {
                    topic: "payments".into(),
                    partitions: vec![0],
                }],
            }],
            committed: Vec::new(),
        };

        assert_eq!(
            groups_for_topic("orders", &[assigned, committed, other]),
            vec!["assigned", "committed"]
        );
    }

    #[test]
    fn assemble_group_computes_lag_from_end_offsets() {
        let group = GroupSnapshot {
            id: "g".into(),
            state: GroupState::Stable,
            protocol: "range".into(),
            coordinator: 1,
            members: vec![GroupMember {
                id: "m1".into(),
                client_id: "c1".into(),
                host: "127.0.0.1".into(),
                assignments: vec![MemberAssignment {
                    topic: "orders".into(),
                    partitions: vec![0],
                }],
            }],
            committed: vec![crate::kafka::model::CommittedOffset {
                topic: "orders".into(),
                partition: 0,
                offset: 4,
            }],
        };
        let mut ends = HashMap::new();
        ends.insert(("orders".into(), 0), 10);

        let view = assemble_group(&group, &ends);
        assert_eq!(view.lag, 6);
        assert_eq!(view.offsets[0].member_id.as_deref(), Some("m1"));
        assert_eq!(view.topics, vec!["orders"]);
    }
}
