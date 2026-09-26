use foldhash::{HashMap, HashMapExt};

use crate::kafka::error::KafkaError;
use crate::kafka::group::CommittedOffset;
use crate::kafka::metadata::Watermarks;
use crate::kafka::session::ClusterSession;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResetTarget {
    Earliest,
    Latest,
    Timestamp(i64),
    Offset(i64),
    Shift(i64),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResetScope {
    Committed,
    Topic {
        topic: String,
        partitions: Option<Vec<i32>>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffsetMove {
    pub topic: String,
    pub partition: i32,
    pub current: Option<i64>,
    pub target: i64,
}

impl OffsetMove {
    pub fn committed(&self) -> CommittedOffset {
        CommittedOffset {
            topic: self.topic.clone(),
            partition: self.partition,
            offset: self.target,
        }
    }
}

pub async fn plan_reset(
    session: &dyn ClusterSession,
    group: &str,
    scope: &ResetScope,
    target: ResetTarget,
) -> Result<Vec<OffsetMove>, KafkaError> {
    let (partitions, committed) = match scope {
        ResetScope::Committed => {
            let committed = session.committed_offsets(group, None).await?;
            let partitions = committed
                .iter()
                .map(|offset| (offset.topic.clone(), offset.partition))
                .collect();
            (partitions, committed)
        }
        ResetScope::Topic { topic, partitions } => {
            let partitions = topic_partitions(session, topic, partitions.as_deref()).await?;
            let committed = session.committed_offsets(group, Some(&partitions)).await?;
            (partitions, committed)
        }
    };
    let mut partitions = partitions;
    partitions.sort();
    partitions.dedup();
    if partitions.is_empty() {
        return Ok(Vec::new());
    }

    let mut by_topic: HashMap<String, Vec<i32>> = HashMap::new();
    for (topic, partition) in &partitions {
        by_topic.entry(topic.clone()).or_default().push(*partition);
    }
    let watermarks = session.watermarks(&by_topic).await?;
    let times = match target {
        ResetTarget::Timestamp(timestamp) => time_offsets(session, &by_topic, timestamp).await?,
        _ => HashMap::new(),
    };

    partitions
        .into_iter()
        .map(|(topic, partition)| {
            let marks = watermarks
                .get(&topic)
                .and_then(|marks| marks.get(&partition))
                .copied()
                .ok_or_else(|| KafkaError::UnknownPartition {
                    cluster: session.identity().name.clone(),
                    topic: topic.clone(),
                    partition,
                })?;
            let current = committed
                .iter()
                .find(|offset| offset.topic == topic && offset.partition == partition)
                .map(|offset| offset.offset);
            let wanted = match target {
                ResetTarget::Earliest => marks.low,
                ResetTarget::Latest => marks.high,
                ResetTarget::Offset(offset) => offset,
                ResetTarget::Timestamp(_) => times
                    .get(&(topic.clone(), partition))
                    .copied()
                    .flatten()
                    .unwrap_or(marks.high),
                ResetTarget::Shift(by) => current
                    .ok_or_else(|| {
                        KafkaError::InvalidRequest(format!(
                            "{topic}/{partition} has no committed offset to shift from"
                        ))
                    })?
                    .saturating_add(by),
            };
            Ok(OffsetMove {
                target: clamp(wanted, marks),
                topic,
                partition,
                current,
            })
        })
        .collect()
}

async fn topic_partitions(
    session: &dyn ClusterSession,
    topic: &str,
    wanted: Option<&[i32]>,
) -> Result<Vec<(String, i32)>, KafkaError> {
    let metadata = session.topic_metadata(topic).await?;
    let known: Vec<i32> = metadata
        .partitions
        .iter()
        .map(|partition| partition.id)
        .collect();
    let Some(wanted) = wanted else {
        return Ok(known
            .into_iter()
            .map(|partition| (topic.to_owned(), partition))
            .collect());
    };
    if wanted.is_empty() {
        return Err(KafkaError::InvalidRequest(
            "partitions must not be empty; omit it to cover every partition".into(),
        ));
    }
    wanted
        .iter()
        .map(|partition| {
            if known.contains(partition) {
                Ok((topic.to_owned(), *partition))
            } else {
                Err(KafkaError::UnknownPartition {
                    cluster: session.identity().name.clone(),
                    topic: topic.to_owned(),
                    partition: *partition,
                })
            }
        })
        .collect()
}

async fn time_offsets(
    session: &dyn ClusterSession,
    by_topic: &HashMap<String, Vec<i32>>,
    timestamp: i64,
) -> Result<HashMap<(String, i32), Option<i64>>, KafkaError> {
    let mut offsets = HashMap::new();
    for (topic, partitions) in by_topic {
        for (partition, offset) in session
            .offsets_for_times(topic, partitions, timestamp)
            .await?
        {
            offsets.insert((topic.clone(), partition), offset);
        }
    }
    Ok(offsets)
}

fn clamp(offset: i64, marks: Watermarks) -> i64 {
    offset.clamp(marks.low, marks.high.max(marks.low))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kafka::FakeCluster;

    const TOPIC: &str = "orders.created";

    fn moves(plan: &[OffsetMove]) -> Vec<(i32, Option<i64>, i64)> {
        plan.iter()
            .map(|step| (step.partition, step.current, step.target))
            .collect()
    }

    fn topic(partitions: Option<Vec<i32>>) -> ResetScope {
        ResetScope::Topic {
            topic: TOPIC.into(),
            partitions,
        }
    }

    async fn plan(scope: ResetScope, target: ResetTarget) -> Result<Vec<OffsetMove>, KafkaError> {
        plan_reset(&FakeCluster::local(), "order-processor", &scope, target).await
    }

    #[tokio::test]
    async fn earliest_and_latest_read_the_watermarks() {
        let earliest = plan(topic(None), ResetTarget::Earliest).await.unwrap();
        let latest = plan(topic(None), ResetTarget::Latest).await.unwrap();

        assert_eq!(moves(&earliest), vec![(0, Some(6), 0), (1, Some(5), 0)]);
        assert_eq!(moves(&latest), vec![(0, Some(6), 8), (1, Some(5), 8)]);
        assert_eq!(earliest[0].topic, TOPIC);
    }

    #[tokio::test]
    async fn an_exact_offset_is_clamped_to_the_log() {
        let past_the_end = plan(topic(Some(vec![0])), ResetTarget::Offset(50))
            .await
            .unwrap();
        let before_the_start = plan(topic(Some(vec![1])), ResetTarget::Offset(-3))
            .await
            .unwrap();
        let inside = plan(topic(Some(vec![1])), ResetTarget::Offset(3))
            .await
            .unwrap();

        assert_eq!(moves(&past_the_end), vec![(0, Some(6), 8)]);
        assert_eq!(moves(&before_the_start), vec![(1, Some(5), 0)]);
        assert_eq!(moves(&inside), vec![(1, Some(5), 3)]);
    }

    #[tokio::test]
    async fn a_shift_moves_from_the_committed_offset() {
        let back = plan(topic(None), ResetTarget::Shift(-2)).await.unwrap();
        let forward = plan(topic(None), ResetTarget::Shift(10)).await.unwrap();

        assert_eq!(moves(&back), vec![(0, Some(6), 4), (1, Some(5), 3)]);
        assert_eq!(
            moves(&forward),
            vec![(0, Some(6), 8), (1, Some(5), 8)],
            "a shift past the end stops at the end"
        );
    }

    #[tokio::test]
    async fn a_shift_needs_a_committed_offset() {
        let cluster = FakeCluster::local();
        cluster.commit_offsets(
            "order-processor",
            vec![CommittedOffset {
                topic: TOPIC.into(),
                partition: 0,
                offset: 6,
            }],
        );

        let error = plan_reset(
            &cluster,
            "order-processor",
            &topic(None),
            ResetTarget::Shift(-1),
        )
        .await
        .unwrap_err();

        assert_eq!(
            error.to_string(),
            "invalid request: orders.created/1 has no committed offset to shift from"
        );
    }

    #[tokio::test]
    async fn a_timestamp_finds_the_first_record_at_or_after_it() {
        let plan = plan(topic(None), ResetTarget::Timestamp(1_700_000_004_500))
            .await
            .unwrap();

        assert_eq!(moves(&plan), vec![(0, Some(6), 5), (1, Some(5), 6)]);
    }

    #[tokio::test]
    async fn a_timestamp_past_every_record_lands_on_the_end() {
        let plan = plan(topic(None), ResetTarget::Timestamp(i64::MAX))
            .await
            .unwrap();

        assert_eq!(moves(&plan), vec![(0, Some(6), 8), (1, Some(5), 8)]);
    }

    #[tokio::test]
    async fn a_partition_the_group_never_committed_has_no_current_offset() {
        let cluster = FakeCluster::local();
        cluster.commit_offsets("order-processor", Vec::new());

        let plan = plan_reset(
            &cluster,
            "order-processor",
            &topic(None),
            ResetTarget::Earliest,
        )
        .await
        .unwrap();

        assert_eq!(moves(&plan), vec![(0, None, 0), (1, None, 0)]);
    }

    #[tokio::test]
    async fn the_committed_scope_covers_what_the_group_committed() {
        let cluster = FakeCluster::local();
        cluster.commit_offsets(
            "order-processor",
            vec![CommittedOffset {
                topic: TOPIC.into(),
                partition: 1,
                offset: 5,
            }],
        );

        let plan = plan_reset(
            &cluster,
            "order-processor",
            &ResetScope::Committed,
            ResetTarget::Latest,
        )
        .await
        .unwrap();

        assert_eq!(moves(&plan), vec![(1, Some(5), 8)]);
    }

    #[tokio::test]
    async fn a_group_without_commits_plans_nothing_for_the_committed_scope() {
        let plan = plan_reset(
            &FakeCluster::local(),
            "never-committed",
            &ResetScope::Committed,
            ResetTarget::Earliest,
        )
        .await
        .unwrap();

        assert!(plan.is_empty());
    }

    #[tokio::test]
    async fn an_unknown_partition_or_topic_is_refused() {
        let partition = plan(topic(Some(vec![0, 7])), ResetTarget::Earliest)
            .await
            .unwrap_err();
        let missing = plan(
            ResetScope::Topic {
                topic: "ghost".into(),
                partitions: None,
            },
            ResetTarget::Earliest,
        )
        .await
        .unwrap_err();

        assert_eq!(partition.code(), "UNKNOWN_PARTITION");
        assert_eq!(missing.code(), "UNKNOWN_TOPIC");
    }

    #[tokio::test]
    async fn an_empty_partition_list_is_refused_rather_than_read_as_all() {
        let error = plan(topic(Some(Vec::new())), ResetTarget::Earliest)
            .await
            .unwrap_err();

        assert_eq!(error.code(), "INVALID_REQUEST");
    }

    #[test]
    fn clamping_tolerates_an_inverted_window() {
        assert_eq!(clamp(5, Watermarks { low: 3, high: 9 }), 5);
        assert_eq!(clamp(1, Watermarks { low: 3, high: 9 }), 3);
        assert_eq!(clamp(12, Watermarks { low: 3, high: 9 }), 9);
        assert_eq!(clamp(7, Watermarks { low: 4, high: 2 }), 4);
    }
}
