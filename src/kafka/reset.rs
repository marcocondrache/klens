use foldhash::{HashMap, HashMapExt};
use futures::future::try_join_all;

use crate::kafka::error::KafkaError;
use crate::kafka::group::CommittedOffset;
use crate::kafka::metadata::Watermarks;
use crate::kafka::session::{ClusterSession, complete_watermarks};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResetTarget {
    Earliest,
    Latest,
    Offset(i64),
    Shift(i64),
    Timestamp(i64),
}

impl ResetTarget {
    fn place(self, from: Option<i64>, marks: Watermarks, at_time: Option<i64>) -> Option<i64> {
        let offset = match self {
            Self::Earliest => marks.low,
            Self::Latest => marks.high,
            Self::Offset(offset) => offset,
            Self::Shift(by) => from?.saturating_add(by),
            Self::Timestamp(_) => at_time.unwrap_or(marks.high),
        };
        Some(offset.clamp(marks.low, marks.high))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffsetReset {
    pub group: String,
    pub topic: Option<String>,
    pub partitions: Vec<i32>,
    pub to: ResetTarget,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffsetMove {
    pub topic: String,
    pub partition: i32,
    pub from: Option<i64>,
    pub to: i64,
    pub end: i64,
}

impl OffsetMove {
    pub fn committed(&self) -> CommittedOffset {
        CommittedOffset {
            topic: self.topic.clone(),
            partition: self.partition,
            offset: self.to,
        }
    }
}

pub async fn plan_reset(
    session: &dyn ClusterSession,
    group: &str,
    partitions: Option<(&str, &[i32])>,
    to: ResetTarget,
) -> Result<Vec<OffsetMove>, KafkaError> {
    let committed = session.committed_offsets(group, None).await?;
    let mut wanted: HashMap<String, Vec<i32>> = HashMap::new();
    match partitions {
        Some((topic, partitions)) => {
            wanted.insert(topic.to_owned(), partitions.to_vec());
        }
        None => {
            for offset in &committed {
                wanted
                    .entry(offset.topic.clone())
                    .or_default()
                    .push(offset.partition);
            }
        }
    }
    let marks = complete_watermarks(session, &wanted).await?;
    let at_times = match to {
        ResetTarget::Timestamp(timestamp) => offsets_at(session, &wanted, timestamp).await?,
        _ => HashMap::new(),
    };
    let current: HashMap<(&str, i32), i64> = committed
        .iter()
        .map(|offset| ((offset.topic.as_str(), offset.partition), offset.offset))
        .collect();

    let mut moves = Vec::new();
    for (topic, partitions) in &wanted {
        for &partition in partitions {
            let Some(&mark) = marks.get(topic).and_then(|marks| marks.get(&partition)) else {
                return Err(KafkaError::UnknownPartition {
                    cluster: session.identity().name.clone(),
                    topic: topic.clone(),
                    partition,
                });
            };
            let from = current.get(&(topic.as_str(), partition)).copied();
            let at_time = at_times
                .get(topic)
                .and_then(|offsets| offsets.get(&partition))
                .copied()
                .flatten();
            let to =
                to.place(from, mark, at_time)
                    .ok_or_else(|| KafkaError::NoCommittedOffset {
                        group: group.to_owned(),
                        topic: topic.clone(),
                        partition,
                    })?;
            moves.push(OffsetMove {
                topic: topic.clone(),
                partition,
                from,
                to,
                end: mark.high,
            });
        }
    }
    moves.sort_unstable_by(|left, right| {
        (&left.topic, left.partition).cmp(&(&right.topic, right.partition))
    });
    Ok(moves)
}

async fn offsets_at(
    session: &dyn ClusterSession,
    wanted: &HashMap<String, Vec<i32>>,
    timestamp: i64,
) -> Result<HashMap<String, HashMap<i32, Option<i64>>>, KafkaError> {
    let found = try_join_all(wanted.iter().map(|(topic, partitions)| async move {
        let offsets = session
            .offsets_for_times(topic, partitions, timestamp)
            .await?;
        Ok::<_, KafkaError>((topic.clone(), offsets))
    }))
    .await?;
    Ok(found.into_iter().collect())
}

#[cfg(test)]
mod tests {
    use krafka::error::{ErrorCode, KrafkaError};
    use krafka::testing::{ApiKey, Control};

    use super::*;
    use crate::kafka::client::testing::Broker;

    const MARKS: Watermarks = Watermarks { low: 10, high: 50 };

    fn assert_denied(planned: &Result<Vec<OffsetMove>, KafkaError>) {
        assert!(
            matches!(
                planned,
                Err(KafkaError::Krafka(KrafkaError::Broker {
                    code: ErrorCode::TopicAuthorizationFailed,
                    ..
                }))
            ),
            "{planned:?}"
        );
    }

    #[tokio::test]
    async fn a_reset_fails_when_kafka_fails_to_list_either_end_of_the_log() {
        let broker = Broker::orders(4).await;
        let client = broker.client().await;
        broker.on_once(ApiKey::ListOffsets, |_| {
            Control::Error(ErrorCode::TopicAuthorizationFailed)
        });

        let planned = plan_reset(
            &client,
            "billing",
            Some(("orders", &[0])),
            ResetTarget::Earliest,
        )
        .await;

        assert_denied(&planned);
    }

    #[tokio::test]
    async fn a_reset_by_time_fails_when_kafka_fails_the_time_lookup() {
        let broker = Broker::orders(4).await;
        let client = broker.client().await;
        broker.clear_requests();
        // The two watermark lookups come first.
        broker.on(ApiKey::ListOffsets, |request| {
            if request.api_call_index < 2 {
                Control::Pass
            } else {
                Control::Error(ErrorCode::TopicAuthorizationFailed)
            }
        });

        let planned = plan_reset(
            &client,
            "billing",
            Some(("orders", &[0])),
            ResetTarget::Timestamp(0),
        )
        .await;

        assert_denied(&planned);
    }

    #[test]
    fn the_log_ends_place_at_the_watermarks() {
        assert_eq!(ResetTarget::Earliest.place(Some(30), MARKS, None), Some(10));
        assert_eq!(ResetTarget::Latest.place(Some(30), MARKS, None), Some(50));
    }

    #[test]
    fn an_offset_outside_the_log_clamps_to_its_nearest_end() {
        assert_eq!(ResetTarget::Offset(25).place(None, MARKS, None), Some(25));
        assert_eq!(ResetTarget::Offset(3).place(None, MARKS, None), Some(10));
        assert_eq!(ResetTarget::Offset(90).place(None, MARKS, None), Some(50));
    }

    #[test]
    fn a_shift_moves_from_the_committed_offset_and_clamps() {
        assert_eq!(
            ResetTarget::Shift(-5).place(Some(30), MARKS, None),
            Some(25)
        );
        assert_eq!(
            ResetTarget::Shift(-40).place(Some(30), MARKS, None),
            Some(10)
        );
        assert_eq!(
            ResetTarget::Shift(40).place(Some(30), MARKS, None),
            Some(50)
        );
        assert_eq!(
            ResetTarget::Shift(i64::MAX).place(Some(30), MARKS, None),
            Some(50)
        );
        assert_eq!(ResetTarget::Shift(-5).place(None, MARKS, None), None);
    }

    #[test]
    fn a_time_past_the_last_record_places_at_the_end() {
        let at = ResetTarget::Timestamp(1_700_000_000_000);

        assert_eq!(at.place(Some(30), MARKS, Some(20)), Some(20));
        assert_eq!(at.place(Some(30), MARKS, None), Some(50));
    }
}
