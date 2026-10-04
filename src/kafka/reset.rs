use foldhash::{HashMap, HashMapExt};
use futures::future::try_join_all;

use crate::kafka::error::KafkaError;
use crate::kafka::group::CommittedOffset;
use crate::kafka::metadata::Watermarks;
use crate::kafka::session::{ClusterSession, watermarks};

/// Where a reset moves each committed offset. Every target lands between the
/// partition's low and high watermarks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResetTarget {
    Earliest,
    Latest,
    Offset(i64),
    Shift(i64),
    /// The first record at or after this time, in milliseconds since the epoch.
    Timestamp(i64),
}

impl ResetTarget {
    /// `at_time` is the partition's first offset at or after a timestamp
    /// target. `None` when a shift has no committed offset to start from.
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
    /// Without a topic, the reset covers every partition the group has
    /// committed an offset for.
    pub topic: Option<String>,
    /// Partitions of `topic`, or every partition of it when empty.
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

/// Plans a reset of `group` from its live committed offsets and watermarks.
/// Without `partitions`, it covers every partition the group has committed.
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
    let marks = watermarks(session, &wanted).await?;
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
                return Err(KafkaError::Admin(format!(
                    "no watermarks for partition {partition} of '{topic}'"
                )));
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
    use super::*;

    const MARKS: Watermarks = Watermarks { low: 10, high: 50 };

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
