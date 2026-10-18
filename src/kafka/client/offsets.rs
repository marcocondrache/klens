use foldhash::{HashMap, HashMapExt};
use krafka::error::{ErrorCode, KrafkaError};

use crate::kafka::error::KafkaError;

/// krafka 0.27 reports Kafka's -1 for a timestamp past the last record as
/// OffsetNotAvailable with this message. A broker sends the same code while
/// its high watermark lags after a leader election, so only the message tells
/// "no record" from a failed lookup.
const NO_RECORD: &str = "the broker reported no offset for this spec";

pub fn time_offsets(
    listed: impl IntoIterator<Item = (String, i32, Result<i64, KrafkaError>)>,
) -> Result<HashMap<i32, Option<i64>>, KafkaError> {
    let mut offsets = HashMap::new();
    for (_, partition, offset) in listed {
        let offset = match offset {
            Ok(offset) => Some(offset),
            Err(KrafkaError::Broker {
                code: ErrorCode::OffsetNotAvailable,
                message,
            }) if message == NO_RECORD => None,
            Err(KrafkaError::Broker {
                code: ErrorCode::UnknownTopicOrPartition,
                ..
            }) => continue,
            Err(error) => return Err(error.into()),
        };
        offsets.insert(partition, offset);
    }
    Ok(offsets)
}

pub fn known_offsets(
    listed: impl IntoIterator<Item = (String, i32, Result<i64, KrafkaError>)>,
) -> HashMap<String, HashMap<i32, i64>> {
    let mut out: HashMap<String, HashMap<i32, i64>> = HashMap::new();
    for (topic, partition, offset) in listed {
        if let Ok(offset) = offset {
            out.entry(topic).or_default().insert(partition, offset);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn broker(code: ErrorCode, message: &str) -> Result<i64, KrafkaError> {
        Err(KrafkaError::broker(code, message))
    }

    fn denied() -> Result<i64, KrafkaError> {
        broker(ErrorCode::TopicAuthorizationFailed, "denied")
    }

    fn unknown() -> Result<i64, KrafkaError> {
        broker(ErrorCode::UnknownTopicOrPartition, "unknown")
    }

    #[test]
    fn time_offsets_tell_no_record_from_a_failed_lookup() {
        let offsets = time_offsets([
            ("orders".into(), 0, Ok(12)),
            ("orders".into(), 1, unknown()),
            (
                "orders".into(),
                2,
                broker(ErrorCode::OffsetNotAvailable, NO_RECORD),
            ),
        ])
        .expect("time offsets");
        assert_eq!(offsets, HashMap::from_iter([(0, Some(12)), (2, None)]));

        for failure in [
            denied(),
            broker(ErrorCode::OffsetNotAvailable, "OffsetNotAvailable"),
        ] {
            let failed =
                time_offsets([("orders".into(), 0, Ok(12)), ("orders".into(), 1, failure)]);
            assert!(failed.is_err(), "{failed:?}");
        }
    }

    #[test]
    fn known_offsets_skip_partitions_without_an_offset() {
        let offsets = known_offsets([
            ("orders".into(), 0, Ok(0)),
            ("orders".into(), 1, Ok(12)),
            ("orders".into(), 2, denied()),
            ("payments".into(), 0, unknown()),
        ]);

        assert_eq!(
            offsets,
            HashMap::from_iter([("orders".into(), HashMap::from_iter([(0, 0), (1, 12)]))])
        );
    }
}
