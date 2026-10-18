use foldhash::{HashMap, HashMapExt};
use krafka::error::{ErrorCode, KrafkaError};

use crate::kafka::error::KafkaError;
use crate::kafka::session::OffsetLookup;

/// krafka 0.27 reports Kafka's -1 for a timestamp past the last record as
/// OffsetNotAvailable with this message. A broker sends the same code while
/// its high watermark lags after a leader election, so only the message tells
/// "no record" from a failed lookup.
const NO_RECORD: &str = "the broker reported no offset for this spec";

pub fn log_offsets(
    listed: impl IntoIterator<Item = (String, i32, Result<i64, KrafkaError>)>,
) -> OffsetLookup {
    let mut lookup = OffsetLookup::default();
    for (topic, partition, offset) in listed {
        match offset {
            Ok(offset) => lookup.insert(topic, partition, offset),
            Err(KrafkaError::Broker {
                code: ErrorCode::UnknownTopicOrPartition,
                ..
            }) => {}
            Err(error) => lookup.fail(error.into()),
        }
    }
    lookup
}

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
    fn log_offsets_keep_a_failed_partition_apart_from_an_unknown_one() {
        let listed = || {
            [
                ("orders".into(), 0, Ok(0)),
                ("orders".into(), 1, Ok(12)),
                ("payments".into(), 0, unknown()),
            ]
        };
        let answered =
            HashMap::from_iter([("orders".into(), HashMap::from_iter([(0, 0), (1, 12)]))]);

        assert_eq!(
            log_offsets(listed()).complete().expect("no failure"),
            answered
        );

        let failed = || listed().into_iter().chain([("orders".into(), 2, denied())]);
        assert_eq!(log_offsets(failed()).found(), answered);
        assert!(log_offsets(failed()).complete().is_err());
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
}
