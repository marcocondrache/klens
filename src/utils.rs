use jiff::Timestamp;

pub(crate) fn utc_now() -> Timestamp {
    Timestamp::now()
}

pub(crate) fn unix_timestamp_secs() -> i64 {
    Timestamp::now().as_second()
}

pub(crate) fn timestamp_from_unix_millis(millis: i64) -> Timestamp {
    Timestamp::from_millisecond(millis).unwrap_or(Timestamp::UNIX_EPOCH)
}
