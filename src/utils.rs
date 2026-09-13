use chrono::{DateTime, Utc};

pub(crate) fn utc_now() -> DateTime<Utc> {
    Utc::now()
}

pub(crate) fn unix_timestamp_secs() -> i64 {
    Utc::now().timestamp()
}

pub(crate) fn unix_timestamp_millis() -> f64 {
    Utc::now().timestamp_millis() as f64
}

pub(crate) fn datetime_from_unix_millis(millis: i64) -> DateTime<Utc> {
    DateTime::from_timestamp_millis(millis).unwrap_or(DateTime::UNIX_EPOCH)
}
