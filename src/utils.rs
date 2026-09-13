//! Shared timestamp/datetime helpers.
//!
//! The crate mixes three clock representations (unix seconds, unix millis,
//! and `chrono` UTC datetimes) across auth sessions, catalog snapshots, and
//! record queries. Centralizing the epoch conversions here keeps every call
//! site consistent, even though each wrapper is a thin pass-through to
//! `chrono`.

use chrono::{DateTime, Utc};

/// Current wall-clock time as a UTC `DateTime`.
pub(crate) fn utc_now() -> DateTime<Utc> {
    Utc::now()
}

/// Seconds since the Unix epoch, per the system clock.
pub(crate) fn unix_timestamp_secs() -> i64 {
    Utc::now().timestamp()
}

/// Milliseconds since the Unix epoch, per the system clock.
pub(crate) fn unix_timestamp_millis() -> f64 {
    Utc::now().timestamp_millis() as f64
}

/// Convert milliseconds since the Unix epoch (e.g. a Kafka record timestamp)
/// into a UTC `DateTime`, falling back to the epoch for out-of-range values.
pub(crate) fn datetime_from_unix_millis(millis: i64) -> DateTime<Utc> {
    DateTime::from_timestamp_millis(millis).unwrap_or(DateTime::UNIX_EPOCH)
}
