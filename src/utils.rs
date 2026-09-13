//! Shared timestamp/datetime helpers.
//!
//! The crate mixes three clock representations (unix seconds, unix millis,
//! and `chrono` UTC datetimes) across auth sessions, catalog snapshots, and
//! record queries. Centralizing the epoch conversions here keeps the
//! fallback behavior (clock skew, out-of-range millis) consistent everywhere.

use std::time::{SystemTime, UNIX_EPOCH};

use chrono::{DateTime, Utc};

/// Current wall-clock time as a UTC `DateTime`.
pub(crate) fn utc_now() -> DateTime<Utc> {
    Utc::now()
}

/// Seconds since the Unix epoch, per the system clock.
///
/// Returns `0` if the system clock is set before the epoch.
pub(crate) fn unix_timestamp_secs() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

/// Milliseconds since the Unix epoch, per the system clock.
///
/// Returns `0.0` if the system clock is set before the epoch.
pub(crate) fn unix_timestamp_millis() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis() as f64)
        .unwrap_or(0.0)
}

/// Convert milliseconds since the Unix epoch (e.g. a Kafka record timestamp)
/// into a UTC `DateTime`, falling back to the epoch for out-of-range values.
pub(crate) fn datetime_from_unix_millis(millis: i64) -> DateTime<Utc> {
    DateTime::from_timestamp_millis(millis).unwrap_or(DateTime::UNIX_EPOCH)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn utc_now_tracks_the_system_clock() {
        let before = SystemTime::now();
        let now = utc_now();
        let after = SystemTime::now();

        assert!(DateTime::<Utc>::from(before) <= now);
        assert!(now <= DateTime::<Utc>::from(after));
    }

    #[test]
    fn unix_timestamp_secs_tracks_the_system_clock() {
        let before = unix_timestamp_secs();
        let system_secs = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;

        assert!((system_secs - before).abs() <= 1);
    }

    #[test]
    fn unix_timestamp_millis_tracks_the_system_clock() {
        let before = unix_timestamp_millis();
        let system_millis = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as f64;

        assert!((system_millis - before).abs() <= 1_000.0);
    }

    #[test]
    fn datetime_from_unix_millis_round_trips() {
        let millis = 1_700_000_000_123;
        assert_eq!(datetime_from_unix_millis(millis).timestamp_millis(), millis);
    }

    #[test]
    fn datetime_from_unix_millis_treats_zero_as_the_epoch() {
        assert_eq!(datetime_from_unix_millis(0), DateTime::<Utc>::UNIX_EPOCH);
    }

    #[test]
    fn datetime_from_unix_millis_falls_back_to_the_epoch_when_out_of_range() {
        assert_eq!(
            datetime_from_unix_millis(i64::MAX),
            DateTime::<Utc>::UNIX_EPOCH
        );
        assert_eq!(
            datetime_from_unix_millis(i64::MIN),
            DateTime::<Utc>::UNIX_EPOCH
        );
    }
}
