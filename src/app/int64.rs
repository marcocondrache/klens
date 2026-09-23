use std::fmt;

use serde::de::{self, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use ts_rs::TS;

/// Signed 64-bit integer, serialized as a string.
///
/// Offsets, watermarks, lag and retained counts routinely pass 2^53, where a
/// JSON number silently loses precision in every JavaScript client. A string
/// crosses the wire intact. Input accepts either a string or an integer.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, TS)]
#[ts(type = "string")]
pub struct Int64(i64);

impl Serialize for Int64 {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for Int64 {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Int64Visitor;

        impl Visitor<'_> for Int64Visitor {
            type Value = Int64;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a string or integer")
            }

            fn visit_str<E: de::Error>(self, value: &str) -> Result<Int64, E> {
                value.parse().map(Int64).map_err(E::custom)
            }

            fn visit_i64<E: de::Error>(self, value: i64) -> Result<Int64, E> {
                Ok(Int64(value))
            }

            fn visit_u64<E: de::Error>(self, value: u64) -> Result<Int64, E> {
                i64::try_from(value).map(Int64).map_err(E::custom)
            }
        }

        deserializer.deserialize_any(Int64Visitor)
    }
}

impl From<i64> for Int64 {
    fn from(value: i64) -> Self {
        Self(value)
    }
}

impl From<u64> for Int64 {
    fn from(value: u64) -> Self {
        Self(value as i64)
    }
}

impl From<i32> for Int64 {
    fn from(value: i32) -> Self {
        Self(i64::from(value))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn large_values_round_trip_as_strings() {
        let offset = Int64::from(9_007_199_254_740_993_i64);
        let json = serde_json::to_value(offset).expect("json");

        assert_eq!(json, serde_json::json!("9007199254740993"));
        assert_eq!(
            serde_json::from_value::<Int64>(json).expect("parse"),
            offset
        );
    }

    #[test]
    fn small_values_may_arrive_as_numbers() {
        let parsed = serde_json::from_value::<Int64>(serde_json::json!(42)).expect("parse");

        assert_eq!(parsed, Int64::from(42));
    }
}
