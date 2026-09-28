//! Durations written like `250ms`, `10s`, `1h 30m` or `PT10S`, for serde's
//! `with`. jiff parses them, from a string read first: serde-saphyr reports a
//! list or map handed to jiff's own `deserialize_str` as the end of the file.

use std::time::Duration;

use jiff::fmt::serde::unsigned_duration;
use serde::de::IntoDeserializer;
use serde::{Deserialize, Deserializer};

fn parse<'de, D: Deserializer<'de>>(text: String) -> Result<Duration, D::Error> {
    unsigned_duration::required::deserialize(IntoDeserializer::<D::Error>::into_deserializer(text))
}

pub(super) mod required {
    use super::*;

    pub(in crate::config) fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Duration, D::Error> {
        parse::<D>(String::deserialize(deserializer)?)
    }
}

pub(super) mod optional {
    use super::*;

    pub(in crate::config) fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Option<Duration>, D::Error> {
        Option::<String>::deserialize(deserializer)?
            .map(parse::<D>)
            .transpose()
    }
}
