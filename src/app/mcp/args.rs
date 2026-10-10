//! Arguments several tools take alike.

use std::cmp::Ordering;

use schemars::JsonSchema;
use serde::Deserialize;

#[derive(Clone, Copy, Default, PartialEq, Eq, Deserialize, JsonSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
#[schemars(inline)]
pub(super) enum ResponseFormat {
    #[default]
    Concise,
    Detailed,
}

impl ResponseFormat {
    pub fn is_detailed(self) -> bool {
        self == Self::Detailed
    }
}

/// Keeps names that contain the text given, ignoring case, and every name when
/// none is given.
pub(super) struct NameFilter(Option<String>);

impl NameFilter {
    pub fn new(contains: Option<&str>) -> Self {
        Self(contains.map(str::to_lowercase))
    }

    pub fn matches(&self, name: &str) -> bool {
        self.0
            .as_deref()
            .is_none_or(|needle| name.to_lowercase().contains(needle))
    }
}

/// Puts the largest value first and a value klens has not measured last.
pub(super) fn largest_first<T>(
    a: Option<T>,
    b: Option<T>,
    order: impl FnOnce(&T, &T) -> Ordering,
) -> Ordering {
    match (a, b) {
        (Some(a), Some(b)) => order(&b, &a),
        (a, b) => a.is_none().cmp(&b.is_none()),
    }
}
