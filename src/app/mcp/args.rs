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

/// Declares the arguments of a tool that reads one cluster. Every such tool
/// takes a `cluster` first, and the schema client sees it with the same text.
macro_rules! input {
    ($(#[$meta:meta])* struct $name:ident { $($fields:tt)* }) => {
        #[derive(serde::Deserialize, schemars::JsonSchema)]
        #[serde(rename_all = "camelCase", deny_unknown_fields)]
        $(#[$meta])*
        struct $name {
            /// A cluster name from klens_clusters. Optional when you see only one.
            cluster: Option<String>,
            $($fields)*
        }
    };
}
pub(super) use input;
