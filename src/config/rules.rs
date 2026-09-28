//! The garde rules the config types name, and the ordered map they dive into.

use std::fmt::Display;
use std::hash::Hash;
use std::ops::Deref;
use std::time::Duration;

use garde::Validate;
use indexmap::IndexMap;
use openidconnect::{IssuerUrl, RedirectUrl};
use serde::Deserialize;
use url::Url;

pub(super) const EMPTY: &str = "must not be empty";

pub(super) fn not_blank(text: &str, _: &()) -> garde::Result {
    if text.trim().is_empty() {
        return Err(garde::Error::new(EMPTY));
    }
    Ok(())
}

/// For a loop that would spin on a shorter period.
pub(super) fn at_least_a_second(period: &Duration, _: &()) -> garde::Result {
    if *period < Duration::from_secs(1) {
        return Err(garde::Error::new(format!(
            "must be at least 1s, got {period:?}"
        )));
    }
    Ok(())
}

/// A URL type whose parsed form [`http_url`] can check.
pub(super) trait ParsedUrl {
    fn parsed(&self) -> &Url;
}

impl ParsedUrl for Url {
    fn parsed(&self) -> &Url {
        self
    }
}

impl ParsedUrl for IssuerUrl {
    fn parsed(&self) -> &Url {
        self.url()
    }
}

impl ParsedUrl for RedirectUrl {
    fn parsed(&self) -> &Url {
        self.url()
    }
}

pub(super) fn http_url<U: ParsedUrl>(url: &U, _: &()) -> garde::Result {
    let url = url.parsed();
    if url.scheme() != "http" && url.scheme() != "https" {
        return Err(garde::Error::new("must be an http or https URL"));
    }
    if url.host_str().is_none() {
        return Err(garde::Error::new("must include a host"));
    }
    Ok(())
}

/// A map in file order, which garde has no impl for. Each value validates
/// under its key, so an error names the cluster or role it is in.
#[derive(Debug, Clone, Deserialize)]
#[serde(
    transparent,
    bound = "K: Deserialize<'de> + Eq + Hash, V: Deserialize<'de>"
)]
pub struct Ordered<K, V>(IndexMap<K, V>);

impl<K, V> Default for Ordered<K, V> {
    fn default() -> Self {
        Self(IndexMap::new())
    }
}

impl<K, V> Deref for Ordered<K, V> {
    type Target = IndexMap<K, V>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[cfg(test)]
impl<K: Eq + Hash, V> FromIterator<(K, V)> for Ordered<K, V> {
    fn from_iter<I: IntoIterator<Item = (K, V)>>(entries: I) -> Self {
        Self(entries.into_iter().collect())
    }
}

impl<K: Display, V: Validate> Validate for Ordered<K, V> {
    type Context = V::Context;

    fn validate_into(
        &self,
        ctx: &Self::Context,
        parent: &mut dyn FnMut() -> garde::Path,
        report: &mut garde::Report,
    ) {
        for (key, value) in &self.0 {
            value.validate_into(ctx, &mut || parent().join(key.to_string()), report);
        }
    }
}
