//! Field types that check their own value while they deserialize, so a config
//! struct names what a field accepts and a bad value fails at its own line.

use std::fmt::Display;
use std::ops::Deref;
use std::str::FromStr;
use std::time::Duration;

use jiff::SignedDuration;
use openidconnect::{IssuerUrl, RedirectUrl};
use serde::Deserialize;
use serde::de::{self, Deserializer};
use url::Url;

pub(super) const EMPTY: &str = "must not be empty";

/// Reads a string and parses it, for a type whose rules live in `FromStr`.
pub(super) fn parsed<'de, D, T>(deserializer: D) -> Result<T, D::Error>
where
    D: Deserializer<'de>,
    T: FromStr<Err: Display>,
{
    String::deserialize(deserializer)?
        .parse()
        .map_err(de::Error::custom)
}

/// A list with at least one item.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(try_from = "Vec<T>")]
pub struct NonEmpty<T>(Vec<T>);

impl<T> TryFrom<Vec<T>> for NonEmpty<T> {
    type Error = &'static str;

    fn try_from(items: Vec<T>) -> Result<Self, Self::Error> {
        if items.is_empty() {
            return Err(EMPTY);
        }
        Ok(Self(items))
    }
}

impl<T> Deref for NonEmpty<T> {
    type Target = [T];

    fn deref(&self) -> &[T] {
        &self.0
    }
}

impl<T> IntoIterator for NonEmpty<T> {
    type Item = T;
    type IntoIter = std::vec::IntoIter<T>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

/// Text that is more than whitespace, kept as written.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NonBlank(String);

impl NonBlank {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl FromStr for NonBlank {
    type Err = &'static str;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        if text.trim().is_empty() {
            return Err(EMPTY);
        }
        Ok(Self(text.to_owned()))
    }
}

impl<'de> Deserialize<'de> for NonBlank {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        parsed(deserializer)
    }
}

/// An http or https URL with a host, read into `U`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpUrl<U = Url>(U);

/// A URL type that [`HttpUrl`] can hold. The OIDC types keep the text as
/// written, which discovery compares byte for byte; [`Url`] would add a slash
/// after a bare host.
pub trait UrlText: Sized {
    fn parse(text: &str) -> Result<Self, url::ParseError>;

    fn url(&self) -> &Url;
}

impl UrlText for Url {
    fn parse(text: &str) -> Result<Self, url::ParseError> {
        Url::parse(text)
    }

    fn url(&self) -> &Url {
        self
    }
}

impl UrlText for IssuerUrl {
    fn parse(text: &str) -> Result<Self, url::ParseError> {
        Self::new(text.to_owned())
    }

    fn url(&self) -> &Url {
        self.url()
    }
}

impl UrlText for RedirectUrl {
    fn parse(text: &str) -> Result<Self, url::ParseError> {
        Self::new(text.to_owned())
    }

    fn url(&self) -> &Url {
        self.url()
    }
}

impl<U> Deref for HttpUrl<U> {
    type Target = U;

    fn deref(&self) -> &U {
        &self.0
    }
}

impl<U: UrlText> FromStr for HttpUrl<U> {
    type Err = String;

    fn from_str(text: &str) -> Result<Self, String> {
        let value = U::parse(text).map_err(|error| format!("not a valid URL: {error}"))?;
        let url = value.url();

        if url.scheme() != "http" && url.scheme() != "https" {
            return Err("must be an http or https URL".to_owned());
        }

        if url.host_str().is_none() {
            return Err("must include a host".to_owned());
        }

        Ok(Self(value))
    }
}

impl<'de, U: UrlText> Deserialize<'de> for HttpUrl<U> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        parsed(deserializer)
    }
}

/// A duration written like `250ms`, `10s`, `1h 30m` or `PT10S`, and at least
/// `MIN_SECS` seconds long.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Period<const MIN_SECS: u64 = 0>(Duration);

impl<const MIN_SECS: u64> Period<MIN_SECS> {
    pub const fn from_secs(secs: u64) -> Self {
        assert!(secs >= MIN_SECS, "period is below its minimum");
        Self(Duration::from_secs(secs))
    }

    pub const fn get(self) -> Duration {
        self.0
    }
}

impl Period {
    pub const fn from_millis(millis: u64) -> Self {
        Self(Duration::from_millis(millis))
    }
}

impl<const MIN_SECS: u64> FromStr for Period<MIN_SECS> {
    type Err = String;

    fn from_str(text: &str) -> Result<Self, String> {
        let signed: SignedDuration = text
            .parse()
            .map_err(|error: jiff::Error| error.to_string())?;
        let duration =
            Duration::try_from(signed).map_err(|_| format!("must not be negative, got {text}"))?;
        let min = Duration::from_secs(MIN_SECS);
        if duration < min {
            return Err(format!("must be at least {min:?}, got {text}"));
        }
        Ok(Self(duration))
    }
}

impl<'de, const MIN_SECS: u64> Deserialize<'de> for Period<MIN_SECS> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        parsed(deserializer)
    }
}
