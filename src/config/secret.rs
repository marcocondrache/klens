//! Secrets name where to read them, so none has to sit in the config file.

use std::fmt::Formatter;
use std::path::PathBuf;

use secrecy::{ExposeSecret, SecretString};
use serde::de::value::MapAccessDeserializer;
use serde::de::{self, MapAccess, Visitor};
use serde::{Deserialize, Deserializer};

use super::rules::EMPTY;

/// A secret read from the source the config names: `{value: ...}`,
/// `{env: NAME}` or `{file: PATH}`. It is never blank.
#[derive(Debug, Clone)]
pub struct Secret(SecretString);

impl ExposeSecret<str> for Secret {
    fn expose_secret(&self) -> &str {
        self.0.expose_secret()
    }
}

impl<'de> Deserialize<'de> for Secret {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let secret = read(deserializer)?;
        if secret.expose_secret().trim().is_empty() {
            return Err(de::Error::custom(EMPTY));
        }
        Ok(Self(secret))
    }
}

#[cfg(test)]
impl From<&str> for Secret {
    fn from(text: &str) -> Self {
        Self(text.into())
    }
}

/// Reads a secret from its source, for a type that checks the text itself.
pub(super) fn read<'de, D: Deserializer<'de>>(deserializer: D) -> Result<SecretString, D::Error> {
    deserializer
        .deserialize_any(SourceVisitor)?
        .resolve()
        .map_err(de::Error::custom)
}

#[derive(Deserialize)]
#[serde(rename_all = "lowercase")]
enum Source {
    Value(Text),
    Env(String),
    File(PathBuf),
}

impl Source {
    fn resolve(self) -> Result<SecretString, String> {
        let secret = match self {
            Self::Value(Text(value)) => value,
            Self::Env(name) => match std::env::var(&name) {
                Ok(value) => value.into(),
                Err(std::env::VarError::NotPresent) => {
                    return Err(format!("environment variable {name} is not set"));
                }
                Err(std::env::VarError::NotUnicode(_)) => {
                    return Err(format!("environment variable {name} is not valid UTF-8"));
                }
            },
            Self::File(path) => {
                let mut contents = std::fs::read_to_string(&path).map_err(|error| {
                    format!("failed to read secret file {}: {error}", path.display())
                })?;
                contents.truncate(contents.trim_end_matches(['\r', '\n']).len());
                contents.into()
            }
        };

        Ok(secret)
    }
}

/// serde's default errors quote the value they reject, which here is the
/// secret, so both visitors reject scalars with their own message.
macro_rules! reject_scalars {
    ($message:expr) => {
        fn visit_bool<E: de::Error>(self, _: bool) -> Result<Self::Value, E> {
            Err(E::custom($message))
        }

        fn visit_i64<E: de::Error>(self, _: i64) -> Result<Self::Value, E> {
            Err(E::custom($message))
        }

        fn visit_u64<E: de::Error>(self, _: u64) -> Result<Self::Value, E> {
            Err(E::custom($message))
        }

        fn visit_f64<E: de::Error>(self, _: f64) -> Result<Self::Value, E> {
            Err(E::custom($message))
        }
    };
}

const UNNAMED: &str = "a secret must name its source: {value: ...}, {env: NAME} or {file: PATH}";

struct SourceVisitor;

impl<'de> Visitor<'de> for SourceVisitor {
    type Value = Source;

    fn expecting(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(UNNAMED)
    }

    fn visit_map<A: MapAccess<'de>>(self, map: A) -> Result<Source, A::Error> {
        Source::deserialize(MapAccessDeserializer::new(map))
    }

    fn visit_str<E: de::Error>(self, _: &str) -> Result<Source, E> {
        Err(E::custom(UNNAMED))
    }

    reject_scalars!(UNNAMED);
}

/// The text under `value`.
struct Text(SecretString);

const NOT_TEXT: &str = "a secret value must be a string; quote it";

impl<'de> Deserialize<'de> for Text {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct TextVisitor;

        impl Visitor<'_> for TextVisitor {
            type Value = Text;

            fn expecting(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
                formatter.write_str(NOT_TEXT)
            }

            fn visit_str<E: de::Error>(self, value: &str) -> Result<Text, E> {
                Ok(Text(value.into()))
            }

            reject_scalars!(NOT_TEXT);
        }

        deserializer.deserialize_any(TextVisitor)
    }
}
