use std::collections::HashSet;
use std::fmt::{Display, Formatter};
use std::hash::Hash;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;

use indexmap::IndexMap;
use openidconnect::{IssuerUrl, RedirectUrl};
use regex::{Regex, RegexBuilder};
use secrecy::{ExposeSecret, SecretString};
use serde::de::value::{MapAccessDeserializer, SeqAccessDeserializer, StrDeserializer};
use serde::de::{self, MapAccess, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer};
use thiserror::Error;
use url::Url;

use crate::environment;

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("failed to read config file {}: {source}", path.display())]
    Read {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to parse config file {}: {source}", path.display())]
    Parse {
        path: PathBuf,
        #[source]
        source: serde_yaml_ng::Error,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    #[serde(deserialize_with = "deserialize_bind")]
    pub bind: SocketAddr,
    #[serde(default = "default_log_level")]
    pub log_level: String,
    #[serde(default, deserialize_with = "clusters")]
    pub clusters: UniqueMap<ClusterName, ClusterConfig>,
    #[serde(default)]
    pub auth: Option<AuthConfig>,
}

fn default_log_level() -> String {
    environment::LOG_LEVEL.clone()
}

fn deserialize_bind<'de, D>(deserializer: D) -> Result<SocketAddr, D::Error>
where
    D: serde::Deserializer<'de>,
{
    String::deserialize(deserializer)?
        .parse()
        .map_err(serde::de::Error::custom)
}

impl Config {
    pub fn path() -> PathBuf {
        PathBuf::from(environment::CONFIG_PATH.as_str())
    }

    pub fn load(path: impl AsRef<Path>) -> Result<Self, ConfigError> {
        let path = path.as_ref();
        let raw = std::fs::read_to_string(path).map_err(|source| ConfigError::Read {
            path: path.to_owned(),
            source,
        })?;
        Self::parse(path, &raw)
    }

    fn parse(path: &Path, raw: &str) -> Result<Self, ConfigError> {
        serde_yaml_ng::from_str(raw).map_err(|source| ConfigError::Parse {
            path: path.to_owned(),
            source,
        })
    }
}

fn clusters<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<UniqueMap<ClusterName, ClusterConfig>, D::Error> {
    UniqueMap::deserialize_checked(deserializer, "cluster name", Ok)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UniqueMap<K: Hash + Eq, V>(IndexMap<K, V>);

impl<K: Hash + Eq, V> Default for UniqueMap<K, V> {
    fn default() -> Self {
        Self(IndexMap::new())
    }
}

impl<K: Hash + Eq, V> UniqueMap<K, V> {
    pub fn iter(&self) -> indexmap::map::Iter<'_, K, V> {
        self.0.iter()
    }

    pub fn get(&self, key: &K) -> Option<&V> {
        self.0.get(key)
    }

    pub fn keys(&self) -> indexmap::map::Keys<'_, K, V> {
        self.0.keys()
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    fn deserialize_checked<'de, D, T>(
        deserializer: D,
        noun: &'static str,
        convert: fn(Self) -> Result<T, String>,
    ) -> Result<T, D::Error>
    where
        D: Deserializer<'de>,
        K: Deserialize<'de> + Display,
        V: Deserialize<'de>,
    {
        deserializer.deserialize_map(UniqueMapVisitor { noun, convert })
    }
}

#[cfg(test)]
impl<K: Hash + Eq, V> FromIterator<(K, V)> for UniqueMap<K, V> {
    fn from_iter<I: IntoIterator<Item = (K, V)>>(entries: I) -> Self {
        Self(entries.into_iter().collect())
    }
}

struct UniqueMapVisitor<K: Hash + Eq, V, T> {
    noun: &'static str,
    convert: fn(UniqueMap<K, V>) -> Result<T, String>,
}

impl<'de, K, V, T> Visitor<'de> for UniqueMapVisitor<K, V, T>
where
    K: Deserialize<'de> + Hash + Eq + Display,
    V: Deserialize<'de>,
{
    type Value = T;

    fn expecting(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "a map keyed by {}", self.noun)
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<T, A::Error> {
        let mut entries = IndexMap::new();
        while let Some(key) = map.next_key::<K>()? {
            if entries.contains_key(&key) {
                return Err(de::Error::custom(format!(
                    "duplicate {} '{key}'",
                    self.noun
                )));
            }
            let value = map.next_value()?;
            entries.insert(key, value);
        }
        (self.convert)(UniqueMap(entries)).map_err(de::Error::custom)
    }
}

/// Converts `Raw` into `T` inside the visitor. serde_yaml_ng gives an error
/// the path and line of the value whose deserializer is running when it is
/// raised, so a check run after `Raw::deserialize` returns would report the
/// parent's location instead.
struct Checked<Raw, T> {
    expecting: &'static str,
    convert: fn(Raw) -> Result<T, String>,
}

impl<Raw, T> Checked<Raw, T> {
    fn new(expecting: &'static str, convert: fn(Raw) -> Result<T, String>) -> Self {
        Self { expecting, convert }
    }
}

impl<'de, Raw: Deserialize<'de>, T> Visitor<'de> for Checked<Raw, T> {
    type Value = T;

    fn expecting(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.expecting)
    }

    fn visit_str<E: de::Error>(self, value: &str) -> Result<T, E> {
        let raw = Raw::deserialize(StrDeserializer::<E>::new(value))?;
        (self.convert)(raw).map_err(E::custom)
    }

    fn visit_seq<A: SeqAccess<'de>>(self, seq: A) -> Result<T, A::Error> {
        let raw = Raw::deserialize(SeqAccessDeserializer::new(seq))?;
        (self.convert)(raw).map_err(de::Error::custom)
    }

    fn visit_map<A: MapAccess<'de>>(self, map: A) -> Result<T, A::Error> {
        let raw = Raw::deserialize(MapAccessDeserializer::new(map))?;
        (self.convert)(raw).map_err(de::Error::custom)
    }
}

const EMPTY: &str = "must not be empty";

const EMPTY_VALUES: &str = "must not contain empty values";

/// Deserializes a map-shaped `T` inside a visitor, for an internally tagged
/// enum whose variant is built from buffered content after its own
/// deserializer has returned.
fn in_place<'de, D: Deserializer<'de>, T: Deserialize<'de>>(
    deserializer: D,
) -> Result<T, D::Error> {
    deserializer.deserialize_map(Checked::new("a map", Ok))
}

fn non_empty<'de, D, T>(deserializer: D) -> Result<Vec<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    deserializer.deserialize_seq(Checked::new("a non-empty list", |items: Vec<T>| {
        if items.is_empty() {
            return Err(EMPTY.to_owned());
        }
        Ok(items)
    }))
}

fn non_blank<'de, D: Deserializer<'de>>(deserializer: D) -> Result<String, D::Error> {
    deserializer.deserialize_str(Checked::new("a string", |value: String| {
        if value.trim().is_empty() {
            return Err(EMPTY.to_owned());
        }
        Ok(value)
    }))
}

fn non_blank_items<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Vec<String>, D::Error> {
    deserializer.deserialize_seq(Checked::new("a list of strings", |items: Vec<String>| {
        if items.iter().any(|item| item.trim().is_empty()) {
            return Err(EMPTY_VALUES.to_owned());
        }
        Ok(items)
    }))
}

fn names<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Vec<String>, D::Error> {
    deserializer.deserialize_seq(Checked::new(
        "a non-empty list of names",
        |names: Vec<String>| {
            if names.is_empty() {
                return Err(EMPTY.to_owned());
            }
            if names.iter().any(|name| name.trim().is_empty()) {
                return Err(EMPTY_VALUES.to_owned());
            }
            Ok(names)
        },
    ))
}

fn some_names<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Option<Vec<String>>, D::Error> {
    names(deserializer).map(Some)
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuthConfig {
    pub oidc: OidcConfig,
    #[serde(default, deserialize_with = "roles")]
    pub roles: Option<UniqueMap<String, RoleConfig>>,
    /// Signing key for the session cookie, as base64 or raw text of at least
    /// 32 bytes. Without one, every restart invalidates every session.
    #[serde(default)]
    pub session_key: Option<KeyMaterial<MIN_SESSION_KEY_BYTES>>,
}

const MAX_ROLES: usize = 64;

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RoleConfig {
    #[serde(deserialize_with = "privileges")]
    pub privileges: Vec<PrivilegeName>,
    #[serde(default)]
    pub bindings: Vec<RoleBinding>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RoleBinding {
    #[serde(deserialize_with = "names")]
    pub groups: Vec<String>,
    #[serde(default, deserialize_with = "some_names")]
    pub clusters: Option<Vec<String>>,
}

fn roles<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<UniqueMap<String, RoleConfig>>, D::Error> {
    UniqueMap::deserialize_checked(
        deserializer,
        "role name",
        |roles: UniqueMap<String, RoleConfig>| {
            if roles.is_empty() {
                return Err(EMPTY.to_owned());
            }
            if roles.len() > MAX_ROLES {
                return Err(format!("too many roles (at most {MAX_ROLES})"));
            }
            if roles.keys().any(|name| name.trim().is_empty()) {
                return Err("role name must not be empty".to_owned());
            }
            if roles.iter().all(|(_, role)| role.bindings.is_empty()) {
                return Err("at least one role must have bindings".to_owned());
            }
            Ok(Some(roles))
        },
    )
}

fn privileges<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Vec<PrivilegeName>, D::Error> {
    deserializer.deserialize_seq(Checked::new(
        "a list of privileges",
        |privileges: Vec<PrivilegeName>| {
            let mut seen = HashSet::with_capacity(privileges.len());
            if let Some(repeated) = privileges
                .iter()
                .find(|privilege| !seen.insert(**privilege))
            {
                return Err(format!("'{repeated}' is listed more than once"));
            }
            Ok(privileges)
        },
    ))
}

fn default_groups_claim() -> String {
    "groups".to_owned()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PrivilegeName {
    Records,
    Configs,
    SchemaText,
    Acls,
}

impl PrivilegeName {
    fn as_str(self) -> &'static str {
        match self {
            Self::Records => "records",
            Self::Configs => "configs",
            Self::SchemaText => "schema_text",
            Self::Acls => "acls",
        }
    }
}

impl Display for PrivilegeName {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OidcConfig {
    /// Kept as written: discovery compares the provider's issuer to this text
    /// byte for byte, and `Url` would append a slash to a bare host.
    #[serde(deserialize_with = "issuer_url")]
    pub issuer: IssuerUrl,
    #[serde(deserialize_with = "non_blank")]
    pub client_id: String,
    #[serde(deserialize_with = "non_blank_secret")]
    pub client_secret: Secret,
    #[serde(deserialize_with = "redirect_url")]
    pub redirect_uri: RedirectUrl,
    #[serde(default = "default_scopes", deserialize_with = "non_blank_items")]
    pub scopes: Vec<String>,
    #[serde(default = "default_groups_claim", deserialize_with = "non_blank")]
    pub groups_claim: String,
    #[serde(default)]
    pub cookie_secure: Option<bool>,
}

fn default_scopes() -> Vec<String> {
    environment::DEFAULT_OIDC_SCOPES
        .iter()
        .map(|scope| (*scope).to_owned())
        .collect()
}

impl OidcConfig {
    pub fn cookie_secure(&self) -> bool {
        self.cookie_secure
            .unwrap_or_else(|| self.redirect_uri.url().scheme() == "https")
    }

    pub fn effective_scopes(&self) -> Vec<String> {
        let mut scopes = self.scopes.clone();
        if !scopes.iter().any(|scope| scope == "openid") {
            scopes.insert(0, "openid".to_owned());
        }
        scopes
    }
}

const HTTP_URL: &str = "an http or https URL";

fn parse_http_url(value: &str) -> Result<Url, String> {
    let parsed = Url::parse(value).map_err(|error| format!("not a valid URL: {error}"))?;

    if parsed.scheme() != "http" && parsed.scheme() != "https" {
        return Err("must be an http or https URL".to_owned());
    }

    if parsed.host_str().is_none() {
        return Err("must include a host".to_owned());
    }

    Ok(parsed)
}

fn http_url<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Url, D::Error> {
    deserializer.deserialize_str(Checked::new(HTTP_URL, |raw: String| parse_http_url(&raw)))
}

fn issuer_url<'de, D: Deserializer<'de>>(deserializer: D) -> Result<IssuerUrl, D::Error> {
    deserializer.deserialize_str(Checked::new(HTTP_URL, |raw: String| {
        parse_http_url(&raw)?;
        IssuerUrl::new(raw).map_err(|error| error.to_string())
    }))
}

fn redirect_url<'de, D: Deserializer<'de>>(deserializer: D) -> Result<RedirectUrl, D::Error> {
    deserializer.deserialize_str(Checked::new(HTTP_URL, |raw: String| {
        parse_http_url(&raw)?;
        RedirectUrl::new(raw).map_err(|error| error.to_string())
    }))
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClusterConfig {
    #[serde(deserialize_with = "non_empty")]
    pub bootstrap_servers: Vec<String>,
    #[serde(default, deserialize_with = "in_place")]
    pub security: SecurityConfig,
    #[serde(default)]
    pub schema_registry: Option<SchemaRegistryConfig>,
    #[serde(default)]
    pub obfuscation: Option<ObfuscationConfig>,
    #[serde(default)]
    pub properties: KafkaProperties,
    #[serde(default, deserialize_with = "ingest")]
    pub ingest: ClusterIngestConfig,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ClusterIngestConfig {
    pub topology_secs: u64,
    pub watermark_secs: u64,
    pub config_secs: u64,
    pub subjects_secs: u64,
    pub offset_tick_secs: u64,
    pub fast_offset_secs: u64,
    pub slow_offset_secs: u64,
}

impl Default for ClusterIngestConfig {
    fn default() -> Self {
        Self {
            topology_secs: 10,
            watermark_secs: 3,
            config_secs: 60,
            subjects_secs: 30,
            offset_tick_secs: 1,
            fast_offset_secs: 2,
            slow_offset_secs: 20,
        }
    }
}

fn ingest<'de, D: Deserializer<'de>>(deserializer: D) -> Result<ClusterIngestConfig, D::Error> {
    deserializer.deserialize_map(Checked::new(
        "ingest intervals",
        |ingest: ClusterIngestConfig| {
            let fields = [
                ("topology_secs", ingest.topology_secs),
                ("watermark_secs", ingest.watermark_secs),
                ("config_secs", ingest.config_secs),
                ("subjects_secs", ingest.subjects_secs),
                ("offset_tick_secs", ingest.offset_tick_secs),
                ("fast_offset_secs", ingest.fast_offset_secs),
                ("slow_offset_secs", ingest.slow_offset_secs),
            ];
            if let Some((field, _)) = fields.iter().find(|(_, secs)| *secs < 1) {
                return Err(format!("{field} must be at least 1"));
            }
            Ok(ingest)
        },
    ))
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct KafkaProperties {
    pub client_id: Option<String>,
    pub request_timeout_ms: Option<u64>,
    pub connect_timeout_ms: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SchemaRegistryConfig {
    #[serde(deserialize_with = "http_url")]
    pub url: Url,
    #[serde(default)]
    pub auth: Option<BasicAuth>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BasicAuth {
    #[serde(deserialize_with = "non_blank")]
    pub username: String,
    #[serde(deserialize_with = "non_empty_secret")]
    pub password: Secret,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ClusterName(String);

impl ClusterName {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl FromStr for ClusterName {
    type Err = String;

    fn from_str(raw: &str) -> Result<Self, String> {
        let name = raw.trim();
        if name.is_empty() {
            return Err(EMPTY.to_owned());
        }
        Ok(Self(name.to_owned()))
    }
}

impl Display for ClusterName {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for ClusterName {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_str(Checked::new("a cluster name", |raw: String| raw.parse()))
    }
}

/// A secret the config names by where to read it: `{value: ...}` inline,
/// `{env: NAME}` from an environment variable, or `{file: PATH}` from a file
/// such as a mounted Kubernetes secret. Resolved once, at load.
#[derive(Clone)]
pub struct Secret(SecretString);

#[derive(Deserialize)]
#[serde(rename_all = "lowercase")]
enum SecretSource {
    Value(#[serde(deserialize_with = "secret_text")] SecretString),
    Env(String),
    File(PathBuf),
}

impl SecretSource {
    fn resolve(self) -> Result<Secret, String> {
        let secret = match self {
            Self::Value(value) => value,
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

        Ok(Secret(secret))
    }
}

impl ExposeSecret<str> for Secret {
    fn expose_secret(&self) -> &str {
        self.0.expose_secret()
    }
}

impl From<&str> for Secret {
    fn from(value: &str) -> Self {
        Self(value.into())
    }
}

impl PartialEq for Secret {
    fn eq(&self, other: &Self) -> bool {
        self.expose_secret() == other.expose_secret()
    }
}

impl Eq for Secret {}

impl std::fmt::Debug for Secret {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("Secret(..)")
    }
}

impl<'de> Deserialize<'de> for Secret {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_any(SecretVisitor(Ok))
    }
}

const PLAIN_SECRET: &str =
    "a secret must name its source: {value: ...}, {env: NAME} or {file: PATH}";

/// Resolves the source and applies the conversion inside `visit_map`, so an
/// error carries the field's path. Rejects scalars itself because serde's
/// default errors quote the offending value, which here is the secret.
struct SecretVisitor<T>(fn(Secret) -> Result<T, String>);

impl<'de, T> Visitor<'de> for SecretVisitor<T> {
    type Value = T;

    fn expecting(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(PLAIN_SECRET)
    }

    fn visit_map<A: MapAccess<'de>>(self, map: A) -> Result<T, A::Error> {
        let secret = SecretSource::deserialize(MapAccessDeserializer::new(map))?
            .resolve()
            .map_err(de::Error::custom)?;
        (self.0)(secret).map_err(de::Error::custom)
    }

    fn visit_str<E: de::Error>(self, _: &str) -> Result<T, E> {
        Err(E::custom(PLAIN_SECRET))
    }

    fn visit_bool<E: de::Error>(self, _: bool) -> Result<T, E> {
        Err(E::custom(PLAIN_SECRET))
    }

    fn visit_i64<E: de::Error>(self, _: i64) -> Result<T, E> {
        Err(E::custom(PLAIN_SECRET))
    }

    fn visit_u64<E: de::Error>(self, _: u64) -> Result<T, E> {
        Err(E::custom(PLAIN_SECRET))
    }

    fn visit_f64<E: de::Error>(self, _: f64) -> Result<T, E> {
        Err(E::custom(PLAIN_SECRET))
    }
}

fn secret_text<'de, D: Deserializer<'de>>(deserializer: D) -> Result<SecretString, D::Error> {
    struct TextVisitor;

    const NOT_TEXT: &str = "a secret value must be a string; quote it";

    impl Visitor<'_> for TextVisitor {
        type Value = SecretString;

        fn expecting(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
            formatter.write_str(NOT_TEXT)
        }

        fn visit_str<E: de::Error>(self, value: &str) -> Result<SecretString, E> {
            Ok(value.into())
        }

        fn visit_bool<E: de::Error>(self, _: bool) -> Result<SecretString, E> {
            Err(E::custom(NOT_TEXT))
        }

        fn visit_i64<E: de::Error>(self, _: i64) -> Result<SecretString, E> {
            Err(E::custom(NOT_TEXT))
        }

        fn visit_u64<E: de::Error>(self, _: u64) -> Result<SecretString, E> {
            Err(E::custom(NOT_TEXT))
        }

        fn visit_f64<E: de::Error>(self, _: f64) -> Result<SecretString, E> {
            Err(E::custom(NOT_TEXT))
        }
    }

    deserializer.deserialize_any(TextVisitor)
}

pub const MIN_OBFUSCATION_SECRET_BYTES: usize = 32;

/// `cookie::Key::derive_from` panics below this.
pub const MIN_SESSION_KEY_BYTES: usize = 32;

/// Key bytes from a [`Secret`], as base64 when that decodes to at least `MIN`
/// bytes, otherwise as the raw text.
#[derive(Clone, PartialEq, Eq)]
pub struct KeyMaterial<const MIN: usize>(Box<[u8]>);

#[derive(Debug, Error, PartialEq, Eq)]
#[error("must decode to at least {min} bytes, got {got}")]
pub struct ShortKeyMaterial {
    min: usize,
    got: usize,
}

impl<const MIN: usize> KeyMaterial<MIN> {
    pub fn parse(raw: &str) -> Result<Self, ShortKeyMaterial> {
        let raw = raw.trim();
        let bytes = match base64::Engine::decode(&base64::engine::general_purpose::STANDARD, raw) {
            Ok(decoded) if decoded.len() >= MIN => decoded,
            _ => raw.as_bytes().to_vec(),
        };

        if bytes.len() < MIN {
            return Err(ShortKeyMaterial {
                min: MIN,
                got: bytes.len(),
            });
        }

        Ok(Self(bytes.into_boxed_slice()))
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

impl<const MIN: usize> std::fmt::Debug for KeyMaterial<MIN> {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("KeyMaterial")
            .finish_non_exhaustive()
    }
}

impl<'de, const MIN: usize> Deserialize<'de> for KeyMaterial<MIN> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_any(SecretVisitor(|secret| {
            Self::parse(secret.expose_secret()).map_err(|error| error.to_string())
        }))
    }
}

fn non_empty_secret<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Secret, D::Error> {
    deserializer.deserialize_any(SecretVisitor(|secret| {
        if secret.expose_secret().is_empty() {
            return Err(EMPTY.to_owned());
        }
        Ok(secret)
    }))
}

fn non_blank_secret<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Secret, D::Error> {
    deserializer.deserialize_any(SecretVisitor(|secret| {
        if secret.expose_secret().trim().is_empty() {
            return Err(EMPTY.to_owned());
        }
        Ok(secret)
    }))
}

pub const OBFUSCATION_MASK: &str = "***";

const REGEX_SIZE_LIMIT: usize = 1024 * 1024;

pub type ObfuscationKey = KeyMaterial<MIN_OBFUSCATION_SECRET_BYTES>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObfuscationConfig {
    pub rules: Vec<ObfuscationRule>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawObfuscationConfig {
    #[serde(default)]
    secret: Option<ObfuscationKey>,
    #[serde(deserialize_with = "non_empty")]
    rules: Vec<RawObfuscationRule>,
}

impl<'de> Deserialize<'de> for ObfuscationConfig {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_map(Checked::new("obfuscation settings", Self::from_raw))
    }
}

impl ObfuscationConfig {
    fn from_raw(raw: RawObfuscationConfig) -> Result<Self, String> {
        let mut covered: Vec<&TopicPattern> = Vec::new();
        for rule in &raw.rules {
            for topic in &rule.topics {
                if let Some(other) = covered.iter().find(|other| other.overlaps(topic)) {
                    return Err(format!(
                        "topics '{topic}' and '{other}' match the same topics; \
                         a topic must be covered by exactly one rule"
                    ));
                }
            }
            covered.extend(&rule.topics);
        }

        let key = raw.secret.map(Arc::new);
        let rules = raw
            .rules
            .into_iter()
            .map(|rule| rule.resolve(key.as_ref()))
            .collect::<Result<_, _>>()?;

        Ok(Self { rules })
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum UnparsedPolicy {
    /// Fail closed: the whole value is masked.
    #[default]
    Mask,
    /// Fail open: undecodable values are served as they came off the wire.
    Allow,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObfuscationRule {
    pub topics: Vec<TopicPattern>,
    pub fields: Vec<ObfuscationField>,
    pub key: Option<ObfuscationStrategy>,
    pub value: Option<ObfuscationStrategy>,
    pub headers: Vec<String>,
    pub patterns: Vec<ObfuscationPattern>,
    pub unparsed: UnparsedPolicy,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawObfuscationRule {
    #[serde(deserialize_with = "non_empty")]
    topics: Vec<TopicPattern>,
    #[serde(default)]
    fields: Vec<RawObfuscationField>,
    #[serde(default)]
    key: Option<StrategyName>,
    #[serde(default)]
    value: Option<StrategyName>,
    #[serde(default, deserialize_with = "non_blank_items")]
    headers: Vec<String>,
    #[serde(default)]
    patterns: Vec<RawObfuscationPattern>,
    #[serde(default)]
    unparsed: UnparsedPolicy,
}

impl RawObfuscationRule {
    fn resolve(self, key: Option<&Arc<ObfuscationKey>>) -> Result<ObfuscationRule, String> {
        if self.fields.is_empty()
            && self.key.is_none()
            && self.value.is_none()
            && self.headers.is_empty()
            && self.patterns.is_empty()
        {
            let topics: Vec<String> = self.topics.iter().map(ToString::to_string).collect();
            return Err(format!(
                "rule for '{}' must set at least one of fields, key, value, headers or patterns",
                topics.join(", ")
            ));
        }

        let strategy = |name: StrategyName| name.resolve(key);

        Ok(ObfuscationRule {
            fields: self
                .fields
                .into_iter()
                .map(|field| {
                    Ok(ObfuscationField {
                        path: field.path,
                        strategy: strategy(field.strategy)?,
                    })
                })
                .collect::<Result<_, String>>()?,
            patterns: self
                .patterns
                .into_iter()
                .map(|pattern| {
                    Ok(ObfuscationPattern {
                        regex: pattern.regex,
                        strategy: strategy(pattern.strategy)?,
                    })
                })
                .collect::<Result<_, String>>()?,
            key: self.key.map(strategy).transpose()?,
            value: self.value.map(strategy).transpose()?,
            topics: self.topics,
            headers: self.headers,
            unparsed: self.unparsed,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObfuscationPattern {
    pub regex: PatternRegex,
    pub strategy: ObfuscationStrategy,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawObfuscationPattern {
    regex: PatternRegex,
    strategy: StrategyName,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObfuscationField {
    pub path: FieldPath,
    pub strategy: ObfuscationStrategy,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawObfuscationField {
    path: FieldPath,
    strategy: StrategyName,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ObfuscationStrategy {
    Mask,
    Hash(Arc<ObfuscationKey>),
    Drop,
}

#[derive(Clone, Copy, Deserialize)]
#[serde(rename_all = "lowercase")]
enum StrategyName {
    Mask,
    Hash,
    Drop,
}

impl StrategyName {
    fn resolve(self, key: Option<&Arc<ObfuscationKey>>) -> Result<ObfuscationStrategy, String> {
        Ok(match self {
            Self::Mask => ObfuscationStrategy::Mask,
            Self::Drop => ObfuscationStrategy::Drop,
            Self::Hash => {
                ObfuscationStrategy::Hash(Arc::clone(key.ok_or("hash strategy requires a secret")?))
            }
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TopicPattern {
    Exact(String),
    Prefix(String),
}

impl TopicPattern {
    fn overlaps(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Exact(left), Self::Exact(right)) => left == right,
            (Self::Exact(name), Self::Prefix(prefix))
            | (Self::Prefix(prefix), Self::Exact(name)) => name.starts_with(prefix.as_str()),
            (Self::Prefix(left), Self::Prefix(right)) => {
                left.starts_with(right.as_str()) || right.starts_with(left.as_str())
            }
        }
    }
}

impl FromStr for TopicPattern {
    type Err = String;

    fn from_str(pattern: &str) -> Result<Self, String> {
        if pattern.trim().is_empty() {
            return Err("topic must not be empty".to_owned());
        }

        match pattern.strip_suffix('*') {
            Some(prefix) if !prefix.contains('*') => Ok(Self::Prefix(prefix.to_owned())),
            None if !pattern.contains('*') => Ok(Self::Exact(pattern.to_owned())),
            _ => Err(format!(
                "topic '{pattern}': '*' is only allowed as the last character"
            )),
        }
    }
}

impl Display for TopicPattern {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Exact(name) => formatter.write_str(name),
            Self::Prefix(prefix) => write!(formatter, "{prefix}*"),
        }
    }
}

impl<'de> Deserialize<'de> for TopicPattern {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_str(Checked::new(
            "a topic name or a trailing-* prefix",
            |raw: String| raw.parse(),
        ))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FieldPath(String);

impl FieldPath {
    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn segments(&self) -> impl Iterator<Item = &str> {
        self.0.split('.')
    }
}

impl FromStr for FieldPath {
    type Err = String;

    fn from_str(path: &str) -> Result<Self, String> {
        if path.trim().is_empty() || path.split('.').any(str::is_empty) {
            return Err(format!("field path '{path}' must not have empty segments"));
        }
        Ok(Self(path.to_owned()))
    }
}

impl<'de> Deserialize<'de> for FieldPath {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_str(Checked::new("a dotted field path", |raw: String| {
            raw.parse()
        }))
    }
}

#[derive(Debug, Clone)]
pub struct PatternRegex(Regex);

impl PatternRegex {
    pub fn as_regex(&self) -> &Regex {
        &self.0
    }
}

impl PartialEq for PatternRegex {
    fn eq(&self, other: &Self) -> bool {
        self.0.as_str() == other.0.as_str()
    }
}

impl Eq for PatternRegex {}

impl FromStr for PatternRegex {
    type Err = String;

    fn from_str(source: &str) -> Result<Self, String> {
        if source.trim().is_empty() {
            return Err("pattern must not be empty".to_owned());
        }

        let regex = RegexBuilder::new(source)
            .size_limit(REGEX_SIZE_LIMIT)
            .build()
            .map_err(|error| format!("invalid pattern '{source}': {error}"))?;

        if regex.is_match("") {
            return Err(format!(
                "invalid pattern '{source}': it matches the empty string"
            ));
        }

        Ok(Self(regex))
    }
}

impl<'de> Deserialize<'de> for PatternRegex {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_str(Checked::new("a regex", |raw: String| raw.parse()))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SecurityProtocol {
    Plaintext,
    Ssl,
    SaslPlaintext,
    SaslSsl,
}

impl SecurityProtocol {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Plaintext => "PLAINTEXT",
            Self::Ssl => "SSL",
            Self::SaslPlaintext => "SASL_PLAINTEXT",
            Self::SaslSsl => "SASL_SSL",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum SaslMechanism {
    Plain,
    #[serde(rename = "SCRAM-SHA-256")]
    ScramSha256,
    #[serde(rename = "SCRAM-SHA-512")]
    ScramSha512,
}

impl SaslMechanism {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Plain => "PLAIN",
            Self::ScramSha256 => "SCRAM-SHA-256",
            Self::ScramSha512 => "SCRAM-SHA-512",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(
    tag = "protocol",
    rename_all = "SCREAMING_SNAKE_CASE",
    deny_unknown_fields
)]
pub enum SecurityConfig {
    /// Braced because serde ignores unknown fields beside the tag of a unit
    /// variant, so `tls` under `PLAINTEXT` would load without an error.
    Plaintext {},
    Ssl {
        #[serde(default)]
        tls: TlsConfig,
    },
    SaslPlaintext {
        sasl: SaslConfig,
    },
    SaslSsl {
        sasl: SaslConfig,
        #[serde(default)]
        tls: TlsConfig,
    },
}

impl Default for SecurityConfig {
    fn default() -> Self {
        Self::Plaintext {}
    }
}

impl SecurityConfig {
    pub fn protocol(&self) -> SecurityProtocol {
        match self {
            Self::Plaintext {} => SecurityProtocol::Plaintext,
            Self::Ssl { .. } => SecurityProtocol::Ssl,
            Self::SaslPlaintext { .. } => SecurityProtocol::SaslPlaintext,
            Self::SaslSsl { .. } => SecurityProtocol::SaslSsl,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SaslConfig {
    pub mechanism: SaslMechanism,
    pub username: String,
    pub password: Secret,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct TlsConfig {
    pub ca_cert: Option<PathBuf>,
    pub client: Option<ClientCert>,
    pub insecure_skip_verify: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClientCert {
    pub cert: PathBuf,
    pub key: PathBuf,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse_cluster(yaml: &str) -> Result<ClusterConfig, serde_yaml_ng::Error> {
        serde_yaml_ng::from_str(yaml)
    }

    fn parse_config(yaml: &str) -> Result<Config, serde_yaml_ng::Error> {
        serde_yaml_ng::from_str(yaml)
    }

    fn cluster_error(yaml: &str) -> String {
        parse_cluster(yaml).unwrap_err().to_string()
    }

    fn config_error(yaml: &str) -> String {
        parse_config(yaml).unwrap_err().to_string()
    }

    #[test]
    fn parses_root_config() {
        let config = parse_config(
            "
            bind: 0.0.0.0:8080
            clusters:
              local:
                bootstrap_servers:
                  - localhost:9092
              staging:
                bootstrap_servers:
                  - broker-1:9092
                  - broker-2:9092
            ",
        )
        .unwrap();

        assert_eq!(cluster_names(&config), vec!["local", "staging"]);
        let staging = config.clusters.get(&"staging".parse().unwrap()).unwrap();
        assert_eq!(
            staging.bootstrap_servers,
            vec!["broker-1:9092", "broker-2:9092"]
        );
        assert_eq!(config.bind, "0.0.0.0:8080".parse().unwrap());
        assert_eq!(config.log_level, "info");
        assert_eq!(config.auth, None);
        assert_eq!(
            staging.ingest,
            ClusterIngestConfig::default(),
            "omitted ingest uses the documented defaults"
        );
    }

    fn cluster_names(config: &Config) -> Vec<&str> {
        config.clusters.keys().map(ClusterName::as_str).collect()
    }

    #[test]
    fn clusters_keep_file_order() {
        let config = parse_config(
            "
            bind: 127.0.0.1:8080
            clusters:
              zeta: {bootstrap_servers: [zeta:9092]}
              alpha: {bootstrap_servers: [alpha:9092]}
              mid: {bootstrap_servers: [mid:9092]}
            ",
        )
        .unwrap();

        assert_eq!(cluster_names(&config), vec!["zeta", "alpha", "mid"]);
    }

    #[test]
    fn clusters_may_be_empty_or_omitted() {
        let empty = parse_config("bind: 127.0.0.1:8080\nclusters: {}").unwrap();
        let omitted = parse_config("bind: 127.0.0.1:8080").unwrap();

        assert_eq!(empty.clusters.len(), 0);
        assert!(omitted.clusters.is_empty());
    }

    #[test]
    fn parses_cluster_ingest_overrides_and_fills_omitted_keys() {
        let cluster = parse_cluster(
            "
            bootstrap_servers:
              - kafka:9092
            ingest:
              topology_secs: 15
              watermark_secs: 5
            ",
        )
        .unwrap();

        assert_eq!(cluster.ingest.topology_secs, 15);
        assert_eq!(cluster.ingest.watermark_secs, 5);
        assert_eq!(cluster.ingest.config_secs, 60);
        assert_eq!(cluster.ingest.subjects_secs, 30);
        assert_eq!(cluster.ingest.offset_tick_secs, 1);
        assert_eq!(cluster.ingest.fast_offset_secs, 2);
        assert_eq!(cluster.ingest.slow_offset_secs, 20);
    }

    #[test]
    fn rejects_unknown_ingest_keys() {
        let error = parse_cluster(
            "
            bootstrap_servers:
              - kafka:9092
            ingest:
              catalog_secs: 10
            ",
        )
        .unwrap_err();

        assert!(error.to_string().contains("unknown field `catalog_secs`"));
    }

    #[test]
    fn rejects_sub_second_ingest_intervals() {
        let error = config_error(
            "
            bind: 127.0.0.1:8080
            clusters:
              prod:
                bootstrap_servers:
                  - kafka:9092
                ingest:
                  topology_secs: 0
            ",
        );

        assert_eq!(
            error,
            "clusters.prod.ingest: topology_secs must be at least 1 at line 8 column 19"
        );
    }

    #[test]
    fn rejects_catalog_poll_interval_yaml() {
        let error = parse_config(
            "
            bind: 127.0.0.1:8080
            catalog_poll_interval_secs: 15
            clusters: {}
            ",
        )
        .unwrap_err();

        assert!(
            error
                .to_string()
                .contains("unknown field `catalog_poll_interval_secs`")
        );
    }

    #[test]
    fn parses_bind_and_log_level() {
        let config = parse_config(
            "
            bind: 127.0.0.1:3000
            log_level: debug
            clusters: {}
            ",
        )
        .unwrap();

        assert_eq!(config.bind, "127.0.0.1:3000".parse().unwrap());
        assert_eq!(config.log_level, "debug");
    }

    #[test]
    fn rejects_missing_bind() {
        assert!(
            parse_config(
                "
            clusters: {}
            "
            )
            .is_err()
        );
    }

    #[test]
    fn rejects_invalid_bind() {
        assert!(
            parse_config(
                "
            bind: not-an-address
            clusters: {}
            "
            )
            .is_err()
        );
    }

    #[test]
    fn parses_minimal_cluster() {
        let config = parse_cluster(
            "
            bootstrap_servers:
              - localhost:9092
            ",
        )
        .unwrap();

        assert_eq!(config.bootstrap_servers, vec!["localhost:9092"]);
        assert_eq!(config.security, SecurityConfig::Plaintext {});
        assert_eq!(config.schema_registry, None);
        assert_eq!(config.properties, KafkaProperties::default());
    }

    #[test]
    fn parses_bootstrap_server_list() {
        let config = parse_cluster(
            "
            bootstrap_servers:
              - broker-1:9092
              - broker-2:9092
            ",
        )
        .unwrap();

        assert_eq!(
            config.bootstrap_servers,
            vec!["broker-1:9092", "broker-2:9092"]
        );
    }

    #[test]
    fn parses_full_security_settings() {
        let config = parse_cluster(
            "
            bootstrap_servers:
              - broker:9092
            security:
              protocol: SASL_SSL
              sasl:
                mechanism: SCRAM-SHA-512
                username: admin
                password: {value: secret}
              tls:
                ca_cert: /etc/ca.pem
                client:
                  cert: /etc/client.pem
                  key: /etc/client.key
                insecure_skip_verify: true
            properties:
              request_timeout_ms: 10000
            ",
        )
        .unwrap();

        assert_eq!(config.security.protocol(), SecurityProtocol::SaslSsl);
        let SecurityConfig::SaslSsl { sasl, tls } = config.security else {
            panic!("SASL_SSL parses to SaslSsl");
        };
        assert_eq!(sasl.mechanism, SaslMechanism::ScramSha512);
        assert_eq!(sasl.username, "admin");
        assert_eq!(sasl.password.expose_secret(), "secret");
        assert_eq!(
            tls,
            TlsConfig {
                ca_cert: Some("/etc/ca.pem".into()),
                client: Some(ClientCert {
                    cert: "/etc/client.pem".into(),
                    key: "/etc/client.key".into(),
                }),
                insecure_skip_verify: true,
            }
        );
        assert_eq!(config.properties.request_timeout_ms, Some(10000));
    }

    #[test]
    fn rejects_unknown_fields() {
        assert!(
            parse_cluster(
                "
            bootstrap_servers:
              - localhost:9092
            bogus: true
            "
            )
            .is_err()
        );
    }

    #[test]
    fn parses_typed_kafka_properties() {
        let config = parse_cluster(
            "
            bootstrap_servers: [localhost:9092]
            properties:
              client_id: browser
              request_timeout_ms: 8000
              connect_timeout_ms: 30000
            ",
        )
        .unwrap();
        assert_eq!(
            config.properties,
            KafkaProperties {
                client_id: Some("browser".into()),
                request_timeout_ms: Some(8000),
                connect_timeout_ms: Some(30000),
            }
        );
    }

    #[test]
    fn kafka_properties_reject_duplicate_timeouts() {
        assert!(
            serde_yaml_ng::from_str::<KafkaProperties>(
                "request_timeout_ms: 5000\nrequest_timeout_ms: 6000",
            )
            .is_err()
        );
    }

    #[test]
    fn kafka_properties_reject_unknown_keys_and_invalid_types() {
        for yaml in [
            "queued.min.messages: 2000",
            "request_timeout_ms: -1",
            "request_timeout_ms: 1.5",
            "request_timeout_ms: true",
            "request_timeout_ms: '5000'",
            "request_timeout_ms: 18446744073709551616",
            "connect_timeout_ms: invalid",
            "request.timeout.ms: 5000",
            "api.version.request.timeout.ms: 5000",
            "socket.connection.setup.timeout.ms: 5000",
            "client.id: browser",
            "bootstrap.servers: [localhost:9092]",
            "bootstrap.servers: localhost:9092",
            "bootstrap_servers: localhost:9092",
        ] {
            assert!(
                serde_yaml_ng::from_str::<KafkaProperties>(yaml).is_err(),
                "accepted invalid properties: {yaml}"
            );
        }
    }

    #[test]
    fn rejects_unknown_root_fields() {
        assert!(
            parse_config(
                "
            bind: 127.0.0.1:8080
            clusters: {}
            bogus: true
            "
            )
            .is_err()
        );
    }

    #[test]
    fn rejects_scalar_bootstrap_servers() {
        assert!(
            parse_cluster(
                "
            bootstrap_servers: localhost:9092
            "
            )
            .is_err()
        );
    }

    #[test]
    fn rejects_empty_bootstrap_servers() {
        let error = cluster_error(
            "
            bootstrap_servers: []
            ",
        );

        assert_eq!(
            error,
            "bootstrap_servers: must not be empty at line 2 column 32"
        );
    }

    #[test]
    fn accepts_plaintext_without_sasl() {
        let config = parse_cluster(
            "
            bootstrap_servers:
              - localhost:9092
            security:
              protocol: PLAINTEXT
            ",
        )
        .unwrap();

        assert_eq!(config.security, SecurityConfig::Plaintext {});
    }

    #[test]
    fn a_tls_block_defaults_when_omitted() {
        let config = parse_cluster(
            "
            bootstrap_servers:
              - localhost:9092
            security:
              protocol: SSL
            ",
        )
        .unwrap();

        assert_eq!(
            config.security,
            SecurityConfig::Ssl {
                tls: TlsConfig::default()
            }
        );
    }

    #[test]
    fn requires_sasl_for_sasl_protocols() {
        let error = cluster_error(
            "
            bootstrap_servers:
              - localhost:9092
            security:
              protocol: SASL_PLAINTEXT
            ",
        );

        assert_eq!(error, "security: missing field `sasl` at line 5 column 15");
    }

    #[test]
    fn rejects_blocks_the_protocol_does_not_use() {
        let error = cluster_error(
            "
            bootstrap_servers:
              - localhost:9092
            security:
              protocol: PLAINTEXT
              tls:
                ca_cert: /etc/ca.pem
            ",
        );
        assert_eq!(
            error,
            "security: unknown field `tls`, there are no fields at line 5 column 15"
        );

        let error = cluster_error(
            "
            bootstrap_servers:
              - localhost:9092
            security:
              protocol: SSL
              sasl:
                mechanism: PLAIN
                username: admin
                password: {value: secret}
            ",
        );
        assert_eq!(
            error,
            "security: unknown field `sasl`, expected `tls` at line 5 column 15"
        );
    }

    #[test]
    fn a_client_cert_needs_its_key() {
        let error = cluster_error(
            "
            bootstrap_servers:
              - localhost:9092
            security:
              protocol: SSL
              tls:
                client:
                  cert: /etc/client.pem
            ",
        );

        assert_eq!(error, "security: missing field `key` at line 5 column 15");
    }

    #[test]
    fn a_cluster_name_is_trimmed_and_must_not_be_blank() {
        let config = parse_config(
            "
            bind: 127.0.0.1:8080
            clusters:
              '  local  ': {bootstrap_servers: [localhost:9092]}
            ",
        )
        .unwrap();
        assert_eq!(cluster_names(&config), vec!["local"]);

        let error = config_error(
            "
            bind: 127.0.0.1:8080
            clusters:
              '   ': {bootstrap_servers: [localhost:9092]}
            ",
        );
        assert_eq!(error, "clusters: must not be empty at line 4 column 15");
    }

    #[test]
    fn rejects_a_repeated_cluster_name() {
        let error = config_error(
            "
            bind: 127.0.0.1:8080
            clusters:
              local:
                bootstrap_servers: [localhost:9092]
              staging:
                bootstrap_servers: [staging:9092]
              local:
                bootstrap_servers: [localhost:9093]
            ",
        );
        assert_eq!(
            error,
            "clusters: duplicate cluster name 'local' at line 4 column 15"
        );

        let error = config_error(
            "
            bind: 127.0.0.1:8080
            clusters:
              local: {bootstrap_servers: [localhost:9092]}
              ' local ': {bootstrap_servers: [localhost:9093]}
            ",
        );
        assert_eq!(
            error,
            "clusters: duplicate cluster name 'local' at line 4 column 15"
        );
    }

    #[test]
    fn parses_oidc_auth_config() {
        let config = parse_config(
            "
            bind: 127.0.0.1:8080
            clusters: {}
            auth:
              oidc:
                issuer: https://keycloak.example.com/realms/klens
                client_id: klens
                client_secret: {value: secret}
                redirect_uri: http://localhost:8080/api/auth/callback
            ",
        )
        .unwrap();

        let oidc = config.auth.as_ref().unwrap().oidc.clone();
        assert_eq!(
            oidc.issuer.as_str(),
            "https://keycloak.example.com/realms/klens"
        );
        assert_eq!(oidc.client_id, "klens");
        assert_eq!(oidc.client_secret.expose_secret(), "secret");
        assert_eq!(
            oidc.redirect_uri.as_str(),
            "http://localhost:8080/api/auth/callback"
        );
        assert_eq!(oidc.scopes, vec!["openid", "email", "profile"]);
        assert_eq!(oidc.cookie_secure, None);
        assert!(!oidc.cookie_secure());
    }

    #[test]
    fn parses_roles_and_their_bindings() {
        let config = parse_config(
            "
            bind: 127.0.0.1:8080
            clusters: {}
            auth:
              oidc:
                issuer: https://idp.example
                client_id: klens
                client_secret: {value: secret}
                redirect_uri: http://localhost:8080/api/auth/callback
              roles:
                admin:
                  privileges: [records, configs, schema_text, acls]
                  bindings:
                    - groups: [klens-admins]
                    - groups: [kafka-operators]
                      clusters: [staging, dev]
                viewer:
                  privileges: []
                  bindings:
                    - groups: [payments-viewers]
                      clusters: [payments]
                auditor:
                  privileges: [acls]
            ",
        )
        .unwrap();

        let roles = config.auth.unwrap().roles.unwrap();
        assert_eq!(
            roles.iter().collect::<Vec<_>>(),
            vec![
                (
                    &"admin".to_owned(),
                    &RoleConfig {
                        privileges: vec![
                            PrivilegeName::Records,
                            PrivilegeName::Configs,
                            PrivilegeName::SchemaText,
                            PrivilegeName::Acls,
                        ],
                        bindings: vec![
                            RoleBinding {
                                groups: vec!["klens-admins".to_owned()],
                                clusters: None,
                            },
                            RoleBinding {
                                groups: vec!["kafka-operators".to_owned()],
                                clusters: Some(vec!["staging".to_owned(), "dev".to_owned()]),
                            },
                        ],
                    },
                ),
                (
                    &"viewer".to_owned(),
                    &RoleConfig {
                        privileges: vec![],
                        bindings: vec![RoleBinding {
                            groups: vec!["payments-viewers".to_owned()],
                            clusters: Some(vec!["payments".to_owned()]),
                        }],
                    },
                ),
                (
                    &"auditor".to_owned(),
                    &RoleConfig {
                        privileges: vec![PrivilegeName::Acls],
                        bindings: vec![],
                    },
                ),
            ]
        );
    }

    fn parse_roles(roles: &str) -> Result<Config, String> {
        parse_config(&format!(
            "
            bind: 127.0.0.1:8080
            clusters: {{}}
            auth:
              oidc:
                issuer: https://idp.example
                client_id: klens
                client_secret: {{value: secret}}
                redirect_uri: http://localhost:8080/api/auth/callback
              roles:{roles}
            "
        ))
        .map_err(|error| error.to_string())
    }

    #[test]
    fn rejects_roles_that_bind_nobody() {
        let error = parse_roles(
            "
                admin:
                  privileges: [records]
                viewer:
                  privileges: []
                  bindings: []",
        )
        .unwrap_err();

        assert_eq!(
            error,
            "auth.roles: at least one role must have bindings at line 11 column 17"
        );
    }

    #[test]
    fn rejects_empty_roles() {
        let error = parse_roles(" {}").unwrap_err();

        assert_eq!(error, "auth.roles: must not be empty at line 10 column 22");
    }

    #[test]
    fn rejects_a_blank_role_name() {
        let error = parse_roles(
            "
                \"  \":
                  privileges: [records]
                  bindings:
                    - groups: [klens-admins]",
        )
        .unwrap_err();

        assert_eq!(
            error,
            "auth.roles: role name must not be empty at line 11 column 17"
        );
    }

    #[test]
    fn rejects_a_repeated_role_name() {
        let error = parse_roles(
            "
                admin:
                  privileges: [records, configs, schema_text, acls]
                  bindings:
                    - groups: [klens-admins]
                viewer:
                  privileges: []
                admin:
                  privileges: []
                  bindings:
                    - groups: [everyone]",
        )
        .unwrap_err();

        assert_eq!(
            error,
            "auth.roles: duplicate role name 'admin' at line 11 column 17"
        );
    }

    #[test]
    fn rejects_a_repeated_privilege_in_a_role() {
        let error = parse_roles(
            "
                operator:
                  privileges: [records, configs, records]
                  bindings:
                    - groups: [kafka-operators]",
        )
        .unwrap_err();

        assert_eq!(
            error,
            "auth.roles.operator.privileges: 'records' is listed more than once \
             at line 12 column 31"
        );
    }

    #[test]
    fn rejects_bindings_without_usable_groups_or_clusters() {
        let cases = [
            (
                "
                    - groups: []",
                "auth.roles.operator.bindings[0].groups: must not be empty at line 14 column 31",
            ),
            (
                "
                    - groups: [ops, ' ']",
                "auth.roles.operator.bindings[0].groups: must not contain empty values \
                 at line 14 column 31",
            ),
            (
                "
                    - groups: [ops]
                      clusters: []",
                "auth.roles.operator.bindings[0].clusters: must not be empty at line 15 column 33",
            ),
            (
                "
                    - groups: [ops]
                      clusters: [prod, '']",
                "auth.roles.operator.bindings[0].clusters: must not contain empty values \
                 at line 15 column 33",
            ),
        ];

        for (bindings, expected) in cases {
            let error = parse_roles(&format!(
                "
                operator:
                  privileges: [records]
                  bindings:{bindings}"
            ))
            .unwrap_err();
            assert_eq!(error, expected);
        }
    }

    fn groups_claim(oidc_extra: &str) -> Result<String, String> {
        parse_config(&format!(
            "
            bind: 127.0.0.1:8080
            auth:
              oidc:
                issuer: https://idp.example
                client_id: klens
                client_secret: {{value: secret}}
                redirect_uri: http://localhost:8080/api/auth/callback{oidc_extra}
            "
        ))
        .map(|config| config.auth.unwrap().oidc.groups_claim)
        .map_err(|error| error.to_string())
    }

    #[test]
    fn the_groups_claim_defaults_to_groups_and_can_be_renamed() {
        assert_eq!(groups_claim("").unwrap(), "groups");
        assert_eq!(
            groups_claim("\n                groups_claim: roles").unwrap(),
            "roles"
        );
    }

    #[test]
    fn rejects_a_blank_groups_claim() {
        let error = groups_claim("\n                groups_claim: ' '").unwrap_err();

        assert_eq!(
            error,
            "auth.oidc.groups_claim: must not be empty at line 9 column 31"
        );
    }

    #[test]
    fn rejects_too_many_roles() {
        let roles: String = (0..=MAX_ROLES)
            .map(|index| {
                format!(
                    "
                role{index}:
                  privileges: [records]
                  bindings:
                    - groups: [group{index}]"
                )
            })
            .collect();
        let error = parse_roles(&roles).unwrap_err();

        assert_eq!(
            error,
            "auth.roles: too many roles (at most 64) at line 11 column 17"
        );
    }

    #[test]
    fn accepts_exactly_the_most_roles_allowed() {
        let roles: String = (0..MAX_ROLES)
            .map(|index| {
                format!(
                    "
                role{index}:
                  privileges: [records]
                  bindings:
                    - groups: [group{index}]"
                )
            })
            .collect();

        let config = parse_roles(&roles).unwrap();
        assert_eq!(config.auth.unwrap().roles.unwrap().len(), MAX_ROLES);
    }

    #[test]
    fn a_list_where_a_map_belongs_names_what_it_expected() {
        let error = config_error(
            "
            bind: 127.0.0.1:8080
            clusters: [local]
            ",
        );
        assert!(
            error.contains("invalid type: sequence, expected a map keyed by cluster name"),
            "{error}"
        );

        let error = cluster_error(
            "
            bootstrap_servers: 5
            ",
        );
        assert!(error.contains("expected a non-empty list"), "{error}");
    }

    #[test]
    fn a_topic_pattern_takes_one_trailing_star() {
        assert_eq!(
            "orders.*".parse::<TopicPattern>(),
            Ok(TopicPattern::Prefix("orders.".to_owned()))
        );
        assert_eq!(
            "orders.*.eu*".parse::<TopicPattern>(),
            Err("topic 'orders.*.eu*': '*' is only allowed as the last character".to_owned())
        );
    }

    #[test]
    fn pattern_regexes_compare_by_source() {
        let regex = |source: &str| source.parse::<PatternRegex>().unwrap();
        assert_eq!(regex("[0-9]+"), regex("[0-9]+"));
        assert_ne!(regex("[0-9]+"), regex("[a-z]+"));
    }

    #[test]
    fn oidc_cookie_secure_follows_redirect_uri_and_override() {
        let https = parse_config(
            "
            bind: 127.0.0.1:8080
            clusters: {}
            auth:
              oidc:
                issuer: https://idp.example
                client_id: klens
                client_secret: {value: secret}
                redirect_uri: https://klens.example/api/auth/callback
            ",
        )
        .unwrap();
        assert!(https.auth.unwrap().oidc.cookie_secure());

        let forced = parse_config(
            "
            bind: 127.0.0.1:8080
            clusters: {}
            auth:
              oidc:
                issuer: https://idp.example
                client_id: klens
                client_secret: {value: secret}
                redirect_uri: https://klens.example/api/auth/callback
                cookie_secure: false
            ",
        )
        .unwrap();
        assert!(!forced.auth.unwrap().oidc.cookie_secure());
    }

    #[test]
    fn oidc_always_includes_openid_scope() {
        let config = parse_config(
            "
            bind: 127.0.0.1:8080
            clusters: {}
            auth:
              oidc:
                issuer: https://idp.example
                client_id: klens
                client_secret: {value: secret}
                redirect_uri: http://localhost:8080/api/auth/callback
                scopes:
                  - email
            ",
        )
        .unwrap();

        assert_eq!(
            config.auth.unwrap().oidc.effective_scopes(),
            vec!["openid", "email"]
        );
    }

    fn oidc_error(issuer: &str, client_id: &str, secret: &str, redirect_uri: &str) -> String {
        config_error(&format!(
            "
            bind: 127.0.0.1:8080
            clusters: {{}}
            auth:
              oidc:
                issuer: {issuer}
                client_id: {client_id}
                client_secret: {secret}
                redirect_uri: {redirect_uri}
            "
        ))
    }

    #[test]
    fn rejects_invalid_oidc_issuer() {
        let error = oidc_error(
            "not-a-url",
            "klens",
            "{value: secret}",
            "http://localhost:8080/api/auth/callback",
        );

        assert_eq!(
            error,
            "auth.oidc.issuer: not a valid URL: relative URL without a base \
             at line 6 column 25"
        );
    }

    #[test]
    fn rejects_empty_oidc_client_secret() {
        let error = oidc_error(
            "https://idp.example",
            "klens",
            "{value: '   '}",
            "http://localhost:8080/api/auth/callback",
        );

        assert_eq!(
            error,
            "auth.oidc.client_secret: must not be empty at line 8 column 32"
        );
    }

    #[test]
    fn rejects_a_blank_oidc_client_id() {
        let error = oidc_error(
            "https://idp.example",
            "' '",
            "{value: secret}",
            "http://localhost:8080/api/auth/callback",
        );

        assert_eq!(
            error,
            "auth.oidc.client_id: must not be empty at line 7 column 28"
        );
    }

    #[test]
    fn rejects_a_blank_oidc_scope() {
        let error = config_error(
            "
            bind: 127.0.0.1:8080
            auth:
              oidc:
                issuer: https://idp.example
                client_id: klens
                client_secret: {value: secret}
                redirect_uri: http://localhost:8080/api/auth/callback
                scopes: [openid, '']
            ",
        );

        assert_eq!(
            error,
            "auth.oidc.scopes: must not contain empty values at line 9 column 25"
        );
    }

    #[test]
    fn an_oidc_issuer_keeps_the_text_discovery_compares() {
        let config = parse_config(
            "
            bind: 127.0.0.1:8080
            auth:
              oidc:
                issuer: https://accounts.google.com
                client_id: klens
                client_secret: {value: secret}
                redirect_uri: https://klens.example/api/auth/callback
            ",
        )
        .unwrap();

        assert_eq!(
            config.auth.unwrap().oidc.issuer.as_str(),
            "https://accounts.google.com"
        );
    }

    #[test]
    fn rejects_non_http_redirect_uri() {
        let error = oidc_error(
            "https://idp.example",
            "klens",
            "{value: secret}",
            "ftp://localhost/api/auth/callback",
        );

        assert_eq!(
            error,
            "auth.oidc.redirect_uri: must be an http or https URL at line 9 column 31"
        );
    }

    #[test]
    fn parses_schema_registry_settings() {
        let config = parse_cluster(
            "
            bootstrap_servers:
              - localhost:9092
            schema_registry:
              url: http://localhost:8081
              auth:
                username: user
                password: {value: secret}
            ",
        )
        .unwrap();

        assert_eq!(
            config.schema_registry,
            Some(SchemaRegistryConfig {
                url: Url::parse("http://localhost:8081").unwrap(),
                auth: Some(BasicAuth {
                    username: "user".to_owned(),
                    password: Secret::from("secret"),
                }),
            })
        );
    }

    #[test]
    fn rejects_invalid_schema_registry_url() {
        let error = cluster_error(
            "
            bootstrap_servers:
              - localhost:9092
            schema_registry:
              url: not-a-url
            ",
        );

        assert_eq!(
            error,
            "schema_registry.url: not a valid URL: relative URL without a base \
             at line 5 column 20"
        );
    }

    fn registry_error(credentials: &str) -> String {
        cluster_error(&format!(
            "
            bootstrap_servers:
              - localhost:9092
            schema_registry:
              url: http://localhost:8081{credentials}
            "
        ))
    }

    #[test]
    fn registry_auth_needs_both_credentials() {
        let error = registry_error(
            "
              auth:
                username: user",
        );
        assert_eq!(
            error,
            "schema_registry.auth: missing field `password` at line 7 column 17"
        );

        let error = registry_error(
            "
              auth:
                password: {value: secret}",
        );
        assert_eq!(
            error,
            "schema_registry.auth: missing field `username` at line 7 column 17"
        );
    }

    #[test]
    fn rejects_blank_schema_registry_credentials() {
        let error = registry_error(
            "
              auth:
                username: ' '
                password: {value: secret}",
        );
        assert_eq!(
            error,
            "schema_registry.auth.username: must not be empty at line 7 column 27"
        );

        let error = registry_error(
            "
              auth:
                username: user
                password: {value: ''}",
        );
        assert_eq!(
            error,
            "schema_registry.auth.password: must not be empty at line 8 column 27"
        );
    }

    #[test]
    fn parses_obfuscation_rules() {
        let config = parse_cluster(
            "
            bootstrap_servers:
              - broker:9092
            obfuscation:
              secret: {value: 0123456789abcdef0123456789abcdef}
              rules:
                - topics: ['payments.*']
                  fields:
                    - path: card.number
                      strategy: hash
                    - path: card.cvv
                      strategy: drop
                  unparsed: allow
                - topics: ['audit.raw']
                  key: mask
                  value: hash
                  headers: ['x-user-id']
            ",
        )
        .unwrap();

        let obfuscation = config.obfuscation.unwrap();
        let key = Arc::new(ObfuscationKey::parse("0123456789abcdef0123456789abcdef").unwrap());
        assert_eq!(obfuscation.rules.len(), 2);
        assert_eq!(
            obfuscation.rules[0].topics,
            vec![TopicPattern::Prefix("payments.".to_owned())]
        );
        assert_eq!(obfuscation.rules[0].fields[0].path.as_str(), "card.number");
        assert_eq!(
            obfuscation.rules[0].fields[0].strategy,
            ObfuscationStrategy::Hash(Arc::clone(&key))
        );
        assert_eq!(
            obfuscation.rules[0].fields[1].strategy,
            ObfuscationStrategy::Drop
        );
        assert_eq!(obfuscation.rules[0].unparsed, UnparsedPolicy::Allow);
        assert_eq!(
            obfuscation.rules[1].topics,
            vec![TopicPattern::Exact("audit.raw".to_owned())]
        );
        assert_eq!(obfuscation.rules[1].key, Some(ObfuscationStrategy::Mask));
        assert_eq!(
            obfuscation.rules[1].value,
            Some(ObfuscationStrategy::Hash(key))
        );
        assert_eq!(obfuscation.rules[1].headers, vec!["x-user-id"]);
    }

    #[test]
    fn obfuscation_fails_closed_on_values_that_never_decode() {
        let config = parse_cluster(
            "
            bootstrap_servers:
              - broker:9092
            obfuscation:
              rules:
                - topics: [cards]
                  fields:
                    - path: pan
                      strategy: mask
            ",
        )
        .unwrap();

        assert_eq!(
            config.obfuscation.unwrap().rules[0].unparsed,
            UnparsedPolicy::Mask
        );
    }

    fn obfuscated(rules: &str) -> Result<(), String> {
        let yaml = format!(
            "
            bootstrap_servers:
              - broker:9092
            obfuscation:
{rules}
            "
        );

        parse_cluster(&yaml)
            .map(drop)
            .map_err(|error| error.to_string())
    }

    #[test]
    fn rejects_hashing_without_a_secret() {
        let fields = obfuscated(
            "
              rules:
                - topics: [cards]
                  fields:
                    - path: pan
                      strategy: hash
            ",
        )
        .unwrap_err();
        let whole_key = obfuscated(
            "
              rules:
                - topics: [cards]
                  key: hash
            ",
        )
        .unwrap_err();

        let expected = "obfuscation: hash strategy requires a secret at line 6 column 15";
        assert_eq!(fields, expected);
        assert_eq!(whole_key, expected);
    }

    #[test]
    fn rejects_a_secret_with_too_little_key_material() {
        let error = obfuscated(
            "
              secret: {value: short}
              rules:
                - topics: [cards]
                  value: hash
            ",
        )
        .unwrap_err();

        assert!(
            error
                .to_string()
                .contains("secret: must decode to at least 32 bytes")
        );
    }

    #[test]
    fn a_secret_is_base64_only_when_it_decodes_to_enough_bytes() {
        obfuscated(
            "
              secret: {value: BwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwc=}
              rules:
                - topics: [cards]
                  value: hash
            ",
        )
        .expect("base64 of 32 bytes");

        let error = obfuscated(
            "
              secret: {value: BwcHBwcHBwcHBwcHBwcHBw==}
              rules:
                - topics: [cards]
                  value: hash
            ",
        )
        .unwrap_err();

        assert!(
            error.contains("secret: must decode to at least 32 bytes, got 24"),
            "{error}"
        );
    }

    #[test]
    fn a_short_secret_reports_its_length() {
        let error = obfuscated(
            "
              secret: {value: short}
              rules:
                - topics: [cards]
                  value: mask
            ",
        )
        .unwrap_err();

        assert!(
            error.contains("secret: must decode to at least 32 bytes, got 5"),
            "{error}"
        );
    }

    #[test]
    fn debug_output_hides_the_key_bytes() {
        let key = KeyMaterial::<32>::parse("0123456789abcdef0123456789abcdef").unwrap();

        assert_eq!(format!("{key:?}"), "KeyMaterial { .. }");
    }

    #[test]
    fn a_blank_secret_is_too_short_rather_than_absent() {
        let error = obfuscated(
            "
              secret: {value: '   '}
              rules:
                - topics: [cards]
                  value: mask
            ",
        )
        .unwrap_err();

        assert!(
            error.contains("secret: must decode to at least 32 bytes, got 0"),
            "{error}"
        );
    }

    #[test]
    fn rejects_empty_rules_and_rules_that_do_nothing() {
        let error = obfuscated(
            "
              rules: []
            ",
        )
        .unwrap_err();
        assert_eq!(
            error,
            "obfuscation.rules: must not be empty at line 6 column 22"
        );

        let error = obfuscated(
            "
              rules:
                - topics: []
                  value: mask
            ",
        )
        .unwrap_err();
        assert_eq!(
            error,
            "obfuscation.rules[0].topics: must not be empty at line 7 column 27"
        );

        let error = obfuscated(
            "
              rules:
                - topics: [cards, 'audit.*']
            ",
        )
        .unwrap_err();
        assert_eq!(
            error,
            "obfuscation: rule for 'cards, audit.*' must set at least one of fields, key, \
             value, headers or patterns at line 6 column 15"
        );
    }

    #[test]
    fn rejects_paths_topics_and_headers_that_cannot_mean_anything() {
        let error = obfuscated(
            "
              rules:
                - topics: [cards]
                  fields:
                    - path: card..number
                      strategy: mask
            ",
        )
        .unwrap_err();
        assert_eq!(
            error,
            "obfuscation.rules[0].fields[0].path: field path 'card..number' must not have \
             empty segments at line 9 column 29"
        );

        let error = obfuscated(
            "
              rules:
                - topics: ['pay*ments']
                  value: mask
            ",
        )
        .unwrap_err();
        assert_eq!(
            error,
            "obfuscation.rules[0].topics[0]: topic 'pay*ments': '*' is only allowed as the \
             last character at line 7 column 28"
        );

        let error = obfuscated(
            "
              rules:
                - topics: [cards]
                  headers: [x-user-id, ' ']
            ",
        )
        .unwrap_err();
        assert_eq!(
            error,
            "obfuscation.rules[0].headers: must not contain empty values at line 8 column 28"
        );
    }

    #[test]
    fn accepts_pattern_rules_and_rejects_ones_that_say_nothing() {
        obfuscated(
            "
              secret: {value: 0123456789abcdef0123456789abcdef}
              rules:
                - topics: ['app.logs']
                  patterns:
                    - regex: '\\b\\d{13,19}\\b'
                      strategy: hash
                    - regex: '[\\w.+-]+@[\\w-]+\\.[\\w.]+'
                      strategy: mask
            ",
        )
        .expect("patterns are a rule of their own");

        let error = obfuscated(
            "
              rules:
                - topics: ['app.logs']
                  patterns:
                    - regex: '  '
                      strategy: mask
            ",
        )
        .unwrap_err();
        assert_eq!(
            error,
            "obfuscation.rules[0].patterns[0].regex: pattern must not be empty \
             at line 9 column 30"
        );
    }

    #[test]
    fn a_pattern_compiles_at_load() {
        let config = parse_cluster(
            r"
            bootstrap_servers: [broker:9092]
            obfuscation:
              rules:
                - topics: ['app.logs']
                  patterns:
                    - regex: '\d{4}'
                      strategy: mask
            ",
        )
        .unwrap();

        let pattern = &config.obfuscation.unwrap().rules[0].patterns[0];
        assert_eq!(
            pattern.regex.as_regex().replace_all("pin 1234", "#"),
            "pin #"
        );
    }

    #[test]
    fn rejects_a_pattern_that_does_not_compile() {
        let error = obfuscated(
            "
              rules:
                - topics: ['app.logs']
                  patterns:
                    - regex: '[unclosed'
                      strategy: mask
            ",
        )
        .unwrap_err();

        assert_eq!(
            error,
            "obfuscation.rules[0].patterns[0].regex: invalid pattern '[unclosed': \
             regex parse error:\n    [unclosed\n    ^\nerror: unclosed character class \
             at line 9 column 30"
        );
    }

    #[test]
    fn rejects_a_pattern_that_matches_everywhere_at_once() {
        let error = obfuscated(
            r"
              rules:
                - topics: ['app.logs']
                  patterns:
                    - regex: '\d*'
                      strategy: mask
            ",
        )
        .unwrap_err();

        assert_eq!(
            error,
            r"obfuscation.rules[0].patterns[0].regex: invalid pattern '\d*': it matches the empty string at line 9 column 30"
        );
    }

    #[test]
    fn rejects_a_hashing_pattern_without_a_secret() {
        let error = obfuscated(
            "
              rules:
                - topics: ['app.logs']
                  patterns:
                    - regex: '\\d{13,19}'
                      strategy: hash
            ",
        )
        .unwrap_err();

        assert_eq!(
            error,
            "obfuscation: hash strategy requires a secret at line 6 column 15"
        );
    }

    #[test]
    fn rejects_two_rules_that_cover_one_topic() {
        let overlaps = [
            ("payments.cards", "payments.cards"),
            ("payments.*", "payments.cards"),
            ("payments.cards", "payments.*"),
            ("payments.*", "payments.eu.*"),
        ];

        for (first, second) in overlaps {
            let error = obfuscated(&format!(
                "
              rules:
                - topics: ['{first}']
                  value: mask
                - topics: ['{second}']
                  value: drop
            "
            ))
            .unwrap_err();

            assert_eq!(
                error,
                format!(
                    "obfuscation: topics '{second}' and '{first}' match the same topics; \
                     a topic must be covered by exactly one rule at line 6 column 15"
                )
            );
        }
    }

    #[test]
    fn accepts_rules_that_only_look_alike() {
        obfuscated(
            "
              rules:
                - topics: ['payments.*']
                  value: mask
                - topics: ['payment', 'payments']
                  value: drop
            ",
        )
        .expect("a prefix rule does not cover the name it was built from");

        obfuscated(
            "
              rules:
                - topics: ['payments.cards']
                  value: mask
                - topics: ['payments.wallets', 'audit.*']
                  value: drop
            ",
        )
        .unwrap();
    }

    fn load_yaml(yaml: &str) -> Result<Config, ConfigError> {
        Config::parse(Path::new("test.yaml"), yaml)
    }

    fn with_client_secret(source: &str) -> String {
        format!(
            "
            bind: 127.0.0.1:8080
            auth:
              oidc:
                issuer: https://idp.example
                client_id: klens
                client_secret: {source}
                redirect_uri: https://klens.example/api/auth/callback
            "
        )
    }

    fn client_secret(source: &str) -> Result<String, String> {
        load_yaml(&with_client_secret(source))
            .map(|config| {
                config
                    .auth
                    .unwrap()
                    .oidc
                    .client_secret
                    .expose_secret()
                    .to_owned()
            })
            .map_err(|error| error.to_string())
    }

    struct TempFile(PathBuf);

    impl TempFile {
        fn new(name: &str, contents: &str) -> Self {
            let path = std::env::temp_dir().join(format!("klens-{}-{name}", std::process::id()));
            std::fs::write(&path, contents).unwrap();
            Self(path)
        }
    }

    impl Drop for TempFile {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.0);
        }
    }

    #[test]
    fn a_secret_reads_from_its_value_env_or_file() {
        assert_eq!(client_secret("{value: inline}").unwrap(), "inline");
        assert_eq!(
            client_secret("{env: CARGO_PKG_NAME}").unwrap(),
            env!("CARGO_PKG_NAME")
        );

        let file = TempFile::new("client-secret", "from-file\r\n");
        assert_eq!(
            client_secret(&format!("{{file: '{}'}}", file.0.display())).unwrap(),
            "from-file"
        );
    }

    #[test]
    fn a_secret_keeps_characters_yaml_would_read_as_structure() {
        assert_eq!(
            client_secret("{value: 'hunter2 #tail'}").unwrap(),
            "hunter2 #tail"
        );

        let file = TempFile::new("yaml-chars", "p@ss: *word #x\n");
        assert_eq!(
            client_secret(&format!("{{file: '{}'}}", file.0.display())).unwrap(),
            "p@ss: *word #x"
        );
    }

    #[test]
    fn a_missing_secret_source_names_the_field_and_source() {
        let error = client_secret("{env: KLENS_TEST_UNSET_VARIABLE}").unwrap_err();
        assert!(error.contains("test.yaml"), "{error}");
        assert!(error.contains("auth.oidc.client_secret"), "{error}");
        assert!(
            error.contains("environment variable KLENS_TEST_UNSET_VARIABLE is not set"),
            "{error}"
        );

        let error = client_secret("{file: /nonexistent/klens-secret}").unwrap_err();
        assert!(
            error.contains("failed to read secret file /nonexistent/klens-secret"),
            "{error}"
        );
    }

    #[test]
    fn a_plain_secret_is_rejected_without_echoing_it() {
        for source in ["hunter2", "123456", "{value: 123456}"] {
            let error = client_secret(source).unwrap_err();
            assert!(!error.contains("hunter2"), "{error}");
            assert!(!error.contains("123456"), "{error}");
            assert!(error.contains("auth.oidc.client_secret"), "{error}");
        }

        assert!(
            client_secret("hunter2")
                .unwrap_err()
                .contains("a secret must name its source")
        );
        assert!(
            client_secret("{value: 123456}")
                .unwrap_err()
                .contains("a secret value must be a string")
        );
    }

    #[test]
    fn a_secret_of_the_wrong_shape_names_what_it_expected() {
        let cases = [
            (
                "[hunter2]",
                "invalid type: sequence, expected a secret must name its source: \
                 {value: ...}, {env: NAME} or {file: PATH}",
            ),
            (
                "true",
                "a secret must name its source: {value: ...}, {env: NAME} or {file: PATH}",
            ),
            (
                "{value: [hunter2]}",
                "invalid type: sequence, expected a secret value must be a string; quote it",
            ),
            ("{value: true}", "a secret value must be a string; quote it"),
            ("{value: -5}", "a secret value must be a string; quote it"),
            ("{value: 1.5}", "a secret value must be a string; quote it"),
        ];

        for (source, expected) in cases {
            let error = client_secret(source).unwrap_err();
            assert!(error.contains(expected), "{source}: {error}");
            assert!(!error.contains("hunter2"), "{source}: {error}");
        }
    }

    #[test]
    fn secrets_compare_by_value() {
        assert_eq!(Secret::from("same"), Secret::from("same"));
        assert_ne!(Secret::from("same"), Secret::from("other"));
    }

    #[test]
    fn debug_output_hides_secrets() {
        let config = load_yaml(&with_client_secret("{value: oidc-secret}")).unwrap();

        let debug = format!("{config:?}");
        assert!(!debug.contains("oidc-secret"), "{debug}");
        assert!(debug.contains("client_secret: Secret(..)"), "{debug}");
    }

    #[test]
    fn a_short_session_key_is_rejected_at_load() {
        let error = load_yaml(
            "
            bind: 127.0.0.1:8080
            auth:
              session_key: {value: too-short}
              oidc:
                issuer: https://idp.example
                client_id: klens
                client_secret: {value: secret}
                redirect_uri: https://klens.example/api/auth/callback
            ",
        )
        .unwrap_err()
        .to_string();

        assert!(
            error.contains("auth.session_key: must decode to at least 32 bytes, got 9"),
            "{error}"
        );
    }

    #[test]
    fn load_reads_the_config_file_and_its_secret_files() {
        let secret = TempFile::new("load-secret", "from-file\n");
        let config = TempFile::new(
            "load-config.yaml",
            &with_client_secret(&format!("{{file: '{}'}}", secret.0.display())),
        );

        let loaded = Config::load(&config.0).unwrap();
        assert_eq!(
            loaded.auth.unwrap().oidc.client_secret.expose_secret(),
            "from-file"
        );
    }
}
