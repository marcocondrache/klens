use std::collections::{BTreeMap, HashSet};
use std::fmt::{Display, Formatter};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use secrecy::{ExposeSecret, SecretString};
use serde::de::{self, MapAccess, Visitor, value::MapAccessDeserializer};
use serde::{Deserialize, Deserializer};
use thiserror::Error;

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
    #[error("invalid configuration for cluster '{cluster}': {reason}")]
    InvalidCluster { cluster: String, reason: String },
    #[error("invalid authentication configuration: {reason}")]
    InvalidAuth { reason: String },
}

impl ConfigError {
    pub(crate) fn invalid_cluster(cluster: impl Into<String>, reason: impl Into<String>) -> Self {
        Self::InvalidCluster {
            cluster: cluster.into(),
            reason: reason.into(),
        }
    }

    pub(crate) fn invalid_auth(reason: impl Into<String>) -> Self {
        Self::InvalidAuth {
            reason: reason.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    #[serde(deserialize_with = "deserialize_bind")]
    pub bind: SocketAddr,
    #[serde(default = "default_log_level")]
    pub log_level: String,
    #[serde(default)]
    pub clusters: Vec<ClusterConfig>,
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
        let config: Self = serde_yaml_ng::from_str(raw).map_err(|source| ConfigError::Parse {
            path: path.to_owned(),
            source,
        })?;

        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> Result<(), ConfigError> {
        if let Some(auth) = &self.auth {
            auth.oidc.validate()?;
            if let Some(roles) = &auth.roles {
                roles.validate()?;
            }
        }

        let mut seen = HashSet::with_capacity(self.clusters.len());

        for cluster in &self.clusters {
            cluster.validate()?;

            let name = cluster.name.trim().to_owned();
            if !seen.insert(name.clone()) {
                return Err(ConfigError::invalid_cluster(name, "duplicate cluster name"));
            }
        }

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuthConfig {
    pub oidc: OidcConfig,
    #[serde(default)]
    pub roles: Option<RolesConfig>,
    /// Signing key for the session cookie, as base64 or raw text of at least
    /// 32 bytes. Without one, every restart invalidates every session.
    #[serde(default)]
    pub session_key: Option<KeyMaterial<MIN_SESSION_KEY_BYTES>>,
}

const MAX_ROLE_DEFINITIONS: usize = 64;

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RolesConfig {
    #[serde(default = "default_groups_claim")]
    pub claim: String,
    /// Role name to the privileges it grants. An empty list is valid and
    /// means the bound clusters are visible but nothing privileged is.
    pub definitions: BTreeMap<String, Vec<PrivilegeName>>,
    pub bindings: Vec<RoleBinding>,
}

pub fn default_groups_claim() -> String {
    "groups".to_owned()
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RoleBinding {
    pub groups: Vec<String>,
    pub role: String,
    #[serde(default)]
    pub clusters: Option<Vec<String>>,
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

impl RolesConfig {
    fn validate(&self) -> Result<(), ConfigError> {
        let fail = |reason: &str| Err(ConfigError::invalid_auth(reason));

        if self.claim.trim().is_empty() {
            return fail("roles claim must not be empty");
        }

        if self.definitions.is_empty() {
            return fail("roles definitions must not be empty");
        }

        if self.definitions.len() > MAX_ROLE_DEFINITIONS {
            return fail(&format!(
                "too many role definitions (at most {MAX_ROLE_DEFINITIONS})"
            ));
        }

        for (role, privileges) in &self.definitions {
            if role.trim().is_empty() {
                return fail("role definition name must not be empty");
            }

            let mut seen = HashSet::with_capacity(privileges.len());
            for privilege in privileges {
                if !seen.insert(privilege) {
                    return fail(&format!("role '{role}' lists '{privilege}' more than once"));
                }
            }
        }

        if self.bindings.is_empty() {
            return fail("roles bindings must not be empty");
        }

        for binding in &self.bindings {
            if !self.definitions.contains_key(&binding.role) {
                return fail(&format!(
                    "roles binding references unknown role '{}'",
                    binding.role
                ));
            }
            if binding.groups.is_empty() {
                return fail("roles binding groups must not be empty");
            }
            if binding.groups.iter().any(|group| group.trim().is_empty()) {
                return fail("roles binding groups must not contain empty values");
            }
            if let Some(clusters) = &binding.clusters {
                if clusters.is_empty() {
                    return fail("roles binding clusters must not be empty when set");
                }
                if clusters.iter().any(|cluster| cluster.trim().is_empty()) {
                    return fail("roles binding clusters must not contain empty values");
                }
            }
        }

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OidcConfig {
    pub issuer: String,
    pub client_id: String,
    pub client_secret: Secret,
    pub redirect_uri: String,
    #[serde(default = "default_scopes")]
    pub scopes: Vec<String>,
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
        self.cookie_secure.unwrap_or_else(|| {
            url::Url::parse(&self.redirect_uri)
                .map(|parsed| parsed.scheme() == "https")
                .unwrap_or(false)
        })
    }

    pub fn effective_scopes(&self) -> Vec<String> {
        let mut scopes = self.scopes.clone();
        if !scopes.iter().any(|scope| scope == "openid") {
            scopes.insert(0, "openid".to_owned());
        }
        scopes
    }

    fn validate(&self) -> Result<(), ConfigError> {
        let fail = |reason: &str| Err(ConfigError::invalid_auth(reason));

        if self.client_id.trim().is_empty() {
            return fail("oidc client_id must not be empty");
        }

        if self.client_secret.expose_secret().trim().is_empty() {
            return fail("oidc client_secret must not be empty");
        }

        validate_http_url("issuer", &self.issuer)?;
        validate_http_url("redirect_uri", &self.redirect_uri)?;

        if self.scopes.iter().any(|scope| scope.trim().is_empty()) {
            return fail("oidc scopes must not contain empty values");
        }

        Ok(())
    }
}

fn parse_http_url(field: &str, value: &str) -> Result<(), String> {
    let parsed =
        url::Url::parse(value).map_err(|error| format!("{field} is not a valid URL: {error}"))?;

    if parsed.scheme() != "http" && parsed.scheme() != "https" {
        return Err(format!("{field} must be an http or https URL"));
    }

    if parsed.host_str().is_none() {
        return Err(format!("{field} must include a host"));
    }

    Ok(())
}

fn validate_http_url(field: &str, value: &str) -> Result<(), ConfigError> {
    parse_http_url(&format!("oidc {field}"), value).map_err(ConfigError::invalid_auth)
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClusterConfig {
    pub name: String,
    pub bootstrap_servers: Vec<String>,
    #[serde(default)]
    pub security: Option<SecurityConfig>,
    #[serde(default)]
    pub schema_registry: Option<SchemaRegistryConfig>,
    #[serde(default)]
    pub obfuscation: Option<ObfuscationConfig>,
    #[serde(default)]
    pub properties: KafkaProperties,
    #[serde(default)]
    pub ingest: ClusterIngestConfig,
}

/// Per-cluster ingest cadence, in seconds. Omitted keys use the defaults.
///
/// Values must be at least 1; sub-second polling is rejected at load.
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

impl ClusterIngestConfig {
    fn validate(&self, cluster: &str) -> Result<(), ConfigError> {
        let fields = [
            ("ingest.topology_secs", self.topology_secs),
            ("ingest.watermark_secs", self.watermark_secs),
            ("ingest.config_secs", self.config_secs),
            ("ingest.subjects_secs", self.subjects_secs),
            ("ingest.offset_tick_secs", self.offset_tick_secs),
            ("ingest.fast_offset_secs", self.fast_offset_secs),
            ("ingest.slow_offset_secs", self.slow_offset_secs),
        ];

        for (field, secs) in fields {
            if secs < 1 {
                return Err(ConfigError::invalid_cluster(
                    cluster,
                    format!("{field} must be at least 1"),
                ));
            }
        }

        Ok(())
    }
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
    pub url: String,
    #[serde(default)]
    pub username: Option<String>,
    #[serde(default)]
    pub password: Option<Secret>,
}

impl SchemaRegistryConfig {
    fn validate(&self, cluster: &str) -> Result<(), ConfigError> {
        let fail = |reason: &str| Err(ConfigError::invalid_cluster(cluster, reason));

        parse_http_url("schema_registry.url", &self.url)
            .map_err(|reason| ConfigError::invalid_cluster(cluster, reason))?;

        let has_user = self
            .username
            .as_ref()
            .is_some_and(|username| !username.trim().is_empty());
        let has_pass = self
            .password
            .as_ref()
            .is_some_and(|password| !password.expose_secret().is_empty());

        if has_user != has_pass {
            return fail("schema_registry username and password must be set together");
        }

        if self
            .username
            .as_ref()
            .is_some_and(|username| username.trim().is_empty())
        {
            return fail("schema_registry username must not be empty");
        }

        Ok(())
    }
}

impl ClusterConfig {
    pub fn validate(&self) -> Result<(), ConfigError> {
        let fail = |reason: &str| Err(ConfigError::invalid_cluster(self.name.clone(), reason));

        if self.name.trim().is_empty() {
            return fail("name must not be empty");
        }

        if self.bootstrap_servers.is_empty() {
            return fail("bootstrap_servers must not be empty");
        }

        if let Some(security) = &self.security {
            let needs_sasl = matches!(
                security.protocol,
                SecurityProtocol::SaslPlaintext | SecurityProtocol::SaslSsl
            );

            if needs_sasl && security.sasl.is_none() {
                return fail("sasl settings are required for SASL protocols");
            }

            if let Some(tls) = &security.tls
                && tls.client_cert.is_some() != tls.client_key.is_some()
            {
                return fail("client_cert and client_key must be set together");
            }
        }

        if let Some(schema_registry) = &self.schema_registry {
            schema_registry.validate(&self.name)?;
        }

        if let Some(obfuscation) = &self.obfuscation {
            obfuscation.validate(&self.name)?;
        }

        self.ingest.validate(&self.name)?;

        Ok(())
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
                // Files written by `echo` or editors end in a newline that is
                // never part of the secret.
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

        fn visit_string<E: de::Error>(self, value: String) -> Result<SecretString, E> {
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

pub const OBFUSCATION_MASK: &str = "***";

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObfuscationConfig {
    /// Key for `hash` tokens, as base64 or raw text of at least 32 bytes.
    /// Required as soon as one rule hashes. Rotating it changes every token,
    /// so correlation across the rotation is lost.
    #[serde(default)]
    pub secret: Option<KeyMaterial<MIN_OBFUSCATION_SECRET_BYTES>>,
    pub rules: Vec<ObfuscationRule>,
}

/// What to do with a value that never became JSON, because the registry is
/// down, the schema is gone, or the bytes were never framed.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum UnparsedPolicy {
    /// Fail closed: the whole value is masked.
    #[default]
    Mask,
    /// Fail open: undecodable values are served as they came off the wire.
    Allow,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObfuscationRule {
    /// Exact topic names, or a trailing-`*` prefix. No topic may be matched
    /// by two rules.
    pub topics: Vec<String>,
    /// Dotted paths into decoded JSON. An array met mid-path fans out over
    /// its elements, so `items.sku` covers every element's `sku`.
    #[serde(default)]
    pub fields: Vec<ObfuscationField>,
    /// Strategy for the whole record key, applied after any field rules.
    #[serde(default)]
    pub key: Option<ObfuscationStrategy>,
    /// Strategy for the whole record value, applied after any field rules.
    #[serde(default)]
    pub value: Option<ObfuscationStrategy>,
    /// Header names whose values are masked.
    #[serde(default)]
    pub headers: Vec<String>,
    /// Regexes applied to the rendered text of key and value, for topics
    /// whose payloads never become JSON. Each match is replaced by its
    /// strategy's output.
    #[serde(default)]
    pub patterns: Vec<ObfuscationPattern>,
    #[serde(default)]
    pub unparsed: UnparsedPolicy,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObfuscationPattern {
    pub regex: String,
    pub strategy: ObfuscationStrategy,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObfuscationField {
    pub path: String,
    pub strategy: ObfuscationStrategy,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ObfuscationStrategy {
    /// Replace with `***`.
    Mask,
    /// Replace with a deterministic keyed token, so equal values still
    /// render equal.
    Hash,
    /// Remove the field entirely, or the matched span for a pattern rule.
    Drop,
}

impl ObfuscationStrategy {
    fn needs_secret(self) -> bool {
        matches!(self, Self::Hash)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TopicPattern<'a> {
    Exact(&'a str),
    Prefix(&'a str),
}

impl<'a> TopicPattern<'a> {
    pub fn parse(pattern: &'a str) -> Result<Self, String> {
        if pattern.trim().is_empty() {
            return Err("topic must not be empty".to_owned());
        }

        match pattern.strip_suffix('*') {
            Some(prefix) if !prefix.contains('*') => Ok(Self::Prefix(prefix)),
            None if !pattern.contains('*') => Ok(Self::Exact(pattern)),
            _ => Err("'*' is only allowed as the last character".to_owned()),
        }
    }

    pub fn matches(self, topic: &str) -> bool {
        match self {
            Self::Exact(name) => topic == name,
            Self::Prefix(prefix) => topic.starts_with(prefix),
        }
    }

    fn overlaps(self, other: Self) -> bool {
        match (self, other) {
            (Self::Exact(left), Self::Exact(right)) => left == right,
            (Self::Exact(name), Self::Prefix(prefix))
            | (Self::Prefix(prefix), Self::Exact(name)) => name.starts_with(prefix),
            (Self::Prefix(left), Self::Prefix(right)) => {
                left.starts_with(right) || right.starts_with(left)
            }
        }
    }

    fn source(self) -> String {
        match self {
            Self::Exact(name) => name.to_owned(),
            Self::Prefix(prefix) => format!("{prefix}*"),
        }
    }
}

impl ObfuscationConfig {
    pub(crate) fn validate(&self, cluster: &str) -> Result<(), ConfigError> {
        let fail = |reason: String| Err(ConfigError::invalid_cluster(cluster, reason));

        if self.rules.is_empty() {
            return fail("obfuscation rules must not be empty".to_owned());
        }

        let hashed = self.secret.is_some();
        let mut selectors: Vec<TopicPattern<'_>> = Vec::new();

        for rule in &self.rules {
            rule.validate(cluster, hashed)?;

            for topic in &rule.topics {
                let pattern = TopicPattern::parse(topic).map_err(|reason| {
                    ConfigError::invalid_cluster(
                        cluster,
                        format!("obfuscation topic '{topic}': {reason}"),
                    )
                })?;

                if let Some(other) = selectors.iter().find(|other| other.overlaps(pattern)) {
                    return fail(format!(
                        "obfuscation topics '{topic}' and '{}' match the same topics; \
                         a topic must be covered by exactly one rule",
                        other.source()
                    ));
                }
            }

            selectors.extend(
                rule.topics
                    .iter()
                    .filter_map(|topic| TopicPattern::parse(topic).ok()),
            );
        }

        Ok(())
    }
}

impl ObfuscationRule {
    fn validate(&self, cluster: &str, has_secret: bool) -> Result<(), ConfigError> {
        let fail = |reason: String| Err(ConfigError::invalid_cluster(cluster, reason));

        if self.topics.is_empty() {
            return fail("obfuscation rule topics must not be empty".to_owned());
        }

        if self.fields.is_empty()
            && self.key.is_none()
            && self.value.is_none()
            && self.headers.is_empty()
            && self.patterns.is_empty()
        {
            return fail(format!(
                "obfuscation rule for '{}' must set at least one of fields, key, value, headers or patterns",
                self.topics.join(", ")
            ));
        }

        for field in &self.fields {
            if field.path.trim().is_empty() || field.path.split('.').any(str::is_empty) {
                return fail(format!(
                    "obfuscation field path '{}' must not have empty segments",
                    field.path
                ));
            }
        }

        for pattern in &self.patterns {
            if pattern.regex.trim().is_empty() {
                return fail("obfuscation patterns must not be empty".to_owned());
            }
        }

        if self.headers.iter().any(|header| header.trim().is_empty()) {
            return fail("obfuscation header names must not be empty".to_owned());
        }

        let hashes = self
            .fields
            .iter()
            .map(|field| field.strategy)
            .chain(self.patterns.iter().map(|pattern| pattern.strategy))
            .chain(self.key)
            .chain(self.value)
            .any(ObfuscationStrategy::needs_secret);

        if hashes && !has_secret {
            return fail("obfuscation hash strategy requires a secret".to_owned());
        }

        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
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
#[serde(deny_unknown_fields)]
pub struct SecurityConfig {
    pub protocol: SecurityProtocol,
    #[serde(default)]
    pub sasl: Option<SaslConfig>,
    #[serde(default)]
    pub tls: Option<TlsConfig>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SaslConfig {
    pub mechanism: SaslMechanism,
    pub username: String,
    pub password: Secret,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TlsConfig {
    #[serde(default)]
    pub ca_cert: Option<PathBuf>,
    #[serde(default)]
    pub client_cert: Option<PathBuf>,
    #[serde(default)]
    pub client_key: Option<PathBuf>,
    #[serde(default)]
    pub insecure_skip_verify: bool,
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

    #[test]
    fn parses_root_config() {
        let config = parse_config(
            "
            bind: 0.0.0.0:8080
            clusters:
              - name: local
                bootstrap_servers:
                  - localhost:9092
              - name: staging
                bootstrap_servers:
                  - broker-1:9092
                  - broker-2:9092
            ",
        )
        .unwrap();

        assert_eq!(config.clusters.len(), 2);
        assert_eq!(config.clusters[0].name, "local");
        assert_eq!(config.clusters[1].name, "staging");
        assert_eq!(config.bind, "0.0.0.0:8080".parse().unwrap());
        assert_eq!(config.log_level, "info");
        assert_eq!(config.auth, None);
        assert_eq!(
            config.clusters[0].ingest,
            ClusterIngestConfig::default(),
            "omitted ingest uses the documented defaults"
        );
        config.validate().unwrap();
    }

    #[test]
    fn parses_cluster_ingest_overrides_and_fills_omitted_keys() {
        let cluster = parse_cluster(
            "
            name: prod
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
        cluster.validate().unwrap();
    }

    #[test]
    fn rejects_unknown_ingest_keys() {
        let error = parse_cluster(
            "
            name: prod
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
        let cluster = parse_cluster(
            "
            name: prod
            bootstrap_servers:
              - kafka:9092
            ingest:
              topology_secs: 0
            ",
        )
        .unwrap();

        let error = cluster.validate().unwrap_err();
        assert!(
            error
                .to_string()
                .contains("ingest.topology_secs must be at least 1")
        );
    }

    #[test]
    fn rejects_catalog_poll_interval_yaml() {
        let error = parse_config(
            "
            bind: 127.0.0.1:8080
            catalog_poll_interval_secs: 15
            clusters: []
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
            clusters: []
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
            clusters: []
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
            clusters: []
            "
            )
            .is_err()
        );
    }

    #[test]
    fn parses_minimal_cluster() {
        let config = parse_cluster(
            "
            name: local
            bootstrap_servers:
              - localhost:9092
            ",
        )
        .unwrap();

        assert_eq!(config.name, "local");
        assert_eq!(config.bootstrap_servers, vec!["localhost:9092"]);
        assert_eq!(config.security, None);
        assert_eq!(config.schema_registry, None);
        assert_eq!(config.properties, KafkaProperties::default());
    }

    #[test]
    fn parses_bootstrap_server_list() {
        let config = parse_cluster(
            "
            name: local
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
            name: secure
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
                client_cert: /etc/client.pem
                client_key: /etc/client.key
                insecure_skip_verify: true
            properties:
              request_timeout_ms: 10000
            ",
        )
        .unwrap();

        let security = config.security.unwrap();
        assert_eq!(security.protocol, SecurityProtocol::SaslSsl);
        assert_eq!(security.sasl.unwrap().mechanism, SaslMechanism::ScramSha512);
        assert_eq!(security.tls.unwrap().ca_cert, Some("/etc/ca.pem".into()));
        assert_eq!(config.properties.request_timeout_ms, Some(10000));
    }

    #[test]
    fn rejects_unknown_fields() {
        assert!(
            parse_cluster(
                "
            name: local
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
            name: local
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
            clusters: []
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
            name: local
            bootstrap_servers: localhost:9092
            "
            )
            .is_err()
        );
    }

    #[test]
    fn rejects_empty_bootstrap_servers() {
        let config = parse_cluster(
            "
            name: local
            bootstrap_servers: []
            ",
        )
        .unwrap();

        let error = config.validate().unwrap_err();
        assert!(
            error
                .to_string()
                .contains("bootstrap_servers must not be empty")
        );
    }

    #[test]
    fn validation_accepts_plaintext_without_sasl() {
        let config = ClusterConfig {
            name: "local".to_owned(),
            bootstrap_servers: vec!["localhost:9092".to_owned()],
            security: None,
            schema_registry: None,
            obfuscation: None,
            properties: KafkaProperties::default(),
            ingest: ClusterIngestConfig::default(),
        };

        config.validate().unwrap();
    }

    #[test]
    fn validation_requires_sasl_for_sasl_protocols() {
        let config = parse_cluster(
            "
            name: local
            bootstrap_servers:
              - localhost:9092
            security:
              protocol: SASL_PLAINTEXT
            ",
        )
        .unwrap();

        let error = config.validate().unwrap_err();
        assert!(error.to_string().contains("sasl settings are required"));
    }

    #[test]
    fn validation_requires_client_cert_and_key_together() {
        let config = parse_cluster(
            "
            name: local
            bootstrap_servers:
              - localhost:9092
            security:
              protocol: SSL
              tls:
                client_cert: /etc/client.pem
            ",
        )
        .unwrap();

        let error = config.validate().unwrap_err();
        assert!(error.to_string().contains("must be set together"));
    }

    #[test]
    fn validation_rejects_duplicate_cluster_names() {
        let config = parse_config(
            "
            bind: 127.0.0.1:8080
            clusters:
              - name: local
                bootstrap_servers:
                  - localhost:9092
              - name: local
                bootstrap_servers:
                  - localhost:9093
            ",
        )
        .unwrap();

        let error = config.validate().unwrap_err();
        assert!(error.to_string().contains("duplicate cluster name"));
    }

    #[test]
    fn parses_oidc_auth_config() {
        let config = parse_config(
            "
            bind: 127.0.0.1:8080
            clusters: []
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
        assert_eq!(oidc.issuer, "https://keycloak.example.com/realms/klens");
        assert_eq!(oidc.client_id, "klens");
        assert_eq!(oidc.client_secret.expose_secret(), "secret");
        assert_eq!(oidc.redirect_uri, "http://localhost:8080/api/auth/callback");
        assert_eq!(oidc.scopes, vec!["openid", "email", "profile"]);
        assert_eq!(oidc.cookie_secure, None);
        assert!(!oidc.cookie_secure());
        config.validate().unwrap();
    }

    #[test]
    fn parses_role_definitions_and_bindings() {
        let config = parse_config(
            "
            bind: 127.0.0.1:8080
            clusters: []
            auth:
              oidc:
                issuer: https://idp.example
                client_id: klens
                client_secret: {value: secret}
                redirect_uri: http://localhost:8080/api/auth/callback
              roles:
                definitions:
                  admin: [records, configs, schema_text, acls]
                  viewer: []
                  operator: [records, configs]
                bindings:
                  - groups: [klens-admins]
                    role: admin
                  - groups: [payments-viewers]
                    role: viewer
                    clusters: [payments]
            ",
        )
        .unwrap();

        let roles = config.auth.as_ref().unwrap().roles.as_ref().unwrap();
        assert_eq!(roles.claim, "groups");
        assert_eq!(
            roles.definitions["admin"],
            vec![
                PrivilegeName::Records,
                PrivilegeName::Configs,
                PrivilegeName::SchemaText,
                PrivilegeName::Acls,
            ]
        );
        assert!(roles.definitions["viewer"].is_empty());
        assert_eq!(
            roles.definitions["operator"],
            vec![PrivilegeName::Records, PrivilegeName::Configs]
        );
        assert_eq!(roles.bindings.len(), 2);
        assert_eq!(roles.bindings[0].role, "admin");
        assert_eq!(roles.bindings[0].clusters, None);
        assert_eq!(roles.bindings[1].role, "viewer");
        assert_eq!(
            roles.bindings[1].clusters.as_deref(),
            Some(["payments".to_owned()].as_slice())
        );
        config.validate().unwrap();
    }

    fn parse_roles(definitions: &str, bindings: &str) -> Config {
        parse_config(&format!(
            "
            bind: 127.0.0.1:8080
            clusters: []
            auth:
              oidc:
                issuer: https://idp.example
                client_id: klens
                client_secret: {{value: secret}}
                redirect_uri: http://localhost:8080/api/auth/callback
              roles:
                definitions:{definitions}
                bindings:{bindings}
            "
        ))
        .unwrap()
    }

    #[test]
    fn rejects_empty_role_bindings() {
        let config = parse_roles("\n                  admin: [records]", " []");

        let error = config.validate().unwrap_err();
        assert!(
            error
                .to_string()
                .contains("roles bindings must not be empty")
        );
    }

    #[test]
    fn rejects_empty_role_definitions() {
        let config = parse_roles(
            " {}",
            "
                  - groups: [klens-admins]
                    role: admin",
        );

        let error = config.validate().unwrap_err();
        assert!(
            error
                .to_string()
                .contains("roles definitions must not be empty")
        );
    }

    #[test]
    fn rejects_a_blank_role_definition_name() {
        let config = parse_roles(
            "
                  \"  \": [records]",
            "
                  - groups: [klens-admins]
                    role: \"  \"",
        );

        let error = config.validate().unwrap_err();
        assert!(
            error
                .to_string()
                .contains("role definition name must not be empty")
        );
    }

    #[test]
    fn rejects_a_repeated_privilege_in_a_definition() {
        let config = parse_roles(
            "
                  operator: [records, configs, records]",
            "
                  - groups: [kafka-operators]
                    role: operator",
        );

        let error = config.validate().unwrap_err();
        assert!(
            error
                .to_string()
                .contains("role 'operator' lists 'records' more than once"),
            "{error}"
        );
    }

    #[test]
    fn rejects_a_binding_naming_an_undefined_role() {
        let config = parse_roles(
            "
                  operator: [records]",
            "
                  - groups: [kafka-operators]
                    role: unknown-role",
        );

        let error = config.validate().unwrap_err();
        assert!(
            error
                .to_string()
                .contains("roles binding references unknown role 'unknown-role'"),
            "{error}"
        );
    }

    #[test]
    fn rejects_too_many_role_definitions() {
        let definitions: String = (0..=MAX_ROLE_DEFINITIONS)
            .map(|index| format!("\n                  role{index}: [records]"))
            .collect();
        let config = parse_roles(
            &definitions,
            "
                  - groups: [klens-admins]
                    role: role0",
        );

        let error = config.validate().unwrap_err();
        assert!(
            error.to_string().contains("too many role definitions"),
            "{error}"
        );
    }

    #[test]
    fn allows_definitions_no_binding_uses() {
        let config = parse_roles(
            "
                  admin: [records, configs, schema_text, acls]
                  auditor: [acls, schema_text]",
            "
                  - groups: [klens-admins]
                    role: admin",
        );

        config.validate().unwrap();
    }

    #[test]
    fn oidc_cookie_secure_follows_redirect_uri_and_override() {
        let https = parse_config(
            "
            bind: 127.0.0.1:8080
            clusters: []
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
            clusters: []
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
            clusters: []
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

    #[test]
    fn rejects_invalid_oidc_issuer() {
        let config = parse_config(
            "
            bind: 127.0.0.1:8080
            clusters: []
            auth:
              oidc:
                issuer: not-a-url
                client_id: klens
                client_secret: {value: secret}
                redirect_uri: http://localhost:8080/api/auth/callback
            ",
        )
        .unwrap();

        let error = config.validate().unwrap_err();
        assert!(error.to_string().contains("issuer"));
    }

    #[test]
    fn rejects_empty_oidc_client_secret() {
        let config = parse_config(
            "
            bind: 127.0.0.1:8080
            clusters: []
            auth:
              oidc:
                issuer: https://idp.example
                client_id: klens
                client_secret: {value: '   '}
                redirect_uri: http://localhost:8080/api/auth/callback
            ",
        )
        .unwrap();

        let error = config.validate().unwrap_err();
        assert!(error.to_string().contains("client_secret"));
    }

    #[test]
    fn rejects_non_http_redirect_uri() {
        let config = parse_config(
            "
            bind: 127.0.0.1:8080
            clusters: []
            auth:
              oidc:
                issuer: https://idp.example
                client_id: klens
                client_secret: {value: secret}
                redirect_uri: ftp://localhost/api/auth/callback
            ",
        )
        .unwrap();

        let error = config.validate().unwrap_err();
        assert!(error.to_string().contains("redirect_uri"));
    }

    #[test]
    fn parses_schema_registry_settings() {
        let config = parse_cluster(
            "
            name: local
            bootstrap_servers:
              - localhost:9092
            schema_registry:
              url: http://localhost:8081
              username: user
              password: {value: secret}
            ",
        )
        .unwrap();

        let registry = config.schema_registry.as_ref().unwrap();
        assert_eq!(registry.url, "http://localhost:8081");
        assert_eq!(registry.username.as_deref(), Some("user"));
        assert_eq!(
            registry.password.as_ref().map(ExposeSecret::expose_secret),
            Some("secret")
        );
        config.validate().unwrap();
    }

    #[test]
    fn rejects_invalid_schema_registry_url() {
        let config = parse_cluster(
            "
            name: local
            bootstrap_servers:
              - localhost:9092
            schema_registry:
              url: not-a-url
            ",
        )
        .unwrap();

        let error = config.validate().unwrap_err();
        assert!(error.to_string().contains("schema_registry.url"));
    }

    #[test]
    fn rejects_mismatched_schema_registry_auth() {
        let config = parse_cluster(
            "
            name: local
            bootstrap_servers:
              - localhost:9092
            schema_registry:
              url: http://localhost:8081
              username: user
            ",
        )
        .unwrap();

        let error = config.validate().unwrap_err();
        assert!(
            error
                .to_string()
                .contains("username and password must be set together")
        );
    }

    #[test]
    fn parses_obfuscation_rules() {
        let config = parse_cluster(
            "
            name: payments
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
        config.validate().unwrap();

        let obfuscation = config.obfuscation.unwrap();
        assert_eq!(obfuscation.rules.len(), 2);
        assert_eq!(obfuscation.rules[0].fields[0].path, "card.number");
        assert_eq!(
            obfuscation.rules[0].fields[0].strategy,
            ObfuscationStrategy::Hash
        );
        assert_eq!(obfuscation.rules[0].unparsed, UnparsedPolicy::Allow);
        assert_eq!(obfuscation.rules[1].key, Some(ObfuscationStrategy::Mask));
        assert_eq!(obfuscation.rules[1].headers, vec!["x-user-id"]);
    }

    #[test]
    fn obfuscation_fails_closed_on_values_that_never_decode() {
        let config = parse_cluster(
            "
            name: payments
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
            name: payments
            bootstrap_servers:
              - broker:9092
            obfuscation:
{rules}
            "
        );

        parse_cluster(&yaml)
            .map_err(|error| error.to_string())?
            .validate()
            .map_err(|error| error.to_string())
    }

    #[test]
    fn rejects_hashing_without_a_secret() {
        let error = obfuscated(
            "
              rules:
                - topics: [cards]
                  fields:
                    - path: pan
                      strategy: hash
            ",
        )
        .unwrap_err();

        assert!(
            error
                .to_string()
                .contains("hash strategy requires a secret")
        );
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
        assert!(error.to_string().contains("rules must not be empty"));

        let error = obfuscated(
            "
              rules:
                - topics: []
                  value: mask
            ",
        )
        .unwrap_err();
        assert!(error.to_string().contains("topics must not be empty"));

        let error = obfuscated(
            "
              rules:
                - topics: [cards]
            ",
        )
        .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("at least one of fields, key, value, headers or patterns")
        );
    }

    #[test]
    fn rejects_paths_and_topics_that_cannot_mean_anything() {
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
        assert!(error.to_string().contains("must not have empty segments"));

        let error = obfuscated(
            "
              rules:
                - topics: ['pay*ments']
                  value: mask
            ",
        )
        .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("'*' is only allowed as the last character")
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
        assert!(error.to_string().contains("patterns must not be empty"));
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

        assert!(
            error
                .to_string()
                .contains("hash strategy requires a secret")
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

            assert!(
                error.to_string().contains("match the same topics"),
                "{first} and {second}: {error}"
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
