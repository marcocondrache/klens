use std::borrow::Cow;
use std::fmt::Write as _;
use std::sync::Arc;

use bytes::Bytes;
use foldhash::{HashMap, HashMapExt};
use hmac::{Hmac, Mac};
use regex::{Captures, Regex, RegexBuilder};
use sha2::Sha256;
use thiserror::Error;

use crate::config::{
    OBFUSCATION_MASK, ObfuscationConfig, ObfuscationStrategy, TopicPattern, UnparsedPolicy,
};

use super::payload::DecodedPayload;

const TOKEN_PREFIX: &str = "kx:";

const TOKEN_BYTES: usize = 8;

const REGEX_SIZE_LIMIT: usize = 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ObfuscationError {
    #[error("the hash strategy requires a secret")]
    MissingSecret,
    #[error("invalid topic '{pattern}': {reason}")]
    InvalidTopic { pattern: String, reason: String },
    #[error("invalid field path '{path}'")]
    InvalidPath { path: String },
    #[error("invalid pattern '{pattern}': {reason}")]
    InvalidPattern { pattern: String, reason: String },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Field {
    Key,
    Value,
}

#[derive(Debug)]
pub struct ObfuscationPolicy {
    exact: HashMap<Box<str>, Arc<TopicObfuscator>>,
    prefixes: Vec<(Box<str>, Arc<TopicObfuscator>)>,
}

impl ObfuscationPolicy {
    pub fn compile(config: &ObfuscationConfig) -> Result<Self, ObfuscationError> {
        let hasher = config
            .secret
            .as_ref()
            .map(|secret| Arc::new(KeyedHasher::new(secret.as_bytes())));

        let mut exact: HashMap<Box<str>, Arc<TopicObfuscator>> = HashMap::new();
        let mut prefixes: Vec<(Box<str>, Arc<TopicObfuscator>)> = Vec::new();

        for rule in &config.rules {
            let fields = rule
                .fields
                .iter()
                .map(|field| CompiledField::compile(&field.path, field.strategy))
                .collect::<Result<Vec<_>, _>>()?;

            let patterns = rule
                .patterns
                .iter()
                .map(|pattern| CompiledPattern::compile(&pattern.regex, pattern.strategy))
                .collect::<Result<Vec<_>, _>>()?;

            let obfuscator = Arc::new(TopicObfuscator {
                fields,
                patterns,
                key: rule.key,
                value: rule.value,
                headers: rule
                    .headers
                    .iter()
                    .map(|header| header.as_str().into())
                    .collect(),
                unparsed: rule.unparsed,
                hasher: hasher.clone(),
            });

            if obfuscator.hashes() && obfuscator.hasher.is_none() {
                return Err(ObfuscationError::MissingSecret);
            }

            for topic in &rule.topics {
                match TopicPattern::parse(topic).map_err(|reason| {
                    ObfuscationError::InvalidTopic {
                        pattern: topic.clone(),
                        reason,
                    }
                })? {
                    TopicPattern::Exact(name) => {
                        exact.insert(name.into(), Arc::clone(&obfuscator));
                    }
                    TopicPattern::Prefix(prefix) => {
                        prefixes.push((prefix.into(), Arc::clone(&obfuscator)));
                    }
                }
            }
        }

        prefixes.sort_by_key(|(prefix, _)| std::cmp::Reverse(prefix.len()));

        Ok(Self { exact, prefixes })
    }

    pub fn for_topic(&self, topic: &str) -> Option<Arc<TopicObfuscator>> {
        if let Some(obfuscator) = self.exact.get(topic) {
            return Some(Arc::clone(obfuscator));
        }

        self.prefixes
            .iter()
            .find(|(prefix, _)| topic.starts_with(prefix.as_ref()))
            .map(|(_, obfuscator)| Arc::clone(obfuscator))
    }
}

#[derive(Debug)]
pub struct TopicObfuscator {
    fields: Vec<CompiledField>,
    patterns: Vec<CompiledPattern>,
    key: Option<ObfuscationStrategy>,
    value: Option<ObfuscationStrategy>,
    headers: Vec<Box<str>>,
    unparsed: UnparsedPolicy,
    hasher: Option<Arc<KeyedHasher>>,
}

impl TopicObfuscator {
    pub fn hides_payload(&self) -> bool {
        !self.fields.is_empty()
            || !self.patterns.is_empty()
            || self.key.is_some()
            || self.value.is_some()
    }

    pub fn mask_headers(&self, headers: &mut [(Bytes, Option<Bytes>)]) {
        if self.headers.is_empty() {
            return;
        }

        for (name, value) in headers.iter_mut() {
            if self
                .headers
                .iter()
                .any(|masked| masked.as_bytes() == name.as_ref())
            {
                *value = Some(Bytes::from_static(OBFUSCATION_MASK.as_bytes()));
            }
        }
    }

    pub fn apply(&self, field: Field, slot: &mut Option<DecodedPayload>) {
        let whole = match field {
            Field::Key => self.key,
            Field::Value => self.value,
        };

        if whole == Some(ObfuscationStrategy::Drop) {
            *slot = None;
            return;
        }

        let Some(payload) = slot.as_mut() else {
            return;
        };

        match payload.json_mut() {
            Some(json) => {
                for rule in &self.fields {
                    rule.apply(json, self.hasher.as_deref());
                }
            }
            None if field == Field::Value
                && !self.fields.is_empty()
                && self.unparsed == UnparsedPolicy::Mask =>
            {
                payload.replace(OBFUSCATION_MASK.to_owned());
            }
            None => {}
        }

        match whole {
            Some(ObfuscationStrategy::Mask) => payload.replace(OBFUSCATION_MASK.to_owned()),
            Some(ObfuscationStrategy::Hash) => {
                let token = token(payload.text(), self.hasher.as_deref());
                payload.replace(token);
            }
            Some(ObfuscationStrategy::Drop) => {}
            None => self.rewrite_matches(payload),
        }
    }

    fn rewrite_matches(&self, payload: &mut DecodedPayload) {
        if self.patterns.is_empty() {
            return;
        }

        let mut text = Cow::Borrowed(payload.text());
        for pattern in &self.patterns {
            if let Cow::Owned(rewritten) = pattern.apply(text.as_ref(), self.hasher.as_deref()) {
                text = Cow::Owned(rewritten);
            }
        }

        if let Cow::Owned(text) = text {
            payload.replace(text);
        }
    }

    fn hashes(&self) -> bool {
        self.fields
            .iter()
            .map(|field| field.strategy)
            .chain(self.patterns.iter().map(|pattern| pattern.strategy))
            .chain(self.key)
            .chain(self.value)
            .any(|strategy| strategy == ObfuscationStrategy::Hash)
    }
}

#[derive(Debug)]
struct CompiledField {
    path: Box<[Box<str>]>,
    strategy: ObfuscationStrategy,
}

impl CompiledField {
    fn compile(path: &str, strategy: ObfuscationStrategy) -> Result<Self, ObfuscationError> {
        if path.trim().is_empty() || path.split('.').any(str::is_empty) {
            return Err(ObfuscationError::InvalidPath {
                path: path.to_owned(),
            });
        }

        Ok(Self {
            path: path.split('.').map(Box::from).collect(),
            strategy,
        })
    }

    fn apply(&self, json: &mut serde_json::Value, hasher: Option<&KeyedHasher>) {
        walk(json, &self.path, self.strategy, hasher);
    }
}

#[derive(Debug)]
struct CompiledPattern {
    regex: Regex,
    strategy: ObfuscationStrategy,
}

impl CompiledPattern {
    fn compile(source: &str, strategy: ObfuscationStrategy) -> Result<Self, ObfuscationError> {
        let invalid = |reason: String| ObfuscationError::InvalidPattern {
            pattern: source.to_owned(),
            reason,
        };

        let regex = RegexBuilder::new(source)
            .size_limit(REGEX_SIZE_LIMIT)
            .build()
            .map_err(|error| invalid(error.to_string()))?;

        if regex.is_match("") {
            return Err(invalid("it matches the empty string".to_owned()));
        }

        Ok(Self { regex, strategy })
    }

    fn apply<'a>(&self, text: &'a str, hasher: Option<&KeyedHasher>) -> Cow<'a, str> {
        match self.strategy {
            ObfuscationStrategy::Mask => replace_literal(&self.regex, text, OBFUSCATION_MASK),
            ObfuscationStrategy::Drop => replace_literal(&self.regex, text, ""),
            ObfuscationStrategy::Hash => self
                .regex
                .replace_all(text, |captures: &Captures<'_>| token(&captures[0], hasher)),
        }
    }
}

fn replace_literal<'a>(regex: &Regex, text: &'a str, replacement: &'static str) -> Cow<'a, str> {
    regex.replace_all(text, regex::NoExpand(replacement))
}

fn walk(
    value: &mut serde_json::Value,
    path: &[Box<str>],
    strategy: ObfuscationStrategy,
    hasher: Option<&KeyedHasher>,
) {
    let Some((head, rest)) = path.split_first() else {
        return;
    };

    match value {
        serde_json::Value::Array(items) => {
            for item in items {
                walk(item, path, strategy, hasher);
            }
        }
        serde_json::Value::Object(fields) if rest.is_empty() => match strategy {
            ObfuscationStrategy::Drop => {
                fields.remove(head.as_ref());
            }
            _ => {
                if let Some(found) = fields.get_mut(head.as_ref()) {
                    *found = serde_json::Value::String(match strategy {
                        ObfuscationStrategy::Hash => token(&leaf_text(found), hasher),
                        _ => OBFUSCATION_MASK.to_owned(),
                    });
                }
            }
        },
        serde_json::Value::Object(fields) => {
            if let Some(found) = fields.get_mut(head.as_ref()) {
                walk(found, rest, strategy, hasher);
            }
        }
        _ => {}
    }
}

fn leaf_text(value: &serde_json::Value) -> Cow<'_, str> {
    match value {
        serde_json::Value::String(text) => Cow::Borrowed(text),
        other => Cow::Owned(other.to_string()),
    }
}

fn token(value: &str, hasher: Option<&KeyedHasher>) -> String {
    match hasher {
        Some(hasher) => hasher.token(value),
        None => OBFUSCATION_MASK.to_owned(),
    }
}

pub struct KeyedHasher {
    mac: Hmac<Sha256>,
}

impl std::fmt::Debug for KeyedHasher {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("KeyedHasher")
            .finish_non_exhaustive()
    }
}

impl KeyedHasher {
    pub fn new(secret: &[u8]) -> Self {
        Self {
            mac: Hmac::<Sha256>::new_from_slice(secret).expect("hmac accepts any key length"),
        }
    }

    pub fn token(&self, value: &str) -> String {
        let mut mac = self.mac.clone();
        mac.update(value.as_bytes());
        let tag = mac.finalize().into_bytes();

        let mut token = String::with_capacity(TOKEN_PREFIX.len() + TOKEN_BYTES * 2);
        token.push_str(TOKEN_PREFIX);
        for byte in &tag[..TOKEN_BYTES] {
            let _ = write!(token, "{byte:02x}");
        }
        token
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(yaml: &str) -> ObfuscationConfig {
        serde_yaml_ng::from_str(yaml).expect("obfuscation config")
    }

    fn policy(yaml: &str) -> ObfuscationPolicy {
        ObfuscationPolicy::compile(&config(yaml)).expect("compiled policy")
    }

    fn payments() -> ObfuscationPolicy {
        policy(
            "
            secret: 0123456789abcdef0123456789abcdef
            rules:
              - topics: ['payments.*']
                fields:
                  - path: card.number
                    strategy: hash
                  - path: card.cvv
                    strategy: drop
                  - path: customer.email
                    strategy: mask
            ",
        )
    }

    fn decoded(json: serde_json::Value) -> Option<DecodedPayload> {
        Some(DecodedPayload::decoded(
            Bytes::from(json.to_string()),
            Some(7),
            json,
        ))
    }

    fn raw(text: &str) -> Option<DecodedPayload> {
        Some(DecodedPayload::raw(Bytes::from(text.to_owned())))
    }

    fn value_of(slot: &Option<DecodedPayload>) -> Option<serde_json::Value> {
        slot.as_ref().and_then(DecodedPayload::json).cloned()
    }

    fn apply(obfuscator: &TopicObfuscator, json: serde_json::Value) -> serde_json::Value {
        let mut slot = decoded(json);
        obfuscator.apply(Field::Value, &mut slot);
        value_of(&slot).expect("value survives")
    }

    #[test]
    fn each_strategy_rewrites_its_own_field_and_leaves_the_rest() {
        let obfuscator = payments().for_topic("payments.authorized").expect("rule");

        let masked = apply(
            &obfuscator,
            serde_json::json!({
                "card": {"number": "4111111111111111", "cvv": "123", "brand": "visa"},
                "customer": {"email": "ada@example.com", "id": 42},
                "amount": 9.5,
            }),
        );

        let number = masked["card"]["number"].as_str().expect("token");
        assert!(number.starts_with("kx:"), "{number}");
        assert_eq!(number.len(), 19);
        assert!(masked["card"].get("cvv").is_none(), "dropped fields vanish");
        assert_eq!(masked["card"]["brand"], "visa");
        assert_eq!(masked["customer"]["email"], "***");
        assert_eq!(masked["customer"]["id"], 42);
        assert_eq!(masked["amount"], 9.5);
    }

    #[test]
    fn the_same_value_always_hashes_to_the_same_token() {
        let obfuscator = payments().for_topic("payments.authorized").expect("rule");

        let first = apply(&obfuscator, serde_json::json!({"card": {"number": "4111"}}));
        let second = apply(&obfuscator, serde_json::json!({"card": {"number": "4111"}}));
        let other = apply(&obfuscator, serde_json::json!({"card": {"number": "4112"}}));

        assert_eq!(first["card"]["number"], second["card"]["number"]);
        assert_ne!(first["card"]["number"], other["card"]["number"]);
    }

    #[test]
    fn a_different_secret_gives_different_tokens() {
        let rules = |secret: &str| {
            policy(&format!(
                "
                secret: {secret}
                rules:
                  - topics: [payments]
                    fields:
                      - path: pan
                        strategy: hash
                "
            ))
            .for_topic("payments")
            .expect("rule")
        };

        let left = apply(
            &rules("0123456789abcdef0123456789abcdef"),
            serde_json::json!({"pan": "4111"}),
        );
        let right = apply(
            &rules("fedcba9876543210fedcba9876543210"),
            serde_json::json!({"pan": "4111"}),
        );

        assert_ne!(left["pan"], right["pan"]);
    }

    #[test]
    fn an_array_on_the_path_fans_out_over_its_elements() {
        let obfuscator = policy(
            "
            rules:
              - topics: [orders]
                fields:
                  - path: items.sku
                    strategy: mask
            ",
        )
        .for_topic("orders")
        .expect("rule");

        let masked = apply(
            &obfuscator,
            serde_json::json!({
                "items": [{"sku": "a", "qty": 1}, {"sku": "b", "qty": 2}],
            }),
        );

        assert_eq!(masked["items"][0]["sku"], "***");
        assert_eq!(masked["items"][1]["sku"], "***");
        assert_eq!(masked["items"][0]["qty"], 1);
    }

    #[test]
    fn a_root_array_is_walked_element_by_element() {
        let obfuscator = policy(
            "
            rules:
              - topics: [orders]
                fields:
                  - path: sku
                    strategy: mask
            ",
        )
        .for_topic("orders")
        .expect("rule");

        let masked = apply(&obfuscator, serde_json::json!([{"sku": "a"}, {"sku": "b"}]));

        assert_eq!(masked, serde_json::json!([{"sku": "***"}, {"sku": "***"}]));
    }

    #[test]
    fn numbers_and_booleans_become_token_strings() {
        let obfuscator = policy(
            "
            secret: 0123456789abcdef0123456789abcdef
            rules:
              - topics: [orders]
                fields:
                  - path: id
                    strategy: hash
                  - path: vip
                    strategy: mask
            ",
        )
        .for_topic("orders")
        .expect("rule");

        let masked = apply(&obfuscator, serde_json::json!({"id": 42, "vip": true}));

        assert!(masked["id"].as_str().expect("token").starts_with("kx:"));
        assert_eq!(masked["vip"], "***");
    }

    #[test]
    fn a_field_and_a_whole_field_rule_token_a_value_the_same_way() {
        let rules = |body: &str| {
            policy(&format!(
                "
                secret: 0123456789abcdef0123456789abcdef
                rules:
                  - topics: [orders]
                    {body}
                "
            ))
            .for_topic("orders")
            .expect("rule")
        };

        let field = apply(
            &rules("fields: [{path: email, strategy: hash}]"),
            serde_json::json!({"email": "ada@example.com"}),
        );
        let mut whole = raw("ada@example.com");
        rules("key: hash").apply(Field::Key, &mut whole);

        assert_eq!(field["email"], whole.expect("key").into_text().as_str());
    }

    #[test]
    fn a_missing_path_changes_nothing() {
        let obfuscator = payments().for_topic("payments.authorized").expect("rule");
        let original = serde_json::json!({"card": "4111", "note": null});

        assert_eq!(apply(&obfuscator, original.clone()), original);
    }

    #[test]
    fn whole_field_rules_replace_key_and_value_text() {
        let obfuscator = policy(
            "
            secret: 0123456789abcdef0123456789abcdef
            rules:
              - topics: ['audit.raw']
                key: mask
                value: hash
            ",
        )
        .for_topic("audit.raw")
        .expect("rule");

        let mut key = raw("ada@example.com");
        let mut value = raw("plain text body");
        obfuscator.apply(Field::Key, &mut key);
        obfuscator.apply(Field::Value, &mut value);

        assert_eq!(key.expect("key").into_text(), "***");
        let value = value.expect("value").into_text();
        assert!(value.starts_with("kx:"), "{value}");
    }

    #[test]
    fn dropping_a_whole_field_leaves_no_field_at_all() {
        let obfuscator = policy(
            "
            rules:
              - topics: ['audit.raw']
                value: drop
            ",
        )
        .for_topic("audit.raw")
        .expect("rule");

        let mut value = raw("secret body");
        obfuscator.apply(Field::Value, &mut value);

        assert!(value.is_none());
    }

    #[test]
    fn a_value_that_never_decoded_is_masked_when_the_topic_has_field_rules() {
        let obfuscator = payments().for_topic("payments.authorized").expect("rule");

        let mut value = raw(r#"{"card":{"number":"4111111111111111"}}"#);
        obfuscator.apply(Field::Value, &mut value);

        assert_eq!(value.expect("value").into_text(), "***");
    }

    #[test]
    fn unparsed_allow_serves_undecodable_values_as_they_are() {
        let obfuscator = policy(
            "
            rules:
              - topics: [payments]
                unparsed: allow
                fields:
                  - path: card.number
                    strategy: mask
            ",
        )
        .for_topic("payments")
        .expect("rule");

        let mut value = raw("unframed bytes");
        obfuscator.apply(Field::Value, &mut value);

        assert_eq!(value.expect("value").into_text(), "unframed bytes");
    }

    #[test]
    fn an_undecodable_key_is_left_alone_by_value_field_rules() {
        let obfuscator = payments().for_topic("payments.authorized").expect("rule");

        let mut key = raw("ord_1");
        obfuscator.apply(Field::Key, &mut key);

        assert_eq!(key.expect("key").into_text(), "ord_1");
    }

    #[test]
    fn field_rules_reach_a_decoded_key_too() {
        let obfuscator = payments().for_topic("payments.authorized").expect("rule");

        let mut key = decoded(serde_json::json!({"customer": {"email": "ada@example.com"}}));
        obfuscator.apply(Field::Key, &mut key);

        assert_eq!(value_of(&key).expect("key")["customer"]["email"], "***");
    }

    #[test]
    fn configured_headers_are_masked_by_name() {
        let obfuscator = policy(
            "
            rules:
              - topics: [orders]
                headers: ['x-user-id']
            ",
        )
        .for_topic("orders")
        .expect("rule");

        let mut headers = vec![
            (
                Bytes::from_static(b"x-user-id"),
                Some(Bytes::from_static(b"ada")),
            ),
            (
                Bytes::from_static(b"source"),
                Some(Bytes::from_static(b"checkout")),
            ),
        ];
        obfuscator.mask_headers(&mut headers);

        assert_eq!(headers[0].1.as_deref(), Some(b"***".as_slice()));
        assert_eq!(headers[1].1.as_deref(), Some(b"checkout".as_slice()));
    }

    #[test]
    fn a_header_only_rule_leaves_the_payload_filterable_on_raw_bytes() {
        let obfuscator = policy(
            "
            rules:
              - topics: [orders]
                headers: ['x-user-id']
            ",
        )
        .for_topic("orders")
        .expect("rule");

        assert!(!obfuscator.hides_payload());
        assert!(
            payments()
                .for_topic("payments.x")
                .expect("rule")
                .hides_payload()
        );
    }

    fn logs() -> ObfuscationPolicy {
        policy(
            r"
            secret: 0123456789abcdef0123456789abcdef
            rules:
              - topics: ['app.logs']
                patterns:
                  - regex: '\b\d{13,19}\b'
                    strategy: hash
                  - regex: '[\w.+-]+@[\w-]+\.[\w.]+'
                    strategy: mask
            ",
        )
    }

    #[test]
    fn patterns_rewrite_every_match_in_text_a_field_rule_could_never_reach() {
        let obfuscator = logs().for_topic("app.logs").expect("rule");

        let mut value = raw("charged 4111111111111111 for ada@example.com, retry 4111111111111111");
        obfuscator.apply(Field::Value, &mut value);
        let text = value.expect("value").into_text();

        assert!(!text.contains("4111111111111111"), "{text}");
        assert!(!text.contains("ada@example.com"), "{text}");
        assert_eq!(text.matches("kx:").count(), 2, "{text}");
        assert!(text.contains("***"), "{text}");
        assert!(
            text.starts_with("charged "),
            "the rest is untouched: {text}"
        );
    }

    #[test]
    fn a_pattern_hashes_equal_spans_to_equal_tokens() {
        let obfuscator = logs().for_topic("app.logs").expect("rule");

        let mut value = raw("pan 4111111111111111 then 4111111111111111 then 4222222222222222");
        obfuscator.apply(Field::Value, &mut value);
        let text = value.expect("value").into_text();

        let tokens: Vec<&str> = text
            .split_whitespace()
            .filter(|w| w.starts_with("kx:"))
            .collect();
        assert_eq!(tokens.len(), 3);
        assert_eq!(tokens[0], tokens[1], "the same span tokens the same way");
        assert_ne!(tokens[0], tokens[2]);
    }

    #[test]
    fn a_pattern_that_matches_nothing_leaves_the_payload_alone() {
        let obfuscator = logs().for_topic("app.logs").expect("rule");

        let mut value = decoded(serde_json::json!({"level": "warn", "took": 12}));
        obfuscator.apply(Field::Value, &mut value);

        assert_eq!(
            value_of(&value).expect("value"),
            serde_json::json!({"level": "warn", "took": 12}),
            "an untouched record keeps its tree, and its shape"
        );
    }

    #[test]
    fn patterns_reach_the_key_as_well_as_the_value() {
        let obfuscator = logs().for_topic("app.logs").expect("rule");

        let mut key = raw("user ada@example.com");
        obfuscator.apply(Field::Key, &mut key);

        assert_eq!(key.expect("key").into_text(), "user ***");
    }

    #[test]
    fn a_pattern_can_delete_what_it_matches() {
        let obfuscator = policy(
            r"
            rules:
              - topics: ['app.logs']
                patterns:
                  - regex: 'token=\S+'
                    strategy: drop
            ",
        )
        .for_topic("app.logs")
        .expect("rule");

        let mut value = raw("GET /orders token=abc123 200");
        obfuscator.apply(Field::Value, &mut value);

        assert_eq!(value.expect("value").into_text(), "GET /orders  200");
    }

    #[test]
    fn a_whole_field_rule_wins_over_the_patterns_beside_it() {
        let obfuscator = policy(
            r"
            rules:
              - topics: ['app.logs']
                value: mask
                patterns:
                  - regex: '\d+'
                    strategy: drop
            ",
        )
        .for_topic("app.logs")
        .expect("rule");

        let mut value = raw("charged 4111111111111111");
        obfuscator.apply(Field::Value, &mut value);

        assert_eq!(value.expect("value").into_text(), "***");
    }

    #[test]
    fn a_pattern_rule_takes_the_payload_off_the_raw_filter_path() {
        assert!(
            logs().for_topic("app.logs").expect("rule").hides_payload(),
            "a filter must not answer from bytes a pattern rewrites"
        );
    }

    #[test]
    fn a_pattern_that_does_not_compile_does_not_compile_the_policy() {
        let error = ObfuscationPolicy::compile(&config(
            "
            rules:
              - topics: ['app.logs']
                patterns:
                  - regex: '[unclosed'
                    strategy: mask
            ",
        ))
        .unwrap_err();

        assert!(matches!(error, ObfuscationError::InvalidPattern { .. }));
    }

    #[test]
    fn a_pattern_that_matches_everywhere_at_once_does_not_compile() {
        let error = ObfuscationPolicy::compile(&config(
            r"
            rules:
              - topics: ['app.logs']
                patterns:
                  - regex: '\d*'
                    strategy: mask
            ",
        ))
        .unwrap_err();

        assert_eq!(
            error.to_string(),
            r"invalid pattern '\d*': it matches the empty string"
        );
    }

    #[test]
    fn hashing_in_a_pattern_without_a_secret_does_not_compile() {
        let error = ObfuscationPolicy::compile(&config(
            r"
            rules:
              - topics: ['app.logs']
                patterns:
                  - regex: '\d{13,19}'
                    strategy: hash
            ",
        ))
        .unwrap_err();

        assert_eq!(error, ObfuscationError::MissingSecret);
    }

    #[test]
    fn topics_match_exactly_or_by_prefix_and_nothing_else() {
        let policy = policy(
            "
            rules:
              - topics: ['payments.*', 'audit.raw']
                value: mask
            ",
        );

        assert!(policy.for_topic("payments.authorized").is_some());
        assert!(policy.for_topic("payments.").is_some());
        assert!(policy.for_topic("audit.raw").is_some());
        assert!(policy.for_topic("audit.rawer").is_none());
        assert!(policy.for_topic("orders.created").is_none());
    }

    #[test]
    fn the_longest_matching_prefix_wins() {
        let policy = policy(
            "
            rules:
              - topics: ['payments.*']
                value: mask
              - topics: ['payments.eu.*']
                value: drop
            ",
        );

        let mut value = raw("body");
        policy
            .for_topic("payments.eu.cards")
            .expect("rule")
            .apply(Field::Value, &mut value);

        assert!(value.is_none(), "the eu rule, not the payments rule");
    }

    #[test]
    fn hashing_without_a_secret_does_not_compile() {
        let error = ObfuscationPolicy::compile(&config(
            "
            rules:
              - topics: [payments]
                fields:
                  - path: card.number
                    strategy: hash
            ",
        ))
        .unwrap_err();

        assert_eq!(error, ObfuscationError::MissingSecret);
    }

    #[test]
    fn empty_path_segments_do_not_compile() {
        let error = ObfuscationPolicy::compile(&config(
            "
            rules:
              - topics: [payments]
                fields:
                  - path: card..number
                    strategy: mask
            ",
        ))
        .unwrap_err();

        assert!(matches!(error, ObfuscationError::InvalidPath { .. }));
    }

    #[test]
    fn a_star_in_the_middle_of_a_topic_does_not_compile() {
        let error = ObfuscationPolicy::compile(&config(
            "
            rules:
              - topics: ['pay*ments']
                value: mask
            ",
        ))
        .unwrap_err();

        assert!(matches!(error, ObfuscationError::InvalidTopic { .. }));
    }

    #[test]
    fn a_base64_secret_and_its_raw_bytes_are_the_same_key() {
        let raw = "0123456789abcdef0123456789abcdef";
        let encoded =
            base64::Engine::encode(&base64::engine::general_purpose::STANDARD, raw.as_bytes());

        let token = |secret: &str| {
            let policy = policy(&format!(
                "
                secret: {secret}
                rules:
                  - topics: [payments]
                    value: hash
                "
            ));
            let mut value = Some(DecodedPayload::raw(Bytes::from_static(b"4111")));
            policy
                .for_topic("payments")
                .expect("rule")
                .apply(Field::Value, &mut value);
            value.expect("value").into_text()
        };

        assert_eq!(token(raw), token(&encoded));
        assert_eq!(token(raw), KeyedHasher::new(raw.as_bytes()).token("4111"));
    }
}
