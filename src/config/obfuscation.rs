//! What each topic's records hide from everyone who browses them.

use std::fmt::{self, Display, Formatter};

use regex::Regex;
use serde::de::Error as _;
use serde::{Deserialize, Deserializer};

use super::KeyMaterial;

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Obfuscation {
    /// Keys the `hash` strategy. Rotating it changes every token.
    pub secret: KeyMaterial,
    /// No topic may match two rules.
    #[serde(deserialize_with = "disjoint")]
    pub rules: Vec<Rule>,
}

/// What to hide in the records of the topics a rule names.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Rule {
    pub topics: Vec<TopicPattern>,
    /// Fields of a value a schema registry decoded.
    pub fields: Vec<Field>,
    /// Matches in the text of key and value, for topics without a schema.
    pub patterns: Vec<Pattern>,
    /// The whole key.
    pub key: Option<Strategy>,
    /// The whole value.
    pub value: Option<Strategy>,
    /// Names of headers whose values are masked.
    pub headers: Vec<String>,
    /// What `fields` do to a value that never decoded.
    pub unparsed: Unparsed,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Field {
    /// Dotted, like `card.number`. An array on the way applies the rest of
    /// the path to each element.
    pub path: String,
    pub strategy: Strategy,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Pattern {
    #[serde(deserialize_with = "regex")]
    pub regex: Regex,
    pub strategy: Strategy,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Strategy {
    /// Shown as `***`.
    Mask,
    /// Shown as a keyed token that equal values share.
    Hash,
    /// Not shown at all.
    Drop,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Unparsed {
    /// The whole value is masked.
    #[default]
    Mask,
    /// The value is shown as it came off the wire.
    Allow,
}

/// A topic name, or a name prefix when it ends in `*`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(try_from = "String")]
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

impl TryFrom<String> for TopicPattern {
    type Error = &'static str;

    fn try_from(pattern: String) -> Result<Self, Self::Error> {
        match pattern.strip_suffix('*') {
            Some(prefix) if !prefix.contains('*') => Ok(Self::Prefix(prefix.to_owned())),
            None if !pattern.contains('*') => Ok(Self::Exact(pattern)),
            _ => Err("'*' may only end a topic pattern"),
        }
    }
}

impl Display for TopicPattern {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Exact(name) => formatter.write_str(name),
            Self::Prefix(prefix) => write!(formatter, "{prefix}*"),
        }
    }
}

fn disjoint<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Vec<Rule>, D::Error> {
    let rules = Vec::<Rule>::deserialize(deserializer)?;
    let mut covered: Vec<&TopicPattern> = Vec::new();
    for topic in rules.iter().flat_map(|rule| &rule.topics) {
        if let Some(other) = covered.iter().find(|other| other.overlaps(topic)) {
            return Err(D::Error::custom(format!(
                "topics '{other}' and '{topic}' overlap"
            )));
        }
        covered.push(topic);
    }
    Ok(rules)
}

fn regex<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Regex, D::Error> {
    Regex::new(&String::deserialize(deserializer)?).map_err(D::Error::custom)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::parse;

    const SECRET: &str = "secret: {value: 0123456789abcdef0123456789abcdef}";

    fn obfuscation(rules: &str) -> anyhow::Result<Obfuscation> {
        parse(&format!("{SECRET}\nrules:\n{rules}"))
    }

    fn topics(rules: &str) -> Vec<String> {
        obfuscation(rules)
            .unwrap()
            .rules
            .iter()
            .flat_map(|rule| &rule.topics)
            .map(ToString::to_string)
            .collect()
    }

    #[test]
    fn reads_every_kind_of_rule() {
        let config = obfuscation(
            "
- topics: [payments.*]
  fields:
    - {path: card.number, strategy: hash}
    - {path: card.cvv, strategy: drop}
  unparsed: allow
- topics: [audit.raw]
  key: mask
  value: hash
  headers: [x-user-id]
- topics: [app.logs]
  patterns:
    - {regex: '\\b\\d{13,19}\\b', strategy: mask}
",
        )
        .unwrap();

        assert_eq!(
            config.secret.as_bytes(),
            b"0123456789abcdef0123456789abcdef"
        );
        let [fields, whole, patterns] = &config.rules[..] else {
            panic!("three rules: {:?}", config.rules);
        };

        assert_eq!(fields.topics, [TopicPattern::Prefix("payments.".into())]);
        assert_eq!(fields.fields[0].path, "card.number");
        assert_eq!(fields.fields[0].strategy, Strategy::Hash);
        assert_eq!(fields.fields[1].strategy, Strategy::Drop);
        assert_eq!(fields.unparsed, Unparsed::Allow);

        assert_eq!(whole.topics, [TopicPattern::Exact("audit.raw".into())]);
        assert_eq!(whole.key, Some(Strategy::Mask));
        assert_eq!(whole.value, Some(Strategy::Hash));
        assert_eq!(whole.headers, ["x-user-id"]);
        assert_eq!(whole.unparsed, Unparsed::Mask);

        assert!(patterns.patterns[0].regex.is_match("card 4111111111111111"));
        assert_eq!(patterns.patterns[0].strategy, Strategy::Mask);
    }

    #[test]
    fn obfuscation_needs_a_long_enough_secret() {
        let missing = parse::<Obfuscation>("rules: []").unwrap_err();
        let short = parse::<Obfuscation>("{secret: {value: short}, rules: []}").unwrap_err();

        assert!(
            missing.to_string().starts_with("missing field `secret`"),
            "{missing}"
        );
        assert!(
            short.to_string().starts_with("must be at least 32 bytes"),
            "{short}"
        );
    }

    #[test]
    fn a_star_may_only_end_a_topic_pattern() {
        assert_eq!(topics("- topics: ['*']"), ["*"]);
        assert_eq!(topics("- topics: [a.b, 'c*']"), ["a.b", "c*"]);

        for pattern in ["a*b", "*a", "a**"] {
            let error = obfuscation(&format!("- topics: ['{pattern}']")).unwrap_err();
            assert!(
                error
                    .to_string()
                    .starts_with("'*' may only end a topic pattern"),
                "{pattern}: {error}"
            );
        }
    }

    #[test]
    fn rejects_two_patterns_that_match_one_topic() {
        for (first, second) in [
            ("orders", "orders"),
            ("orders", "ord*"),
            ("ord*", "orders"),
            ("orders.*", "ord*"),
            ("ord*", "orders.*"),
        ] {
            let error =
                obfuscation(&format!("- topics: [{first}]\n- topics: ['{second}']")).unwrap_err();
            assert!(
                error
                    .to_string()
                    .starts_with(&format!("topics '{first}' and '{second}' overlap")),
                "{error}"
            );
        }
    }

    #[test]
    fn accepts_patterns_that_only_look_alike() {
        assert_eq!(
            topics("- topics: [orders, orders.v2]\n- topics: ['payments.*', 'pay.*']"),
            ["orders", "orders.v2", "payments.*", "pay.*"]
        );
    }

    #[test]
    fn rejects_a_regex_that_does_not_compile() {
        let error =
            obfuscation("- topics: [t]\n  patterns: [{regex: '(', strategy: mask}]").unwrap_err();

        assert!(
            error.to_string().starts_with("regex parse error"),
            "{error}"
        );
    }
}
