use std::num::NonZeroUsize;
use std::time::Duration;

use jiff::SignedDuration;
use serde::Deserialize;
use serde::de::{self, Deserializer};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Tuning {
    pub kafka: KafkaTuning,
    pub schema_registry: SchemaRegistryTuning,
    pub scan: ScanTuning,
    pub records: RecordLimits,
    pub tail: TailTuning,
    pub ingest: IngestTuning,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct KafkaTuning {
    #[serde(deserialize_with = "duration")]
    pub connect_timeout: Duration,
    #[serde(deserialize_with = "duration")]
    pub request_timeout: Duration,
    #[serde(deserialize_with = "duration")]
    pub consume_timeout: Duration,
    pub max_in_flight_requests: NonZeroUsize,
    pub max_response_mib: NonZeroUsize,
}

impl KafkaTuning {
    pub fn max_response_bytes(&self) -> usize {
        self.max_response_mib.get().saturating_mul(1024 * 1024)
    }
}

impl Default for KafkaTuning {
    fn default() -> Self {
        Self {
            connect_timeout: Duration::from_secs(10),
            request_timeout: Duration::from_secs(10),
            consume_timeout: Duration::from_secs(5),
            max_in_flight_requests: NonZeroUsize::new(32).unwrap(),
            max_response_mib: NonZeroUsize::new(32).unwrap(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct SchemaRegistryTuning {
    #[serde(deserialize_with = "duration")]
    pub timeout: Duration,
    pub subject_fetch_concurrency: NonZeroUsize,
    #[serde(deserialize_with = "duration")]
    pub missing_schema_ttl: Duration,
}

impl Default for SchemaRegistryTuning {
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(5),
            subject_fetch_concurrency: NonZeroUsize::new(8).unwrap(),
            missing_schema_ttl: Duration::from_secs(60),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ScanTuning {
    pub pool_per_topic: NonZeroUsize,
    pub pool_total: NonZeroUsize,
    #[serde(deserialize_with = "interval")]
    pub pool_idle_ttl: Duration,
    #[serde(deserialize_with = "duration")]
    pub poll_wait: Duration,
}

impl Default for ScanTuning {
    fn default() -> Self {
        Self {
            pool_per_topic: NonZeroUsize::new(2).unwrap(),
            pool_total: NonZeroUsize::new(16).unwrap(),
            pool_idle_ttl: Duration::from_secs(60),
            poll_wait: Duration::from_millis(100),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct RecordLimits {
    pub max_limit: NonZeroUsize,
    pub min_window: usize,
    pub window_multiplier: usize,
    pub search_window_multiplier: usize,
}

impl Default for RecordLimits {
    fn default() -> Self {
        Self {
            max_limit: NonZeroUsize::new(500).unwrap(),
            min_window: 4,
            window_multiplier: 2,
            search_window_multiplier: 8,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct TailTuning {
    pub batch_limit: NonZeroUsize,
    #[serde(deserialize_with = "duration")]
    pub interval: Duration,
    #[serde(deserialize_with = "duration")]
    pub poll_wait: Duration,
    pub max_live: usize,
}

impl Default for TailTuning {
    fn default() -> Self {
        Self {
            batch_limit: NonZeroUsize::new(100).unwrap(),
            interval: Duration::from_millis(250),
            poll_wait: Duration::from_millis(500),
            max_live: 32,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct IngestTuning {
    #[serde(deserialize_with = "duration")]
    pub interest_ttl: Duration,
    pub offset_fetch_concurrency: NonZeroUsize,
    #[serde(deserialize_with = "duration")]
    pub idle_heartbeat: Duration,
    #[serde(deserialize_with = "duration")]
    pub max_sample_gap: Duration,
}

impl Default for IngestTuning {
    fn default() -> Self {
        Self {
            interest_ttl: Duration::from_secs(30),
            offset_fetch_concurrency: NonZeroUsize::new(32).unwrap(),
            idle_heartbeat: Duration::from_secs(15),
            max_sample_gap: Duration::from_secs(15),
        }
    }
}

fn at_least<'de, D: Deserializer<'de>>(
    deserializer: D,
    min: Duration,
) -> Result<Duration, D::Error> {
    let text = String::deserialize(deserializer)?;
    let signed: SignedDuration = text.parse().map_err(de::Error::custom)?;
    let duration = Duration::try_from(signed)
        .map_err(|_| de::Error::custom(format!("must not be negative, got {text}")))?;
    if duration < min {
        return Err(de::Error::custom(format!(
            "must be at least {min:?}, got {text}"
        )));
    }
    Ok(duration)
}

pub(super) fn duration<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Duration, D::Error> {
    at_least(deserializer, Duration::ZERO)
}

pub(super) fn optional_duration<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<Duration>, D::Error> {
    #[derive(Deserialize)]
    struct ConfigDuration(#[serde(deserialize_with = "duration")] Duration);

    Option::<ConfigDuration>::deserialize(deserializer).map(|duration| duration.map(|d| d.0))
}

pub(super) fn interval<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Duration, D::Error> {
    at_least(deserializer, Duration::from_secs(1))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;

    fn nz(value: usize) -> NonZeroUsize {
        NonZeroUsize::new(value).unwrap()
    }

    fn parse(yaml: &str) -> Result<Config, String> {
        crate::config::from_yaml(&format!("bind: 127.0.0.1:8080\n{yaml}"))
            .map_err(|error| crate::config::describe(&error))
    }

    fn tuning(yaml: &str) -> Result<Tuning, String> {
        parse(yaml).map(|config| config.tuning)
    }

    #[test]
    fn a_duration_that_is_not_text_is_rejected_where_it_is() {
        let error = tuning("tuning:\n  tail:\n    interval: [1]\n").unwrap_err();

        assert_eq!(error, "expected string scalar at line 4, column 15");
    }

    #[test]
    fn parses_every_tuning_key() {
        let tuning = tuning(
            "
tuning:
  kafka:
    connect_timeout: 3s
    request_timeout: 20s
    consume_timeout: 7s
    max_in_flight_requests: 4
    max_response_mib: 64
  schema_registry:
    timeout: 2s
    subject_fetch_concurrency: 3
    missing_schema_ttl: 5m
  scan:
    pool_per_topic: 1
    pool_total: 5
    pool_idle_ttl: 90s
    poll_wait: 50ms
  records:
    max_limit: 1000
    window_multiplier: 3
    search_window_multiplier: 16
    min_window: 0
  tail:
    batch_limit: 10
    interval: 1s
    poll_wait: 750ms
    max_live: 0
  ingest:
    interest_ttl: 1m
    offset_fetch_concurrency: 6
    idle_heartbeat: 20s
    max_sample_gap: 45s
",
        )
        .unwrap();

        assert_eq!(
            tuning,
            Tuning {
                kafka: KafkaTuning {
                    connect_timeout: Duration::from_secs(3),
                    request_timeout: Duration::from_secs(20),
                    consume_timeout: Duration::from_secs(7),
                    max_in_flight_requests: nz(4),
                    max_response_mib: nz(64),
                },
                schema_registry: SchemaRegistryTuning {
                    timeout: Duration::from_secs(2),
                    subject_fetch_concurrency: nz(3),
                    missing_schema_ttl: Duration::from_secs(300),
                },
                scan: ScanTuning {
                    pool_per_topic: nz(1),
                    pool_total: nz(5),
                    pool_idle_ttl: Duration::from_secs(90),
                    poll_wait: Duration::from_millis(50),
                },
                records: RecordLimits {
                    max_limit: nz(1000),
                    min_window: 0,
                    window_multiplier: 3,
                    search_window_multiplier: 16,
                },
                tail: TailTuning {
                    batch_limit: nz(10),
                    interval: Duration::from_secs(1),
                    poll_wait: Duration::from_millis(750),
                    max_live: 0,
                },
                ingest: IngestTuning {
                    interest_ttl: Duration::from_secs(60),
                    offset_fetch_concurrency: nz(6),
                    idle_heartbeat: Duration::from_secs(20),
                    max_sample_gap: Duration::from_secs(45),
                },
            }
        );
        assert_eq!(tuning.kafka.max_response_bytes(), 64 * 1024 * 1024);
    }

    #[test]
    fn omitted_keys_keep_their_defaults() {
        let defaults = Tuning {
            kafka: KafkaTuning {
                connect_timeout: Duration::from_secs(10),
                request_timeout: Duration::from_secs(10),
                consume_timeout: Duration::from_secs(5),
                max_in_flight_requests: nz(32),
                max_response_mib: nz(32),
            },
            schema_registry: SchemaRegistryTuning {
                timeout: Duration::from_secs(5),
                subject_fetch_concurrency: nz(8),
                missing_schema_ttl: Duration::from_secs(60),
            },
            scan: ScanTuning {
                pool_per_topic: nz(2),
                pool_total: nz(16),
                pool_idle_ttl: Duration::from_secs(60),
                poll_wait: Duration::from_millis(100),
            },
            records: RecordLimits {
                max_limit: nz(500),
                min_window: 4,
                window_multiplier: 2,
                search_window_multiplier: 8,
            },
            tail: TailTuning {
                batch_limit: nz(100),
                interval: Duration::from_millis(250),
                poll_wait: Duration::from_millis(500),
                max_live: 32,
            },
            ingest: IngestTuning {
                interest_ttl: Duration::from_secs(30),
                offset_fetch_concurrency: nz(32),
                idle_heartbeat: Duration::from_secs(15),
                max_sample_gap: Duration::from_secs(15),
            },
        };

        assert_eq!(tuning("").unwrap(), defaults);
        assert_eq!(tuning("tuning: {}").unwrap(), defaults);
        assert_eq!(
            tuning("tuning: {tail: {interval: 10ms}}").unwrap(),
            Tuning {
                tail: TailTuning {
                    interval: Duration::from_millis(10),
                    ..defaults.tail
                },
                ..defaults
            }
        );
    }

    #[test]
    fn durations_accept_friendly_and_iso_forms() {
        let interval = |value: &str| {
            tuning(&format!("tuning: {{tail: {{interval: '{value}'}}}}"))
                .map(|tuning| tuning.tail.interval)
        };

        assert_eq!(interval("10ms"), Ok(Duration::from_millis(10)));
        assert_eq!(interval("1.5s"), Ok(Duration::from_millis(1500)));
        assert_eq!(interval("1h 30m"), Ok(Duration::from_secs(5400)));
        assert_eq!(interval("PT2S"), Ok(Duration::from_secs(2)));
        assert_eq!(interval("0s"), Ok(Duration::ZERO));
    }

    #[test]
    fn bad_values_name_the_field_and_line() {
        for (yaml, expected) in [
            (
                "tuning: {tail: {interval: -5s}}",
                "must not be negative, got -5s at line 2, column 27",
            ),
            (
                "tuning: {tail: {interval: 250}}",
                "failed to parse input in the \"friendly\" duration \
                 format: expected to find unit designator suffix (e.g., `years` or `secs`) \
                 after parsing integer at line 2, column 27",
            ),
            (
                "tuning: {tail: {batch_limit: 0}}",
                "invalid value: integer `0`, expected a nonzero usize \
                 at line 2, column 17",
            ),
            (
                "tuning: {scan: {pool_idle_ttl: 500ms}}",
                "must be at least 1s, got 500ms at line 2, column 32",
            ),
            (
                "tuning: {kafka: {request_timeout_ms: 5000}}",
                "unknown field `request_timeout_ms`, expected one of connect_timeout, \
                 request_timeout, consume_timeout, max_in_flight_requests, max_response_mib \
                 at line 2, column 18",
            ),
            (
                "tuning: {tails: {}}",
                "unknown field `tails`, expected one of kafka, schema_registry, scan, \
                 records, tail, ingest at line 2, column 10",
            ),
        ] {
            assert_eq!(tuning(yaml).unwrap_err(), expected, "{yaml}");
        }
    }

    #[test]
    fn auth_session_lifetimes_parse_as_durations() {
        let auth = |lifetimes: &str| {
            parse(&format!(
                "
auth:
  oidc:
    issuer: https://issuer.example.com
    client_id: klens
    client_secret: {{value: secret}}
    redirect_uri: https://klens.example.com/api/auth/callback
{lifetimes}"
            ))
            .map(|config| {
                let auth = config.auth.unwrap();
                (auth.login_max_age, auth.max_session)
            })
        };

        assert_eq!(
            auth(""),
            Ok((Duration::from_secs(600), Duration::from_secs(43_200)))
        );
        assert_eq!(
            auth("  login_max_age: 5m\n  max_session: 1h\n"),
            Ok((Duration::from_secs(300), Duration::from_secs(3_600)))
        );
        assert_eq!(
            auth("  max_session: soon\n").unwrap_err(),
            "failed to parse input in the \"friendly\" duration format: \
             expected duration to start with a unit value (a decimal integer) after an \
             optional sign, but no integer was found at line 9, column 16"
        );
    }
}
