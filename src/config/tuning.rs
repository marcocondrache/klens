use std::num::{NonZeroU32, NonZeroUsize};
use std::time::Duration;

use serde::Deserialize;

use super::{at_least_one_second, duration};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Tuning {
    pub kafka: KafkaTuning,
    pub schema_registry: SchemaRegistryTuning,
    pub scan: ScanTuning,
    pub records: RecordLimits,
    pub tail: TailTuning,
    pub ingest: IngestTuning,
    pub mcp: McpTuning,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct KafkaTuning {
    #[serde(deserialize_with = "duration")]
    pub connect_timeout: Duration,
    /// Raised to `connect_timeout` if smaller.
    #[serde(deserialize_with = "duration")]
    pub request_timeout: Duration,
    /// How long one record page or tail open may read.
    #[serde(deserialize_with = "duration")]
    pub consume_timeout: Duration,
    /// Per broker connection.
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
            max_in_flight_requests: nonzero(32),
            max_response_mib: nonzero(32),
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
            subject_fetch_concurrency: nonzero(8),
            missing_schema_ttl: Duration::from_secs(60),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ScanTuning {
    /// Idle scan consumers kept per topic.
    pub pool_per_topic: NonZeroUsize,
    /// Idle scan consumers kept across all topics.
    pub pool_total: NonZeroUsize,
    #[serde(deserialize_with = "at_least_one_second")]
    pub pool_idle_ttl: Duration,
    #[serde(deserialize_with = "duration")]
    pub poll_wait: Duration,
}

impl Default for ScanTuning {
    fn default() -> Self {
        Self {
            pool_per_topic: nonzero(2),
            pool_total: nonzero(16),
            pool_idle_ttl: Duration::from_secs(60),
            poll_wait: Duration::from_millis(100),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct RecordLimits {
    /// The most records one page may ask for.
    pub max_limit: NonZeroUsize,
    /// The fewest offsets read from each partition.
    pub min_window: usize,
    pub window_multiplier: usize,
    pub search_window_multiplier: usize,
}

impl Default for RecordLimits {
    fn default() -> Self {
        Self {
            max_limit: nonzero(500),
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
    /// The least time between frames.
    #[serde(deserialize_with = "duration")]
    pub interval: Duration,
    #[serde(deserialize_with = "duration")]
    pub poll_wait: Duration,
    /// Live tails served at once, across every cluster.
    pub max_live: usize,
}

impl Default for TailTuning {
    fn default() -> Self {
        Self {
            batch_limit: nonzero(100),
            interval: Duration::from_millis(250),
            poll_wait: Duration::from_millis(500),
            max_live: 32,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct IngestTuning {
    #[serde(deserialize_with = "at_least_one_second")]
    pub topology: Duration,
    #[serde(deserialize_with = "at_least_one_second")]
    pub high_watermark: Duration,
    /// How often the watermark lane rereads low watermarks.
    #[serde(deserialize_with = "at_least_one_second")]
    pub low_watermark: Duration,
    #[serde(deserialize_with = "at_least_one_second")]
    pub config: Duration,
    #[serde(deserialize_with = "at_least_one_second")]
    pub subjects: Duration,
    #[serde(deserialize_with = "at_least_one_second")]
    pub log_dirs: Duration,
    #[serde(deserialize_with = "at_least_one_second")]
    pub acls: Duration,
    #[serde(deserialize_with = "at_least_one_second")]
    pub quotas: Duration,
    #[serde(deserialize_with = "at_least_one_second")]
    pub scram_users: Duration,
    /// How often the offset lane wakes to see which groups are due.
    #[serde(deserialize_with = "at_least_one_second")]
    pub offset_tick: Duration,
    /// For groups someone is looking at.
    #[serde(deserialize_with = "at_least_one_second")]
    pub fast_offset: Duration,
    /// For every other group.
    #[serde(deserialize_with = "at_least_one_second")]
    pub slow_offset: Duration,
    pub offset_fetch_concurrency: NonZeroUsize,
    /// How long a viewed group stays on `fast_offset`.
    #[serde(deserialize_with = "duration")]
    pub interest_ttl: Duration,
    /// An idle topic's rate drops to zero after this.
    #[serde(deserialize_with = "duration")]
    pub idle_heartbeat: Duration,
    /// Older watermark samples do not count toward a rate.
    #[serde(deserialize_with = "duration")]
    pub max_sample_gap: Duration,
}

impl Default for IngestTuning {
    fn default() -> Self {
        Self {
            topology: Duration::from_secs(10),
            high_watermark: Duration::from_secs(3),
            low_watermark: Duration::from_secs(30),
            config: Duration::from_secs(60),
            subjects: Duration::from_secs(30),
            log_dirs: Duration::from_secs(60),
            acls: Duration::from_secs(60),
            quotas: Duration::from_secs(60),
            scram_users: Duration::from_secs(60),
            offset_tick: Duration::from_secs(1),
            fast_offset: Duration::from_secs(2),
            slow_offset: Duration::from_secs(20),
            offset_fetch_concurrency: nonzero(32),
            interest_ttl: Duration::from_secs(30),
            idle_heartbeat: Duration::from_secs(15),
            max_sample_gap: Duration::from_secs(15),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct McpTuning {
    /// MCP tool calls served at once, across every client. A call past it
    /// is refused rather than queued.
    pub max_concurrent_calls: NonZeroUsize,
    /// Calls a minute to the MCP tools that make klens read more from Kafka,
    /// such as klens_group_describe. A call past it is refused. With auth,
    /// each user has a budget of their own. Without it, every client shares
    /// one.
    pub live_calls_per_minute: NonZeroU32,
}

impl Default for McpTuning {
    fn default() -> Self {
        Self {
            max_concurrent_calls: nonzero(16),
            live_calls_per_minute: nonzero_u32(30),
        }
    }
}

const fn nonzero(count: usize) -> NonZeroUsize {
    NonZeroUsize::new(count).expect("a default count is not zero")
}

const fn nonzero_u32(count: u32) -> NonZeroU32 {
    NonZeroU32::new(count).expect("a default count is not zero")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::{yaml, yaml_err};

    fn secs(secs: u64) -> Duration {
        Duration::from_secs(secs)
    }

    fn millis(millis: u64) -> Duration {
        Duration::from_millis(millis)
    }

    #[test]
    fn omitted_keys_keep_their_defaults() {
        let defaults = Tuning {
            kafka: KafkaTuning {
                connect_timeout: secs(10),
                request_timeout: secs(10),
                consume_timeout: secs(5),
                max_in_flight_requests: nonzero(32),
                max_response_mib: nonzero(32),
            },
            schema_registry: SchemaRegistryTuning {
                timeout: secs(5),
                subject_fetch_concurrency: nonzero(8),
                missing_schema_ttl: secs(60),
            },
            scan: ScanTuning {
                pool_per_topic: nonzero(2),
                pool_total: nonzero(16),
                pool_idle_ttl: secs(60),
                poll_wait: millis(100),
            },
            records: RecordLimits {
                max_limit: nonzero(500),
                min_window: 4,
                window_multiplier: 2,
                search_window_multiplier: 8,
            },
            tail: TailTuning {
                batch_limit: nonzero(100),
                interval: millis(250),
                poll_wait: millis(500),
                max_live: 32,
            },
            ingest: IngestTuning {
                topology: secs(10),
                high_watermark: secs(3),
                low_watermark: secs(30),
                config: secs(60),
                subjects: secs(30),
                log_dirs: secs(60),
                acls: secs(60),
                quotas: secs(60),
                scram_users: secs(60),
                offset_tick: secs(1),
                fast_offset: secs(2),
                slow_offset: secs(20),
                offset_fetch_concurrency: nonzero(32),
                interest_ttl: secs(30),
                idle_heartbeat: secs(15),
                max_sample_gap: secs(15),
            },
            mcp: McpTuning {
                max_concurrent_calls: nonzero(16),
                live_calls_per_minute: nonzero_u32(30),
            },
        };

        assert_eq!(yaml::<Tuning>("{}"), defaults);
        assert_eq!(
            yaml::<Tuning>("tail: {interval: 10ms}"),
            Tuning {
                tail: TailTuning {
                    interval: millis(10),
                    ..defaults.tail
                },
                ..defaults
            }
        );
    }

    #[test]
    fn reads_every_key() {
        let tuning: Tuning = yaml(
            "
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
  min_window: 0
  window_multiplier: 3
  search_window_multiplier: 16
tail:
  batch_limit: 10
  interval: 1s
  poll_wait: 750ms
  max_live: 0
ingest:
  topology: 15s
  high_watermark: 1m 30s
  low_watermark: 2m 30s
  config: 2m
  subjects: 45s
  log_dirs: 5m
  acls: 3m
  quotas: 90s
  scram_users: 2m
  offset_tick: 2s
  fast_offset: 4s
  slow_offset: 40s
  offset_fetch_concurrency: 6
  interest_ttl: 1m
  idle_heartbeat: 20s
  max_sample_gap: 45s
mcp:
  max_concurrent_calls: 4
  live_calls_per_minute: 5
",
        );

        assert_eq!(
            tuning,
            Tuning {
                kafka: KafkaTuning {
                    connect_timeout: secs(3),
                    request_timeout: secs(20),
                    consume_timeout: secs(7),
                    max_in_flight_requests: nonzero(4),
                    max_response_mib: nonzero(64),
                },
                schema_registry: SchemaRegistryTuning {
                    timeout: secs(2),
                    subject_fetch_concurrency: nonzero(3),
                    missing_schema_ttl: secs(300),
                },
                scan: ScanTuning {
                    pool_per_topic: nonzero(1),
                    pool_total: nonzero(5),
                    pool_idle_ttl: secs(90),
                    poll_wait: millis(50),
                },
                records: RecordLimits {
                    max_limit: nonzero(1000),
                    min_window: 0,
                    window_multiplier: 3,
                    search_window_multiplier: 16,
                },
                tail: TailTuning {
                    batch_limit: nonzero(10),
                    interval: secs(1),
                    poll_wait: millis(750),
                    max_live: 0,
                },
                ingest: IngestTuning {
                    topology: secs(15),
                    high_watermark: secs(90),
                    low_watermark: secs(150),
                    config: secs(120),
                    subjects: secs(45),
                    log_dirs: secs(300),
                    acls: secs(180),
                    quotas: secs(90),
                    scram_users: secs(120),
                    offset_tick: secs(2),
                    fast_offset: secs(4),
                    slow_offset: secs(40),
                    offset_fetch_concurrency: nonzero(6),
                    interest_ttl: secs(60),
                    idle_heartbeat: secs(20),
                    max_sample_gap: secs(45),
                },
                mcp: McpTuning {
                    max_concurrent_calls: nonzero(4),
                    live_calls_per_minute: nonzero_u32(5),
                },
            }
        );
        assert_eq!(tuning.kafka.max_response_bytes(), 64 * 1024 * 1024);
    }

    #[test]
    fn a_polling_period_is_at_least_a_second() {
        let topology = |value: &str| format!("ingest: {{topology: {value}}}");

        assert_eq!(yaml::<Tuning>(&topology("1s")).ingest.topology, secs(1));
        assert_eq!(
            yaml_err::<Tuning>(&topology("999ms")),
            "must be at least 1s at line 1, column 20"
        );
    }
}
