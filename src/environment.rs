use std::sync::LazyLock;
use std::time::Duration;

use crate::r#macro::lazy_env_parse;

/// Path to the YAML config file (default: `config.yaml`).
///
/// Override with `KLENS_CONFIG_PATH`.
pub static CONFIG_PATH: LazyLock<String> =
    lazy_env_parse!("KLENS_CONFIG_PATH", String, || "config.yaml".to_owned());

/// Default log filter when `log_level` is omitted from the config file
/// (default: `info`).
///
/// Override with `KLENS_LOG_LEVEL`.
pub static LOG_LEVEL: LazyLock<String> =
    lazy_env_parse!("KLENS_LOG_LEVEL", String, || "info".to_owned());

/// OIDC scopes requested when the config does not set `auth.oidc.scopes`.
pub const DEFAULT_OIDC_SCOPES: &[&str] = &["openid", "email", "profile"];

/// Signed session cookie name used by axum-login / tower-sessions.
pub const SESSION_COOKIE: &str = "klens_session";

/// How long OIDC PKCE/CSRF state stays in the session (default: 10 minutes).
///
/// Override with `KLENS_LOGIN_MAX_AGE_SECS`.
pub static LOGIN_MAX_AGE_SECS: LazyLock<i64> =
    lazy_env_parse!("KLENS_LOGIN_MAX_AGE_SECS", i64, 10 * 60);

/// Upper bound on session lifetime, even if the ID token lasts longer
/// (default: 12 hours).
///
/// Override with `KLENS_MAX_SESSION_SECS`.
pub static MAX_SESSION_SECS: LazyLock<i64> =
    lazy_env_parse!("KLENS_MAX_SESSION_SECS", i64, 12 * 60 * 60);

/// Versioned prefix mixed into the session auth hash.
pub const SESSION_COOKIE_KEY_PREFIX: &str = "klens-session-v1";

/// Signing key for the session cookie, as base64 or raw text of at least
/// [`MIN_SESSION_KEY_BYTES`].
///
/// Takes precedence over `auth.session_key` in the config file. Without
/// either, a key is generated per boot and every deploy logs everyone out.
///
/// Override with `KLENS_SESSION_KEY`.
pub static SESSION_KEY: LazyLock<Option<String>> =
    LazyLock::new(|| std::env::var("KLENS_SESSION_KEY").ok());

/// Shortest accepted session signing key. `cookie::Key::derive_from` panics
/// below this.
pub const MIN_SESSION_KEY_BYTES: usize = 32;

/// Prefix for Kafka `client.id` values (`klens-<cluster>[-<role>]`).
pub const CLIENT_ID_PREFIX: &str = "klens";

/// Prefix for consumer groups created by klens itself.
pub const INTERNAL_GROUP_PREFIX: &str = "klens.internal.";

/// TCP connect timeout for the Kafka client (default: 10 seconds).
///
/// Override with `KLENS_SOCKET_CONNECTION_SETUP_TIMEOUT_MS`.
pub static SOCKET_CONNECTION_SETUP_TIMEOUT_MS: LazyLock<u32> =
    lazy_env_parse!("KLENS_SOCKET_CONNECTION_SETUP_TIMEOUT_MS", u32, 10_000);

/// Timeout for Schema Registry HTTP requests (default: 5 seconds).
///
/// Override with `KLENS_SCHEMA_REGISTRY_TIMEOUT` (seconds).
pub static SCHEMA_REGISTRY_TIMEOUT: LazyLock<Duration> = lazy_env_parse!(
    duration,
    "KLENS_SCHEMA_REGISTRY_TIMEOUT",
    Duration::from_secs(5)
);

/// Default per-request Kafka timeout (default: 10 seconds).
/// Cluster `properties.request_timeout_ms` takes precedence.
///
/// Override with `KLENS_REQUEST_TIMEOUT` (seconds).
pub static REQUEST_TIMEOUT: LazyLock<Duration> =
    lazy_env_parse!(duration, "KLENS_REQUEST_TIMEOUT", Duration::from_secs(10));

/// Timeout for topic-browser consume loops (default: 5 seconds).
///
/// Override with `KLENS_CONSUME_TIMEOUT` (seconds).
pub static CONSUME_TIMEOUT: LazyLock<Duration> =
    lazy_env_parse!(duration, "KLENS_CONSUME_TIMEOUT", Duration::from_secs(5));

/// Requests one broker connection may hold in flight (default: 32).
///
/// Override with `KLENS_MAX_IN_FLIGHT_REQUESTS`.
pub static MAX_IN_FLIGHT_REQUESTS: LazyLock<usize> =
    lazy_env_parse!("KLENS_MAX_IN_FLIGHT_REQUESTS", usize, 32);

/// Largest broker response frame the client will accept, in MiB
/// (default: 32).
///
/// Override with `KLENS_MAX_RESPONSE_MB`.
pub static MAX_RESPONSE_MB: LazyLock<usize> =
    lazy_env_parse!("KLENS_MAX_RESPONSE_MB", usize, 32 * 1024 * 1024);

/// Idle scan consumers kept per topic (default: 2).
///
/// Override with `KLENS_SCAN_POOL_PER_TOPIC`.
pub static SCAN_POOL_PER_TOPIC: LazyLock<usize> =
    lazy_env_parse!("KLENS_SCAN_POOL_PER_TOPIC", usize, 2);

/// Idle scan consumers kept across every topic (default: 16).
///
/// Override with `KLENS_SCAN_POOL_TOTAL`.
pub static SCAN_POOL_TOTAL: LazyLock<usize> = lazy_env_parse!("KLENS_SCAN_POOL_TOTAL", usize, 16);

/// How long an idle scan consumer stays poolable (default: 60 seconds).
///
/// Override with `KLENS_SCAN_POOL_IDLE_TTL` (seconds).
pub static SCAN_POOL_IDLE_TTL: LazyLock<Duration> = lazy_env_parse!(
    duration,
    "KLENS_SCAN_POOL_IDLE_TTL",
    Duration::from_secs(60)
);

/// How long one scan poll waits, and how long the broker may park the fetch
/// (default: 100 milliseconds).
///
/// Same duration on purpose: the broker must release when the scan moves on.
///
/// Override with `KLENS_SCAN_PACE_BOUND_MS`.
pub static SCAN_PACE_BOUND: LazyLock<Duration> = lazy_env_parse!(
    millis,
    "KLENS_SCAN_PACE_BOUND_MS",
    Duration::from_millis(100)
);

/// Maximum records a browse or search query may request (default: 500).
///
/// Override with `KLENS_MAX_RECORD_LIMIT`.
pub static MAX_RECORD_LIMIT: LazyLock<usize> =
    lazy_env_parse!("KLENS_MAX_RECORD_LIMIT", usize, 500);

/// How often the topology lane refreshes metadata and consumer-group
/// membership (default: 10 seconds).
///
/// Override with `KLENS_TOPOLOGY_LANE_INTERVAL` (seconds). Values below 1
/// second fall back to the default.
pub static TOPOLOGY_LANE_INTERVAL: LazyLock<Duration> = LazyLock::new(|| {
    parse_poll_interval(
        std::env::var("KLENS_TOPOLOGY_LANE_INTERVAL").ok(),
        DEFAULT_TOPOLOGY_LANE_INTERVAL,
    )
});

/// How often the watermark lane samples low and high watermarks, which also
/// sets the produce-rate resolution (default: 3 seconds).
///
/// Override with `KLENS_WATERMARK_LANE_INTERVAL` (seconds). Values below 1
/// second fall back to the default.
pub static WATERMARK_LANE_INTERVAL: LazyLock<Duration> = LazyLock::new(|| {
    parse_poll_interval(
        std::env::var("KLENS_WATERMARK_LANE_INTERVAL").ok(),
        DEFAULT_WATERMARK_LANE_INTERVAL,
    )
});

/// How often the config lane describes every topic config (default: 60
/// seconds).
///
/// Override with `KLENS_CONFIG_LANE_INTERVAL` (seconds). Values below 1
/// second fall back to the default.
pub static CONFIG_LANE_INTERVAL: LazyLock<Duration> = LazyLock::new(|| {
    parse_poll_interval(
        std::env::var("KLENS_CONFIG_LANE_INTERVAL").ok(),
        DEFAULT_CONFIG_LANE_INTERVAL,
    )
});

/// How often the subjects lane sweeps the Schema Registry listing (default:
/// 30 seconds).
///
/// Override with `KLENS_SUBJECT_LANE_INTERVAL` (seconds). Values below 1
/// second fall back to the default.
pub static SUBJECT_LANE_INTERVAL: LazyLock<Duration> = LazyLock::new(|| {
    parse_poll_interval(
        std::env::var("KLENS_SUBJECT_LANE_INTERVAL").ok(),
        DEFAULT_SUBJECT_LANE_INTERVAL,
    )
});

/// How often the offsets scheduler wakes to decide which consumer groups are
/// due for an offset fetch (default: 1 second).
///
/// Override with `KLENS_OFFSET_LANE_TICK` (seconds). Values below 1 second
/// fall back to the default.
pub static OFFSET_LANE_TICK: LazyLock<Duration> = LazyLock::new(|| {
    parse_poll_interval(
        std::env::var("KLENS_OFFSET_LANE_TICK").ok(),
        DEFAULT_OFFSET_LANE_TICK,
    )
});

/// Offset refresh cadence for groups someone is looking at (default: 2
/// seconds).
///
/// Override with `KLENS_FAST_OFFSET_INTERVAL` (seconds). Values below 1
/// second fall back to the default.
pub static FAST_OFFSET_INTERVAL: LazyLock<Duration> = LazyLock::new(|| {
    parse_poll_interval(
        std::env::var("KLENS_FAST_OFFSET_INTERVAL").ok(),
        DEFAULT_FAST_OFFSET_INTERVAL,
    )
});

/// Offset refresh cadence for groups nobody is looking at (default: 20
/// seconds).
///
/// Override with `KLENS_SLOW_OFFSET_INTERVAL` (seconds). Values below 1
/// second fall back to the default.
pub static SLOW_OFFSET_INTERVAL: LazyLock<Duration> = LazyLock::new(|| {
    parse_poll_interval(
        std::env::var("KLENS_SLOW_OFFSET_INTERVAL").ok(),
        DEFAULT_SLOW_OFFSET_INTERVAL,
    )
});

/// How long a one-shot query keeps a consumer group in the fast offset tier
/// (default: 30 seconds).
///
/// Override with `KLENS_INTEREST_TTL` (seconds).
pub static INTEREST_TTL: LazyLock<Duration> =
    lazy_env_parse!(duration, "KLENS_INTEREST_TTL", Duration::from_secs(30));

/// In-flight `OffsetFetch` requests per offsets-lane wave (default: 32).
///
/// Override with `KLENS_OFFSET_FETCH_CONCURRENCY`.
pub static OFFSET_FETCH_CONCURRENCY: LazyLock<usize> =
    lazy_env_parse!("KLENS_OFFSET_FETCH_CONCURRENCY", usize, 32);

/// In-flight per-subject Schema Registry loads per sweep (default: 8).
///
/// Override with `KLENS_SUBJECT_FETCH_CONCURRENCY`.
pub static SUBJECT_FETCH_CONCURRENCY: LazyLock<usize> =
    lazy_env_parse!("KLENS_SUBJECT_FETCH_CONCURRENCY", usize, 8);

/// How long a schema id the registry does not know stays cached as missing
/// (default: 60 seconds).
///
/// Resolved schemas are cached for the process lifetime because a registered
/// schema is immutable. A missing one is not: registering it later must not
/// leave every record rendering as raw bytes forever.
///
/// Override with `KLENS_MISSING_SCHEMA_TTL` (seconds).
pub static MISSING_SCHEMA_TTL: LazyLock<Duration> = lazy_env_parse!(
    duration,
    "KLENS_MISSING_SCHEMA_TTL",
    Duration::from_secs(60)
);

/// How long an idle cluster may go without a watermark tick before the lane
/// publishes a zero rate so an idle topic does not freeze (default: 15 seconds).
///
/// Override with `KLENS_IDLE_HEARTBEAT` (seconds).
pub static IDLE_HEARTBEAT: LazyLock<Duration> =
    lazy_env_parse!(duration, "KLENS_IDLE_HEARTBEAT", Duration::from_secs(15));

const DEFAULT_TOPOLOGY_LANE_INTERVAL: Duration = Duration::from_secs(10);
const DEFAULT_WATERMARK_LANE_INTERVAL: Duration = Duration::from_secs(3);
const DEFAULT_CONFIG_LANE_INTERVAL: Duration = Duration::from_secs(60);
const DEFAULT_SUBJECT_LANE_INTERVAL: Duration = Duration::from_secs(30);
const DEFAULT_OFFSET_LANE_TICK: Duration = Duration::from_secs(1);
const DEFAULT_FAST_OFFSET_INTERVAL: Duration = Duration::from_secs(2);
const DEFAULT_SLOW_OFFSET_INTERVAL: Duration = Duration::from_secs(20);
const MIN_POLL_INTERVAL: Duration = Duration::from_secs(1);

fn parse_poll_interval(raw: Option<String>, default: Duration) -> Duration {
    raw.and_then(|value| value.parse::<u64>().ok())
        .map(Duration::from_secs)
        .filter(|interval| *interval >= MIN_POLL_INTERVAL)
        .unwrap_or(default)
}

/// Ignore a previous watermark snapshot older than this when computing a
/// produce rate (default: 15 seconds).
///
/// Override with `KLENS_MAX_SAMPLE_GAP` (seconds).
pub static MAX_SAMPLE_GAP: LazyLock<Duration> =
    lazy_env_parse!(duration, "KLENS_MAX_SAMPLE_GAP", Duration::from_secs(15));

/// Extra partition-window multiplier when a record search is active
/// (default: 8).
///
/// Override with `KLENS_RECORD_SEARCH_WINDOW_MULTIPLIER`.
pub static RECORD_SEARCH_WINDOW_MULTIPLIER: LazyLock<usize> =
    lazy_env_parse!("KLENS_RECORD_SEARCH_WINDOW_MULTIPLIER", usize, 8);

/// Partition-window multiplier for ordinary record pages (default: 2).
///
/// Override with `KLENS_RECORD_WINDOW_MULTIPLIER`.
pub static RECORD_WINDOW_MULTIPLIER: LazyLock<usize> =
    lazy_env_parse!("KLENS_RECORD_WINDOW_MULTIPLIER", usize, 2);

/// Floor on the number of offsets read from each partition window
/// (default: 4).
///
/// Override with `KLENS_RECORD_MIN_WINDOW`.
pub static RECORD_MIN_WINDOW: LazyLock<usize> =
    lazy_env_parse!("KLENS_RECORD_MIN_WINDOW", usize, 4);

/// Cache-Control for hashed UI assets (1 year, immutable).
pub const STATIC_ASSET_CACHE_CONTROL: &str = "public, max-age=31536000, immutable";

/// Cache-Control for `index.html` so clients pick up new asset hashes.
pub const INDEX_CACHE_CONTROL: &str = "no-cache";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_poll_interval_defaults_when_unset_or_invalid() {
        assert_eq!(
            parse_poll_interval(None, DEFAULT_TOPOLOGY_LANE_INTERVAL),
            DEFAULT_TOPOLOGY_LANE_INTERVAL
        );
        assert_eq!(
            parse_poll_interval(Some("not-a-number".into()), DEFAULT_TOPOLOGY_LANE_INTERVAL),
            DEFAULT_TOPOLOGY_LANE_INTERVAL
        );
        assert_eq!(
            parse_poll_interval(Some("0".into()), DEFAULT_TOPOLOGY_LANE_INTERVAL),
            DEFAULT_TOPOLOGY_LANE_INTERVAL,
            "sub-second polling is a footgun, not a configuration"
        );
    }

    #[test]
    fn a_poll_interval_accepts_values_at_or_above_one_second() {
        assert_eq!(
            parse_poll_interval(Some("1".into()), DEFAULT_TOPOLOGY_LANE_INTERVAL),
            MIN_POLL_INTERVAL
        );
        assert_eq!(
            parse_poll_interval(Some("15".into()), DEFAULT_TOPOLOGY_LANE_INTERVAL),
            Duration::from_secs(15)
        );
    }
}
