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

/// Private cookie that holds the authenticated session.
pub const SESSION_COOKIE: &str = "klens_session";

/// Private cookie that holds PKCE/CSRF state during the OIDC redirect.
pub const LOGIN_COOKIE: &str = "klens_login";

/// How long the login cookie is valid (default: 10 minutes).
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

/// Versioned context mixed into the private-cookie key derivation.
pub const SESSION_COOKIE_KEY_PREFIX: &str = "klens-session-v1";

/// Minimum material length accepted by `cookie::Key::derive_from`.
pub const COOKIE_KEY_MIN_LEN: usize = 32;

/// Prefix for rdkafka `client.id` values (`klens-<cluster>[-<role>]`).
pub const CLIENT_ID_PREFIX: &str = "klens";

/// Prefix for consumer groups created by klens itself.
pub const INTERNAL_GROUP_PREFIX: &str = "klens.internal.";

/// Consumer group prefix used by the topic browser.
pub const BROWSE_GROUP_PREFIX: &str = "klens.internal.browse";

/// rdkafka `socket.connection.setup.timeout.ms` (default: 10 seconds).
///
/// Override with `KLENS_SOCKET_CONNECTION_SETUP_TIMEOUT_MS`.
pub static SOCKET_CONNECTION_SETUP_TIMEOUT_MS: LazyLock<u32> =
    lazy_env_parse!("KLENS_SOCKET_CONNECTION_SETUP_TIMEOUT_MS", u32, 10_000);

/// rdkafka `api.version.request.timeout.ms` (default: 10 seconds).
///
/// Override with `KLENS_API_VERSION_REQUEST_TIMEOUT_MS`.
pub static API_VERSION_REQUEST_TIMEOUT_MS: LazyLock<u32> =
    lazy_env_parse!("KLENS_API_VERSION_REQUEST_TIMEOUT_MS", u32, 10_000);

/// How long cluster metadata and group lists stay cached (default: 3 seconds).
///
/// Override with `KLENS_METADATA_TTL` (seconds).
pub static METADATA_TTL: LazyLock<Duration> =
    lazy_env_parse!(duration, "KLENS_METADATA_TTL", Duration::from_secs(3));

/// Timeout for Kafka metadata requests (default: 5 seconds).
///
/// Override with `KLENS_METADATA_TIMEOUT` (seconds).
pub static METADATA_TIMEOUT: LazyLock<Duration> =
    lazy_env_parse!(duration, "KLENS_METADATA_TIMEOUT", Duration::from_secs(5));

/// Timeout for watermark fetches (default: 3 seconds).
///
/// Override with `KLENS_WATERMARK_TIMEOUT` (seconds).
pub static WATERMARK_TIMEOUT: LazyLock<Duration> =
    lazy_env_parse!(duration, "KLENS_WATERMARK_TIMEOUT", Duration::from_secs(3));

/// Timeout for admin and offset-fetch calls (default: 10 seconds).
///
/// Override with `KLENS_ADMIN_TIMEOUT` (seconds).
pub static ADMIN_TIMEOUT: LazyLock<Duration> =
    lazy_env_parse!(duration, "KLENS_ADMIN_TIMEOUT", Duration::from_secs(10));

/// Timeout for topic-browser consume loops (default: 5 seconds).
///
/// Override with `KLENS_CONSUME_TIMEOUT` (seconds).
pub static CONSUME_TIMEOUT: LazyLock<Duration> =
    lazy_env_parse!(duration, "KLENS_CONSUME_TIMEOUT", Duration::from_secs(5));

/// Extra time granted to `spawn_blocking` Kafka calls beyond the request
/// timeout, so a slow broker does not also trip the Tokio join budget
/// (default: 2 seconds).
///
/// Override with `KLENS_BLOCKING_SLACK` (seconds).
pub static BLOCKING_SLACK: LazyLock<Duration> =
    lazy_env_parse!(duration, "KLENS_BLOCKING_SLACK", Duration::from_secs(2));

/// Overall budget for assembling a cluster overview (default: 20 seconds).
///
/// Override with `KLENS_OVERVIEW_BUDGET` (seconds).
pub static OVERVIEW_BUDGET: LazyLock<Duration> =
    lazy_env_parse!(duration, "KLENS_OVERVIEW_BUDGET", Duration::from_secs(20));

/// Partition watermark fetches issued per Kafka round-trip (default: 32).
///
/// Override with `KLENS_WATERMARK_BATCH`.
pub static WATERMARK_BATCH: LazyLock<usize> = lazy_env_parse!("KLENS_WATERMARK_BATCH", usize, 32);

/// Topic config fetches issued per Kafka round-trip (default: 20).
///
/// Override with `KLENS_CONFIG_BATCH`.
pub static CONFIG_BATCH: LazyLock<usize> = lazy_env_parse!("KLENS_CONFIG_BATCH", usize, 20);

/// Consumer groups whose committed offsets are fetched together (default: 8).
///
/// Override with `KLENS_OFFSET_FETCH_BATCH`.
pub static OFFSET_FETCH_BATCH: LazyLock<usize> =
    lazy_env_parse!("KLENS_OFFSET_FETCH_BATCH", usize, 8);

/// Skip per-partition watermark fetches on the topic list when a cluster
/// has more partitions than this (default: 128).
///
/// Override with `KLENS_LIST_WATERMARK_CAP`.
pub static LIST_WATERMARK_CAP: LazyLock<usize> =
    lazy_env_parse!("KLENS_LIST_WATERMARK_CAP", usize, 128);

/// Maximum records a browse or search query may request (default: 500).
///
/// Override with `KLENS_MAX_RECORD_LIMIT`.
pub static MAX_RECORD_LIMIT: LazyLock<usize> =
    lazy_env_parse!("KLENS_MAX_RECORD_LIMIT", usize, 500);

/// How often a live `topicRates` subscription samples high watermarks
/// (default: 2 seconds).
///
/// Override with `KLENS_SAMPLE_INTERVAL` (seconds).
pub static SAMPLE_INTERVAL: LazyLock<Duration> =
    lazy_env_parse!(duration, "KLENS_SAMPLE_INTERVAL", Duration::from_secs(2));

/// Ignore a previous watermark snapshot older than this when computing a
/// produce rate (default: 15 seconds).
///
/// Override with `KLENS_MAX_SAMPLE_GAP` (seconds).
pub static MAX_SAMPLE_GAP: LazyLock<Duration> =
    lazy_env_parse!(duration, "KLENS_MAX_SAMPLE_GAP", Duration::from_secs(15));

/// How many throughput points to keep per topic and for the cluster total
/// (default: 60).
///
/// Override with `KLENS_HISTORY_LEN`.
pub static HISTORY_LEN: LazyLock<usize> = lazy_env_parse!("KLENS_HISTORY_LEN", usize, 60);

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
