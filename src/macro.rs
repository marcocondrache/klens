/// Lazily parse an environment variable, falling back to a default if the
/// variable is unset or the value cannot be parsed.
#[allow(unused_macro_rules)]
macro_rules! lazy_env_parse {
    ($key:expr, Option<String>) => {
        std::sync::LazyLock::new(|| std::env::var($key).ok())
    };
    ($key:expr, $t:ty) => {
        std::sync::LazyLock::new(|| {
            std::env::var($key)
                .ok()
                .and_then(|s| s.parse::<$t>().ok())
                .unwrap_or_default()
        })
    };
    ($key:expr, $t:ty, || $default:expr) => {
        std::sync::LazyLock::new(|| {
            std::env::var($key)
                .ok()
                .and_then(|s| s.parse::<$t>().ok())
                .unwrap_or_else(|| $default)
        })
    };
    ($key:expr, $t:ty, $default:expr) => {
        std::sync::LazyLock::new(|| {
            std::env::var($key)
                .ok()
                .and_then(|s| s.parse::<$t>().ok())
                .unwrap_or($default)
        })
    };
    (duration, $key:expr, $default:expr) => {
        std::sync::LazyLock::new(|| {
            std::env::var($key)
                .ok()
                .and_then(|s| s.parse::<u64>().ok())
                .map(std::time::Duration::from_secs)
                .unwrap_or($default)
        })
    };
}

pub(crate) use lazy_env_parse;
