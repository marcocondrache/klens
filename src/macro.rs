/// Lazily parse an environment variable, falling back to a default if the
/// variable is unset or the value cannot be parsed.
macro_rules! lazy_env_parse {
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
    (millis, $key:expr, $default:expr) => {
        std::sync::LazyLock::new(|| {
            std::env::var($key)
                .ok()
                .and_then(|s| s.parse::<u64>().ok())
                .map(std::time::Duration::from_millis)
                .unwrap_or($default)
        })
    };
}

pub(crate) use lazy_env_parse;

/// Generate a `From` between two enums whose variants have the same names.
///
/// The variants are listed rather than inferred so the generated `match` stays
/// exhaustive: a new variant on the source enum fails to compile until it is
/// added here. Enums whose variant names differ (`ConfigSource`) stay
/// hand-written.
macro_rules! from_same_variants {
    ($src:ty => $dst:ty { $($variant:ident),+ $(,)? }) => {
        impl From<$src> for $dst {
            fn from(value: $src) -> Self {
                match value {
                    $( <$src>::$variant => Self::$variant, )+
                }
            }
        }
    };
}

pub(crate) use from_same_variants;
