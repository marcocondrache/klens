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
