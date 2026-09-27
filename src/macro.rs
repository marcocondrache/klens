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
