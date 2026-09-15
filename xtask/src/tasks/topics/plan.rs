use std::fmt;

const ADJECTIVES: [&str; 6] = ["amber", "brisk", "coral", "dusk", "ember", "flint"];
const NOUNS: [&str; 6] = ["river", "cedar", "quartz", "meadow", "harbor", "pine"];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct TopicCount(u16);

impl TopicCount {
    pub(super) const DEFAULT: Self = Self(20);
    pub(super) const MIN: u16 = 1;
    pub(super) const MAX: u16 = 256;

    pub(super) fn try_new(n: u16) -> Result<Self, PlanError> {
        if (Self::MIN..=Self::MAX).contains(&n) {
            Ok(Self(n))
        } else {
            Err(PlanError::CountOutOfRange { got: n })
        }
    }

    pub(super) fn parse_cli(raw: &str) -> Result<Self, PlanError> {
        let n = raw.parse::<u16>().map_err(|_| PlanError::InvalidCount {
            raw: raw.to_owned(),
        })?;
        Self::try_new(n)
    }

    pub(super) fn get(self) -> u16 {
        self.0
    }
}

impl Default for TopicCount {
    fn default() -> Self {
        Self::DEFAULT
    }
}

impl fmt::Display for TopicCount {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct Seed(u64);

impl Seed {
    pub(super) const LOCAL: Self = Self(1);
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct TopicName(String);

impl TopicName {
    pub(super) fn parse(raw: &str) -> Result<Self, PlanError> {
        if raw == "." || raw == ".." {
            return Err(PlanError::InvalidTopicName(raw.to_owned()));
        }
        if raw.is_empty() || raw.len() > 249 {
            return Err(PlanError::InvalidTopicName(raw.to_owned()));
        }
        if !raw
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'))
        {
            return Err(PlanError::InvalidTopicName(raw.to_owned()));
        }
        Ok(Self(raw.to_owned()))
    }

    pub(super) fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct TopicPlan {
    names: Vec<TopicName>,
}

impl TopicPlan {
    pub(super) fn generate(count: TopicCount, seed: Seed) -> Self {
        let names = (0..count.get())
            .map(|index| {
                let adj = ADJECTIVES[(mix(seed.0, index, 0) % 6) as usize];
                let noun = NOUNS[(mix(seed.0, index, 1) % 6) as usize];
                let tag = mix(seed.0, index, 2) as u16;
                let raw = format!("dev-{adj}-{noun}-{index:02x}{tag:04x}");
                TopicName::parse(&raw).expect("generated names are valid")
            })
            .collect();
        Self { names }
    }

    pub(super) fn names(&self) -> &[TopicName] {
        &self.names
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum PlanError {
    CountOutOfRange { got: u16 },
    InvalidCount { raw: String },
    InvalidTopicName(String),
}

impl fmt::Display for PlanError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CountOutOfRange { got } => {
                write!(f, "count {got} is outside 1..=256")
            }
            Self::InvalidCount { raw } => write!(f, "invalid count: {raw}"),
            Self::InvalidTopicName(name) => write!(f, "invalid topic name: {name}"),
        }
    }
}

impl std::error::Error for PlanError {}

fn mix(seed: u64, index: u16, lane: u8) -> u64 {
    let mut z = seed
        .wrapping_add(u64::from(index).wrapping_mul(0x9E37_79B9_7F4A_7C15))
        .wrapping_add(u64::from(lane).wrapping_mul(0xBF58_476D_1CE4_E5B9));
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generate_three_local_names() {
        let plan = TopicPlan::generate(TopicCount::try_new(3).unwrap(), Seed::LOCAL);
        assert_eq!(
            plan.names()
                .iter()
                .map(TopicName::as_str)
                .collect::<Vec<_>>(),
            [
                "dev-brisk-quartz-00b3cf",
                "dev-flint-pine-01e53c",
                "dev-brisk-pine-02726c",
            ]
        );
    }

    #[test]
    fn generate_fifty_starts_with_generate_twenty() {
        let twenty = TopicPlan::generate(TopicCount::try_new(20).unwrap(), Seed::LOCAL);
        let fifty = TopicPlan::generate(TopicCount::try_new(50).unwrap(), Seed::LOCAL);
        assert_eq!(&fifty.names()[..20], twenty.names());
    }

    #[test]
    fn topic_count_rejects_zero() {
        assert_eq!(
            TopicCount::try_new(0),
            Err(PlanError::CountOutOfRange { got: 0 })
        );
    }
}
