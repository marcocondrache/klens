use crate::config::IngestTuning;
use crate::testing::identity;

use super::ClusterStore;

impl ClusterStore {
    pub fn named(name: &str) -> Self {
        Self::new(identity(name), IngestTuning::default().interest_ttl)
    }
}
