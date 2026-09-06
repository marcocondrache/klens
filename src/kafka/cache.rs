use std::time::{Duration, Instant};

use tokio::sync::RwLock;

use crate::kafka::model::{GroupSnapshot, MetadataSnapshot};

struct Entry<T> {
    value: T,
    fetched_at: Instant,
}

impl<T: Clone> Entry<T> {
    fn fresh(&self, ttl: Duration) -> Option<T> {
        (self.fetched_at.elapsed() < ttl).then(|| self.value.clone())
    }
}

/// Short-lived snapshots so list queries do not each hit Kafka.
pub struct MetadataCache {
    ttl: Duration,
    metadata: RwLock<Option<Entry<MetadataSnapshot>>>,
    groups: RwLock<Option<Entry<Vec<GroupSnapshot>>>>,
}

impl MetadataCache {
    pub fn new(ttl: Duration) -> Self {
        Self {
            ttl,
            metadata: RwLock::new(None),
            groups: RwLock::new(None),
        }
    }

    pub async fn metadata(&self) -> Option<MetadataSnapshot> {
        self.metadata
            .read()
            .await
            .as_ref()
            .and_then(|entry| entry.fresh(self.ttl))
    }

    pub async fn store_metadata(&self, value: MetadataSnapshot) {
        *self.metadata.write().await = Some(Entry {
            value,
            fetched_at: Instant::now(),
        });
    }

    pub async fn groups(&self) -> Option<Vec<GroupSnapshot>> {
        self.groups
            .read()
            .await
            .as_ref()
            .and_then(|entry| entry.fresh(self.ttl))
    }

    pub async fn store_groups(&self, value: Vec<GroupSnapshot>) {
        *self.groups.write().await = Some(Entry {
            value,
            fetched_at: Instant::now(),
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kafka::model::MetadataSnapshot;

    #[tokio::test]
    async fn expires_metadata_after_ttl() {
        let cache = MetadataCache::new(Duration::from_millis(20));
        cache
            .store_metadata(MetadataSnapshot {
                cluster_id: Some("id".into()),
                brokers: Vec::new(),
                topics: Vec::new(),
            })
            .await;

        assert!(cache.metadata().await.is_some());
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert!(cache.metadata().await.is_none());
    }
}
