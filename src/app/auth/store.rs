use std::time::{Duration, Instant};

use async_trait::async_trait;
use moka::Expiry;
use moka::future::Cache;
use tower_sessions::cookie::time::OffsetDateTime;
use tower_sessions::session::{Id, Record};
use tower_sessions::session_store::{self, SessionStore};

/// `MemoryStore` only hides an expired record, so every abandoned login
/// would stay resident for the life of the process.
#[derive(Clone, Debug)]
pub(crate) struct ExpiringStore(Cache<Id, Record>);

struct UntilExpiry;

impl Expiry<Id, Record> for UntilExpiry {
    fn expire_after_create(&self, _: &Id, record: &Record, _: Instant) -> Option<Duration> {
        let left = record.expiry_date - OffsetDateTime::now_utc();
        Some(left.try_into().unwrap_or_default())
    }

    fn expire_after_update(
        &self,
        id: &Id,
        record: &Record,
        updated_at: Instant,
        _: Option<Duration>,
    ) -> Option<Duration> {
        self.expire_after_create(id, record, updated_at)
    }
}

impl Default for ExpiringStore {
    fn default() -> Self {
        Self(Cache::builder().expire_after(UntilExpiry).build())
    }
}

#[async_trait]
impl SessionStore for ExpiringStore {
    async fn create(&self, record: &mut Record) -> session_store::Result<()> {
        loop {
            let entry = self.0.entry(record.id).or_insert(record.clone()).await;
            if entry.is_fresh() {
                return Ok(());
            }
            record.id = Id::default();
        }
    }

    async fn save(&self, record: &Record) -> session_store::Result<()> {
        self.0.insert(record.id, record.clone()).await;
        Ok(())
    }

    async fn load(&self, id: &Id) -> session_store::Result<Option<Record>> {
        Ok(self.0.get(id).await)
    }

    async fn delete(&self, id: &Id) -> session_store::Result<()> {
        self.0.invalidate(id).await;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use tower_sessions::cookie::time::Duration;

    fn record(expiry_date: OffsetDateTime) -> Record {
        Record {
            id: Id::default(),
            data: HashMap::default(),
            expiry_date,
        }
    }

    fn live() -> Record {
        record(OffsetDateTime::now_utc() + Duration::hours(1))
    }

    fn due() -> Record {
        record(OffsetDateTime::now_utc() - Duration::seconds(1))
    }

    #[tokio::test]
    async fn the_store_serves_what_it_saved_until_deleted() {
        let store = ExpiringStore::default();
        let saved = live();
        let mut created = live();
        store.save(&saved).await.unwrap();
        store.create(&mut created).await.unwrap();

        assert_eq!(store.load(&saved.id).await.unwrap(), Some(saved.clone()));
        assert_eq!(store.load(&created.id).await.unwrap(), Some(created));

        store.delete(&saved.id).await.unwrap();
        assert_eq!(store.load(&saved.id).await.unwrap(), None);
    }

    #[tokio::test]
    async fn an_expired_session_is_dropped() {
        let store = ExpiringStore::default();
        let mut created = due();
        let saved = due();
        store.create(&mut created).await.unwrap();
        store.save(&saved).await.unwrap();

        assert_eq!(store.load(&created.id).await.unwrap(), None);
        assert_eq!(store.load(&saved.id).await.unwrap(), None);
    }

    #[tokio::test]
    async fn a_save_moves_the_expiry() {
        let store = ExpiringStore::default();
        let mut extended = live();
        let mut shortened = live();
        store.create(&mut extended).await.unwrap();
        store.create(&mut shortened).await.unwrap();

        extended.expiry_date += Duration::hours(1);
        shortened.expiry_date = due().expiry_date;
        store.save(&extended).await.unwrap();
        store.save(&shortened).await.unwrap();

        assert_eq!(store.load(&extended.id).await.unwrap(), Some(extended));
        assert_eq!(store.load(&shortened.id).await.unwrap(), None);
    }

    #[tokio::test]
    async fn create_never_overwrites_a_live_session() {
        let store = ExpiringStore::default();
        let existing = live();
        store.save(&existing).await.unwrap();

        let mut colliding = live();
        colliding.id = existing.id;
        store.create(&mut colliding).await.unwrap();

        assert_ne!(colliding.id, existing.id);
        assert_eq!(store.load(&existing.id).await.unwrap(), Some(existing));
        assert_eq!(store.load(&colliding.id).await.unwrap(), Some(colliding));
    }
}
