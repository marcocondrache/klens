use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use tower_sessions::cookie::time::OffsetDateTime;
use tower_sessions::session::{Id, Record};
use tower_sessions::session_store::{self, SessionStore};

const SWEEP_GROWTH: usize = 64;

/// `MemoryStore` only hides an expired record, so every abandoned login
/// would stay resident for the life of the process.
#[derive(Clone, Debug, Default)]
pub(crate) struct ExpiringStore(Arc<Mutex<Records>>);

#[derive(Debug, Default)]
struct Records {
    live: HashMap<Id, Record>,
    sweep_at: usize,
}

impl Records {
    fn create(&mut self, record: &mut Record, now: OffsetDateTime) {
        if self.live.len() >= self.sweep_at {
            self.live.retain(|_, record| record.expiry_date > now);
            self.sweep_at = self.live.len() + SWEEP_GROWTH;
        }
        while self.live.contains_key(&record.id) {
            record.id = Id::default();
        }
        self.live.insert(record.id, record.clone());
    }

    fn load(&mut self, id: &Id, now: OffsetDateTime) -> Option<Record> {
        match self.live.get(id) {
            Some(record) if record.expiry_date > now => Some(record.clone()),
            Some(_) => {
                self.live.remove(id);
                None
            }
            None => None,
        }
    }
}

impl ExpiringStore {
    fn records(&self) -> std::sync::MutexGuard<'_, Records> {
        self.0.lock().expect("session store lock")
    }
}

#[async_trait]
impl SessionStore for ExpiringStore {
    async fn create(&self, record: &mut Record) -> session_store::Result<()> {
        self.records().create(record, OffsetDateTime::now_utc());
        Ok(())
    }

    async fn save(&self, record: &Record) -> session_store::Result<()> {
        self.records().live.insert(record.id, record.clone());
        Ok(())
    }

    async fn load(&self, id: &Id) -> session_store::Result<Option<Record>> {
        Ok(self.records().load(id, OffsetDateTime::now_utc()))
    }

    async fn delete(&self, id: &Id) -> session_store::Result<()> {
        self.records().live.remove(id);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;
    use tower_sessions::cookie::time::Duration;

    fn now() -> OffsetDateTime {
        OffsetDateTime::UNIX_EPOCH + Duration::days(1)
    }

    fn record(expiry_date: OffsetDateTime) -> Record {
        Record {
            id: Id::default(),
            data: HashMap::default(),
            expiry_date,
        }
    }

    #[test]
    fn a_session_is_live_until_its_expiry_and_dropped_after() {
        let mut records = Records::default();
        let live = record(now() + Duration::seconds(1));
        let due = record(now());
        records.live.insert(live.id, live.clone());
        records.live.insert(due.id, due.clone());

        assert_eq!(records.load(&live.id, now()), Some(live.clone()));
        assert_eq!(records.load(&due.id, now()), None);
        assert_eq!(records.live.keys().collect::<Vec<_>>(), [&live.id]);
    }

    #[test]
    fn creating_sweeps_once_the_map_has_grown_by_the_threshold() {
        let mut records = Records::default();
        let mut first = record(now() + Duration::hours(1));
        records.create(&mut first, now());
        for _ in 0..SWEEP_GROWTH - 2 {
            let abandoned = record(now());
            records.live.insert(abandoned.id, abandoned);
        }

        let mut second = record(now() + Duration::hours(1));
        records.create(&mut second, now());
        assert_eq!(records.live.len(), SWEEP_GROWTH, "below the threshold");

        let mut third = record(now() + Duration::hours(1));
        records.create(&mut third, now());
        assert_eq!(
            records.live.keys().collect::<HashSet<_>>(),
            HashSet::from([&first.id, &second.id, &third.id])
        );
    }

    #[tokio::test]
    async fn the_store_serves_what_it_saved_until_deleted() {
        let store = ExpiringStore::default();
        let saved = record(OffsetDateTime::now_utc() + Duration::hours(1));
        let mut created = record(OffsetDateTime::now_utc() + Duration::hours(1));
        store.save(&saved).await.unwrap();
        store.create(&mut created).await.unwrap();

        assert_eq!(store.load(&saved.id).await.unwrap(), Some(saved.clone()));
        assert_eq!(store.load(&created.id).await.unwrap(), Some(created));

        store.delete(&saved.id).await.unwrap();
        assert_eq!(store.load(&saved.id).await.unwrap(), None);
    }
}
