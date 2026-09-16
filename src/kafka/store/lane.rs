use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use chrono::{DateTime, Utc};
use tokio::sync::Notify;

use crate::utils::utc_now;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LaneHealth {
    pub updated_at: Option<DateTime<Utc>>,
    pub last_error: Option<String>,
    pub last_poll_ms: Option<u64>,
}

pub struct Lane<T> {
    table: RwLock<Option<Arc<T>>>,
    version: AtomicU64,
    health: RwLock<LaneHealth>,
    kick: Notify,
}

impl<T> Lane<T> {
    pub fn new() -> Self {
        Self {
            table: RwLock::new(None),
            version: AtomicU64::new(0),
            health: RwLock::new(LaneHealth::default()),
            kick: Notify::new(),
        }
    }

    pub fn load(&self) -> Option<Arc<T>> {
        self.table.read().expect("lane lock").clone()
    }

    pub fn commit(&self, next: Arc<T>) -> u64 {
        *self.table.write().expect("lane lock") = Some(next);
        let version = self.version.fetch_add(1, Ordering::AcqRel) + 1;
        self.health.write().expect("lane health lock").updated_at = Some(utc_now());
        version
    }

    pub fn version(&self) -> u64 {
        self.version.load(Ordering::Acquire)
    }

    pub fn kick(&self) {
        self.kick.notify_one();
    }

    pub fn health(&self) -> LaneHealth {
        self.health.read().expect("lane health lock").clone()
    }

    pub fn record_health(&self, elapsed: Duration, error: Option<String>) {
        let mut health = self.health.write().expect("lane health lock");
        health.last_poll_ms = Some(elapsed.as_millis() as u64);
        health.last_error = error;
    }

    pub async fn wait(&self, interval: Duration) {
        tokio::select! {
            _ = tokio::time::sleep(interval) => {}
            _ = self.kick.notified() => {}
        }
    }
}

impl<T> Default for Lane<T> {
    fn default() -> Self {
        Self::new()
    }
}
