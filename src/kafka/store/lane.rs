use std::sync::Arc;
use std::sync::RwLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use arc_swap::ArcSwapOption;
use chrono::{DateTime, Utc};
use tokio::sync::Notify;

use crate::utils::utc_now;

/// Freshness and failure state for one ingestion lane.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LaneHealth {
    /// When the lane last committed a table, i.e. when the data last moved.
    pub updated_at: Option<DateTime<Utc>>,
    /// When the lane last completed a fetch, whether or not it changed
    /// anything. A lane whose data is stable stays fresh here.
    pub checked_at: Option<DateTime<Utc>>,
    pub last_error: Option<String>,
    pub last_poll_ms: Option<u64>,
}

impl LaneHealth {
    pub fn healthy(&self) -> bool {
        self.last_error.is_none() && self.updated_at.is_some()
    }
}

/// An immutable table behind a swappable pointer.
pub struct Lane<T> {
    table: ArcSwapOption<T>,
    version: AtomicU64,
    health: RwLock<LaneHealth>,
    kick: Notify,
}

impl<T> Default for Lane<T> {
    fn default() -> Self {
        Self {
            table: ArcSwapOption::new(None),
            version: AtomicU64::new(0),
            health: RwLock::new(LaneHealth::default()),
            kick: Notify::new(),
        }
    }
}

impl<T> std::fmt::Debug for Lane<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Lane")
            .field("version", &self.version())
            .field("health", &self.health())
            .finish_non_exhaustive()
    }
}

impl<T> Lane<T> {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn load(&self) -> Option<Arc<T>> {
        self.table.load_full()
    }

    pub fn version(&self) -> u64 {
        self.version.load(Ordering::Acquire)
    }

    /// True once the lane has committed at least one table.
    pub fn ready(&self) -> bool {
        self.version() > 0
    }

    pub fn commit(&self, next: Arc<T>) -> u64 {
        self.table.store(Some(next));
        let version = self.version.fetch_add(1, Ordering::AcqRel) + 1;
        self.health.write().expect("lane health lock").updated_at = Some(utc_now());
        version
    }

    pub fn record_poll(&self, elapsed: Duration, error: Option<String>) {
        let mut health = self.health.write().expect("lane health lock");
        health.last_poll_ms = Some(elapsed.as_millis() as u64);
        if error.is_none() {
            health.checked_at = Some(utc_now());
        }
        health.last_error = error;
    }

    pub fn health(&self) -> LaneHealth {
        self.health.read().expect("lane health lock").clone()
    }

    /// Wake the lane runner before its interval elapses.
    pub fn kick(&self) {
        self.kick.notify_one();
    }

    /// Sleep for `interval`, returning early on a kick.
    pub async fn wait(&self, interval: Duration) {
        tokio::select! {
            () = tokio::time::sleep(interval) => {}
            () = self.kick.notified() => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_fresh_lane_serves_nothing_and_is_not_ready() {
        let lane = Lane::<u32>::new();
        assert!(lane.load().is_none());
        assert_eq!(lane.version(), 0);
        assert!(!lane.ready());
        assert_eq!(lane.health(), LaneHealth::default());
    }

    #[test]
    fn commit_swaps_the_pointer_and_bumps_the_version() {
        let lane = Lane::new();
        assert_eq!(lane.commit(Arc::new(1_u32)), 1);
        assert_eq!(*lane.load().unwrap(), 1);
        assert_eq!(lane.commit(Arc::new(2_u32)), 2);
        assert_eq!(*lane.load().unwrap(), 2);
        assert!(lane.ready());
        assert!(lane.health().updated_at.is_some());
    }

    #[test]
    fn a_failed_poll_keeps_serving_the_last_table() {
        let lane = Lane::new();
        lane.commit(Arc::new(7_u32));
        lane.record_poll(Duration::from_millis(3), None);
        let committed_at = lane.health().updated_at;

        lane.record_poll(Duration::from_millis(12), Some("broker down".into()));

        assert_eq!(*lane.load().unwrap(), 7, "stale data still serves");
        assert_eq!(lane.version(), 1, "a failure is not a commit");
        let health = lane.health();
        assert_eq!(health.updated_at, committed_at);
        assert_eq!(health.last_error.as_deref(), Some("broker down"));
        assert_eq!(health.last_poll_ms, Some(12));
        assert!(!health.healthy());
    }

    #[test]
    fn a_successful_poll_clears_the_previous_error() {
        let lane = Lane::new();
        lane.record_poll(Duration::from_millis(1), Some("boom".into()));
        lane.commit(Arc::new(1_u32));
        lane.record_poll(Duration::from_millis(1), None);

        let health = lane.health();
        assert!(health.last_error.is_none());
        assert!(health.checked_at.is_some());
        assert!(health.healthy());
    }

    #[tokio::test(start_paused = true)]
    async fn a_kick_cuts_the_wait_short() {
        let lane = Arc::new(Lane::<u32>::new());
        let waiter = Arc::clone(&lane);
        let handle = tokio::spawn(async move { waiter.wait(Duration::from_secs(600)).await });

        tokio::task::yield_now().await;
        lane.kick();

        tokio::time::timeout(Duration::from_secs(1), handle)
            .await
            .expect("kick must wake the runner")
            .expect("waiter task");
    }
}
