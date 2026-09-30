use std::sync::Arc;
use std::sync::RwLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use arc_swap::ArcSwapOption;
use jiff::Timestamp;
use tokio::sync::{Notify, watch};

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LaneHealth {
    pub updated_at: Option<Timestamp>,
    pub checked_at: Option<Timestamp>,
    pub last_error: Option<String>,
    pub last_poll_ms: Option<u64>,
}

impl LaneHealth {
    pub fn healthy(&self) -> bool {
        self.last_error.is_none() && self.updated_at.is_some()
    }
}

pub struct Lane<T> {
    table: ArcSwapOption<T>,
    version: AtomicU64,
    health: RwLock<LaneHealth>,
    kick: Notify,
    /// One runner polls at a time, so the nth poll to begin is the nth to
    /// finish.
    begun: AtomicU64,
    finished: watch::Sender<u64>,
}

impl<T> Default for Lane<T> {
    fn default() -> Self {
        Self {
            table: ArcSwapOption::new(None),
            version: AtomicU64::new(0),
            health: RwLock::new(LaneHealth::default()),
            kick: Notify::new(),
            begun: AtomicU64::new(0),
            finished: watch::Sender::new(0),
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

    pub fn ready(&self) -> bool {
        self.version() > 0
    }

    pub fn commit(&self, next: Arc<T>) -> u64 {
        self.table.store(Some(next));
        let version = self.version.fetch_add(1, Ordering::AcqRel) + 1;
        self.health.write().expect("lane health lock").updated_at = Some(Timestamp::now());
        version
    }

    /// Call before a poll reads the cluster.
    pub fn begin_poll(&self) {
        self.begun.fetch_add(1, Ordering::AcqRel);
    }

    /// Call after every poll, whether or not it committed.
    pub fn record_poll(&self, elapsed: Duration, error: Option<String>) {
        {
            let mut health = self.health.write().expect("lane health lock");
            health.last_poll_ms = Some(elapsed.as_millis() as u64);
            if error.is_none() {
                health.checked_at = Some(Timestamp::now());
            }
            health.last_error = error;
        }
        self.finished.send_modify(|finished| *finished += 1);
    }

    pub fn health(&self) -> LaneHealth {
        self.health.read().expect("lane health lock").clone()
    }

    pub fn kick(&self) {
        self.kick.notify_one();
    }

    /// Kicks the runner and waits for a poll that began after this call. A
    /// poll already under way when it is called may have read the cluster
    /// before a change the caller just made, so it does not count. Never
    /// returns if no runner drives the lane; callers bound the wait.
    pub async fn refresh(&self) {
        let mut finished = self.finished.subscribe();
        let target = self.begun.load(Ordering::Acquire) + 1;
        self.kick();
        // The sender lives as long as `self`, so the wait cannot fail.
        let _ = finished.wait_for(|finished| *finished >= target).await;
    }

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

    /// Polls the way a lane runner does: poll, then wait out the interval.
    /// Each poll holds until `release` lets it finish, and commits its number.
    fn runner(
        lane: &Arc<Lane<u32>>,
    ) -> (
        tokio::sync::mpsc::UnboundedReceiver<u32>,
        tokio::sync::mpsc::UnboundedSender<()>,
        tokio::task::JoinHandle<()>,
    ) {
        let (began, polls) = tokio::sync::mpsc::unbounded_channel();
        let (release, mut released) = tokio::sync::mpsc::unbounded_channel();
        let lane = Arc::clone(lane);
        let task = tokio::spawn(async move {
            let mut poll = 0;
            loop {
                poll += 1;
                lane.begin_poll();
                began.send(poll).expect("test holds the receiver");
                released.recv().await.expect("test holds the sender");
                lane.commit(Arc::new(poll));
                lane.record_poll(Duration::from_millis(1), None);
                lane.wait(Duration::from_secs(600)).await;
            }
        });
        (polls, release, task)
    }

    #[tokio::test(start_paused = true)]
    async fn a_refresh_wakes_an_idle_runner_and_waits_for_its_poll() {
        let lane = Arc::new(Lane::new());
        let (mut polls, release, task) = runner(&lane);
        assert_eq!(polls.recv().await, Some(1));
        release.send(()).unwrap();

        let refresh = tokio::spawn({
            let lane = Arc::clone(&lane);
            async move { lane.refresh().await }
        });
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), polls.recv()).await,
            Ok(Some(2)),
            "the refresh must cut the 600s interval short"
        );
        tokio::task::yield_now().await;
        assert!(!refresh.is_finished(), "poll 2 has not finished yet");

        release.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(1), refresh)
            .await
            .expect("the refresh ends with poll 2")
            .expect("refresh task");
        assert_eq!(*lane.load().unwrap(), 2);
        task.abort();
    }

    #[tokio::test(start_paused = true)]
    async fn a_refresh_does_not_settle_for_a_poll_already_under_way() {
        let lane = Arc::new(Lane::new());
        let (mut polls, release, task) = runner(&lane);
        assert_eq!(polls.recv().await, Some(1));

        let refresh = tokio::spawn({
            let lane = Arc::clone(&lane);
            async move { lane.refresh().await }
        });
        tokio::task::yield_now().await;
        release.send(()).unwrap();
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), polls.recv()).await,
            Ok(Some(2)),
            "the kick left a permit, so poll 2 begins without waiting"
        );
        assert_eq!(*lane.load().unwrap(), 1);
        assert!(
            !refresh.is_finished(),
            "poll 1 began before the refresh and may predate the change"
        );

        release.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(1), refresh)
            .await
            .expect("the refresh ends with poll 2")
            .expect("refresh task");
        assert_eq!(*lane.load().unwrap(), 2);
        task.abort();
    }

    #[tokio::test(start_paused = true)]
    async fn a_failed_poll_still_ends_a_refresh() {
        let lane = Arc::new(Lane::<u32>::new());
        let refresh = tokio::spawn({
            let lane = Arc::clone(&lane);
            async move { lane.refresh().await }
        });
        tokio::task::yield_now().await;

        lane.begin_poll();
        lane.record_poll(Duration::from_millis(1), Some("broker down".into()));

        tokio::time::timeout(Duration::from_secs(1), refresh)
            .await
            .expect("a failure is still a finished poll")
            .expect("refresh task");
        assert_eq!(lane.version(), 0);
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
