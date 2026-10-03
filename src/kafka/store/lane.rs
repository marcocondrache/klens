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
    commits: watch::Sender<()>,
    polls: watch::Sender<()>,
}

impl<T> Default for Lane<T> {
    fn default() -> Self {
        Self {
            table: ArcSwapOption::new(None),
            version: AtomicU64::new(0),
            health: RwLock::new(LaneHealth::default()),
            kick: Notify::new(),
            commits: watch::Sender::new(()),
            polls: watch::Sender::new(()),
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
        self.commits.send_replace(());
        version
    }

    pub fn record_poll(&self, elapsed: Duration, error: Option<String>) {
        let mut health = self.health.write().expect("lane health lock");
        health.last_poll_ms = Some(elapsed.as_millis() as u64);
        if error.is_none() {
            health.checked_at = Some(Timestamp::now());
        }
        health.last_error = error;
        drop(health);
        self.polls.send_replace(());
    }

    pub fn health(&self) -> LaneHealth {
        self.health.read().expect("lane health lock").clone()
    }

    pub fn kick(&self) {
        self.kick.notify_one();
    }

    pub async fn wait(&self, interval: Duration) {
        tokio::select! {
            () = tokio::time::sleep(interval) => {}
            () = self.kick.notified() => {}
        }
    }

    pub async fn refresh_until(&self, done: impl Fn(&T) -> bool) {
        let mut polls = self.polls.subscribe();
        while !self.load().is_some_and(|table| done(&table)) {
            self.kick();
            polls.changed().await.expect("a lane outlives its watchers");
            if self.health().last_error.is_some() {
                return;
            }
        }
    }

    pub fn follow(&self) -> Follower<'_, T> {
        Follower {
            lane: self,
            commits: self.commits.subscribe(),
        }
    }
}

pub struct Follower<'a, T> {
    lane: &'a Lane<T>,
    commits: watch::Receiver<()>,
}

impl<T> Follower<'_, T> {
    pub async fn table(&mut self) -> Arc<T> {
        loop {
            self.commits.mark_unchanged();
            if let Some(table) = self.lane.load() {
                return table;
            }
            self.commits
                .changed()
                .await
                .expect("a lane outlives its followers");
        }
    }

    pub async fn commit(&mut self) -> Arc<T> {
        self.commits
            .changed()
            .await
            .expect("a lane outlives its followers");
        self.table().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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

    const PATIENCE: Duration = Duration::from_secs(600);

    #[tokio::test(start_paused = true)]
    async fn a_follower_waits_for_the_first_commit() {
        let lane = Lane::new();
        let mut follower = lane.follow();
        assert!(
            tokio::time::timeout(PATIENCE, follower.table())
                .await
                .is_err(),
            "an empty lane has no table to follow"
        );

        lane.commit(Arc::new(1_u32));

        let table = tokio::time::timeout(PATIENCE, follower.table())
            .await
            .expect("the first commit must wake the follower");
        assert_eq!(*table, 1);
    }

    #[tokio::test(start_paused = true)]
    async fn a_follower_reports_only_commits_after_the_table_it_holds() {
        let lane = Lane::new();
        let mut follower = lane.follow();
        lane.commit(Arc::new(1_u32));

        assert_eq!(*follower.table().await, 1);
        assert!(
            tokio::time::timeout(PATIENCE, follower.commit())
                .await
                .is_err(),
            "the table already held is not a new commit"
        );

        lane.commit(Arc::new(2_u32));
        let table = tokio::time::timeout(PATIENCE, follower.commit())
            .await
            .expect("a new commit must wake the follower");
        assert_eq!(*table, 2);
    }

    fn runner(lane: &Arc<Lane<u32>>, polls: Vec<Result<u32, &'static str>>) {
        let lane = Arc::clone(lane);
        tokio::spawn(async move {
            for poll in polls {
                lane.wait(PATIENCE).await;
                match poll {
                    Ok(table) => {
                        lane.commit(Arc::new(table));
                        lane.record_poll(Duration::ZERO, None);
                    }
                    Err(error) => lane.record_poll(Duration::ZERO, Some(error.into())),
                }
            }
        });
    }

    #[tokio::test(start_paused = true)]
    async fn refresh_until_leaves_a_lane_alone_that_already_shows_the_change() {
        let lane = Lane::new();
        lane.commit(Arc::new(7_u32));

        tokio::time::timeout(PATIENCE, lane.refresh_until(|table| *table == 7))
            .await
            .expect("nothing to wait for");

        assert!(
            tokio::time::timeout(Duration::from_secs(1), lane.wait(PATIENCE))
                .await
                .is_err(),
            "no kick is pending"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn refresh_until_kicks_the_lane_until_a_poll_shows_the_change() {
        let lane = Arc::new(Lane::new());
        lane.commit(Arc::new(1_u32));
        runner(&lane, vec![Ok(1), Ok(2)]);
        let started = tokio::time::Instant::now();

        tokio::time::timeout(PATIENCE, lane.refresh_until(|table| *table == 2))
            .await
            .expect("each kick must start a poll");

        assert!(started.elapsed() < PATIENCE, "kicks, not the interval");
        assert_eq!(*lane.load().unwrap(), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn refresh_until_gives_up_after_a_failed_poll() {
        let lane = Arc::new(Lane::<u32>::new());
        runner(&lane, vec![Err("broker down")]);

        tokio::time::timeout(PATIENCE, lane.refresh_until(|table| *table == 2))
            .await
            .expect("a failed poll must end the wait");

        assert!(lane.load().is_none());
    }
}
