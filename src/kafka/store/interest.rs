use std::sync::{Arc, Mutex};
use std::time::Duration;

use foldhash::{HashMap, HashMapExt, HashSet};

use tokio::time::Instant;

use crate::environment::INTEREST_TTL;

#[derive(Debug, Default)]
struct InterestState {
    leases: usize,
    touched_at: Option<Instant>,
}

impl InterestState {
    fn hot(&self, now: Instant, ttl: Duration) -> bool {
        self.leases > 0
            || self
                .touched_at
                .is_some_and(|at| now.saturating_duration_since(at) < ttl)
    }

    fn idle(&self) -> bool {
        self.leases == 0 && self.touched_at.is_none()
    }
}

type Groups = Arc<Mutex<HashMap<Arc<str>, InterestState>>>;

#[derive(Debug, Clone)]
pub struct InterestRegistry {
    groups: Groups,
    ttl: Duration,
}

impl Default for InterestRegistry {
    fn default() -> Self {
        Self::with_ttl(*INTEREST_TTL)
    }
}

impl InterestRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_ttl(ttl: Duration) -> Self {
        Self {
            groups: Arc::new(Mutex::new(HashMap::new())),
            ttl,
        }
    }

    /// Held for a live subscription's lifetime. Dropping it releases the fast
    /// tier immediately, so a disconnected client stops costing broker calls.
    pub fn lease_group(&self, id: &str) -> InterestLease {
        let mut groups = self.groups.lock().expect("interest registry lock");
        let key = match groups.get_key_value(id) {
            Some((key, _)) => Arc::clone(key),
            None => Arc::from(id),
        };
        groups.entry(Arc::clone(&key)).or_default().leases += 1;
        drop(groups);

        InterestLease {
            groups: Arc::clone(&self.groups),
            group: key,
        }
    }

    /// Recorded by one-shot queries. Expires after the TTL, so opening a
    /// group page keeps it fresh for a while after the request finishes.
    pub fn touch_group(&self, id: &Arc<str>) {
        let now = Instant::now();
        let mut groups = self.groups.lock().expect("interest registry lock");
        match groups.get_mut(id) {
            Some(state) => state.touched_at = Some(now),
            None => {
                groups.insert(
                    Arc::clone(id),
                    InterestState {
                        touched_at: Some(now),
                        ..InterestState::default()
                    },
                );
            }
        }
    }

    pub fn hot_groups(&self) -> HashSet<Arc<str>> {
        let now = Instant::now();
        let mut groups = self.groups.lock().expect("interest registry lock");
        groups.retain(|_, state| state.leases > 0 || state.hot(now, self.ttl));
        groups
            .iter()
            .filter(|(_, state)| state.hot(now, self.ttl))
            .map(|(id, _)| Arc::clone(id))
            .collect()
    }

    pub fn is_hot(&self, id: &str) -> bool {
        let now = Instant::now();
        self.groups
            .lock()
            .expect("interest registry lock")
            .get(id)
            .is_some_and(|state| state.hot(now, self.ttl))
    }
}

/// Releases its group's fast-tier claim on drop.
#[derive(Debug)]
pub struct InterestLease {
    groups: Groups,
    group: Arc<str>,
}

impl InterestLease {
    pub fn group(&self) -> &Arc<str> {
        &self.group
    }
}

impl Drop for InterestLease {
    fn drop(&mut self) {
        let mut groups = self.groups.lock().expect("interest registry lock");
        let Some(state) = groups.get_mut(&self.group) else {
            return;
        };
        state.leases = state.leases.saturating_sub(1);
        if state.idle() {
            groups.remove(&self.group);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn nothing_is_hot_until_someone_looks() {
        let interest = InterestRegistry::new();
        assert!(interest.hot_groups().is_empty());
        assert!(!interest.is_hot("billing"));
    }

    #[tokio::test(start_paused = true)]
    async fn a_lease_keeps_a_group_hot_until_it_is_dropped() {
        let interest = InterestRegistry::with_ttl(Duration::from_secs(30));
        let lease = interest.lease_group("billing");

        tokio::time::advance(Duration::from_secs(3_600)).await;
        assert_eq!(
            interest.hot_groups(),
            HashSet::from_iter([Arc::from("billing")])
        );

        drop(lease);
        assert!(interest.hot_groups().is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn overlapping_leases_release_independently() {
        let interest = InterestRegistry::new();
        let first = interest.lease_group("billing");
        let second = interest.lease_group("billing");

        drop(first);
        assert!(interest.is_hot("billing"));

        drop(second);
        assert!(!interest.is_hot("billing"));
    }

    #[tokio::test(start_paused = true)]
    async fn a_touch_expires_after_the_ttl() {
        let interest = InterestRegistry::with_ttl(Duration::from_secs(30));
        interest.touch_group(&Arc::from("billing"));
        assert!(interest.is_hot("billing"));

        tokio::time::advance(Duration::from_secs(29)).await;
        assert!(interest.is_hot("billing"));

        tokio::time::advance(Duration::from_secs(2)).await;
        assert!(!interest.is_hot("billing"));
    }

    #[tokio::test(start_paused = true)]
    async fn expired_entries_do_not_accumulate() {
        let interest = InterestRegistry::with_ttl(Duration::from_secs(1));
        for index in 0..100 {
            interest.touch_group(&Arc::from(format!("group-{index}")));
        }
        tokio::time::advance(Duration::from_secs(2)).await;

        assert!(interest.hot_groups().is_empty());
        assert!(
            interest.groups.lock().unwrap().is_empty(),
            "the sweep must reclaim expired entries"
        );
    }
}
