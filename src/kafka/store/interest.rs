use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use foldhash::{HashMap, HashSet};

use tokio::time::Instant;

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

#[derive(Debug, Clone, Default)]
struct Groups(Arc<Mutex<HashMap<Arc<str>, InterestState>>>);

impl Groups {
    // A panic mid-update can at worst leave a group hot until its lease ends,
    // so a poisoned guard still holds usable counts.
    fn lock(&self) -> MutexGuard<'_, HashMap<Arc<str>, InterestState>> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

#[derive(Debug)]
pub struct InterestRegistry {
    groups: Groups,
    ttl: Duration,
}

impl InterestRegistry {
    pub fn new(ttl: Duration) -> Self {
        Self {
            groups: Groups::default(),
            ttl,
        }
    }

    pub fn lease_group(&self, id: &str) -> InterestLease {
        let mut groups = self.groups.lock();
        let key = match groups.get_key_value(id) {
            Some((key, _)) => Arc::clone(key),
            None => Arc::from(id),
        };
        groups.entry(Arc::clone(&key)).or_default().leases += 1;
        drop(groups);

        InterestLease {
            groups: self.groups.clone(),
            group: key,
        }
    }

    pub fn touch_group(&self, id: &Arc<str>) {
        let now = Instant::now();
        let mut groups = self.groups.lock();
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
        let mut groups = self.groups.lock();
        groups.retain(|_, state| state.hot(now, self.ttl));
        groups.keys().cloned().collect()
    }

    #[cfg(test)]
    pub(crate) fn is_hot(&self, id: &str) -> bool {
        let now = Instant::now();
        self.groups
            .lock()
            .get(id)
            .is_some_and(|state| state.hot(now, self.ttl))
    }
}

#[derive(Debug)]
pub struct InterestLease {
    groups: Groups,
    group: Arc<str>,
}

impl Drop for InterestLease {
    fn drop(&mut self) {
        let mut groups = self.groups.lock();
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
    async fn a_lease_keeps_a_group_hot_until_it_is_dropped() {
        let interest = InterestRegistry::new(Duration::from_secs(30));
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
        let interest = InterestRegistry::new(Duration::from_secs(30));
        let first = interest.lease_group("billing");
        let second = interest.lease_group("billing");

        drop(first);
        assert!(interest.is_hot("billing"));

        drop(second);
        assert!(!interest.is_hot("billing"));
    }

    #[tokio::test(start_paused = true)]
    async fn a_touch_expires_after_the_ttl() {
        let interest = InterestRegistry::new(Duration::from_secs(30));
        interest.touch_group(&Arc::from("billing"));
        assert!(interest.is_hot("billing"));

        tokio::time::advance(Duration::from_secs(29)).await;
        assert!(interest.is_hot("billing"));

        tokio::time::advance(Duration::from_secs(2)).await;
        assert!(!interest.is_hot("billing"));
    }

    #[tokio::test(start_paused = true)]
    async fn expired_entries_do_not_accumulate() {
        let interest = InterestRegistry::new(Duration::from_secs(1));
        for index in 0..100 {
            interest.touch_group(&Arc::from(format!("group-{index}")));
        }
        tokio::time::advance(Duration::from_secs(2)).await;

        assert!(interest.hot_groups().is_empty());
        assert!(
            interest.groups.lock().is_empty(),
            "the sweep must reclaim expired entries"
        );
    }
}
