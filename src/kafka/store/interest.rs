use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::time::Instant;

use crate::environment::INTEREST_TTL;

#[derive(Debug, Default)]
struct InterestState {
    leases: usize,
    last_touch: Option<Instant>,
}

pub struct InterestRegistry {
    groups: Mutex<HashMap<Arc<str>, InterestState>>,
    ttl: Duration,
}

impl Default for InterestRegistry {
    fn default() -> Self {
        Self::new(*INTEREST_TTL)
    }
}

impl InterestRegistry {
    pub fn new(ttl: Duration) -> Self {
        Self {
            groups: Mutex::new(HashMap::new()),
            ttl,
        }
    }

    pub fn lease_group(&self, id: &str) -> InterestLease<'_> {
        let key = {
            let mut groups = self.groups.lock().expect("interest lock");
            let key = groups
                .get_key_value(id)
                .map(|(key, _)| Arc::clone(key))
                .unwrap_or_else(|| Arc::from(id));
            groups.entry(Arc::clone(&key)).or_default().leases += 1;
            key
        };
        InterestLease {
            registry: self,
            id: key,
        }
    }

    pub fn touch_group(&self, id: &str) {
        let mut groups = self.groups.lock().expect("interest lock");
        let key = groups
            .get_key_value(id)
            .map(|(key, _)| Arc::clone(key))
            .unwrap_or_else(|| Arc::from(id));
        groups.entry(key).or_default().last_touch = Some(Instant::now());
    }

    pub fn hot_groups(&self) -> HashSet<Arc<str>> {
        let now = Instant::now();
        let mut groups = self.groups.lock().expect("interest lock");
        groups.retain(|_, state| {
            state.leases > 0
                || state
                    .last_touch
                    .is_some_and(|touched| now.saturating_duration_since(touched) < self.ttl)
        });
        groups
            .iter()
            .filter_map(|(id, state)| {
                let leased = state.leases > 0;
                let touched = state
                    .last_touch
                    .is_some_and(|touched| now.saturating_duration_since(touched) < self.ttl);
                (leased || touched).then(|| Arc::clone(id))
            })
            .collect()
    }

    fn release(&self, id: &str) {
        let mut groups = self.groups.lock().expect("interest lock");
        if let Some(state) = groups.get_mut(id) {
            state.leases = state.leases.saturating_sub(1);
            if state.leases == 0 && state.last_touch.is_none() {
                groups.remove(id);
            }
        }
    }
}

pub struct InterestLease<'a> {
    registry: &'a InterestRegistry,
    id: Arc<str>,
}

impl Drop for InterestLease<'_> {
    fn drop(&mut self) {
        self.registry.release(&self.id);
    }
}
