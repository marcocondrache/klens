use std::collections::HashMap;
use std::sync::Arc;

use axum::Router;
use axum::middleware;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::app::auth::access::{
    AccessError, ClusterAccess, EffectiveAccess, Privilege, PrivilegeSet,
};
use crate::config::Config;
use crate::environment::MAX_LIVE_TAILS;
use crate::kafka::{Clusters, TailLimits};

mod acls;
pub(crate) mod auth;
mod brokers;
mod clusters;
mod configs;
mod context;
mod error;
mod extract;
mod group_offsets;
mod groups;
mod health;
mod int64;
mod paging;
mod records;
mod search;
mod subjects;
mod topics;
#[cfg(feature = "typescript")]
pub mod typescript;
mod updates;
mod whoami;
mod writes;

#[cfg(test)]
mod harness;

pub use auth::AuthState;

#[derive(Clone)]
pub struct AppState {
    clusters: Arc<Clusters>,
    auth: AuthState,
    limits: TailLimits,
    tails: Arc<Semaphore>,
    writes: Arc<HashMap<String, PrivilegeSet>>,
}

impl AppState {
    pub fn new(clusters: Clusters, auth: AuthState, limits: Limits) -> Self {
        Self {
            clusters: Arc::new(clusters),
            auth,
            limits: limits.tail,
            tails: Arc::new(Semaphore::new(limits.live_tails)),
            writes: Arc::default(),
        }
    }

    pub fn with_writes_from(self, config: &Config) -> Self {
        let writes: HashMap<String, PrivilegeSet> = config
            .clusters
            .iter()
            .filter(|cluster| !cluster.writes.is_empty())
            .map(|cluster| {
                let privileges = PrivilegeSet::from_privileges(
                    cluster.writes.iter().copied().map(Privilege::from),
                );
                (cluster.name.trim().to_owned(), privileges)
            })
            .collect();

        for (cluster, privileges) in &writes {
            let privileges: Vec<_> = privileges.iter().map(Privilege::name).collect();
            if self.auth.is_enabled() {
                tracing::info!(cluster, ?privileges, "cluster accepts writes");
            } else {
                tracing::warn!(
                    cluster,
                    ?privileges,
                    "cluster accepts writes from anyone who can reach klens: authentication is disabled"
                );
            }
        }

        Self {
            writes: Arc::new(writes),
            ..self
        }
    }

    #[cfg(test)]
    pub(crate) fn with_writes(self, cluster: &str, privileges: &[Privilege]) -> Self {
        let mut writes = (*self.writes).clone();
        writes.insert(
            cluster.to_owned(),
            PrivilegeSet::from_privileges(privileges.iter().copied()),
        );
        Self {
            writes: Arc::new(writes),
            ..self
        }
    }

    pub(crate) fn cluster_access<'a>(
        &self,
        access: &'a EffectiveAccess,
        cluster: &'a str,
    ) -> Result<ClusterAccess<'a>, AccessError> {
        let writes = self.writes.get(cluster).copied().unwrap_or_default();
        Ok(access
            .cluster(cluster)?
            .capped(PrivilegeSet::READS.union(writes.intersection(PrivilegeSet::WRITES))))
    }

    pub(crate) fn tail_permit(&self) -> Option<OwnedSemaphorePermit> {
        Arc::clone(&self.tails).try_acquire_owned().ok()
    }
}

/// How much one request may read, and how many live tails run at once.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Limits {
    /// Sizes live tails; its `records` also bounds one-shot record pages.
    pub tail: TailLimits,
    /// Live tails served at once, across every cluster.
    pub live_tails: usize,
}

impl Limits {
    pub fn from_env() -> Self {
        Self {
            tail: TailLimits::from_env(),
            live_tails: *MAX_LIVE_TAILS,
        }
    }
}

fn resources() -> Router<AppState> {
    Router::new()
        .merge(whoami::router())
        .nest("/clusters", clusters::router())
}

fn auth_routes() -> Router<AppState> {
    Router::new().nest("/auth", auth::router())
}

#[cfg(test)]
fn api() -> Router<AppState> {
    health::router()
        .merge(auth_routes())
        .merge(resources())
        .layer(middleware::from_fn(writes::reject_cross_site))
}

pub fn router(state: AppState) -> Router {
    let resources = resources().route_layer(middleware::from_fn_with_state(
        state.clone(),
        auth::require_session,
    ));
    let auth_layer = state.auth.layer();

    let api = auth_routes()
        .merge(resources)
        .layer(middleware::from_fn(writes::reject_cross_site));

    Router::new()
        .merge(health::router())
        .nest("/api", api)
        .with_state(state)
        .fallback(crate::server::web::serve)
        .layer(auth_layer)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{ClusterConfig, PrivilegeName};
    use crate::kafka::FakeCluster;

    fn cluster_config(name: &str, writes: Vec<PrivilegeName>) -> ClusterConfig {
        ClusterConfig {
            name: name.to_owned(),
            bootstrap_servers: vec!["localhost:9092".to_owned()],
            security: None,
            schema_registry: None,
            obfuscation: None,
            properties: Default::default(),
            ingest: Default::default(),
            writes,
        }
    }

    fn state(clusters: Vec<FakeCluster>) -> AppState {
        AppState::new(
            Clusters::from_sessions(clusters),
            AuthState::disabled(),
            Limits::from_env(),
        )
    }

    #[test]
    fn each_cluster_accepts_only_the_writes_its_config_lists() {
        let config = Config {
            bind: "127.0.0.1:8080".parse().expect("bind"),
            log_level: "info".to_owned(),
            clusters: vec![
                cluster_config(" local ", vec![PrivilegeName::ResetOffsets]),
                cluster_config("payments", Vec::new()),
            ],
            auth: None,
        };
        let state = state(vec![FakeCluster::local(), FakeCluster::named("payments")])
            .with_writes_from(&config);
        let access = EffectiveAccess::Unrestricted;

        let local = state.cluster_access(&access, "local").expect("local");
        let payments = state.cluster_access(&access, "payments").expect("payments");

        assert!(local.allows(Privilege::ResetOffsets));
        assert!(!local.allows(Privilege::DeleteGroupOffsets));
        assert!(local.allows(Privilege::Records), "reads are never capped");
        assert_eq!(
            payments.privileges(),
            PrivilegeSet::READS.iter().collect::<Vec<_>>()
        );
    }

    #[test]
    fn a_state_built_without_config_is_read_only() {
        let state = state(vec![FakeCluster::local()]);

        let local = state
            .cluster_access(&EffectiveAccess::Unrestricted, "local")
            .expect("local");

        assert_eq!(
            local.privileges(),
            PrivilegeSet::READS.iter().collect::<Vec<_>>()
        );
    }
}
