use std::collections::HashMap;
use std::sync::Arc;

use axum::Router;
use axum::middleware;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::app::auth::access::{
    AccessError, ClusterAccess, EffectiveAccess, Privilege, PrivilegeSet,
};
use crate::config::{ClusterIngestConfig, Config};
use crate::environment::MAX_LIVE_TAILS;
use crate::kafka::ingest::Ingest;
use crate::kafka::model::{AclListing, RegisteredSchema};
use crate::kafka::store::{ClusterStore, StoreSet};
use crate::kafka::{
    ConfigEntry, KafkaError, RecordLimits, RecordPage, RecordQuery, SessionSet, Tail, TailLimits,
    TailQuery, read_page,
};

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
    pub(crate) sessions: Arc<SessionSet>,
    pub(crate) stores: Arc<StoreSet>,
    pub(crate) auth: AuthState,
    limits: RecordLimits,
    tail_limits: TailLimits,
    tails: Arc<Semaphore>,
    /// Write privileges each cluster accepts. A cluster missing here is
    /// read-only.
    writes: Arc<HashMap<String, PrivilegeSet>>,
    _ingest: Option<Arc<Ingest>>,
}

impl AppState {
    pub fn new(sessions: Arc<SessionSet>) -> Self {
        Self::build(sessions, AuthState::disabled())
    }

    pub fn with_auth(sessions: Arc<SessionSet>, auth: AuthState) -> Self {
        Self::build(sessions, auth)
    }

    fn build(sessions: Arc<SessionSet>, auth: AuthState) -> Self {
        Self {
            stores: Arc::new(StoreSet::new(sessions.identities())),
            sessions,
            auth,
            limits: RecordLimits::from_env(),
            tail_limits: TailLimits::from_env(),
            tails: Arc::new(Semaphore::new(*MAX_LIVE_TAILS)),
            writes: Arc::default(),
            _ingest: None,
        }
    }

    /// Opens each cluster to the writes its config lists. Without this call
    /// every cluster stays read-only.
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

    #[cfg(test)]
    pub(crate) fn with_tail_capacity(self, tails: usize) -> Self {
        Self {
            tails: Arc::new(Semaphore::new(tails)),
            ..self
        }
    }

    pub fn with_ingest(self) -> Self {
        self.ingest_with(|_| ClusterIngestConfig::default())
    }

    pub fn with_ingest_from(self, config: &Config) -> Self {
        self.ingest_with(|name| {
            config
                .clusters
                .iter()
                .find(|cluster| cluster.name.trim() == name)
                .map(|cluster| cluster.ingest)
                .unwrap_or_default()
        })
    }

    fn ingest_with(self, ingest: impl Fn(&str) -> ClusterIngestConfig) -> Self {
        let clusters = self
            .sessions
            .sessions()
            .into_iter()
            .filter_map(|session| {
                let store = self.stores.get(&session.identity().name)?;
                let ingest = ingest(&session.identity().name);
                Some((Arc::clone(store), session, ingest))
            })
            .collect::<Vec<_>>();

        Self {
            _ingest: Some(Arc::new(Ingest::start(clusters))),
            ..self
        }
    }

    pub(crate) fn cluster(&self, name: &str) -> Result<&Arc<ClusterStore>, KafkaError> {
        self.stores.cluster(name)
    }

    /// What `access` may do on `cluster`, narrowed to the writes the cluster
    /// accepts. Every privilege check goes through here.
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

    pub(crate) fn is_ready(&self) -> bool {
        self.stores.ready()
    }

    pub(crate) async fn live_records(
        &self,
        cluster: &str,
        query: RecordQuery,
    ) -> Result<RecordPage, KafkaError> {
        read_page(
            self.sessions.session(cluster)?,
            self.cluster(cluster)?,
            query,
            self.limits,
        )
        .await
    }

    pub(crate) fn tail_permit(&self) -> Option<OwnedSemaphorePermit> {
        Arc::clone(&self.tails).try_acquire_owned().ok()
    }

    pub(crate) async fn live_tail(
        &self,
        cluster: &str,
        query: TailQuery,
    ) -> Result<Tail, KafkaError> {
        Tail::open(
            self.sessions.session(cluster)?,
            self.cluster(cluster)?,
            query,
            self.tail_limits,
        )
        .await
    }

    pub(crate) async fn live_broker_configs(
        &self,
        cluster: &str,
        id: i32,
    ) -> Result<Vec<ConfigEntry>, KafkaError> {
        let store = self.cluster(cluster)?;
        if let Some(topology) = store.topology.load()
            && !topology.brokers.contains_key(&id)
        {
            return Err(KafkaError::UnknownBroker {
                cluster: cluster.to_owned(),
                id,
            });
        }

        self.sessions.session(cluster)?.broker_configs(id).await
    }

    pub(crate) async fn live_acls(&self, cluster: &str) -> Result<AclListing, KafkaError> {
        self.sessions.session(cluster)?.acls().await
    }

    pub(crate) async fn live_subject_schema(
        &self,
        cluster: &str,
        subject: &str,
        version: i32,
    ) -> Result<RegisteredSchema, KafkaError> {
        self.sessions
            .session(cluster)?
            .subject_schema(subject, version)
            .await
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
    use crate::config::ClusterConfig;
    use crate::kafka::FakeCluster;

    async fn wait_until(predicate: impl Fn() -> bool) {
        for _ in 0..1_000 {
            if predicate() {
                return;
            }
            tokio::task::yield_now().await;
        }
        panic!("condition not met");
    }

    #[tokio::test]
    async fn ingestion_fills_the_store_the_api_projects_from() {
        let state = AppState::new(Arc::new(SessionSet::from_sessions(vec![
            FakeCluster::local(),
        ])))
        .with_ingest();

        wait_until(|| {
            state.is_ready() && !state.cluster("local").unwrap().subject_rows().is_empty()
        })
        .await;

        let store = state.cluster("local").unwrap();
        assert_eq!(store.topic_rows()[0].name.as_ref(), "orders.created");
        assert_eq!(
            store.subject_rows()[0].subject.as_ref(),
            "orders.created-value"
        );
    }

    #[tokio::test]
    async fn ingestion_never_describes_acls() {
        let session = FakeCluster::local();
        let state =
            AppState::new(Arc::new(SessionSet::from_sessions(vec![session.clone()]))).with_ingest();

        wait_until(|| state.is_ready()).await;

        assert!(
            session.calls().metadata() > 0,
            "topology lane never called metadata"
        );
        assert_eq!(session.calls().acls(), 0);
    }

    fn cluster_config(name: &str, writes: Vec<crate::config::PrivilegeName>) -> ClusterConfig {
        ClusterConfig {
            name: name.to_owned(),
            bootstrap_servers: vec!["localhost:9092".to_owned()],
            security: None,
            schema_registry: None,
            obfuscation: None,
            properties: Default::default(),
            ingest: ClusterIngestConfig::default(),
            writes,
        }
    }

    #[test]
    fn each_cluster_accepts_only_the_writes_its_config_lists() {
        use crate::config::PrivilegeName;

        let config = Config {
            bind: "127.0.0.1:8080".parse().expect("bind"),
            log_level: "info".to_owned(),
            clusters: vec![
                cluster_config(" local ", vec![PrivilegeName::ResetOffsets]),
                cluster_config("payments", Vec::new()),
            ],
            auth: None,
        };
        let state = AppState::new(Arc::new(SessionSet::from_sessions(vec![
            FakeCluster::local(),
            FakeCluster::named("payments"),
        ])))
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
        let state = AppState::new(Arc::new(SessionSet::from_sessions(vec![
            FakeCluster::local(),
        ])));

        let local = state
            .cluster_access(&EffectiveAccess::Unrestricted, "local")
            .expect("local");

        assert_eq!(
            local.privileges(),
            PrivilegeSet::READS.iter().collect::<Vec<_>>()
        );
    }

    #[tokio::test]
    async fn a_state_without_ingestion_never_becomes_ready() {
        let state = AppState::new(Arc::new(SessionSet::from_sessions(vec![
            FakeCluster::local(),
        ])));

        assert!(!state.is_ready());
        assert!(state.cluster("ghost").is_err());
    }
}
