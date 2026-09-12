use std::sync::Arc;
use std::time::Duration;

use crate::environment::{CONFIG_POLL_INTERVAL, OVERVIEW_BUDGET, SUBJECT_POLL_INTERVAL};
use crate::kafka::model::SchemaSubject;
use crate::kafka::{
    CatalogCache, CatalogHealth, CatalogPoller, CatalogPollerIntervals, CatalogPollerIo,
    CatalogRevision, ClusterIdentity, ClusterOverview, ClusterSession, ClusterSnapshot,
    ConfigEntry, ConsumerGroup, KafkaError, LagStore, QueryEngine, RateStore, RecordPage,
    RecordQuery, SearchHit, SubjectCache, ThroughputPoint, TopicRate,
};
use axum::Router;
use axum::middleware;
use futures::future::join_all;
use tokio::time::timeout;

mod auth;
mod graphql;
mod health;
mod sampler;

pub use auth::AuthState;

use graphql::Samplers;

pub(crate) struct CatalogSearch {
    pub hits: Vec<SearchHit>,
    pub schema_registry_error: Option<String>,
}

#[derive(Clone)]
pub struct AppState {
    pub(crate) query: Arc<QueryEngine<dyn ClusterSession>>,
    pub(crate) catalog: CatalogCache,
    pub(crate) subjects: SubjectCache,
    pub(crate) rates: RateStore,
    pub(crate) lags: LagStore,
    pub(crate) samplers: Arc<Samplers>,
    pub(crate) auth: AuthState,
    _poller: Option<Arc<CatalogPoller>>,
}

impl AppState {
    pub fn new(query: Arc<QueryEngine<dyn ClusterSession>>) -> Self {
        Self::build(query, AuthState::disabled())
    }

    pub fn with_auth(query: Arc<QueryEngine<dyn ClusterSession>>, auth: AuthState) -> Self {
        Self::build(query, auth)
    }

    fn build(query: Arc<QueryEngine<dyn ClusterSession>>, auth: AuthState) -> Self {
        Self {
            query,
            catalog: CatalogCache::new(),
            subjects: SubjectCache::new(),
            rates: RateStore::new(),
            lags: LagStore::new(),
            samplers: Arc::new(Samplers::default()),
            auth,
            _poller: None,
        }
    }

    pub fn with_catalog_poller(self, interval: Duration) -> Self {
        let query = Arc::clone(&self.query);
        let catalog_query = Arc::clone(&query);
        let subject_query = Arc::clone(&query);
        let rates = self.rates.clone();
        let poller = CatalogPoller::start(
            self.catalog.clone(),
            self.subjects.clone(),
            query.names().into_iter().map(str::to_owned),
            CatalogPollerIntervals {
                catalog: interval,
                subjects: *SUBJECT_POLL_INTERVAL,
                configs: *CONFIG_POLL_INTERVAL,
            },
            CatalogPollerIo {
                fetch_catalog: move |cluster: String, reuse, fetch_configs| {
                    let query = Arc::clone(&catalog_query);
                    async move {
                        query
                            .assemble_catalog(&cluster, Some(&reuse), fetch_configs)
                            .await
                    }
                },
                observe: move |cluster: &str, counts| rates.observe(cluster, counts),
                fetch_subjects: move |cluster: String| {
                    let query = Arc::clone(&subject_query);
                    async move { query.schema_subjects(&cluster).await }
                },
            },
        );
        Self {
            _poller: Some(Arc::new(poller)),
            ..self
        }
    }

    pub fn kick_catalog(&self, cluster: &str) {
        if let Some(poller) = &self._poller {
            poller.kick(cluster);
        }
    }

    pub fn invalidate_catalog(&self, cluster: &str) {
        self.catalog.invalidate(cluster);
        self.subjects.invalidate(cluster);
    }

    pub(crate) fn is_ready(&self) -> bool {
        self.query
            .names()
            .into_iter()
            .all(|name| self.catalog.snapshot(name).is_some())
    }

    pub(crate) fn catalog_health(&self, cluster: &str) -> CatalogHealth {
        let snapshot = self.catalog.snapshot(cluster);
        let subjects = self.subjects.snapshot(cluster);
        let catalog = self.catalog.poll_lane(cluster);
        let subjects_lane = self.subjects.poll_lane(cluster);
        CatalogHealth {
            cluster: cluster.to_owned(),
            updated_at: snapshot.as_ref().map(|snapshot| snapshot.updated_at),
            subjects_updated_at: subjects_lane.updated_at,
            last_error: catalog.last_error,
            last_poll_duration_ms: catalog.last_poll_duration_ms,
            topic_count: snapshot
                .as_ref()
                .map(|snapshot| snapshot.topics.len() as i32)
                .unwrap_or(0),
            group_count: snapshot
                .as_ref()
                .map(|snapshot| snapshot.groups.len() as i32)
                .unwrap_or(0),
            broker_count: snapshot
                .as_ref()
                .map(|snapshot| snapshot.brokers.len() as i32)
                .unwrap_or(0),
            subject_count: subjects
                .as_ref()
                .map(|subjects| subjects.len() as i32)
                .unwrap_or(0),
        }
    }

    pub(crate) fn require_cluster(&self, name: &str) -> Result<(), KafkaError> {
        self.query.session(name).map(|_| ())
    }

    pub(crate) fn catalog_updates(
        &self,
        cluster: &str,
    ) -> tokio::sync::watch::Receiver<Option<CatalogRevision>> {
        self.catalog.subscribe_updates(cluster)
    }

    pub(crate) async fn catalog_snapshot(
        &self,
        cluster: &str,
    ) -> Result<Arc<ClusterSnapshot>, KafkaError> {
        if let Some(snapshot) = self.catalog.snapshot(cluster) {
            return Ok(snapshot);
        }

        let snapshot = Arc::new(self.query.catalog(cluster).await?);
        self.catalog.seed(cluster, Arc::clone(&snapshot));
        Ok(snapshot)
    }

    pub(crate) async fn subject_snapshot(
        &self,
        cluster: &str,
    ) -> Result<Arc<Vec<SchemaSubject>>, KafkaError> {
        if let Some(subjects) = self.subjects.snapshot(cluster) {
            return Ok(subjects);
        }

        let subjects = Arc::new(self.live_schema_subjects(cluster).await?);
        self.subjects.seed(cluster, Arc::clone(&subjects));
        Ok(subjects)
    }

    pub(crate) async fn catalog_search(
        &self,
        cluster: &str,
        term: &str,
    ) -> Result<CatalogSearch, KafkaError> {
        let snapshot = self.catalog_snapshot(cluster).await?;
        let (subjects, schema_registry_error) = match self.subject_snapshot(cluster).await {
            Ok(subjects) => (subjects, None),
            Err(error) => {
                tracing::warn!(
                    cluster,
                    %error,
                    "schema registry unavailable during search"
                );
                (Arc::new(Vec::new()), Some(error.to_string()))
            }
        };
        Ok(CatalogSearch {
            hits: snapshot.search(term, subjects.as_ref()),
            schema_registry_error,
        })
    }

    pub(crate) async fn live_records(
        &self,
        cluster: &str,
        query: RecordQuery,
    ) -> Result<RecordPage, KafkaError> {
        self.query.records(cluster, query).await
    }

    pub(crate) async fn live_topic_configs(
        &self,
        cluster: &str,
        name: &str,
    ) -> Result<Vec<ConfigEntry>, KafkaError> {
        self.query.topic_configs(cluster, name).await
    }

    pub(crate) async fn live_broker_configs(
        &self,
        cluster: &str,
        id: i32,
    ) -> Result<Vec<ConfigEntry>, KafkaError> {
        self.query.broker_configs(cluster, id).await
    }

    pub(crate) async fn live_schema_subjects(
        &self,
        cluster: &str,
    ) -> Result<Vec<SchemaSubject>, KafkaError> {
        self.query.schema_subjects(cluster).await
    }

    pub(crate) async fn live_consumer_group(
        &self,
        cluster: &str,
        id: &str,
    ) -> Result<ConsumerGroup, KafkaError> {
        self.query.consumer_group(cluster, id).await
    }

    pub(crate) fn series_topic_rate(&self, cluster: &str, topic: &str) -> Option<TopicRate> {
        self.rates.topic_rate(cluster, topic)
    }

    pub(crate) fn series_topic_rates(&self, cluster: &str) -> Vec<TopicRate> {
        self.rates.topic_rates(cluster)
    }

    pub(crate) fn series_topic_history(&self, cluster: &str, topic: &str) -> Vec<ThroughputPoint> {
        self.rates.topic_history(cluster, topic)
    }

    pub(crate) fn series_group_lag_history(&self, cluster: &str, id: &str) -> Vec<ThroughputPoint> {
        self.lags.history(cluster, id)
    }

    pub(crate) fn series_observe_group_lag(&self, cluster: &str, id: &str, lag: i64) {
        self.lags.observe(cluster, id, lag);
    }

    pub(crate) async fn cluster_overviews(&self) -> Vec<ClusterOverview> {
        let identities = self.query.identities();
        join_all(
            identities
                .iter()
                .map(|identity| self.overview_or_offline(identity)),
        )
        .await
    }

    pub(crate) async fn cluster_overview(&self, name: &str) -> Option<ClusterOverview> {
        let identity = self.query.session(name).ok()?.identity().clone();
        Some(self.overview_or_offline(&identity).await)
    }

    async fn overview_or_offline(&self, identity: &ClusterIdentity) -> ClusterOverview {
        if let Some(snapshot) = self.catalog.snapshot(&identity.name) {
            return snapshot.overview.clone();
        }

        match timeout(*OVERVIEW_BUDGET, self.catalog_snapshot(&identity.name)).await {
            Ok(Ok(snapshot)) => snapshot.overview.clone(),
            Ok(Err(_)) | Err(_) => ClusterOverview::offline(identity.clone()),
        }
    }
}

pub use graphql::{Schema, schema};

pub fn router(state: AppState) -> Router {
    let graphql = graphql::router().route_layer(middleware::from_fn_with_state(
        state.clone(),
        auth::require_session,
    ));

    Router::new()
        .merge(graphql)
        .merge(auth::router())
        .merge(health::router())
        .with_state(state)
        .fallback(crate::server::web::serve)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kafka::FakeCluster;

    async fn wait_until(predicate: impl Fn() -> bool) {
        for _ in 0..200 {
            if predicate() {
                return;
            }
            tokio::task::yield_now().await;
        }
        panic!("condition not met");
    }

    #[tokio::test]
    async fn catalog_poller_fills_caches_and_observes_rates() {
        let state = AppState::new(Arc::new(QueryEngine::from_sessions(vec![
            FakeCluster::local(),
        ])))
        .with_catalog_poller(Duration::from_secs(60));

        wait_until(|| {
            state.catalog.snapshot("local").is_some()
                && state.subjects.snapshot("local").is_some()
                && state.series_topic_rate("local", "orders.created").is_some()
        })
        .await;

        assert_eq!(
            state.catalog.snapshot("local").unwrap().topics[0].name,
            "orders.created"
        );
        assert_eq!(
            state
                .series_topic_rate("local", "orders.created")
                .unwrap()
                .messages_per_sec,
            0.0
        );
        assert_eq!(
            state.subjects.snapshot("local").unwrap()[0].subject,
            "orders.created-value"
        );
    }
}
