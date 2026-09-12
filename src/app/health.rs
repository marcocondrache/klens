use axum::extract::State;
use axum::{Router, http::StatusCode, routing::get};

use crate::AppState;

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/health", get(health))
        .route("/ready", get(ready))
}

async fn health() -> StatusCode {
    StatusCode::NO_CONTENT
}

async fn ready(State(state): State<AppState>) -> StatusCode {
    if state.is_ready() {
        StatusCode::NO_CONTENT
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    }
}

#[cfg(test)]
mod tests {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use std::sync::Arc;
    use tower::ServiceExt;

    use crate::AppState;
    use crate::app::router;
    use crate::kafka::model::CleanupPolicy;
    use crate::kafka::{ClusterSnapshot, FakeCluster, QueryEngine, Topic};

    fn app(state: AppState) -> axum::Router {
        router(state)
    }

    fn empty_topic(name: &str) -> Topic {
        Topic {
            name: name.to_owned(),
            internal: false,
            partitions: Vec::new(),
            replication_factor: 1,
            message_count: 0,
            cleanup_policy: CleanupPolicy::Delete,
            retention_ms: 0,
            consumer_groups: Vec::new(),
            under_replicated: false,
        }
    }

    #[tokio::test]
    async fn ready_is_unavailable_until_every_cluster_has_a_snapshot() {
        let state = AppState::new(Arc::new(QueryEngine::from_sessions(vec![
            FakeCluster::local(),
        ])));
        let response = app(state.clone())
            .oneshot(
                Request::builder()
                    .uri("/ready")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);

        state.catalog.store(
            "local",
            ClusterSnapshot::from_topics(vec![empty_topic("ready")]),
        );
        let response = app(state)
            .oneshot(
                Request::builder()
                    .uri("/ready")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
    }

    #[tokio::test]
    async fn health_stays_up_when_the_catalog_is_empty() {
        let state = AppState::new(Arc::new(QueryEngine::from_sessions(vec![
            FakeCluster::local(),
        ])));
        let response = app(state)
            .oneshot(
                Request::builder()
                    .uri("/health")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
    }
}
