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
    use crate::kafka::store::fixtures::{partition, topic, topology};
    use crate::kafka::{FakeCluster, QueryEngine};

    fn state() -> AppState {
        AppState::new(Arc::new(QueryEngine::from_sessions(vec![
            FakeCluster::local(),
        ])))
    }

    async fn status(state: AppState, path: &str) -> StatusCode {
        router(state)
            .oneshot(Request::builder().uri(path).body(Body::empty()).unwrap())
            .await
            .unwrap()
            .status()
    }

    #[tokio::test]
    async fn ready_is_unavailable_until_topology_has_committed() {
        let state = state();
        assert_eq!(
            status(state.clone(), "/ready").await,
            StatusCode::SERVICE_UNAVAILABLE
        );

        state
            .cluster("local")
            .unwrap()
            .topology
            .commit(Arc::new(topology(
                vec![topic("ready", vec![partition(0, vec![1], vec![1])])],
                Vec::new(),
            )));

        assert_eq!(status(state, "/ready").await, StatusCode::NO_CONTENT);
    }

    #[tokio::test]
    async fn health_stays_up_while_the_store_is_still_empty() {
        assert_eq!(status(state(), "/health").await, StatusCode::NO_CONTENT);
    }
}
