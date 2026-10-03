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
    if state.clusters.ready() {
        StatusCode::NO_CONTENT
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    }
}

#[cfg(test)]
mod tests {
    use axum::http::StatusCode;

    use crate::testing::{FakeCluster, TestApp};

    #[tokio::test]
    async fn ready_is_unavailable_until_topology_has_committed() {
        let app = TestApp::of([FakeCluster::local()]).build();
        assert_eq!(
            app.get("/ready").await.status,
            StatusCode::SERVICE_UNAVAILABLE
        );

        app.ingest().await;

        assert_eq!(app.get("/ready").await.status, StatusCode::NO_CONTENT);
    }
}
