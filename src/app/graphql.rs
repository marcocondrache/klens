use std::sync::Arc;
use std::time::Instant;

use axum::http::HeaderMap;
use axum::response::{IntoResponse, Response};
use axum::{
    Router,
    extract::{Extension, State},
    routing::post,
};
use juniper::{EmptyMutation, RootNode};
use juniper_axum::{extract::JuniperRequest, response::JuniperResponse};

use crate::AppState;
use crate::app::auth::SessionGuard;
use crate::app::auth::access::EffectiveAccess;
use crate::telemetry::{OperationId, complete};

mod context;
mod error;
mod query;
mod scalars;
mod sse;
mod subscription;
mod types;

use context::GraphQlContext;
use query::Query;
use subscription::Subscription;

pub type Schema = RootNode<Query, EmptyMutation<GraphQlContext>, Subscription>;

pub fn schema() -> Schema {
    Schema::new(Query, EmptyMutation::<GraphQlContext>::new(), Subscription)
}

pub fn router() -> Router<AppState> {
    let router = Router::new()
        .route("/graphql", post(graphql))
        .layer(Extension(Arc::new(schema())));

    #[cfg(debug_assertions)]
    let router = router.route(
        "/graphiql",
        axum::routing::get(juniper_axum::graphiql("/graphql", None)),
    );

    router
}

async fn graphql(
    headers: HeaderMap,
    Extension(schema): Extension<Arc<Schema>>,
    State(state): State<AppState>,
    Extension(access): Extension<EffectiveAccess>,
    Extension(guard): Extension<SessionGuard>,
    JuniperRequest(request): JuniperRequest,
) -> Response {
    if sse::wants_stream(&headers) {
        return sse::open(schema, state, access, guard, request);
    }

    let context = GraphQlContext {
        state,
        access,
        guard,
    };
    let identities = OperationId::from_each(request.operation_names());
    let started = Instant::now();
    let response = request.execute(&*schema, &context).await;
    JuniperResponse(complete(&identities, response, started.elapsed())).into_response()
}

#[cfg(test)]
mod tests;
