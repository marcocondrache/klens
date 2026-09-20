use std::sync::Arc;
use std::time::Instant;

use axum::extract::WebSocketUpgrade;
use axum::response::Response;
use axum::{
    Router,
    extract::{Extension, State},
    routing::get,
};
use juniper::{EmptyMutation, RootNode};
use juniper_axum::subscriptions;
use juniper_axum::{extract::JuniperRequest, response::JuniperResponse};
use juniper_graphql_ws::ConnectionConfig;

use crate::AppState;
use crate::app::auth::SessionGuard;
use crate::app::auth::access::EffectiveAccess;
use crate::telemetry::{OperationId, complete, record_ws_upgrade};

mod context;
mod error;
mod query;
mod scalars;
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
        .route("/graphql", get(graphql_ws).post(graphql))
        .layer(Extension(Arc::new(schema())));

    #[cfg(debug_assertions)]
    let router = router.route(
        "/graphiql",
        get(juniper_axum::graphiql("/graphql", "/graphql")),
    );

    router
}

async fn graphql(
    Extension(schema): Extension<Arc<Schema>>,
    State(state): State<AppState>,
    Extension(access): Extension<EffectiveAccess>,
    Extension(guard): Extension<SessionGuard>,
    JuniperRequest(request): JuniperRequest,
) -> JuniperResponse {
    let context = GraphQlContext {
        state,
        access,
        guard,
    };
    let identities = OperationId::from_each(request.operation_names());
    let started = Instant::now();
    let response = request.execute(&*schema, &context).await;
    JuniperResponse(complete(&identities, response, started.elapsed()))
}

async fn graphql_ws(
    Extension(schema): Extension<Arc<Schema>>,
    State(state): State<AppState>,
    Extension(access): Extension<EffectiveAccess>,
    Extension(guard): Extension<SessionGuard>,
    ws: WebSocketUpgrade,
) -> Response {
    let context = GraphQlContext {
        state,
        access,
        guard,
    };
    record_ws_upgrade();
    ws.protocols(["graphql-transport-ws", "graphql-ws"])
        .on_upgrade(move |socket| {
            subscriptions::serve_ws(socket, schema, ConnectionConfig::new(context))
        })
}

#[cfg(test)]
mod tests;
