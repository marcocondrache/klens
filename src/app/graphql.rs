use std::sync::Arc;

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
use crate::app::auth::access::EffectiveAccess;

mod context;
mod error;
mod query;
mod subscription;
mod types;

use context::GraphQlContext;
use query::Query;
use subscription::Subscription;

pub(crate) use subscription::Samplers;

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
    JuniperRequest(request): JuniperRequest,
) -> JuniperResponse {
    let context = GraphQlContext { state, access };
    JuniperResponse(request.execute(&*schema, &context).await)
}

async fn graphql_ws(
    Extension(schema): Extension<Arc<Schema>>,
    State(state): State<AppState>,
    Extension(access): Extension<EffectiveAccess>,
    ws: WebSocketUpgrade,
) -> Response {
    let context = GraphQlContext { state, access };
    ws.protocols(["graphql-transport-ws", "graphql-ws"])
        .on_upgrade(move |socket| {
            subscriptions::serve_ws(socket, schema, ConnectionConfig::new(context))
        })
}

#[cfg(test)]
mod tests;
