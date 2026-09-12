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

mod query;
mod subscription;
mod types;

use query::Query;
use subscription::Subscription;

pub(crate) use subscription::Samplers;

impl juniper::Context for AppState {}

pub type Schema = RootNode<Query, EmptyMutation<AppState>, Subscription>;

pub fn schema() -> Schema {
    Schema::new(Query, EmptyMutation::<AppState>::new(), Subscription)
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
    JuniperRequest(request): JuniperRequest,
) -> JuniperResponse {
    JuniperResponse(request.execute(&*schema, &state).await)
}

async fn graphql_ws(
    Extension(schema): Extension<Arc<Schema>>,
    State(state): State<AppState>,
    ws: WebSocketUpgrade,
) -> Response {
    ws.protocols(["graphql-transport-ws", "graphql-ws"])
        .on_upgrade(move |socket| {
            subscriptions::serve_ws(socket, schema, ConnectionConfig::new(state))
        })
}

#[cfg(test)]
mod tests;
