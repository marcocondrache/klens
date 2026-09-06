use std::sync::Arc;

use axum::{
    Router,
    extract::{Extension, State},
    routing::get,
};
use juniper::{EmptyMutation, EmptySubscription, RootNode};
use juniper_axum::{extract::JuniperRequest, response::JuniperResponse};

use crate::AppState;

mod query;
mod types;

use query::Query;

impl juniper::Context for AppState {}

type Schema = RootNode<Query, EmptyMutation<AppState>, EmptySubscription<AppState>>;

fn schema() -> Schema {
    Schema::new(
        Query,
        EmptyMutation::<AppState>::new(),
        EmptySubscription::<AppState>::new(),
    )
}

pub(crate) fn schema_sdl() -> String {
    schema().as_sdl()
}

pub fn router() -> Router<AppState> {
    let router = Router::new()
        .route("/graphql", get(graphql).post(graphql))
        .layer(Extension(Arc::new(schema())));

    #[cfg(debug_assertions)]
    let router = router.route("/graphiql", get(juniper_axum::graphiql("/graphql", None)));

    router
}

async fn graphql(
    Extension(schema): Extension<Arc<Schema>>,
    State(state): State<AppState>,
    JuniperRequest(request): JuniperRequest,
) -> JuniperResponse {
    JuniperResponse(request.execute(&*schema, &state).await)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use crate::config::ClusterConfig;
    use crate::kafka::ClusterRegistry;
    use juniper::{Variables, execute};

    use super::*;

    fn state() -> AppState {
        let config = ClusterConfig {
            name: "local".to_owned(),
            bootstrap_servers: vec!["localhost:9092".to_owned()],
            security: None,
            properties: HashMap::new(),
        };

        AppState::new(Arc::new(ClusterRegistry::build(vec![config]).unwrap()))
    }

    #[tokio::test]
    async fn resolves_cluster_list() {
        let state = state();
        let schema = schema();

        let (value, errors) = execute(
            "{ clusters { name bootstrapServers } }",
            None,
            &schema,
            &Variables::new(),
            &state,
        )
        .await
        .unwrap();

        assert!(errors.is_empty());
        assert_eq!(
            serde_json::to_value(value).unwrap(),
            serde_json::json!({
                "clusters": [
                    { "name": "local", "bootstrapServers": ["localhost:9092"] }
                ]
            })
        );
    }

    #[tokio::test]
    async fn resolves_cluster_by_name() {
        let state = state();
        let schema = schema();

        let (value, errors) = execute(
            r#"{ cluster(name: "local") { name bootstrapServers } }"#,
            None,
            &schema,
            &Variables::new(),
            &state,
        )
        .await
        .unwrap();

        assert!(errors.is_empty());
        assert_eq!(
            serde_json::to_value(value).unwrap(),
            serde_json::json!({
                "cluster": { "name": "local", "bootstrapServers": ["localhost:9092"] }
            })
        );
    }

    #[tokio::test]
    async fn returns_none_for_unknown_cluster() {
        let state = state();
        let schema = schema();

        let (value, errors) = execute(
            r#"{ cluster(name: "missing") { name } }"#,
            None,
            &schema,
            &Variables::new(),
            &state,
        )
        .await
        .unwrap();

        assert!(errors.is_empty());
        assert_eq!(
            serde_json::to_value(value).unwrap(),
            serde_json::json!({ "cluster": serde_json::Value::Null })
        );
    }

    #[tokio::test]
    async fn fills_cluster_identity_from_config() {
        let state = state();
        let schema = schema();

        let (value, errors) = execute(
            "{ cluster(name: \"local\") { label securityProtocol status version clusterId } }",
            None,
            &schema,
            &Variables::new(),
            &state,
        )
        .await
        .unwrap();

        assert!(errors.is_empty());
        assert_eq!(
            serde_json::to_value(value).unwrap(),
            serde_json::json!({
                "cluster": {
                    "label": "local",
                    "securityProtocol": "PLAINTEXT",
                    "status": "HEALTHY",
                    "version": "",
                    "clusterId": ""
                }
            })
        );
    }

    #[tokio::test]
    async fn stubs_return_empty_collections() {
        let state = state();
        let schema = schema();

        let (value, errors) = execute(
            r#"{ brokers(cluster: "local") { id } topics(cluster: "local") { name } }"#,
            None,
            &schema,
            &Variables::new(),
            &state,
        )
        .await
        .unwrap();

        assert!(errors.is_empty());
        assert_eq!(
            serde_json::to_value(value).unwrap(),
            serde_json::json!({
                "brokers": [],
                "topics": []
            })
        );
    }
}
