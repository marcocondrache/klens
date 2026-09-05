use std::sync::Arc;

use axum::{
    Router,
    extract::{Extension, State},
    routing::get,
};
use juniper::{EmptyMutation, EmptySubscription, GraphQLObject, RootNode, graphql_object};
use juniper_axum::{extract::JuniperRequest, response::JuniperResponse};

use crate::AppState;

impl juniper::Context for AppState {}

type AppSchema = RootNode<Query, EmptyMutation<AppState>, EmptySubscription<AppState>>;

fn schema() -> AppSchema {
    RootNode::new(
        Query,
        EmptyMutation::<AppState>::new(),
        EmptySubscription::<AppState>::new(),
    )
}

pub fn router() -> Router<AppState> {
    let router = Router::new()
        .route("/graphql", get(graphql).post(graphql))
        .layer(Extension(Arc::new(schema())));

    #[cfg(debug_assertions)]
    let router = router.route("/graphiql", get(juniper_axum::graphiql("/graphql", None)));

    router
}

#[derive(GraphQLObject)]
struct Cluster {
    name: String,
    bootstrap_servers: Vec<String>,
}

struct Query;

#[graphql_object(context = AppState)]
impl Query {
    async fn clusters(context: &AppState) -> Vec<Cluster> {
        context
            .clusters
            .list()
            .into_iter()
            .map(|client| Cluster {
                name: client.name().to_owned(),
                bootstrap_servers: client.config().bootstrap_servers.clone(),
            })
            .collect()
    }

    async fn cluster(context: &AppState, name: String) -> Option<Cluster> {
        context.clusters.get(&name).map(|client| Cluster {
            name: client.name().to_owned(),
            bootstrap_servers: client.config().bootstrap_servers.clone(),
        })
    }
}

async fn graphql(
    Extension(schema): Extension<Arc<AppSchema>>,
    State(state): State<AppState>,
    JuniperRequest(request): JuniperRequest,
) -> JuniperResponse {
    JuniperResponse(request.execute(&*schema, &state).await)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use juniper::{Variables, execute};
    use klens_kafka::{ClusterConfig, ClusterRegistry};

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
}
