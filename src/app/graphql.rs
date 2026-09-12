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
mod tests {
    use crate::kafka::{FakeCluster, QueryEngine};
    use juniper::{Variables, execute};

    use super::*;

    fn state() -> AppState {
        AppState::new(Arc::new(QueryEngine::from_sessions(vec![
            FakeCluster::local(),
        ])))
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
                    "clusterId": "test-cluster"
                }
            })
        );
    }

    #[tokio::test]
    async fn resolves_catalog_from_the_query_engine() {
        let state = state();
        let schema = schema();

        let (value, errors) = execute(
            r#"{
                brokers(cluster: "local") { id host }
                topics(cluster: "local") { name messageCount consumerGroups }
                consumerGroups(cluster: "local") { id lag }
            }"#,
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
                "brokers": [{ "id": 1, "host": "localhost" }],
                "topics": [{
                    "name": "orders.created",
                    "messageCount": 16.0,
                    "consumerGroups": ["order-processor"]
                }],
                "consumerGroups": [{ "id": "order-processor", "lag": 5.0 }]
            })
        );
    }

    #[tokio::test]
    async fn resolves_schema_subjects_from_the_query_engine() {
        let state = state();
        let schema = schema();

        let (value, errors) = execute(
            r#"{ schemaSubjects(cluster: "local") { subject id type latestVersion versions compatibility schema } }"#,
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
                "schemaSubjects": [{
                    "subject": "orders.created-value",
                    "id": 1,
                    "type": "AVRO",
                    "latestVersion": 2,
                    "versions": [1, 2],
                    "compatibility": "BACKWARD",
                    "schema": "{\"type\":\"record\",\"name\":\"Order\",\"fields\":[{\"name\":\"orderId\",\"type\":\"string\"}]}"
                }]
            })
        );
    }

    #[tokio::test]
    async fn browses_and_searches_records() {
        let state = state();
        let schema = schema();

        let (value, errors) = execute(
            r#"{
                records(query: {
                    cluster: "local"
                    topic: "orders.created"
                    filter: "key == \"ord_1\""
                    limit: 10
                    order: OLDEST
                }) { records { key } hasMore nextCursor }
                search(cluster: "local", term: "order") { kind id }
            }"#,
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
                "records": { "records": [{ "key": "ord_1" }], "hasMore": false, "nextCursor": null },
                "search": [
                    { "kind": "TOPIC", "id": "orders.created" },
                    { "kind": "GROUP", "id": "order-processor" },
                    { "kind": "SUBJECT", "id": "orders.created-value" }
                ]
            })
        );
    }

    #[tokio::test]
    async fn records_accept_schema_overrides_and_expose_wire_ids() {
        let state = state();
        let schema = schema();

        let (value, errors) = execute(
            r#"{
                records(query: {
                    cluster: "local"
                    topic: "orders.created"
                    filter: ""
                    limit: 1
                    order: OLDEST
                    schemaId: 1
                }) { records { schemaId } }
            }"#,
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
                "records": { "records": [{ "schemaId": null }] }
            })
        );
    }

    #[tokio::test]
    async fn pages_through_records() {
        let state = state();
        let schema = schema();

        let (value, errors) = execute(
            r#"{
                records(query: {
                    cluster: "local"
                    topic: "orders.created"
                    filter: ""
                    limit: 5
                    order: OLDEST
                }) { records { key } hasMore nextCursor }
            }"#,
            None,
            &schema,
            &Variables::new(),
            &state,
        )
        .await
        .unwrap();

        assert!(errors.is_empty());
        let page = serde_json::to_value(value).unwrap();
        assert_eq!(page["records"]["hasMore"], true);
        assert_eq!(page["records"]["records"].as_array().unwrap().len(), 5);
        let cursor = page["records"]["nextCursor"].as_str().unwrap();

        let (value, errors) = execute(
            &format!(
                r#"{{
                records(query: {{
                    cluster: "local"
                    topic: "orders.created"
                    filter: ""
                    limit: 5
                    order: OLDEST
                    cursor: "{cursor}"
                }}) {{ records {{ key }} hasMore nextCursor }}
            }}"#
            ),
            None,
            &schema,
            &Variables::new(),
            &state,
        )
        .await
        .unwrap();

        assert!(errors.is_empty());
        let next = serde_json::to_value(value).unwrap();
        assert!(!next["records"]["records"].as_array().unwrap().is_empty());
    }

    #[tokio::test]
    async fn records_honor_timestamp_bounds() {
        let state = state();
        let schema = schema();

        let (value, errors) = execute(
            r#"{
                records(query: {
                    cluster: "local"
                    topic: "orders.created"
                    filter: ""
                    timestampFrom: "2023-11-14T22:13:23Z"
                    timestampTo: "2023-11-14T22:13:25Z"
                    limit: 50
                    order: OLDEST
                }) { records { key timestamp } hasMore nextCursor }
            }"#,
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
                "records": {
                    "records": [
                        { "key": "ord_3", "timestamp": "2023-11-14T22:13:23Z" },
                        { "key": "ord_4", "timestamp": "2023-11-14T22:13:24Z" },
                        { "key": "ord_5", "timestamp": "2023-11-14T22:13:25Z" }
                    ],
                    "hasMore": false,
                    "nextCursor": null
                }
            })
        );
    }

    #[tokio::test]
    async fn records_reject_inverted_timestamp_range() {
        let state = state();
        let schema = schema();

        let (_, errors) = execute(
            r#"{
                records(query: {
                    cluster: "local"
                    topic: "orders.created"
                    filter: ""
                    timestampFrom: "1970-01-01T00:00:02Z"
                    timestampTo: "1970-01-01T00:00:01Z"
                    limit: 10
                    order: OLDEST
                }) { records { key } }
            }"#,
            None,
            &schema,
            &Variables::new(),
            &state,
        )
        .await
        .unwrap();

        assert!(
            errors
                .iter()
                .any(|error| error.error().message().contains("timestampFrom"))
        );
    }

    #[tokio::test]
    async fn records_reject_invalid_filter() {
        let state = state();
        let schema = schema();

        let (_, errors) = execute(
            r#"{
                records(query: {
                    cluster: "local"
                    topic: "orders.created"
                    filter: "value.status =="
                    limit: 10
                    order: OLDEST
                }) { records { key } }
            }"#,
            None,
            &schema,
            &Variables::new(),
            &state,
        )
        .await
        .unwrap();

        assert!(
            errors
                .iter()
                .any(|error| error.error().message().contains("invalid filter")),
            "{errors:?}"
        );
    }

    #[tokio::test]
    async fn consumer_groups_can_filter_by_topic() {
        let state = state();
        let schema = schema();

        let (value, errors) = execute(
            r#"{
                matching: consumerGroups(cluster: "local", topic: "orders.created") { id }
                none: consumerGroups(cluster: "local", topic: "missing") { id }
            }"#,
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
                "matching": [{ "id": "order-processor" }],
                "none": []
            })
        );
    }

    #[tokio::test]
    async fn schema_includes_topic_rate_subscription() {
        let sdl = schema().as_sdl();
        assert!(sdl.contains("type Subscription"));
        assert!(sdl.contains("topicRates(cluster: String!): [TopicRate!]!"));
        assert!(sdl.contains("consumerGroupLag(cluster: String!, id: String!): ConsumerGroup!"));
        assert!(sdl.contains("schemaId: Int"));
        assert!(sdl.contains("type ClusterCatalog"));
        assert!(sdl.contains("clusterCatalog(cluster: String!): ClusterCatalog!"));
    }

    #[tokio::test]
    async fn topics_and_catalog_read_the_in_memory_snapshot() {
        let state = state();
        state.catalog.store(
            "local",
            crate::kafka::ClusterSnapshot::from_topics(vec![crate::kafka::Topic {
                name: "from-cache".into(),
                internal: false,
                partitions: Vec::new(),
                replication_factor: 1,
                message_count: 3,
                cleanup_policy: crate::kafka::model::CleanupPolicy::Delete,
                retention_ms: 0,
                consumer_groups: vec!["cached-group".into()],
                under_replicated: false,
            }]),
        );

        let schema = schema();
        let (value, errors) = execute(
            r#"{
                topics(cluster: "local") { name messageCount consumerGroups }
                clusterCatalog(cluster: "local") { topics { name } }
            }"#,
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
                "topics": [{
                    "name": "from-cache",
                    "messageCount": 3.0,
                    "consumerGroups": ["cached-group"]
                }],
                "clusterCatalog": { "topics": [{ "name": "from-cache" }] }
            })
        );
    }

    fn cached_group(id: &str, topic: &str, lag: i64) -> crate::kafka::ConsumerGroup {
        crate::kafka::ConsumerGroup {
            id: id.into(),
            state: crate::kafka::model::GroupState::Stable,
            protocol: "range".into(),
            coordinator: 1,
            members: vec![crate::kafka::model::GroupMember {
                id: "member-1".into(),
                client_id: "client".into(),
                host: "127.0.0.1".into(),
                assignments: vec![crate::kafka::model::MemberAssignment {
                    topic: topic.into(),
                    partitions: vec![0],
                }],
            }],
            topics: vec![topic.into()],
            lag,
            offsets: vec![crate::kafka::model::GroupOffset {
                topic: topic.into(),
                partition: 0,
                current_offset: 1,
                end_offset: 1 + lag,
                lag,
                member_id: Some("member-1".into()),
            }],
        }
    }

    #[tokio::test]
    async fn groups_and_catalog_read_the_in_memory_snapshot() {
        let state = state();
        state.catalog.store(
            "local",
            crate::kafka::ClusterSnapshot::from_catalog(
                Vec::new(),
                vec![
                    cached_group("from-cache", "orders", 9),
                    cached_group("other", "payments", 2),
                ],
            ),
        );

        let schema = schema();
        let (value, errors) = execute(
            r#"{
                consumerGroups(cluster: "local") { id lag topics }
                matching: consumerGroups(cluster: "local", topic: "orders") { id }
                none: consumerGroups(cluster: "local", topic: "missing") { id }
                consumerGroup(cluster: "local", id: "from-cache") {
                    id
                    state
                    protocol
                    coordinator
                    lag
                    members { id clientId host }
                    offsets { topic partition lag memberId }
                }
                missing: consumerGroup(cluster: "local", id: "ghost") { id }
                clusterCatalog(cluster: "local") { consumerGroups { id } }
            }"#,
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
                "consumerGroups": [
                    { "id": "from-cache", "lag": 9.0, "topics": ["orders"] },
                    { "id": "other", "lag": 2.0, "topics": ["payments"] }
                ],
                "matching": [{ "id": "from-cache" }],
                "none": [],
                "consumerGroup": {
                    "id": "from-cache",
                    "state": "STABLE",
                    "protocol": "range",
                    "coordinator": 1,
                    "lag": 9.0,
                    "members": [{ "id": "member-1", "clientId": "client", "host": "127.0.0.1" }],
                    "offsets": [{ "topic": "orders", "partition": 0, "lag": 9.0, "memberId": "member-1" }]
                },
                "missing": null,
                "clusterCatalog": { "consumerGroups": [{ "id": "from-cache" }, { "id": "other" }] }
            })
        );
    }

    #[tokio::test]
    async fn cluster_catalog_exposes_updated_at_after_fallback() {
        let state = state();
        let schema = schema();

        let (value, errors) = execute(
            r#"{ clusterCatalog(cluster: "local") { updatedAt topics { name } } }"#,
            None,
            &schema,
            &Variables::new(),
            &state,
        )
        .await
        .unwrap();

        assert!(errors.is_empty());
        let body = serde_json::to_value(value).unwrap();
        assert_eq!(
            body["clusterCatalog"]["topics"][0]["name"],
            "orders.created"
        );
        assert!(
            body["clusterCatalog"]["updatedAt"]
                .as_str()
                .is_some_and(|timestamp| !timestamp.is_empty())
        );
        assert_eq!(
            state.catalog.topic("local", "orders.created").unwrap().name,
            "orders.created"
        );
        assert_eq!(
            state.catalog.group("local", "order-processor").unwrap().id,
            "order-processor"
        );
    }

    #[tokio::test]
    async fn topics_query_uses_stored_produce_rates() {
        let state = state();
        let start = tokio::time::Instant::now();
        state.rates.observe_at(
            "local",
            [("orders.created".to_owned(), 10)].into_iter().collect(),
            start,
            1_000.0,
        );
        state.rates.observe_at(
            "local",
            [("orders.created".to_owned(), 30)].into_iter().collect(),
            start + std::time::Duration::from_secs(2),
            3_000.0,
        );

        let schema = schema();
        let (value, errors) = execute(
            r#"{ topics(cluster: "local") { name messagesPerSec } }"#,
            None,
            &schema,
            &Variables::new(),
            &state,
        )
        .await
        .unwrap();

        assert!(errors.is_empty());
        assert_eq!(
            serde_json::to_value(value).unwrap()["topics"][0],
            serde_json::json!({ "name": "orders.created", "messagesPerSec": 10.0 })
        );
    }
}
