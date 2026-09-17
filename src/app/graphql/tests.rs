use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use futures::StreamExt as _;
use juniper::{
    DefaultScalarValue, ExecutionError, Value, Variables, execute, graphql_value,
    resolve_into_stream,
};

use crate::AppState;
use crate::app::auth::SessionGuard;
use crate::app::auth::access::{ClusterScope, EffectiveAccess, Grant, Role};
use crate::kafka::store::bus::BUS_CAPACITY;
use crate::kafka::store::fixtures::{
    at, config, group, offline_partition, offsets, partition, subject, topic, topology, watermarks,
};
use crate::kafka::store::{
    Change, ClusterStore, ConfigTable, ConfigsDelta, GroupLagUpdate, GroupOffsetsWave, Interner,
    OffsetTable, SubjectTable, SubjectsDelta, TopicRate, TopologyDelta, WatermarksTick,
};
use crate::kafka::{FakeCluster, QueryEngine};

use super::context::GraphQlContext;
use super::schema;

// ------------------------------------------------------------ harness

fn with(sessions: Vec<FakeCluster>) -> AppState {
    AppState::new(Arc::new(QueryEngine::from_sessions(sessions)))
}

fn state() -> AppState {
    with(vec![FakeCluster::local()])
}

fn two_clusters() -> AppState {
    with(vec![FakeCluster::local(), FakeCluster::named("payments")])
}

fn ctx(state: &AppState) -> GraphQlContext {
    GraphQlContext::unrestricted(state.clone())
}

fn ctx_with(state: &AppState, access: EffectiveAccess) -> GraphQlContext {
    GraphQlContext {
        access,
        ..ctx(state)
    }
}

fn only(clusters: &[&str]) -> ClusterScope {
    ClusterScope::Only(clusters.iter().map(|name| (*name).to_owned()).collect())
}

fn granted(grants: Vec<(Role, ClusterScope)>) -> EffectiveAccess {
    EffectiveAccess::Granted(
        grants
            .into_iter()
            .map(|(role, scope)| Grant { role, scope })
            .collect(),
    )
}

fn viewer_everywhere() -> EffectiveAccess {
    granted(vec![(Role::Viewer, ClusterScope::All)])
}

async fn run(
    context: &GraphQlContext,
    query: &str,
) -> (serde_json::Value, Vec<ExecutionError<DefaultScalarValue>>) {
    let (data, errors) = execute(query, None, &schema(), &Variables::new(), context)
        .await
        .expect("query is valid against the schema");
    (serde_json::to_value(data).expect("data is serializable"), errors)
}

/// Runs `query` and fails the test if it produced any error.
async fn ok(context: &GraphQlContext, query: &str) -> serde_json::Value {
    let (data, errors) = run(context, query).await;
    assert!(
        errors.is_empty(),
        "{:?}",
        errors
            .iter()
            .map(|error| error.error().message())
            .collect::<Vec<_>>()
    );
    data
}

fn code_of(error: &ExecutionError<DefaultScalarValue>) -> String {
    match error.error().extensions() {
        Value::Object(object) => match object.get_field_value("code") {
            Some(Value::Scalar(DefaultScalarValue::String(code))) => code.clone(),
            other => panic!("error has no string code: {other:?}"),
        },
        other => panic!("error has no extensions object: {other:?}"),
    }
}

/// Runs `query` and returns the `extensions.code` of every error it produced.
async fn codes(context: &GraphQlContext, query: &str) -> Vec<String> {
    run(context, query).await.1.iter().map(code_of).collect()
}

// ------------------------------------------------------------ seeding

/// The store every query test reads from: two topics, one group committed
/// behind the log, one subject, one topic config.
fn seed(store: &ClusterStore) {
    store.topology.commit(Arc::new(topology(
        vec![
            topic("orders.created", vec![
                partition(0, vec![1], vec![1]),
                partition(1, vec![1], vec![1]),
            ]),
            topic("payments.settled", vec![partition(0, vec![1], vec![1])]),
        ],
        vec![group("order-processor", "orders.created", vec![0, 1])],
    )));

    store.watermarks.commit(Arc::new(watermarks(at(1_000), &[
        ("orders.created", 0, 0, 100),
        ("orders.created", 1, 10, 60),
        ("payments.settled", 0, 0, 5),
    ])));

    store.offsets.commit(Arc::new(OffsetTable {
        groups: HashMap::from([(
            Arc::from("order-processor"),
            Arc::new(offsets(at(1_000), &[
                ("orders.created", 0, 90),
                ("orders.created", 1, 55),
            ])),
        )]),
    }));

    store.configs.commit(Arc::new(ConfigTable {
        topics: HashMap::from([(
            Arc::from("orders.created"),
            Arc::new(vec![
                config("cleanup.policy", "compact"),
                config("retention.ms", "604800000"),
            ]),
        )]),
    }));

    store.subjects.commit(Arc::new(SubjectTable::assemble(
        &[subject("orders.created-value", 1, 2)],
        &mut Interner::default(),
    )));

    store.rebuild_search();
}

fn seeded() -> AppState {
    seeded_with(FakeCluster::local()).0
}

/// The same seeded store, with the session handle kept so a test can assert
/// the broker was never called.
fn seeded_with(session: FakeCluster) -> (AppState, FakeCluster) {
    let state = with(vec![session.clone()]);
    seed(state.cluster("local").expect("local cluster"));
    (state, session)
}

// ------------------------------------------------------------ identity

#[tokio::test]
async fn whoami_reports_no_subject_when_auth_is_disabled() {
    let state = two_clusters();
    let data = ok(&ctx(&state), "{ whoami { subject clusters { cluster role } } }").await;

    assert_eq!(data["whoami"]["subject"], serde_json::Value::Null);
    assert_eq!(data["whoami"]["clusters"][0]["cluster"], "local");
    assert_eq!(data["whoami"]["clusters"][0]["role"], "ADMIN");
    assert_eq!(data["whoami"]["clusters"][1]["cluster"], "payments");
}

#[tokio::test]
async fn whoami_resolves_each_cluster_against_its_own_grant() {
    let state = two_clusters();
    let context = ctx_with(
        &state,
        granted(vec![
            (Role::Admin, only(&["local"])),
            (Role::Viewer, only(&["payments"])),
        ]),
    );

    let data = ok(&context, "{ whoami { clusters { cluster role privileges } } }").await;
    let clusters = data["whoami"]["clusters"].as_array().expect("clusters");

    assert_eq!(clusters.len(), 2);
    assert_eq!(clusters[0]["cluster"], "local");
    assert_eq!(clusters[0]["role"], "ADMIN");
    assert_eq!(
        clusters[0]["privileges"],
        serde_json::json!(["RECORDS", "CONFIGS", "SCHEMA_TEXT", "ACLS"])
    );

    // The wide Admin grant on `local` must not raise the narrow Viewer grant.
    assert_eq!(clusters[1]["cluster"], "payments");
    assert_eq!(clusters[1]["role"], "VIEWER");
    assert_eq!(clusters[1]["privileges"], serde_json::json!([]));
}

#[tokio::test]
async fn whoami_omits_clusters_the_session_cannot_see() {
    let state = two_clusters();
    let context = ctx_with(&state, granted(vec![(Role::Viewer, only(&["payments"]))]));

    let data = ok(&context, "{ whoami { clusters { cluster } } }").await;

    assert_eq!(
        data["whoami"]["clusters"],
        serde_json::json!([{ "cluster": "payments" }])
    );
}

// ------------------------------------------------------------ denials

#[tokio::test]
async fn an_invisible_cluster_reads_as_unknown_not_forbidden() {
    let state = two_clusters();
    let context = ctx_with(&state, granted(vec![(Role::Admin, only(&["local"]))]));

    assert_eq!(
        codes(&context, r#"{ topicRows(cluster: "payments") { total } }"#).await,
        vec!["UNKNOWN_CLUSTER"]
    );
}

#[tokio::test]
async fn a_cluster_nobody_configured_reads_as_unknown() {
    assert_eq!(
        codes(&ctx(&state()), r#"{ topicRows(cluster: "nope") { total } }"#).await,
        vec!["UNKNOWN_CLUSTER"]
    );
}

#[tokio::test]
async fn a_visible_cluster_without_the_privilege_reads_as_forbidden() {
    let state = seeded();
    let context = ctx_with(&state, viewer_everywhere());

    for query in [
        r#"{ topicConfigs(cluster: "local", name: "orders.created") { name } }"#,
        r#"{ brokerConfigs(cluster: "local", id: 1) { name } }"#,
        r#"{ acls(cluster: "local") { authorizer } }"#,
        r#"{ subject(cluster: "local", name: "orders.created-value") { schema } }"#,
        r#"{ records(cluster: "local", query: { topic: "orders.created" }) { complete } }"#,
    ] {
        assert_eq!(codes(&context, query).await, vec!["FORBIDDEN"], "{query}");
    }
}

#[tokio::test]
async fn unprivileged_projections_stay_open_to_a_viewer() {
    let state = seeded();
    let context = ctx_with(&state, viewer_everywhere());

    let data = ok(
        &context,
        r#"{
            topicRows(cluster: "local") { total }
            groupRows(cluster: "local") { total }
            brokerRows(cluster: "local") { id }
            subjectRows(cluster: "local") { rows { subject } }
        }"#,
    )
    .await;

    assert_eq!(data["topicRows"]["total"], 2);
    assert_eq!(data["groupRows"]["total"], 1);
    assert_eq!(data["brokerRows"][0]["id"], 1);
    assert_eq!(data["subjectRows"]["rows"][0]["subject"], "orders.created-value");
}

// ------------------------------------------------------------ Int64

#[tokio::test]
async fn sixty_four_bit_counters_cross_the_wire_as_strings() {
    let state = state();
    let store = state.cluster("local").expect("local cluster");
    let huge = 9_007_199_254_740_993_i64; // 2^53 + 1: lossy as a JSON number.

    store.topology.commit(Arc::new(topology(
        vec![topic("wide", vec![partition(0, vec![1], vec![1])])],
        Vec::new(),
    )));
    store
        .watermarks
        .commit(Arc::new(watermarks(at(1_000), &[("wide", 0, 0, huge)])));

    let data = ok(
        &ctx(&state),
        r#"{ topicRows(cluster: "local") { rows { retainedMessages producedTotal } } }"#,
    )
    .await;

    assert_eq!(data["topicRows"]["rows"][0]["retainedMessages"], huge.to_string());
    assert_eq!(data["topicRows"]["rows"][0]["producedTotal"], huge.to_string());
}

// ------------------------------------------------------------ rows

#[tokio::test]
async fn topic_rows_project_counts_and_configs_without_touching_the_broker() {
    let (state, session) = seeded_with(FakeCluster::local());

    let data = ok(
        &ctx(&state),
        r#"{ topicRows(cluster: "local", sort: { field: NAME }) {
            total
            rows { name partitionCount retainedMessages cleanupPolicy retentionMs groupCount }
        } }"#,
    )
    .await;
    let rows = &data["topicRows"]["rows"];

    assert_eq!(data["topicRows"]["total"], 2);
    assert_eq!(rows[0]["name"], "orders.created");
    assert_eq!(rows[0]["partitionCount"], 2);
    // (100 - 0) + (60 - 10)
    assert_eq!(rows[0]["retainedMessages"], "150");
    assert_eq!(rows[0]["cleanupPolicy"], "COMPACT");
    assert_eq!(rows[0]["retentionMs"], "604800000");
    assert_eq!(rows[0]["groupCount"], 1);
    assert_eq!(session.calls().metadata(), 0);
}

#[tokio::test]
async fn topic_rows_filter_by_name_before_paging() {
    let state = seeded();

    let data = ok(
        &ctx(&state),
        r#"{ topicRows(cluster: "local", filter: { contains: "PAY" }) {
            total
            rows { name }
        } }"#,
    )
    .await;

    assert_eq!(data["topicRows"]["total"], 1);
    assert_eq!(data["topicRows"]["rows"][0]["name"], "payments.settled");
}

#[tokio::test]
async fn topic_rows_page_by_key_and_report_the_unpaged_total() {
    let state = seeded();

    let first = ok(
        &ctx(&state),
        r#"{ topicRows(cluster: "local", limit: 1) { total nextCursor rows { name } } }"#,
    )
    .await;

    assert_eq!(first["topicRows"]["total"], 2);
    assert_eq!(first["topicRows"]["rows"][0]["name"], "orders.created");
    assert_eq!(first["topicRows"]["nextCursor"], "orders.created");

    let second = ok(
        &ctx(&state),
        r#"{ topicRows(cluster: "local", limit: 1, after: "orders.created") {
            nextCursor
            rows { name }
        } }"#,
    )
    .await;

    assert_eq!(second["topicRows"]["rows"][0]["name"], "payments.settled");
    assert_eq!(second["topicRows"]["nextCursor"], serde_json::Value::Null);
}

#[tokio::test]
async fn topic_rows_sort_descending_on_the_requested_column() {
    let state = seeded();

    let data = ok(
        &ctx(&state),
        r#"{ topicRows(cluster: "local", sort: { field: RETAINED_MESSAGES, desc: true }) {
            rows { name }
        } }"#,
    )
    .await;

    assert_eq!(data["topicRows"]["rows"][0]["name"], "orders.created");
    assert_eq!(data["topicRows"]["rows"][1]["name"], "payments.settled");
}

#[tokio::test]
async fn group_rows_join_commits_against_watermarks() {
    let state = seeded();

    let data = ok(
        &ctx(&state),
        r#"{ groupRows(cluster: "local") {
            rows { id state memberCount topicNames totalLag lagComplete }
        } }"#,
    )
    .await;
    let row = &data["groupRows"]["rows"][0];

    assert_eq!(row["id"], "order-processor");
    assert_eq!(row["state"], "STABLE");
    assert_eq!(row["memberCount"], 1);
    assert_eq!(row["topicNames"], serde_json::json!(["orders.created"]));
    // (100 - 90) + (60 - 55)
    assert_eq!(row["totalLag"], "15");
    assert_eq!(row["lagComplete"], true);
}

#[tokio::test]
async fn broker_rows_count_the_partitions_each_node_carries() {
    let state = seeded();

    let data = ok(
        &ctx(&state),
        r#"{ brokerRows(cluster: "local") { id host port controller partitionCount leaderCount } }"#,
    )
    .await;

    assert_eq!(data["brokerRows"][0]["host"], "localhost");
    assert_eq!(data["brokerRows"][0]["partitionCount"], 3);
    assert_eq!(data["brokerRows"][0]["leaderCount"], 3);
}

// ------------------------------------------------------------ details

#[tokio::test]
async fn topic_detail_flags_under_replication_per_partition() {
    let state = state();
    let store = state.cluster("local").expect("local cluster");
    store.topology.commit(Arc::new(topology(
        vec![topic("orders.created", vec![
            partition(0, vec![1, 2], vec![1, 2]),
            offline_partition(1, vec![1, 2]),
        ])],
        Vec::new(),
    )));

    let data = ok(
        &ctx(&state),
        r#"{ topic(cluster: "local", name: "orders.created") {
            name
            replicationFactor
            underReplicated
            partitions { id leader underReplicated }
        } }"#,
    )
    .await;
    let topic = &data["topic"];

    assert_eq!(topic["replicationFactor"], 2);
    assert_eq!(topic["underReplicated"], true);
    assert_eq!(topic["partitions"][0]["underReplicated"], false);
    assert_eq!(topic["partitions"][1]["underReplicated"], true);
    assert_eq!(topic["partitions"][1]["leader"], -1);
}

#[tokio::test]
async fn a_missing_topic_is_null_not_an_error() {
    let state = seeded();
    let (data, errors) = run(
        &ctx(&state),
        r#"{ topic(cluster: "local", name: "ghost") { name } }"#,
    )
    .await;

    assert!(errors.is_empty());
    assert_eq!(data["topic"], serde_json::Value::Null);
}

#[tokio::test]
async fn topic_groups_report_lag_on_that_topic_alone() {
    let state = seeded();

    let data = ok(
        &ctx(&state),
        r#"{ topicGroups(cluster: "local", topic: "orders.created") {
            id state memberCount lagOnTopic
        } }"#,
    )
    .await;

    assert_eq!(data["topicGroups"][0]["id"], "order-processor");
    assert_eq!(data["topicGroups"][0]["lagOnTopic"], "15");
}

#[tokio::test]
async fn opening_a_group_registers_interest_so_its_offsets_poll_faster() {
    let state = seeded();
    let store = state.cluster("local").expect("local cluster");
    assert!(!store.interest.is_hot("order-processor"));

    let data = ok(
        &ctx(&state),
        r#"{ group(cluster: "local", id: "order-processor") {
            id totalLag lagComplete
            members { id clientId }
            offsets { topic partition currentOffset endOffset lag }
        } }"#,
    )
    .await;

    assert_eq!(data["group"]["totalLag"], "15");
    assert_eq!(data["group"]["members"][0]["clientId"], "c1");
    assert_eq!(data["group"]["offsets"][0]["currentOffset"], "90");
    assert_eq!(data["group"]["offsets"][0]["endOffset"], "100");
    assert_eq!(data["group"]["offsets"][0]["lag"], "10");
    assert!(store.interest.is_hot("order-processor"));
}

#[tokio::test]
async fn topic_configs_come_from_the_lane_not_the_broker() {
    let (state, session) = seeded_with(FakeCluster::local());

    let data = ok(
        &ctx(&state),
        r#"{ topicConfigs(cluster: "local", name: "orders.created") { name value source } }"#,
    )
    .await;

    assert_eq!(data["topicConfigs"][0]["name"], "cleanup.policy");
    assert_eq!(data["topicConfigs"][0]["value"], "compact");
    assert_eq!(data["topicConfigs"][0]["source"], "DYNAMIC_TOPIC_CONFIG");
    assert_eq!(session.calls().topic_configs(), 0);
}

#[tokio::test]
async fn topic_configs_for_an_unknown_topic_are_an_error() {
    let state = seeded();

    assert_eq!(
        codes(
            &ctx(&state),
            r#"{ topicConfigs(cluster: "local", name: "ghost") { name } }"#
        )
        .await,
        vec!["UNKNOWN_TOPIC"]
    );
}

// ------------------------------------------------------------ live reads

#[tokio::test]
async fn broker_configs_stay_live_because_no_lane_sweeps_them() {
    let state = seeded();

    let data = ok(
        &ctx(&state),
        r#"{ brokerConfigs(cluster: "local", id: 1) { name value } }"#,
    )
    .await;

    assert_eq!(data["brokerConfigs"][0]["name"], "log.retention.hours");
}

#[tokio::test]
async fn acls_stay_live_and_carry_the_authorizer_state() {
    let state = seeded();

    let data = ok(
        &ctx(&state),
        r#"{ acls(cluster: "local") { authorizer bindings { resourceType principal operation permission } } }"#,
    )
    .await;

    assert_eq!(data["acls"]["authorizer"], "ENABLED");
    assert!(!data["acls"]["bindings"].as_array().expect("bindings").is_empty());
}

#[tokio::test]
async fn a_schema_body_is_fetched_on_demand_rather_than_kept_in_the_lane() {
    let state = seeded();

    let data = ok(
        &ctx(&state),
        r#"{ subject(cluster: "local", name: "orders.created-value") {
            subject version id type schema
        } }"#,
    )
    .await;

    assert_eq!(data["subject"]["subject"], "orders.created-value");
    assert_eq!(data["subject"]["type"], "AVRO");
    assert!(
        data["subject"]["schema"]
            .as_str()
            .expect("schema body")
            .contains("orderId")
    );
}

#[tokio::test]
async fn an_unknown_subject_is_a_typed_error() {
    let state = seeded();

    assert_eq!(
        codes(
            &ctx(&state),
            r#"{ subject(cluster: "local", name: "ghost-value") { schema } }"#
        )
        .await,
        vec!["UNKNOWN_SUBJECT"]
    );
}

#[tokio::test]
async fn records_are_read_live_through_the_scan_path() {
    let state = seeded();

    let data = ok(
        &ctx(&state),
        r#"{ records(cluster: "local", query: { topic: "orders.created", limit: 3 }) {
            complete
            records { topic partition offset key sizeBytes compression }
        } }"#,
    )
    .await;

    let records = data["records"]["records"].as_array().expect("records");
    assert_eq!(records.len(), 3);
    assert_eq!(records[0]["topic"], "orders.created");
    assert_eq!(records[0]["sizeBytes"], "24");
    assert_eq!(records[0]["compression"], "NONE");
}

#[tokio::test]
async fn a_record_filter_cannot_be_both_a_substring_and_an_expression() {
    let state = seeded();

    assert_eq!(
        codes(
            &ctx(&state),
            r#"{ records(
                cluster: "local",
                query: { topic: "orders.created", filter: { contains: "a", cel: "true" } }
            ) { complete } }"#
        )
        .await,
        vec!["INVALID_FILTER"]
    );
}

// ------------------------------------------------------------ health, series, search

#[tokio::test]
async fn cluster_health_reports_per_lane_freshness_and_counts() {
    let state = seeded();

    let data = ok(
        &ctx(&state),
        r#"{ clusters {
            cluster ready
            topology { healthy updatedAt }
            subjects { healthy }
            topicCount partitionCount groupCount brokerCount subjectCount
            underReplicatedPartitions offlinePartitions
        } }"#,
    )
    .await;
    let health = &data["clusters"][0];

    assert_eq!(health["cluster"], "local");
    assert_eq!(health["ready"], true);
    assert_eq!(health["topology"]["healthy"], true);
    assert_ne!(health["topology"]["updatedAt"], serde_json::Value::Null);
    assert_eq!(health["topicCount"], 2);
    assert_eq!(health["partitionCount"], 3);
    assert_eq!(health["groupCount"], 1);
    assert_eq!(health["brokerCount"], 1);
    assert_eq!(health["subjectCount"], 1);
    assert_eq!(health["underReplicatedPartitions"], 0);
}

#[tokio::test]
async fn a_cluster_with_no_commits_yet_is_visible_but_not_ready() {
    let state = state();

    let data = ok(&ctx(&state), "{ clusters { cluster ready topicCount } }").await;

    assert_eq!(data["clusters"][0]["ready"], false);
    assert_eq!(data["clusters"][0]["topicCount"], 0);
}

#[tokio::test]
async fn history_replays_the_same_points_the_subscription_streams() {
    let state = seeded();
    let store = state.cluster("local").expect("local cluster");
    let topic: Arc<str> = Arc::from("orders.created");
    let group: Arc<str> = Arc::from("order-processor");

    store.series.push_topic_rate(&topic, at(1_000), 12.5);
    store.series.push_topic_rate(&topic, at(2_000), 13.5);
    store.series.push_group_lag(&group, at(2_000), 15);

    let data = ok(
        &ctx(&state),
        r#"{
            topicRateHistory(cluster: "local", topic: "orders.created") { at value }
            groupLagHistory(cluster: "local", group: "order-processor") { value }
        }"#,
    )
    .await;

    assert_eq!(data["topicRateHistory"].as_array().expect("points").len(), 2);
    assert_eq!(data["topicRateHistory"][1]["value"], 13.5);
    assert_eq!(data["groupLagHistory"][0]["value"], 15.0);
}

#[tokio::test]
async fn search_is_answered_from_the_prebuilt_index() {
    let state = seeded();

    let data = ok(
        &ctx(&state),
        r#"{ search(cluster: "local", term: "orders") { kind id } }"#,
    )
    .await;
    let kinds: BTreeSet<&str> = data["search"]
        .as_array()
        .expect("hits")
        .iter()
        .map(|hit| hit["kind"].as_str().expect("kind"))
        .collect();

    assert!(kinds.contains("TOPIC"), "{kinds:?}");
    assert!(kinds.contains("SUBJECT"), "{kinds:?}");
}

// ------------------------------------------------------------ subscription

/// Opens `updates`, runs `publish` once the subscriber is attached, and
/// returns at most `take` events. An event is `Err` when the stream
/// terminated with a GraphQL error.
async fn updates(
    context: &GraphQlContext,
    query: &str,
    publish: impl FnOnce(),
    take: usize,
) -> Vec<Result<Value, String>> {
    let schema = schema();
    let (stream, errors) = resolve_into_stream(query, None, &schema, &Variables::new(), context)
        .await
        .expect("subscription is valid against the schema");
    let mut connection = juniper_subscriptions::Connection::from_stream(stream, errors);

    // The resolver has already run, so the store's bus has this subscriber.
    publish();

    let mut events = Vec::new();
    for _ in 0..take {
        let Some(output) = connection.next().await else {
            break;
        };
        events.push(match output.errors.first() {
            Some(error) => Err(error.error().message().to_owned()),
            None => Ok(output.data),
        });
    }
    events
}

/// The `extensions.code` of every error raised while opening `updates`.
async fn subscribe_codes(context: &GraphQlContext, query: &str) -> Vec<String> {
    let schema = schema();
    let (_, errors) = resolve_into_stream(query, None, &schema, &Variables::new(), context)
        .await
        .expect("subscription is valid against the schema");
    errors.iter().map(code_of).collect()
}

fn tick(topics: &[(&str, f64)]) -> Change {
    Change::Watermarks(Arc::new(WatermarksTick {
        version: 1,
        at: at(1_000),
        rates: topics
            .iter()
            .map(|(topic, rate)| TopicRate {
                topic: Arc::from(*topic),
                rate: *rate,
            })
            .collect(),
        cluster_rate: topics.iter().map(|(_, rate)| rate).sum(),
    }))
}

fn wave(groups: &[(&str, i64)]) -> Change {
    Change::GroupOffsets(Arc::new(GroupOffsetsWave {
        version: 1,
        at: at(1_000),
        groups: groups
            .iter()
            .map(|(group, lag)| GroupLagUpdate {
                group: Arc::from(*group),
                total_lag: *lag,
                lag_complete: true,
                offsets: Vec::new(),
            })
            .collect(),
    }))
}

#[tokio::test]
async fn an_unscoped_subscriber_gets_the_whole_cluster_firehose() {
    let state = seeded();
    let store = Arc::clone(state.cluster("local").expect("local cluster"));

    let events = updates(
        &ctx(&state),
        r#"subscription { updates(cluster: "local") {
            __typename
            ... on WatermarksTick { clusterRate topics { topic rate } }
        } }"#,
        || store.bus.publish(tick(&[("orders.created", 10.0), ("payments.settled", 2.0)])),
        1,
    )
    .await;

    let value = events[0].as_ref().expect("no error");
    assert_eq!(
        *value,
        graphql_value!({ "updates": {
            "__typename": "WatermarksTick",
            "clusterRate": 12.0,
            "topics": [
                { "topic": "orders.created", "rate": 10.0 },
                { "topic": "payments.settled", "rate": 2.0 },
            ],
        } })
    );
}

#[tokio::test]
async fn a_topic_scoped_subscriber_pays_only_for_its_own_topic() {
    let state = seeded();
    let store = Arc::clone(state.cluster("local").expect("local cluster"));

    let events = updates(
        &ctx(&state),
        r#"subscription { updates(cluster: "local", scope: { topic: "orders.created" }) {
            ... on WatermarksTick { topics { topic rate } }
        } }"#,
        || store.bus.publish(tick(&[("orders.created", 10.0), ("payments.settled", 2.0)])),
        1,
    )
    .await;

    assert_eq!(
        *events[0].as_ref().expect("no error"),
        graphql_value!({ "updates": { "topics": [{ "topic": "orders.created", "rate": 10.0 }] } })
    );
}

#[tokio::test]
async fn an_event_outside_the_scope_never_reaches_the_socket() {
    let state = seeded();
    let store = Arc::clone(state.cluster("local").expect("local cluster"));

    let events = updates(
        &ctx(&state),
        r#"subscription { updates(cluster: "local", scope: { topic: "payments.settled" }) {
            __typename
            ... on ConfigsChanged { topics }
        } }"#,
        || {
            // Only `orders.created` changed, so the scoped subscriber must
            // see the topology delta that names it and nothing else.
            store.bus.publish(Change::Configs(Arc::new(ConfigsDelta {
                version: 1,
                topics: vec![Arc::from("orders.created")],
            })));
            store.bus.publish(Change::Subjects(Arc::new(SubjectsDelta {
                version: 1,
                added: vec![Arc::from("payments.settled-value")],
                removed: Vec::new(),
                changed: Vec::new(),
            })));
        },
        1,
    )
    .await;

    assert_eq!(
        *events[0].as_ref().expect("no error"),
        graphql_value!({ "updates": { "__typename": "SubjectsChanged" } })
    );
}

#[tokio::test]
async fn an_unscoped_lag_wave_fans_out_one_update_per_group_without_offsets() {
    let state = seeded();
    let store = Arc::clone(state.cluster("local").expect("local cluster"));

    let events = updates(
        &ctx(&state),
        r#"subscription { updates(cluster: "local") {
            ... on GroupLagUpdate { group lag offsets { partition } }
        } }"#,
        || store.bus.publish(wave(&[("order-processor", 15), ("audit", 3)])),
        2,
    )
    .await;

    assert_eq!(
        *events[0].as_ref().expect("no error"),
        graphql_value!({ "updates": { "group": "order-processor", "lag": "15", "offsets": [] } })
    );
    assert_eq!(
        *events[1].as_ref().expect("no error"),
        graphql_value!({ "updates": { "group": "audit", "lag": "3", "offsets": [] } })
    );
}

#[tokio::test]
async fn a_group_scoped_subscriber_holds_an_interest_lease_for_the_stream() {
    let state = seeded();
    let store = Arc::clone(state.cluster("local").expect("local cluster"));
    let context = ctx(&state);
    let schema = schema();

    {
        let (stream, errors) = resolve_into_stream(
            r#"subscription { updates(cluster: "local", scope: { group: "order-processor" }) {
                ... on GroupLagUpdate { group lag }
            } }"#,
            None,
            &schema,
            &Variables::new(),
            &context,
        )
        .await
        .expect("subscription is valid against the schema");
        let mut connection = juniper_subscriptions::Connection::from_stream(stream, errors);

        store
            .bus
            .publish(wave(&[("order-processor", 15), ("audit", 3)]));

        let output = connection.next().await.expect("an event");
        assert_eq!(
            output.data,
            graphql_value!({ "updates": { "group": "order-processor", "lag": "15" } })
        );
        assert!(store.interest.is_hot("order-processor"));
    }

    // Disconnecting releases the offsets lane's fast tier immediately, so a
    // closed page stops costing broker calls.
    assert!(!store.interest.is_hot("order-processor"));
}

#[tokio::test]
async fn a_topology_delta_reaches_a_scoped_subscriber_only_when_it_names_its_topic() {
    let state = seeded();
    let store = Arc::clone(state.cluster("local").expect("local cluster"));

    let events = updates(
        &ctx(&state),
        r#"subscription { updates(cluster: "local", scope: { topic: "orders.created" }) {
            ... on TopologyDelta { version addedTopics changedTopics }
        } }"#,
        || {
            store.bus.publish(Change::Topology(Arc::new(TopologyDelta {
                version: 4,
                added_topics: vec![Arc::from("unrelated")],
                removed_topics: Vec::new(),
                changed_topics: Vec::new(),
                added_groups: Vec::new(),
                removed_groups: Vec::new(),
                changed_groups: Vec::new(),
                brokers_changed: false,
            })));
            store.bus.publish(Change::Topology(Arc::new(TopologyDelta {
                version: 5,
                added_topics: Vec::new(),
                removed_topics: Vec::new(),
                changed_topics: vec![Arc::from("orders.created")],
                added_groups: Vec::new(),
                removed_groups: Vec::new(),
                changed_groups: Vec::new(),
                brokers_changed: false,
            })));
        },
        1,
    )
    .await;

    assert_eq!(
        *events[0].as_ref().expect("no error"),
        graphql_value!({ "updates": {
            "version": "5",
            "addedTopics": [],
            "changedTopics": ["orders.created"],
        } })
    );
}

#[tokio::test]
async fn falling_behind_the_bus_asks_the_client_to_refetch_instead_of_dropping_it() {
    let state = seeded();
    let store = Arc::clone(state.cluster("local").expect("local cluster"));

    let events = updates(
        &ctx(&state),
        r#"subscription { updates(cluster: "local") {
            __typename
            ... on Resync { reason }
        } }"#,
        move || {
            // The bus is bounded, so publishing past its capacity before the
            // stream is first polled guarantees the subscriber lagged.
            for index in 0..(2 * BUS_CAPACITY) {
                store.bus.publish(tick(&[("orders.created", index as f64)]));
            }
        },
        1,
    )
    .await;

    assert_eq!(
        *events[0].as_ref().expect("no error"),
        graphql_value!({ "updates": { "__typename": "Resync", "reason": "LAGGED" } })
    );
}

#[tokio::test]
async fn a_cluster_the_session_cannot_see_is_never_subscribable() {
    let state = two_clusters();
    seed(state.cluster("payments").expect("payments cluster"));
    let context = ctx_with(&state, granted(vec![(Role::Viewer, only(&["local"]))]));

    assert_eq!(
        subscribe_codes(
            &context,
            r#"subscription { updates(cluster: "payments") { __typename } }"#
        )
        .await,
        vec!["UNKNOWN_CLUSTER"]
    );
}

#[tokio::test]
async fn a_session_that_expires_mid_stream_terminates_it() {
    let state = seeded();
    let store = Arc::clone(state.cluster("local").expect("local cluster"));
    let context = GraphQlContext {
        guard: SessionGuard::expired(),
        ..ctx(&state)
    };

    let events = updates(
        &context,
        r#"subscription { updates(cluster: "local") { __typename } }"#,
        move || store.bus.publish(tick(&[("orders.created", 1.0)])),
        2,
    )
    .await;

    assert_eq!(events.len(), 1);
    assert!(
        events[0]
            .as_ref()
            .expect_err("the stream must fail")
            .contains("session is no longer valid")
    );
}

// ------------------------------------------------------------ routing

#[tokio::test]
async fn the_subscription_route_is_wired_with_the_session_extensions() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt as _;

    // A plain GET is not an upgrade, so a wired route rejects it as a bad
    // request. A missing route would be 404 and a missing extension a 500.
    let response = crate::app::router(seeded())
        .oneshot(
            Request::builder()
                .uri("/graphql")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}
