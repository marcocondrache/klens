use std::collections::BTreeSet;
use std::sync::Arc;

use foldhash::HashMap;

use futures::StreamExt as _;
use juniper::{
    DefaultScalarValue, ExecutionError, Value, Variables, execute, graphql_value,
    resolve_into_stream,
};

use crate::AppState;
use crate::app::auth::SessionGuard;
use crate::app::auth::access::{ClusterScope, EffectiveAccess, Grant, Privilege, PrivilegeSet};
use crate::kafka::store::bus::BUS_CAPACITY;
use crate::kafka::store::fixtures::{
    at, config, group, offline_partition, offsets, partition, subject, topic, topology, watermarks,
};
use crate::kafka::store::{
    Change, ClusterStore, ConfigTable, ConfigsDelta, GroupLagUpdate, GroupOffsetsWave, Interner,
    OffsetTable, SubjectTable, SubjectsDelta, TopicRate, TopologyDelta, WatermarksTick,
};
use crate::kafka::{FakeCluster, SessionSet, card_record};

use super::context::GraphQlContext;
use super::schema;

fn with(sessions: Vec<FakeCluster>) -> AppState {
    AppState::new(Arc::new(SessionSet::from_sessions(sessions)))
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

fn granted(grants: Vec<(&str, PrivilegeSet, ClusterScope)>) -> EffectiveAccess {
    EffectiveAccess::Granted(
        grants
            .into_iter()
            .map(|(role_name, privileges, scope)| Grant {
                role_name: Arc::from(role_name),
                privileges,
                scope,
            })
            .collect(),
    )
}

fn admin(scope: ClusterScope) -> (&'static str, PrivilegeSet, ClusterScope) {
    ("admin", PrivilegeSet::ALL, scope)
}

fn viewer(scope: ClusterScope) -> (&'static str, PrivilegeSet, ClusterScope) {
    ("viewer", PrivilegeSet::NONE, scope)
}

fn viewer_everywhere() -> EffectiveAccess {
    granted(vec![viewer(ClusterScope::All)])
}

async fn run(
    context: &GraphQlContext,
    query: &str,
) -> (serde_json::Value, Vec<ExecutionError<DefaultScalarValue>>) {
    let (data, errors) = execute(query, None, &schema(), &Variables::new(), context)
        .await
        .expect("query is valid against the schema");
    (
        serde_json::to_value(data).expect("data is serializable"),
        errors,
    )
}

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

async fn codes(context: &GraphQlContext, query: &str) -> Vec<String> {
    run(context, query).await.1.iter().map(code_of).collect()
}

fn seed(store: &ClusterStore) {
    store.topology.commit(Arc::new(topology(
        vec![
            topic(
                "orders.created",
                vec![
                    partition(0, vec![1], vec![1]),
                    partition(1, vec![1], vec![1]),
                ],
            ),
            topic("payments.settled", vec![partition(0, vec![1], vec![1])]),
        ],
        vec![group("order-processor", "orders.created", vec![0, 1])],
    )));

    store.watermarks.commit(Arc::new(watermarks(
        at(1_000),
        &[
            ("orders.created", 0, 0, 100),
            ("orders.created", 1, 10, 60),
            ("payments.settled", 0, 0, 5),
        ],
    )));

    store.offsets.commit(Arc::new(OffsetTable {
        groups: HashMap::from_iter([(
            Arc::from("order-processor"),
            Arc::new(offsets(
                at(1_000),
                &[("orders.created", 0, 90), ("orders.created", 1, 55)],
            )),
        )]),
    }));

    store.configs.commit(Arc::new(ConfigTable {
        topics: HashMap::from_iter([(
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

fn seeded_with(session: FakeCluster) -> (AppState, FakeCluster) {
    let state = with(vec![session.clone()]);
    seed(state.cluster("local").expect("local cluster"));
    (state, session)
}

#[tokio::test]
async fn whoami_reports_no_subject_when_auth_is_disabled() {
    let state = two_clusters();
    let data = ok(
        &ctx(&state),
        "{ whoami { subject clusters { cluster roles privileges } } }",
    )
    .await;

    assert_eq!(data["whoami"]["subject"], serde_json::Value::Null);
    assert_eq!(data["whoami"]["clusters"][0]["cluster"], "local");
    assert_eq!(
        data["whoami"]["clusters"][0]["roles"],
        serde_json::json!([]),
        "no role table decided this"
    );
    assert_eq!(
        data["whoami"]["clusters"][0]["privileges"],
        serde_json::json!(["RECORDS", "CONFIGS", "SCHEMA_TEXT", "ACLS"])
    );
    assert_eq!(data["whoami"]["clusters"][1]["cluster"], "payments");
}

#[tokio::test]
async fn whoami_resolves_each_cluster_against_its_own_grant() {
    let state = two_clusters();
    let context = ctx_with(
        &state,
        granted(vec![admin(only(&["local"])), viewer(only(&["payments"]))]),
    );

    let data = ok(
        &context,
        "{ whoami { clusters { cluster roles privileges } } }",
    )
    .await;
    let clusters = data["whoami"]["clusters"].as_array().expect("clusters");

    assert_eq!(clusters.len(), 2);
    assert_eq!(clusters[0]["cluster"], "local");
    assert_eq!(clusters[0]["roles"], serde_json::json!(["admin"]));
    assert_eq!(
        clusters[0]["privileges"],
        serde_json::json!(["RECORDS", "CONFIGS", "SCHEMA_TEXT", "ACLS"])
    );

    assert_eq!(clusters[1]["cluster"], "payments");
    assert_eq!(clusters[1]["roles"], serde_json::json!(["viewer"]));
    assert_eq!(clusters[1]["privileges"], serde_json::json!([]));
}

#[tokio::test]
async fn whoami_unions_the_privileges_of_every_role_covering_a_cluster() {
    let state = state();
    let context = ctx_with(
        &state,
        granted(vec![
            (
                "operator",
                PrivilegeSet::from_privileges([Privilege::Records, Privilege::Configs]),
                ClusterScope::All,
            ),
            (
                "auditor",
                PrivilegeSet::from_privileges([Privilege::Acls, Privilege::SchemaText]),
                only(&["local"]),
            ),
        ]),
    );

    let data = ok(
        &context,
        "{ whoami { clusters { cluster roles privileges } } }",
    )
    .await;
    let local = &data["whoami"]["clusters"][0];

    assert_eq!(local["roles"], serde_json::json!(["auditor", "operator"]));
    assert_eq!(
        local["privileges"],
        serde_json::json!(["RECORDS", "CONFIGS", "SCHEMA_TEXT", "ACLS"])
    );
}

#[tokio::test]
async fn whoami_omits_clusters_the_session_cannot_see() {
    let state = two_clusters();
    let context = ctx_with(&state, granted(vec![viewer(only(&["payments"]))]));

    let data = ok(&context, "{ whoami { clusters { cluster } } }").await;

    assert_eq!(
        data["whoami"]["clusters"],
        serde_json::json!([{ "cluster": "payments" }])
    );
}

#[tokio::test]
async fn an_invisible_cluster_reads_as_unknown_not_forbidden() {
    let state = two_clusters();
    let context = ctx_with(&state, granted(vec![admin(only(&["local"]))]));

    assert_eq!(
        codes(&context, r#"{ topicRows(cluster: "payments") { total } }"#).await,
        vec!["UNKNOWN_CLUSTER"]
    );
}

#[tokio::test]
async fn a_cluster_nobody_configured_reads_as_unknown() {
    assert_eq!(
        codes(
            &ctx(&state()),
            r#"{ topicRows(cluster: "nope") { total } }"#
        )
        .await,
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
    assert_eq!(
        data["subjectRows"]["rows"][0]["subject"],
        "orders.created-value"
    );
}

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

    assert_eq!(
        data["topicRows"]["rows"][0]["retainedMessages"],
        huge.to_string()
    );
    assert_eq!(
        data["topicRows"]["rows"][0]["producedTotal"],
        huge.to_string()
    );
}

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
    assert_eq!(rows[0]["retainedMessages"], "150");
    assert_eq!(rows[0]["cleanupPolicy"], "COMPACT");
    assert_eq!(rows[0]["retentionMs"], "604800000");
    assert_eq!(rows[0]["groupCount"], 1);
    assert_eq!(session.calls().metadata(), 0);
}

#[tokio::test]
async fn topic_rows_expose_the_latest_rate() {
    let state = seeded();
    let store = state.cluster("local").expect("local cluster");
    let topic: Arc<str> = Arc::from("orders.created");
    store.rates.set(&topic, 12.5);
    store.rates.set(&topic, 13.5);

    let data = ok(
        &ctx(&state),
        r#"{ topicRows(cluster: "local", sort: { field: NAME }) {
            rows { name rate }
        } }"#,
    )
    .await;
    let rows = &data["topicRows"]["rows"];

    assert_eq!(rows[0]["name"], "orders.created");
    assert_eq!(rows[0]["rate"], 13.5);
    assert_eq!(rows[1]["name"], "payments.settled");
    assert_eq!(rows[1]["rate"], 0.0);
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

#[tokio::test]
async fn topic_detail_flags_under_replication_per_partition() {
    let state = state();
    let store = state.cluster("local").expect("local cluster");
    store.topology.commit(Arc::new(topology(
        vec![topic(
            "orders.created",
            vec![
                partition(0, vec![1, 2], vec![1, 2]),
                offline_partition(1, vec![1, 2]),
            ],
        )],
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
    assert!(
        !data["acls"]["bindings"]
            .as_array()
            .expect("bindings")
            .is_empty()
    );
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
async fn an_obfuscated_topic_serves_tokens_instead_of_payloads() {
    let pan = "4111111111111111";
    let records = (0..3).map(|offset| card_record(offset, pan)).collect();
    let state = seeded_with(
        FakeCluster::local()
            .with_orders_records(records)
            .with_obfuscation(
                "
                secret: 0123456789abcdef0123456789abcdef
                rules:
                  - topics: ['orders.*']
                    headers: ['x-user-id']
                    fields:
                      - path: card.number
                        strategy: hash
                      - path: card.cvv
                        strategy: drop
                ",
            ),
    )
    .0;

    let data = ok(
        &ctx(&state),
        r#"{ records(cluster: "local", query: { topic: "orders.created", limit: 3 }) {
            records { key value headers { key value } }
        } }"#,
    )
    .await;

    let records = data["records"]["records"].as_array().expect("records");
    assert_eq!(records.len(), 3);
    for record in records {
        let value = record["value"].as_str().expect("value");
        assert!(value.contains("\"kx:"), "{value}");
        assert!(!value.contains(pan), "{value}");
        assert!(!value.contains("cvv"), "dropped fields vanish: {value}");
        assert!(
            record["key"].as_str().expect("key").starts_with("ord_"),
            "no rule names the key: {record}"
        );
        assert_eq!(record["headers"][0]["value"], "***");
    }
}

#[tokio::test]
async fn a_page_says_whether_a_rule_covers_its_topic() {
    let pan = "4111111111111111";
    let records: Vec<_> = (0..2).map(|offset| card_record(offset, pan)).collect();
    let rules = "
        rules:
          - topics: ['orders.*']
            fields:
              - path: card.number
                strategy: mask
        ";

    let plain = seeded_with(FakeCluster::local().with_orders_records(records.clone())).0;
    let protected = seeded_with(
        FakeCluster::local()
            .with_orders_records(records)
            .with_obfuscation(rules),
    )
    .0;

    let query = r#"{ records(cluster: "local", query: { topic: "orders.created", limit: 2 }) {
        obfuscated records { offset }
    } }"#;

    assert_eq!(
        ok(&ctx(&plain), query).await["records"]["obfuscated"],
        serde_json::json!(false)
    );
    assert_eq!(
        ok(&ctx(&protected), query).await["records"]["obfuscated"],
        serde_json::json!(true)
    );
}

#[tokio::test]
async fn a_pattern_rule_tokens_a_topic_no_registry_ever_decodes() {
    let pan = "4111111111111111";
    let records = (0..3)
        .map(|offset| {
            let mut record = card_record(offset, pan);
            record.value = Some(format!("charged {pan} for ada@example.com"));
            record
        })
        .collect();
    let state = seeded_with(
        FakeCluster::local()
            .with_orders_records(records)
            .with_obfuscation(
                r"
                secret: 0123456789abcdef0123456789abcdef
                rules:
                  - topics: ['orders.*']
                    patterns:
                      - regex: '\d{13,19}'
                        strategy: hash
                      - regex: '[\w.+-]+@[\w-]+\.[\w.]+'
                        strategy: mask
                ",
            ),
    )
    .0;

    let data = ok(
        &ctx(&state),
        r#"{ records(cluster: "local", query: { topic: "orders.created", limit: 3 }) {
            obfuscated records { value }
        } }"#,
    )
    .await;

    assert_eq!(data["records"]["obfuscated"], serde_json::json!(true));
    let records = data["records"]["records"].as_array().expect("records");
    assert_eq!(records.len(), 3);
    for record in records {
        let value = record["value"].as_str().expect("value");
        assert!(value.starts_with("charged kx:"), "{value}");
        assert!(!value.contains(pan), "{value}");
        assert!(value.ends_with("for ***"), "{value}");
    }
}

#[tokio::test]
async fn an_obfuscated_topic_cannot_be_filtered_on_the_cleartext_it_hides() {
    let pan = "4111111111111111";
    let records = (0..3).map(|offset| card_record(offset, pan)).collect();
    let state = seeded_with(
        FakeCluster::local()
            .with_orders_records(records)
            .with_obfuscation(
                "
                secret: 0123456789abcdef0123456789abcdef
                rules:
                  - topics: ['orders.*']
                    fields:
                      - path: card.number
                        strategy: hash
                ",
            ),
    )
    .0;

    let hidden = ok(
        &ctx(&state),
        r#"{ records(cluster: "local", query: {
            topic: "orders.created", limit: 3, filter: { contains: "4111" }
        }) { records { offset } } }"#,
    )
    .await;
    let visible = ok(
        &ctx(&state),
        r#"{ records(cluster: "local", query: {
            topic: "orders.created", limit: 3, filter: { contains: "ord_1" }
        }) { records { offset } } }"#,
    )
    .await;

    assert!(
        hidden["records"]["records"]
            .as_array()
            .expect("records")
            .is_empty(),
        "a filter must not answer questions about an obfuscated field"
    );
    assert_eq!(
        visible["records"]["records"]
            .as_array()
            .expect("records")
            .len(),
        1
    );
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
            ... on WatermarksTick { topics { topic rate } }
        } }"#,
        || {
            store
                .bus
                .publish(tick(&[("orders.created", 10.0), ("payments.settled", 2.0)]))
        },
        1,
    )
    .await;

    let value = events[0].as_ref().expect("no error");
    assert_eq!(
        *value,
        graphql_value!({ "updates": {
            "__typename": "WatermarksTick",
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
        || {
            store
                .bus
                .publish(tick(&[("orders.created", 10.0), ("payments.settled", 2.0)]))
        },
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
        || {
            store
                .bus
                .publish(wave(&[("order-processor", 15), ("audit", 3)]))
        },
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
    let context = ctx_with(&state, granted(vec![viewer(only(&["local"]))]));

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

#[tokio::test]
async fn the_subscription_route_is_wired_with_the_session_extensions() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt as _;

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
