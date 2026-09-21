use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use axum::middleware::{self, Next};
use axum::response::Response;
use foldhash::HashMap;
use futures::StreamExt as _;
use serde_json::Value;
use tower::ServiceExt as _;

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

fn with(sessions: Vec<FakeCluster>) -> AppState {
    AppState::new(Arc::new(SessionSet::from_sessions(sessions)))
}

fn state() -> AppState {
    with(vec![FakeCluster::local()])
}

fn two_clusters() -> AppState {
    with(vec![FakeCluster::local(), FakeCluster::named("payments")])
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

fn api(state: AppState, access: EffectiveAccess, guard: SessionGuard) -> Router {
    super::router()
        .layer(middleware::from_fn(
            move |mut request: Request<Body>, next: Next| {
                let access = access.clone();
                let guard = guard.clone();
                async move {
                    request.extensions_mut().insert(access);
                    request.extensions_mut().insert(guard);
                    next.run(request).await
                }
            },
        ))
        .with_state(state)
}

async fn call(
    state: &AppState,
    path: &str,
    access: EffectiveAccess,
    guard: SessionGuard,
) -> (StatusCode, Value) {
    let response = api(state.clone(), access, guard)
        .oneshot(
            Request::builder()
                .uri(path)
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    let json = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).unwrap_or_else(|error| {
            panic!(
                "body is not json ({error}): {}",
                String::from_utf8_lossy(&bytes)
            )
        })
    };
    (status, json)
}

async fn ok(state: &AppState, path: &str) -> Value {
    ok_as(state, path, EffectiveAccess::Unrestricted).await
}

async fn ok_as(state: &AppState, path: &str, access: EffectiveAccess) -> Value {
    let (status, json) = call(state, path, access, SessionGuard::open()).await;
    assert_eq!(status, StatusCode::OK, "{path} {json}");
    json
}

async fn failure(state: &AppState, path: &str, access: EffectiveAccess) -> (StatusCode, String) {
    let (status, json) = call(state, path, access, SessionGuard::open()).await;
    let code = json["code"]
        .as_str()
        .unwrap_or_else(|| panic!("error has no code: {json}"))
        .to_owned();
    (status, code)
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
    let data = ok(&state, "/api/whoami").await;

    assert_eq!(data["subject"], Value::Null);
    assert_eq!(data["clusters"][0]["cluster"], "local");
    assert_eq!(
        data["clusters"][0]["roles"],
        serde_json::json!([]),
        "no role table decided this"
    );
    assert_eq!(
        data["clusters"][0]["privileges"],
        serde_json::json!(["RECORDS", "CONFIGS", "SCHEMA_TEXT", "ACLS"])
    );
    assert_eq!(data["clusters"][1]["cluster"], "payments");
}

#[tokio::test]
async fn whoami_resolves_each_cluster_against_its_own_grant() {
    let state = two_clusters();
    let access = granted(vec![admin(only(&["local"])), viewer(only(&["payments"]))]);
    let data = ok_as(&state, "/api/whoami", access).await;
    let clusters = data["clusters"].as_array().expect("clusters");

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
    let access = granted(vec![
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
    ]);
    let data = ok_as(&state, "/api/whoami", access).await;
    let local = &data["clusters"][0];

    assert_eq!(local["roles"], serde_json::json!(["auditor", "operator"]));
    assert_eq!(
        local["privileges"],
        serde_json::json!(["RECORDS", "CONFIGS", "SCHEMA_TEXT", "ACLS"])
    );
}

#[tokio::test]
async fn whoami_omits_clusters_the_session_cannot_see() {
    let state = two_clusters();
    let data = ok_as(
        &state,
        "/api/whoami",
        granted(vec![viewer(only(&["payments"]))]),
    )
    .await;

    assert_eq!(
        data["clusters"],
        serde_json::json!([{
            "cluster": "payments",
            "roles": ["viewer"],
            "privileges": []
        }])
    );
}

#[tokio::test]
async fn an_invisible_cluster_is_not_found_rather_than_forbidden() {
    let state = two_clusters();
    let (status, code) = failure(
        &state,
        "/api/clusters/payments",
        granted(vec![admin(only(&["local"]))]),
    )
    .await;

    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(code, "UNKNOWN_CLUSTER");
}

#[tokio::test]
async fn a_cluster_nobody_configured_is_not_found() {
    let (status, code) = failure(
        &state(),
        "/api/clusters/nope",
        EffectiveAccess::Unrestricted,
    )
    .await;

    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(code, "UNKNOWN_CLUSTER");
}

#[tokio::test]
async fn a_visible_cluster_without_the_privilege_reads_as_forbidden() {
    let state = seeded();
    for path in [
        "/api/clusters/local/topics/orders.created/configs",
        "/api/clusters/local/brokers/1/configs",
        "/api/clusters/local/acls",
        "/api/clusters/local/subjects/orders.created-value",
        "/api/clusters/local/topics/orders.created/records",
    ] {
        let (status, code) = failure(&state, path, viewer_everywhere()).await;
        assert_eq!(
            (status, code.as_str()),
            (StatusCode::FORBIDDEN, "FORBIDDEN"),
            "{path}"
        );
    }
}

#[tokio::test]
async fn unprivileged_projections_stay_open_to_a_viewer() {
    let state = seeded();
    let access = viewer_everywhere();

    let topics = ok_as(&state, "/api/clusters/local/topics", access.clone()).await;
    let groups = ok_as(&state, "/api/clusters/local/groups", access.clone()).await;
    let brokers = ok_as(&state, "/api/clusters/local/brokers", access.clone()).await;
    let subjects = ok_as(&state, "/api/clusters/local/subjects", access).await;

    assert_eq!(topics["total"], 2);
    assert_eq!(groups["total"], 1);
    assert_eq!(brokers[0]["id"], 1);
    assert_eq!(subjects["rows"][0]["subject"], "orders.created-value");
}

#[tokio::test]
async fn sixty_four_bit_counters_cross_the_wire_as_strings() {
    let state = state();
    let store = state.cluster("local").expect("local cluster");
    let huge = 9_007_199_254_740_993_i64;

    store.topology.commit(Arc::new(topology(
        vec![topic("wide", vec![partition(0, vec![1], vec![1])])],
        Vec::new(),
    )));
    store
        .watermarks
        .commit(Arc::new(watermarks(at(1_000), &[("wide", 0, 0, huge)])));

    let data = ok(&state, "/api/clusters/local/topics").await;
    let row = &data["rows"][0];

    assert_eq!(row["retainedMessages"], huge.to_string());
    assert_eq!(row["producedTotal"], huge.to_string());
}

#[tokio::test]
async fn topic_rows_project_counts_and_configs_without_touching_the_broker() {
    let (state, session) = seeded_with(FakeCluster::local());
    let data = ok(&state, "/api/clusters/local/topics?sort=NAME").await;
    let rows = &data["rows"];

    assert_eq!(data["total"], 2);
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

    let data = ok(&state, "/api/clusters/local/topics?sort=NAME").await;
    let rows = &data["rows"];

    assert_eq!(rows[0]["name"], "orders.created");
    assert_eq!(rows[0]["rate"], 13.5);
    assert_eq!(rows[1]["name"], "payments.settled");
    assert_eq!(rows[1]["rate"], 0.0);
}

#[tokio::test]
async fn topic_rows_filter_by_name_before_paging() {
    let state = seeded();
    let data = ok(&state, "/api/clusters/local/topics?contains=PAY").await;

    assert_eq!(data["total"], 1);
    assert_eq!(data["rows"][0]["name"], "payments.settled");
}

#[tokio::test]
async fn topic_rows_page_by_key_and_report_the_unpaged_total() {
    let state = seeded();
    let first = ok(&state, "/api/clusters/local/topics?limit=1").await;

    assert_eq!(first["total"], 2);
    assert_eq!(first["rows"][0]["name"], "orders.created");
    assert_eq!(first["nextCursor"], "orders.created");

    let second = ok(
        &state,
        "/api/clusters/local/topics?limit=1&after=orders.created",
    )
    .await;

    assert_eq!(second["rows"][0]["name"], "payments.settled");
    assert_eq!(second["nextCursor"], Value::Null);
}

#[tokio::test]
async fn topic_rows_sort_descending_on_the_requested_column() {
    let state = seeded();
    let data = ok(
        &state,
        "/api/clusters/local/topics?sort=RETAINED_MESSAGES&desc=true",
    )
    .await;

    assert_eq!(data["rows"][0]["name"], "orders.created");
    assert_eq!(data["rows"][1]["name"], "payments.settled");
}

#[tokio::test]
async fn group_rows_join_commits_against_watermarks() {
    let state = seeded();
    let data = ok(&state, "/api/clusters/local/groups").await;
    let row = &data["rows"][0];

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
    let data = ok(&state, "/api/clusters/local/brokers").await;

    assert_eq!(data[0]["host"], "localhost");
    assert_eq!(data[0]["partitionCount"], 3);
    assert_eq!(data[0]["leaderCount"], 3);
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

    let topic = ok(&state, "/api/clusters/local/topics/orders.created").await;

    assert_eq!(topic["replicationFactor"], 2);
    assert_eq!(topic["underReplicated"], true);
    assert_eq!(topic["rate"], 0.0);
    assert_eq!(topic["retentionMs"], "0");
    assert_eq!(topic["cleanupPolicy"], "DELETE");
    assert_eq!(topic["partitions"][0]["underReplicated"], false);
    assert_eq!(topic["partitions"][1]["underReplicated"], true);
    assert_eq!(topic["partitions"][1]["leader"], -1);
}

#[tokio::test]
async fn a_missing_topic_is_not_found() {
    let (status, code) = failure(
        &seeded(),
        "/api/clusters/local/topics/ghost",
        EffectiveAccess::Unrestricted,
    )
    .await;

    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(code, "UNKNOWN_TOPIC");
}

#[tokio::test]
async fn topic_groups_report_lag_on_that_topic_alone() {
    let state = seeded();
    let data = ok(&state, "/api/clusters/local/topics/orders.created/groups").await;

    assert_eq!(data[0]["id"], "order-processor");
    assert_eq!(data[0]["lagOnTopic"], "15");
}

#[tokio::test]
async fn opening_a_group_registers_interest_so_its_offsets_poll_faster() {
    let state = seeded();
    let store = state.cluster("local").expect("local cluster");
    assert!(!store.interest.is_hot("order-processor"));

    let group = ok(&state, "/api/clusters/local/groups/order-processor").await;

    assert_eq!(group["totalLag"], "15");
    assert_eq!(group["members"][0]["clientId"], "c1");
    assert_eq!(group["offsets"][0]["currentOffset"], "90");
    assert_eq!(group["offsets"][0]["endOffset"], "100");
    assert_eq!(group["offsets"][0]["lag"], "10");
    assert!(store.interest.is_hot("order-processor"));
}

#[tokio::test]
async fn a_group_id_with_a_slash_is_one_resource() {
    let state = state();
    let store = state.cluster("local").expect("local cluster");
    store.topology.commit(Arc::new(topology(
        vec![topic(
            "orders.created",
            vec![partition(0, vec![1], vec![1])],
        )],
        vec![group("billing/nightly", "orders.created", vec![0])],
    )));

    let group = ok(&state, "/api/clusters/local/groups/billing/nightly").await;

    assert_eq!(group["id"], "billing/nightly");
}

#[tokio::test]
async fn topic_configs_come_from_the_lane_not_the_broker() {
    let (state, session) = seeded_with(FakeCluster::local());
    let configs = ok(&state, "/api/clusters/local/topics/orders.created/configs").await;

    assert_eq!(configs[0]["name"], "cleanup.policy");
    assert_eq!(configs[0]["value"], "compact");
    assert_eq!(configs[0]["source"], "DYNAMIC_TOPIC_CONFIG");
    assert_eq!(session.calls().topic_configs(), 0);
}

#[tokio::test]
async fn topic_configs_for_an_unknown_topic_are_an_error() {
    let (status, code) = failure(
        &seeded(),
        "/api/clusters/local/topics/ghost/configs",
        EffectiveAccess::Unrestricted,
    )
    .await;

    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(code, "UNKNOWN_TOPIC");
}

#[tokio::test]
async fn broker_configs_stay_live_because_no_lane_sweeps_them() {
    let state = seeded();
    let configs = ok(&state, "/api/clusters/local/brokers/1/configs").await;

    assert_eq!(configs[0]["name"], "log.retention.hours");
}

#[tokio::test]
async fn acls_stay_live_and_carry_the_authorizer_state() {
    let state = seeded();
    let acls = ok(&state, "/api/clusters/local/acls").await;

    assert_eq!(acls["authorizer"], "ENABLED");
    assert!(!acls["bindings"].as_array().expect("bindings").is_empty());
}

#[tokio::test]
async fn a_schema_body_is_fetched_on_demand_rather_than_kept_in_the_lane() {
    let state = seeded();
    let subject = ok(&state, "/api/clusters/local/subjects/orders.created-value").await;

    assert_eq!(subject["subject"], "orders.created-value");
    assert_eq!(subject["type"], "AVRO");
    assert!(
        subject["schema"]
            .as_str()
            .expect("schema body")
            .contains("orderId")
    );
}

#[tokio::test]
async fn an_unknown_subject_is_a_typed_error() {
    let (status, code) = failure(
        &seeded(),
        "/api/clusters/local/subjects/ghost-value",
        EffectiveAccess::Unrestricted,
    )
    .await;

    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(code, "UNKNOWN_SUBJECT");
}

#[tokio::test]
async fn records_are_read_live_through_the_scan_path() {
    let state = seeded();
    let data = ok(
        &state,
        "/api/clusters/local/topics/orders.created/records?limit=3",
    )
    .await;
    let records = data["records"].as_array().expect("records");

    assert_eq!(records.len(), 3);
    assert_eq!(records[0]["topic"], "orders.created");
    assert_eq!(records[0]["sizeBytes"], "24");
    assert_eq!(records[0]["compression"], "NONE");
    records[0]["timestamp"]
        .as_str()
        .expect("timestamp")
        .parse::<jiff::Timestamp>()
        .expect("RFC 3339");
}

#[tokio::test]
async fn records_accept_rfc3339_timestamp_bounds() {
    let state = seeded();
    let data = ok(
        &state,
        "/api/clusters/local/topics/orders.created/records?order=OLDEST&from=2023-11-14T22:13:23Z&to=2023-11-14T22:13:25Z",
    )
    .await;
    let records = data["records"].as_array().expect("records");
    let offsets: Vec<_> = records
        .iter()
        .map(|record| record["offset"].as_str().expect("offset"))
        .collect();

    assert_eq!(offsets, ["3", "4", "5"]);
    assert_eq!(records[0]["timestamp"], "2023-11-14T22:13:23Z");
    assert_eq!(records[2]["timestamp"], "2023-11-14T22:13:25Z");
}

#[tokio::test]
async fn an_inverted_record_range_is_rejected() {
    let (status, code) = failure(
        &seeded(),
        "/api/clusters/local/topics/orders.created/records?from=2023-11-14T22:13:25Z&to=2023-11-14T22:13:23Z",
        EffectiveAccess::Unrestricted,
    )
    .await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(code, "INVERTED_TIMESTAMP_RANGE");
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
        &state,
        "/api/clusters/local/topics/orders.created/records?limit=3",
    )
    .await;
    let records = data["records"].as_array().expect("records");

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
    let path = "/api/clusters/local/topics/orders.created/records?limit=2";

    assert_eq!(
        ok(&plain, path).await["obfuscated"],
        serde_json::json!(false)
    );
    assert_eq!(
        ok(&protected, path).await["obfuscated"],
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
        &state,
        "/api/clusters/local/topics/orders.created/records?limit=3",
    )
    .await;

    assert_eq!(data["obfuscated"], serde_json::json!(true));
    let records = data["records"].as_array().expect("records");
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
        &state,
        "/api/clusters/local/topics/orders.created/records?limit=3&contains=4111",
    )
    .await;
    let visible = ok(
        &state,
        "/api/clusters/local/topics/orders.created/records?limit=3&contains=ord_1",
    )
    .await;

    assert!(
        hidden["records"].as_array().expect("records").is_empty(),
        "a filter must not answer questions about an obfuscated field"
    );
    assert_eq!(visible["records"].as_array().expect("records").len(), 1);
}

#[tokio::test]
async fn cluster_health_reports_per_lane_freshness_and_counts() {
    let state = seeded();
    let health = &ok(&state, "/api/clusters").await[0];

    assert_eq!(health["cluster"], "local");
    assert_eq!(health["ready"], true);
    assert_eq!(health["topology"]["healthy"], true);
    health["topology"]["updatedAt"]
        .as_str()
        .expect("updatedAt")
        .parse::<jiff::Timestamp>()
        .expect("RFC 3339");
    assert_eq!(health["topicCount"], 2);
    assert_eq!(health["partitionCount"], 3);
    assert_eq!(health["groupCount"], 1);
    assert_eq!(health["brokerCount"], 1);
    assert_eq!(health["subjectCount"], 1);
    assert_eq!(health["underReplicatedPartitions"], 0);
}

#[tokio::test]
async fn a_cluster_with_no_commits_yet_is_visible_but_not_ready() {
    let health = &ok(&state(), "/api/clusters").await[0];

    assert_eq!(health["cluster"], "local");
    assert_eq!(health["ready"], false);
    assert_eq!(health["topicCount"], 0);
}

#[tokio::test]
async fn search_is_answered_from_the_prebuilt_index() {
    let state = seeded();
    let data = ok(&state, "/api/clusters/local/search?q=orders").await;
    let kinds: BTreeSet<&str> = data
        .as_array()
        .expect("hits")
        .iter()
        .map(|hit| hit["kind"].as_str().expect("kind"))
        .collect();

    assert!(kinds.contains("TOPIC"), "{kinds:?}");
    assert!(kinds.contains("SUBJECT"), "{kinds:?}");
}

async fn open_updates(
    state: &AppState,
    path: &str,
    access: EffectiveAccess,
    guard: SessionGuard,
) -> Response {
    api(state.clone(), access, guard)
        .oneshot(
            Request::builder()
                .uri(path)
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response")
}

async fn read_events(response: Response, count: usize) -> Vec<Value> {
    assert_eq!(response.status(), StatusCode::OK, "updates did not open");
    let mut stream = response.into_body().into_data_stream();
    let mut buffer = String::new();
    let mut events = Vec::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);

    while events.len() < count {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        assert!(
            !remaining.is_zero(),
            "timed out with {events:?} buffered {buffer}"
        );
        let chunk = tokio::time::timeout(remaining, stream.next())
            .await
            .expect("timeout")
            .unwrap_or_else(|| panic!("stream ended after {} events: {buffer}", events.len()))
            .expect("frame");
        buffer.push_str(&String::from_utf8_lossy(&chunk));
        while let Some(index) = buffer.find("\n\n") {
            let frame = buffer[..index].to_owned();
            buffer.drain(..index + 2);
            let Some(data) = frame.lines().find_map(|line| line.strip_prefix("data:")) else {
                continue;
            };
            events.push(
                serde_json::from_str(data.trim())
                    .unwrap_or_else(|error| panic!("sse data is not json ({error}): {data}")),
            );
        }
    }
    events
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
    let response = open_updates(
        &state,
        "/api/clusters/local/updates",
        EffectiveAccess::Unrestricted,
        SessionGuard::open(),
    )
    .await;
    store
        .bus
        .publish(tick(&[("orders.created", 10.0), ("payments.settled", 2.0)]));
    let events = read_events(response, 1).await;

    assert_eq!(events[0]["type"], "watermarks");
    assert_eq!(
        events[0]["topics"],
        serde_json::json!([
            { "topic": "orders.created", "rate": 10.0 },
            { "topic": "payments.settled", "rate": 2.0 }
        ])
    );
}

#[tokio::test]
async fn a_topic_scoped_subscriber_pays_only_for_its_own_topic() {
    let state = seeded();
    let store = Arc::clone(state.cluster("local").expect("local cluster"));
    let response = open_updates(
        &state,
        "/api/clusters/local/updates?topic=orders.created",
        EffectiveAccess::Unrestricted,
        SessionGuard::open(),
    )
    .await;
    store
        .bus
        .publish(tick(&[("orders.created", 10.0), ("payments.settled", 2.0)]));
    let events = read_events(response, 1).await;

    assert_eq!(
        events[0]["topics"],
        serde_json::json!([{ "topic": "orders.created", "rate": 10.0 }])
    );
}

#[tokio::test]
async fn an_event_outside_the_scope_never_reaches_the_socket() {
    let state = seeded();
    let store = Arc::clone(state.cluster("local").expect("local cluster"));
    let response = open_updates(
        &state,
        "/api/clusters/local/updates?topic=payments.settled",
        EffectiveAccess::Unrestricted,
        SessionGuard::open(),
    )
    .await;
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
    let events = read_events(response, 1).await;

    assert_eq!(events[0]["type"], "subjects");
}

#[tokio::test]
async fn an_unscoped_lag_wave_fans_out_one_update_per_group_without_offsets() {
    let state = seeded();
    let store = Arc::clone(state.cluster("local").expect("local cluster"));
    let response = open_updates(
        &state,
        "/api/clusters/local/updates",
        EffectiveAccess::Unrestricted,
        SessionGuard::open(),
    )
    .await;
    store
        .bus
        .publish(wave(&[("order-processor", 15), ("audit", 3)]));
    let events = read_events(response, 2).await;

    assert_eq!(events[0]["type"], "groupLag");
    assert_eq!(events[0]["group"], "order-processor");
    assert_eq!(events[0]["lag"], "15");
    assert_eq!(events[0]["offsets"], serde_json::json!([]));
    assert_eq!(events[1]["group"], "audit");
    assert_eq!(events[1]["lag"], "3");
    assert_eq!(events[1]["offsets"], serde_json::json!([]));
}

#[tokio::test]
async fn a_group_scoped_subscriber_holds_an_interest_lease_for_the_stream() {
    let state = seeded();
    let store = Arc::clone(state.cluster("local").expect("local cluster"));
    let response = open_updates(
        &state,
        "/api/clusters/local/updates?group=order-processor",
        EffectiveAccess::Unrestricted,
        SessionGuard::open(),
    )
    .await;
    store
        .bus
        .publish(wave(&[("order-processor", 15), ("audit", 3)]));
    let mut stream = response.into_body().into_data_stream();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    let chunk = tokio::time::timeout_at(deadline, stream.next())
        .await
        .expect("timeout")
        .expect("frame")
        .expect("bytes");
    let text = String::from_utf8_lossy(&chunk);
    assert!(text.contains("order-processor"), "{text}");
    assert!(store.interest.is_hot("order-processor"));

    drop(stream);
    assert!(!store.interest.is_hot("order-processor"));
}

#[tokio::test]
async fn a_topology_delta_reaches_a_scoped_subscriber_only_when_it_names_its_topic() {
    let state = seeded();
    let store = Arc::clone(state.cluster("local").expect("local cluster"));
    let response = open_updates(
        &state,
        "/api/clusters/local/updates?topic=orders.created",
        EffectiveAccess::Unrestricted,
        SessionGuard::open(),
    )
    .await;
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
    let events = read_events(response, 1).await;

    assert_eq!(events[0]["type"], "topology");
    assert_eq!(events[0]["version"], "5");
    assert_eq!(events[0]["addedTopics"], serde_json::json!([]));
    assert_eq!(
        events[0]["changedTopics"],
        serde_json::json!(["orders.created"])
    );
}

#[tokio::test]
async fn falling_behind_the_bus_asks_the_client_to_refetch_instead_of_dropping_it() {
    let state = seeded();
    let store = Arc::clone(state.cluster("local").expect("local cluster"));
    let response = open_updates(
        &state,
        "/api/clusters/local/updates",
        EffectiveAccess::Unrestricted,
        SessionGuard::open(),
    )
    .await;
    for index in 0..(2 * BUS_CAPACITY) {
        store.bus.publish(tick(&[("orders.created", index as f64)]));
    }
    let events = read_events(response, 1).await;

    assert_eq!(events[0]["type"], "resync");
    assert_eq!(events[0]["reason"], "LAGGED");
}

#[tokio::test]
async fn a_cluster_the_session_cannot_see_is_never_subscribable() {
    let state = two_clusters();
    seed(state.cluster("payments").expect("payments cluster"));
    let (status, code) = failure(
        &state,
        "/api/clusters/payments/updates",
        granted(vec![viewer(only(&["local"]))]),
    )
    .await;

    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(code, "UNKNOWN_CLUSTER");
}

#[tokio::test]
async fn a_session_that_expires_mid_stream_terminates_it() {
    let state = seeded();
    let store = Arc::clone(state.cluster("local").expect("local cluster"));
    let response = open_updates(
        &state,
        "/api/clusters/local/updates",
        EffectiveAccess::Unrestricted,
        SessionGuard::expired(),
    )
    .await;
    store.bus.publish(tick(&[("orders.created", 1.0)]));
    let events = read_events(response, 1).await;

    assert_eq!(events[0]["code"], "SESSION_EXPIRED");
    assert!(
        events[0]["error"]
            .as_str()
            .expect("message")
            .contains("session is no longer valid")
    );
}

#[tokio::test]
async fn the_updates_route_is_wired_with_the_session_extensions() {
    let response = crate::app::router(seeded())
        .oneshot(
            Request::builder()
                .uri("/api/clusters/local/updates")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");

    assert_eq!(response.status(), StatusCode::OK);
    let content_type = response
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("");
    assert!(
        content_type.starts_with("text/event-stream"),
        "{content_type}"
    );
}
