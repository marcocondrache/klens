use serde_json::{Value, json};

use crate::app::auth::access::{Privilege, PrivilegeSet};
use crate::testing::{FakeCluster, TestApp, access, admin, role, viewer};

fn two_clusters() -> TestApp {
    TestApp::of([FakeCluster::local(), FakeCluster::named("payments")]).build()
}

#[tokio::test]
async fn whoami_reports_no_subject_when_auth_is_disabled() {
    let whoami = two_clusters().get("/whoami").await.ok();

    assert_eq!(whoami["subject"], Value::Null);
    assert_eq!(whoami["clusters"][0]["cluster"], "local");
    assert_eq!(
        whoami["clusters"][0]["roles"],
        json!([]),
        "no role table decided this"
    );
    assert_eq!(
        whoami["clusters"][0]["privileges"],
        json!([
            "RECORDS",
            "TOPIC_CONFIGS",
            "BROKER_CONFIGS",
            "SCHEMA_TEXT",
            "ACLS",
            "CREATE_TOPICS",
            "DELETE_TOPICS",
            "ALTER_TOPIC_CONFIGS",
            "ADD_PARTITIONS",
            "DELETE_RECORDS",
            "PRODUCE",
            "RESET_OFFSETS",
            "DELETE_OFFSETS",
            "DELETE_GROUPS",
            "REGISTER_SCHEMAS",
            "SET_COMPATIBILITY",
            "DELETE_SCHEMAS",
            "CREATE_ACLS",
            "DELETE_ACLS",
            "ALTER_QUOTAS",
            "SET_SCRAM_CREDENTIALS",
            "DELETE_SCRAM_CREDENTIALS",
            "ALTER_BROKER_CONFIGS"
        ])
    );
    assert_eq!(whoami["clusters"][1]["cluster"], "payments");
}

#[tokio::test]
async fn whoami_resolves_each_cluster_against_its_own_grant() {
    let whoami = two_clusters()
        .with_access(access([admin().on(&["local"]), viewer().on(&["payments"])]))
        .get("/whoami")
        .await
        .ok();
    let clusters = whoami["clusters"].as_array().expect("clusters");

    assert_eq!(clusters.len(), 2);
    assert_eq!(clusters[0]["cluster"], "local");
    assert_eq!(clusters[0]["roles"], json!(["admin"]));
    assert_eq!(
        clusters[0]["privileges"],
        json!([
            "RECORDS",
            "TOPIC_CONFIGS",
            "BROKER_CONFIGS",
            "SCHEMA_TEXT",
            "ACLS",
            "CREATE_TOPICS",
            "DELETE_TOPICS",
            "ALTER_TOPIC_CONFIGS",
            "ADD_PARTITIONS",
            "DELETE_RECORDS",
            "PRODUCE",
            "RESET_OFFSETS",
            "DELETE_OFFSETS",
            "DELETE_GROUPS",
            "REGISTER_SCHEMAS",
            "SET_COMPATIBILITY",
            "DELETE_SCHEMAS",
            "CREATE_ACLS",
            "DELETE_ACLS",
            "ALTER_QUOTAS",
            "SET_SCRAM_CREDENTIALS",
            "DELETE_SCRAM_CREDENTIALS",
            "ALTER_BROKER_CONFIGS"
        ])
    );
    assert_eq!(clusters[1]["cluster"], "payments");
    assert_eq!(clusters[1]["roles"], json!(["viewer"]));
    assert_eq!(clusters[1]["privileges"], json!([]));
}

#[tokio::test]
async fn whoami_unions_the_privileges_of_every_role_covering_a_cluster() {
    let whoami = TestApp::of([FakeCluster::local()])
        .build()
        .with_access(access([
            role(
                "operator",
                PrivilegeSet::from_privileges([Privilege::Records, Privilege::TopicConfigs]),
            ),
            role(
                "auditor",
                PrivilegeSet::from_privileges([Privilege::Acls, Privilege::SchemaText]),
            )
            .on(&["local"]),
        ]))
        .get("/whoami")
        .await
        .ok();
    let local = &whoami["clusters"][0];

    assert_eq!(local["roles"], json!(["auditor", "operator"]));
    assert_eq!(
        local["privileges"],
        json!(["RECORDS", "TOPIC_CONFIGS", "SCHEMA_TEXT", "ACLS"])
    );
}

#[tokio::test]
async fn whoami_omits_clusters_the_session_cannot_see() {
    let whoami = two_clusters()
        .with_access(access([viewer().on(&["payments"])]))
        .get("/whoami")
        .await
        .ok();

    assert_eq!(
        whoami["clusters"],
        json!([{
            "cluster": "payments",
            "roles": ["viewer"],
            "privileges": [],
            "writable": false
        }])
    );
}

#[tokio::test]
async fn whoami_reports_which_clusters_accept_changes() {
    let whoami = TestApp::of([FakeCluster::local(), FakeCluster::named("payments")])
        .writable(&["payments"])
        .build()
        .get("/whoami")
        .await
        .ok();

    assert_eq!(whoami["clusters"][0]["cluster"], "local");
    assert_eq!(whoami["clusters"][0]["writable"], false);
    assert_eq!(whoami["clusters"][1]["cluster"], "payments");
    assert_eq!(whoami["clusters"][1]["writable"], true);
}
