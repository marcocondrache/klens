use serde_json::Value;

use crate::app::auth::access::{ClusterScope, Privilege, PrivilegeSet};

use super::super::harness::{admin, granted, ok, ok_as, only, state, two_clusters, viewer};

#[tokio::test]
async fn whoami_reports_no_subject_when_auth_is_disabled() {
    let state = two_clusters();
    let data = ok(&state, "/whoami").await;

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
    let data = ok_as(&state, "/whoami", access).await;
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
    let data = ok_as(&state, "/whoami", access).await;
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
        "/whoami",
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
