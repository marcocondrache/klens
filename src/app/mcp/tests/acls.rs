use super::*;

#[tokio::test]
async fn acls_list_shows_what_the_http_route_returns() {
    let app = TestApp::local().await;
    let http = app.get("/clusters/local/acls").await.ok();
    let all = http["bindings"].as_array().expect("bindings");
    let keeping = |field: &str, value: &str| -> Vec<Value> {
        all.iter()
            .filter(|acl| acl[field] == value)
            .cloned()
            .collect()
    };

    for (arguments, bindings) in [
        (json!({}), all.clone()),
        (
            json!({ "contains": "ALICE" }),
            keeping("principal", "User:alice"),
        ),
        (json!({ "contains": "10.0.0" }), keeping("host", "10.0.0.1")),
        (
            json!({ "contains": "processor" }),
            keeping("resourceType", "GROUP"),
        ),
        (
            json!({ "resourceType": "GROUP" }),
            keeping("resourceType", "GROUP"),
        ),
        (
            json!({ "operation": "WRITE" }),
            keeping("operation", "WRITE"),
        ),
        (
            json!({ "permission": "DENY" }),
            keeping("permission", "DENY"),
        ),
    ] {
        let listed = structured(&call(&app, "klens_acls_list", arguments.clone()).await);

        assert_eq!(listed["status"], http["status"], "{arguments}");
        assert_eq!(listed["bindings"], json!(bindings), "{arguments}");
        assert_eq!(listed["notice"], CLIENT_VALUES_NOTICE);
    }
    assert_eq!(app.cluster().calls(Api::Acls), 0);
}

#[tokio::test]
async fn acls_list_says_why_a_cluster_shows_no_bindings() {
    let disabled = FakeCluster::named("open");
    disabled.set_acls(AclListing::Disabled);
    let denied = FakeCluster::named("locked");
    denied.set_acls(AclListing::Denied);
    let unread = FakeCluster::named("down");
    unread.fail(Api::Acls, "broker down");
    let app = TestApp::of([disabled, denied, unread]).ingested().await;
    let list =
        async |cluster: &str| call(&app, "klens_acls_list", json!({ "cluster": cluster })).await;

    let open = structured(&list("open").await);
    let locked = structured(&list("locked").await);
    let down = refusal(&list("down").await);

    assert_eq!(
        open,
        json!({
            "status": "DISABLED",
            "bindings": [],
            "showing": "0 of 0",
            "notice": CLIENT_VALUES_NOTICE,
        })
    );
    assert_eq!(locked["status"], "DENIED");
    assert_eq!(down["code"], "NOT_READY");
}
