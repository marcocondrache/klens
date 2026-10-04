use axum::body::Body;
use axum::http::{Method, Request, StatusCode, header};

use crate::app::auth::access::{Privilege, PrivilegeSet};
use crate::testing::{FakeCluster, TestApp, access, admin, group, json_request, role};

const ROUTES: &[(&str, Option<Privilege>)] = &[
    ("/whoami", None),
    ("/clusters", None),
    ("/clusters/local", None),
    ("/clusters/local/search?q=orders", None),
    ("/clusters/local/updates", None),
    ("/clusters/local/topics", None),
    ("/clusters/local/topics/orders.created", None),
    ("/clusters/local/topics/orders.created/groups", None),
    (
        "/clusters/local/topics/orders.created/configs",
        Some(Privilege::Configs),
    ),
    (
        "/clusters/local/topics/orders.created/records",
        Some(Privilege::Records),
    ),
    (
        "/clusters/local/topics/orders.created/records/0/1",
        Some(Privilege::Records),
    ),
    (
        "/clusters/local/topics/orders.created/records/tail",
        Some(Privilege::Records),
    ),
    (
        "/clusters/local/topics/orders.created/records/export",
        Some(Privilege::Records),
    ),
    ("/clusters/local/groups", None),
    ("/clusters/local/groups/order-processor", None),
    ("/clusters/local/brokers", None),
    (
        "/clusters/local/brokers/1/configs",
        Some(Privilege::Configs),
    ),
    ("/clusters/local/subjects", None),
    (
        "/clusters/local/subjects/orders.created-value",
        Some(Privilege::SchemaText),
    ),
    ("/clusters/local/acls", Some(Privilege::Acls)),
    ("/clusters/local/quotas", Some(Privilege::Configs)),
    ("/clusters/local/scram-users", Some(Privilege::Acls)),
];

const WRITES: &[(Method, &str, Option<&str>, Privilege)] = &[
    (
        Method::POST,
        "/clusters/local/topics",
        Some(r#"{ "name": "invoices" }"#),
        Privilege::CreateTopics,
    ),
    (
        Method::DELETE,
        "/clusters/local/topics/orders.created",
        None,
        Privilege::DeleteTopics,
    ),
    (
        Method::PATCH,
        "/clusters/local/topics/orders.created/configs",
        Some(r#"{ "reset": ["retention.ms"] }"#),
        Privilege::AlterTopicConfigs,
    ),
    (
        Method::POST,
        "/clusters/local/topics/orders.created/partitions",
        Some(r#"{ "count": 4 }"#),
        Privilege::AddPartitions,
    ),
    (
        Method::DELETE,
        "/clusters/local/topics/orders.created/records",
        None,
        Privilege::DeleteRecords,
    ),
    (
        Method::POST,
        "/clusters/local/topics/orders.created/records",
        Some(r#"{ "key": null, "value": null }"#),
        Privilege::Produce,
    ),
    (
        Method::PATCH,
        "/clusters/local/group-offsets/order-processor",
        Some(r#"{ "to": { "kind": "EARLIEST" }, "dryRun": true }"#),
        Privilege::ResetOffsets,
    ),
    (
        Method::DELETE,
        "/clusters/local/group-offsets/archive?topic=orders.created",
        None,
        Privilege::DeleteOffsets,
    ),
    (
        Method::DELETE,
        "/clusters/local/groups/archive",
        None,
        Privilege::DeleteGroups,
    ),
    (
        Method::POST,
        "/clusters/local/subjects/orders.created-value",
        Some(r#"{ "type": "AVRO", "schema": "\"string\"" }"#),
        Privilege::RegisterSchemas,
    ),
    (
        Method::PATCH,
        "/clusters/local/subjects/orders.created-value",
        Some(r#"{ "compatibility": "FULL" }"#),
        Privilege::SetCompatibility,
    ),
    (
        Method::DELETE,
        "/clusters/local/subjects/orders.created-value?version=1",
        None,
        Privilege::DeleteSchemas,
    ),
    (
        Method::POST,
        "/clusters/local/acls",
        Some(
            r#"{ "bindings": [{ "resourceType": "TOPIC", "resourceName": "orders.created", "patternType": "LITERAL", "principal": "User:bob", "host": "*", "operation": "READ", "permission": "ALLOW" }] }"#,
        ),
        Privilege::CreateAcls,
    ),
    (
        Method::DELETE,
        "/clusters/local/acls?resourceType=TOPIC&resourceName=orders.created&patternType=LITERAL&principal=User:alice&host=*&operation=READ&permission=ALLOW",
        None,
        Privilege::DeleteAcls,
    ),
    (
        Method::PUT,
        "/clusters/local/quotas",
        Some(
            r#"{ "entity": [{ "entityType": "USER", "name": "bob" }], "producerByteRate": 1024 }"#,
        ),
        Privilege::AlterQuotas,
    ),
    (
        Method::PUT,
        "/clusters/local/scram-users/carol",
        Some(r#"{ "mechanism": "SHA256", "password": "s3cret" }"#),
        Privilege::SetScramCredentials,
    ),
    (
        Method::DELETE,
        "/clusters/local/scram-users/alice?mechanism=SHA256",
        None,
        Privilege::DeleteScramCredentials,
    ),
    (
        Method::PATCH,
        "/clusters/local/brokers/1/configs",
        Some(r#"{ "set": { "log.retention.hours": "72" } }"#),
        Privilege::AlterBrokerConfigs,
    ),
    (
        Method::PATCH,
        "/clusters/local/brokers/configs",
        Some(r#"{ "reset": ["log.retention.hours"] }"#),
        Privilege::AlterBrokerConfigs,
    ),
];

fn write(method: &Method, route: &str, body: Option<&str>) -> Request<Body> {
    match body {
        Some(body) => json_request(method.clone(), route, body.to_owned()),
        None => Request::builder()
            .method(method.clone())
            .uri(route)
            .body(Body::empty())
            .expect("request"),
    }
}

#[tokio::test]
async fn every_route_opens_to_exactly_the_privilege_it_names() {
    let app = TestApp::local().await;
    let mut wrong = Vec::new();

    for held in std::iter::once(None).chain(Privilege::ALL.map(Some)) {
        let session = app.with_access(access([role("probe", PrivilegeSet::from_privileges(held))]));
        for &(route, needs) in ROUTES {
            let expected = if needs.is_none() || needs == held {
                StatusCode::OK
            } else {
                StatusCode::FORBIDDEN
            };
            let status = session.status(route).await;
            if status != expected {
                wrong.push(format!(
                    "{route} holding {held:?} answered {status}, not {expected}"
                ));
            }
        }
    }

    assert!(wrong.is_empty(), "\n{}", wrong.join("\n"));
}

#[tokio::test]
async fn every_cluster_route_hides_a_cluster_the_session_cannot_see() {
    let app = TestApp::of([FakeCluster::local(), FakeCluster::named("payments")])
        .ingested()
        .await
        .with_access(access([admin().on(&["local"])]));

    for (route, _) in ROUTES {
        if let Some(rest) = route.strip_prefix("/clusters/local") {
            app.get(&format!("/clusters/payments{rest}"))
                .await
                .assert_error(StatusCode::NOT_FOUND, "UNKNOWN_CLUSTER");
        }
    }
    for (method, route, body, _) in WRITES {
        let route = route.replacen("/clusters/local", "/clusters/payments", 1);
        app.reply(write(method, &route, *body))
            .await
            .assert_error(StatusCode::NOT_FOUND, "UNKNOWN_CLUSTER");
    }
}

#[tokio::test(start_paused = true)]
async fn every_write_opens_to_exactly_the_privilege_it_names() {
    let archive = group("archive", "orders.created", vec![0]).stopped();
    let app = TestApp::of([FakeCluster::local().with_groups([archive])])
        .writable(&["local"])
        .ingested()
        .await;
    let mut wrong = Vec::new();

    for held in std::iter::once(None).chain(Privilege::ALL.map(Some)) {
        let session = app.with_access(access([role("probe", PrivilegeSet::from_privileges(held))]));
        for (method, route, body, needs) in WRITES {
            let reply = session.reply(write(method, route, *body)).await;
            if reply.status.is_success() != (held == Some(*needs)) {
                wrong.push(format!(
                    "{method} {route} holding {held:?} answered {} {}",
                    reply.status, reply.body
                ));
            }
        }
    }

    assert!(wrong.is_empty(), "\n{}", wrong.join("\n"));
}

#[tokio::test]
async fn every_write_is_refused_on_a_read_only_cluster() {
    let app = TestApp::local().await;

    for (method, route, body, _) in WRITES {
        let reply = app.reply(write(method, route, *body)).await;
        reply.assert_error(StatusCode::FORBIDDEN, "READ_ONLY_CLUSTER");
        assert_eq!(reply.body["error"], "cluster 'local' is read-only");
    }
}

#[tokio::test]
async fn every_write_refuses_a_body_a_cross_site_form_can_send() {
    let app = TestApp::of([FakeCluster::local()])
        .writable(&["local"])
        .ingested()
        .await;

    for (method, route, body, _) in WRITES {
        let Some(body) = body else { continue };
        for content_type in ["text/plain", "application/x-www-form-urlencoded"] {
            let request = Request::builder()
                .method(method.clone())
                .uri(*route)
                .header(header::CONTENT_TYPE, content_type)
                .body(Body::from(*body))
                .expect("request");
            app.reply(request)
                .await
                .assert_error(StatusCode::UNSUPPORTED_MEDIA_TYPE, "INVALID_REQUEST");
        }
    }
}
