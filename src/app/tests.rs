use std::time::Duration;

use axum::body::Body;
use axum::http::{HeaderValue, Method, Request, StatusCode, header};
use strum::VariantArray as _;
use tokio::io::AsyncWriteExt as _;
use tokio::net::{TcpListener, TcpStream};
use tracing::Level;

use crate::app::AuthState;
use crate::app::auth::access::{Privilege, PrivilegeSet};
use crate::config::Config;
use crate::testing::{
    Api, FakeCluster, LogCapture, TestApp, access, admin, eventually, group, json_request, role,
};

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
        Some(Privilege::TopicConfigs),
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
        Some(Privilege::BrokerConfigs),
    ),
    ("/clusters/local/subjects", None),
    (
        "/clusters/local/subjects/orders.created-value",
        Some(Privilege::SchemaText),
    ),
    ("/clusters/local/acls", Some(Privilege::Acls)),
    ("/clusters/local/quotas", Some(Privilege::BrokerConfigs)),
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

    for held in std::iter::once(None).chain(Privilege::VARIANTS.iter().copied().map(Some)) {
        let session = app.with_access(access([role("probe", PrivilegeSet::from_iter(held))]));
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

    for held in std::iter::once(None).chain(Privilege::VARIANTS.iter().copied().map(Some)) {
        let session = app.with_access(access([role("probe", PrivilegeSet::from_iter(held))]));
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

async fn assert_unknown_api_paths_are_json(auth: AuthState) {
    let app = TestApp::of([FakeCluster::local()]).auth(auth).build();
    let request = |method: Method, path: &str| {
        Request::builder()
            .method(method)
            .uri(path)
            .header(header::HOST, "localhost")
            .body(Body::empty())
            .expect("request")
    };

    for path in ["/api", "/api/", "/api/nope", "/api/clusters/local/nope"] {
        app.reply_through_router(request(Method::GET, path))
            .await
            .assert_error(StatusCode::NOT_FOUND, "NOT_FOUND");
    }
    app.reply_through_router(request(Method::PUT, "/api/clusters"))
        .await
        .assert_error(StatusCode::METHOD_NOT_ALLOWED, "INVALID_REQUEST");
}

#[tokio::test]
async fn an_unknown_api_path_is_a_json_error_with_auth_off() {
    assert_unknown_api_paths_are_json(AuthState::disabled()).await;
}

#[tokio::test]
async fn an_unknown_api_path_is_a_json_error_with_auth_on() {
    assert_unknown_api_paths_are_json(AuthState::enabled_for_tests()).await;
}

#[tokio::test]
async fn every_route_refuses_a_host_off_the_list_while_auth_is_off() {
    let app = TestApp::of([FakeCluster::local()])
        .writable(&["local"])
        .ingested()
        .await;
    let reads = ROUTES.iter().map(|(route, _)| (Method::GET, *route, None));
    let writes = WRITES
        .iter()
        .map(|(method, route, body, _)| (method.clone(), *route, *body));

    for (method, route, body) in reads.chain(writes) {
        let mut request = write(&method, &format!("/api{route}"), body);
        request
            .headers_mut()
            .insert(header::HOST, HeaderValue::from_static("attacker.example"));

        app.reply_through_router(request)
            .await
            .assert_error(StatusCode::FORBIDDEN, "HOST_NOT_ALLOWED");
    }
}

#[tokio::test]
async fn a_change_finishes_and_logs_after_the_client_hangs_up_and_shutdown_waits_for_it() {
    let app =
        TestApp::of(
            [FakeCluster::local().with_delay(Api::DeleteTopic, Duration::from_millis(100))],
        )
        .writable(&["local"])
        .ingested()
        .await;
    let mut rig = app.rig();
    let lane = rig.topology();
    rig.spawn(lane);
    let logs = LogCapture::at(Level::INFO);
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("free port");
    let address = listener.local_addr().expect("bound address");
    let router = super::router(app.state().clone(), &Config::default().allowed_hosts, None);
    tokio::spawn(async move { axum::serve(listener, router).await });

    let mut client = TcpStream::connect(address).await.expect("connect");
    client
        .write_all(
            b"DELETE /api/clusters/local/topics/orders.created HTTP/1.1\r\nHost: localhost\r\n\r\n",
        )
        .await
        .expect("request");
    eventually("the delete reaches kafka", || {
        app.cluster().calls(Api::DeleteTopic) == 1
    })
    .await;
    drop(client);
    let writes = app.state().writes();
    writes.close();
    writes.wait().await;

    logs.assert_contains("deleted topic");
}

#[tokio::test]
async fn a_resource_request_another_site_sent_is_refused() {
    let app = TestApp::of([FakeCluster::local()]).build();
    let request = |path: &str, site: &str| {
        Request::builder()
            .uri(path)
            .header(header::HOST, "localhost")
            .header("sec-fetch-site", site)
            .header("sec-fetch-mode", "navigate")
            .header("sec-fetch-dest", "document")
            .body(Body::empty())
            .expect("request")
    };

    for site in ["cross-site", "same-site"] {
        app.reply_through_router(request("/api/clusters", site))
            .await
            .assert_error(StatusCode::FORBIDDEN, "CROSS_SITE");
    }
    for (path, site) in [
        ("/api/clusters", "same-origin"),
        ("/api/clusters", "none"),
        ("/api/auth/me", "cross-site"),
    ] {
        app.reply_through_router(request(path, site)).await.ok();
    }
}
