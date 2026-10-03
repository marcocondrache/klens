use axum::http::StatusCode;

use crate::app::auth::access::{Privilege, PrivilegeSet};
use crate::testing::{FakeCluster, TestApp, access, admin, role};

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
];

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
}
