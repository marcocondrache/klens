use std::pin::pin;

use axum::Router;
use axum::body::{Body, to_bytes};
use axum::http::{HeaderMap, Request as HttpRequest, StatusCode};
use axum::routing::{any, post};
use axum::{Json, middleware};
use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use futures::future::join_all;
use futures::poll;
use jsonwebtoken::{EncodingKey, Header};
use serde_json::json;
use tower::ServiceExt as _;
use wiremock::ResponseTemplate;

use super::*;
use crate::app::auth::access::{Ceiling, EffectiveAccess, Narrowed, Privilege};
use crate::app::auth::testing::{
    Browser, Idp, RESOURCE, RESOURCE_HOST, Signer, bearing, key_set, mcp,
};
use crate::app::auth::{AuthState, SessionUser};
use crate::app::{AppState, Limits};
use crate::config::{Config, Mcp, Tuning};
use crate::kafka::Clusters;
use crate::testing::{FakeCluster, LogCapture, eventually, mcp_request, quiesce, yaml};

const CHALLENGE: &str = r#"Bearer resource_metadata="https://klens.example.com/.well-known/oauth-protected-resource/mcp""#;

fn invalid() -> String {
    format!(r#"{CHALLENGE}, error="invalid_token""#)
}

struct Answer {
    status: StatusCode,
    headers: HeaderMap,
    body: Value,
}

impl Answer {
    async fn of(response: Response) -> Self {
        let status = response.status();
        let headers = response.headers().clone();
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("a body");
        Self {
            status,
            headers,
            body: serde_json::from_slice(&body).unwrap_or(Value::Null),
        }
    }

    fn challenge(&self) -> &str {
        self.headers
            .get(header::WWW_AUTHENTICATE)
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default()
    }
}

fn probe(auth: &AuthState) -> Router {
    let bearer = Arc::clone(auth.bearer().expect("a token check"));
    let guard = SessionGuard::capped(auth.clone(), Ceiling::new("mcp", &Privilege::ALL, None));
    Router::new()
        .route("/mcp", post(handed_on))
        .layer(middleware::from_fn_with_state(
            (bearer, guard),
            require_bearer,
        ))
}

async fn handed_on(request: Request) -> Json<Value> {
    let guard = request.extensions().get::<SessionGuard>().expect("a guard");
    let access: EffectiveAccess = request
        .extensions()
        .get::<Narrowed>()
        .cloned()
        .expect("narrowed access")
        .into();
    let privileges: Vec<&str> = access
        .privileges_for("local")
        .expect("local is visible")
        .iter()
        .map(Privilege::name)
        .collect();
    Json(json!({
        "user": guard.subject(),
        "client": guard.client(),
        "privileges": privileges,
        "authorization": request.headers().contains_key(header::AUTHORIZATION),
    }))
}

fn post_mcp(token: &str) -> HttpRequest<Body> {
    bearing(
        HttpRequest::post("/mcp")
            .body(Body::empty())
            .expect("request"),
        token,
    )
}

async fn probed(app: &Router, token: &str) -> Answer {
    Answer::of(
        app.clone()
            .oneshot(post_mcp(token))
            .await
            .expect("response"),
    )
    .await
}

fn served(auth: AuthState, mcp: &Mcp) -> Router {
    crate::app::router(
        AppState::new(
            Clusters::from_sessions(vec![FakeCluster::local()]),
            auth,
            Limits::new(&Tuning::default()),
        ),
        &Config::default().allowed_hosts,
        Some(mcp),
    )
}

fn explain() -> HttpRequest<Body> {
    mcp_request(&json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "tools/call",
        "params": { "name": "klens_access_explain", "arguments": {} },
    }))
}

fn with(mut claims: Value, name: &str, value: Value) -> Value {
    claims[name] = value;
    claims
}

fn without(mut claims: Value, name: &str) -> Value {
    claims.as_object_mut().expect("claims").remove(name);
    claims
}

fn now() -> i64 {
    Timestamp::now().as_second()
}

fn unsigned(claims: &Value) -> String {
    let header = URL_SAFE_NO_PAD.encode(br#"{"alg":"none","kid":"a"}"#);
    let claims = URL_SAFE_NO_PAD.encode(claims.to_string());
    format!("{header}.{claims}.")
}

#[tokio::test]
async fn a_valid_token_opens_mcp_with_its_roles_under_the_ceiling() {
    let a = Signer::a();
    let idp = Idp::start(&[&a], &[&a]).await;
    let mcp = mcp("privileges: [records, broker_configs, acls]");
    let app = served(idp.auth(&mcp).await, &mcp);

    let answer = Answer::of(
        app.oneshot(bearing(explain(), &a.sign(&idp.claims())))
            .await
            .expect("response"),
    )
    .await;

    assert_eq!(answer.status, StatusCode::OK, "{}", answer.body);
    let explained = &answer.body["result"]["structuredContent"]["clusters"][0];
    assert_eq!(explained["cluster"], "local");
    assert_eq!(explained["privileges"], json!(["RECORDS", "ACLS"]));
    assert_eq!(idp.key_fetches().await, 1, "the discovered keys verify it");
}

#[tokio::test]
async fn each_asymmetric_key_family_the_provider_publishes_verifies_its_tokens() {
    let signers = [Signer::a(), Signer::rsa(), Signer::ed25519()];
    let published: Vec<&Signer> = signers.iter().collect();
    let idp = Idp::start(&published, &published).await;
    let app = probe(&idp.auth(&mcp("")).await);

    for signer in &signers {
        let answer = probed(&app, &signer.sign(&idp.claims())).await;

        assert_eq!(answer.status, StatusCode::OK, "{}", signer.kid);
    }
    assert_eq!(idp.key_fetches().await, 1);
}

#[tokio::test]
async fn the_token_check_hands_on_the_user_and_client_but_not_the_token() {
    let a = Signer::a();
    let idp = Idp::start(&[&a], &[&a]).await;
    let app = probe(&idp.auth(&mcp("")).await);

    let answer = probed(&app, &a.sign(&idp.claims())).await;

    assert_eq!(
        answer.body,
        json!({
            "user": "user-1",
            "client": "claude-code",
            "privileges": ["records", "topicConfigs", "acls"],
            "authorization": false,
        })
    );
}

#[tokio::test]
async fn a_token_passes_with_any_audience_it_lists_and_an_access_token_type() {
    let a = Signer::a();
    let idp = Idp::start(&[&a], &[&a]).await;
    let app = probe(&idp.auth(&mcp("")).await);
    let valid = idp.claims();
    let typed = |typ: Option<&str>| {
        a.sign_with(
            Header {
                typ: typ.map(ToOwned::to_owned),
                kid: Some("a".to_owned()),
                ..Header::new(Algorithm::ES256)
            },
            &valid,
        )
    };

    for (case, token) in [
        (
            "several audiences",
            a.sign(&with(valid.clone(), "aud", json!(["account", RESOURCE]))),
        ),
        (
            "valid from within the leeway",
            a.sign(&with(valid.clone(), "nbf", json!(now() + 10))),
        ),
        (
            "issued within the leeway",
            a.sign(&with(valid.clone(), "iat", json!(now() + 10))),
        ),
        (
            "lasting exactly max_age",
            a.sign(&with(
                valid.clone(),
                "exp",
                valid["iat"].as_i64().map(|iat| iat + 3_600).into(),
            )),
        ),
        ("typed jwt", typed(Some("jwt"))),
        ("typed at+jwt", typed(Some("at+jwt"))),
        (
            "typed application/at+jwt",
            typed(Some("application/AT+JWT")),
        ),
        ("untyped", typed(None)),
    ] {
        let answer = probed(&app, &token).await;

        assert_eq!(answer.status, StatusCode::OK, "{case}");
    }
}

#[tokio::test]
async fn a_token_issued_as_far_ahead_as_the_leeway_allows_passes() {
    let a = Signer::a();
    let idp = Idp::start(&[&a], &[&a]).await;
    let app = probe(&idp.auth(&mcp("")).await);

    // Only an answer that comes back within the second of issue saw the
    // token exactly 30 seconds ahead.
    let answer = loop {
        let issued = now();
        let token = a.sign(&with(idp.claims(), "iat", json!(issued + 30)));
        let answer = probed(&app, &token).await;
        if now() == issued {
            break answer;
        }
    };

    assert_eq!(answer.status, StatusCode::OK);
}

#[tokio::test]
async fn a_token_that_fails_a_check_is_refused_as_invalid() {
    let (a, b) = (Signer::a(), Signer::b());
    let idp = Idp::start(&[&a, &b], &[&a, &b]).await;
    let app = probe(&idp.auth(&mcp("")).await);
    let valid = idp.claims();
    let header = |change: fn(&mut Header)| {
        let mut header = Header {
            kid: Some("a".to_owned()),
            ..Header::new(Algorithm::ES256)
        };
        change(&mut header);
        a.sign_with(header, &valid)
    };
    let guessed = jsonwebtoken::encode(
        &Header {
            kid: Some("a".to_owned()),
            ..Header::new(Algorithm::HS256)
        },
        &valid,
        &EncodingKey::from_secret(b"secret"),
    )
    .expect("an hs256 token");
    let forger = Signer::ec("a", include_str!("../testing/es256-b.pem"));
    let self_signed = forger.sign_with(
        Header {
            kid: Some("a".to_owned()),
            jwk: serde_json::from_value(forger.jwk()).ok(),
            ..Header::new(Algorithm::ES256)
        },
        &valid,
    );

    for (case, token) in [
        (
            "another audience",
            a.sign(&with(valid.clone(), "aud", json!("https://other.example"))),
        ),
        (
            "the ui client as audience",
            a.sign(&with(valid.clone(), "aud", json!("klens"))),
        ),
        ("no audience", a.sign(&without(valid.clone(), "aud"))),
        (
            "another issuer",
            a.sign(&with(valid.clone(), "iss", json!("https://evil.example"))),
        ),
        ("no issuer", a.sign(&without(valid.clone(), "iss"))),
        ("no subject", a.sign(&without(valid.clone(), "sub"))),
        ("no expiry", a.sign(&without(valid.clone(), "exp"))),
        (
            "expired a moment ago",
            a.sign(&with(valid.clone(), "exp", json!(now() - 1))),
        ),
        (
            "expired past the leeway",
            a.sign(&with(valid.clone(), "exp", json!(now() - 60))),
        ),
        (
            "not valid yet",
            a.sign(&with(valid.clone(), "nbf", json!(now() + 120))),
        ),
        ("no issue time", a.sign(&without(valid.clone(), "iat"))),
        (
            "issued in the future",
            a.sign(&with(valid.clone(), "iat", json!(now() + 120))),
        ),
        (
            "lasting past max_age",
            a.sign(&with(valid.clone(), "exp", json!(now() + 3_601))),
        ),
        ("hs256 with a guessed secret", guessed),
        ("unsigned", unsigned(&valid)),
        (
            "typed as a logout token",
            header(|header| header.typ = Some("logout+jwt".to_owned())),
        ),
        (
            "with a critical header",
            header(|header| header.crit = Some(vec!["exp".to_owned()])),
        ),
        (
            "with no kid among two keys",
            header(|header| header.kid = None),
        ),
        (
            "with an unknown kid",
            header(|header| header.kid = Some("c".to_owned())),
        ),
        (
            "with a key it was not signed with",
            header(|header| header.kid = Some("b".to_owned())),
        ),
        ("signed by the key in its jwk header", self_signed),
        ("not a jwt", "opaque-access-token".to_owned()),
    ] {
        let answer = probed(&app, &token).await;

        assert_eq!(answer.status, StatusCode::UNAUTHORIZED, "{case}");
        assert_eq!(answer.challenge(), invalid(), "{case}");
    }
}

#[tokio::test]
async fn only_a_signing_key_for_the_tokens_algorithm_verifies_it() {
    let a = Signer::a();
    let key = |change: fn(&mut Value)| {
        let mut jwk = a.jwk();
        change(&mut jwk);
        json!({ "keys": [jwk] })
    };

    for (case, keys) in [
        ("an encryption key", key(|jwk| jwk["use"] = json!("enc"))),
        ("a key for ES384", key(|jwk| jwk["alg"] = json!("ES384"))),
        (
            "a key with no kid",
            key(|jwk| {
                jwk.as_object_mut().expect("a jwk").remove("kid");
            }),
        ),
    ] {
        let idp = Idp::serving(keys.clone(), ResponseTemplate::new(200).set_body_json(keys)).await;
        let app = probe(&idp.auth(&mcp("")).await);

        let answer = probed(&app, &a.sign(&idp.claims())).await;

        assert_eq!(answer.status, StatusCode::UNAUTHORIZED, "{case}");
    }
}

#[tokio::test]
async fn a_token_with_no_kid_passes_only_while_the_provider_publishes_one_key() {
    let a = Signer::a();
    let idp = Idp::start(&[&a], &[&a]).await;
    let app = probe(&idp.auth(&mcp("")).await);
    let token = a.sign_with(Header::new(Algorithm::ES256), &idp.claims());

    let answer = probed(&app, &token).await;

    assert_eq!(answer.status, StatusCode::OK);
}

#[tokio::test]
async fn a_valid_token_whose_groups_bind_no_role_is_forbidden() {
    let a = Signer::a();
    let idp = Idp::start(&[&a], &[&a]).await;
    let app = probe(&idp.auth(&mcp("")).await);
    let logs = LogCapture::at(tracing::Level::INFO);

    let answer = probed(
        &app,
        &a.sign(&with(idp.claims(), "groups", json!(["guests"]))),
    )
    .await;

    assert_eq!(answer.status, StatusCode::FORBIDDEN);
    assert_eq!(
        answer.body,
        json!({ "error": "no role is bound to a group of this token", "code": "FORBIDDEN" })
    );
    assert_eq!(answer.challenge(), "");
    logs.assert_contains("bearer token refused: no matching role");
}

#[tokio::test]
async fn only_listed_clients_pass_when_clients_are_set() {
    let a = Signer::a();
    let idp = Idp::start(&[&a], &[&a]).await;
    let app = probe(&idp.auth(&mcp("token: {clients: [vscode]}")).await);
    let valid = without(idp.claims(), "azp");

    for (claims, status) in [
        (with(valid.clone(), "azp", json!("vscode")), StatusCode::OK),
        (
            with(valid.clone(), "client_id", json!("vscode")),
            StatusCode::OK,
        ),
        (
            with(valid.clone(), "azp", json!("claude-code")),
            StatusCode::UNAUTHORIZED,
        ),
        (
            with(
                with(valid.clone(), "azp", json!("claude-code")),
                "client_id",
                json!("vscode"),
            ),
            StatusCode::UNAUTHORIZED,
        ),
        (valid, StatusCode::UNAUTHORIZED),
    ] {
        let answer = probed(&app, &a.sign(&claims)).await;

        assert_eq!(answer.status, status, "{claims}");
    }
}

#[tokio::test]
async fn the_user_and_groups_come_from_the_configured_claims() {
    let a = Signer::a();
    let idp = Idp::start(&[&a], &[&a]).await;
    let app = probe(
        &idp.auth(&mcp(
            "token: {user_claim: preferred_username, groups_claim: roles}",
        ))
        .await,
    );
    let claims = with(
        with(idp.claims(), "groups", json!([])),
        "roles",
        json!(["ops"]),
    );

    let named = probed(
        &app,
        &a.sign(&with(claims.clone(), "preferred_username", json!("alice"))),
    )
    .await;
    let unnamed = probed(&app, &a.sign(&claims)).await;

    assert_eq!(named.body["user"], "alice");
    assert_eq!(unnamed.body["user"], "user-1", "the sub names it instead");
}

#[tokio::test]
async fn an_unknown_key_refetches_the_set_at_most_once_per_window() {
    let (a, b) = (Signer::a(), Signer::b());
    let idp = Idp::start(&[&a], &[&a, &b]).await;
    let auth = idp.auth(&mcp("")).await;
    let app = probe(&auth);
    let valid = idp.claims();
    let stranger = Signer::ec("c", include_str!("../testing/es256-b.pem"));

    assert_eq!(probed(&app, &b.sign(&valid)).await.status, StatusCode::OK);
    assert_eq!(idp.key_fetches().await, 2);

    let refused = probed(&app, &stranger.sign(&valid)).await;
    assert_eq!(refused.status, StatusCode::UNAUTHORIZED);
    assert_eq!(idp.key_fetches().await, 2, "inside the window");

    let keys = &auth.bearer().expect("a token check").keys;
    *keys.missed_at.lock().await = Instant::now().checked_sub(MISS_REFETCH);
    probed(&app, &stranger.sign(&valid)).await;
    assert_eq!(idp.key_fetches().await, 3, "after the window");
}

#[tokio::test]
async fn an_unknown_key_refetches_the_set_the_moment_the_window_closes() {
    let (a, b) = (Signer::a(), Signer::b());
    let idp = Idp::start(&[&a], &[&a, &b]).await;
    let auth = idp.auth(&mcp("")).await;
    let keys = &auth.bearer().expect("a token check").keys;

    // The window is checked on the paused clock in the first poll. The
    // refetch runs on the real clock, since a paused one would skip ahead
    // to the fetch timeout while the request is in flight.
    tokio::time::pause();
    *keys.missed_at.lock().await = Instant::now().checked_sub(MISS_REFETCH);
    let mut lookup = pin!(keys.key(Some("b")));
    assert!(poll!(&mut lookup).is_pending(), "no refetch started");
    tokio::time::resume();

    assert!(lookup.await.is_some());
    assert_eq!(idp.key_fetches().await, 2);
}

#[tokio::test]
async fn callers_with_a_new_key_share_one_refetch() {
    let (a, b) = (Signer::a(), Signer::b());
    let idp = Idp::serving(
        key_set(&[&a]),
        ResponseTemplate::new(200)
            .set_body_json(key_set(&[&a, &b]))
            .set_delay(Duration::from_millis(50)),
    )
    .await;
    let app = probe(&idp.auth(&mcp("")).await);
    let token = b.sign(&idp.claims());

    let answers = join_all((0..8).map(|_| probed(&app, &token))).await;

    for answer in answers {
        assert_eq!(answer.status, StatusCode::OK);
    }
    assert_eq!(idp.key_fetches().await, 2);
}

#[tokio::test]
async fn a_caller_that_hangs_up_leaves_its_refetch_to_finish() {
    let (a, b) = (Signer::a(), Signer::b());
    let idp = Idp::serving(
        key_set(&[&a]),
        ResponseTemplate::new(200)
            .set_body_json(key_set(&[&a, &b]))
            .set_delay(Duration::from_millis(50)),
    )
    .await;
    let auth = idp.auth(&mcp("")).await;
    let keys = &auth.bearer().expect("a token check").keys;

    let hung_up = tokio::time::timeout(Duration::from_millis(10), keys.key(Some("made-up"))).await;

    assert!(hung_up.is_err(), "the refetch outran the caller");
    assert!(keys.key(Some("b")).await.is_some());
    assert_eq!(idp.key_fetches().await, 2);
}

#[tokio::test]
async fn the_key_set_is_replaced_every_15_minutes() {
    let (a, b) = (Signer::a(), Signer::b());
    let idp = Idp::start(&[&a], &[&b]).await;
    let auth = idp.auth(&mcp("")).await;
    let keys = &auth.bearer().expect("a token check").keys;
    // Let the refresh task start its sleep before the clock stops.
    quiesce().await;

    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(15 * 60 - 10)).await;
    tokio::time::resume();
    // An early refresh needs real time to reach the provider.
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(idp.key_fetches().await, 1, "before 15 minutes");

    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(10)).await;
    tokio::time::resume();

    eventually("the refresh", || keys.cached("b").is_some()).await;
    assert!(keys.cached("a").is_none());
    assert_eq!(idp.key_fetches().await, 2);
}

#[tokio::test]
async fn a_miss_only_adds_keys_and_a_refresh_drops_the_unpublished_ones() {
    let (a, b) = (Signer::a(), Signer::b());
    let idp = Idp::start(&[&a], &[&b]).await;
    let auth = idp.auth(&mcp("")).await;
    let app = probe(&auth);
    let valid = idp.claims();

    assert_eq!(probed(&app, &b.sign(&valid)).await.status, StatusCode::OK);
    assert_eq!(probed(&app, &a.sign(&valid)).await.status, StatusCode::OK);

    auth.bearer().expect("a token check").keys.refresh().await;

    assert_eq!(probed(&app, &b.sign(&valid)).await.status, StatusCode::OK);
    assert_eq!(
        probed(&app, &a.sign(&valid)).await.status,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(idp.key_fetches().await, 3);
}

#[tokio::test]
async fn a_key_set_past_the_cap_adds_no_key() {
    let (a, b) = (Signer::a(), Signer::b());
    let mut padded = key_set(&[&a, &b]);
    padded["padding"] = json!("x".repeat(MAX_KEY_SET_BYTES));
    let idp = Idp::serving(
        key_set(&[&a]),
        ResponseTemplate::new(200).set_body_json(padded),
    )
    .await;
    let app = probe(&idp.auth(&mcp("")).await);
    let logs = LogCapture::at(tracing::Level::WARN);

    let answer = probed(&app, &b.sign(&idp.claims())).await;

    assert_eq!(answer.status, StatusCode::UNAUTHORIZED);
    logs.assert_contains("the key set is larger than 65536 bytes");
}

#[tokio::test]
async fn a_key_set_of_64_kib_adds_its_keys_and_one_byte_more_adds_none() {
    let (a, b) = (Signer::a(), Signer::b());

    for (size, status) in [
        (64 * 1024, StatusCode::OK),
        (64 * 1024 + 1, StatusCode::UNAUTHORIZED),
    ] {
        let mut body = key_set(&[&a, &b]).to_string().into_bytes();
        body.resize(size, b' ');
        let idp = Idp::serving(
            key_set(&[&a]),
            ResponseTemplate::new(200).set_body_raw(body, "application/json"),
        )
        .await;
        let app = probe(&idp.auth(&mcp("")).await);

        let answer = probed(&app, &b.sign(&idp.claims())).await;

        assert_eq!(answer.status, status, "{size} bytes");
    }
}

#[tokio::test]
async fn a_refusal_is_logged_at_debug_without_the_token() {
    let a = Signer::a();
    let idp = Idp::start(&[&a], &[&a]).await;
    let app = probe(&idp.auth(&mcp("")).await);
    let token = a.sign(&with(idp.claims(), "aud", json!("https://other.example")));
    let logs = LogCapture::at(tracing::Level::DEBUG);

    probed(&app, &token).await;

    logs.assert_contains(r#"refused a bearer token reason="InvalidAudience""#);
    logs.assert_lacks(&token);
    logs.assert_lacks(token.split('.').nth(1).expect("a payload"));
}

#[tokio::test]
async fn the_challenge_names_the_configured_resource_and_scopes_whatever_the_host() {
    let a = Signer::a();
    let idp = Idp::start(&[&a], &[&a]).await;
    let app = served(
        idp.auth(&mcp("token: {scopes: [mcp.read, offline_access]}"))
            .await,
        &mcp(""),
    );

    let mut request = explain();
    request
        .headers_mut()
        .insert(header::HOST, HeaderValue::from_static("evil.example"));
    request.headers_mut().insert(
        header::AUTHORIZATION,
        HeaderValue::from_static("Basic dXNlcjpwYXNz"),
    );
    let answer = Answer::of(app.oneshot(request).await.expect("response")).await;

    assert_eq!(answer.status, StatusCode::UNAUTHORIZED);
    assert_eq!(
        answer.challenge(),
        format!(r#"{CHALLENGE}, scope="mcp.read offline_access""#)
    );
    assert_eq!(
        answer.body,
        json!({ "error": "unauthorized", "code": "UNAUTHORIZED" })
    );
}

#[tokio::test]
async fn a_bearer_call_leaves_the_browser_session_of_its_subject_alone() {
    let a = Signer::a();
    let idp = Idp::start(&[&a], &[&a]).await;
    let mcp = mcp("");
    let mut browser = Browser::serving(idp.auth(&mcp).await, Some(&mcp));
    browser
        .impersonate(&SessionUser::new(
            "user-1",
            None,
            None,
            vec!["ops".to_owned()],
            now() + 3_600,
        ))
        .await;
    assert_eq!(browser.get("/api/whoami").await.status, StatusCode::OK);

    let token = a.sign(&with(idp.claims(), "groups", json!(["ops", "admins"])));
    let called = browser.send(bearing(explain(), &token)).await;
    assert_eq!(called.status, StatusCode::OK);

    let whoami = browser.get("/api/whoami").await;
    assert_eq!(whoami.status, StatusCode::OK);
    assert_eq!(whoami.json()["subject"], "user-1");
}

#[tokio::test]
async fn a_session_cookie_does_not_open_mcp() {
    let a = Signer::a();
    let idp = Idp::start(&[&a], &[&a]).await;
    let mcp = mcp("");
    let mut browser = Browser::serving(idp.auth(&mcp).await, Some(&mcp));
    browser
        .impersonate(&SessionUser::new(
            "user-1",
            None,
            None,
            vec!["ops".to_owned()],
            now() + 3_600,
        ))
        .await;
    let mut request = explain();
    request
        .headers_mut()
        .insert(header::HOST, HeaderValue::from_static(RESOURCE_HOST));

    let page = browser.send(request).await;

    assert_eq!(page.status, StatusCode::UNAUTHORIZED);
    assert_eq!(
        page.headers[header::WWW_AUTHENTICATE].to_str().ok(),
        Some(CHALLENGE)
    );
}

fn spoofed(path: &str) -> HttpRequest<Body> {
    HttpRequest::get(path)
        .header(header::HOST, "evil.example")
        .header("x-forwarded-host", "evil.example")
        .header("x-forwarded-proto", "http")
        .body(Body::empty())
        .expect("request")
}

#[tokio::test]
async fn the_metadata_names_the_resource_the_provider_and_the_scopes_at_both_paths() {
    let a = Signer::a();
    let idp = Idp::start(&[&a], &[&a]).await;
    let mcp = mcp("token: {scopes: [mcp.read, offline_access]}");
    let app = served(idp.auth(&mcp).await, &mcp);

    for path in [
        "/.well-known/oauth-protected-resource/mcp",
        "/.well-known/oauth-protected-resource",
    ] {
        let answer = Answer::of(app.clone().oneshot(spoofed(path)).await.expect("response")).await;

        assert_eq!(answer.status, StatusCode::OK, "{path}");
        assert_eq!(
            answer.body,
            json!({
                "resource": RESOURCE,
                "authorization_servers": [idp.issuer()],
                "scopes_supported": ["mcp.read", "offline_access"],
                "bearer_methods_supported": ["header"],
                "resource_name": "klens",
            }),
            "{path}"
        );
    }
}

#[tokio::test]
async fn the_metadata_and_the_challenge_follow_wherever_the_resource_sits() {
    let a = Signer::a();
    let idp = Idp::start(&[&a], &[&a]).await;

    for (resource, host, metadata) in [
        (
            "https://klens.example.com/klens/mcp",
            "klens.example.com",
            "https://klens.example.com/.well-known/oauth-protected-resource/klens/mcp",
        ),
        (
            "https://klens.example.com:8443",
            "klens.example.com:8443",
            "https://klens.example.com:8443/.well-known/oauth-protected-resource",
        ),
    ] {
        let mcp: Mcp = yaml(&format!("resource: '{resource}'"));
        let app = served(idp.auth(&mcp).await, &mcp);
        let mut unsigned = explain();
        unsigned
            .headers_mut()
            .insert(header::HOST, host.parse().expect("a host"));
        let path = &metadata[metadata.find("/.well-known").expect("a path")..];

        let challenged = Answer::of(app.clone().oneshot(unsigned).await.expect("response")).await;
        let document =
            Answer::of(app.clone().oneshot(spoofed(path)).await.expect("response")).await;
        let root = Answer::of(
            app.oneshot(spoofed("/.well-known/oauth-protected-resource"))
                .await
                .expect("response"),
        )
        .await;

        assert_eq!(
            challenged.challenge(),
            format!(r#"Bearer resource_metadata="{metadata}""#),
            "{resource}"
        );
        let expected = Url::parse(resource).expect("a url").to_string();
        assert_eq!(document.body["resource"], expected, "{resource}");
        assert_eq!(document.body.get("scopes_supported"), None, "{resource}");
        assert_eq!(root.body, document.body, "{resource}");
    }
}

#[tokio::test]
async fn no_metadata_is_served_without_auth_or_without_mcp() {
    let a = Signer::a();
    let idp = Idp::start(&[&a], &[&a]).await;
    let mcp = mcp("");
    let without_mcp = crate::app::router(
        AppState::new(
            Clusters::from_sessions(vec![FakeCluster::local()]),
            idp.auth(&mcp).await,
            Limits::new(&Tuning::default()),
        ),
        &Config::default().allowed_hosts,
        None,
    );
    let without_auth = served(AuthState::disabled(), &Mcp::default());

    for app in [without_mcp, without_auth] {
        for path in [
            "/.well-known/oauth-protected-resource/mcp",
            "/.well-known/oauth-protected-resource",
        ] {
            let request = HttpRequest::get(path)
                .header(header::HOST, "localhost")
                .body(Body::empty())
                .expect("request");

            let answer = Answer::of(app.clone().oneshot(request).await.expect("response")).await;

            assert_eq!(answer.body.get("resource"), None, "{path}");
        }
    }
}

#[tokio::test]
async fn the_metadata_routes_sit_beside_a_catch_all_under_well_known() {
    let a = Signer::a();
    let idp = Idp::start(&[&a], &[&a]).await;
    let auth = idp.auth(&mcp("")).await;
    let app = Router::new()
        .nest_service("/.well-known", any(|| async { StatusCode::NOT_FOUND }))
        .merge(auth.bearer().expect("a token check").metadata());

    for (path, status) in [
        ("/.well-known/oauth-protected-resource/mcp", StatusCode::OK),
        ("/.well-known/oauth-protected-resource", StatusCode::OK),
        (
            "/.well-known/oauth-protected-resource/other",
            StatusCode::NOT_FOUND,
        ),
        ("/.well-known/openid-configuration", StatusCode::NOT_FOUND),
    ] {
        let response = app.clone().oneshot(spoofed(path)).await.expect("response");

        assert_eq!(response.status(), status, "{path}");
    }
}

#[test]
fn a_key_set_yields_no_hmac_key() {
    let hmac =
        json!({ "kty": "oct", "kid": "h", "alg": "HS256", "k": URL_SAFE_NO_PAD.encode(b"secret") });

    let keys = usable(vec![hmac, Signer::a().jwk()]);

    assert_eq!(keys.keys().collect::<Vec<_>>(), ["a"]);
}

#[test]
fn only_jwt_and_access_token_types_name_an_access_token() {
    for typ in ["JWT", "jwt", "at+jwt", "AT+JWT", "application/at+jwt"] {
        assert!(names_an_access_token(typ), "{typ}");
    }
    for typ in [
        "logout+jwt",
        "dpop+jwt",
        "secevent+jwt",
        "application/jwt+at",
        "",
    ] {
        assert!(!names_an_access_token(typ), "{typ}");
    }
}

#[test]
fn the_metadata_path_puts_the_well_known_segment_before_the_resource_path() {
    for (resource, expected) in [
        (
            "https://klens.example.com/mcp",
            "/.well-known/oauth-protected-resource/mcp",
        ),
        (
            "https://klens.example.com/klens/mcp",
            "/.well-known/oauth-protected-resource/klens/mcp",
        ),
        (
            "https://klens.example.com:8443",
            "/.well-known/oauth-protected-resource",
        ),
    ] {
        assert_eq!(
            metadata_path(&Url::parse(resource).expect("a url")),
            expected
        );
    }
}
