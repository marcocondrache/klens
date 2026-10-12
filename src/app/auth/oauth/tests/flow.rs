use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use indexmap::IndexMap;
use openidconnect::PkceCodeChallenge;
use serde_json::json;
use url::Url;
use url::form_urlencoded::Serializer;

use crate::app::auth::access::AccessPolicy;
use crate::app::auth::testing::{Browser, FakeOidc, Page, RESOURCE, bearing, mcp};
use crate::app::auth::{AuthState, SessionUser};
use crate::config::{Mcp, Role};
use crate::testing::{mcp_request, yaml};

const CALLBACK: &str = "http://localhost:54321/callback";
const ISSUER: &str = "https://klens.example.com";
const BROKEN: &str = "/oauth/consent?error=request";

fn auth(mcp: &Mcp) -> AuthState {
    let roles: IndexMap<String, Role> = yaml(
        "
        operator:
          privileges: [records, acls, produce]
          bindings: [{groups: [ops]}]
        ",
    );
    AuthState::enabled_for_tests_with(
        FakeOidc {
            groups: vec!["ops".to_owned()],
        },
        AccessPolicy::from_roles(Some(&roles)),
    )
    .serving_mcp(mcp)
}

fn query(url: &str, name: &str) -> Option<String> {
    Url::parse(url)
        .or_else(|_| Url::parse(&format!("http://klens{url}")))
        .expect("a url")
        .query_pairs()
        .find(|(key, _)| key == name)
        .map(|(_, value)| value.into_owned())
}

fn explain() -> Request<Body> {
    mcp_request(&json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "tools/call",
        "params": { "name": "klens_access_explain", "arguments": {} },
    }))
}

/// An MCP client and the browser it opens.
struct Agent {
    auth: AuthState,
    browser: Browser,
    client_id: String,
    verifier: String,
    challenge: String,
}

impl Agent {
    async fn registered() -> Self {
        Self::registered_with(&mcp("")).await
    }

    async fn registered_with(mcp: &Mcp) -> Self {
        let auth = auth(mcp);
        let mut browser = Browser::serving(auth.clone(), Some(mcp));
        let body = json!({ "client_name": "Claude Code", "redirect_uris": [CALLBACK] });
        let page = browser
            .send(
                Request::post("/api/oauth/register")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(body.to_string()))
                    .expect("request"),
            )
            .await;
        assert_eq!(page.status, StatusCode::CREATED);
        let (challenge, verifier) = PkceCodeChallenge::new_random_sha256();
        Self {
            auth,
            browser,
            client_id: page.json()["client_id"]
                .as_str()
                .expect("a client id")
                .to_owned(),
            verifier: verifier.secret().clone(),
            challenge: challenge.as_str().to_owned(),
        }
    }

    fn params(&self) -> Vec<(&'static str, String)> {
        vec![
            ("response_type", "code".to_owned()),
            ("client_id", self.client_id.clone()),
            ("redirect_uri", CALLBACK.to_owned()),
            ("code_challenge", self.challenge.clone()),
            ("code_challenge_method", "S256".to_owned()),
            ("state", "st-1".to_owned()),
            ("resource", RESOURCE.to_owned()),
        ]
    }

    async fn authorize(&mut self, params: &[(&str, String)]) -> Page {
        let query = Serializer::new(String::new()).extend_pairs(params).finish();
        self.browser
            .get(&format!("/api/oauth/authorize?{query}"))
            .await
    }

    /// Runs the IdP leg and returns the id of the request to answer.
    async fn sign_in_with(&mut self, params: &[(&str, String)]) -> String {
        let login = self.authorize(params).await;
        assert_eq!(login.status, StatusCode::SEE_OTHER);
        assert!(
            login
                .location()
                .starts_with("https://idp.example/authorize"),
            "{}",
            login.location()
        );
        let state = query(login.location(), "state").expect("a login state");
        let back = self
            .browser
            .get(&format!("/api/auth/callback?code=test-code&state={state}"))
            .await;
        query(back.location(), "request").expect("a consent request")
    }

    async fn sign_in(&mut self) -> String {
        self.sign_in_with(&self.params()).await
    }

    async fn answer_from(&mut self, origin: Option<&str>, id: &str, allow: bool) -> Page {
        let mut request = Request::post(format!("/api/oauth/consent/{id}"))
            .header(header::CONTENT_TYPE, "application/json");
        if let Some(origin) = origin {
            request = request.header(header::ORIGIN, origin);
        }
        self.browser
            .send(
                request
                    .body(Body::from(json!({ "allow": allow }).to_string()))
                    .expect("request"),
            )
            .await
    }

    async fn answer(&mut self, id: &str, allow: bool) -> Page {
        self.answer_from(Some(ISSUER), id, allow).await
    }

    async fn code(&mut self) -> String {
        let id = self.sign_in().await;
        let page = self.answer(&id, true).await;
        assert_eq!(page.status, StatusCode::OK);
        let redirect = page.json()["redirect"]
            .as_str()
            .expect("a redirect")
            .to_owned();
        query(&redirect, "code").expect("a code")
    }

    fn trade_params(&self, code: &str) -> Vec<(&'static str, String)> {
        vec![
            ("grant_type", "authorization_code".to_owned()),
            ("code", code.to_owned()),
            ("redirect_uri", CALLBACK.to_owned()),
            ("client_id", self.client_id.clone()),
            ("code_verifier", self.verifier.clone()),
            ("resource", RESOURCE.to_owned()),
        ]
    }

    async fn trade_with(&mut self, params: &[(&str, String)]) -> Page {
        let body = Serializer::new(String::new()).extend_pairs(params).finish();
        self.browser
            .send(
                Request::post("/api/oauth/token")
                    .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
                    .body(Body::from(body))
                    .expect("request"),
            )
            .await
    }

    async fn token(&mut self) -> String {
        let code = self.code().await;
        let page = self.trade_with(&self.trade_params(&code)).await;
        assert_eq!(page.status, StatusCode::OK);
        page.json()["access_token"]
            .as_str()
            .expect("a token")
            .to_owned()
    }

    async fn call_mcp(&mut self, token: &str) -> Page {
        self.browser.send(bearing(explain(), token)).await
    }
}

fn with(
    mut params: Vec<(&'static str, String)>,
    name: &'static str,
    value: &str,
) -> Vec<(&'static str, String)> {
    params.retain(|(key, _)| *key != name);
    params.push((name, value.to_owned()));
    params
}

fn without(mut params: Vec<(&'static str, String)>, name: &str) -> Vec<(&'static str, String)> {
    params.retain(|(key, _)| *key != name);
    params
}

#[tokio::test]
async fn an_agent_signs_in_through_the_login_and_calls_mcp_with_its_token() {
    let mut agent = Agent::registered().await;

    let id = agent.sign_in().await;
    let shown = agent.browser.get(&format!("/api/oauth/consent/{id}")).await;
    assert_eq!(shown.status, StatusCode::OK);
    assert_eq!(
        shown.json(),
        json!({
            "client": "Claude Code",
            "redirect": { "kind": "loopback", "port": 54321 },
            "user": { "sub": "user-1", "email": "user@example.com", "name": "Test User" },
            "clusters": [{
                "cluster": "local",
                "roles": ["operator"],
                "privileges": ["RECORDS", "ACLS"],
                "writable": false,
            }],
            "lifetime": 43_200,
        })
    );

    let answered = agent.answer(&id, true).await;
    assert_eq!(answered.status, StatusCode::OK);
    let redirect = answered.json()["redirect"]
        .as_str()
        .expect("a redirect")
        .to_owned();
    assert!(
        redirect.starts_with(&format!("{CALLBACK}?code=klens_code_")),
        "{redirect}"
    );
    assert_eq!(query(&redirect, "state").as_deref(), Some("st-1"));
    assert_eq!(query(&redirect, "iss").as_deref(), Some(ISSUER));

    let code = query(&redirect, "code").expect("a code");
    let traded = agent.trade_with(&agent.trade_params(&code)).await;
    assert_eq!(traded.status, StatusCode::OK);
    assert_eq!(traded.headers[header::CACHE_CONTROL], "no-store");
    let issued = traded.json();
    let token = issued["access_token"].as_str().expect("a token");
    assert!(token.starts_with("klens_mcp_"), "{token}");
    assert_eq!(issued["token_type"], "Bearer");
    let expires_in = issued["expires_in"].as_i64().expect("a lifetime");
    assert!((43_190..=43_200).contains(&expires_in), "{expires_in}");

    let called = agent.call_mcp(token).await;
    assert_eq!(called.status, StatusCode::OK);
    let explained = &called.json()["result"]["structuredContent"]["clusters"][0];
    assert_eq!(explained["privileges"], json!(["RECORDS", "ACLS"]));
}

#[tokio::test]
async fn authorize_refuses_a_bad_client_or_redirect_without_redirecting() {
    let mut agent = Agent::registered().await;
    let params = agent.params();
    let forged = format!(
        "klens_client_{}",
        base64::Engine::encode(
            &base64::engine::general_purpose::URL_SAFE_NO_PAD,
            br#"{"r":["https://evil.example/callback"]}"#,
        )
    );

    for params in [
        with(params.clone(), "client_id", "klens-mcp"),
        with(params.clone(), "client_id", &forged),
        without(params.clone(), "client_id"),
        with(
            params.clone(),
            "redirect_uri",
            "https://evil.example/callback",
        ),
        with(
            params.clone(),
            "redirect_uri",
            "http://localhost:54321/other",
        ),
        without(params.clone(), "redirect_uri"),
    ] {
        let page = agent.authorize(&params).await;

        assert_eq!(page.status, StatusCode::SEE_OTHER);
        assert_eq!(page.location(), BROKEN, "{params:?}");
    }
}

#[tokio::test]
async fn authorize_sends_other_errors_back_to_the_client() {
    let mut agent = Agent::registered().await;
    let params = agent.params();

    for (params, error) in [
        (
            with(params.clone(), "response_type", "token"),
            "unsupported_response_type",
        ),
        (
            with(params.clone(), "code_challenge_method", "plain"),
            "invalid_request",
        ),
        (
            without(params.clone(), "code_challenge_method"),
            "invalid_request",
        ),
        (
            with(params.clone(), "code_challenge", "short"),
            "invalid_request",
        ),
        (without(params.clone(), "code_challenge"), "invalid_request"),
        (
            with(params.clone(), "resource", "https://other.example/mcp"),
            "invalid_target",
        ),
    ] {
        let page = agent.authorize(&params).await;

        assert_eq!(page.status, StatusCode::SEE_OTHER);
        let location = page.location();
        assert!(location.starts_with(CALLBACK), "{location}");
        assert_eq!(
            query(location, "error").as_deref(),
            Some(error),
            "{location}"
        );
        assert_eq!(query(location, "state").as_deref(), Some("st-1"));
        assert_eq!(query(location, "iss").as_deref(), Some(ISSUER));
    }
}

#[tokio::test]
async fn authorize_takes_every_base64url_character_in_a_challenge() {
    let mut agent = Agent::registered().await;
    let challenge = format!("-_{}", "a".repeat(41));
    let padded = format!("{}=", "a".repeat(42));

    let taken = agent
        .authorize(&with(agent.params(), "code_challenge", &challenge))
        .await;
    let refused = agent
        .authorize(&with(agent.params(), "code_challenge", &padded))
        .await;

    assert!(
        taken
            .location()
            .starts_with("https://idp.example/authorize"),
        "{}",
        taken.location()
    );
    assert_eq!(
        query(refused.location(), "error").as_deref(),
        Some("invalid_request")
    );
}

#[tokio::test]
async fn a_loopback_redirect_may_name_another_port() {
    let mut agent = Agent::registered().await;
    let elsewhere = "http://localhost:60000/callback";

    let id = agent
        .sign_in_with(&with(agent.params(), "redirect_uri", elsewhere))
        .await;
    let page = agent.answer(&id, true).await;

    let redirect = page.json()["redirect"]
        .as_str()
        .expect("a redirect")
        .to_owned();
    assert!(
        redirect.starts_with(&format!("{elsewhere}?code=")),
        "{redirect}"
    );
}

#[tokio::test]
async fn only_the_browser_that_signed_in_may_answer() {
    let mut agent = Agent::registered().await;
    let id = agent.sign_in().await;
    let mut stranger = Browser::serving(agent.auth.clone(), Some(&mcp("")));

    let anonymous = stranger.get(&format!("/api/oauth/consent/{id}")).await;
    assert_eq!(anonymous.status, StatusCode::UNAUTHORIZED);
    stranger.log_in("test-code").await;
    let elsewhere = stranger.get(&format!("/api/oauth/consent/{id}")).await;
    assert_eq!(elsewhere.status, StatusCode::NOT_FOUND);

    let page = agent.answer(&id, true).await;
    assert_eq!(page.status, StatusCode::OK);
}

#[tokio::test]
async fn an_answer_goes_to_the_request_it_names_for_the_person_who_signed_in() {
    let mut agent = Agent::registered().await;
    let first = agent.sign_in().await;
    let second = agent
        .sign_in_with(&with(agent.params(), "state", "st-2"))
        .await;

    let page = agent.answer(&second, true).await;
    let redirect = page.json()["redirect"]
        .as_str()
        .expect("a redirect")
        .to_owned();
    assert_eq!(query(&redirect, "state").as_deref(), Some("st-2"));
    agent
        .browser
        .impersonate(&SessionUser::new(
            "user-2",
            None,
            None,
            vec!["ops".to_owned()],
            jiff::Timestamp::now().as_second() + 3_600,
        ))
        .await;

    let shown = agent
        .browser
        .get(&format!("/api/oauth/consent/{first}"))
        .await;
    assert_eq!(shown.status, StatusCode::NOT_FOUND);
    let answered = agent.answer(&first, true).await;
    assert_eq!(answered.status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn an_answer_must_come_from_klens_itself() {
    let mut agent = Agent::registered().await;
    let id = agent.sign_in().await;

    for origin in [None, Some("https://evil.example"), Some("null")] {
        let page = agent.answer_from(origin, &id, true).await;

        assert_eq!(page.status, StatusCode::FORBIDDEN, "{origin:?}");
    }
    assert_eq!(agent.answer(&id, true).await.status, StatusCode::OK);
}

#[tokio::test]
async fn a_denial_sends_access_denied_back_and_ends_the_request() {
    let mut agent = Agent::registered().await;
    let id = agent.sign_in().await;

    let page = agent.answer(&id, false).await;

    assert_eq!(page.status, StatusCode::OK);
    let redirect = page.json()["redirect"]
        .as_str()
        .expect("a redirect")
        .to_owned();
    assert!(redirect.starts_with(CALLBACK), "{redirect}");
    assert_eq!(query(&redirect, "error").as_deref(), Some("access_denied"));
    assert_eq!(query(&redirect, "state").as_deref(), Some("st-1"));
    assert_eq!(query(&redirect, "code"), None);
    assert_eq!(agent.answer(&id, true).await.status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn a_bad_trade_is_refused_and_spends_the_code() {
    let mut agent = Agent::registered().await;
    let other = PkceCodeChallenge::new_random_sha256().1.secret().clone();

    for (name, value, error) in [
        ("code_verifier", other.as_str(), "invalid_grant"),
        ("code_verifier", "short", "invalid_grant"),
        (
            "redirect_uri",
            "http://localhost:60000/callback",
            "invalid_grant",
        ),
        ("client_id", "klens-mcp", "invalid_grant"),
        ("resource", "https://other.example/mcp", "invalid_target"),
    ] {
        let code = agent.code().await;
        let params = agent.trade_params(&code);

        let page = agent.trade_with(&with(params.clone(), name, value)).await;

        assert_eq!(page.status, StatusCode::BAD_REQUEST, "{name}");
        assert_eq!(page.json()["error"], error, "{name}");
        let again = agent.trade_with(&params).await;
        assert_eq!(again.json()["error"], "invalid_grant", "{name}");
    }
}

#[tokio::test]
async fn the_token_endpoint_takes_only_a_code_in_a_form() {
    let mut agent = Agent::registered().await;
    let code = agent.code().await;
    let params = agent.trade_params(&code);

    let refresh = agent
        .trade_with(&with(params.clone(), "grant_type", "refresh_token"))
        .await;
    assert_eq!(refresh.json()["error"], "unsupported_grant_type");
    let json = agent
        .browser
        .send(
            Request::post("/api/oauth/token")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(json!({ "code": code }).to_string()))
                .expect("request"),
        )
        .await;
    assert_eq!(json.status, StatusCode::BAD_REQUEST);
    assert_eq!(json.json()["error"], "invalid_request");

    assert_eq!(agent.trade_with(&params).await.status, StatusCode::OK);
}

#[tokio::test]
async fn a_code_used_twice_revokes_its_token() {
    let mut agent = Agent::registered().await;
    let code = agent.code().await;
    let params = agent.trade_params(&code);
    let first = agent.trade_with(&params).await.json();
    let token = first["access_token"].as_str().expect("a token");
    assert_eq!(agent.call_mcp(token).await.status, StatusCode::OK);

    let second = agent.trade_with(&params).await;

    assert_eq!(second.json()["error"], "invalid_grant");
    let refused = agent.call_mcp(token).await;
    assert_eq!(refused.status, StatusCode::UNAUTHORIZED);
    let challenge = refused.headers[header::WWW_AUTHENTICATE]
        .to_str()
        .expect("a challenge");
    assert!(
        challenge.contains(r#"error="invalid_token""#),
        "{challenge}"
    );
}

#[tokio::test]
async fn a_token_opens_mcp_and_nothing_else() {
    let mut agent = Agent::registered().await;
    let token = agent.token().await;
    let mut stranger = Browser::serving(agent.auth.clone(), Some(&mcp("")));

    let api = stranger
        .send(bearing(
            Request::get("/api/whoami")
                .body(Body::empty())
                .expect("request"),
            &token,
        ))
        .await;

    assert_eq!(api.status, StatusCode::UNAUTHORIZED);
    assert_eq!(
        stranger.send(bearing(explain(), &token)).await.status,
        StatusCode::OK
    );
}

#[tokio::test]
async fn only_a_token_klens_issued_opens_mcp() {
    let mut agent = Agent::registered().await;
    let token = agent.token().await;

    let forged = agent.call_mcp(&format!("{token}x")).await;
    let other = agent.call_mcp("klens_mcp_forged").await;

    assert_eq!(forged.status, StatusCode::UNAUTHORIZED);
    assert_eq!(other.status, StatusCode::UNAUTHORIZED);
    assert_eq!(agent.call_mcp(&token).await.status, StatusCode::OK);
}

#[tokio::test]
async fn a_restart_forgets_every_token() {
    let mut agent = Agent::registered().await;
    let token = agent.token().await;
    let mut restarted = Browser::serving(auth(&mcp("")), Some(&mcp("")));

    let page = restarted.send(bearing(explain(), &token)).await;

    assert_eq!(page.status, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn the_consent_lists_what_the_ceiling_leaves() {
    let mut agent = Agent::registered_with(&mcp("privileges: [acls]")).await;

    let id = agent.sign_in().await;
    let shown = agent.browser.get(&format!("/api/oauth/consent/{id}")).await;

    assert_eq!(shown.json()["clusters"][0]["privileges"], json!(["ACLS"]));
}

#[test]
fn each_redirect_shows_as_the_kind_of_place_it_leads() {
    use super::super::consent::RedirectTarget;
    let value = |uri: &str| {
        let target = RedirectTarget::of(&Url::parse(uri).expect("a url"));
        serde_json::to_value(target).expect("json")
    };

    assert_eq!(
        value("http://127.0.0.1:33418/"),
        json!({ "kind": "loopback", "port": 33418 })
    );
    assert_eq!(
        value("cursor://anysphere.cursor-retrieval/oauth/cb"),
        json!({ "kind": "app", "scheme": "cursor" })
    );
    assert_eq!(
        value("https://claude.ai/api/mcp/auth_callback"),
        json!({ "kind": "site", "host": "claude.ai" })
    );
}
