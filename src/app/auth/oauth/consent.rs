use axum::Json;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Redirect, Response};
use axum_extra::extract::{Query, QueryRejection};
use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use jiff::Timestamp;
use serde::{Deserialize, Serialize};
use tower_sessions::Session;
use ts_rs::TS;
use url::{Host, Url};

use super::client::Client;
use super::token::Approval;
use crate::AppState;
use crate::app::auth::access::EffectiveAccess;
use crate::app::auth::{AuthSession, AuthUser, Then, start_login};
use crate::app::error::ApiError;
use crate::app::resources::whoami::ClusterGrant;

const PENDING_KEY: &str = "klens.oauth_pending";

/// A person may sign in a few agents from one browser before answering for
/// one.
const MAX_PENDING: usize = 4;

/// The consent page shows why it has nothing to approve.
const BROKEN: &str = "/oauth/consent?error=request";

/// An authorization request, carried through the login.
#[derive(Debug, Serialize, Deserialize)]
pub(in crate::app::auth) struct Authorization {
    id: String,
    client_id: String,
    redirect_uri: Url,
    state: Option<String>,
    challenge: String,
}

/// A request kept in the browser's session until the person answers it.
#[derive(Serialize, Deserialize)]
struct Pending {
    authorization: Authorization,
    /// The subject who signed in for the request, the only one who may answer
    /// it.
    user: String,
}

impl Pending {
    fn answers(&self, id: &str, subject: &str) -> bool {
        self.authorization.id == id && self.user == subject
    }
}

#[derive(Deserialize)]
pub(super) struct AuthorizeQuery {
    response_type: Option<String>,
    client_id: Option<String>,
    redirect_uri: Option<String>,
    code_challenge: Option<String>,
    code_challenge_method: Option<String>,
    state: Option<String>,
    resource: Option<String>,
}

/// Checks the client and the redirect first and never sends the browser to the
/// client when either fails, so klens is no open redirect. Then it runs the
/// UI's login even for a signed-in browser, so the agent starts with the
/// groups the IdP sends now.
pub(super) async fn authorize(
    State(state): State<AppState>,
    auth_session: AuthSession,
    query: Result<Query<AuthorizeQuery>, QueryRejection>,
) -> Response {
    let Some(oauth) = state.auth.oauth() else {
        return ApiError::NotFound.into_response();
    };
    let Ok(Query(query)) = query else {
        return Redirect::to(BROKEN).into_response();
    };
    let Some(client_id) = query.client_id else {
        return Redirect::to(BROKEN).into_response();
    };
    let Some(redirect_uri) = query
        .redirect_uri
        .as_deref()
        .and_then(|uri| Client::decode(&client_id)?.redirect(uri))
    else {
        return Redirect::to(BROKEN).into_response();
    };
    let refuse = |error: &str, description: &str| {
        let params = [("error", error), ("error_description", description)];
        Redirect::to(
            oauth
                .back(&redirect_uri, query.state.as_deref(), &params)
                .as_str(),
        )
        .into_response()
    };
    if query.response_type.as_deref() != Some("code") {
        return refuse("unsupported_response_type", "klens issues only codes");
    }
    if query.code_challenge_method.as_deref() != Some("S256") {
        return refuse("invalid_request", "klens takes only S256 PKCE");
    }
    let Some(challenge) = query.code_challenge.filter(|c| is_s256_challenge(c)) else {
        return refuse(
            "invalid_request",
            "the code challenge is not an S256 digest",
        );
    };
    if query
        .resource
        .is_some_and(|resource| !oauth.serves(&resource))
    {
        return refuse(
            "invalid_target",
            "klens issues tokens only for its own /mcp",
        );
    }

    let authorization = Authorization {
        id: URL_SAFE_NO_PAD.encode(rand::random::<[u8; 16]>()),
        client_id,
        redirect_uri,
        state: query.state,
        challenge,
    };
    start_login(&state, &auth_session, Then::Consent(authorization)).await
}

/// RFC 7636 4.2: a base64url SHA-256 digest without padding.
fn is_s256_challenge(challenge: &str) -> bool {
    challenge.len() == 43
        && challenge
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_')
}

/// Binds the request to the person who signed in for it.
pub(in crate::app::auth) async fn signed_in(
    session: &Session,
    authorization: Authorization,
    subject: &str,
) -> Response {
    let consent = format!("/oauth/consent?request={}", authorization.id);
    let mut kept = load(session).await;
    kept.push(Pending {
        authorization,
        user: subject.to_owned(),
    });
    let excess = kept.len().saturating_sub(MAX_PENDING);
    kept.drain(..excess);
    if let Err(error) = session.insert(PENDING_KEY, kept).await {
        tracing::error!(%error, "failed to store an mcp authorization request");
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    }
    Redirect::to(&consent).into_response()
}

/// What the consent page shows before the person answers.
#[derive(Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct ConsentRequest {
    /// The name the client registered, which it chose itself.
    pub client: Option<String>,
    pub redirect: RedirectTarget,
    pub user: AuthUser,
    /// What the agent may do, under the ceiling the operator set for MCP.
    pub clusters: Vec<ClusterGrant>,
    /// How long the agent keeps this access, in seconds.
    pub lifetime: u64,
}

/// Where the browser goes once the person answers.
#[derive(Debug, PartialEq, Eq, Serialize, TS)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum RedirectTarget {
    /// An app on the person's computer, listening on this port.
    Loopback { port: u16 },
    /// A desktop app that the OS hands this URI scheme to.
    App { scheme: String },
    /// A hosted client's own server.
    Site { host: String },
}

impl RedirectTarget {
    pub(super) fn of(url: &Url) -> Self {
        match (url.scheme(), url.host()) {
            ("http", _) => Self::Loopback {
                port: url.port().unwrap_or(80),
            },
            ("https", Some(Host::Domain(host))) => Self::Site {
                host: host.to_owned(),
            },
            (scheme, _) => Self::App {
                scheme: scheme.to_owned(),
            },
        }
    }
}

pub(super) async fn show(
    State(state): State<AppState>,
    auth_session: AuthSession,
    Path(id): Path<String>,
) -> Result<Json<ConsentRequest>, ApiError> {
    let oauth = state.auth.oauth().ok_or(ApiError::NotFound)?;
    let user = auth_session.user.as_ref().ok_or(ApiError::Unauthorized)?;
    let Pending { authorization, .. } = load(&auth_session.session)
        .await
        .into_iter()
        .find(|pending| pending.answers(&id, &user.sub))
        .ok_or(ApiError::NotFound)?;
    let client = Client::decode(&authorization.client_id).ok_or(ApiError::NotFound)?;
    let access = state.auth.access_from_user(user).ok_or(ApiError::NoRole)?;
    let access = EffectiveAccess::from(access.narrow(&oauth.ceiling));
    let clusters = state
        .clusters
        .iter()
        .filter_map(|cluster| {
            let granted = access.cluster(cluster.name()).ok()?;
            Some(ClusterGrant {
                cluster: cluster.name().to_owned(),
                roles: granted
                    .role_names()
                    .into_iter()
                    .map(ToOwned::to_owned)
                    .collect(),
                privileges: granted.privileges(),
                writable: cluster.writable,
            })
        })
        .collect();
    Ok(Json(ConsentRequest {
        client: client.name,
        redirect: RedirectTarget::of(&authorization.redirect_uri),
        user: AuthUser::from(user.clone()),
        clusters,
        lifetime: oauth.lifetime.as_secs(),
    }))
}

#[derive(Deserialize)]
pub(super) struct Decision {
    allow: bool,
}

/// Where the consent page sends the browser after the person answers.
#[derive(Debug, Serialize, TS)]
pub struct Decided {
    pub redirect: String,
}

/// `fetch` cannot follow a redirect to an app's URI scheme, so the page
/// navigates to the returned URL itself.
pub(super) async fn decide(
    State(state): State<AppState>,
    auth_session: AuthSession,
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(decision): Json<Decision>,
) -> Result<Json<Decided>, ApiError> {
    let oauth = state.auth.oauth().ok_or(ApiError::NotFound)?;
    if headers
        .get(header::ORIGIN)
        .is_none_or(|origin| origin.as_bytes() != oauth.origin.as_bytes())
    {
        return Err(ApiError::InvalidRequest {
            status: StatusCode::FORBIDDEN,
            message: "an mcp client can only be approved from klens itself".to_owned(),
        });
    }
    let user = auth_session.user.as_ref().ok_or(ApiError::Unauthorized)?;
    let mut kept = load(&auth_session.session).await;
    let index = kept
        .iter()
        .position(|pending| pending.answers(&id, &user.sub))
        .ok_or(ApiError::NotFound)?;
    let Pending { authorization, .. } = kept.remove(index);
    if let Err(error) = auth_session.session.insert(PENDING_KEY, kept).await {
        tracing::error!(%error, "failed to store mcp authorization requests");
        return Err(ApiError::InvalidRequest {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            message: "failed to store the session".to_owned(),
        });
    }
    let client = Client::decode(&authorization.client_id).ok_or(ApiError::NotFound)?;
    let name = client.name.as_deref().unwrap_or_default();
    let redirect = if decision.allow {
        state.auth.access_from_user(user).ok_or(ApiError::NoRole)?;
        let code = oauth.grants.approve(Approval {
            client_id: authorization.client_id,
            client: client.name.clone(),
            redirect_uri: authorization.redirect_uri.clone(),
            challenge: authorization.challenge,
            user: user.sub.clone(),
            groups: user.groups.clone(),
            exp: Timestamp::now()
                .as_second()
                .saturating_add(i64::try_from(oauth.lifetime.as_secs()).unwrap_or(i64::MAX)),
        });
        tracing::info!(
            user = user.sub.as_str(),
            client = name,
            "approved an mcp client"
        );
        oauth.back(
            &authorization.redirect_uri,
            authorization.state.as_deref(),
            &[("code", &code)],
        )
    } else {
        tracing::info!(
            user = user.sub.as_str(),
            client = name,
            "denied an mcp client"
        );
        oauth.back(
            &authorization.redirect_uri,
            authorization.state.as_deref(),
            &[("error", "access_denied")],
        )
    };
    Ok(Json(Decided {
        redirect: redirect.into(),
    }))
}

async fn load(session: &Session) -> Vec<Pending> {
    session
        .get::<Vec<Pending>>(PENDING_KEY)
        .await
        .ok()
        .flatten()
        .unwrap_or_default()
}
