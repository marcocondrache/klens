use axum::extract::Request;
use axum::http::{HeaderMap, HeaderName, Method};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};

use super::error::ApiError;

const SEC_FETCH_SITE: HeaderName = HeaderName::from_static("sec-fetch-site");
const SEC_FETCH_MODE: HeaderName = HeaderName::from_static("sec-fetch-mode");
const SEC_FETCH_DEST: HeaderName = HeaderName::from_static("sec-fetch-dest");

/// The session cookie is `SameSite=Lax`, and without auth there is no cookie
/// at all, so another site's page can still make a visitor's browser send
/// `GET`s that start an export or hold every live tail. Browsers name where a
/// request came from in `Sec-Fetch-Site`, and klens refuses one from another
/// site unless it opens a page, as the identity provider's redirect back to
/// the login callback does. A client that sends no such header passes.
pub async fn refuse_cross_site(request: Request, next: Next) -> Response {
    if sent_by_another_site(request.method(), request.headers()) {
        return ApiError::CrossSite.into_response();
    }
    next.run(request).await
}

fn sent_by_another_site(method: &Method, headers: &HeaderMap) -> bool {
    let header = |name| headers.get(name).map(|value| value.as_bytes());
    let foreign = matches!(header(SEC_FETCH_SITE), Some(b"cross-site" | b"same-site"));
    let opens_a_page = method == Method::GET
        && header(SEC_FETCH_MODE) == Some(b"navigate")
        && header(SEC_FETCH_DEST) == Some(b"document");

    foreign && !opens_a_page
}
