use axum::extract::Request;
use axum::http::HeaderName;
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};

use super::error::ApiError;

const SEC_FETCH_SITE: HeaderName = HeaderName::from_static("sec-fetch-site");

/// The session cookie is `SameSite=Lax`, and without auth there is no cookie
/// at all, so another site's page can still make a visitor's browser send
/// `GET`s that start an export or hold every live tail, through an image, a
/// link or a prefetch. Browsers name where a request came from in
/// `Sec-Fetch-Site` when klens is served over https or on localhost, and only
/// klens' own UI calls the resource routes, so one from another site is
/// refused. A client that sends no such header passes.
pub async fn refuse_cross_site(request: Request, next: Next) -> Response {
    let site = request.headers().get(SEC_FETCH_SITE);
    if site.is_some_and(|site| matches!(site.as_bytes(), b"cross-site" | b"same-site")) {
        return ApiError::CrossSite.into_response();
    }
    next.run(request).await
}
