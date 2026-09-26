use axum::extract::Request;
use axum::http::{HeaderMap, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};

use super::error::ApiError;

/// `SameSite=Lax` keeps the session cookie off cross-site requests, but a
/// deployment without login has no cookie to withhold, and a sibling
/// subdomain counts as the same site.
pub(crate) async fn reject_cross_origin(request: Request, next: Next) -> Response {
    if !request.method().is_safe() && browser_sent_cross_origin(request.headers()) {
        return ApiError::CrossOrigin.into_response();
    }
    next.run(request).await
}

fn browser_sent_cross_origin(headers: &HeaderMap) -> bool {
    if let Some(site) = headers.get("sec-fetch-site") {
        return !matches!(site.as_bytes(), b"same-origin" | b"none");
    }
    let Some(origin) = headers.get(header::ORIGIN) else {
        return false;
    };
    let authority = origin
        .to_str()
        .ok()
        .and_then(|origin| origin.split_once("://"))
        .map(|(_, authority)| authority);
    authority
        != headers
            .get(header::HOST)
            .and_then(|host| host.to_str().ok())
}

#[cfg(test)]
mod tests {
    use axum::http::HeaderValue;

    use super::*;

    fn headers(pairs: &[(&'static str, &'static str)]) -> HeaderMap {
        pairs
            .iter()
            .map(|(name, value)| {
                (
                    header::HeaderName::from_static(name),
                    HeaderValue::from_static(value),
                )
            })
            .collect()
    }

    #[test]
    fn fetch_metadata_decides_when_present() {
        assert!(!browser_sent_cross_origin(&headers(&[(
            "sec-fetch-site",
            "same-origin"
        )])));
        assert!(!browser_sent_cross_origin(&headers(&[(
            "sec-fetch-site",
            "none"
        )])));
        assert!(browser_sent_cross_origin(&headers(&[(
            "sec-fetch-site",
            "same-site"
        )])));
        assert!(browser_sent_cross_origin(&headers(&[
            ("sec-fetch-site", "cross-site"),
            ("origin", "http://klens.local"),
            ("host", "klens.local"),
        ])));
    }

    #[test]
    fn origin_must_match_host_without_fetch_metadata() {
        assert!(!browser_sent_cross_origin(&headers(&[
            ("origin", "https://klens.example.com:8443"),
            ("host", "klens.example.com:8443"),
        ])));
        assert!(browser_sent_cross_origin(&headers(&[
            ("origin", "https://evil.example.com"),
            ("host", "klens.example.com"),
        ])));
        assert!(browser_sent_cross_origin(&headers(&[
            ("origin", "null"),
            ("host", "klens.example.com")
        ])));
    }

    #[test]
    fn a_request_without_browser_headers_passes() {
        assert!(!browser_sent_cross_origin(&headers(&[(
            "host",
            "klens.example.com"
        )])));
    }
}
