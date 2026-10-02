use axum::extract::Request;
use axum::http::{HeaderMap, header};
use axum::middleware::Next;
use axum::response::Response;

use super::error::ApiError;

/// Refuses a request that changes state when the browser says another site
/// sent it. Clients that send neither header, such as curl, pass.
pub(crate) async fn refuse_cross_site(request: Request, next: Next) -> Result<Response, ApiError> {
    if request.method().is_safe() || same_origin(request.headers()) {
        Ok(next.run(request).await)
    } else {
        Err(ApiError::CrossSite)
    }
}

fn same_origin(headers: &HeaderMap) -> bool {
    if let Some(site) = headers.get("sec-fetch-site") {
        return site == "same-origin" || site == "none";
    }
    let Some(origin) = headers.get(header::ORIGIN) else {
        return true;
    };
    let authority = origin
        .to_str()
        .ok()
        .and_then(|origin| origin.split_once("://"))
        .map(|(_, authority)| authority);
    authority.is_some_and(|authority| {
        headers
            .get(header::HOST)
            .is_some_and(|host| host == authority)
    })
}

#[cfg(test)]
mod tests {
    use axum::body::{Body, to_bytes};
    use axum::http::{Method, Request, StatusCode};
    use tower::ServiceExt;

    use crate::AppState;
    use crate::app::{AuthState, Limits};
    use crate::config::Tuning;
    use crate::kafka::{Clusters, FakeCluster};

    async fn send(method: Method, path: &str, headers: &[(&str, &str)]) -> (StatusCode, String) {
        let app = crate::app::router(AppState::new(
            Clusters::from_sessions(vec![FakeCluster::local()]),
            AuthState::disabled(),
            Limits::new(&Tuning::default()),
        ));
        let mut request = Request::builder().method(method).uri(path);
        for (name, value) in headers {
            request = request.header(*name, *value);
        }
        let response = app
            .oneshot(request.body(Body::empty()).unwrap())
            .await
            .unwrap();
        let status = response.status();
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        (status, String::from_utf8(body.to_vec()).unwrap())
    }

    async fn logout(headers: &[(&str, &str)]) -> (StatusCode, String) {
        send(Method::POST, "/api/auth/logout", headers).await
    }

    #[tokio::test]
    async fn a_cross_site_change_is_refused() {
        for site in ["cross-site", "same-site"] {
            let (status, body) = logout(&[("sec-fetch-site", site)]).await;
            assert_eq!(status, StatusCode::FORBIDDEN, "{site}");
            assert_eq!(
                body,
                r#"{"error":"cross-site requests cannot change anything","code":"CROSS_SITE"}"#
            );
        }
    }

    #[tokio::test]
    async fn a_same_origin_change_passes() {
        for site in ["same-origin", "none"] {
            let (status, _) = logout(&[("sec-fetch-site", site)]).await;
            assert_eq!(status, StatusCode::NO_CONTENT, "{site}");
        }
    }

    #[tokio::test]
    async fn fetch_metadata_wins_over_the_origin() {
        let (status, _) = logout(&[
            ("sec-fetch-site", "same-origin"),
            ("origin", "https://elsewhere.example"),
            ("host", "klens.example"),
        ])
        .await;
        assert_eq!(status, StatusCode::NO_CONTENT);
    }

    #[tokio::test]
    async fn without_fetch_metadata_the_origin_must_match_the_host() {
        let host = ("host", "klens.example:8080");
        let cases = [
            ("http://klens.example:8080", StatusCode::NO_CONTENT),
            ("https://klens.example:8080", StatusCode::NO_CONTENT),
            ("http://klens.example", StatusCode::FORBIDDEN),
            ("http://evil.example:8080", StatusCode::FORBIDDEN),
            ("null", StatusCode::FORBIDDEN),
        ];
        for (origin, expected) in cases {
            let (status, _) = logout(&[("origin", origin), host]).await;
            assert_eq!(status, expected, "{origin}");
        }
        let (status, _) = logout(&[("origin", "http://klens.example:8080")]).await;
        assert_eq!(status, StatusCode::FORBIDDEN, "no host to match");
    }

    #[tokio::test]
    async fn a_client_without_either_header_passes() {
        let (status, _) = logout(&[]).await;
        assert_eq!(status, StatusCode::NO_CONTENT);
    }

    #[tokio::test]
    async fn a_cross_site_read_passes() {
        let (status, _) = send(
            Method::GET,
            "/api/clusters",
            &[("sec-fetch-site", "cross-site")],
        )
        .await;
        assert_eq!(status, StatusCode::OK);
    }
}
