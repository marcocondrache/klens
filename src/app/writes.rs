//! What every write endpoint shares: the body extractor, the confirmation
//! check for irreversible writes, the audit log, and the guard against
//! cross-site requests.

use axum::Json;
use axum::extract::{FromRequest, Request};
use axum::http::{HeaderMap, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use serde::de::DeserializeOwned;

use super::context::Session;
use super::error::ApiError;

/// Log target for the audit trail: one event per write attempt.
const AUDIT_TARGET: &str = "klens::audit";

/// A JSON request body whose rejections answer in the API's error shape.
pub(crate) struct JsonBody<T>(pub T);

impl<T: DeserializeOwned, S: Send + Sync> FromRequest<S> for JsonBody<T> {
    type Rejection = ApiError;

    async fn from_request(request: Request, state: &S) -> Result<Self, Self::Rejection> {
        match Json::<T>::from_request(request, state).await {
            Ok(Json(body)) => Ok(Self(body)),
            Err(rejection) => Err(ApiError::InvalidBody {
                status: rejection.status(),
                message: rejection.body_text(),
            }),
        }
    }
}

/// Irreversible writes carry the name of what they destroy, so a stray
/// request or a slipped click cannot run one.
pub(crate) fn confirm(given: &str, expected: &str, what: &'static str) -> Result<(), ApiError> {
    if given == expected {
        Ok(())
    } else {
        Err(ApiError::ConfirmationRequired(what))
    }
}

/// One write attempt, logged under [`AUDIT_TARGET`] whatever its outcome.
pub(crate) struct Audit<'a> {
    pub subject: Option<&'a str>,
    pub cluster: &'a str,
    pub action: &'static str,
    pub resource: &'a str,
    pub dry_run: bool,
}

impl<'a> Audit<'a> {
    pub(crate) fn new(
        session: &'a Session,
        cluster: &'a str,
        action: &'static str,
        resource: &'a str,
    ) -> Self {
        Self {
            subject: session.guard.subject(),
            cluster,
            action,
            resource,
            dry_run: false,
        }
    }

    pub(crate) fn dry_run(self, dry_run: bool) -> Self {
        Self { dry_run, ..self }
    }

    /// Logs the outcome and hands it back unchanged. Never logs payloads.
    pub(crate) fn record<T>(self, outcome: Result<T, ApiError>) -> Result<T, ApiError> {
        let subject = self.subject.unwrap_or("anonymous");
        match &outcome {
            Ok(_) => tracing::info!(
                target: AUDIT_TARGET,
                subject,
                cluster = self.cluster,
                action = self.action,
                // Debug-quoted: a group id may hold a newline.
                resource = ?self.resource,
                dry_run = self.dry_run,
                outcome = "ok",
                "write"
            ),
            Err(error) => tracing::info!(
                target: AUDIT_TARGET,
                subject,
                cluster = self.cluster,
                action = self.action,
                resource = ?self.resource,
                dry_run = self.dry_run,
                outcome = "failed",
                code = error.code(),
                "write"
            ),
        }
        outcome
    }
}

/// Refuses a request that could change something when a browser says another
/// site sent it.
///
/// `SameSite=Lax` keeps the session cookie off cross-site POSTs, but with
/// authentication off there is no cookie to withhold. Browsers mark every
/// request with `Sec-Fetch-Site`; older ones only send `Origin`, which must
/// then match the host. A request with neither did not come from a web page.
pub(crate) async fn reject_cross_site(request: Request, next: Next) -> Response {
    if request.method().is_safe() || same_site(request.headers()) {
        return next.run(request).await;
    }
    ApiError::CrossSite.into_response()
}

fn same_site(headers: &HeaderMap) -> bool {
    if let Some(site) = headers.get("sec-fetch-site") {
        return matches!(site.as_bytes(), b"same-origin" | b"none");
    }
    let Some(origin) = headers.get(header::ORIGIN) else {
        return true;
    };
    let Some(origin) = origin
        .to_str()
        .ok()
        .and_then(|origin| url::Url::parse(origin).ok())
    else {
        return false;
    };
    let Some(host) = origin.host_str() else {
        return false;
    };
    let authority = match origin.port() {
        Some(port) => format!("{host}:{port}"),
        None => host.to_owned(),
    };
    [header::HOST.as_str(), "x-forwarded-host"]
        .into_iter()
        .filter_map(|name| headers.get(name)?.to_str().ok())
        .any(|value| value.eq_ignore_ascii_case(&authority))
}

#[cfg(test)]
mod tests {
    use std::io;
    use std::sync::{Arc, Mutex};

    use axum::http::HeaderValue;

    use super::*;

    #[derive(Clone, Default)]
    struct LogBuf(Arc<Mutex<Vec<u8>>>);

    impl io::Write for LogBuf {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.0.lock().expect("log buf").extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for LogBuf {
        type Writer = Self;

        fn make_writer(&'a self) -> Self::Writer {
            self.clone()
        }
    }

    fn audited(audit: Audit<'_>, outcome: Result<(), ApiError>) -> String {
        let logs = LogBuf::default();
        let subscriber = tracing_subscriber::fmt()
            .with_writer(logs.clone())
            .with_target(true)
            .with_ansi(false)
            .without_time()
            .finish();
        tracing::subscriber::with_default(subscriber, || {
            let _outcome = audit.record(outcome);
        });
        String::from_utf8_lossy(&logs.0.lock().expect("log buf")).into_owned()
    }

    fn audit(resource: &str) -> Audit<'_> {
        Audit {
            subject: None,
            cluster: "staging",
            action: "group_offsets.reset",
            resource,
            dry_run: true,
        }
    }

    #[test]
    fn a_write_is_audited_with_who_what_and_how_it_went() {
        let ok = audited(audit("billing"), Ok(()));
        let failed = audited(
            Audit {
                subject: Some("alice"),
                ..audit("billing")
            },
            Err(ApiError::CrossSite),
        );

        assert!(ok.contains("klens::audit"), "{ok}");
        for field in [
            "subject=\"anonymous\"",
            "cluster=\"staging\"",
            "action=\"group_offsets.reset\"",
            "resource=\"billing\"",
            "dry_run=true",
            "outcome=\"ok\"",
        ] {
            assert!(ok.contains(field), "{field} missing from {ok}");
        }
        assert!(failed.contains("subject=\"alice\""), "{failed}");
        assert!(failed.contains("outcome=\"failed\""), "{failed}");
        assert!(failed.contains("code=\"CROSS_SITE\""), "{failed}");
    }

    #[test]
    fn an_audited_resource_cannot_forge_a_log_line() {
        let text = audited(audit("billing\nINFO forged"), Ok(()));

        assert_eq!(text.lines().count(), 1, "{text}");
        assert!(
            text.contains(r#"resource="billing\nINFO forged""#),
            "{text}"
        );
    }

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
        assert!(same_site(&headers(&[("sec-fetch-site", "same-origin")])));
        assert!(same_site(&headers(&[("sec-fetch-site", "none")])));
        assert!(!same_site(&headers(&[("sec-fetch-site", "same-site")])));
        assert!(!same_site(&headers(&[
            ("sec-fetch-site", "cross-site"),
            ("origin", "http://klens:8080"),
            ("host", "klens:8080"),
        ])));
    }

    #[test]
    fn origin_must_match_the_host_without_fetch_metadata() {
        assert!(same_site(&headers(&[
            ("origin", "http://klens:8080"),
            ("host", "klens:8080"),
        ])));
        assert!(same_site(&headers(&[
            ("origin", "https://klens.example.com"),
            ("host", "KLENS.example.com"),
        ])));
        assert!(!same_site(&headers(&[
            ("origin", "http://evil.example:8080"),
            ("host", "klens:8080"),
        ])));
        assert!(!same_site(&headers(&[
            ("origin", "http://klens:9999"),
            ("host", "klens:8080"),
        ])));
        assert!(!same_site(&headers(&[
            ("origin", "null"),
            ("host", "klens:8080"),
        ])));
    }

    #[test]
    fn a_proxy_may_forward_the_public_host() {
        assert!(same_site(&headers(&[
            ("origin", "https://klens.example.com"),
            ("host", "klens.internal:8080"),
            ("x-forwarded-host", "klens.example.com"),
        ])));
    }

    #[test]
    fn a_request_without_origin_is_not_from_a_page() {
        assert!(same_site(&HeaderMap::new()));
    }

    #[test]
    fn confirmation_must_repeat_the_name_exactly() {
        assert!(confirm("billing", "billing", "group id").is_ok());
        let error = confirm("Billing", "billing", "group id").unwrap_err();
        assert_eq!(error.code(), "CONFIRMATION_REQUIRED");
        assert_eq!(
            error.to_string(),
            "confirm must repeat the group id exactly"
        );
        assert!(confirm("", "billing", "group id").is_err());
    }
}
