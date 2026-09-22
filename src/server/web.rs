use axum::{
    http::Uri,
    response::{IntoResponse, Response},
};

#[cfg(feature = "ui")]
mod embedded {
    use std::borrow::Cow;
    use std::fmt::Write;

    use super::*;
    use axum::{
        body::{Body, Bytes},
        http::{HeaderMap, HeaderValue, StatusCode, header},
    };
    use rust_embed::{EmbeddedFile, RustEmbed};

    #[derive(RustEmbed)]
    #[folder = "static/"]
    struct WebAssets;

    pub async fn serve(uri: Uri, headers: HeaderMap) -> Response {
        let path = uri.path().trim_start_matches('/');

        match WebAssets::get(path).filter(|_| !path.is_empty()) {
            Some(file) => {
                let cache = path
                    .starts_with("assets/")
                    .then_some(crate::environment::STATIC_ASSET_CACHE_CONTROL);
                let mime = mime_guess::from_path(path).first_or_octet_stream();
                respond(file, mime.as_ref(), cache, &headers)
            }
            None => match WebAssets::get("index.html") {
                Some(file) => respond(
                    file,
                    "text/html; charset=utf-8",
                    Some(crate::environment::INDEX_CACHE_CONTROL),
                    &headers,
                ),
                None => StatusCode::NOT_FOUND.into_response(),
            },
        }
    }

    fn respond(
        file: EmbeddedFile,
        content_type: &str,
        cache: Option<&'static str>,
        request: &HeaderMap,
    ) -> Response {
        let etag = etag(&file.metadata.sha256_hash());
        let fresh = request
            .get(header::IF_NONE_MATCH)
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| matches(value, &etag));

        let mut response = if fresh {
            StatusCode::NOT_MODIFIED.into_response()
        } else {
            let body = match file.data {
                Cow::Borrowed(data) => Bytes::from_static(data),
                Cow::Owned(data) => Bytes::from(data),
            };
            ([(header::CONTENT_TYPE, content_type)], Body::from(body)).into_response()
        };

        let headers = response.headers_mut();
        if let Some(cache) = cache {
            headers.insert(header::CACHE_CONTROL, HeaderValue::from_static(cache));
        }
        if let Ok(etag) = HeaderValue::from_str(&etag) {
            headers.insert(header::ETAG, etag);
        }
        response
    }

    fn etag(hash: &[u8; 32]) -> String {
        let mut etag = String::with_capacity(66);
        etag.push('"');
        for byte in hash {
            let _ = write!(etag, "{byte:02x}");
        }
        etag.push('"');
        etag
    }

    fn matches(if_none_match: &str, etag: &str) -> bool {
        if_none_match.split(',').map(str::trim).any(|candidate| {
            candidate == "*" || candidate.strip_prefix("W/").unwrap_or(candidate) == etag
        })
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn if_none_match_accepts_lists_weak_tags_and_wildcards() {
            let etag = etag(&[0xab; 32]);
            assert!(matches(&etag, &etag));
            assert!(matches(&format!("\"other\", W/{etag}"), &etag));
            assert!(matches("*", &etag));
            assert!(!matches("\"other\"", &etag));
        }
    }
}

#[cfg(not(feature = "ui"))]
mod empty {
    use super::*;
    use axum::{body::Bytes, response::Html};

    pub async fn serve(_uri: Uri) -> Response {
        Html(Bytes::from_static(b"")).into_response()
    }
}

#[cfg(feature = "ui")]
pub use embedded::serve;

#[cfg(not(feature = "ui"))]
pub use empty::serve;
