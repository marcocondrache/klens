use axum::{
    http::Uri,
    response::{IntoResponse, Response},
};

#[cfg(feature = "ui")]
mod embedded {
    use super::*;
    use axum::{
        http::{HeaderValue, StatusCode, header},
        response::Html,
    };
    use rust_embed::RustEmbed;

    #[derive(RustEmbed)]
    #[folder = "static/"]
    struct WebAssets;

    pub async fn serve(uri: Uri) -> Response {
        let path = uri.path().trim_start_matches('/');

        if path.is_empty() {
            return index();
        }

        match WebAssets::get(path) {
            Some(file) => {
                let mime = mime_guess::from_path(path).first_or_octet_stream();
                let mut response = (
                    [(header::CONTENT_TYPE, mime.as_ref())],
                    file.data.into_owned(),
                )
                    .into_response();

                if path.starts_with("assets/") {
                    response.headers_mut().insert(
                        header::CACHE_CONTROL,
                        HeaderValue::from_static("public, max-age=31536000, immutable"),
                    );
                }

                response
            }
            None => index(),
        }
    }

    fn index() -> Response {
        match WebAssets::get("index.html") {
            Some(file) => {
                let mut response = Html(file.data.into_owned()).into_response();
                response
                    .headers_mut()
                    .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-cache"));
                response
            }
            None => StatusCode::NOT_FOUND.into_response(),
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
