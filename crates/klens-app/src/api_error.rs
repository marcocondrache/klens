use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use klens_kafka::KafkaError;
use serde_json::json;

#[derive(Debug, thiserror::Error)]
pub enum ApiError {
    #[error("resource not found")]
    NotFound,

    #[error("invalid request: {0}")]
    BadRequest(String),

    #[error(transparent)]
    Internal(#[from] anyhow::Error),
}

impl From<KafkaError> for ApiError {
    fn from(error: KafkaError) -> Self {
        Self::Internal(error.into())
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, message) = match &self {
            Self::NotFound => (StatusCode::NOT_FOUND, self.to_string()),
            Self::BadRequest(_) => (StatusCode::BAD_REQUEST, self.to_string()),
            Self::Internal(error) => {
                tracing::error!(error = ?error, "request failed");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "internal server error".to_owned(),
                )
            }
        };

        (status, Json(json!({ "message": message }))).into_response()
    }
}
