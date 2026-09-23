use axum::Json;
use axum::http::StatusCode;
use axum::response::sse::Event;
use axum::response::{IntoResponse, Response};
use serde::Serialize;

use crate::app::auth::access::AccessError;
use crate::kafka::{KafkaError, QueryError};

#[derive(Debug)]
pub(crate) enum ApiError {
    Kafka(KafkaError),
    Access(AccessError),
    SessionExpired,
    Unauthorized,
    TooManyTails,
    InvalidRequest { status: StatusCode, message: String },
    InvalidBody { status: StatusCode, message: String },
    ConfirmationRequired(&'static str),
    CrossSite,
}

impl ApiError {
    pub(crate) fn code(&self) -> &'static str {
        match self {
            Self::Kafka(error) => error.code(),
            Self::Access(error) => error.code(),
            Self::SessionExpired => "SESSION_EXPIRED",
            Self::Unauthorized => "UNAUTHORIZED",
            Self::TooManyTails => "TOO_MANY_TAILS",
            Self::InvalidRequest { .. } => "INVALID_REQUEST",
            Self::InvalidBody { .. } => "INVALID_BODY",
            Self::ConfirmationRequired(_) => "CONFIRMATION_REQUIRED",
            Self::CrossSite => "CROSS_SITE",
        }
    }

    pub(crate) fn event(&self) -> Event {
        Event::default()
            .event("error")
            .json_data(self.body())
            .expect("error body is serializable")
    }

    fn body(&self) -> ErrorBody<'_> {
        ErrorBody {
            error: self.to_string(),
            code: self.code(),
        }
    }

    fn status(&self) -> StatusCode {
        match self {
            Self::SessionExpired | Self::Unauthorized => StatusCode::UNAUTHORIZED,
            Self::TooManyTails => StatusCode::SERVICE_UNAVAILABLE,
            Self::InvalidRequest { status, .. } => *status,
            Self::InvalidBody { status, .. } => *status,
            Self::ConfirmationRequired(_) => StatusCode::BAD_REQUEST,
            Self::CrossSite => StatusCode::FORBIDDEN,
            Self::Access(AccessError::Forbidden { .. }) => StatusCode::FORBIDDEN,
            Self::Access(AccessError::UnknownCluster(_)) => StatusCode::NOT_FOUND,
            Self::Kafka(error) => kafka_status(error),
        }
    }
}

fn kafka_status(error: &KafkaError) -> StatusCode {
    match error {
        KafkaError::UnknownCluster(_)
        | KafkaError::UnknownTopic { .. }
        | KafkaError::UnknownGroup { .. }
        | KafkaError::UnknownBroker { .. }
        | KafkaError::UnknownSubject { .. }
        | KafkaError::UnknownPartition { .. } => StatusCode::NOT_FOUND,
        KafkaError::InvalidQuery(_) | KafkaError::InvalidRequest(_) => StatusCode::BAD_REQUEST,
        KafkaError::GroupNotEmpty { .. } => StatusCode::CONFLICT,
        KafkaError::Denied(_) => StatusCode::FORBIDDEN,
        KafkaError::Timeout => StatusCode::GATEWAY_TIMEOUT,
        KafkaError::Admin(_)
        | KafkaError::BrokerConfigs { .. }
        | KafkaError::SchemaRegistry { .. }
        | KafkaError::Obfuscation { .. }
        | KafkaError::Krafka(_) => StatusCode::BAD_GATEWAY,
    }
}

impl std::fmt::Display for ApiError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Kafka(error) => error.fmt(formatter),
            Self::Access(error) => error.fmt(formatter),
            Self::SessionExpired => formatter.write_str("session is no longer valid"),
            Self::Unauthorized => formatter.write_str("unauthorized"),
            Self::TooManyTails => {
                formatter.write_str("too many live tails are open, try again later")
            }
            Self::InvalidRequest { message, .. } => formatter.write_str(message),
            Self::InvalidBody { message, .. } => formatter.write_str(message),
            Self::ConfirmationRequired(what) => {
                write!(formatter, "confirm must repeat the {what} exactly")
            }
            Self::CrossSite => {
                formatter.write_str("requests from another site may not change anything")
            }
        }
    }
}

impl From<KafkaError> for ApiError {
    fn from(error: KafkaError) -> Self {
        Self::Kafka(error)
    }
}

impl From<QueryError> for ApiError {
    fn from(error: QueryError) -> Self {
        Self::Kafka(error.into())
    }
}

impl From<AccessError> for ApiError {
    fn from(error: AccessError) -> Self {
        Self::Access(error)
    }
}

#[derive(Serialize)]
struct ErrorBody<'a> {
    error: String,
    code: &'a str,
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (self.status(), Json(self.body())).into_response()
    }
}
