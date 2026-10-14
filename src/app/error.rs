use axum::Json;
use axum::http::StatusCode;
use axum::response::sse::Event;
use axum::response::{IntoResponse, Response};
use serde::Serialize;

use crate::app::auth::access::AccessError;
use crate::kafka::{KafkaError, QueryError};

#[derive(Debug, thiserror::Error)]
pub(crate) enum ApiError {
    #[error(transparent)]
    Kafka(#[from] KafkaError),
    #[error(transparent)]
    Access(#[from] AccessError),
    #[error("session is no longer valid")]
    SessionExpired,
    #[error("unauthorized")]
    Unauthorized,
    #[error("host is not in allowed_hosts")]
    HostNotAllowed,
    #[error("/mcp answers only its gateway, and this call lacks the gateway's secret")]
    NotFromGateway,
    #[error("not found")]
    NotFound,
    #[error("too many live tails are open, try again later")]
    TooManyTails,
    #[error("too many tool calls are running at once")]
    RateLimited,
    #[error("too many calls this minute to tools that read more from Kafka")]
    TooManyLiveCalls,
    #[error(
        "klens has not read the {lane} of cluster '{cluster}' yet{}",
        last_attempt(last_error.as_deref())
    )]
    NotReady {
        cluster: String,
        lane: &'static str,
        last_error: Option<String>,
    },
    #[error("{message}")]
    InvalidRequest { status: StatusCode, message: String },
}

fn last_attempt(error: Option<&str>) -> String {
    error.map_or_else(String::new, |error| {
        format!("; its last attempt failed: {error}")
    })
}

impl ApiError {
    pub(crate) fn unprocessable(message: impl Into<String>) -> Self {
        Self::InvalidRequest {
            status: StatusCode::UNPROCESSABLE_ENTITY,
            message: message.into(),
        }
    }

    pub(crate) fn code(&self) -> &'static str {
        match self {
            Self::Kafka(error) => error.code(),
            Self::Access(error) => error.code(),
            Self::SessionExpired => "SESSION_EXPIRED",
            Self::Unauthorized => "UNAUTHORIZED",
            Self::HostNotAllowed => "HOST_NOT_ALLOWED",
            Self::NotFromGateway => "NOT_FROM_GATEWAY",
            Self::NotFound => "NOT_FOUND",
            Self::TooManyTails => "TOO_MANY_TAILS",
            Self::RateLimited | Self::TooManyLiveCalls => "RATE_LIMITED",
            Self::NotReady { .. } => "NOT_READY",
            Self::InvalidRequest { .. } => "INVALID_REQUEST",
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
            Self::TooManyTails | Self::NotReady { .. } => StatusCode::SERVICE_UNAVAILABLE,
            Self::RateLimited | Self::TooManyLiveCalls => StatusCode::TOO_MANY_REQUESTS,
            Self::InvalidRequest { status, .. } => *status,
            Self::HostNotAllowed
            | Self::NotFromGateway
            | Self::Access(AccessError::Forbidden { .. } | AccessError::ReadOnlyCluster(_)) => {
                StatusCode::FORBIDDEN
            }
            Self::NotFound | Self::Access(AccessError::UnknownCluster(_)) => StatusCode::NOT_FOUND,
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
        | KafkaError::UnknownSchema { .. }
        | KafkaError::UnknownPartition { .. }
        | KafkaError::UnknownOffset { .. }
        | KafkaError::NoSchemaRegistry(_) => StatusCode::NOT_FOUND,
        KafkaError::InvalidQuery(_) => StatusCode::BAD_REQUEST,
        KafkaError::Refused(_)
        | KafkaError::RegistryRefused(_)
        | KafkaError::Unencodable { .. }
        | KafkaError::InternalTopic(_)
        | KafkaError::PrefixedAcl
        | KafkaError::NoCommittedOffset { .. } => StatusCode::UNPROCESSABLE_ENTITY,
        KafkaError::ActiveGroup { .. } | KafkaError::ConsumedTopic { .. } => StatusCode::CONFLICT,
        KafkaError::Timeout => StatusCode::GATEWAY_TIMEOUT,
        KafkaError::Admin(_)
        | KafkaError::BrokerConfigs { .. }
        | KafkaError::SchemaRegistry { .. }
        | KafkaError::Krafka(_) => StatusCode::BAD_GATEWAY,
    }
}

impl From<QueryError> for ApiError {
    fn from(error: QueryError) -> Self {
        Self::Kafka(error.into())
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
