use juniper::{FieldError, IntoFieldError, ScalarValue, graphql_value};

use crate::app::auth::access::AccessError;
use crate::kafka::{KafkaError, QueryError};

/// Every failure the API surfaces, carrying a stable `extensions.code`.
///
/// Denials are errors, never silent empties or redacted strings: a client
/// that cannot tell "no topics" from "not allowed to see topics" cannot show
/// the user anything useful.
#[derive(Debug)]
pub enum GqlError {
    Kafka(KafkaError),
    Access(AccessError),
    /// The session expired or was revoked while a subscription was running.
    SessionExpired,
}

impl GqlError {
    pub fn code(&self) -> &'static str {
        match self {
            Self::Kafka(error) => error.code(),
            Self::Access(error) => error.code(),
            Self::SessionExpired => "SESSION_EXPIRED",
        }
    }
}

impl std::fmt::Display for GqlError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Kafka(error) => error.fmt(formatter),
            Self::Access(error) => error.fmt(formatter),
            Self::SessionExpired => formatter.write_str("session is no longer valid"),
        }
    }
}

impl From<KafkaError> for GqlError {
    fn from(error: KafkaError) -> Self {
        Self::Kafka(error)
    }
}

impl From<QueryError> for GqlError {
    fn from(error: QueryError) -> Self {
        Self::Kafka(error.into())
    }
}

impl From<AccessError> for GqlError {
    fn from(error: AccessError) -> Self {
        Self::Access(error)
    }
}

impl<S: ScalarValue> IntoFieldError<S> for GqlError {
    fn into_field_error(self) -> FieldError<S> {
        FieldError::new(self.to_string(), graphql_value!({ "code": self.code() }))
    }
}

impl<S: ScalarValue> IntoFieldError<S> for KafkaError {
    fn into_field_error(self) -> FieldError<S> {
        GqlError::from(self).into_field_error()
    }
}
