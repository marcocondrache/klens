use juniper::{FieldError, IntoFieldError, ScalarValue, graphql_value};

use crate::kafka::KafkaError;

#[derive(Debug)]
pub enum GqlError {
    Kafka(KafkaError),
    Forbidden,
}

impl From<KafkaError> for GqlError {
    fn from(error: KafkaError) -> Self {
        Self::Kafka(error)
    }
}

impl From<crate::kafka::QueryError> for GqlError {
    fn from(error: crate::kafka::QueryError) -> Self {
        Self::Kafka(error.into())
    }
}

impl<S: ScalarValue> IntoFieldError<S> for GqlError {
    fn into_field_error(self) -> FieldError<S> {
        match self {
            Self::Kafka(error) => error.into_field_error(),
            Self::Forbidden => {
                FieldError::new("forbidden", graphql_value!({ "code": "FORBIDDEN" }))
            }
        }
    }
}

impl<S: ScalarValue> IntoFieldError<S> for KafkaError {
    fn into_field_error(self) -> FieldError<S> {
        FieldError::new(self.to_string(), graphql_value!({ "code": self.code() }))
    }
}
