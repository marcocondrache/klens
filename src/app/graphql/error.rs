use juniper::{FieldError, IntoFieldError, ScalarValue, graphql_value};

use crate::kafka::KafkaError;

impl<S: ScalarValue> IntoFieldError<S> for KafkaError {
    fn into_field_error(self) -> FieldError<S> {
        FieldError::new(self.to_string(), graphql_value!({ "code": self.code() }))
    }
}
