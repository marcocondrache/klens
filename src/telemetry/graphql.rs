use std::fmt;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum OperationId {
    Named(Arc<str>),
    Unknown,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum OperationOutcome {
    Ok,
    FieldErrors { count: NonZeroUsize },
    RequestFailed { reason: RequestFailure },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RequestFailure {
    Parse,
    Validation,
    NoOperation,
    MultipleOperations,
    UnknownOperationName,
    SubscriptionOnHttp,
    Other,
}

impl fmt::Display for RequestFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Parse => "parse",
            Self::Validation => "validation",
            Self::NoOperation => "no_operation",
            Self::MultipleOperations => "multiple_operations",
            Self::UnknownOperationName => "unknown_operation_name",
            Self::SubscriptionOnHttp => "subscription_on_http",
            Self::Other => "other",
        })
    }
}

impl OperationId {
    pub fn display_name(&self) -> &str {
        match self {
            Self::Named(name) => name,
            Self::Unknown => "(unknown)",
        }
    }

    pub fn from_name(name: Option<&str>) -> Self {
        match name.map(str::trim).filter(|name| !name.is_empty()) {
            Some(name) => Self::Named(Arc::from(name)),
            None => Self::Unknown,
        }
    }

    pub fn from_each<'a>(names: impl IntoIterator<Item = Option<&'a str>>) -> Vec<Self> {
        names.into_iter().map(Self::from_name).collect()
    }
}

impl OperationOutcome {
    pub fn executed(field_error_count: usize) -> Self {
        match NonZeroUsize::new(field_error_count) {
            None => Self::Ok,
            Some(count) => Self::FieldErrors { count },
        }
    }

    pub fn from_execution<S: juniper::ScalarValue>(
        result: &Result<
            (juniper::Value<S>, Vec<juniper::ExecutionError<S>>),
            juniper::GraphQLError,
        >,
    ) -> Self {
        match result {
            Ok((_, errors)) => Self::executed(errors.len()),
            Err(error) => Self::RequestFailed {
                reason: RequestFailure::from_graphql_error(error),
            },
        }
    }
}

impl RequestFailure {
    fn from_graphql_error(error: &juniper::GraphQLError) -> Self {
        use juniper::GraphQLError::*;
        match error {
            ParseError(_) => Self::Parse,
            ValidationError(_) => Self::Validation,
            NoOperationProvided => Self::NoOperation,
            MultipleOperationsProvided => Self::MultipleOperations,
            UnknownOperationName => Self::UnknownOperationName,
            IsSubscription => Self::SubscriptionOnHttp,
            NotSubscription => Self::Other,
        }
    }
}

pub fn record(id: &OperationId, outcome: &OperationOutcome, latency: Duration) {
    let operation = id.display_name();
    let latency_ms = latency.as_millis();

    match outcome {
        OperationOutcome::Ok => {
            tracing::info!(
                operation = %operation,
                latency_ms,
                outcome = %"ok",
                "graphql"
            )
        }
        OperationOutcome::FieldErrors { count } => {
            tracing::info!(
                operation = %operation,
                latency_ms,
                outcome = %"field_errors",
                errors = count.get(),
                "graphql"
            )
        }
        OperationOutcome::RequestFailed { reason } => {
            tracing::warn!(
                operation = %operation,
                latency_ms,
                outcome = %"request_failed",
                reason = %reason,
                "graphql"
            )
        }
    }
}

pub fn record_subscription_connected(id: &OperationId) {
    tracing::info!(operation = %id.display_name(), "graphql subscription connected");
}

pub fn record_subscription_rejected(
    id: &OperationId,
    error: &juniper::GraphQLError,
    latency: Duration,
) {
    record(
        id,
        &OperationOutcome::from_execution::<juniper::DefaultScalarValue>(&Err(error.clone())),
        latency,
    );
}

pub fn complete<S: juniper::ScalarValue>(
    identities: &[OperationId],
    response: juniper::http::GraphQLBatchResponse<S>,
    latency: Duration,
) -> juniper::http::GraphQLBatchResponse<S> {
    use juniper::http::{GraphQLBatchResponse, GraphQLResponse};

    fn one<S: juniper::ScalarValue>(
        id: Option<&OperationId>,
        response: GraphQLResponse<S>,
        latency: Duration,
    ) -> GraphQLResponse<S> {
        let result = response.into_result();
        let outcome = OperationOutcome::from_execution(&result);
        let id = id.cloned().unwrap_or(OperationId::Unknown);
        record(&id, &outcome, latency);
        GraphQLResponse::from_result(result)
    }

    match response {
        GraphQLBatchResponse::Single(single) => {
            GraphQLBatchResponse::Single(one(identities.first(), single, latency))
        }
        GraphQLBatchResponse::Batch(batch) => GraphQLBatchResponse::Batch(
            identities
                .iter()
                .map(Some)
                .chain(std::iter::repeat(None))
                .zip(batch)
                .map(|(id, response)| one(id, response, latency))
                .collect(),
        ),
    }
}

#[cfg(test)]
mod tests;
