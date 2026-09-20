use std::sync::Arc;
use std::time::Duration;

use juniper::parser::{ParseError, SourcePosition, Spanning};

use super::{OperationId, OperationOutcome, RequestFailure, complete, record};
use crate::telemetry::capture::subscriber as capture;
use crate::telemetry::log_http_completed;

fn parse_error_result() -> Result<
    (
        juniper::Value,
        Vec<juniper::ExecutionError<juniper::DefaultScalarValue>>,
    ),
    juniper::GraphQLError,
> {
    Err(juniper::GraphQLError::ParseError(Spanning::zero_width(
        &SourcePosition::new_origin(),
        ParseError::LexerError(juniper::parser::LexerError::UnexpectedEndOfFile),
    )))
}

#[test]
fn operation_name_from_the_wire_is_the_identity() {
    assert_eq!(
        OperationId::from_name(Some("Whoami")),
        OperationId::Named(Arc::from("Whoami"))
    );
    assert_eq!(
        OperationId::from_name(Some("Clusters")),
        OperationId::Named(Arc::from("Clusters"))
    );
}

#[test]
fn missing_or_blank_operation_name_is_unknown() {
    assert_eq!(OperationId::from_name(None), OperationId::Unknown);
    assert_eq!(OperationId::from_name(Some("")), OperationId::Unknown);
    assert_eq!(OperationId::from_name(Some("  ")), OperationId::Unknown);
}

#[test]
fn record_whoami_ok_omits_the_document() {
    let (logs, _guard) = capture(tracing::Level::INFO);
    record(
        &OperationId::from_name(Some("Whoami")),
        &OperationOutcome::Ok,
        Duration::from_millis(12),
    );
    let text = logs.as_string();
    assert!(text.contains("operation=Whoami"), "{text}");
    assert!(text.contains("outcome=ok"), "{text}");
    assert!(!text.contains("request_id"), "{text}");
    assert!(!text.contains("HTTP/1"), "{text}");
    assert!(!text.contains("whoami {"), "{text}");
    assert!(!text.contains("POST /graphql"), "{text}");
    assert!(!text.contains("subject"), "{text}");
}

#[test]
fn field_errors_are_not_ok_and_do_not_log_the_document() {
    let (logs, _guard) = capture(tracing::Level::INFO);
    record(
        &OperationId::from_name(Some("TopicRows")),
        &OperationOutcome::executed(2),
        Duration::from_millis(40),
    );
    let text = logs.as_string();
    assert!(text.contains("operation=TopicRows"), "{text}");
    assert!(text.contains("outcome=field_errors"), "{text}");
    assert!(text.contains("errors=2"), "{text}");
    assert!(!text.contains("outcome=ok"), "{text}");
    assert!(!text.contains("topicRows"), "{text}");
    assert!(!text.contains("$cluster"), "{text}");
}

#[test]
fn request_failed_is_warn_and_distinct_from_field_errors() {
    let (logs, _guard) = capture(tracing::Level::WARN);
    record(
        &OperationId::Unknown,
        &OperationOutcome::from_execution(&parse_error_result()),
        Duration::from_millis(1),
    );
    let text = logs.as_string();
    assert!(text.contains("outcome=request_failed"), "{text}");
    assert!(text.contains("reason=parse"), "{text}");
    assert!(text.contains("WARN"), "{text}");
    assert!(!text.contains("outcome=field_errors"), "{text}");
    assert!(!text.contains("outcome=ok"), "{text}");
}

#[test]
fn complete_records_the_named_operation() {
    let (logs, _guard) = capture(tracing::Level::INFO);
    let identities = [OperationId::from_name(Some("Whoami"))];
    let response = juniper::http::GraphQLBatchResponse::Single(
        juniper::http::GraphQLResponse::from_result(Ok((
            juniper::Value::<juniper::DefaultScalarValue>::null(),
            vec![],
        ))),
    );
    let _ = complete(&identities, response, Duration::from_millis(12));
    let text = logs.as_string();
    assert!(text.contains("operation=Whoami"), "{text}");
    assert!(text.contains("outcome=ok"), "{text}");
}

#[test]
fn executed_zero_is_ok_and_nonzero_is_field_errors() {
    assert_eq!(OperationOutcome::executed(0), OperationOutcome::Ok);
    assert_eq!(
        OperationOutcome::executed(2),
        OperationOutcome::FieldErrors {
            count: std::num::NonZeroUsize::new(2).expect("2"),
        }
    );
    assert_eq!(
        OperationOutcome::from_execution::<juniper::DefaultScalarValue>(&Err(
            juniper::GraphQLError::ParseError(Spanning::zero_width(
                &SourcePosition::new_origin(),
                ParseError::UnexpectedEndOfFile,
            ))
        )),
        OperationOutcome::RequestFailed {
            reason: RequestFailure::Parse,
        }
    );
}

#[test]
fn log_http_completed_204_is_silent_at_info() {
    let (logs, _guard) = capture(tracing::Level::INFO);
    log_http_completed(204, Duration::from_millis(1));
    let text = logs.as_string();
    assert!(!text.contains("request completed"), "{text}");
}

#[test]
fn log_http_completed_503_is_silent_at_info() {
    let (logs, _guard) = capture(tracing::Level::INFO);
    log_http_completed(503, Duration::from_millis(1));
    let text = logs.as_string();
    assert!(!text.contains("request completed"), "{text}");
}

#[test]
fn log_http_completed_500_is_warn() {
    let (logs, _guard) = capture(tracing::Level::WARN);
    log_http_completed(500, Duration::from_millis(3));
    let text = logs.as_string();
    assert!(text.contains("request completed"), "{text}");
    assert!(text.contains("status=500"), "{text}");
    assert!(text.contains("WARN"), "{text}");
}
