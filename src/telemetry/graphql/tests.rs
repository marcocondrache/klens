use std::io;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use juniper::parser::{ParseError, SourcePosition, Spanning};

use super::{
    OperationId, OperationKind, OperationName, OperationOutcome, RequestFailure, complete, record,
};
use crate::telemetry::log_http_completed;

#[derive(Clone, Default)]
struct LogBuf(Arc<Mutex<Vec<u8>>>);

impl io::Write for LogBuf {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0.lock().expect("log buf").extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for LogBuf {
    type Writer = Self;

    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

impl LogBuf {
    fn as_string(&self) -> String {
        String::from_utf8_lossy(&self.0.lock().expect("log buf")).into_owned()
    }
}

fn capture(max_level: tracing::Level) -> (LogBuf, tracing::subscriber::DefaultGuard) {
    let logs = LogBuf::default();
    let subscriber = tracing_subscriber::fmt()
        .with_writer(logs.clone())
        .with_max_level(max_level)
        .with_ansi(false)
        .without_time()
        .finish();
    (logs, tracing::subscriber::set_default(subscriber))
}

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
fn ui_whoami_document_is_query_whoami() {
    let id = OperationId::parse("query Whoami { whoami { subject } }", None);
    assert_eq!(
        id,
        OperationId::Identified {
            name: OperationName::named("Whoami"),
            kind: OperationKind::Query,
        }
    );
}

#[test]
fn json_operation_name_whoami_matches_the_document() {
    let id = OperationId::parse("query Whoami { whoami { subject } }", Some("Whoami"));
    assert_eq!(
        id,
        OperationId::Identified {
            name: OperationName::named("Whoami"),
            kind: OperationKind::Query,
        }
    );
}

#[test]
fn shorthand_document_is_anonymous_query() {
    let id = OperationId::parse("{ clusters { cluster } }", None);
    assert_eq!(
        id,
        OperationId::Identified {
            name: OperationName::Anonymous,
            kind: OperationKind::Query,
        }
    );
}

#[test]
fn json_operation_name_selects_among_definitions() {
    let id = OperationId::parse("query A { a } query B { b }", Some("B"));
    assert_eq!(
        id,
        OperationId::Identified {
            name: OperationName::named("B"),
            kind: OperationKind::Query,
        }
    );
}

#[test]
fn two_operations_without_a_name_are_unresolved() {
    let id = OperationId::parse("query A { a } query B { b }", None);
    assert_eq!(id, OperationId::Unresolved { hint: None });
}

#[test]
fn non_graphql_text_is_unresolved() {
    let id = OperationId::parse("not graphql", None);
    assert_eq!(id, OperationId::Unresolved { hint: None });
}

#[test]
fn fragments_are_skipped_and_whoami_is_recovered() {
    let id = OperationId::parse(
        r#"
    query Whoami {
  whoami {
    ...IdentityFields
  }
}
    fragment IdentityFields on Identity {
  subject
}"#,
        None,
    );
    assert_eq!(
        id,
        OperationId::Identified {
            name: OperationName::named("Whoami"),
            kind: OperationKind::Query,
        }
    );
}

#[test]
fn unnamed_query_keyword_is_anonymous() {
    let id = OperationId::parse("query { clusters { cluster } }", None);
    assert_eq!(
        id,
        OperationId::Identified {
            name: OperationName::Anonymous,
            kind: OperationKind::Query,
        }
    );
}

#[test]
fn subscription_updates_is_identified() {
    let id = OperationId::parse(
        "subscription Updates($cluster: String!, $scope: UpdateScope) { updates(cluster: $cluster, scope: $scope) { __typename } }",
        None,
    );
    assert_eq!(
        id,
        OperationId::Identified {
            name: OperationName::named("Updates"),
            kind: OperationKind::Subscription,
        }
    );
}

#[test]
fn unknown_json_name_does_not_fall_back() {
    let id = OperationId::parse("query A { a }", Some("Nope"));
    assert_eq!(
        id,
        OperationId::Unresolved {
            hint: Some(Arc::from("Nope")),
        }
    );
}

#[test]
fn field_named_query_is_not_an_operation() {
    let id = OperationId::parse("query Whoami { query { subject } }", None);
    assert_eq!(
        id,
        OperationId::Identified {
            name: OperationName::named("Whoami"),
            kind: OperationKind::Query,
        }
    );
}

#[test]
fn record_whoami_ok_inherits_request_id_and_omits_the_document() {
    let (logs, _guard) = capture(tracing::Level::INFO);
    let id = OperationId::parse("query Whoami { whoami { subject } }", None);
    let _span = tracing::info_span!("http.request", request_id = "req-1").entered();
    record(&id, &OperationOutcome::Ok, Duration::from_millis(12));
    let text = logs.as_string();
    assert!(text.contains("operation=Whoami"), "{text}");
    assert!(text.contains("kind=query"), "{text}");
    assert!(text.contains("outcome=ok"), "{text}");
    assert!(text.contains("req-1"), "{text}");
    assert!(!text.contains("whoami"), "{text}");
    assert!(!text.contains("POST /graphql"), "{text}");
    assert!(!text.contains("subject"), "{text}");
}

#[test]
fn field_errors_are_not_ok_and_do_not_log_the_document() {
    let (logs, _guard) = capture(tracing::Level::INFO);
    let id = OperationId::parse(
        "query TopicRows($cluster: String!) { topicRows(cluster: $cluster) { rows { name } } }",
        None,
    );
    record(
        &id,
        &OperationOutcome::executed(2),
        Duration::from_millis(40),
    );
    let text = logs.as_string();
    assert!(text.contains("operation=TopicRows"), "{text}");
    assert!(text.contains("kind=query"), "{text}");
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
        &OperationId::parse("{", None),
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
fn complete_records_the_identified_operation() {
    let (logs, _guard) = capture(tracing::Level::INFO);
    let identities = [OperationId::parse(
        "query Whoami { whoami { subject } }",
        None,
    )];
    let response = juniper::http::GraphQLBatchResponse::Single(
        juniper::http::GraphQLResponse::from_result(Ok((
            juniper::Value::<juniper::DefaultScalarValue>::null(),
            vec![],
        ))),
    );
    let _ = complete(&identities, response, Duration::from_millis(12));
    let text = logs.as_string();
    assert!(text.contains("operation=Whoami"), "{text}");
    assert!(text.contains("kind=query"), "{text}");
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
