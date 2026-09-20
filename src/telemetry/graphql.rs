use std::fmt;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use juniper::parser::{Lexer, Token};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum OperationId {
    Identified {
        name: OperationName,
        kind: OperationKind,
    },
    Unresolved {
        hint: Option<Arc<str>>,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum OperationName {
    Named(Arc<str>),
    Anonymous,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OperationKind {
    Query,
    Mutation,
    Subscription,
}

#[derive(Clone, Copy, Debug)]
pub struct DocumentHead<'a> {
    pub query: &'a str,
    pub operation_name: Option<&'a str>,
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

impl OperationName {
    pub fn named(name: impl AsRef<str>) -> Self {
        Self::Named(Arc::from(name.as_ref()))
    }
}

impl fmt::Display for OperationName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Named(name) => f.write_str(name),
            Self::Anonymous => f.write_str("(anonymous)"),
        }
    }
}

impl fmt::Display for OperationKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Query => "query",
            Self::Mutation => "mutation",
            Self::Subscription => "subscription",
        })
    }
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
            Self::Identified {
                name: OperationName::Named(name),
                ..
            } => name,
            Self::Identified {
                name: OperationName::Anonymous,
                ..
            } => "(anonymous)",
            Self::Unresolved { hint: Some(name) } => name,
            Self::Unresolved { hint: None } => "(unknown)",
        }
    }

    pub fn kind(&self) -> Option<OperationKind> {
        match self {
            Self::Identified { kind, .. } => Some(*kind),
            Self::Unresolved { .. } => None,
        }
    }

    pub fn parse(query: &str, operation_name: Option<&str>) -> Self {
        match definitions(query) {
            Err(_) => Self::Unresolved {
                hint: operation_name.map(Arc::from),
            },
            Ok(defs) => select(&defs, operation_name),
        }
    }

    pub fn parse_each<'a>(docs: impl IntoIterator<Item = DocumentHead<'a>>) -> Vec<Self> {
        docs.into_iter()
            .map(|doc| Self::parse(doc.query, doc.operation_name))
            .collect()
    }
}

struct Definition {
    kind: OperationKind,
    name: Option<String>,
}

enum ParseFail {
    Lexer,
}

fn definitions(query: &str) -> Result<Vec<Definition>, ParseFail> {
    let mut lexer = Lexer::new(query);
    let mut defs = Vec::new();
    loop {
        match next_token(&mut lexer)? {
            Token::EndOfFile => return Ok(defs),
            Token::Name("fragment") => skip_rest(&mut lexer, None)?,
            Token::Name("query") => defs.push(take_operation(&mut lexer, OperationKind::Query)?),
            Token::Name("mutation") => {
                defs.push(take_operation(&mut lexer, OperationKind::Mutation)?)
            }
            Token::Name("subscription") => {
                defs.push(take_operation(&mut lexer, OperationKind::Subscription)?)
            }
            Token::CurlyOpen => {
                skip_rest(&mut lexer, Some(Token::CurlyOpen))?;
                defs.push(Definition {
                    kind: OperationKind::Query,
                    name: None,
                });
            }
            _ => return Err(ParseFail::Lexer),
        }
    }
}

fn take_operation(lexer: &mut Lexer<'_>, kind: OperationKind) -> Result<Definition, ParseFail> {
    let (name, first) = match next_token(lexer)? {
        Token::Name(name) => (Some(name.to_owned()), None),
        Token::EndOfFile => return Err(ParseFail::Lexer),
        token => (None, Some(token)),
    };
    skip_rest(lexer, first)?;
    Ok(Definition { kind, name })
}

fn select(defs: &[Definition], operation_name: Option<&str>) -> OperationId {
    if let Some(want) = operation_name {
        return match defs.iter().find(|def| def.name.as_deref() == Some(want)) {
            Some(def) => OperationId::Identified {
                name: OperationName::named(want),
                kind: def.kind,
            },
            None => OperationId::Unresolved {
                hint: Some(Arc::from(want)),
            },
        };
    }
    match defs {
        [def] => OperationId::Identified {
            name: match def.name.as_deref() {
                Some(name) => OperationName::named(name),
                None => OperationName::Anonymous,
            },
            kind: def.kind,
        },
        _ => OperationId::Unresolved { hint: None },
    }
}

#[derive(Default)]
struct Nesting {
    paren: i32,
    bracket: i32,
    brace: i32,
}

impl Nesting {
    fn apply(&mut self, token: Token<'_>) {
        match token {
            Token::ParenOpen => self.paren += 1,
            Token::ParenClose => self.paren -= 1,
            Token::BracketOpen => self.bracket += 1,
            Token::BracketClose => self.bracket -= 1,
            Token::CurlyOpen => self.brace += 1,
            Token::CurlyClose => self.brace -= 1,
            _ => {}
        }
    }

    fn idle(&self) -> bool {
        self.paren == 0 && self.bracket == 0 && self.brace == 0
    }

    fn valid(&self) -> bool {
        self.paren >= 0 && self.bracket >= 0 && self.brace >= 0
    }
}

fn skip_rest<'a>(lexer: &mut Lexer<'a>, first: Option<Token<'a>>) -> Result<(), ParseFail> {
    let mut nesting = Nesting::default();
    let mut seen_brace = false;
    if let Some(token) = first {
        apply_skip(&mut nesting, &mut seen_brace, token)?;
        if seen_brace && nesting.idle() {
            return Ok(());
        }
    }
    loop {
        apply_skip(&mut nesting, &mut seen_brace, next_token(lexer)?)?;
        if seen_brace && nesting.idle() {
            return Ok(());
        }
    }
}

fn apply_skip(
    nesting: &mut Nesting,
    seen_brace: &mut bool,
    token: Token<'_>,
) -> Result<(), ParseFail> {
    if matches!(token, Token::EndOfFile) {
        return Err(ParseFail::Lexer);
    }
    if matches!(token, Token::CurlyOpen) {
        *seen_brace = true;
    }
    nesting.apply(token);
    if nesting.valid() {
        Ok(())
    } else {
        Err(ParseFail::Lexer)
    }
}

fn next_token<'a>(lexer: &mut Lexer<'a>) -> Result<Token<'a>, ParseFail> {
    match lexer.next() {
        None => Ok(Token::EndOfFile),
        Some(Ok(spanned)) => Ok(spanned.item),
        Some(Err(_)) => Err(ParseFail::Lexer),
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
    let kind = id.kind().map(tracing::field::display);
    let latency_ms = latency.as_millis();

    match outcome {
        OperationOutcome::Ok => {
            tracing::info!(
                operation = %operation,
                kind,
                latency_ms,
                outcome = %"ok",
                "graphql"
            )
        }
        OperationOutcome::FieldErrors { count } => {
            tracing::info!(
                operation = %operation,
                kind,
                latency_ms,
                outcome = %"field_errors",
                errors = count.get(),
                "graphql"
            )
        }
        OperationOutcome::RequestFailed { reason } => {
            tracing::warn!(
                operation = %operation,
                kind,
                latency_ms,
                outcome = %"request_failed",
                reason = %reason,
                "graphql"
            )
        }
    }
}

pub fn record_ws_upgrade() {
    tracing::info!("graphql websocket connected");
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
        let id = id
            .cloned()
            .unwrap_or(OperationId::Unresolved { hint: None });
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
