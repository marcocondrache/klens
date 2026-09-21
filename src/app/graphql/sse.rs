//! GraphQL subscriptions over server-sent events.
//!
//! One `GET /graphql` is one subscription. The query string carries the
//! GraphQL request. Each execution result is a `next` event, and the stream
//! ends with `complete`. Closing the response unsubscribes.

use std::convert::Infallible;
use std::sync::Arc;
use std::time::Instant;

use async_stream::stream;
use axum::http::{HeaderName, HeaderValue};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use futures::StreamExt as _;
use juniper::http::{GraphQLBatchRequest, GraphQLResponse};
use juniper::{DefaultScalarValue, GraphQLError};

use crate::AppState;
use crate::app::auth::SessionGuard;
use crate::app::auth::access::EffectiveAccess;
use crate::telemetry::{OperationId, record_subscription_connected, record_subscription_rejected};

use super::Schema;
use super::context::GraphQlContext;

pub fn open(
    schema: Arc<Schema>,
    state: AppState,
    access: EffectiveAccess,
    guard: SessionGuard,
    request: GraphQLBatchRequest<DefaultScalarValue>,
) -> Response {
    let context = GraphQlContext {
        state,
        access,
        guard,
    };
    let events = stream! {
        let request = match request {
            GraphQLBatchRequest::Single(request) => request,
            GraphQLBatchRequest::Batch(_) => {
                yield event(next_data(
                    r#"{"errors":[{"message":"Expected a single subscription"}]}"#,
                ));
                yield event(complete());
                return;
            }
        };

        let started = Instant::now();
        let identity = OperationId::from_name(request.operation_name.as_deref());
        match juniper::http::resolve_into_stream(&request, schema.as_ref(), &context).await {
            Err(error) => {
                record_subscription_rejected(&identity, &error, started.elapsed());
                yield event(next_error(&error));
                yield event(complete());
            }
            Ok((values, errors)) => {
                record_subscription_connected(&identity);
                let mut connection =
                    juniper_subscriptions::Connection::from_stream(values, errors);
                while let Some(output) = connection.next().await {
                    yield event(next_json(&output));
                }
                yield event(complete());
            }
        };
    };

    let mut response = Sse::new(events)
        .keep_alive(KeepAlive::default())
        .into_response();
    response.headers_mut().insert(
        HeaderName::from_static("x-accel-buffering"),
        HeaderValue::from_static("no"),
    );
    response
}

fn event(event: Event) -> Result<Event, Infallible> {
    Ok(event)
}

fn next_error(error: &GraphQLError) -> Event {
    next_json(&GraphQLResponse::<DefaultScalarValue>::from_result(Err(
        error.clone(),
    )))
}

fn next_json(payload: &impl serde::Serialize) -> Event {
    Event::default()
        .event("next")
        .json_data(payload)
        .unwrap_or_else(|_| {
            next_data(r#"{"errors":[{"message":"failed to encode execution result"}]}"#)
        })
}

fn next_data(data: &str) -> Event {
    Event::default().event("next").data(data)
}

fn complete() -> Event {
    Event::default().event("complete").data("null")
}
