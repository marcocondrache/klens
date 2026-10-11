use axum::extract::{Request, State};
use axum::http::StatusCode;
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use tokio_util::task::TaskTracker;
use tracing::Instrument as _;

/// A change logs its audit line once Kafka answers, and hyper drops a handler
/// whose client hangs up, so a change could reach Kafka with nothing logged.
/// A request that may change something runs in a task of its own, which
/// finishes either way and keeps the request's span for the audit line.
/// `writes` tracks those tasks so shutdown can wait for them.
pub async fn run_to_completion(
    State(writes): State<TaskTracker>,
    request: Request,
    next: Next,
) -> Response {
    if request.method().is_safe() {
        return next.run(request).await;
    }

    match writes.spawn(next.run(request).in_current_span()).await {
        Ok(response) => response,
        Err(error) => match error.try_into_panic() {
            Ok(panic) => std::panic::resume_unwind(panic),
            Err(_) => StatusCode::SERVICE_UNAVAILABLE.into_response(),
        },
    }
}
