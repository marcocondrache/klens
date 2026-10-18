use tracing::{Dispatch, Span, dispatcher};

/// The work runs in the caller's tracing context, so a warning it logs still
/// names the request that asked for it.
pub(crate) async fn off_runtime<T: Send + 'static>(work: impl FnOnce() -> T + Send + 'static) -> T {
    let span = Span::current();
    let dispatch = dispatcher::get_default(Dispatch::clone);
    let work = move || dispatcher::with_default(&dispatch, || span.in_scope(work));

    match tokio::task::spawn_blocking(work).await {
        Ok(output) => output,
        // The pool cancels a task only while the runtime shuts down, so the
        // error carries the work's own panic.
        Err(error) => std::panic::resume_unwind(error.into_panic()),
    }
}
