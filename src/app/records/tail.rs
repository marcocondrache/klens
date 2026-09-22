use std::convert::Infallible;

use axum::extract::{Path, Query};
use axum::response::sse::{Event, KeepAlive, KeepAliveStream, Sse};
use futures::stream::{self, BoxStream, StreamExt as _};
use tokio::sync::OwnedSemaphorePermit;

use crate::app::auth::SessionGuard;
use crate::environment::SSE_KEEP_ALIVE;
use crate::kafka::Tail;

use super::super::context::Session;
use super::super::error::ApiError;
use super::types::{TailEvent, TailParams, tail_query};

type Events = BoxStream<'static, Result<Event, Infallible>>;

/// Records produced to a topic from now on, as a `ready` frame followed by
/// paced `records` frames.
///
/// Opening fails like any request: an unknown topic or a missing privilege
/// never starts a stream. Once open, losing access or a broker error ends it
/// with one `error` frame.
pub(super) async fn tail(
    session: Session,
    Path((name, topic)): Path<(String, String)>,
    Query(params): Query<TailParams>,
) -> Result<Sse<KeepAliveStream<Events>>, ApiError> {
    let cluster = session
        .cluster(&name)?
        .access
        .records()?
        .cluster()
        .to_owned();
    let permit = session.state.tail_permit().ok_or(ApiError::TooManyTails)?;
    let tail = session
        .state
        .live_tail(&cluster, tail_query(topic, params))
        .await?;

    let ready = frame(&TailEvent::ready(&tail));
    let follow = Follow {
        tail,
        guard: session.guard,
        cluster,
        done: false,
        _permit: permit,
    };

    Ok(Sse::new(
        stream::once(async { Ok(ready) })
            .chain(follow.into_stream())
            .boxed(),
    )
    .keep_alive(KeepAlive::new().interval(SSE_KEEP_ALIVE).text("keep-alive")))
}

struct Follow {
    tail: Tail,
    guard: SessionGuard,
    cluster: String,
    done: bool,
    /// Released, with the tail's consumer, when the browser goes away.
    _permit: OwnedSemaphorePermit,
}

impl Follow {
    fn into_stream(self) -> Events {
        stream::unfold(self, |mut state| async move {
            loop {
                if state.done {
                    return None;
                }

                let batch = match state.tail.next().await {
                    Ok(batch) => batch,
                    Err(error) => return Some(state.end(error.into())),
                };
                if let Some(error) = state.denied() {
                    return Some(state.end(error));
                }
                if batch.is_empty() {
                    // An empty batch took a heartbeat to arrive; should one
                    // ever come back at once, still let the runtime breathe.
                    tokio::task::yield_now().await;
                    continue;
                }
                return Some((Ok(frame(&TailEvent::from(batch))), state));
            }
        })
        .boxed()
    }

    fn end(mut self, error: ApiError) -> (Result<Event, Infallible>, Self) {
        self.done = true;
        (Ok(error.event()), self)
    }

    /// Access is rechecked before every frame, and at least every heartbeat
    /// on a quiet topic.
    fn denied(&self) -> Option<ApiError> {
        let Some(access) = self.guard.revalidate() else {
            return Some(ApiError::SessionExpired);
        };
        access
            .cluster(&self.cluster)
            .and_then(|cluster| cluster.records().map(drop))
            .err()
            .map(ApiError::from)
    }
}

fn frame(event: &TailEvent) -> Event {
    Event::default()
        .event(event.event())
        .json_data(event)
        .expect("tail event is serializable")
}
