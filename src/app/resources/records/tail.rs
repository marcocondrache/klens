use std::convert::Infallible;

use axum::response::sse::{Event, KeepAlive, KeepAliveStream, Sse};
use futures::stream::{self, BoxStream, StreamExt as _};
use tokio::sync::OwnedSemaphorePermit;

use crate::app::SSE_KEEP_ALIVE;
use crate::app::auth::SessionGuard;
use crate::kafka::Tail;

use super::denied;
use super::types::{TailEvent, TailParams};
use crate::app::context::Session;
use crate::app::error::ApiError;
use crate::app::extract::{Path, Query};

type Events = BoxStream<'static, Result<Event, Infallible>>;

pub(super) async fn tail(
    session: Session,
    Path((name, topic)): Path<(String, String)>,
    Query(params): Query<TailParams>,
) -> Result<Sse<KeepAliveStream<Events>>, ApiError> {
    let records = session.cluster(&name)?.records()?;
    let permit = session.state.tail_permit().ok_or(ApiError::TooManyTails)?;
    let tail = records.tail(params.into_query(topic)?).await?;

    let ready = TailEvent::ready(&tail).sse();
    let follow = Follow {
        tail,
        guard: session.guard,
        cluster: name,
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
                if let Some(error) = denied(&state.guard, &state.cluster) {
                    return Some(state.end(error));
                }
                if batch.is_empty() {
                    tokio::task::yield_now().await;
                    continue;
                }
                return Some((Ok(TailEvent::from(batch).sse()), state));
            }
        })
        .boxed()
    }

    fn end(mut self, error: ApiError) -> (Result<Event, Infallible>, Self) {
        self.done = true;
        (Ok(error.event()), self)
    }
}
