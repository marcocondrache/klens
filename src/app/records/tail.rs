use std::convert::Infallible;

use axum::response::sse::{Event, KeepAlive, KeepAliveStream, Sse};
use futures::stream::{self, BoxStream, StreamExt as _};
use tokio::sync::OwnedSemaphorePermit;

use crate::app::auth::SessionGuard;
use crate::environment::SSE_KEEP_ALIVE;
use crate::kafka::Tail;

use super::super::context::Session;
use super::super::error::ApiError;
use super::super::extract::{Path, Query};
use super::types::{TailEvent, TailParams, tail_query};

type Events = BoxStream<'static, Result<Event, Infallible>>;

pub(super) async fn tail(
    session: Session,
    Path((name, topic)): Path<(String, String)>,
    Query(params): Query<TailParams>,
) -> Result<Sse<KeepAliveStream<Events>>, ApiError> {
    let records = session.cluster(&name)?.records()?;
    let cluster = records.name().to_owned();
    let permit = session.state.tail_permit().ok_or(ApiError::TooManyTails)?;
    let tail = records.tail(tail_query(topic, params)).await?;

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
