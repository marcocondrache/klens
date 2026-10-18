use std::sync::{Arc, OnceLock};

use axum::response::sse::Event;
use foldhash::HashMap;
use tokio::sync::broadcast::error::RecvError;
use tokio::sync::broadcast::{self, Receiver, Sender};

use crate::kafka::store::bus::BUS_CAPACITY;
use crate::kafka::store::{Change, ClusterStore};
use crate::kafka::{Clusters, KafkaError};

use super::Update;

/// One feed per configured cluster.
pub(crate) struct Feeds(HashMap<String, Feed>);

impl Feeds {
    pub(crate) fn new(clusters: &Clusters) -> Self {
        Self(
            clusters
                .iter()
                .map(|cluster| {
                    let feed = Feed::new(Arc::clone(&cluster.store));
                    (cluster.name().to_owned(), feed)
                })
                .collect(),
        )
    }

    pub(crate) fn get(&self, cluster: &str) -> Result<&Feed, KafkaError> {
        self.0
            .get(cluster)
            .ok_or_else(|| KafkaError::UnknownCluster(cluster.to_owned()))
    }
}

/// Relays a cluster's change bus to its update streams, so a change that many
/// streams send in full is rendered once rather than once per stream.
pub(crate) struct Feed {
    store: Arc<ClusterStore>,
    frames: OnceLock<Sender<Frame>>,
}

impl Feed {
    fn new(store: Arc<ClusterStore>) -> Self {
        Self {
            store,
            frames: OnceLock::new(),
        }
    }

    /// Starts the relay on the first subscriber, so a cluster nobody watches
    /// runs no relay.
    pub(crate) fn subscribe(&self) -> Receiver<Frame> {
        self.frames
            .get_or_init(|| {
                let (frames, _) = broadcast::channel(BUS_CAPACITY);
                let relay = Relay {
                    cluster: self.store.name().to_owned(),
                    changes: self.store.bus.subscribe(),
                    frames: frames.clone(),
                };
                tokio::spawn(relay.run());
                frames
            })
            .subscribe()
    }
}

#[derive(Clone)]
pub(crate) enum Frame {
    Change(Arc<SharedChange>),
    /// The relay fell behind the change bus and dropped changes.
    Resync,
}

/// A change as every stream receives it. The first stream that sends it in full
/// renders the events, and the others reuse them.
pub(crate) struct SharedChange {
    pub(crate) change: Change,
    firehose: OnceLock<Box<[Event]>>,
}

impl SharedChange {
    pub(crate) fn firehose(&self) -> &[Event] {
        self.firehose.get_or_init(|| {
            Update::firehose(&self.change)
                .iter()
                .map(Update::sse)
                .collect()
        })
    }
}

struct Relay {
    cluster: String,
    changes: Receiver<Change>,
    frames: Sender<Frame>,
}

impl Relay {
    async fn run(mut self) {
        loop {
            let frame = match self.changes.recv().await {
                Ok(change) => Frame::Change(Arc::new(SharedChange {
                    change,
                    firehose: OnceLock::new(),
                })),
                Err(RecvError::Lagged(missed)) => {
                    tracing::debug!(
                        cluster = %self.cluster,
                        missed,
                        "update relay lagged the change bus"
                    );
                    Frame::Resync
                }
                Err(RecvError::Closed) => return,
            };

            let _ = self.frames.send(frame);
        }
    }
}
