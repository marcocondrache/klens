use std::time::Duration;

mod kafka;
mod server;
mod stream;

pub use kafka::Kafka;
pub use server::Klens;
pub use stream::{Event, Stream};

const PATIENCE: Duration = Duration::from_secs(30);
const POLL: Duration = Duration::from_millis(100);
