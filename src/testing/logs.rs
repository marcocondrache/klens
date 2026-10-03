use std::io;
use std::sync::{Arc, Mutex, OnceLock};

use tracing::subscriber::{DefaultGuard, NoSubscriber};
use tracing::{Dispatch, Level};
use tracing_subscriber::fmt::MakeWriter;

pub struct LogCapture {
    buffer: Buffer,
    _guard: DefaultGuard,
}

impl LogCapture {
    pub fn at(level: Level) -> Self {
        // While one dispatcher exists, tracing asks only the registering
        // thread's subscriber whether a new callsite is wanted. A callsite
        // another test's thread hits first would then stay silent here. A
        // second, idle dispatcher makes it ask every live subscriber.
        static IDLE: OnceLock<Dispatch> = OnceLock::new();
        IDLE.get_or_init(|| Dispatch::new(NoSubscriber::default()));

        let buffer = Buffer::default();
        let subscriber = tracing_subscriber::fmt()
            .with_writer(buffer.clone())
            .with_max_level(level)
            .with_ansi(false)
            .without_time()
            .finish();
        Self {
            buffer,
            _guard: tracing::subscriber::set_default(subscriber),
        }
    }

    pub fn text(&self) -> String {
        String::from_utf8_lossy(&self.buffer.0.lock().expect("log buffer")).into_owned()
    }

    pub fn count(&self, needle: &str) -> usize {
        self.text().matches(needle).count()
    }

    #[track_caller]
    pub fn assert_contains(&self, needle: &str) {
        let text = self.text();
        assert!(text.contains(needle), "no `{needle}` in the logs:\n{text}");
    }

    #[track_caller]
    pub fn assert_lacks(&self, needle: &str) {
        let text = self.text();
        assert!(!text.contains(needle), "`{needle}` in the logs:\n{text}");
    }
}

#[derive(Clone, Default)]
struct Buffer(Arc<Mutex<Vec<u8>>>);

impl io::Write for Buffer {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0.lock().expect("log buffer").extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl MakeWriter<'_> for Buffer {
    type Writer = Self;

    fn make_writer(&self) -> Self::Writer {
        self.clone()
    }
}
