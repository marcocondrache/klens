use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::{
    EnvFilter, filter::ParseError, layer::SubscriberExt, util::SubscriberInitExt,
};

mod graphql;

pub(crate) use graphql::{OperationId, complete, record_ws_upgrade};

pub struct Telemetry {
    _guard: WorkerGuard,
}

impl Telemetry {
    pub fn init(filter: &str, target: &str) -> anyhow::Result<Self> {
        let filter = filter_from_value(filter, target)?;
        let (writer, guard) = tracing_appender::non_blocking(std::io::stdout());
        let layer = tracing_subscriber::fmt::layer().with_ansi(false);

        tracing_subscriber::registry()
            .with(filter)
            .with(layer.with_writer(writer))
            .try_init()?;

        Ok(Self { _guard: guard })
    }
}

pub fn filter_from_value(value: &str, target: &str) -> Result<EnvFilter, ParseError> {
    match value.trim() {
        "none" | "off" => Ok(EnvFilter::default()),
        "error" => Ok(EnvFilter::default().add_directive(tracing::Level::ERROR.into())),
        "warn" => Ok(EnvFilter::default().add_directive(tracing::Level::WARN.into())),
        "info" => Ok(EnvFilter::default()
            .add_directive(tracing::Level::WARN.into())
            .add_directive(format!("{target}=info").parse()?)),
        "debug" => Ok(EnvFilter::default()
            .add_directive(tracing::Level::WARN.into())
            .add_directive(format!("{target}=debug").parse()?)),
        "trace" => Ok(EnvFilter::default()
            .add_directive(tracing::Level::WARN.into())
            .add_directive(format!("{target}=trace").parse()?)),
        custom => EnvFilter::builder().parse(custom),
    }
}

pub(crate) fn log_http_completed(status: u16, latency: std::time::Duration) {
    let latency_ms = latency.as_millis();
    if status == 500 {
        tracing::warn!(status, latency_ms, "request completed");
    } else {
        tracing::debug!(status, latency_ms, "request completed");
    }
}

#[cfg(test)]
pub(crate) mod capture {
    use std::io;
    use std::sync::{Arc, Mutex};

    #[derive(Clone, Default)]
    pub(crate) struct LogBuf(Arc<Mutex<Vec<u8>>>);

    impl io::Write for LogBuf {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.0.lock().expect("log buf").extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for LogBuf {
        type Writer = Self;

        fn make_writer(&'a self) -> Self::Writer {
            self.clone()
        }
    }

    impl LogBuf {
        pub(crate) fn as_string(&self) -> String {
            String::from_utf8_lossy(&self.0.lock().expect("log buf")).into_owned()
        }
    }

    pub(crate) fn subscriber(
        max_level: tracing::Level,
    ) -> (LogBuf, tracing::subscriber::DefaultGuard) {
        let logs = LogBuf::default();
        let subscriber = tracing_subscriber::fmt()
            .with_writer(logs.clone())
            .with_max_level(max_level)
            .with_ansi(false)
            .without_time()
            .finish();
        (logs, tracing::subscriber::set_default(subscriber))
    }
}
