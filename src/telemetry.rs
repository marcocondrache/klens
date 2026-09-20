use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::{
    EnvFilter, filter::ParseError, layer::SubscriberExt, util::SubscriberInitExt,
};

mod graphql;

pub(crate) use graphql::{DocumentHead, OperationId, complete, record_ws_upgrade};

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
