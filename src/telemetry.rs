use std::io::IsTerminal;

use tracing_appender::non_blocking::{NonBlockingBuilder, WorkerGuard};
use tracing_subscriber::{
    EnvFilter, filter::ParseError, layer::SubscriberExt, util::SubscriberInitExt,
};

use crate::config::LogLevel;

pub struct Telemetry {
    _guard: WorkerGuard,
}

impl Telemetry {
    pub fn init(level: LogLevel, target: &str) -> anyhow::Result<Self> {
        let filter = filter(level, target)?;
        // The default 128k-line queue is allocated and touched up front, about
        // 4 MB resident. Lines past the limit are dropped either way.
        let (writer, guard) = NonBlockingBuilder::default()
            .buffered_lines_limit(8_192)
            .finish(std::io::stdout());
        let layer = tracing_subscriber::fmt::layer()
            .with_ansi(cfg!(debug_assertions) && std::io::stdout().is_terminal());

        tracing_subscriber::registry()
            .with(filter)
            .with(layer.with_writer(writer))
            .try_init()?;

        Ok(Self { _guard: guard })
    }
}

/// rmcp logs tool arguments and results at debug and warns on each request it
/// refuses, so it never logs below error.
fn filter(level: LogLevel, target: &str) -> Result<EnvFilter, ParseError> {
    let directives = match level {
        LogLevel::Off => "off".to_owned(),
        LogLevel::Error => "error".to_owned(),
        LogLevel::Warn => "warn,rmcp=error".to_owned(),
        LogLevel::Info => format!("warn,rmcp=error,{target}=info"),
        LogLevel::Debug => format!("warn,rmcp=error,{target}=debug"),
        LogLevel::Trace => format!("warn,rmcp=error,{target}=trace"),
    };
    EnvFilter::builder().parse(directives)
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
mod tests {
    use std::time::Duration;

    use tracing::Level;

    use super::{LogLevel, filter, log_http_completed};
    use crate::testing::LogCapture;

    #[test]
    fn a_log_level_filters_klens_and_leaves_its_dependencies_at_warn_and_rmcp_at_error() {
        for (level, expected) in [
            (LogLevel::Off, "off"),
            (LogLevel::Error, "error"),
            (LogLevel::Warn, "rmcp=error,warn"),
            (LogLevel::Info, "klens=info,rmcp=error,warn"),
            (LogLevel::Debug, "klens=debug,rmcp=error,warn"),
            (LogLevel::Trace, "klens=trace,rmcp=error,warn"),
        ] {
            assert_eq!(filter(level, "klens").unwrap().to_string(), expected);
        }
    }

    #[test]
    fn log_http_completed_204_is_silent_at_info() {
        let logs = LogCapture::at(Level::INFO);
        log_http_completed(204, Duration::from_millis(1));
        logs.assert_lacks("request completed");
    }

    #[test]
    fn log_http_completed_500_is_warn() {
        let logs = LogCapture::at(Level::WARN);
        log_http_completed(500, Duration::from_millis(3));
        logs.assert_contains("request completed");
        logs.assert_contains("status=500");
        logs.assert_contains("WARN");
    }
}
