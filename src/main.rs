use std::sync::Arc;

use anyhow::Context;
use klens::app::{AppState, router};
use klens::config::Config;
use klens::kafka::SessionSet;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config = Config::load(Config::path())?;
    let _telemetry =
        klens::telemetry::Telemetry::init(&config.log_level, env!("CARGO_CRATE_NAME"))?;

    let sessions = Arc::new(
        SessionSet::from_config(&config)
            .await
            .context("failed to connect to the configured kafka clusters")?,
    );

    tracing::info!(clusters = ?sessions.names(), "configured kafka clusters");

    let auth = klens::app::AuthState::from_config(config.auth.as_ref())
        .await
        .context("failed to initialize authentication")?;

    if auth.is_enabled() {
        tracing::info!("oidc authentication enabled");
    }

    let state = AppState::with_auth(sessions, auth)
        .with_writes_from(&config)
        .with_ingest_from(&config);

    klens::serve(router(state), config.bind).await
}
