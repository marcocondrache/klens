use std::sync::Arc;

use anyhow::Context;
use klens::app::{AppState, router};
use klens::config::Config;
use klens::kafka::QueryEngine;
use klens::kafka::ingest::LaneIntervals;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config = Config::load(Config::path())?;
    let _telemetry =
        klens::telemetry::Telemetry::init(&config.log_level, env!("CARGO_CRATE_NAME"))?;

    let engine = Arc::new(
        QueryEngine::from_config(&config)
            .await
            .context("failed to initialize kafka query engine")?,
    );

    tracing::info!(clusters = ?engine.names(), "configured kafka clusters");

    let auth = klens::app::AuthState::from_config(config.auth.as_ref())
        .await
        .context("failed to initialize authentication")?;

    if auth.is_enabled() {
        tracing::info!("oidc authentication enabled");
    }

    let state = AppState::with_auth(engine, auth).with_ingest(LaneIntervals::default());

    klens::serve(router(state), config.bind).await
}
