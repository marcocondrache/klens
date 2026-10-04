use anyhow::Context;
use klens::app::{AppState, AuthState, Limits, router};
use klens::config::Config;
use klens::kafka::Clusters;
use klens::kafka::ingest::Ingest;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let path = std::env::var_os("KLENS_CONFIG_PATH").unwrap_or_else(|| "config.yaml".into());
    let config = Config::load(path)?;
    let _telemetry = klens::telemetry::Telemetry::init(config.log_level, env!("CARGO_CRATE_NAME"))?;

    let clusters = Clusters::connect(&config.clusters, &config.tuning)
        .await
        .context("failed to connect to the configured kafka clusters")?;

    tracing::info!(
        clusters = ?clusters.names().collect::<Vec<_>>(),
        "configured kafka clusters"
    );

    let auth = AuthState::from_config(config.auth.as_ref())
        .await
        .context("failed to initialize authentication")?;

    if auth.is_enabled() {
        tracing::info!("oidc authentication enabled");
    }

    let unguarded = config.writable_without_auth();
    if !unguarded.is_empty() {
        tracing::warn!(
            clusters = ?unguarded,
            "auth is off, so anyone who reaches klens can change these writable clusters"
        );
    }

    let _ingest = Ingest::start(&clusters, &config.tuning.ingest);
    let state = AppState::new(clusters, auth, Limits::new(&config.tuning));

    klens::serve(router(state), config.bind).await
}
