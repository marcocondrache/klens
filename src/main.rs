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

    let auth = AuthState::from_config(config.auth.as_ref(), config.mcp.as_ref())
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
    if !auth.is_enabled() {
        tracing::info!(
            hosts = ?config.allowed_hosts.iter().map(ToString::to_string).collect::<Vec<_>>(),
            "auth is off, so klens refuses requests addressed to any host but these"
        );
    }

    if let Some(mcp) = &config.mcp {
        let privileges: Vec<&str> = mcp
            .privileges
            .iter()
            .map(|privilege| privilege.name())
            .collect();
        let clusters = mcp
            .clusters
            .as_ref()
            .map_or_else(|| "every cluster".to_owned(), |names| format!("{names:?}"));
        match &mcp.resource {
            Some(resource) => {
                tracing::info!(
                    ?privileges,
                    %clusters,
                    resource = resource.as_str(),
                    "serving MCP tools at /mcp to holders of an access token from the oidc provider"
                );
                if mcp.token.clients.is_empty() {
                    tracing::warn!(
                        "mcp.token.clients is empty, so /mcp takes a token for its audience from \
                         any client of the oidc provider"
                    );
                }
            }
            None => tracing::info!(
                ?privileges,
                %clusters,
                "serving MCP tools at /mcp to anyone who reaches klens"
            ),
        }
    }

    let _ingest = Ingest::start(&clusters, &config.tuning.ingest);
    let state = AppState::new(clusters, auth, Limits::new(&config.tuning));

    klens::serve(
        router(state, &config.allowed_hosts, config.mcp.as_ref()),
        config.bind,
    )
    .await
}
