//! `web` binary: HTTP API.

use std::net::SocketAddr;

use sos_allert::bootstrap;
use sos_allert::config::Config;
use sos_allert::delivery::http::routes;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tracing();

    if std::env::args().nth(1).as_deref() == Some("migrate") {
        // Explicit DDL: never on boot, never from a request.
        let config = Config::load()?;
        let state = bootstrap::build_state(config)?;
        bootstrap::migrate(state.db()).await?;
        tracing::info!("migrations applied");
        return Ok(());
    }

    let config = Config::load()?;
    let port = config.web_port();
    let state = bootstrap::build_state(config)?;

    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    state.spawn_background(shutdown_rx);

    let app = routes::router(state);
    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .map_err(|e| anyhow::anyhow!("listen on {addr}: {e}"))?;

    tracing::info!(%addr, "web listening");

    let result = axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .with_graceful_shutdown(shutdown_signal())
    .await;

    let _ = shutdown_tx.send(true);
    result?;

    tracing::info!("web stopped");
    Ok(())
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info,sos_allert=debug"));
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(true)
        .try_init();
}

/// SIGINT/SIGTERM drain the listener; the accept loop stops and background
/// loops see the shutdown flag.
async fn shutdown_signal() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };

    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut sig) => {
                sig.recv().await;
            }
            Err(e) => tracing::error!(error = %e, "cannot install SIGTERM handler"),
        }
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
    tracing::info!("shutdown signal received");
}
