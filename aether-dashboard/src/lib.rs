pub mod server;
pub mod state;

pub use server::DashboardConfig;
pub use state::AppState;

pub async fn start(
    state: std::sync::Arc<AppState>,
    config: DashboardConfig,
) -> std::io::Result<std::net::SocketAddr> {
    server::start(state, config).await
}
