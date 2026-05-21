use aether_core::health_poller::HealthPoller;
use aether_core::registry_server::make_registry_router;
use aether_core::registry_store::RegistryStore;
use std::time::Duration;

#[tokio::main]
async fn main() {
    let db_path = std::env::var("AETHER_DB_PATH").unwrap_or_else(|_| "aether.db".to_string());
    let port: u16 = std::env::var("AETHER_PORT")
        .unwrap_or_else(|_| "7070".to_string())
        .parse()
        .unwrap_or(7070);
    let poll_interval_secs: u64 = std::env::var("AETHER_POLL_INTERVAL_SECS")
        .unwrap_or_else(|_| "30".to_string())
        .parse()
        .unwrap_or(30);

    let store = RegistryStore::open(&db_path).unwrap_or_else(|e| {
        eprintln!("Failed to open registry store at {}: {}", db_path, e);
        std::process::exit(1);
    });

    HealthPoller::new(store.clone(), Duration::from_secs(poll_interval_secs)).start();

    let app = make_registry_router(store, poll_interval_secs);
    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{}", port))
        .await
        .unwrap_or_else(|e| {
            eprintln!("Failed to bind port {}: {}", port, e);
            std::process::exit(1);
        });

    eprintln!("Aether registry listening on port {}", port);
    axum::serve(listener, app).await.unwrap();
}
