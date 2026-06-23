use aether_core::{Envelope, EnvelopeKind};
use axum::{
    extract::Json,
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Router,
};
use tokio::net::TcpListener;

#[tokio::main]
async fn main() {
    let port: u16 = std::env::var("AGENT_PORT")
        .unwrap_or_else(|_| "0".to_string())
        .parse()
        .unwrap_or(0);

    let listener = TcpListener::bind(format!("127.0.0.1:{}", port))
        .await
        .expect("failed to bind");

    println!("{}", listener.local_addr().unwrap().port());

    let app = Router::new()
        .route("/aether/invoke", post(handle_invoke))
        .route("/health", get(handle_health));

    axum::serve(listener, app).await.unwrap();
}

async fn handle_invoke(Json(env): Json<Envelope>) -> impl IntoResponse {
    let response = Envelope {
        kind: EnvelopeKind::Result,
        ..env
    };
    (StatusCode::OK, Json(response))
}

async fn handle_health() -> impl IntoResponse {
    (
        StatusCode::OK,
        Json(serde_json::json!({"status": "healthy"})),
    )
}
