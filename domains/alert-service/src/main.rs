use actix_web::{App, HttpServer, web};
use sqlx::postgres::PgPoolOptions;
use tracing::info;
use tracing_subscriber::EnvFilter;

use alert_service::app_state::AppState;
use alert_service::listener;
use alert_service::routes;

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    dotenvy::dotenv().ok();

    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::from_default_env().add_directive(tracing::Level::INFO.into()),
        )
        .init();

    let config = rpc_core::config::Config::from_env()
        .expect("failed to load config from env");

    // ── Postgres ─────────────────────────────────────────────────────────────
    let pool = PgPoolOptions::new()
        .max_connections(10)
        .connect(&config.database_url)
        .await
        .expect("failed to connect to Postgres");

    info!("connected to Postgres");

    // ── Postgres LISTEN/NOTIFY for real-time incident alerts ─────────────────
    listener::spawn_pg_listener(pool.clone());

    // ── HTTP server ──────────────────────────────────────────────────────────
    let state = AppState { db: pool };
    let port: u16 = 7005;

    info!("starting alert-service on port {port}");

    HttpServer::new(move || {
        App::new()
            .app_data(web::Data::new(state.clone()))
            .service(routes::health)
            .service(routes::test_alert)
            .service(routes::list_channels)
            .service(routes::create_channel)
            .service(routes::update_channel)
            .service(routes::delete_channel)
            .service(routes::alert_history)
    })
    .bind(("0.0.0.0", port))?
    .run()
    .await
}
