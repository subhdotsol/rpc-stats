use actix_web::{web, App, HttpServer};
use tracing::info;
use tracing_subscriber::EnvFilter;
use sqlx::postgres::PgPoolOptions;

use rpc_core::config::Config;
use api::app_state::AppState;
use api::routes;
use api::cron::{spawn_detect_incidents, spawn_refresh_leaderboard, spawn_snapshot_rankings};

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    dotenvy::dotenv().ok();
    // Initialize logging
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive(tracing::Level::INFO.into()))
        .init();

    let config = Config::from_env().expect("Failed to load environment configuration");

    let pool = PgPoolOptions::new()
        .max_connections(10)
        .connect(&config.database_url)
        .await
        .expect("Failed to connect to Postgres");

    let redis = rpc_cache::create_pool(&config.redis_url)
        .expect("Failed to create Redis pool");

    let state = AppState {
        db: pool.clone(),
        redis: redis.clone(),
    };

    // ── Background cron jobs ──────────────────────────────────────────────────
    // Refresh leaderboard_current every 30 seconds
    spawn_refresh_leaderboard(pool.clone(), redis.clone(), 30);
    // Detect new incidents every 60 seconds
    spawn_detect_incidents(pool.clone(), redis.clone(), 60);
    // Snapshot historical rankings every hour
    spawn_snapshot_rankings(pool.clone(), 3_600);
    // ─────────────────────────────────────────────────────────────────────────

    info!(
        "Starting Actix RPC Stats server at http://{}:{} (Redis: {})",
        config.api_host, config.api_port, config.redis_url
    );


    HttpServer::new(move || {
        App::new()
            .app_data(web::Data::new(state.clone()))
            .configure(routes::config)
    })
    .bind((config.api_host.as_str(), config.api_port))?
    .run()
    .await
}
