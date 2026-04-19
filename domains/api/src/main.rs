use actix_web::{web, App, HttpServer};
use tracing::info;
use tracing_subscriber::EnvFilter;
use sqlx::postgres::PgPoolOptions;

use rpc_core::config::Config;
use api::app_state::AppState;
use api::routes;

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

    let state = AppState { db: pool };

    info!(
        "Starting Actix RPC Stats server at http://{}:{}",
        config.api_host, config.api_port
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
