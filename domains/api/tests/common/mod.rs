use api::app_state::AppState;
use sqlx::postgres::PgPoolOptions;

use rpc_core::config::Config;

pub async fn setup_app_state() -> AppState {
    dotenvy::dotenv().ok();
    // Use fallback to avoid failing if no env variables exist in tests
    let config = Config::from_env().unwrap_or_else(|_| Config {
        database_url: "postgres://user:pass@localhost:5432/rpc".to_string(),
        api_host: "127.0.0.1".to_string(),
        api_port: 8080,
        helius_rpc: "".to_string(),
        alchemy_rpc: "".to_string(),
        keypair_path: "~/.config/solana/id.json".to_string(),
        probe_interval_secs: 30,
        kafka_brokers: "localhost:9092".to_string(),
    });
    // We attempt to connect; if it fails in CI/CD without DB, we might panic.
    // For a real setup, we would use Testcontainers or conditional execution.
    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&config.database_url)
        .await
        .expect("Test DB connection failed. Ensure docker-compose is running.");
        
    AppState { db: pool }
}
