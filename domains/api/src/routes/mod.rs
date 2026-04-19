use actix_web::web;

pub mod leaderboard;
pub mod rpcs;
pub mod incidents;
pub mod system;
pub mod benchmarks;
pub mod health;

pub fn config(cfg: &mut web::ServiceConfig) {
    // Top-level endpoints
    cfg.configure(health::config);

    // API versioned endpoints
    cfg.service(
        web::scope("/api/v1")
            .configure(leaderboard::config)
            .configure(rpcs::config)
            .configure(incidents::config)
            .configure(system::config)
            .configure(benchmarks::config)
    );
}
