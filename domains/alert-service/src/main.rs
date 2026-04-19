use actix_web::{get, post, App, HttpResponse, HttpServer, Responder};
use serde_json::json;
use rpc_core::types::HealthResponse;

#[get("/internal/alerts/health")]
async fn health() -> impl Responder {
    HttpResponse::Ok().json(HealthResponse { status: "ok" })
}

#[post("/internal/alerts/test")]
async fn test_alert() -> impl Responder {
    println!("Triggering test alert...");
    // Placeholder for triggering test alert
    HttpResponse::Ok().json(json!({ "message": "Test alert triggered" }))
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let port = 7005;
    println!("Starting alert-service on port {}", port);

    HttpServer::new(|| {
        App::new()
            .service(health)
            .service(test_alert)
    })
    .bind(("0.0.0.0", port))?
    .run()
    .await
}
