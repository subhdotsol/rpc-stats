use actix_web::{App, HttpResponse, HttpServer, Responder, get};
use tracing::info;
use tracing_subscriber::EnvFilter;

#[get("/health")]
async fn health_check() -> impl Responder {
    HttpResponse::Ok().body("OK")
}

#[get("/")]
async fn index() -> impl Responder {
    HttpResponse::Ok().body("Welcome to RPC STATS")
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    // Initialize logging
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive(tracing::Level::INFO.into()))
        .init();

    let host = "127.0.0.1";
    let port = 8080;

    info!(
        "Starting Actix RPC Stats server at http://{}:{}",
        host, port
    );

    HttpServer::new(|| App::new().service(index).service(health_check))
        .bind((host, port))?
        .run()
        .await
}
