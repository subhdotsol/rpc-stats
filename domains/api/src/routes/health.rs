use actix_web::{get, HttpResponse, Responder, web};

#[get("/health")]
async fn health_check() -> impl Responder {
    HttpResponse::Ok().body("OK")
}

#[get("/")]
async fn index() -> impl Responder {
    HttpResponse::Ok().body("Welcome to RPC STATS")
}

pub fn config(cfg: &mut web::ServiceConfig) {
    cfg.service(health_check).service(index);
}
