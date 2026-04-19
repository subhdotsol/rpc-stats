use actix_web::{get, post, web, HttpResponse, Responder};
use rpc_core::types::{HealthResponse, ScheduleRunResponse};
use std::sync::Arc;
use dashmap::DashMap;
use kafka::FutureProducer;
use rpc_core::types::rpc::{RpcProvider, SentTx};
use solana_sdk::signature::Keypair;
use crate::scheduler::run_batch;

pub struct AppState {
    pub providers: Vec<RpcProvider>,
    pub payer: Arc<Keypair>,
    pub producer: Arc<FutureProducer>,
    pub sent_map: Arc<DashMap<String, SentTx>>,
}

#[get("/internal/schedule/health")]
pub async fn health() -> impl Responder {
    HttpResponse::Ok().json(HealthResponse { status: "ok" })
}

#[post("/internal/schedule/run")]
pub async fn run_schedule(data: web::Data<Arc<AppState>>) -> impl Responder {
    let providers = data.providers.clone();
    let payer = data.payer.clone();
    let producer = data.producer.clone();
    let sent_map = data.sent_map.clone();

    tokio::spawn(async move {
        run_batch(providers, payer, producer, sent_map).await;
    });

    HttpResponse::Ok().json(ScheduleRunResponse { status: "jobs dispatched" })
}
