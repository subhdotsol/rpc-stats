mod common;

use actix_web::{test, App, web};
use api::routes::leaderboard;

#[actix_web::test]
async fn test_get_leaderboard_success() {
    if std::env::var("DATABASE_URL").is_err() && std::env::var("CI").is_ok() {
        println!("Skipping DB test in CI without DATABASE_URL");
        return;
    }

    // Try acquiring state; panic if local DB is unreachable to alert dev
    let state = common::setup_app_state().await;

    let app = test::init_service(
        App::new()
            .app_data(web::Data::new(state))
            .configure(leaderboard::config)
    ).await;

    let req = test::TestRequest::get().uri("/leaderboard?window=5m").to_request();
    let resp = test::call_service(&app, req).await;
    
    assert!(resp.status().is_success());
}

#[actix_web::test]
async fn test_get_leaderboard_invalid_window() {
    if std::env::var("DATABASE_URL").is_err() && std::env::var("CI").is_ok() { return; }
    
    let state = common::setup_app_state().await;

    let app = test::init_service(
        App::new()
            .app_data(web::Data::new(state))
            .configure(leaderboard::config)
    ).await;

    let req = test::TestRequest::get().uri("/leaderboard?window=10m").to_request();
    let resp = test::call_service(&app, req).await;
    
    assert!(resp.status().is_client_error());
}
