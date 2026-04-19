mod common;

use actix_web::{test, App, web};
use api::routes::rpcs;

#[actix_web::test]
async fn test_get_rpcs_list() {
    if std::env::var("DATABASE_URL").is_err() && std::env::var("CI").is_ok() { return; }

    let state = common::setup_app_state().await;

    let app = test::init_service(
        App::new()
            .app_data(web::Data::new(state))
            .configure(rpcs::config)
    ).await;

    let req = test::TestRequest::get().uri("/rpcs").to_request();
    let resp = test::call_service(&app, req).await;
    
    assert!(resp.status().is_success());
}

#[actix_web::test]
async fn test_get_rpc_not_found() {
    if std::env::var("DATABASE_URL").is_err() && std::env::var("CI").is_ok() { return; }

    let state = common::setup_app_state().await;

    let app = test::init_service(
        App::new()
            .app_data(web::Data::new(state))
            .configure(rpcs::config)
    ).await;

    // Use a random UUID that likely doesn't exist
    let req = test::TestRequest::get().uri("/rpcs/some-fake-id-123").to_request();
    let resp = test::call_service(&app, req).await;
    
    assert!(resp.status().is_client_error());
}
