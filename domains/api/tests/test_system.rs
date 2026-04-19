mod common;

use actix_web::{test, App, web};
use api::routes::system;

#[actix_web::test]
async fn test_get_summary_success() {
    if std::env::var("DATABASE_URL").is_err() && std::env::var("CI").is_ok() { return; }

    let state = common::setup_app_state().await;

    let app = test::init_service(
        App::new()
            .app_data(web::Data::new(state))
            .configure(system::config)
    ).await;

    let req = test::TestRequest::get().uri("/summary").to_request();
    let resp = test::call_service(&app, req).await;
    
    // Status should be 200 OK since query handles empty sets with 0
    assert!(resp.status().is_success());
}
