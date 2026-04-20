use actix_web::{HttpResponse, ResponseError, http::StatusCode};
use rpc_core::types::api::{ApiErrorDetail, ApiErrorResponse};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum ApiError {
    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),

    #[error("Not found: {0}")]
    NotFound(String),

    #[error("Invalid query: {0}")]
    InvalidQuery(String),

    #[error(transparent)]
    Internal(#[from] anyhow::Error),
}

impl ResponseError for ApiError {
    fn error_response(&self) -> HttpResponse {
        let (status, code, message) = match self {
            ApiError::Database(_e) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "DB_ERROR".to_string(),
                "A database error occurred".to_string(), // don't leak db details optionally, but let's keep it simple
            ),
            ApiError::NotFound(m) => (StatusCode::NOT_FOUND, "NOT_FOUND".to_string(), m.clone()),
            ApiError::InvalidQuery(m) => {
                (StatusCode::BAD_REQUEST, "INVALID_QUERY".to_string(), m.clone())
            }
            ApiError::Internal(e) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "INTERNAL_ERROR".to_string(),
                e.to_string(),
            ),
        };

        HttpResponse::build(status).json(ApiErrorResponse {
            error: ApiErrorDetail { code, message },
        })
    }

    fn status_code(&self) -> StatusCode {
        match self {
            ApiError::Database(_) | ApiError::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
            ApiError::NotFound(_) => StatusCode::NOT_FOUND,
            ApiError::InvalidQuery(_) => StatusCode::BAD_REQUEST,
        }
    }
}
