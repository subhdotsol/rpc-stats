use thiserror::Error;

#[derive(Debug, Error)]
pub enum AppError {
    #[error("database error: {0}")] Database(#[from] sqlx::Error),

    #[error("redis error: {0}")] Redis(#[from] redis::RedisError),

    #[error("not found: {0}")] NotFound(String),

    #[error("config error: {0}")] Config(#[from] envy::Error),

    #[error("internal: {0}")] Internal(String),
}

impl axum::response::IntoResponse for AppError {
    fn into_response(self) -> axum::response::Response {
        let (status, msg) = match &self {
            AppError::NotFound(_) => (axum::http::StatusCode::NOT_FOUND, self.to_string()),
            _ => (axum::http::StatusCode::INTERNAL_SERVER_ERROR, self.to_string()),
        };
        (status, msg).into_response()
    }
}

pub type AppResult<T> = Result<T, AppError>;
