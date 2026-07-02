//! Structured error envelope: `{ "error": { "code", "message" } }`, so clients
//! get a consistent body instead of axum's default plaintext.

use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde_json::json;

#[derive(Debug)]
pub enum ApiError {
    /// Unknown kind, or a named object not in cache.
    NotFound(String),
    /// A feature is disabled, or a prerequisite isn't ready (e.g. no endpoint yet).
    Unavailable(String),
    /// An upstream call MUSE proxied on the caller's behalf failed.
    BadGateway(String),
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, code, message) = match self {
            ApiError::NotFound(msg) => (StatusCode::NOT_FOUND, "not_found", msg),
            ApiError::Unavailable(msg) => (StatusCode::SERVICE_UNAVAILABLE, "unavailable", msg),
            ApiError::BadGateway(msg) => (StatusCode::BAD_GATEWAY, "bad_gateway", msg),
        };
        (
            status,
            Json(json!({ "error": { "code": code, "message": message } })),
        )
            .into_response()
    }
}
