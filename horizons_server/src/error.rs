use axum::Json;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use horizons_core::error as core_error;
use serde::Serialize;

#[derive(Debug, Serialize)]
pub struct ErrorBody {
    pub error: String,
}

#[derive(thiserror::Error, Debug)]
pub enum ApiError {
    #[error("missing x-org-id header")]
    MissingOrgId,

    #[error("invalid x-org-id header: {0}")]
    InvalidOrgId(String),

    #[error("invalid x-project-id header: {0}")]
    InvalidProjectId(String),

    #[error("invalid x-user-id header: {0}")]
    InvalidUserId(String),

    #[error("{0}")]
    Core(#[from] horizons_core::Error),

    #[error("{0}")]
    Events(#[from] horizons_core::events::Error),

    #[error("invalid input: {0}")]
    InvalidInput(String),
}

impl ApiError {
    fn status_code(&self) -> StatusCode {
        match self {
            ApiError::MissingOrgId
            | ApiError::InvalidOrgId(_)
            | ApiError::InvalidProjectId(_)
            | ApiError::InvalidUserId(_) => StatusCode::BAD_REQUEST,
            ApiError::InvalidInput(_) => StatusCode::BAD_REQUEST,
            ApiError::Core(err) => match err {
                core_error::Error::InvalidInput(_) => StatusCode::BAD_REQUEST,
                core_error::Error::NotFound(_) => StatusCode::NOT_FOUND,
                core_error::Error::Conflict(_) => StatusCode::CONFLICT,
                core_error::Error::Unauthorized(_) => StatusCode::UNAUTHORIZED,
                core_error::Error::Backend { .. } | core_error::Error::BackendMessage(_) => {
                    StatusCode::BAD_GATEWAY
                }
            },
            ApiError::Events(err) => match err {
                horizons_core::events::Error::InvalidOrgId
                | horizons_core::events::Error::InvalidTopic
                | horizons_core::events::Error::InvalidSource
                | horizons_core::events::Error::InvalidDedupeKey
                | horizons_core::events::Error::InvalidTopicPattern
                | horizons_core::events::Error::InvalidSubscriptionHandler
                | horizons_core::events::Error::InvalidQuery => StatusCode::BAD_REQUEST,
                horizons_core::events::Error::Message { .. } => StatusCode::BAD_REQUEST,
                _ => StatusCode::BAD_REQUEST,
            },
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let code = self.status_code();
        let body = ErrorBody {
            error: self.to_string(),
        };
        (code, Json(body)).into_response()
    }
}
