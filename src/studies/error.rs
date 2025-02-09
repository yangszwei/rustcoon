use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use thiserror::Error;

/// Represents all possible errors that can occur within the studies service.
#[derive(Error, Debug)]
pub enum StudiesServiceError {
    #[error("A database error occurred: {0}")]
    DatabaseFailure(sqlx::Error),

    #[error("Failed to serialize DICOM data: {0}")]
    DicomJsonError(serde_json::Error),

    #[error("Failed to render the DICOM image: {0}")]
    DicomRenderError(Box<dyn std::error::Error>),

    #[error("Failed to read the file: {0}")]
    FileReadFailure(Box<dyn std::error::Error>),

    #[error("The requested resource was not found.")]
    NotFound,

    #[error("An unexpected error occurred.")]
    Other(Box<dyn std::error::Error>),
}

impl From<sqlx::Error> for StudiesServiceError {
    fn from(err: sqlx::Error) -> Self {
        StudiesServiceError::DatabaseFailure(err)
    }
}

impl IntoResponse for StudiesServiceError {
    fn into_response(self) -> Response {
        match self {
            StudiesServiceError::NotFound => StatusCode::NOT_FOUND.into_response(),
            _ => {
                tracing::error!("{}", self);
                StatusCode::INTERNAL_SERVER_ERROR.into_response()
            }
        }
    }
}
