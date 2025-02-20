use axum::body::Body;
use axum::http::header::CONTENT_TYPE;
use axum::http::{HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use dicom::core::Tag;
use dicom::object::InMemDicomObject;

/// Helper struct to convert a `Vec<serde_json::value::Value>` into a DICOM JSON response.
pub struct Image(pub &'static str, pub Vec<u8>);

impl IntoResponse for Image {
    /// Converts the `RenderedResponse` into an HTTP response.
    fn into_response(self) -> Response {
        Response::builder()
            .status(StatusCode::OK)
            .header(CONTENT_TYPE, HeaderValue::from_static(self.0))
            .body(Body::from(self.1))
            .expect("Failed to build response")
    }
}

/// Helper struct to convert a `Vec<serde_json::value::Value>` into a DICOM JSON response.
pub struct Json(pub Vec<serde_json::value::Value>);

impl IntoResponse for Json {
    #[rustfmt::skip]
    fn into_response(self) -> Response {
        Response::builder()
            .status(StatusCode::OK)
            .header(CONTENT_TYPE, HeaderValue::from_static("application/dicom+json"))
            .body(Body::from(serde_json::to_vec(&self.0).unwrap()))
            .expect("Failed to build response")
    }
}

/// Gets an optional string value from a DICOM attribute with the given tag.
pub fn element_to_str(obj: &InMemDicomObject, tag: Tag) -> Option<String> {
    obj.element(tag)
        .ok()
        .and_then(|e| e.to_str().ok().map(String::from))
}

/// Gets a string value from a DICOM attribute with the given tag, or an empty string if failed.
pub fn empty_if_unknown(obj: &InMemDicomObject, tag: Tag) -> String {
    element_to_str(obj, tag).unwrap_or_default()
}
