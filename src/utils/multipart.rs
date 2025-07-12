use axum::body::{Body, Bytes};
use axum::extract::{FromRequest, Request};
use axum::http::header::CONTENT_TYPE;
use axum::http::{HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::RequestExt;
use std::convert::Infallible;
use std::future::Future;
use thiserror::Error;
use tokio_stream::StreamExt;

/// Errors that may occur during parsing or building of multipart messages.
#[derive(Error, Debug)]
pub enum MultipartError {
    #[error("Invalid boundary \"{0}\" contains invalid characters")]
    InvalidBoundary(String),

    #[error("No parts in multipart message")]
    EmptyMessage,

    #[error("Referenced start Content-ID not found: {0}")]
    StartNotFound(String),

    #[error("Not a multipart/related request")]
    NotMultipartRelated,
}

impl IntoResponse for MultipartError {
    fn into_response(self) -> Response {
        let body = self.to_string();
        let status = match self {
            Self::InvalidBoundary(_) | Self::NotMultipartRelated => StatusCode::BAD_REQUEST,
            Self::EmptyMessage | Self::StartNotFound(_) => StatusCode::UNPROCESSABLE_ENTITY,
        };

        (status, body).into_response()
    }
}

/// A single part of a multipart message.
#[derive(Clone, Debug)]
pub struct Part {
    content_type: String,
    body: Bytes,
    content_id: Option<String>,
    encoding: Option<String>,
}

impl Part {
    /// Create a new Part with the specified content type and body.
    pub fn new(content_type: impl Into<String>, body: impl Into<Bytes>) -> Self {
        Self {
            content_type: content_type.into(),
            body: body.into(),
            content_id: None,
            encoding: None,
        }
    }

    /// Set the Content-ID header for this part.
    pub fn with_id(mut self, id: impl Into<String>) -> Self {
        self.content_id = Some(id.into());
        self
    }

    /// Set the Content-Transfer-Encoding header for this part.
    pub fn with_encoding(mut self, encoding: impl Into<String>) -> Self {
        self.encoding = Some(encoding.into());
        self
    }

    /// Format the headers for this part.
    fn format_headers(&self, boundary: &str) -> String {
        let mut headers = vec![
            format!("--{}", boundary),
            format!("Content-Type: {}", self.content_type),
        ];

        if let Some(id) = &self.content_id {
            headers.push(format!("Content-ID: <{id}>"));
        }

        if let Some(enc) = &self.encoding {
            headers.push(format!("Content-Transfer-Encoding: {enc}"));
        }

        format!("{}\r\n\r\n", headers.join("\r\n"))
    }
}

/// Configuration for a multipart/related message.
#[derive(Clone, Debug)]
pub struct RelatedConfig {
    boundary: String,
    root_type: Option<String>,
    start: Option<String>,
}

impl RelatedConfig {
    /// Create a new RelatedConfig with the specified boundary.
    ///
    /// Returns an error if the boundary is invalid.
    pub fn new(boundary: impl Into<String>) -> Result<Self, MultipartError> {
        let boundary = boundary.into();
        if boundary.contains(|c: char| c.is_whitespace() || c == '"' || c == '\\') {
            return Err(MultipartError::InvalidBoundary(boundary));
        }

        Ok(Self {
            boundary,
            root_type: None,
            start: None,
        })
    }

    /// Set the root Content-Type for the message.
    pub fn root_type(mut self, content_type: impl Into<String>) -> Self {
        self.root_type = Some(content_type.into());
        self
    }

    /// Set the Content-ID of the starting part for the message.
    pub fn start(mut self, content_id: impl Into<String>) -> Self {
        self.start = Some(content_id.into());
        self
    }
}

/// A builder for creating multipart/related messages.
#[derive(Clone)]
pub struct Related {
    config: RelatedConfig,
    parts: Vec<Part>,
}

impl Related {
    /// Create a new MultipartBuilder with the specified configuration.
    pub fn new(config: RelatedConfig) -> Self {
        Self {
            config,
            parts: Vec::new(),
        }
    }

    /// Add a part to the multipart message.
    pub fn add_part(&mut self, part: Part) {
        self.parts.push(part);
    }

    /// Validate the parts and configuration.
    fn validate(&self) -> Result<(), MultipartError> {
        if self.parts.is_empty() {
            return Err(MultipartError::EmptyMessage);
        }

        if let Some(start) = &self.config.start {
            if !self
                .parts
                .iter()
                .any(|part| part.content_id.as_ref() == Some(start))
            {
                return Err(MultipartError::StartNotFound(start.clone()));
            }
        }

        Ok(())
    }

    /// Build the Content-Type header for the message.
    fn build_content_type(&self) -> String {
        let mut content_type = format!("multipart/related; boundary={}", self.config.boundary);

        if let Some(root_type) = &self.config.root_type {
            content_type.push_str(&format!(r#"; type="{root_type}""#));
        }

        if let Some(start) = &self.config.start {
            content_type.push_str(&format!(r#"; start="<{start}>""#));
        }

        content_type
    }

    /// Build the multipart message and return it as an HTTP response.
    ///
    /// Returns an error if validation fails.
    pub fn build(self) -> Result<Response, MultipartError> {
        self.validate()?;
        let boundary = self.config.boundary.clone();
        let parts = self.parts.clone();
        let content_type = self.build_content_type();

        let body_stream = tokio_stream::iter(parts).map(move |part| {
            let boundary = boundary.clone();
            let headers = part.format_headers(&boundary);
            let body_bytes = [headers.as_bytes(), &part.body, b"\r\n"].concat();

            Ok::<_, Infallible>(Bytes::from(body_bytes))
        });

        let final_boundary = format!("--{}--\r\n", self.config.boundary.clone());
        let body_stream = body_stream.chain(tokio_stream::iter([Ok(Bytes::from(final_boundary))]));

        let response = Response::builder()
            .header(CONTENT_TYPE, content_type)
            .status(StatusCode::OK)
            .body(Body::from_stream(body_stream))
            .expect("failed to build response");

        Ok(response)
    }
}

impl IntoResponse for Related {
    fn into_response(self) -> Response {
        self.build().unwrap_or_else(MultipartError::into_response)
    }
}

/// Handles parsing of `multipart/related` request bodies.
pub struct RelatedBody<'r>(multer::Multipart<'r>);

impl<S> FromRequest<S> for RelatedBody<'_>
where
    S: Send + Sync,
{
    type Rejection = MultipartError;

    /// Extract a RelatedParser from the request.
    ///
    /// Returns an error if the request is not multipart/related or if the content type is invalid.
    fn from_request(
        req: Request,
        _: &S,
    ) -> impl Future<Output = Result<Self, <Self as FromRequest<S>>::Rejection>> + Send {
        Box::pin(async move {
            let content_type = req
                .headers()
                .get(CONTENT_TYPE)
                .map(HeaderValue::to_str)
                .ok_or(MultipartError::NotMultipartRelated)?
                .map_err(|_| MultipartError::NotMultipartRelated)?;

            // Extract the boundary from the Content-Type header.
            let boundary = RelatedBody::parse_boundary(content_type)
                .map_err(|_| MultipartError::NotMultipartRelated)?;

            // Get the request body and initialize the `multer` Multipart parser.
            let stream = req.with_limited_body().into_body();
            let multipart = multer::Multipart::new(stream.into_data_stream(), boundary);

            Ok(Self(multipart))
        })
    }
}

impl<'r> RelatedBody<'r> {
    /// Parses the boundary parameter from a `multipart/related` Content-Type header.
    fn parse_boundary(content_type: &str) -> multer::Result<String> {
        let mime = content_type
            .parse::<mime::Mime>()
            .map_err(multer::Error::DecodeContentType)?;

        if !(mime.type_() == mime::MULTIPART && mime.subtype().as_str() == "related") {
            return Err(multer::Error::NoMultipart);
        }

        mime.get_param(mime::BOUNDARY)
            .map(|name| name.as_str().to_owned())
            .ok_or(multer::Error::NoBoundary)
    }

    /// Returns the next field in the multipart stream.
    pub async fn next_field(&mut self) -> multer::Result<Option<multer::Field<'r>>> {
        self.0.next_field().await
    }
}

/// Generate a random boundary string for multipart messages.
pub fn random_boundary() -> String {
    uuid::Uuid::new_v4().to_string()
}
