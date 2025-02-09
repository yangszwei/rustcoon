use axum::http::header::CONTENT_TYPE;
use axum::http::HeaderValue;
use axum::response::{IntoResponse, Response};
use axum::Json;
use dicom::core::value::{DataSetSequence, Value};
use dicom::core::{DataElement, Tag, VR};
use dicom::dictionary_std::tags;
use dicom::object::mem::InMemElement;
use dicom::object::InMemDicomObject;
use dicom_json::DicomJson;

/// The response to a store instances request.
///
/// This struct holds information related to the success or failure of storing
/// a set of SOP (Service-Object Pair) Instances. It includes the retrieval URL,
/// sequences of failed, referenced, and other failure instances.
pub struct StoreInstancesResponse {
    /// URL from which the stored SOP Instances can be retrieved.
    pub retrieve_url: String,

    /// A sequence of SOP Instances that failed to store.
    pub failed_sop_sequence: Vec<FailedSopInstance>,

    /// A sequence of successfully stored SOP Instances.
    pub referenced_sop_sequence: Vec<ReferencedSopInstance>,

    /// A sequence of failure reasons that do not relate to specific SOP Instances.
    pub other_failure_sequence: Vec<OtherFailure>,
}

/// The result associated with storing a single SOP Instance.
///
/// Represents the outcome of storing a SOP Instance, where `Ok` denotes success
/// and `Err` denotes failure.
pub enum Result {
    /// Successfully stored SOP Instance.
    Ok(ReferencedSopInstance),

    /// SOP Instance that failed to store.
    Err(FailedSopInstance),
}

/// An item referencing a single SOP Instance for which storage could not be provided.
pub struct FailedSopInstance {
    /// SOP Class UID of the failed SOP Instance.
    pub sop_class_uid: String,

    /// SOP Instance UID of the failed SOP Instance.
    pub sop_instance_uid: String,

    /// Reason for the failure.
    pub failure_reason: String,
}

impl Into<InMemDicomObject> for FailedSopInstance {
    /// Convert a `FailedSopInstance` into an `InMemDicomObject`.
    #[rustfmt::skip]
    fn into(self) -> InMemDicomObject {
        InMemDicomObject::from_element_iter(vec![
            DataElement::new(tags::REFERENCED_SOP_CLASS_UID, VR::UI, self.sop_class_uid),
            DataElement::new(tags::REFERENCED_SOP_INSTANCE_UID, VR::UI, self.sop_instance_uid),
            DataElement::new(tags::FAILURE_REASON, VR::US, self.failure_reason),
        ])
    }
}

/// An item referencing a single SOP Instance that was successfully stored.
pub struct ReferencedSopInstance {
    /// Not part of the response, used to check if SOP Instances belong to the same study.
    pub study_instance_uid: String,

    /// Not part of the response, used to check if SOP Instances belong to the same series.
    pub series_instance_uid: String,

    /// SOP Class UID of the successfully stored SOP Instance.
    pub sop_class_uid: String,

    /// SOP Instance UID of the successfully stored SOP Instance.
    pub sop_instance_uid: String,

    /// URL from which the successfully stored SOP Instance can be retrieved.
    pub retrieve_url: String,

    /// Optional warning reason.
    pub warning_reason: Option<String>,
}

impl Into<InMemDicomObject> for ReferencedSopInstance {
    /// Convert a `ReferencedSopInstance` into an `InMemDicomObject`.
    #[rustfmt::skip]
    fn into(self) -> InMemDicomObject {
        let mut sequence: Vec<DataElement<InMemDicomObject>> = vec![
            DataElement::new(tags::REFERENCED_SOP_CLASS_UID, VR::UI, self.sop_class_uid),
            DataElement::new(tags::REFERENCED_SOP_INSTANCE_UID, VR::UI, self.sop_instance_uid),
            DataElement::new(tags::RETRIEVE_URL, VR::UR, self.retrieve_url),
        ];

        if let Some(warning_reason) = self.warning_reason {
            sequence.push(DataElement::new(tags::WARNING_REASON, VR::LO, warning_reason));
        }

        InMemDicomObject::from_element_iter(sequence.into_iter())
    }
}

/// A reason not associated with a specific SOP Instance that storage could not be provided.
pub struct OtherFailure {
    /// Reason for the failure.
    pub failure_reason: String,
}

impl Into<InMemDicomObject> for OtherFailure {
    /// Convert an `OtherFailure` into an `InMemDicomObject`.
    #[rustfmt::skip]
    fn into(self) -> InMemDicomObject {
        InMemDicomObject::from_element_iter(vec![
            DataElement::new(tags::FAILURE_REASON, VR::US, self.failure_reason),
        ])
    }
}

impl From<StoreInstancesResponse> for InMemDicomObject {
    /// Convert a `StoreInstancesResponse` into an `InMemDicomObject`.
    ///
    /// This includes sequences for failed, referenced, and other failure SOP Instances,
    /// as well as a retrieval URL.
    #[rustfmt::skip]
    fn from(response: StoreInstancesResponse) -> Self {
        /// Creates a sequence element with the given tag and items.
        fn sequence(tag: Tag, items: Vec<impl Into<InMemDicomObject>>) -> InMemElement {
            let mut element = InMemElement::new(tag, VR::SQ, Value::Sequence(DataSetSequence::empty()));
            element.items_mut().expect("Sequence exists").extend(items.into_iter().map(Into::into));
            element
        }

        let mut obj = Self::from_element_iter([
            InMemElement::new(tags::RETRIEVE_URL, VR::UR, response.retrieve_url),
            sequence(tags::FAILED_SOP_SEQUENCE, response.failed_sop_sequence),
            sequence(tags::REFERENCED_SOP_SEQUENCE, response.referenced_sop_sequence),
        ]);

        if !response.other_failure_sequence.is_empty() {
            obj.put(sequence(tags::OTHER_FAILURES_SEQUENCE, response.other_failure_sequence));
        }

        obj
    }
}

impl IntoResponse for StoreInstancesResponse {
    /// Convert a `StoreInstancesResponse` into an HTTP response.
    fn into_response(self) -> Response {
        let dicom_json = Json(DicomJson::from(InMemDicomObject::from(self)));
        let content_type = HeaderValue::from_static("application/dicom+json");

        Response::builder()
            .header(CONTENT_TYPE, content_type)
            .body(dicom_json.into_response().into_body())
            .expect("Failed to build response")
    }
}
