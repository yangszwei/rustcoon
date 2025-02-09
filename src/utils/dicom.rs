use dicom::core::Tag;
use dicom::object::InMemDicomObject;

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
