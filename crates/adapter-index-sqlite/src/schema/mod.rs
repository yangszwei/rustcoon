mod attributes;
mod tables;

use std::collections::HashMap;

pub(crate) use attributes::AttributeMapping;
use dicom_core::Tag;
use rustcoon_index::AttributePath;
pub(crate) use tables::{INSTANCES, SERIES, STUDIES, TableId};

#[derive(Debug, Clone)]
pub(crate) struct CatalogSchema {
    by_tag: HashMap<Tag, AttributeMapping>,
}

impl CatalogSchema {
    pub(crate) fn new() -> Self {
        let by_tag = attributes::definitions()
            .into_iter()
            .map(|mapping| (mapping.tag, mapping))
            .collect();
        Self { by_tag }
    }

    pub(crate) fn attribute_for(&self, path: &AttributePath) -> Option<&AttributeMapping> {
        top_level_tag(path).and_then(|tag| self.by_tag.get(&tag))
    }
}

pub(crate) fn top_level_tag(path: &AttributePath) -> Option<Tag> {
    match path.segments().first() {
        Some(rustcoon_index::AttributePathSegment::Tag(tag)) if path.segments().len() == 1 => {
            Some(*tag)
        }
        _ => None,
    }
}

pub(crate) fn format_tag_key(tag: Tag) -> String {
    format!("{:04X}{:04X}", tag.0, tag.1)
}

#[cfg(test)]
mod tests {
    use dicom_dictionary_std::tags;
    use rustcoon_index::AttributePath;

    use super::{CatalogSchema, TableId, format_tag_key};

    #[test]
    fn schema_resolves_known_indexed_attributes() {
        let schema = CatalogSchema::new();
        let mapping = schema
            .attribute_for(&AttributePath::from_tag(tags::MODALITY))
            .expect("mapped modality");

        assert_eq!(mapping.table, TableId::Series);
        assert_eq!(mapping.column, "modality");
    }

    #[test]
    fn format_tag_key_uses_dicom_json_shape() {
        assert_eq!(format_tag_key(tags::SOP_INSTANCE_UID), "00080018");
    }
}
