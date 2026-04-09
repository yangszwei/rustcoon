use dicom_core::Tag;
use thiserror::Error;

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum AttributePathError {
    #[error("attribute path must not be empty")]
    Empty,

    #[error("attribute path cannot start with an item selector")]
    StartsWithItemSelector,

    #[error("attribute path cannot contain consecutive item selectors")]
    ConsecutiveItemSelectors,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ItemSelector {
    Any,
    Index(u32),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AttributePathSegment {
    Tag(Tag),
    Item(ItemSelector),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct AttributePath {
    segments: Vec<AttributePathSegment>,
}

impl AttributePath {
    #[must_use]
    pub fn from_tag(tag: Tag) -> Self {
        Self {
            segments: vec![AttributePathSegment::Tag(tag)],
        }
    }

    #[must_use]
    pub fn push_tag(mut self, tag: Tag) -> Self {
        self.segments.push(AttributePathSegment::Tag(tag));
        self
    }

    #[must_use]
    pub fn push_any(mut self) -> Self {
        self.segments
            .push(AttributePathSegment::Item(ItemSelector::Any));
        self
    }

    #[must_use]
    pub fn push_item(mut self, index: u32) -> Self {
        self.segments
            .push(AttributePathSegment::Item(ItemSelector::Index(index)));
        self
    }

    #[must_use]
    pub fn segments(&self) -> &[AttributePathSegment] {
        &self.segments
    }

    #[must_use]
    pub fn matches(&self, other: &Self) -> bool {
        use AttributePathSegment::{Item, Tag};

        if self.segments.len() != other.segments.len() {
            return false;
        }

        self.segments
            .iter()
            .zip(&other.segments)
            .all(|(a, b)| match (a, b) {
                (Tag(x), Tag(y)) => x == y,
                (Item(ItemSelector::Any), Item(_)) => true,
                (Item(ItemSelector::Index(i)), Item(ItemSelector::Index(j))) => i == j,
                _ => false,
            })
    }

    pub fn validate(&self) -> Result<(), AttributePathError> {
        if self.segments.is_empty() {
            return Err(AttributePathError::Empty);
        }

        if matches!(self.segments.first(), Some(AttributePathSegment::Item(_))) {
            return Err(AttributePathError::StartsWithItemSelector);
        }

        if self.segments.windows(2).any(|window| {
            matches!(
                window,
                [AttributePathSegment::Item(_), AttributePathSegment::Item(_)]
            )
        }) {
            return Err(AttributePathError::ConsecutiveItemSelectors);
        }

        Ok(())
    }
}

impl std::fmt::Display for AttributePath {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for (i, segment) in self.segments.iter().enumerate() {
            if i > 0 && !matches!(segment, AttributePathSegment::Item(_)) {
                write!(f, ".")?;
            }

            match segment {
                AttributePathSegment::Tag(tag) => write!(f, "{tag}")?,
                AttributePathSegment::Item(ItemSelector::Any) => write!(f, "[*]")?,
                AttributePathSegment::Item(ItemSelector::Index(index)) => write!(f, "[{index}]")?,
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use dicom_dictionary_std::tags;

    use super::{AttributePath, AttributePathError, AttributePathSegment, ItemSelector};

    #[test]
    fn build_simple_tag_path() {
        let path = AttributePath::from_tag(tags::PATIENT_ID);
        assert_eq!(
            path.segments(),
            &[AttributePathSegment::Tag(tags::PATIENT_ID)]
        );
    }

    #[test]
    fn matches_with_any_item_wildcard() {
        let wildcard = AttributePath::from_tag(tags::SCHEDULED_PROCEDURE_STEP_SEQUENCE)
            .push_any()
            .push_tag(tags::SCHEDULED_PROCEDURE_STEP_ID);
        let concrete = AttributePath::from_tag(tags::SCHEDULED_PROCEDURE_STEP_SEQUENCE)
            .push_item(0)
            .push_tag(tags::SCHEDULED_PROCEDURE_STEP_ID);

        assert!(wildcard.matches(&concrete));
        assert!(!concrete.matches(&wildcard));
    }

    #[test]
    fn display_attribute_path() {
        let path = AttributePath::from_tag(tags::SCHEDULED_PROCEDURE_STEP_SEQUENCE)
            .push_any()
            .push_tag(tags::SCHEDULED_PROCEDURE_STEP_ID)
            .push_item(0);

        assert_eq!(path.to_string(), "(0040,0100)[*].(0040,0009)[0]");
    }

    #[test]
    fn validate_rejects_invalid_shapes() {
        let empty = AttributePath {
            segments: Vec::new(),
        };
        assert_eq!(empty.validate().unwrap_err(), AttributePathError::Empty);

        let starts_with_item = AttributePath {
            segments: vec![AttributePathSegment::Item(ItemSelector::Any)],
        };
        assert_eq!(
            starts_with_item.validate().unwrap_err(),
            AttributePathError::StartsWithItemSelector
        );

        let consecutive_items = AttributePath {
            segments: vec![
                AttributePathSegment::Tag(tags::REQUEST_ATTRIBUTES_SEQUENCE),
                AttributePathSegment::Item(ItemSelector::Any),
                AttributePathSegment::Item(ItemSelector::Index(0)),
            ],
        };
        assert_eq!(
            consecutive_items.validate().unwrap_err(),
            AttributePathError::ConsecutiveItemSelectors
        );
    }
}
