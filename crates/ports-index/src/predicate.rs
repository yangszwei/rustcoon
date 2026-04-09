use crate::attribute_path::AttributePathError;
use crate::{AttributePath, ItemSelector};

#[derive(Debug, Clone)]
pub enum Predicate {
    All(Vec<Predicate>),
    Any(Vec<Predicate>),
    Not(Box<Predicate>),
    Attribute(AttributePath, MatchingRule),
}

#[derive(Debug, Clone)]
pub enum MatchingRule {
    SingleValue(String),
    UidList(Vec<String>),
    Universal,
    Wildcard(String),
    Range(RangeMatching),
    DateTimeRange(RangeMatching),
    Sequence(SequenceMatching),
    EmptyValue,
    MultipleValues(Vec<String>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RangeMatching {
    pub start: Option<String>,
    pub end: Option<String>,
}

impl RangeMatching {
    #[must_use]
    pub fn closed(start: impl Into<String>, end: impl Into<String>) -> Self {
        Self {
            start: Some(start.into()),
            end: Some(end.into()),
        }
    }

    #[must_use]
    pub fn from(start: impl Into<String>) -> Self {
        Self {
            start: Some(start.into()),
            end: None,
        }
    }

    #[must_use]
    pub fn until(end: impl Into<String>) -> Self {
        Self {
            start: None,
            end: Some(end.into()),
        }
    }
}

#[derive(Debug, Clone)]
pub struct SequenceMatching {
    pub item: ItemSelector,
    pub predicate: Box<Predicate>,
}

impl Predicate {
    pub fn validate(&self) -> Result<(), AttributePathError> {
        match self {
            Self::All(items) | Self::Any(items) => {
                for item in items {
                    item.validate()?;
                }
            }
            Self::Not(inner) => inner.validate()?,
            Self::Attribute(path, MatchingRule::Sequence(sequence)) => {
                path.validate()?;
                sequence.predicate.validate()?;
            }
            Self::Attribute(path, _) => path.validate()?,
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use dicom_dictionary_std::tags;

    use crate::{
        AttributePath, ItemSelector, MatchingRule, Predicate, RangeMatching, SequenceMatching,
    };

    #[test]
    fn range_helpers_build_expected_shapes() {
        assert_eq!(
            RangeMatching::closed("20260101", "20261231"),
            RangeMatching {
                start: Some("20260101".to_string()),
                end: Some("20261231".to_string())
            }
        );
        assert_eq!(
            RangeMatching::from("20260101"),
            RangeMatching {
                start: Some("20260101".to_string()),
                end: None
            }
        );
        assert_eq!(
            RangeMatching::until("20261231"),
            RangeMatching {
                start: None,
                end: Some("20261231".to_string())
            }
        );
    }

    #[test]
    fn complex_predicate_tree_can_be_built() {
        let predicate = Predicate::All(vec![
            Predicate::Attribute(
                AttributePath::from_tag(tags::PATIENT_ID),
                MatchingRule::SingleValue("PAT-001".to_string()),
            ),
            Predicate::Attribute(
                AttributePath::from_tag(tags::STUDY_DATE),
                MatchingRule::Range(RangeMatching::closed("20260101", "20261231")),
            ),
            Predicate::Attribute(
                AttributePath::from_tag(tags::REQUEST_ATTRIBUTES_SEQUENCE),
                MatchingRule::Sequence(SequenceMatching {
                    item: ItemSelector::Any,
                    predicate: Box::new(Predicate::Attribute(
                        AttributePath::from_tag(tags::SCHEDULED_PROCEDURE_STEP_ID),
                        MatchingRule::Wildcard("STEP*".to_string()),
                    )),
                }),
            ),
        ]);

        assert!(matches!(predicate, Predicate::All(ref items) if items.len() == 3));
        predicate.validate().expect("predicate should validate");
    }

    #[test]
    fn datetime_range_predicate_variant_validates_like_other_attribute_rules() {
        let predicate = Predicate::Attribute(
            AttributePath::from_tag(tags::ACQUISITION_DATE_TIME),
            MatchingRule::DateTimeRange(RangeMatching::closed(
                "20260411120000-0800",
                "20260412120000+0200",
            )),
        );

        predicate.validate().expect("predicate should validate");
    }
}
