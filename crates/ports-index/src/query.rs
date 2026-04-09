use crate::{AttributePath, Predicate};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StudyRootQueryRetrieveLevel {
    Study,
    Series,
    Image,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PatientRootQueryRetrieveLevel {
    Patient,
    Study,
    Series,
    Image,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryRetrieveScope {
    StudyRoot(StudyRootQueryRetrieveLevel),
    PatientRoot(PatientRootQueryRetrieveLevel),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SortDirection {
    Ascending,
    Descending,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SortKey {
    pub path: AttributePath,
    pub direction: SortDirection,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Paging {
    offset: u64,
    limit: u64,
}

impl Paging {
    pub fn new(offset: u64, limit: u64) -> Result<Self, crate::IndexError> {
        if limit == 0 {
            return Err(crate::IndexError::InvalidPageSize(limit));
        }

        Ok(Self { offset, limit })
    }

    pub fn offset(&self) -> u64 {
        self.offset
    }

    pub fn limit(&self) -> u64 {
        self.limit
    }
}

#[derive(Debug, Clone)]
pub struct CatalogQuery {
    scope: QueryRetrieveScope,
    predicate: Option<Predicate>,
    return_keys: Vec<AttributePath>,
    sort: Vec<SortKey>,
    paging: Option<Paging>,
}

impl CatalogQuery {
    pub fn new(
        scope: QueryRetrieveScope,
        return_keys: Vec<AttributePath>,
    ) -> Result<Self, crate::IndexError> {
        if return_keys.is_empty() {
            return Err(crate::IndexError::invalid_query(
                "query must include at least one return key",
            ));
        }

        for path in &return_keys {
            path.validate()
                .map_err(|err| crate::IndexError::invalid_query(err.to_string()))?;
        }

        Ok(Self {
            scope,
            predicate: None,
            return_keys,
            sort: Vec::new(),
            paging: None,
        })
    }

    pub fn with_predicate(mut self, predicate: Predicate) -> Result<Self, crate::IndexError> {
        predicate
            .validate()
            .map_err(|err| crate::IndexError::invalid_query(err.to_string()))?;
        self.predicate = Some(predicate);
        Ok(self)
    }

    pub fn with_sort(mut self, sort: Vec<SortKey>) -> Result<Self, crate::IndexError> {
        for key in &sort {
            key.path
                .validate()
                .map_err(|err| crate::IndexError::invalid_query(err.to_string()))?;
        }
        self.sort = sort;
        Ok(self)
    }

    pub fn with_paging(mut self, paging: Paging) -> Self {
        self.paging = Some(paging);
        self
    }

    pub fn scope(&self) -> QueryRetrieveScope {
        self.scope
    }

    pub fn predicate(&self) -> Option<&Predicate> {
        self.predicate.as_ref()
    }

    pub fn return_keys(&self) -> &[AttributePath] {
        &self.return_keys
    }

    pub fn sort(&self) -> &[SortKey] {
        &self.sort
    }

    pub fn paging(&self) -> Option<Paging> {
        self.paging
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PageSummary {
    pub offset: u64,
    pub limit: u64,
    pub returned: usize,
    pub total: Option<usize>,
}

#[derive(Debug, Clone)]
pub struct Page<T> {
    pub items: Vec<T>,
    pub summary: PageSummary,
}

impl<T> Page<T> {
    pub fn new(items: Vec<T>, paging: Option<Paging>, total: Option<usize>) -> Self {
        let paging = paging.unwrap_or(Paging {
            offset: 0,
            limit: items.len() as u64,
        });
        let returned = items.len();

        Self {
            items,
            summary: PageSummary {
                offset: paging.offset,
                limit: paging.limit,
                returned,
                total,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use dicom_dictionary_std::tags;

    use crate::{
        AttributePath, CatalogQuery, MatchingRule, Page, Paging, PatientRootQueryRetrieveLevel,
        Predicate, QueryRetrieveScope, SortDirection, SortKey, StudyRootQueryRetrieveLevel,
    };

    #[test]
    fn paging_new_sets_expected_values() {
        let paging = Paging::new(20, 10).unwrap();
        assert_eq!(paging.offset(), 20);
        assert_eq!(paging.limit(), 10);
    }

    #[test]
    fn page_summary_tracks_returned_count() {
        let page = Page::new(
            vec![1_u8, 2_u8],
            Some(Paging::new(20, 10).unwrap()),
            Some(42),
        );
        assert_eq!(page.summary.offset, 20);
        assert_eq!(page.summary.limit, 10);
        assert_eq!(page.summary.returned, 2);
        assert_eq!(page.summary.total, Some(42));
    }

    #[test]
    fn query_can_model_free_form_projection_and_predicates() {
        let query = CatalogQuery::new(
            QueryRetrieveScope::StudyRoot(StudyRootQueryRetrieveLevel::Series),
            vec![
                AttributePath::from_tag(tags::STUDY_INSTANCE_UID),
                AttributePath::from_tag(tags::SERIES_INSTANCE_UID),
                AttributePath::from_tag(tags::MODALITY),
            ],
        )
        .unwrap()
        .with_predicate(Predicate::Attribute(
            AttributePath::from_tag(tags::MODALITY),
            MatchingRule::SingleValue("CT".to_string()),
        ))
        .unwrap()
        .with_sort(vec![SortKey {
            path: AttributePath::from_tag(tags::SERIES_NUMBER),
            direction: SortDirection::Ascending,
        }])
        .unwrap()
        .with_paging(Paging::new(0, 50).unwrap());

        assert!(matches!(
            query.scope(),
            QueryRetrieveScope::StudyRoot(StudyRootQueryRetrieveLevel::Series)
        ));
        assert_eq!(query.return_keys().len(), 3);
        assert_eq!(query.sort().len(), 1);
        assert!(query.paging().is_some());
        assert!(matches!(
            query.predicate(),
            Some(Predicate::Attribute(_, MatchingRule::SingleValue(value))) if value == "CT"
        ));

        let patient_query = CatalogQuery::new(
            QueryRetrieveScope::PatientRoot(PatientRootQueryRetrieveLevel::Patient),
            vec![AttributePath::from_tag(tags::PATIENT_ID)],
        )
        .unwrap();

        assert!(matches!(
            patient_query.scope(),
            QueryRetrieveScope::PatientRoot(PatientRootQueryRetrieveLevel::Patient)
        ));
    }

    #[test]
    fn query_rejects_empty_projection_and_invalid_paging() {
        let err = CatalogQuery::new(
            QueryRetrieveScope::StudyRoot(StudyRootQueryRetrieveLevel::Study),
            Vec::new(),
        )
        .unwrap_err();
        assert_eq!(
            err.to_string(),
            "invalid query: query must include at least one return key"
        );

        let err = Paging::new(0, 0).unwrap_err();
        assert_eq!(err.to_string(), "invalid page size: 0");
    }
}
