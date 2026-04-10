use rustcoon_index::{IndexError, IndexOperation};

pub(crate) fn map_sqlx(operation: IndexOperation, source: sqlx::Error) -> IndexError {
    match &source {
        sqlx::Error::PoolTimedOut
        | sqlx::Error::PoolClosed
        | sqlx::Error::Io(_)
        | sqlx::Error::Tls(_) => IndexError::unavailable(true, source),
        _ => IndexError::backend("postgres", operation, source),
    }
}

#[cfg(test)]
mod tests {
    use rustcoon_index::{IndexError, IndexOperation};

    use super::map_sqlx;

    #[test]
    fn maps_pool_timeout_as_unavailable() {
        let error = map_sqlx(IndexOperation::Query, sqlx::Error::PoolTimedOut);
        assert!(matches!(
            error,
            IndexError::Unavailable {
                transient: true,
                source: Some(_)
            }
        ));
    }

    #[test]
    fn maps_other_errors_as_backend_failures() {
        let error = map_sqlx(
            IndexOperation::GetInstance,
            sqlx::Error::Protocol("boom".to_string()),
        );
        assert!(matches!(
            error,
            IndexError::Backend {
                backend: "postgres",
                operation: IndexOperation::GetInstance,
                ..
            }
        ));
    }
}
