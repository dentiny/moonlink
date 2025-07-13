/// This module contains tokio retry related util functions.
///
/// TODO(hjiang): Switch to [`backon`](https://github.com/Xuanwo/backon).
use iceberg::Error as IcebergError;
use tokio_retry2::RetryError as TokioRetryError;
use tracing::error;

/// Convert iceberg error to tokio retry error. Only `Unexpected` iceberg error translates to transient error.
pub(crate) fn iceberg_to_tokio_retry_error(err: IcebergError) -> TokioRetryError<IcebergError> {
    match err.kind() {
        iceberg::ErrorKind::Unexpected | iceberg::ErrorKind::CatalogCommitConflicts => {
            // Only logging on retriable error, non-retriable ones will be handled by error propagation, should avoid double handling.
            error!("Encounter retriable iceberg error: {err}");
            TokioRetryError::Transient {
                err,
                retry_after: None,
            }
        }
        _ => TokioRetryError::Permanent(err),
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_iceberg_and_tokio_retry_error_conversion() {
        // Test convert to transient tokio retry error.
        let permanent_error = IcebergError::new(iceberg::ErrorKind::DataInvalid, "Data invalid");
        let tokio_retry_error = iceberg_to_tokio_retry_error(permanent_error);
        match tokio_retry_error {
            TokioRetryError::Permanent(err) => {
                assert_eq!(err.kind(), iceberg::ErrorKind::DataInvalid);
            }
            _ => unreachable!("Converted tokio retry error should be permanent."),
        }

        // Test convert to permanent tokio retry error.
        let transient_error = IcebergError::new(iceberg::ErrorKind::Unexpected, "Unexpected error");
        let tokio_retry_error = iceberg_to_tokio_retry_error(transient_error);
        match tokio_retry_error {
            TokioRetryError::Transient {
                err,
                retry_after: _,
            } => {
                assert_eq!(err.kind(), iceberg::ErrorKind::Unexpected);
            }
            _ => unreachable!("Converted tokio retry error should be permanent."),
        }
    }
}
