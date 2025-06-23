use url::Url;

/// Util function to decide whether the given filepath is a local filepath.
/// It's worth noting only absolute path is acceptable.
/// No IO operation is involved.
#[allow(dead_code)]
pub(crate) fn is_local_filepath(filepath: &str) -> bool {
    Url::from_file_path(filepath).is_ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_local_filepath() {
        // Absolute local path.
        assert!(is_local_filepath("/tmp/non_existent_folder/random_file"));
        // S3 object path.
        assert!(!is_local_filepath("s3://bucket/object"));
    }
}
