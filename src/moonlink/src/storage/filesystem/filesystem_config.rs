/// FileSystemConfig contains configuration for multiple storage backends.
#[derive(Clone, Debug, PartialEq)]
pub enum FileSystemConfig {
    #[cfg(feature = "storage-fs")]
    FileSystem { root_directory: String },
    #[cfg(feature = "storage-s3")]
    S3 {
        access_key_id: String,
        secret_access_key: String,
        region: String,
        bucket: String,
        endpoint: String,
    },
    #[cfg(feature = "storage-gcs")]
    Gcs {
        /// GCS project.
        project: String,
        /// GCS bucket.
        bucket: String,
        /// If authentication required and credential unassigned, fallback to well-known location by default.
        cred_path: Option<String>,
        /// Used for fake GCS server.
        endpoint: Option<String>,
        /// Used for fake GCS server.
        disable_auth: bool,
    },
}
