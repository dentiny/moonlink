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
        /// Used for fake S3.
        endpoint: Option<String>,
    },
    #[cfg(feature = "storage-gcs")]
    Gcs {
        /// GCS project.
        project: String,
        /// GCS bucket region.
        region: String,
        /// GCS bucket.
        bucket: String,
        /// HMAC key and secret.
        access_key_id: String,
        secret_access_key: String,
        /// Used for fake GCS server.
        endpoint: Option<String>,
        /// Used for fake GCS server.
        disable_auth: bool,
    },
}
