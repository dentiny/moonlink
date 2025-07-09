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
        project: String,
        bucket: String,
        endpoint: Option<String>,
        disable_auth: bool,
        cred_path: Option<String>,
    },
}
