/// Secret entry for object object storage access.
/// WARNING: Not expected to log anywhere!

#[derive(Clone)]
pub enum SecretType {
    #[cfg(feature = "storage-gcs")]
    Gcs,
    #[cfg(feature = "storage-s3")]
    S3,
}

#[derive(Clone)]
pub struct SecretEntry {
    pub secret_type: SecretType,
    pub key_id: String,
    pub secret: String,
    pub endpoint: Option<String>,
    pub region: Option<String>,
}
