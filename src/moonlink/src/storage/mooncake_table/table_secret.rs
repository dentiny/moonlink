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
    pub project: String,
    pub region: String,
    pub bucket: String,
    pub access_key_id: String,
    pub secret_access_key: String,
}
