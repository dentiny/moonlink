/// Secret entry for object object storage access.
/// WARNING: Not expected to log anywhere!

#[derive(Clone, Debug, PartialEq)]
pub enum SecretType {
    // Used to suppress compilation warning.
    Unknown,
    #[cfg(feature = "storage-gcs")]
    Gcs,
    #[cfg(feature = "storage-s3")]
    S3,
}

#[derive(Clone, PartialEq)]
pub struct SecretEntry {
    pub secret_type: SecretType,
    pub key_id: String,
    pub secret: String,
    pub endpoint: Option<String>,
    pub region: Option<String>,
}

impl std::fmt::Debug for SecretEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SecretEntry")
            .field("secret_type", &self.secret_type)
            .field("key_id", &"<key>")
            .field("secret", &"<secret>")
            .field("endpoint", &self.endpoint)
            .field("region", &self.region)
            .finish()
    }
}

impl SecretEntry {
    /// Get secret type in string format.
    pub fn get_secret_type(&self) -> String {
        match &self.secret_type {
            SecretType::Unknown => "unknown".to_string(),
            #[cfg(feature = "storage-gcs")]
            SecretType::Gcs => "gcs".to_string(),
            #[cfg(feature = "storage-s3")]
            SecretType::S3 => "s3".to_string(),
        }
    }

    /// Convert secret type from string format.
    pub fn convert_secret_type(secret_type: &str) -> SecretType {
        #[cfg(feature = "storage-gcs")]
        {
            if secret_type == "gcs" {
                return SecretType::Gcs;
            }
        }
        #[cfg(feature = "storage-s3")]
        {
            if secret_type == "s3" {
                return SecretType::S3;
            }
        }
        // Used to suppress compilation warning.
        assert_eq!(secret_type, "unknown");
        SecretType::Unknown
    }
}
