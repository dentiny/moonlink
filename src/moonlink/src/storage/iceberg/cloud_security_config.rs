/// Cloud vendor security config.
///
/// AWS security config.
use serde::{Deserialize, Serialize};

use crate::MoonlinkTableSecret;

#[derive(Clone, Deserialize, PartialEq, Serialize)]
pub struct AwsSecurityConfig {
    #[serde(rename = "access_key_id")]
    #[serde(default)]
    pub access_key_id: String,

    #[serde(rename = "security_access_key")]
    #[serde(default)]
    pub security_access_key: String,

    #[serde(rename = "region")]
    #[serde(default)]
    pub region: String,
}

impl std::fmt::Debug for AwsSecurityConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AwsSecurityConfig")
            .field("access_key_id", &"xxxxx")
            .field("security_access_key", &"xxxx")
            .field("region", &self.region)
            .finish()
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub enum CloudSecurityConfig {
    #[cfg(feature = "storage-s3")]
    Aws(AwsSecurityConfig),
}

impl CloudSecurityConfig {
    /// Extract security metadata entry from the current cloud security config.
    pub fn extract_security_metadata_entry(&self) -> Option<MoonlinkTableSecret> {
        match self {
            #[cfg(feature = "storage-s3")]
            CloudSecurityConfig::Aws(aws_security_config) => {
                Some(MoonlinkTableSecret {
                    secret_type: crate::MoonlinkSecretType::S3,
                    key_id: aws_security_config.access_key_id.clone(),
                    secret: aws_security_config.security_access_key.clone(),
                    project: None,
                    endpoint: None,
                    region: None,
                })
            }
            _ => None
        }
    }
}
