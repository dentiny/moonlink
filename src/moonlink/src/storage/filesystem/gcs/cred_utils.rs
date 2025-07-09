/// This module contains GCS credential related util functions.
use base64::engine::general_purpose::STANDARD as base64;
use base64::Engine;

use crate::Result;

/// Default well-known location for credential.
const WELL_KNOWN_LOCATION: &str = ".config/gcloud/application_default_credentials.json";

/// Get GCS credential path (priority goes from high to low):
/// 1. The provided path
/// 2. The well-known path: `$HOME/.config/gcloud/application_default_credentials.json`
pub(crate) fn get_credential_path(cred_path: &Option<String>) -> String {
    if let Some(path) = cred_path {
        return path.clone();
    }

    let home = std::env::var("HOME").unwrap();
    let default_path = std::path::Path::new(&home).join(WELL_KNOWN_LOCATION);
    default_path.as_path().to_str().unwrap().to_string()
}

/// Load GCS credential JSON object from (priority goes from high to low):
/// 1. The provided path
/// 2. The well-known path: `$HOME/.config/gcloud/application_default_credentials.json`
///
/// TODO(hjiang):
/// 1. Consider using environment variable to get credential path.
/// 2. Considering using async credential loading.
pub(crate) fn load_gcs_credentials(cred_path: &Option<String>) -> Result<String> {
    // 1. Load from provided path.
    if let Some(path) = cred_path {
        let content = std::fs::read_to_string(path)?;
        let encoded = base64.encode(content.as_bytes());
        return Ok(encoded);
    }

    // 2. Fallback to well-known location.
    let home = std::env::var("HOME").unwrap();
    let default_path = std::path::Path::new(&home).join(WELL_KNOWN_LOCATION);
    let content = std::fs::read_to_string(&default_path)?;
    Ok(base64.encode(content.as_bytes()))
}
