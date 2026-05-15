use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use thiserror::Error;

type HmacSha256 = Hmac<Sha256>;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum SignatureError {
    #[error("missing x-hub-signature-256 header")]
    MissingHeader,
    #[error("signature must start with sha256=")]
    InvalidPrefix,
    #[error("signature hex is invalid")]
    InvalidHex,
    #[error("signature does not match payload")]
    Mismatch,
}

pub fn verify_signature(
    webhook_secret: &str,
    payload: &[u8],
    signature_header: Option<&str>,
) -> Result<(), SignatureError> {
    let signature_header = signature_header.ok_or(SignatureError::MissingHeader)?;
    let signature_hex = signature_header
        .strip_prefix("sha256=")
        .ok_or(SignatureError::InvalidPrefix)?;
    let expected = hex::decode(signature_hex).map_err(|_| SignatureError::InvalidHex)?;

    let mut mac = HmacSha256::new_from_slice(webhook_secret.as_bytes())
        .expect("HMAC accepts keys of any size");
    mac.update(payload);
    mac.verify_slice(&expected)
        .map_err(|_| SignatureError::Mismatch)
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct WebhookEnvelope {
    pub delivery_id: String,
    pub event_name: String,
}

#[cfg(test)]
mod tests {
    use super::verify_signature;

    #[test]
    fn rejects_missing_signature() {
        assert!(verify_signature("secret", b"{}", None).is_err());
    }
}
