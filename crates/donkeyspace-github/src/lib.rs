use hmac::{Hmac, Mac};
use octocrab::{Octocrab, models::IssueState};
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

#[derive(Debug, Error)]
pub enum GitHubClientError {
    #[error("github client error: {0}")]
    Octocrab(#[from] octocrab::Error),
}

#[derive(Debug, Clone)]
pub struct GitHubClient {
    client: Octocrab,
}

impl GitHubClient {
    pub fn new(token: impl Into<String>) -> Result<Self, GitHubClientError> {
        Ok(Self {
            client: Octocrab::builder().personal_token(token.into()).build()?,
        })
    }

    pub async fn add_issue_label(
        &self,
        owner: &str,
        repo: &str,
        issue_number: i64,
        label: &str,
    ) -> Result<(), GitHubClientError> {
        self.ensure_label(owner, repo, label).await?;
        self.client
            .issues(owner, repo)
            .add_labels(issue_number as u64, &[label.to_string()])
            .await?;
        Ok(())
    }

    pub async fn ensure_labels(
        &self,
        owner: &str,
        repo: &str,
        labels: &[String],
    ) -> Result<(), GitHubClientError> {
        for label in labels {
            self.ensure_label(owner, repo, label).await?;
        }
        Ok(())
    }

    pub async fn remove_issue_label(
        &self,
        owner: &str,
        repo: &str,
        issue_number: i64,
        label: &str,
    ) -> Result<(), GitHubClientError> {
        if let Err(error) = self
            .client
            .issues(owner, repo)
            .remove_label(issue_number as u64, label)
            .await
        {
            if github_error_status(&error) == Some(404) {
                return Ok(());
            }
            return Err(error.into());
        }
        Ok(())
    }

    pub async fn create_issue_comment(
        &self,
        owner: &str,
        repo: &str,
        issue_number: i64,
        body: &str,
    ) -> Result<(), GitHubClientError> {
        self.client
            .issues(owner, repo)
            .create_comment(issue_number as u64, body)
            .await?;
        Ok(())
    }

    pub async fn issue_is_closed(
        &self,
        owner: &str,
        repo: &str,
        issue_number: i64,
    ) -> Result<bool, GitHubClientError> {
        let issue = self
            .client
            .issues(owner, repo)
            .get(issue_number as u64)
            .await?;
        Ok(issue.state == IssueState::Closed)
    }

    pub async fn create_pull_request(
        &self,
        owner: &str,
        repo: &str,
        title: &str,
        head: &str,
        base: &str,
        body: &str,
    ) -> Result<String, GitHubClientError> {
        let pull_request = self
            .client
            .pulls(owner, repo)
            .create(title, head, base)
            .body(body.to_string())
            .send()
            .await?;

        Ok(pull_request.html_url.to_string())
    }

    async fn ensure_label(
        &self,
        owner: &str,
        repo: &str,
        label: &str,
    ) -> Result<(), GitHubClientError> {
        match self.client.issues(owner, repo).get_label(label).await {
            Ok(_) => return Ok(()),
            Err(error) if github_error_status(&error) == Some(404) => {}
            Err(error) => return Err(error.into()),
        }

        if let Err(error) = self
            .client
            .issues(owner, repo)
            .create_label(label, workflow_label_color(label), "Managed by donkeyspace")
            .await
        {
            if github_error_status(&error) == Some(422) {
                return Ok(());
            }
            return Err(error.into());
        }
        Ok(())
    }
}

fn github_error_status(error: &octocrab::Error) -> Option<u16> {
    match error {
        octocrab::Error::GitHub { source, .. } => Some(source.status_code.as_u16()),
        _ => None,
    }
}

fn workflow_label_color(label: &str) -> &'static str {
    match label {
        "ai:needs-info" => "d4a72c",
        "ai:ready" => "2da44e",
        "ai:in-progress" => "0969da",
        "ai:pr-open" => "8250df",
        "ai:needs-human" => "bf8700",
        "ai:blocked" => "cf222e",
        _ => "6e7781",
    }
}

#[cfg(test)]
mod tests {
    use super::verify_signature;

    #[test]
    fn rejects_missing_signature() {
        assert!(verify_signature("secret", b"{}", None).is_err());
    }
}
