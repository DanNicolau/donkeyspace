use hmac::{Hmac, Mac};
use http::header::ACCEPT;
use octocrab::{Octocrab, models::IssueState};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::Sha256;
use std::collections::BTreeMap;
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
    #[error("invalid github response: {0}")]
    InvalidResponse(String),
}

#[derive(Debug, Clone)]
pub struct GitHubClient {
    client: Octocrab,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct GitHubWorkItem {
    pub id: String,
    pub spec: String,
    pub body: String,
    #[serde(default)]
    pub depends_on: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GitHubProjectedIssue {
    pub id: i64,
    pub number: i64,
}

impl GitHubClient {
    pub fn new(token: impl Into<String>) -> Result<Self, GitHubClientError> {
        Ok(Self {
            client: Octocrab::builder()
                .personal_token(token.into())
                .add_header(ACCEPT, "application/vnd.github+json".into())
                .build()?,
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
    ) -> Result<String, GitHubClientError> {
        let comment = self
            .client
            .issues(owner, repo)
            .create_comment(issue_number as u64, body)
            .await?;
        Ok(comment.id.to_string())
    }

    pub async fn authenticated_login(&self) -> Result<String, GitHubClientError> {
        Ok(self.client.current().user().await?.login)
    }

    pub async fn collaborator_permission(
        &self,
        owner: &str,
        repo: &str,
        username: &str,
    ) -> Result<String, GitHubClientError> {
        let permission = self
            .client
            .repos(owner, repo)
            .get_contributor_permission(username)
            .send()
            .await?;
        Ok(permission.role_name)
    }

    pub async fn organization_member(
        &self,
        organization: &str,
        username: &str,
    ) -> Result<bool, GitHubClientError> {
        Ok(self
            .client
            .orgs(organization)
            .check_membership(username)
            .await?)
    }

    pub async fn team_member(
        &self,
        organization: &str,
        team_slug: &str,
        username: &str,
    ) -> Result<bool, GitHubClientError> {
        let membership: Value = self
            .client
            .get(
                format!("/orgs/{organization}/teams/{team_slug}/memberships/{username}"),
                None::<&()>,
            )
            .await?;
        Ok(membership.get("state").and_then(Value::as_str) == Some("active"))
    }

    pub async fn project_work_items(
        &self,
        owner: &str,
        repo: &str,
        parent_issue_number: i64,
        work_items: &[GitHubWorkItem],
    ) -> Result<BTreeMap<String, GitHubProjectedIssue>, GitHubClientError> {
        let mut projected = BTreeMap::<String, (i64, i64)>::new();
        for item in work_items {
            let issue: Value = self
                .client
                .post(
                    format!("/repos/{owner}/{repo}/issues"),
                    Some(&serde_json::json!({
                        "title": format!("[block] {}", item.id),
                        "body": format!(
                            "<!-- donkeyspace-work-item -->\n\nParent lifecycle issue: #{parent_issue_number}\n\nSpecification path: `{}`\n\n{}",
                            item.spec, item.body
                        ),
                    })),
                )
                .await?;
            let issue_id = issue["id"].as_i64().ok_or_else(|| {
                GitHubClientError::InvalidResponse("created work-item issue has no id".into())
            })?;
            let issue_number = issue["number"].as_i64().ok_or_else(|| {
                GitHubClientError::InvalidResponse("created work-item issue has no number".into())
            })?;
            self.client
                .post::<_, Value>(
                    format!("/repos/{owner}/{repo}/issues/{parent_issue_number}/sub_issues"),
                    Some(&serde_json::json!({"sub_issue_id": issue_id})),
                )
                .await?;
            projected.insert(item.id.clone(), (issue_id, issue_number));
        }

        for item in work_items {
            let (_, issue_number) = projected[&item.id];
            for dependency in &item.depends_on {
                let (blocking_issue_id, _) = projected[dependency];
                self.client
                    .post::<_, Value>(
                        format!(
                            "/repos/{owner}/{repo}/issues/{issue_number}/dependencies/blocked_by"
                        ),
                        Some(&serde_json::json!({"issue_id": blocking_issue_id})),
                    )
                    .await?;
            }
        }
        Ok(projected
            .into_iter()
            .map(|(key, (id, number))| (key, GitHubProjectedIssue { id, number }))
            .collect())
    }

    pub async fn close_issue(
        &self,
        owner: &str,
        repo: &str,
        issue_number: i64,
    ) -> Result<(), GitHubClientError> {
        self.client
            .issues(owner, repo)
            .update(issue_number as u64)
            .state(IssueState::Closed)
            .send()
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

    pub async fn repository(&self, owner: &str, repo: &str) -> Result<Value, GitHubClientError> {
        Ok(self
            .client
            .get(format!("/repos/{owner}/{repo}"), None::<&()>)
            .await?)
    }

    pub async fn repository_events(
        &self,
        owner: &str,
        repo: &str,
        max_pages: usize,
    ) -> Result<Vec<Value>, GitHubClientError> {
        let route = format!("/repos/{owner}/{repo}/events?per_page=100");
        let mut page: octocrab::Page<Value> = self.client.get(route, None::<&()>).await?;
        let mut events = page.take_items();

        for _ in 1..max_pages.max(1) {
            let Some(mut next_page) = self.client.get_page(&page.next).await? else {
                break;
            };
            events.append(&mut next_page.take_items());
            page = next_page;
        }

        Ok(events)
    }

    pub async fn pull_request(
        &self,
        owner: &str,
        repo: &str,
        pull_request_number: u64,
    ) -> Result<Value, GitHubClientError> {
        Ok(self
            .client
            .get(
                format!("/repos/{owner}/{repo}/pulls/{pull_request_number}"),
                None::<&()>,
            )
            .await?)
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
