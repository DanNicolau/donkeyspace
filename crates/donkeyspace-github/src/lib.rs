use hmac::{Hmac, Mac};
use http::{
    StatusCode,
    header::{ACCEPT, ETAG, HeaderMap, HeaderValue, IF_NONE_MATCH},
};
use octocrab::{FromResponse, Octocrab, Page, models::IssueState};
use secrecy::ExposeSecret;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::Sha256;
use std::{
    collections::BTreeMap,
    env, fs,
    path::PathBuf,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};
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
    #[error("github rate limit requires waiting {retry_after_seconds} seconds")]
    RateLimited { retry_after_seconds: u64 },
    #[error("github authentication configuration is invalid: {0}")]
    InvalidAuthConfig(String),
    #[error("failed to read github private key `{path}`: {source}")]
    PrivateKeyRead {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("github app private key is invalid: {0}")]
    InvalidPrivateKey(#[from] jsonwebtoken::errors::Error),
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum GitHubAuthMode {
    App,
    Pat,
}

#[derive(Clone)]
pub enum GitHubAuthConfig {
    App {
        app_id: u64,
        installation_id: u64,
        private_key_file: PathBuf,
    },
    Pat {
        token: String,
    },
}

impl std::fmt::Debug for GitHubAuthConfig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::App {
                app_id,
                installation_id,
                private_key_file,
            } => formatter
                .debug_struct("App")
                .field("app_id", app_id)
                .field("installation_id", installation_id)
                .field("private_key_file", private_key_file)
                .finish(),
            Self::Pat { .. } => formatter
                .debug_struct("Pat")
                .field("token", &"[REDACTED]")
                .finish(),
        }
    }
}

impl GitHubAuthConfig {
    pub fn from_env() -> Result<Option<Self>, GitHubClientError> {
        Self::from_lookup(|name| env::var(name).ok())
    }

    fn from_lookup(
        lookup: impl Fn(&str) -> Option<String>,
    ) -> Result<Option<Self>, GitHubClientError> {
        let get = |name| {
            lookup(name)
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty())
        };
        let mode = get("DONKEYSPACE_GITHUB_AUTH_MODE");
        let app_id = get("DONKEYSPACE_GITHUB_APP_ID");
        let installation_id = get("DONKEYSPACE_GITHUB_INSTALLATION_ID");
        let private_key_file = get("DONKEYSPACE_GITHUB_PRIVATE_KEY_FILE");
        let pat = get("DONKEYSPACE_GITHUB_TOKEN");
        let any_app = app_id.is_some() || installation_id.is_some() || private_key_file.is_some();

        let selected = match mode.as_deref() {
            Some("app") => GitHubAuthMode::App,
            Some("pat") => GitHubAuthMode::Pat,
            Some(other) => {
                return Err(GitHubClientError::InvalidAuthConfig(format!(
                    "DONKEYSPACE_GITHUB_AUTH_MODE must be `app` or `pat`, got `{other}`"
                )));
            }
            None if any_app => GitHubAuthMode::App,
            None if pat.is_some() => GitHubAuthMode::Pat,
            None => return Ok(None),
        };

        if any_app && pat.is_some() {
            return Err(GitHubClientError::InvalidAuthConfig(
                "App and PAT credentials cannot be configured together".into(),
            ));
        }

        match selected {
            GitHubAuthMode::App => {
                let missing = [
                    ("DONKEYSPACE_GITHUB_APP_ID", app_id.is_none()),
                    (
                        "DONKEYSPACE_GITHUB_INSTALLATION_ID",
                        installation_id.is_none(),
                    ),
                    (
                        "DONKEYSPACE_GITHUB_PRIVATE_KEY_FILE",
                        private_key_file.is_none(),
                    ),
                ]
                .into_iter()
                .filter_map(|(name, missing)| missing.then_some(name))
                .collect::<Vec<_>>();
                if !missing.is_empty() {
                    return Err(GitHubClientError::InvalidAuthConfig(format!(
                        "partial App configuration; missing {}",
                        missing.join(", ")
                    )));
                }
                Ok(Some(Self::App {
                    app_id: parse_id("DONKEYSPACE_GITHUB_APP_ID", app_id.unwrap())?,
                    installation_id: parse_id(
                        "DONKEYSPACE_GITHUB_INSTALLATION_ID",
                        installation_id.unwrap(),
                    )?,
                    private_key_file: PathBuf::from(private_key_file.unwrap()),
                }))
            }
            GitHubAuthMode::Pat => Ok(Some(Self::Pat {
                token: pat.ok_or_else(|| {
                    GitHubClientError::InvalidAuthConfig(
                        "PAT mode requires DONKEYSPACE_GITHUB_TOKEN".into(),
                    )
                })?,
            })),
        }
    }

    pub fn mode(&self) -> GitHubAuthMode {
        match self {
            Self::App { .. } => GitHubAuthMode::App,
            Self::Pat { .. } => GitHubAuthMode::Pat,
        }
    }

    pub fn installation_id(&self) -> Option<u64> {
        match self {
            Self::App {
                installation_id, ..
            } => Some(*installation_id),
            Self::Pat { .. } => None,
        }
    }

    pub fn app_id(&self) -> Option<u64> {
        match self {
            Self::App { app_id, .. } => Some(*app_id),
            Self::Pat { .. } => None,
        }
    }
}

impl GitHubClientError {
    pub fn retry_after_seconds(&self) -> Option<u64> {
        match self {
            Self::RateLimited {
                retry_after_seconds,
            } => Some(*retry_after_seconds),
            Self::Octocrab(error) if matches!(github_error_status(error), Some(403 | 429)) => {
                Some(60)
            }
            _ => None,
        }
    }
}

#[derive(Clone)]
pub struct GitHubCredentialProvider {
    config: GitHubAuthConfig,
    client: Arc<Octocrab>,
    app_client: Option<Arc<Octocrab>>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct GitHubRepository {
    pub owner: String,
    pub name: String,
    pub full_name: String,
    pub private: bool,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct GitHubAppWebhookDelivery {
    pub id: Option<u64>,
    pub event: Option<String>,
    pub action: Option<String>,
    pub status: Option<String>,
    pub status_code: Option<u16>,
    pub delivered_at: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct GitHubAppWebhookStatus {
    pub url: Option<String>,
    pub content_type: Option<String>,
    pub subscribed_events: Vec<String>,
    pub deliveries: Vec<GitHubAppWebhookDelivery>,
}

impl std::fmt::Debug for GitHubCredentialProvider {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("GitHubCredentialProvider")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl GitHubCredentialProvider {
    pub fn new(config: GitHubAuthConfig) -> Result<Self, GitHubClientError> {
        Self::new_with_base_uri(config, None)
    }

    pub fn app_id(&self) -> Option<u64> {
        self.config.app_id()
    }

    fn new_with_base_uri(
        config: GitHubAuthConfig,
        base_uri: Option<&str>,
    ) -> Result<Self, GitHubClientError> {
        let (client, app_client) = match &config {
            GitHubAuthConfig::App {
                app_id,
                installation_id,
                private_key_file,
            } => {
                let pem = fs::read(private_key_file).map_err(|source| {
                    GitHubClientError::PrivateKeyRead {
                        path: private_key_file.clone(),
                        source,
                    }
                })?;
                let key = jsonwebtoken::EncodingKey::from_rsa_pem(&pem)?;
                let builder = Octocrab::builder()
                    .app((*app_id).into(), key)
                    .add_header(ACCEPT, "application/vnd.github+json".into());
                let builder = match base_uri {
                    Some(base_uri) => builder.base_uri(base_uri)?,
                    None => builder,
                };
                let app_client = builder.build()?;
                let client = app_client.installation((*installation_id).into())?;
                (client, Some(Arc::new(app_client)))
            }
            GitHubAuthConfig::Pat { token } => {
                eprintln!(
                    "warning: PAT authentication is deprecated; configure a GitHub App installation"
                );
                let builder = Octocrab::builder()
                    .personal_token(token.clone())
                    .add_header(ACCEPT, "application/vnd.github+json".into());
                let builder = match base_uri {
                    Some(base_uri) => builder.base_uri(base_uri)?,
                    None => builder,
                };
                (builder.build()?, None)
            }
        };
        Ok(Self {
            config,
            client: Arc::new(client),
            app_client,
        })
    }

    pub fn from_env() -> Result<Option<Self>, GitHubClientError> {
        GitHubAuthConfig::from_env()?.map(Self::new).transpose()
    }

    pub fn mode(&self) -> GitHubAuthMode {
        self.config.mode()
    }

    pub fn installation_id(&self) -> Option<u64> {
        self.config.installation_id()
    }

    pub async fn token(&self) -> Result<String, GitHubClientError> {
        match &self.config {
            GitHubAuthConfig::App { .. } => Ok(self
                .client
                .installation_token()
                .await?
                .expose_secret()
                .to_string()),
            GitHubAuthConfig::Pat { token } => Ok(token.clone()),
        }
    }

    pub fn client(&self) -> GitHubClient {
        GitHubClient {
            client: (*self.client).clone(),
        }
    }

    pub async fn validate_installation(&self) -> Result<(), GitHubClientError> {
        let GitHubAuthConfig::App {
            installation_id, ..
        } = &self.config
        else {
            return Ok(());
        };
        let app_client = self.app_client.as_ref().ok_or_else(|| {
            GitHubClientError::InvalidResponse("App client is unavailable".into())
        })?;
        let installation: Value = app_client
            .get(format!("/app/installations/{installation_id}"), None::<&()>)
            .await?;
        validate_installation_response(&installation)
    }

    pub async fn validate_members_permission(&self) -> Result<(), GitHubClientError> {
        if self.mode() == GitHubAuthMode::Pat {
            return Ok(());
        }
        let installation_id = self
            .installation_id()
            .expect("App mode has installation id");
        let app_client = self.app_client.as_ref().ok_or_else(|| {
            GitHubClientError::InvalidAuthConfig("App client is unavailable".into())
        })?;
        let installation: Value = app_client
            .get(format!("/app/installations/{installation_id}"), None::<&()>)
            .await?;
        validate_members_permission_response(&installation)
    }

    pub async fn repositories(&self) -> Result<Vec<GitHubRepository>, GitHubClientError> {
        let mut repositories = Vec::new();
        for page in 1.. {
            let query = serde_json::json!({"per_page": 100, "page": page});
            let response: Value = match self.mode() {
                GitHubAuthMode::App => {
                    self.client
                        .get("/installation/repositories", Some(&query))
                        .await?
                }
                GitHubAuthMode::Pat => self.client.get("/user/repos", Some(&query)).await?,
            };
            let values = match self.mode() {
                GitHubAuthMode::App => response["repositories"].as_array(),
                GitHubAuthMode::Pat => response.as_array(),
            }
            .ok_or_else(|| {
                GitHubClientError::InvalidResponse(
                    "repository listing did not contain an array".into(),
                )
            })?;
            for repository in values {
                repositories.push(parse_repository(repository)?);
            }
            if values.len() < 100 {
                break;
            }
        }
        repositories.sort_by(|left, right| left.full_name.cmp(&right.full_name));
        repositories.dedup_by(|left, right| left.full_name == right.full_name);
        Ok(repositories)
    }

    pub async fn app_webhook_status(
        &self,
    ) -> Result<Option<GitHubAppWebhookStatus>, GitHubClientError> {
        let Some(app_client) = &self.app_client else {
            return Ok(None);
        };
        let app: Value = app_client.get("/app", None::<&()>).await?;
        let hook: Value = app_client.get("/app/hook/config", None::<&()>).await?;
        let deliveries: Value = app_client
            .get(
                "/app/hook/deliveries",
                Some(&serde_json::json!({"per_page": 10})),
            )
            .await?;

        parse_app_webhook_status(&app, &hook, &deliveries).map(Some)
    }
}

fn parse_app_webhook_status(
    app: &Value,
    hook: &Value,
    deliveries: &Value,
) -> Result<GitHubAppWebhookStatus, GitHubClientError> {
    let mut subscribed_events = app["events"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .map(str::to_string)
        .collect::<Vec<_>>();
    subscribed_events.sort_unstable();
    subscribed_events.dedup();
    let deliveries = deliveries
        .as_array()
        .ok_or_else(|| {
            GitHubClientError::InvalidResponse(
                "GitHub App webhook deliveries response is not an array".into(),
            )
        })?
        .iter()
        .map(|delivery| GitHubAppWebhookDelivery {
            id: delivery["id"].as_u64(),
            event: delivery["event"].as_str().map(str::to_string),
            action: delivery["action"].as_str().map(str::to_string),
            status: delivery["status"].as_str().map(str::to_string),
            status_code: delivery["status_code"]
                .as_u64()
                .and_then(|status| u16::try_from(status).ok()),
            delivered_at: delivery["delivered_at"].as_str().map(str::to_string),
        })
        .collect();
    Ok(GitHubAppWebhookStatus {
        url: hook["url"].as_str().map(str::to_string),
        content_type: hook["content_type"].as_str().map(str::to_string),
        subscribed_events,
        deliveries,
    })
}

fn parse_repository(value: &Value) -> Result<GitHubRepository, GitHubClientError> {
    let owner = value["owner"]["login"]
        .as_str()
        .filter(|owner| !owner.is_empty())
        .ok_or_else(|| GitHubClientError::InvalidResponse("repository omitted owner".into()))?;
    let name = value["name"]
        .as_str()
        .filter(|name| !name.is_empty())
        .ok_or_else(|| GitHubClientError::InvalidResponse("repository omitted name".into()))?;
    let full_name = value["full_name"]
        .as_str()
        .filter(|full_name| !full_name.is_empty())
        .map(str::to_string)
        .unwrap_or_else(|| format!("{owner}/{name}"));
    Ok(GitHubRepository {
        owner: owner.to_string(),
        name: name.to_string(),
        full_name,
        private: value["private"].as_bool().unwrap_or(false),
    })
}

fn required_string(
    value: &Value,
    field: &str,
    resource: &str,
) -> Result<String, GitHubClientError> {
    value[field]
        .as_str()
        .filter(|value| !value.is_empty())
        .map(str::to_string)
        .ok_or_else(|| GitHubClientError::InvalidResponse(format!("{resource} omitted `{field}`")))
}

pub async fn discover_installation_id(
    app_id: u64,
    private_key_file: PathBuf,
    repository_owner: &str,
) -> Result<u64, GitHubClientError> {
    let pem = fs::read(&private_key_file).map_err(|source| GitHubClientError::PrivateKeyRead {
        path: private_key_file,
        source,
    })?;
    let key = jsonwebtoken::EncodingKey::from_rsa_pem(&pem)?;
    let app_client = Octocrab::builder()
        .app(app_id.into(), key)
        .add_header(ACCEPT, "application/vnd.github+json".into())
        .build()?;
    let installations: Value = app_client
        .get(
            "/app/installations",
            Some(&serde_json::json!({"per_page": 100})),
        )
        .await?;
    select_installation_id(&installations, repository_owner)
}

fn select_installation_id(
    installations: &Value,
    repository_owner: &str,
) -> Result<u64, GitHubClientError> {
    let installations = installations.as_array().ok_or_else(|| {
        GitHubClientError::InvalidResponse("installation listing is not an array".into())
    })?;
    let matching = installations
        .iter()
        .filter(|installation| {
            installation["account"]["login"]
                .as_str()
                .is_some_and(|login| login.eq_ignore_ascii_case(repository_owner))
        })
        .collect::<Vec<_>>();
    let [installation] = matching.as_slice() else {
        return Err(GitHubClientError::InvalidResponse(match matching.len() {
            0 => {
                format!("GitHub App has no installation for repository owner `{repository_owner}`")
            }
            count => format!(
                "GitHub App has {count} installations for repository owner `{repository_owner}`"
            ),
        }));
    };
    validate_installation_response(installation)?;
    installation["id"]
        .as_u64()
        .filter(|id| *id > 0)
        .ok_or_else(|| {
            GitHubClientError::InvalidResponse("matching installation has no valid id".into())
        })
}

fn validate_installation_response(installation: &Value) -> Result<(), GitHubClientError> {
    if !installation["suspended_at"].is_null() {
        return Err(GitHubClientError::InvalidResponse(
            "configured installation is suspended".into(),
        ));
    }
    for permission in ["contents", "issues", "pull_requests"] {
        if installation["permissions"][permission].as_str() != Some("write") {
            return Err(GitHubClientError::InvalidResponse(format!(
                "installation is missing `{permission}: write` permission"
            )));
        }
    }
    Ok(())
}

fn validate_members_permission_response(installation: &Value) -> Result<(), GitHubClientError> {
    if matches!(
        installation["permissions"]["members"].as_str(),
        Some("read" | "write")
    ) {
        Ok(())
    } else {
        Err(GitHubClientError::InvalidResponse(
            "installation is missing `members: read` organization permission".into(),
        ))
    }
}

fn parse_id(name: &str, value: String) -> Result<u64, GitHubClientError> {
    value.parse().ok().filter(|id| *id > 0).ok_or_else(|| {
        GitHubClientError::InvalidAuthConfig(format!("{name} must be a positive integer"))
    })
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

#[derive(Debug, Clone, PartialEq)]
pub struct GitHubEventPoll {
    pub events: Vec<Value>,
    pub etag: Option<String>,
    pub poll_interval_seconds: Option<u64>,
    pub not_modified: bool,
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

    pub async fn upsert_issue_comment(
        &self,
        owner: &str,
        repo: &str,
        issue_number: i64,
        marker: &str,
        body: &str,
    ) -> Result<String, GitHubClientError> {
        let comments: Vec<Value> = self
            .client
            .get(
                format!("/repos/{owner}/{repo}/issues/{issue_number}/comments?per_page=100"),
                None::<&()>,
            )
            .await?;
        if let Some(comment_id) = comments.iter().find_map(|comment| {
            comment["body"]
                .as_str()
                .filter(|body| body.contains(marker))
                .and_then(|_| comment["id"].as_i64())
        }) {
            let updated: Value = self
                .client
                .patch(
                    format!("/repos/{owner}/{repo}/issues/comments/{comment_id}"),
                    Some(&serde_json::json!({"body": body})),
                )
                .await?;
            return Ok(updated["id"].as_i64().unwrap_or(comment_id).to_string());
        }
        self.create_issue_comment(owner, repo, issue_number, body)
            .await
    }

    pub async fn authenticated_login(&self) -> Result<String, GitHubClientError> {
        Ok(self.client.current().user().await?.login)
    }

    pub async fn canonical_user_login(&self, username: &str) -> Result<String, GitHubClientError> {
        let user: Value = self
            .client
            .get(format!("/users/{username}"), None::<&()>)
            .await?;
        required_string(&user, "login", "GitHub user")
    }

    pub async fn canonical_organization_login(
        &self,
        organization: &str,
    ) -> Result<String, GitHubClientError> {
        let organization: Value = self
            .client
            .get(format!("/orgs/{organization}"), None::<&()>)
            .await?;
        required_string(&organization, "login", "GitHub organization")
    }

    pub async fn canonical_team_slug(
        &self,
        organization: &str,
        team_slug: &str,
    ) -> Result<String, GitHubClientError> {
        let team: Value = self
            .client
            .get(
                format!("/orgs/{organization}/teams/{team_slug}"),
                None::<&()>,
            )
            .await?;
        required_string(&team, "slug", "GitHub team")
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
                        "body": projected_work_item_body(parent_issue_number, item),
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
                let Some((blocking_issue_id, _)) = projected.get(dependency) else {
                    // The dependency may be an existing repository block that
                    // is intentionally outside this lifecycle. It is already
                    // satisfied and must not be projected again.
                    continue;
                };
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

    pub async fn update_projected_work_item(
        &self,
        owner: &str,
        repo: &str,
        parent_issue_number: i64,
        issue_number: i64,
        item: &GitHubWorkItem,
    ) -> Result<(), GitHubClientError> {
        let title = format!("[block] {}", item.id);
        let body = projected_work_item_body(parent_issue_number, item);
        self.client
            .issues(owner, repo)
            .update(issue_number as u64)
            .title(&title)
            .body(&body)
            .send()
            .await?;
        Ok(())
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

    pub async fn repository_events_conditional(
        &self,
        owner: &str,
        repo: &str,
        max_pages: usize,
        etag: Option<&str>,
    ) -> Result<GitHubEventPoll, GitHubClientError> {
        let route = format!("/repos/{owner}/{repo}/events?per_page=100");
        let mut headers = HeaderMap::new();
        if let Some(etag) = etag {
            headers.insert(
                IF_NONE_MATCH,
                HeaderValue::from_str(etag).map_err(|_| {
                    GitHubClientError::InvalidResponse("invalid cached events ETag".into())
                })?,
            );
        }
        let response = self
            .client
            ._get_with_headers(route.as_str(), Some(headers))
            .await?;
        let status = response.status();
        let response_etag = response
            .headers()
            .get(ETAG)
            .and_then(|value| value.to_str().ok())
            .map(str::to_string)
            .or_else(|| etag.map(str::to_string));
        let poll_interval_seconds = response
            .headers()
            .get("x-poll-interval")
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse::<u64>().ok());
        if status == StatusCode::NOT_MODIFIED {
            return Ok(GitHubEventPoll {
                events: Vec::new(),
                etag: response_etag,
                poll_interval_seconds,
                not_modified: true,
            });
        }
        if matches!(
            status,
            StatusCode::FORBIDDEN | StatusCode::TOO_MANY_REQUESTS
        ) {
            let retry_after_seconds = response
                .headers()
                .get("retry-after")
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.parse::<u64>().ok())
                .or_else(|| {
                    let remaining = response
                        .headers()
                        .get("x-ratelimit-remaining")?
                        .to_str()
                        .ok()?
                        .parse::<u64>()
                        .ok()?;
                    if remaining != 0 {
                        return None;
                    }
                    let reset = response
                        .headers()
                        .get("x-ratelimit-reset")?
                        .to_str()
                        .ok()?
                        .parse::<u64>()
                        .ok()?;
                    let now = SystemTime::now().duration_since(UNIX_EPOCH).ok()?.as_secs();
                    Some(reset.saturating_sub(now).max(1))
                })
                .unwrap_or(60);
            return Err(GitHubClientError::RateLimited {
                retry_after_seconds,
            });
        }
        if !status.is_success() {
            return Err(GitHubClientError::InvalidResponse(format!(
                "github repository events returned {status}"
            )));
        }

        let mut page = Page::<Value>::from_response(response).await?;
        let mut events = page.take_items();
        for _ in 1..max_pages.max(1) {
            let Some(mut next_page) = self.client.get_page(&page.next).await? else {
                break;
            };
            events.append(&mut next_page.take_items());
            page = next_page;
        }
        Ok(GitHubEventPoll {
            events,
            etag: response_etag,
            poll_interval_seconds,
            not_modified: false,
        })
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

fn projected_work_item_body(parent_issue_number: i64, item: &GitHubWorkItem) -> String {
    format!(
        "<!-- donkeyspace-work-item -->\n\nParent lifecycle issue: #{parent_issue_number}\n\nSpecification path: `{}`\n\n{}",
        item.spec, item.body
    )
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
    use super::{
        GitHubAuthConfig, GitHubAuthMode, GitHubClient, GitHubWorkItem, SignatureError,
        parse_app_webhook_status, parse_repository, projected_work_item_body,
        select_installation_id, validate_installation_response,
        validate_members_permission_response, verify_signature,
    };
    use hmac::{Hmac, Mac};
    use http_body_util::Full;
    use jsonwebtoken::EncodingKey;
    use octocrab::{AuthState, OctocrabBuilder, auth::AppAuth};
    use serde_json::json;
    use sha2::Sha256;
    use std::{
        collections::HashMap,
        convert::Infallible,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
    };
    use tower::service_fn;

    #[test]
    fn rejects_missing_signature() {
        assert!(verify_signature("secret", b"{}", None).is_err());
    }

    #[test]
    fn parses_app_webhook_configuration_and_deliveries() {
        let status = parse_app_webhook_status(
            &json!({"events": ["push", "issue_comment", "push"]}),
            &json!({"url": "https://hooks.example/webhooks/github", "content_type": "json"}),
            &json!([{
                "id": 42,
                "event": "issue_comment",
                "action": "created",
                "status": "OK",
                "status_code": 202,
                "delivered_at": "2026-09-01T20:00:00Z"
            }]),
        )
        .unwrap();
        assert_eq!(status.subscribed_events, vec!["issue_comment", "push"]);
        assert_eq!(status.deliveries[0].status_code, Some(202));
        assert_eq!(
            status.url.as_deref(),
            Some("https://hooks.example/webhooks/github")
        );
    }

    #[test]
    fn projected_work_item_body_contains_current_specification() {
        let body = projected_work_item_body(
            58,
            &GitHubWorkItem {
                id: "divider".into(),
                spec: "docs/divider/spec.md".into(),
                body: "Specification version: 1.1.0\nSigned division.".into(),
                depends_on: Vec::new(),
            },
        );

        assert!(body.contains("Parent lifecycle issue: #58"));
        assert!(body.contains("Specification path: `docs/divider/spec.md`"));
        assert!(body.contains("Specification version: 1.1.0"));
        assert!(body.contains("Signed division."));
    }

    #[test]
    fn verifies_valid_signature_and_rejects_malformed_or_bad_signatures() {
        let payload = br#"{"action":"opened"}"#;
        let mut mac = Hmac::<Sha256>::new_from_slice(b"secret").unwrap();
        mac.update(payload);
        let signature = format!("sha256={}", hex::encode(mac.finalize().into_bytes()));
        assert_eq!(
            verify_signature("secret", payload, Some(&signature)),
            Ok(())
        );
        assert_eq!(
            verify_signature("secret", payload, Some("md5=abcd")),
            Err(SignatureError::InvalidPrefix)
        );
        assert_eq!(
            verify_signature("secret", payload, Some("sha256=not-hex")),
            Err(SignatureError::InvalidHex)
        );
        assert_eq!(
            verify_signature("different", payload, Some(&signature)),
            Err(SignatureError::Mismatch)
        );
    }

    fn config(
        values: &[(&str, &str)],
    ) -> Result<Option<GitHubAuthConfig>, super::GitHubClientError> {
        let values = values
            .iter()
            .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
            .collect::<HashMap<_, _>>();
        GitHubAuthConfig::from_lookup(|name| values.get(name).cloned())
    }

    #[test]
    fn app_configuration_requires_every_field() {
        let error = config(&[("DONKEYSPACE_GITHUB_APP_ID", "7")]).unwrap_err();
        let message = error.to_string();
        assert!(message.contains("DONKEYSPACE_GITHUB_INSTALLATION_ID"));
        assert!(message.contains("DONKEYSPACE_GITHUB_PRIVATE_KEY_FILE"));
    }

    #[test]
    fn rejects_mixed_app_and_pat_credentials() {
        let error = config(&[
            ("DONKEYSPACE_GITHUB_APP_ID", "7"),
            ("DONKEYSPACE_GITHUB_INSTALLATION_ID", "8"),
            ("DONKEYSPACE_GITHUB_PRIVATE_KEY_FILE", "/secret/key.pem"),
            ("DONKEYSPACE_GITHUB_TOKEN", "sensitive"),
        ])
        .unwrap_err();
        assert!(error.to_string().contains("cannot be configured together"));
    }

    #[test]
    fn pat_debug_output_redacts_token() {
        let config = config(&[("DONKEYSPACE_GITHUB_TOKEN", "ghp_sensitive")])
            .unwrap()
            .unwrap();
        assert_eq!(config.mode(), GitHubAuthMode::Pat);
        let output = format!("{config:?}");
        assert!(!output.contains("ghp_sensitive"));
        assert!(output.contains("REDACTED"));
    }

    #[test]
    fn explicit_pat_mode_rejects_app_fields() {
        assert!(
            config(&[
                ("DONKEYSPACE_GITHUB_AUTH_MODE", "pat"),
                ("DONKEYSPACE_GITHUB_APP_ID", "7"),
                ("DONKEYSPACE_GITHUB_TOKEN", "token"),
            ])
            .is_err()
        );
    }

    #[test]
    fn app_ids_must_be_positive() {
        let error = config(&[
            ("DONKEYSPACE_GITHUB_APP_ID", "0"),
            ("DONKEYSPACE_GITHUB_INSTALLATION_ID", "8"),
            ("DONKEYSPACE_GITHUB_PRIVATE_KEY_FILE", "/secret/key.pem"),
        ])
        .unwrap_err();
        assert!(error.to_string().contains("positive integer"));
    }

    fn installation(
        permissions: serde_json::Value,
        suspended_at: serde_json::Value,
    ) -> serde_json::Value {
        json!({
            "suspended_at": suspended_at,
            "permissions": permissions,
        })
    }

    #[test]
    fn parses_repository_selection_metadata_without_credentials() {
        let repository = parse_repository(&json!({
            "owner": {"login": "example"},
            "name": "private-repo",
            "full_name": "example/private-repo",
            "private": true
        }))
        .unwrap();
        assert_eq!(repository.full_name, "example/private-repo");
        assert!(repository.private);
    }

    #[test]
    fn accepts_installation_with_exact_required_permissions() {
        let response = installation(
            json!({
                "metadata": "read",
                "contents": "write",
                "issues": "write",
                "pull_requests": "write"
            }),
            serde_json::Value::Null,
        );
        validate_installation_response(&response).unwrap();
    }

    #[test]
    fn members_permission_is_required_for_organization_access() {
        assert!(
            validate_members_permission_response(&installation(
                json!({"members": "read"}),
                serde_json::Value::Null,
            ))
            .is_ok()
        );
        assert!(
            validate_members_permission_response(&installation(
                json!({"metadata": "read"}),
                serde_json::Value::Null,
            ))
            .unwrap_err()
            .to_string()
            .contains("members: read")
        );
    }

    #[test]
    fn rejects_suspended_installation() {
        let response = installation(
            json!({
                "contents": "write",
                "issues": "write",
                "pull_requests": "write"
            }),
            json!("2026-08-11T00:00:00Z"),
        );
        let error = validate_installation_response(&response).unwrap_err();
        assert!(error.to_string().contains("suspended"));
    }

    #[test]
    fn rejects_installation_with_missing_permissions() {
        let response = installation(
            json!({
                "contents": "write",
                "issues": "read",
                "pull_requests": "write"
            }),
            serde_json::Value::Null,
        );
        let error = validate_installation_response(&response).unwrap_err();
        assert!(error.to_string().contains("issues: write"));
    }

    #[test]
    fn discovers_installation_for_repository_owner() {
        let installations = json!([
            {
                "id": 8,
                "account": {"login": "another-owner"},
                "suspended_at": null,
                "permissions": {"contents":"write","issues":"write","pull_requests":"write"}
            },
            {
                "id": 42,
                "account": {"login": "Example-Owner"},
                "suspended_at": null,
                "permissions": {"contents":"write","issues":"write","pull_requests":"write"}
            }
        ]);
        assert_eq!(
            select_installation_id(&installations, "example-owner").unwrap(),
            42
        );
    }

    #[test]
    fn installation_discovery_rejects_missing_or_ambiguous_owner() {
        let one = json!([{
            "id": 42,
            "account": {"login": "owner"},
            "suspended_at": null,
            "permissions": {"contents":"write","issues":"write","pull_requests":"write"}
        }]);
        assert!(select_installation_id(&one, "missing").is_err());
        let duplicate = json!([one[0].clone(), one[0].clone()]);
        assert!(select_installation_id(&duplicate, "owner").is_err());
    }

    #[tokio::test]
    async fn refreshes_expired_installation_token() {
        let requests = Arc::new(AtomicUsize::new(0));
        let request_count = Arc::clone(&requests);
        let service = service_fn(move |request: http::Request<octocrab::OctoBody>| {
            let request_count = Arc::clone(&request_count);
            async move {
                assert_eq!(request.method(), http::Method::POST);
                assert_eq!(request.uri().path(), "/app/installations/8/access_tokens");
                let index = request_count.fetch_add(1, Ordering::SeqCst) + 1;
                let body = format!(
                    r#"{{"token":"installation-token-{index}","expires_at":"2000-01-01T00:00:00Z","permissions":{{}},"repositories":null}}"#
                );
                Ok::<_, Infallible>(
                    http::Response::builder()
                        .status(http::StatusCode::CREATED)
                        .header(http::header::CONTENT_TYPE, "application/json")
                        .body(Full::new(bytes::Bytes::from(body)))
                        .unwrap(),
                )
            }
        });
        let key =
            EncodingKey::from_rsa_pem(include_bytes!("../tests/fixtures/test-github-app.pem"))
                .unwrap();
        let app_client = OctocrabBuilder::new_empty()
            .with_service(service)
            .with_auth(AuthState::App(AppAuth {
                app_id: 7_u64.into(),
                key,
            }))
            .build()
            .unwrap();
        let installation_client = app_client.installation(8_u64.into()).unwrap();
        let provider = super::GitHubCredentialProvider {
            config: GitHubAuthConfig::App {
                app_id: 7,
                installation_id: 8,
                private_key_file: "unused-test-key.pem".into(),
            },
            client: Arc::new(installation_client),
            app_client: Some(Arc::new(app_client)),
        };

        assert_eq!(provider.token().await.unwrap(), "installation-token-1");
        assert_eq!(provider.token().await.unwrap(), "installation-token-2");
        assert_eq!(requests.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn repository_events_reuse_etag_and_honor_poll_interval_header() {
        let requests = Arc::new(AtomicUsize::new(0));
        let request_count = Arc::clone(&requests);
        let service = service_fn(move |request: http::Request<octocrab::OctoBody>| {
            let request_count = Arc::clone(&request_count);
            async move {
                let index = request_count.fetch_add(1, Ordering::SeqCst);
                assert_eq!(request.uri().path(), "/repos/acme/rtl/events");
                if index == 0 {
                    assert!(request.headers().get(http::header::IF_NONE_MATCH).is_none());
                    Ok::<_, Infallible>(
                        http::Response::builder()
                            .status(http::StatusCode::OK)
                            .header(http::header::CONTENT_TYPE, "application/json")
                            .header(http::header::ETAG, "\"events-v1\"")
                            .header("x-poll-interval", "60")
                            .body(Full::new(bytes::Bytes::from_static(
                                br#"[{"id":"1","type":"IssuesEvent","payload":{}}]"#,
                            )))
                            .unwrap(),
                    )
                } else {
                    assert_eq!(
                        request.headers()[http::header::IF_NONE_MATCH],
                        "\"events-v1\""
                    );
                    Ok::<_, Infallible>(
                        http::Response::builder()
                            .status(http::StatusCode::NOT_MODIFIED)
                            .header(http::header::ETAG, "\"events-v1\"")
                            .header("x-poll-interval", "60")
                            .body(Full::new(bytes::Bytes::new()))
                            .unwrap(),
                    )
                }
            }
        });
        let client = GitHubClient {
            client: OctocrabBuilder::new_empty()
                .with_service(service)
                .with_auth(AuthState::None)
                .build()
                .unwrap(),
        };

        let first = client
            .repository_events_conditional("acme", "rtl", 1, None)
            .await
            .unwrap();
        assert_eq!(first.events.len(), 1);
        assert_eq!(first.poll_interval_seconds, Some(60));
        let second = client
            .repository_events_conditional("acme", "rtl", 1, first.etag.as_deref())
            .await
            .unwrap();
        assert!(second.not_modified);
        assert!(second.events.is_empty());
    }
}
