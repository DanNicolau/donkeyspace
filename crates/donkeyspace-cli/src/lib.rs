use donkeyspace_core::EngagementSelector;
use donkeyspace_github::{
    GitHubAuthConfig, GitHubCredentialProvider, GitHubRepository, discover_installation_id,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    env, fs,
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    path::{Path, PathBuf},
    process::{Command, Stdio},
};
use thiserror::Error;

mod plugins;
pub mod tui;
pub use plugins::{PluginConnectOptions, PluginEnvironmentInput};

const SCHEMA_VERSION: u32 = 4;
pub const DEFAULT_API_PORT: u16 = 8080;
pub const DEFAULT_WEB_PORT: u16 = 5173;
const PORT_SUGGESTION_ATTEMPTS: u16 = 100;
const CONFIG_FILE: &str = "instance.json";
const GENERATED_ENV: &str = "compose.env";
const PENDING_GITHUB_FILE: &str = "pending-github.json";

#[derive(Debug, Error)]
pub enum SetupError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("configuration error: {0}")]
    Config(String),
    #[error("invalid configuration JSON: {0}")]
    Json(#[from] serde_json::Error),
    #[error("invalid configuration YAML: {0}")]
    Yaml(#[from] serde_yaml::Error),
    #[error("github error: {0}")]
    GitHub(#[from] donkeyspace_github::GitHubClientError),
    #[error("command `{command}` failed: {detail}")]
    Command { command: String, detail: String },
}

#[derive(Debug, PartialEq, Eq)]
struct ManifestConversion {
    app_id: u64,
    slug: String,
    private_key: Vec<u8>,
    webhook_secret: Vec<u8>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum RuntimeSource {
    LocalBuild,
    RegistryImage,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InstanceConfig {
    pub schema_version: u32,
    pub source_tree: PathBuf,
    pub runtime_source: RuntimeSource,
    pub api_port: u16,
    #[serde(default = "default_web_port")]
    pub web_port: u16,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub codex_home: Option<PathBuf>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub github: Option<GitHubInstanceConfig>,
    #[serde(default)]
    pub github_access: BTreeMap<String, Vec<GitHubAccessSubject>>,
    #[serde(default)]
    pub plugins: BTreeMap<String, InstalledPlugin>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active_plugin: Option<ActivePlugin>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "kebab-case", deny_unknown_fields)]
pub enum GitHubAccessSubject {
    User {
        login: String,
    },
    Organization {
        login: String,
    },
    Team {
        organization: String,
        team_slug: String,
    },
}

impl GitHubAccessSubject {
    pub fn display_name(&self) -> String {
        match self {
            Self::User { login } => format!("user:{login}"),
            Self::Organization { login } => format!("organization:{login}"),
            Self::Team {
                organization,
                team_slug,
            } => format!("team:{organization}/{team_slug}"),
        }
    }

    fn selector(&self) -> EngagementSelector {
        match self {
            Self::User { login } => EngagementSelector::User {
                login: login.clone(),
            },
            Self::Organization { login } => EngagementSelector::OrganizationMember {
                organization: login.clone(),
            },
            Self::Team {
                organization,
                team_slug,
            } => EngagementSelector::TeamMember {
                organization: organization.clone(),
                team_slug: team_slug.clone(),
            },
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct InstalledPlugin {
    pub id: String,
    pub source_path: PathBuf,
    pub manifest_path: PathBuf,
    pub image: String,
    pub build_context: PathBuf,
    pub dockerfile: PathBuf,
    pub flows: BTreeMap<String, PluginFlowClass>,
    #[serde(default)]
    pub environment_files: BTreeMap<String, PathBuf>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ActivePlugin {
    pub id: String,
    pub flow: String,
    pub class: PluginFlowClass,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum PluginFlowClass {
    LifecycleReplacement,
    Developer,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "mode", rename_all = "lowercase")]
pub enum GitHubInstanceConfig {
    App {
        app_id: u64,
        installation_id: u64,
        private_key_file: PathBuf,
        webhook_secret_file: PathBuf,
        repositories: Vec<String>,
        ingress: IngressMode,
    },
    Pat {
        token_file: PathBuf,
        repositories: Vec<String>,
        ingress: IngressMode,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum IngressMode {
    Polling,
    Webhook { public_url: String },
}

#[derive(Debug)]
pub struct ConnectGitHubOptions {
    pub app_id: Option<u64>,
    pub installation_id: Option<u64>,
    pub private_key_file: Option<PathBuf>,
    pub webhook_secret_file: Option<PathBuf>,
    pub repositories: Vec<String>,
    pub public_url: Option<String>,
    pub callback_port: u16,
    pub organization: Option<String>,
    pub pat: bool,
}

#[derive(Debug, Clone, Copy)]
pub enum CodexLoginMethod {
    ChatGpt,
    ApiKey,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum CheckLevel {
    Pass,
    Warning,
    Fail,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DoctorCheck {
    pub name: String,
    pub level: CheckLevel,
    pub detail: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DoctorReport {
    pub checks: Vec<DoctorCheck>,
}

impl DoctorReport {
    pub fn passed(&self) -> bool {
        !self
            .checks
            .iter()
            .any(|check| check.level == CheckLevel::Fail)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ServiceStatus {
    pub name: String,
    pub state: String,
    pub health: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DeploymentStatus {
    pub services: Vec<ServiceStatus>,
}

impl DeploymentStatus {
    pub fn running(&self) -> bool {
        ["postgres", "api", "worker", "web"].iter().all(|expected| {
            self.services.iter().any(|service| {
                service.name == *expected && service.state.eq_ignore_ascii_case("running")
            })
        })
    }

    pub fn service_running(&self, expected: &str) -> bool {
        self.services.iter().any(|service| {
            service.name == expected && service.state.eq_ignore_ascii_case("running")
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PendingGitHubApp {
    pub app_id: u64,
    pub slug: String,
    pub owner: String,
    pub ingress: IngressMode,
    pub callback_port: u16,
    pub organization: Option<String>,
    private_key_file: PathBuf,
    webhook_secret_file: PathBuf,
}

pub struct Instance {
    directory: PathBuf,
    config: Option<InstanceConfig>,
}

impl Instance {
    pub fn open(directory: Option<PathBuf>) -> Result<Self, SetupError> {
        let directory = directory.unwrap_or_else(default_config_directory);
        let path = directory.join(CONFIG_FILE);
        let (config, migrated) = if path.exists() {
            let bytes = fs::read(&path)?;
            let mut config: InstanceConfig = serde_json::from_slice(&bytes)?;
            let migrated = match config.schema_version {
                1 | 2 | 3 => {
                    reconcile_github_access(&mut config);
                    config.schema_version = SCHEMA_VERSION;
                    true
                }
                SCHEMA_VERSION => false,
                version => {
                    return Err(SetupError::Config(format!(
                        "unsupported schema_version {version}; expected {SCHEMA_VERSION}",
                    )));
                }
            };
            (Some(config), migrated)
        } else {
            (None, false)
        };
        let instance = Self { directory, config };
        if migrated {
            instance.save()?;
        }
        Ok(instance)
    }

    pub fn config_path(&self) -> PathBuf {
        self.directory.join(CONFIG_FILE)
    }

    pub fn directory(&self) -> &Path {
        &self.directory
    }

    pub fn config(&self) -> Option<&InstanceConfig> {
        self.config.as_ref()
    }

    pub fn is_initialized(&self) -> bool {
        self.config.is_some()
    }

    pub fn init(
        &mut self,
        source_tree: PathBuf,
        runtime_source: RuntimeSource,
    ) -> Result<(), SetupError> {
        self.init_with_ports(source_tree, runtime_source, None, None)
    }

    pub fn init_with_ports(
        &mut self,
        source_tree: PathBuf,
        runtime_source: RuntimeSource,
        api_port: Option<u16>,
        web_port: Option<u16>,
    ) -> Result<(), SetupError> {
        if runtime_source == RuntimeSource::RegistryImage {
            return Err(SetupError::Config(
                "registry-image is reserved for a future release backend".into(),
            ));
        }
        fs::create_dir_all(&self.directory)?;
        set_directory_mode(&self.directory)?;
        let source_tree = fs::canonicalize(source_tree)?;
        let config = match self.config.clone() {
            Some(mut existing) => {
                existing.source_tree = source_tree;
                existing.runtime_source = runtime_source;
                existing.api_port = api_port.unwrap_or(existing.api_port);
                existing.web_port = web_port.unwrap_or(existing.web_port);
                existing
            }
            None => InstanceConfig {
                schema_version: SCHEMA_VERSION,
                source_tree,
                runtime_source,
                api_port: api_port.unwrap_or(DEFAULT_API_PORT),
                web_port: web_port.unwrap_or(DEFAULT_WEB_PORT),
                codex_home: None,
                github: None,
                github_access: BTreeMap::new(),
                plugins: BTreeMap::new(),
                active_plugin: None,
            },
        };
        validate_ports(config.api_port, config.web_port)?;
        self.config = Some(config);
        self.save()
    }

    pub fn configure_ports(
        &mut self,
        api_port: Option<u16>,
        web_port: Option<u16>,
    ) -> Result<(), SetupError> {
        self.configure_ports_with_availability(api_port, web_port, port_is_available)
    }

    fn configure_ports_with_availability<F>(
        &mut self,
        api_port: Option<u16>,
        web_port: Option<u16>,
        available: F,
    ) -> Result<(), SetupError>
    where
        F: Fn(u16) -> bool,
    {
        if api_port.is_none() && web_port.is_none() {
            return Err(SetupError::Config(
                "provide --api-port, --web-port, or both".into(),
            ));
        }
        let config = self.require_config()?;
        let api_port = api_port.unwrap_or(config.api_port);
        let web_port = web_port.unwrap_or(config.web_port);
        validate_ports(api_port, web_port)?;
        for (label, flag, current, requested) in [
            ("API", "--api-port", config.api_port, api_port),
            ("dashboard", "--web-port", config.web_port, web_port),
        ] {
            if requested != current && !available(requested) {
                let suggestion =
                    suggest_available_port_with(requested, &[api_port, web_port], &available)
                        .map(|port| format!("; try `{flag} {port}`"))
                        .unwrap_or_default();
                return Err(SetupError::Config(format!(
                    "{label} port 127.0.0.1:{requested} is already in use{suggestion}"
                )));
            }
        }
        self.config.as_mut().unwrap().api_port = api_port;
        self.config.as_mut().unwrap().web_port = web_port;
        self.save()
    }

    pub fn api_url(&self) -> Result<String, SetupError> {
        Ok(format!(
            "http://127.0.0.1:{}",
            self.require_config()?.api_port
        ))
    }

    pub fn dashboard_url(&self) -> Result<String, SetupError> {
        Ok(format!(
            "http://127.0.0.1:{}",
            self.require_config()?.web_port
        ))
    }

    pub fn github_access(&self, repository: &str) -> Result<&[GitHubAccessSubject], SetupError> {
        let config = self.require_config()?;
        let repository = configured_repository(config, repository)?;
        Ok(config
            .github_access
            .get(repository)
            .map(Vec::as_slice)
            .unwrap_or_default())
    }

    pub async fn add_github_access(
        &mut self,
        repository: &str,
        subject: GitHubAccessSubject,
    ) -> Result<GitHubAccessSubject, SetupError> {
        let repository = configured_repository(self.require_config()?, repository)?.to_string();
        let owner = repository.split_once('/').expect("validated repository").0;
        validate_subject_owner(owner, &subject)?;
        let provider = self.github_credential_provider()?;
        let subject = match subject {
            GitHubAccessSubject::User { login } => GitHubAccessSubject::User {
                login: provider.client().canonical_user_login(login.trim()).await?,
            },
            GitHubAccessSubject::Organization { login } => {
                provider.validate_members_permission().await?;
                GitHubAccessSubject::Organization {
                    login: provider
                        .client()
                        .canonical_organization_login(login.trim())
                        .await?,
                }
            }
            GitHubAccessSubject::Team {
                organization,
                team_slug,
            } => {
                provider.validate_members_permission().await?;
                let team_slug = provider
                    .client()
                    .canonical_team_slug(organization.trim(), team_slug.trim())
                    .await?;
                GitHubAccessSubject::Team {
                    organization: owner.to_string(),
                    team_slug,
                }
            }
        };
        validate_subject_owner(owner, &subject)?;
        let entries = self
            .config
            .as_mut()
            .expect("configuration was validated")
            .github_access
            .entry(repository)
            .or_default();
        if entries
            .iter()
            .any(|existing| subjects_equal(existing, &subject))
        {
            return Err(SetupError::Config(format!(
                "GitHub access `{}` is already configured",
                subject.display_name()
            )));
        }
        entries.push(subject.clone());
        entries.sort_by_key(GitHubAccessSubject::display_name);
        self.save_and_apply_github_access()?;
        Ok(subject)
    }

    pub fn remove_github_access(
        &mut self,
        repository: &str,
        subject: &GitHubAccessSubject,
    ) -> Result<(), SetupError> {
        let repository = configured_repository(self.require_config()?, repository)?.to_string();
        let entries = self
            .config
            .as_mut()
            .expect("configuration was validated")
            .github_access
            .entry(repository)
            .or_default();
        let previous = entries.len();
        entries.retain(|existing| !subjects_equal(existing, subject));
        if entries.len() == previous {
            return Err(SetupError::Config(format!(
                "GitHub access `{}` is not configured",
                subject.display_name()
            )));
        }
        self.save_and_apply_github_access()
    }

    pub fn pending_github_app(&self) -> Result<Option<PendingGitHubApp>, SetupError> {
        let path = self.directory.join(PENDING_GITHUB_FILE);
        if !path.exists() {
            return Ok(None);
        }
        Ok(Some(serde_json::from_slice(&fs::read(path)?)?))
    }

    pub fn discard_pending_github_app(&self) -> Result<(), SetupError> {
        let Some(pending) = self.pending_github_app()? else {
            return Ok(());
        };
        remove_file_if_present(&pending.private_key_file)?;
        remove_file_if_present(&pending.webhook_secret_file)?;
        remove_file_if_present(&self.directory.join(PENDING_GITHUB_FILE))
    }

    pub fn begin_github_app(
        &self,
        owner: String,
        ingress: IngressMode,
        callback_port: u16,
        organization: Option<String>,
    ) -> Result<PendingGitHubApp, SetupError> {
        self.require_config()?;
        validate_owner(&owner)?;
        validate_organization(organization.as_deref())?;
        validate_ingress(&ingress)?;
        let conversion =
            run_manifest_registration(callback_port, organization.as_deref(), &ingress)?;
        let private_key_file = self.secret_path("pending-github-app.pem");
        let webhook_secret_file = self.secret_path("pending-github-webhook-secret");
        write_secret(&private_key_file, &conversion.private_key)?;
        write_secret(&webhook_secret_file, &conversion.webhook_secret)?;
        let pending = PendingGitHubApp {
            app_id: conversion.app_id,
            slug: conversion.slug,
            owner,
            ingress,
            callback_port,
            organization,
            private_key_file,
            webhook_secret_file,
        };
        write_secret(
            &self.directory.join(PENDING_GITHUB_FILE),
            &serde_json::to_vec_pretty(&pending)?,
        )?;
        Ok(pending)
    }

    pub async fn pending_github_repositories(
        &self,
        pending: &PendingGitHubApp,
    ) -> Result<(u64, Vec<GitHubRepository>), SetupError> {
        let installation_id = discover_installation_id(
            pending.app_id,
            pending.private_key_file.clone(),
            &pending.owner,
        )
        .await?;
        let provider = GitHubCredentialProvider::new(GitHubAuthConfig::App {
            app_id: pending.app_id,
            installation_id,
            private_key_file: pending.private_key_file.clone(),
        })?;
        provider.validate_installation().await?;
        let repositories = provider
            .repositories()
            .await?
            .into_iter()
            .filter(|repository| repository.owner.eq_ignore_ascii_case(&pending.owner))
            .collect();
        Ok((installation_id, repositories))
    }

    pub async fn complete_pending_github_app(
        &mut self,
        pending: PendingGitHubApp,
        installation_id: u64,
        repositories: Vec<String>,
    ) -> Result<(), SetupError> {
        self.require_config()?;
        validate_repositories(&repositories)?;
        if repositories.iter().any(|repository| {
            !repository
                .split_once('/')
                .is_some_and(|(owner, _)| owner.eq_ignore_ascii_case(&pending.owner))
        }) {
            return Err(SetupError::Config(format!(
                "selected repositories must belong to `{}`",
                pending.owner
            )));
        }
        let provider = GitHubCredentialProvider::new(GitHubAuthConfig::App {
            app_id: pending.app_id,
            installation_id,
            private_key_file: pending.private_key_file.clone(),
        })?;
        provider.validate_installation().await?;
        for repository in &repositories {
            let (owner, repo) = repository.split_once('/').expect("validated repository");
            provider.client().repository(owner, repo).await?;
        }
        let private_key_file = self.secret_path(&format!("github-app-{}.pem", pending.app_id));
        let webhook_secret_file =
            self.secret_path(&format!("github-webhook-secret-{}", pending.app_id));
        write_secret(&private_key_file, &fs::read(&pending.private_key_file)?)?;
        write_secret(
            &webhook_secret_file,
            &fs::read(&pending.webhook_secret_file)?,
        )?;
        self.config.as_mut().unwrap().github = Some(GitHubInstanceConfig::App {
            app_id: pending.app_id,
            installation_id,
            private_key_file,
            webhook_secret_file,
            repositories,
            ingress: pending.ingress,
        });
        reconcile_github_access(self.config.as_mut().unwrap());
        self.save()?;
        self.discard_pending_github_app()?;
        Ok(())
    }

    pub async fn app_repositories(
        &self,
        app_id: u64,
        installation_id: u64,
        private_key_file: PathBuf,
    ) -> Result<Vec<GitHubRepository>, SetupError> {
        let provider = GitHubCredentialProvider::new(GitHubAuthConfig::App {
            app_id,
            installation_id,
            private_key_file,
        })?;
        provider.validate_installation().await?;
        Ok(provider.repositories().await?)
    }

    pub async fn pat_repositories(&self, token: &str) -> Result<Vec<GitHubRepository>, SetupError> {
        let provider = GitHubCredentialProvider::new(GitHubAuthConfig::Pat {
            token: token.trim().to_string(),
        })?;
        Ok(provider.repositories().await?)
    }

    pub async fn connect_github_pat(
        &mut self,
        token: &str,
        repositories: Vec<String>,
        ingress: IngressMode,
    ) -> Result<(), SetupError> {
        self.require_config()?;
        validate_repositories(&repositories)?;
        validate_ingress(&ingress)?;
        if token.trim().is_empty() {
            return Err(SetupError::Config("PAT cannot be empty".into()));
        }
        let provider = GitHubCredentialProvider::new(GitHubAuthConfig::Pat {
            token: token.trim().to_string(),
        })?;
        for repository in &repositories {
            let (owner, repo) = repository.split_once('/').expect("validated repository");
            provider.client().repository(owner, repo).await?;
        }
        let token_file = self.secret_path("github-pat");
        write_secret(&token_file, token.trim().as_bytes())?;
        self.config.as_mut().unwrap().github = Some(GitHubInstanceConfig::Pat {
            token_file,
            repositories,
            ingress,
        });
        reconcile_github_access(self.config.as_mut().unwrap());
        self.save()
    }

    pub async fn connect_github(
        &mut self,
        options: ConnectGitHubOptions,
    ) -> Result<(), SetupError> {
        self.require_config()?;
        validate_repositories(&options.repositories)?;
        if let Some(organization) = &options.organization
            && (organization.is_empty()
                || !organization
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-'))
        {
            return Err(SetupError::Config(
                "organization must contain only letters, numbers, and hyphens".into(),
            ));
        }
        let ingress = match options.public_url {
            Some(url) => {
                if !url.starts_with("https://") {
                    return Err(SetupError::Config(
                        "public webhook URL must use HTTPS".into(),
                    ));
                }
                IngressMode::Webhook { public_url: url }
            }
            None => {
                eprintln!(
                    "warning: no public HTTPS URL configured; repository polling is delayed and is not real-time"
                );
                IngressMode::Polling
            }
        };

        let github = if options.pat {
            if options.app_id.is_some()
                || options.installation_id.is_some()
                || options.private_key_file.is_some()
            {
                return Err(SetupError::Config(
                    "--pat cannot be combined with GitHub App options".into(),
                ));
            }
            eprintln!("warning: fine-grained PAT mode is deprecated and remains user-linked");
            let token = read_secret("Fine-grained GitHub PAT: ")?;
            if token.trim().is_empty() {
                return Err(SetupError::Config("PAT cannot be empty".into()));
            }
            let token_file = self.secret_path("github-pat");
            write_secret(&token_file, token.trim().as_bytes())?;
            GitHubInstanceConfig::Pat {
                token_file,
                repositories: options.repositories,
                ingress,
            }
        } else {
            let (app_id, installation_id, private_key, webhook_secret) =
                match (options.app_id, options.installation_id, options.private_key_file) {
                    (Some(app_id), Some(installation_id), Some(private_key_file)) => {
                        let webhook_secret = match options.webhook_secret_file {
                            Some(path) => fs::read(path)?,
                            None if matches!(ingress, IngressMode::Polling) => random_hex(32)?.into_bytes(),
                            None => return Err(SetupError::Config(
                                "webhook mode requires --webhook-secret-file".into(),
                            )),
                        };
                        (
                            app_id,
                            Some(installation_id),
                            fs::read(private_key_file)?,
                            webhook_secret,
                        )
                    }
                    (None, None, None) => {
                        let (app_id, private_key, webhook_secret) = run_manifest_flow(
                            options.callback_port,
                            options.organization.as_deref(),
                            &ingress,
                        )?;
                        (app_id, None, private_key, webhook_secret)
                    }
                    _ => return Err(SetupError::Config(
                        "provide --app-id, --installation-id, and --private-key-file together, or omit all three to use the manifest flow".into(),
                    )),
                };
            let private_key_file = self.secret_path("github-app.pem");
            let webhook_secret_file = self.secret_path("github-webhook-secret");
            write_secret(&private_key_file, &private_key)?;
            write_secret(&webhook_secret_file, &webhook_secret)?;
            let repository_owner = options.repositories[0]
                .split_once('/')
                .expect("validated repository")
                .0;
            let installation_id = match installation_id {
                Some(installation_id) => installation_id,
                None => {
                    let installation_id = discover_installation_id(
                        app_id,
                        private_key_file.clone(),
                        repository_owner,
                    )
                    .await?;
                    println!(
                        "Discovered GitHub App installation {installation_id} for {repository_owner}"
                    );
                    installation_id
                }
            };
            let provider = GitHubCredentialProvider::new(GitHubAuthConfig::App {
                app_id,
                installation_id,
                private_key_file: private_key_file.clone(),
            })?;
            provider.validate_installation().await?;
            let client = provider.client();
            for repository in &options.repositories {
                let (owner, repo) = repository.split_once('/').expect("validated repository");
                client.repository(owner, repo).await.map_err(|error| {
                    SetupError::Config(format!("installation cannot access {repository}: {error}"))
                })?;
            }
            GitHubInstanceConfig::App {
                app_id,
                installation_id,
                private_key_file,
                webhook_secret_file,
                repositories: options.repositories,
                ingress,
            }
        };
        self.config.as_mut().unwrap().github = Some(github);
        reconcile_github_access(self.config.as_mut().unwrap());
        self.save()?;
        println!("GitHub connection saved and validated");
        Ok(())
    }

    pub fn connect_codex(&mut self, method: CodexLoginMethod) -> Result<(), SetupError> {
        self.require_config()?;
        match method {
            CodexLoginMethod::ChatGpt => run_status(Command::new("codex").arg("login"))?,
            CodexLoginMethod::ApiKey => {
                let key = read_secret("OpenAI project API key: ")?;
                self.run_codex_api_key_login(&key)?;
            }
        }
        self.finish_codex_connection()
    }

    pub fn connect_codex_api_key(&mut self, key: &str) -> Result<(), SetupError> {
        self.require_config()?;
        self.run_codex_api_key_login(key)?;
        self.finish_codex_connection()
    }

    pub fn codex_login_status(&self) -> Result<(), SetupError> {
        check_command("codex", &["login", "status"])
    }

    fn run_codex_api_key_login(&self, key: &str) -> Result<(), SetupError> {
        if key.trim().is_empty() {
            return Err(SetupError::Config("API key cannot be empty".into()));
        }
        let mut child = Command::new("codex")
            .args(["login", "--with-api-key"])
            .stdin(Stdio::piped())
            .spawn()?;
        child
            .stdin
            .take()
            .unwrap()
            .write_all(key.trim().as_bytes())?;
        let status = child.wait()?;
        if !status.success() {
            return Err(SetupError::Command {
                command: "codex login --with-api-key".into(),
                detail: status.to_string(),
            });
        }
        Ok(())
    }

    fn finish_codex_connection(&mut self) -> Result<(), SetupError> {
        run_status(Command::new("codex").args(["login", "status"]))?;
        self.config.as_mut().unwrap().codex_home = Some(default_codex_home());
        self.save()
    }

    pub async fn doctor_report(&self) -> Result<DoctorReport, SetupError> {
        let config = self.require_config()?;
        let mut checks = vec![
            command_doctor_check("Docker CLI", "docker", &["--version"]),
            command_doctor_check("Docker Compose", "docker", &["compose", "version"]),
            command_doctor_check("Docker daemon", "docker", &["info"]),
        ];
        for file in ["Dockerfile", "docker-compose.yml", "web/package.json"] {
            checks.push(if config.source_tree.join(file).exists() {
                DoctorCheck {
                    name: format!("Source: {file}"),
                    level: CheckLevel::Pass,
                    detail: "found".into(),
                }
            } else {
                DoctorCheck {
                    name: format!("Source: {file}"),
                    level: CheckLevel::Fail,
                    detail: format!("missing from {}", config.source_tree.display()),
                }
            });
        }
        if let Some(active) = &config.active_plugin {
            let plugin = config.plugins.get(&active.id);
            checks.push(match plugin {
                Some(plugin) if plugin.manifest_path.is_file() => DoctorCheck {
                    name: "Active plugin".into(),
                    level: CheckLevel::Warning,
                    detail: format!(
                        "{}:{} is active ({:?}); the worker receives the Docker socket",
                        active.id, active.flow, active.class
                    ),
                },
                Some(plugin) => DoctorCheck {
                    name: "Active plugin".into(),
                    level: CheckLevel::Fail,
                    detail: format!("manifest is missing: {}", plugin.manifest_path.display()),
                },
                None => DoctorCheck {
                    name: "Active plugin".into(),
                    level: CheckLevel::Fail,
                    detail: format!("{} is not in the installed plugin registry", active.id),
                },
            });
            if let Some(plugin) = plugin {
                for (name, path) in &plugin.environment_files {
                    checks.push(doctor_result(
                        &format!("Plugin environment: {name}"),
                        check_secret_permissions(path),
                    ));
                }
                checks.push(doctor_result(
                    "Plugin image",
                    check_command("docker", &["image", "inspect", &plugin.image]),
                ));
            }
        }
        let deployment = self.deployment_status().unwrap_or(DeploymentStatus {
            services: Vec::new(),
        });
        for (name, service, flag, port) in [
            ("API port", "api", "--api-port", config.api_port),
            ("Dashboard port", "web", "--web-port", config.web_port),
        ] {
            let available = port_is_available(port);
            let service_running = deployment.service_running(service);
            checks.push(DoctorCheck {
                name: name.into(),
                level: if available || service_running {
                    CheckLevel::Pass
                } else {
                    CheckLevel::Fail
                },
                detail: if available {
                    format!("127.0.0.1:{port} is available")
                } else if service_running {
                    format!("127.0.0.1:{port} is used by the running stack")
                } else {
                    let suggestion =
                        suggest_available_port(port, &[config.api_port, config.web_port])
                            .map(|port| {
                                format!("; try `donkeyspace configure ports {flag} {port}`")
                            })
                            .unwrap_or_default();
                    format!("127.0.0.1:{port} is already in use{suggestion}")
                },
            });
        }
        if let Some(github) = &config.github {
            let result: Result<(), SetupError> = async {
                match github {
                    GitHubInstanceConfig::App {
                        app_id,
                        installation_id,
                        private_key_file,
                        repositories,
                        ..
                    } => {
                        check_secret_permissions(private_key_file)?;
                        let provider = GitHubCredentialProvider::new(GitHubAuthConfig::App {
                            app_id: *app_id,
                            installation_id: *installation_id,
                            private_key_file: private_key_file.clone(),
                        })?;
                        provider.validate_installation().await?;
                        if config.github_access.values().flatten().any(|subject| {
                            matches!(
                                subject,
                                GitHubAccessSubject::Organization { .. }
                                    | GitHubAccessSubject::Team { .. }
                            )
                        }) {
                            provider.validate_members_permission().await?;
                        }
                        for repository in repositories {
                            let (owner, repo) = repository
                                .split_once('/')
                                .expect("saved repository is valid");
                            provider.client().repository(owner, repo).await?;
                        }
                    }
                    GitHubInstanceConfig::Pat { token_file, .. } => {
                        check_secret_permissions(token_file)?
                    }
                }
                Ok(())
            }
            .await;
            checks.push(doctor_result("GitHub connection", result));
        } else {
            checks.push(DoctorCheck {
                name: "GitHub connection".into(),
                level: CheckLevel::Warning,
                detail: "not connected".into(),
            });
        }
        for repository in github_repositories(config) {
            let subjects = config
                .github_access
                .get(repository)
                .map(Vec::as_slice)
                .unwrap_or_default();
            checks.push(DoctorCheck {
                name: format!("GitHub access {repository}"),
                level: if subjects.is_empty() {
                    CheckLevel::Fail
                } else {
                    CheckLevel::Pass
                },
                detail: if subjects.is_empty() {
                    "deny all: add a trusted user, organization, or team".into()
                } else {
                    format!("{} trusted subject(s)", subjects.len())
                },
            });
        }
        checks.push(command_doctor_check(
            "Codex authentication",
            "codex",
            &["login", "status"],
        ));
        checks.push(match self.deployment_status() {
            Ok(status) => DoctorCheck {
                name: "Compose stack".into(),
                level: CheckLevel::Pass,
                detail: if status.services.is_empty() {
                    "not started".into()
                } else {
                    format!("{} service(s) discovered", status.services.len())
                },
            },
            Err(error) => DoctorCheck {
                name: "Compose stack".into(),
                level: CheckLevel::Fail,
                detail: error.to_string(),
            },
        });
        Ok(DoctorReport { checks })
    }

    pub async fn doctor(&self) -> Result<(), SetupError> {
        let report = self.doctor_report().await?;
        for check in &report.checks {
            let label = match check.level {
                CheckLevel::Pass => "pass",
                CheckLevel::Warning => "warning",
                CheckLevel::Fail => "fail",
            };
            println!("{label}: {}: {}", check.name, check.detail);
        }
        if !report.passed() {
            return Err(SetupError::Config(
                "doctor found one or more required failures".into(),
            ));
        }
        println!("doctor: all required checks passed");
        Ok(())
    }

    pub fn up(&self) -> Result<(), SetupError> {
        self.ensure_start_ports_available()?;
        self.compose(&["up", "-d", "--build"], false)?;
        self.print_endpoints()
    }

    pub fn down(&self) -> Result<(), SetupError> {
        self.compose(&["down"], false)
    }

    pub fn status(&self) -> Result<(), SetupError> {
        self.compose(&["ps"], false)?;
        self.print_endpoints()
    }

    pub fn deployment_status(&self) -> Result<DeploymentStatus, SetupError> {
        let mut command = self.compose_command(&["ps", "--format", "json"])?;
        let output = command.output()?;
        if !output.status.success() {
            return Err(SetupError::Command {
                command: "docker compose ps --format json".into(),
                detail: String::from_utf8_lossy(&output.stderr).trim().into(),
            });
        }
        parse_compose_status(&output.stdout)
    }

    pub fn start(&self) -> Result<(), SetupError> {
        self.ensure_start_ports_available()?;
        self.compose_captured(&["up", "-d", "--build"])
    }

    pub fn stop(&self) -> Result<(), SetupError> {
        self.compose_captured(&["down"])
    }

    pub fn reset(&self, delete_data: bool, confirm: bool) -> Result<(), SetupError> {
        if !delete_data {
            return Err(SetupError::Config(
                "reset requires --delete-data; ordinary `down` preserves volumes".into(),
            ));
        }
        if !confirm {
            return Err(SetupError::Config(
                "data deletion requires both --delete-data and --confirm".into(),
            ));
        }
        self.compose(&["down", "--volumes"], true)
    }

    fn ensure_start_ports_available(&self) -> Result<(), SetupError> {
        let config = self.require_config()?;
        let deployment = self.deployment_status().unwrap_or(DeploymentStatus {
            services: Vec::new(),
        });
        validate_start_ports(config, &deployment, port_is_available)
    }

    fn print_endpoints(&self) -> Result<(), SetupError> {
        println!("Dashboard: {}", self.dashboard_url()?);
        println!("API: {}", self.api_url()?);
        Ok(())
    }

    fn compose(&self, arguments: &[&str], destructive: bool) -> Result<(), SetupError> {
        let config = self.require_config()?;
        if config.github.as_ref().is_some_and(|github| {
            matches!(
                github,
                GitHubInstanceConfig::App {
                    ingress: IngressMode::Polling,
                    ..
                } | GitHubInstanceConfig::Pat {
                    ingress: IngressMode::Polling,
                    ..
                }
            )
        }) {
            eprintln!(
                "warning: GitHub ingress uses delayed repository polling; events are not real-time"
            );
        }
        let mut command = self.compose_command(arguments)?;
        if destructive {
            eprintln!("deleting Donkeyspace Compose volumes");
        }
        run_status(&mut command)
    }

    fn compose_captured(&self, arguments: &[&str]) -> Result<(), SetupError> {
        let mut command = self.compose_command(arguments)?;
        let description = describe_command(&command);
        let output = command.output()?;
        if !output.status.success() {
            let detail = String::from_utf8_lossy(&output.stderr).trim().to_string();
            return Err(SetupError::Command {
                command: description,
                detail,
            });
        }
        Ok(())
    }

    fn save_and_apply_github_access(&self) -> Result<(), SetupError> {
        self.save()?;
        let running = self
            .deployment_status()
            .map(|status| status.service_running("api"))
            .unwrap_or(false);
        if running {
            self.compose_captured(&["up", "-d", "--force-recreate", "api"])
                .map_err(|error| {
                    SetupError::Config(format!(
                        "access was saved but the API restart failed; run `donkeyspace down` then `donkeyspace up`: {error}"
                    ))
                })?;
        }
        Ok(())
    }

    fn github_credential_provider(&self) -> Result<GitHubCredentialProvider, SetupError> {
        match self.require_config()?.github.as_ref() {
            Some(GitHubInstanceConfig::App {
                app_id,
                installation_id,
                private_key_file,
                ..
            }) => Ok(GitHubCredentialProvider::new(GitHubAuthConfig::App {
                app_id: *app_id,
                installation_id: *installation_id,
                private_key_file: private_key_file.clone(),
            })?),
            Some(GitHubInstanceConfig::Pat { token_file, .. }) => {
                Ok(GitHubCredentialProvider::new(GitHubAuthConfig::Pat {
                    token: fs::read_to_string(token_file)?.trim().to_string(),
                })?)
            }
            None => Err(SetupError::Config(
                "GitHub is not connected; configure GitHub first".into(),
            )),
        }
    }

    fn compose_command(&self, arguments: &[&str]) -> Result<Command, SetupError> {
        let config = self.require_config()?;
        self.write_compose_env(config)?;
        self.write_plugin_runtime_files()?;
        let mut command = Command::new("docker");
        command
            .current_dir(&config.source_tree)
            .args(["compose", "--env-file"])
            .arg(self.directory.join(GENERATED_ENV))
            .arg("-f")
            .arg(config.source_tree.join("docker-compose.yml"));
        if config.active_plugin.is_some() {
            command.arg("-f").arg(self.plugin_overlay_path());
        }
        command.args(arguments);
        if let Some(GitHubInstanceConfig::Pat { token_file, .. }) = &config.github {
            command.env(
                "DONKEYSPACE_GITHUB_TOKEN",
                fs::read_to_string(token_file)?.trim(),
            );
        }
        Ok(command)
    }

    fn write_compose_env(&self, config: &InstanceConfig) -> Result<(), SetupError> {
        let mut lines = vec![
            format!("DONKEYSPACE_API_PORT={}", config.api_port),
            format!("DONKEYSPACE_WEB_PORT={}", config.web_port),
            format!(
                "DONKEYSPACE_POLICY_SOURCE={}",
                self.directory.join("effective-policy.yml").display()
            ),
        ];
        if let Some(codex_home) = &config.codex_home {
            lines.push(format!(
                "DONKEYSPACE_CODEX_HOME_SOURCE={}",
                codex_home.display()
            ));
        }
        if let Some(github) = &config.github {
            match github {
                GitHubInstanceConfig::App {
                    app_id,
                    installation_id,
                    private_key_file,
                    webhook_secret_file,
                    repositories,
                    ingress,
                } => {
                    lines.extend([
                        "DONKEYSPACE_GITHUB_AUTH_MODE=app".into(),
                        format!("DONKEYSPACE_GITHUB_APP_ID={app_id}"),
                        format!("DONKEYSPACE_GITHUB_INSTALLATION_ID={installation_id}"),
                        format!(
                            "DONKEYSPACE_GITHUB_PRIVATE_KEY_SOURCE={}",
                            private_key_file.display()
                        ),
                        "DONKEYSPACE_GITHUB_PRIVATE_KEY_FILE=/run/secrets/github_private_key"
                            .into(),
                        format!(
                            "DONKEYSPACE_WEBHOOK_SECRET_SOURCE={}",
                            webhook_secret_file.display()
                        ),
                        "DONKEYSPACE_WEBHOOK_SECRET_FILE=/run/secrets/github_webhook_secret".into(),
                        format!("DONKEYSPACE_GITHUB_REPOSITORIES={}", repositories.join(",")),
                        format!(
                            "DONKEYSPACE_GITHUB_POLL_REPOSITORIES={}",
                            if matches!(ingress, IngressMode::Polling) {
                                repositories.join(",")
                            } else {
                                String::new()
                            }
                        ),
                    ]);
                }
                GitHubInstanceConfig::Pat {
                    repositories,
                    ingress,
                    ..
                } => lines.extend([
                    "DONKEYSPACE_GITHUB_AUTH_MODE=pat".into(),
                    format!("DONKEYSPACE_GITHUB_REPOSITORIES={}", repositories.join(",")),
                    format!(
                        "DONKEYSPACE_GITHUB_POLL_REPOSITORIES={}",
                        if matches!(ingress, IngressMode::Polling) {
                            repositories.join(",")
                        } else {
                            String::new()
                        }
                    ),
                ]),
            }
        }
        let path = self.directory.join(GENERATED_ENV);
        write_secret(&path, format!("{}\n", lines.join("\n")).as_bytes())
    }

    fn save(&self) -> Result<(), SetupError> {
        fs::create_dir_all(&self.directory)?;
        set_directory_mode(&self.directory)?;
        let bytes = serde_json::to_vec_pretty(self.require_config()?)?;
        let temporary = self.directory.join(".instance.json.tmp");
        write_secret(&temporary, &bytes)?;
        fs::rename(temporary, self.config_path())?;
        Ok(())
    }

    fn secret_path(&self, name: &str) -> PathBuf {
        self.directory.join("secrets").join(name)
    }

    fn require_config(&self) -> Result<&InstanceConfig, SetupError> {
        self.config.as_ref().ok_or_else(|| {
            SetupError::Config("instance is not initialized; run `donkeyspace init`".into())
        })
    }
}

fn default_web_port() -> u16 {
    DEFAULT_WEB_PORT
}

fn validate_ports(api_port: u16, web_port: u16) -> Result<(), SetupError> {
    if api_port == 0 || web_port == 0 {
        return Err(SetupError::Config(
            "ports must be between 1 and 65535".into(),
        ));
    }
    if api_port == web_port {
        return Err(SetupError::Config(
            "API and dashboard ports must be different".into(),
        ));
    }
    Ok(())
}

fn port_is_available(port: u16) -> bool {
    TcpListener::bind(("127.0.0.1", port)).is_ok()
}

fn validate_start_ports<F>(
    config: &InstanceConfig,
    deployment: &DeploymentStatus,
    available: F,
) -> Result<(), SetupError>
where
    F: Fn(u16) -> bool,
{
    for (label, flag, service, port) in [
        ("API", "--api-port", "api", config.api_port),
        ("dashboard", "--web-port", "web", config.web_port),
    ] {
        if !deployment.service_running(service) && !available(port) {
            let suggestion =
                suggest_available_port_with(port, &[config.api_port, config.web_port], &available)
                    .map(|port| format!("; try `donkeyspace configure ports {flag} {port}`"))
                    .unwrap_or_default();
            return Err(SetupError::Config(format!(
                "{label} port 127.0.0.1:{port} is already in use{suggestion}"
            )));
        }
    }
    Ok(())
}

pub fn suggest_available_port(preferred: u16, excluded: &[u16]) -> Option<u16> {
    suggest_available_port_with(preferred, excluded, port_is_available)
}

fn suggest_available_port_with<F>(preferred: u16, excluded: &[u16], available: F) -> Option<u16>
where
    F: Fn(u16) -> bool,
{
    (0..=PORT_SUGGESTION_ATTEMPTS)
        .filter_map(|offset| preferred.checked_add(offset))
        .find(|port| !excluded.contains(port) && available(*port))
}

fn default_config_directory() -> PathBuf {
    if let Some(path) = env::var_os("XDG_CONFIG_HOME") {
        return PathBuf::from(path).join("donkeyspace");
    }
    env::var_os("HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("."))
        .join(".config/donkeyspace")
}

fn default_codex_home() -> PathBuf {
    env::var_os("CODEX_HOME")
        .map(PathBuf::from)
        .or_else(|| env::var_os("HOME").map(|home| PathBuf::from(home).join(".codex")))
        .unwrap_or_else(|| PathBuf::from(".codex"))
}

fn validate_owner(owner: &str) -> Result<(), SetupError> {
    if owner.is_empty()
        || !owner
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
    {
        return Err(SetupError::Config(
            "GitHub owner must contain only letters, numbers, and hyphens".into(),
        ));
    }
    Ok(())
}

fn validate_organization(organization: Option<&str>) -> Result<(), SetupError> {
    if let Some(organization) = organization {
        validate_owner(organization).map_err(|_| {
            SetupError::Config(
                "organization must contain only letters, numbers, and hyphens".into(),
            )
        })?;
    }
    Ok(())
}

fn validate_ingress(ingress: &IngressMode) -> Result<(), SetupError> {
    if let IngressMode::Webhook { public_url } = ingress
        && (!public_url.starts_with("https://") || public_url.trim() != public_url)
    {
        return Err(SetupError::Config(
            "public webhook URL must be a trimmed HTTPS URL".into(),
        ));
    }
    Ok(())
}

fn validate_repositories(repositories: &[String]) -> Result<(), SetupError> {
    if repositories.is_empty() {
        return Err(SetupError::Config(
            "select at least one repository with --repositories owner/name".into(),
        ));
    }
    for repository in repositories {
        let valid = repository.split_once('/').is_some_and(|(owner, repo)| {
            !owner.is_empty() && !repo.is_empty() && !repo.contains('/')
        });
        if !valid {
            return Err(SetupError::Config(format!(
                "invalid repository `{repository}`; expected owner/name"
            )));
        }
    }
    let owner = repositories[0].split_once('/').unwrap().0;
    if repositories.iter().any(|repository| {
        !repository
            .split_once('/')
            .unwrap()
            .0
            .eq_ignore_ascii_case(owner)
    }) {
        return Err(SetupError::Config(
            "all repositories must belong to one owner per Donkeyspace instance".into(),
        ));
    }
    Ok(())
}

fn github_repositories(config: &InstanceConfig) -> &[String] {
    match config.github.as_ref() {
        Some(GitHubInstanceConfig::App { repositories, .. })
        | Some(GitHubInstanceConfig::Pat { repositories, .. }) => repositories,
        None => &[],
    }
}

fn reconcile_github_access(config: &mut InstanceConfig) {
    let repositories = github_repositories(config).to_vec();
    config.github_access.retain(|repository, _| {
        repositories
            .iter()
            .any(|selected| selected.eq_ignore_ascii_case(repository))
    });
    for repository in repositories {
        if !config
            .github_access
            .keys()
            .any(|existing| existing.eq_ignore_ascii_case(&repository))
        {
            config.github_access.insert(repository, Vec::new());
        }
    }
}

fn configured_repository<'a>(
    config: &'a InstanceConfig,
    requested: &str,
) -> Result<&'a str, SetupError> {
    github_repositories(config)
        .iter()
        .find(|repository| repository.eq_ignore_ascii_case(requested.trim()))
        .map(String::as_str)
        .ok_or_else(|| {
            SetupError::Config(format!(
                "repository `{requested}` is not selected for this instance"
            ))
        })
}

fn validate_subject_owner(owner: &str, subject: &GitHubAccessSubject) -> Result<(), SetupError> {
    let subject_owner = match subject {
        GitHubAccessSubject::User { login } => {
            if login.trim().is_empty() {
                return Err(SetupError::Config("GitHub user cannot be empty".into()));
            }
            return Ok(());
        }
        GitHubAccessSubject::Organization { login } => login,
        GitHubAccessSubject::Team { organization, .. } => organization,
    };
    if subject_owner.eq_ignore_ascii_case(owner) {
        Ok(())
    } else {
        Err(SetupError::Config(format!(
            "organization and team access must use repository owner `{owner}`"
        )))
    }
}

fn subjects_equal(left: &GitHubAccessSubject, right: &GitHubAccessSubject) -> bool {
    left.display_name()
        .eq_ignore_ascii_case(&right.display_name())
}

fn remove_file_if_present(path: &Path) -> Result<(), SetupError> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

fn run_manifest_flow(
    port: u16,
    organization: Option<&str>,
    ingress: &IngressMode,
) -> Result<(u64, Vec<u8>, Vec<u8>), SetupError> {
    let conversion = run_manifest_registration(port, organization, ingress)?;
    read_line("Press Enter after the App installation is complete: ")?;
    Ok((
        conversion.app_id,
        conversion.private_key,
        conversion.webhook_secret,
    ))
}

fn run_manifest_registration(
    port: u16,
    organization: Option<&str>,
    ingress: &IngressMode,
) -> Result<ManifestConversion, SetupError> {
    let state = random_hex(32)?;
    let callback_url = format!("http://127.0.0.1:{port}/callback");
    let manifest = github_app_manifest(&state, &callback_url, ingress);
    let listener = TcpListener::bind(("127.0.0.1", port))?;
    let local_url = format!("http://127.0.0.1:{port}/");
    println!("GitHub App registration URL: {local_url}");
    println!(
        "Headless host: from your workstation run `ssh -N -L {port}:127.0.0.1:{port} USER@HOST`, then open the registration URL in your workstation browser."
    );
    let _ = launch_url(&local_url);
    let code = loop {
        let (mut stream, _) = listener.accept()?;
        let target = read_request_target(&mut stream)?;
        if let Some(result) = manifest_callback_code(&target, &state) {
            match result {
                Ok(code) => {
                    respond(
                        &mut stream,
                        "200 OK",
                        "GitHub App created. Return to the terminal.",
                    )?;
                    break code;
                }
                Err(error) => {
                    respond(&mut stream, "400 Bad Request", "Invalid callback")?;
                    return Err(error);
                }
            }
        } else {
            let action = match organization {
                Some(organization) => format!(
                    "https://github.com/organizations/{organization}/settings/apps/new?state={state}"
                ),
                None => format!("https://github.com/settings/apps/new?state={state}"),
            };
            let escaped = html_escape(&manifest.to_string());
            let body = format!(
                "<form id=f method=post action=\"{action}\"><input type=hidden name=manifest value=\"{escaped}\"></form><p>Redirecting to GitHub…</p><script>f.submit()</script>"
            );
            respond(&mut stream, "200 OK", &body)?;
        }
    };
    let output = Command::new("curl")
        .args(["--fail", "--silent", "--show-error", "-X", "POST"])
        .arg(format!(
            "https://api.github.com/app-manifests/{code}/conversions"
        ))
        .args([
            "-H",
            "Accept: application/vnd.github+json",
            "-H",
            "X-GitHub-Api-Version: 2022-11-28",
        ])
        .output()?;
    if !output.status.success() {
        return Err(SetupError::Command {
            command: "GitHub manifest conversion".into(),
            detail: String::from_utf8_lossy(&output.stderr).trim().into(),
        });
    }
    let conversion = parse_manifest_conversion(&output.stdout)?;
    let install_url = format!(
        "https://github.com/apps/{}/installations/new",
        conversion.slug
    );
    println!("Install the App and select repositories: {install_url}");
    let _ = launch_url(&install_url);
    Ok(conversion)
}

fn github_app_manifest(state: &str, callback_url: &str, ingress: &IngressMode) -> Value {
    let hook_attributes = match ingress {
        IngressMode::Webhook { public_url } => json!({
            "url": format!("{}/webhooks/github", public_url.trim_end_matches('/')),
            "active": true
        }),
        // GitHub's manifest schema requires a public hook URL even when
        // delivery is disabled. This documented example endpoint is never
        // used because `active` is false; polling remains the only ingress.
        IngressMode::Polling => json!({
            "url": "https://example.com/github/events",
            "active": false
        }),
    };
    json!({
        "name": format!("Donkeyspace {}", &state[..8]),
        "url": "https://github.com/DanNicolau/donkeyspace",
        "hook_attributes": hook_attributes,
        "redirect_url": callback_url,
        "public": false,
        "default_permissions": {
            "metadata": "read", "contents": "write", "issues": "write",
            "pull_requests": "write", "members": "read"
        },
        "default_events": ["issues", "issue_comment", "pull_request", "push"]
    })
}

fn manifest_callback_code(
    target: &str,
    expected_state: &str,
) -> Option<Result<String, SetupError>> {
    let query = target.strip_prefix("/callback?")?;
    let params = query_parameters(query);
    Some(
        if params.get("state").map(String::as_str) != Some(expected_state) {
            Err(SetupError::Config(
                "invalid GitHub manifest callback state".into(),
            ))
        } else {
            params
                .get("code")
                .filter(|code| !code.is_empty())
                .cloned()
                .ok_or_else(|| SetupError::Config("manifest callback omitted code".into()))
        },
    )
}

fn parse_manifest_conversion(bytes: &[u8]) -> Result<ManifestConversion, SetupError> {
    let response: Value = serde_json::from_slice(bytes)?;
    Ok(ManifestConversion {
        app_id: response["id"]
            .as_u64()
            .ok_or_else(|| SetupError::Config("manifest response omitted app id".into()))?,
        slug: response["slug"]
            .as_str()
            .filter(|slug| !slug.is_empty())
            .ok_or_else(|| SetupError::Config("manifest response omitted app slug".into()))?
            .to_owned(),
        private_key: response["pem"]
            .as_str()
            .filter(|pem| !pem.is_empty())
            .ok_or_else(|| SetupError::Config("manifest response omitted private key".into()))?
            .as_bytes()
            .to_vec(),
        webhook_secret: response["webhook_secret"]
            .as_str()
            .filter(|secret| !secret.is_empty())
            .ok_or_else(|| SetupError::Config("manifest response omitted webhook secret".into()))?
            .as_bytes()
            .to_vec(),
    })
}

fn read_request_target(stream: &mut TcpStream) -> Result<String, SetupError> {
    let mut buffer = [0_u8; 8192];
    let size = stream.read(&mut buffer)?;
    let request = String::from_utf8_lossy(&buffer[..size]);
    request
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .map(str::to_string)
        .ok_or_else(|| SetupError::Config("invalid local callback request".into()))
}

fn respond(stream: &mut TcpStream, status: &str, body: &str) -> Result<(), SetupError> {
    write!(
        stream,
        "HTTP/1.1 {status}\r\nContent-Type: text/html; charset=utf-8\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    )?;
    Ok(())
}

fn query_parameters(query: &str) -> std::collections::HashMap<String, String> {
    query
        .split('&')
        .filter_map(|pair| {
            let (key, value) = pair.split_once('=')?;
            Some((percent_decode(key), percent_decode(value)))
        })
        .collect()
}

fn percent_decode(value: &str) -> String {
    let mut output = Vec::new();
    let bytes = value.as_bytes();
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%' && index + 2 < bytes.len() {
            if let Ok(byte) = u8::from_str_radix(&value[index + 1..index + 3], 16) {
                output.push(byte);
                index += 3;
                continue;
            }
        }
        output.push(if bytes[index] == b'+' {
            b' '
        } else {
            bytes[index]
        });
        index += 1;
    }
    String::from_utf8_lossy(&output).into_owned()
}

fn html_escape(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('"', "&quot;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

fn launch_url(url: &str) -> Result<(), SetupError> {
    if !graphical_browser_available(
        cfg!(target_os = "macos"),
        env::var_os("DISPLAY").is_some(),
        env::var_os("WAYLAND_DISPLAY").is_some(),
    ) {
        return Ok(());
    }
    let command = if cfg!(target_os = "macos") {
        "open"
    } else {
        "xdg-open"
    };
    Command::new(command)
        .arg(url)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()?;
    Ok(())
}

fn graphical_browser_available(is_macos: bool, display: bool, wayland_display: bool) -> bool {
    is_macos || display || wayland_display
}

fn random_hex(bytes: usize) -> Result<String, SetupError> {
    let mut data = vec![0_u8; bytes];
    fs::File::open("/dev/urandom")?.read_exact(&mut data)?;
    Ok(data.iter().map(|byte| format!("{byte:02x}")).collect())
}

fn read_line(prompt: &str) -> Result<String, SetupError> {
    eprint!("{prompt}");
    std::io::stderr().flush()?;
    let mut value = String::new();
    std::io::stdin().read_line(&mut value)?;
    Ok(value)
}

fn read_secret(prompt: &str) -> Result<String, SetupError> {
    let _ = Command::new("stty").arg("-echo").status();
    let result = read_line(prompt);
    let _ = Command::new("stty").arg("echo").status();
    eprintln!();
    result
}

fn run_status(command: &mut Command) -> Result<(), SetupError> {
    // `Debug` for Command includes environment overrides. That would expose a
    // legacy PAT or another credential when a subprocess fails.
    let description = describe_command(command);
    let status = command.status()?;
    if !status.success() {
        return Err(SetupError::Command {
            command: description,
            detail: status.to_string(),
        });
    }
    Ok(())
}

fn describe_command(command: &Command) -> String {
    std::iter::once(command.get_program())
        .chain(command.get_args())
        .map(|value| value.to_string_lossy())
        .collect::<Vec<_>>()
        .join(" ")
}

fn command_doctor_check(name: &str, program: &str, args: &[&str]) -> DoctorCheck {
    doctor_result(name, check_command(program, args))
}

fn doctor_result(name: &str, result: Result<(), SetupError>) -> DoctorCheck {
    match result {
        Ok(()) => DoctorCheck {
            name: name.into(),
            level: CheckLevel::Pass,
            detail: "available".into(),
        },
        Err(error) => DoctorCheck {
            name: name.into(),
            level: CheckLevel::Fail,
            detail: error.to_string(),
        },
    }
}

fn parse_compose_status(bytes: &[u8]) -> Result<DeploymentStatus, SetupError> {
    let text = String::from_utf8_lossy(bytes);
    if text.trim().is_empty() {
        return Ok(DeploymentStatus {
            services: Vec::new(),
        });
    }
    let values = match serde_json::from_str::<Value>(&text) {
        Ok(Value::Array(values)) => values,
        Ok(value @ Value::Object(_)) => vec![value],
        Ok(_) => {
            return Err(SetupError::Config(
                "docker compose status was not JSON objects".into(),
            ));
        }
        Err(error) if error.classify() == serde_json::error::Category::Syntax => text
            .lines()
            .filter(|line| !line.trim().is_empty())
            .map(serde_json::from_str::<Value>)
            .collect::<Result<Vec<_>, _>>()?,
        Err(error) => return Err(error.into()),
    };
    if values.iter().any(|value| !value.is_object()) {
        return Err(SetupError::Config(
            "docker compose status was not JSON objects".into(),
        ));
    }
    let mut services = values
        .iter()
        .filter_map(|value| {
            let name = value["Service"].as_str()?.to_string();
            let state = value["State"].as_str().unwrap_or("unknown").to_string();
            let health = value["Health"]
                .as_str()
                .filter(|health| !health.is_empty())
                .map(str::to_string);
            Some(ServiceStatus {
                name,
                state,
                health,
            })
        })
        .collect::<Vec<_>>();
    services.sort_by(|left, right| left.name.cmp(&right.name));
    Ok(DeploymentStatus { services })
}

fn check_command(program: &str, args: &[&str]) -> Result<(), SetupError> {
    let output = Command::new(program).args(args).output()?;
    if !output.status.success() {
        return Err(SetupError::Command {
            command: format!("{program} {}", args.join(" ")),
            detail: String::from_utf8_lossy(&output.stderr).trim().into(),
        });
    }
    Ok(())
}

fn write_secret(path: &Path, bytes: &[u8]) -> Result<(), SetupError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
        set_directory_mode(parent)?;
    }
    fs::write(path, bytes)?;
    set_file_mode(path)?;
    Ok(())
}

#[cfg(unix)]
fn set_file_mode(path: &Path) -> Result<(), SetupError> {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
    Ok(())
}

#[cfg(not(unix))]
fn set_file_mode(_: &Path) -> Result<(), SetupError> {
    Ok(())
}

#[cfg(unix)]
fn set_directory_mode(path: &Path) -> Result<(), SetupError> {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(path, fs::Permissions::from_mode(0o700))?;
    Ok(())
}

#[cfg(not(unix))]
fn set_directory_mode(_: &Path) -> Result<(), SetupError> {
    Ok(())
}

#[cfg(unix)]
fn check_secret_permissions(path: &Path) -> Result<(), SetupError> {
    use std::os::unix::fs::PermissionsExt;
    let mode = fs::metadata(path)?.permissions().mode() & 0o777;
    if mode != 0o600 {
        return Err(SetupError::Config(format!(
            "secret `{}` has mode {mode:o}; expected 600",
            path.display()
        )));
    }
    Ok(())
}

#[cfg(not(unix))]
fn check_secret_permissions(path: &Path) -> Result<(), SetupError> {
    if path.is_file() {
        Ok(())
    } else {
        Err(SetupError::Config(format!(
            "secret `{}` is missing",
            path.display()
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn rejects_invalid_repository_names() {
        assert!(validate_repositories(&["owner/repo/extra".into()]).is_err());
        assert!(validate_repositories(&[]).is_err());
        assert!(validate_repositories(&["owner/repo".into()]).is_ok());
        assert!(validate_repositories(&["owner/one".into(), "other/two".into()]).is_err());
    }

    #[test]
    fn callback_query_decoding_is_exact() {
        let values = query_parameters("state=abc%20123&code=hello%2Fworld");
        assert_eq!(values["state"], "abc 123");
        assert_eq!(values["code"], "hello/world");
    }

    #[test]
    fn manifest_callback_rejects_wrong_state_and_missing_code() {
        assert!(manifest_callback_code("/", "expected").is_none());
        let wrong_state = manifest_callback_code("/callback?state=wrong&code=abc", "expected")
            .unwrap()
            .unwrap_err();
        assert!(wrong_state.to_string().contains("state"));
        let missing_code = manifest_callback_code("/callback?state=expected", "expected")
            .unwrap()
            .unwrap_err();
        assert!(missing_code.to_string().contains("omitted code"));
    }

    #[test]
    fn polling_manifest_uses_inactive_public_placeholder_hook() {
        let manifest = github_app_manifest(
            "0123456789abcdef",
            "http://127.0.0.1:8787/callback",
            &IngressMode::Polling,
        );
        assert_eq!(manifest["hook_attributes"]["active"], false);
        assert_eq!(
            manifest["hook_attributes"]["url"],
            "https://example.com/github/events"
        );
        assert_eq!(manifest["redirect_url"], "http://127.0.0.1:8787/callback");
        assert_eq!(manifest["default_permissions"]["members"], "read");
    }

    #[test]
    fn webhook_manifest_uses_active_public_https_hook() {
        let manifest = github_app_manifest(
            "0123456789abcdef",
            "http://127.0.0.1:8787/callback",
            &IngressMode::Webhook {
                public_url: "https://donkeyspace.example/".into(),
            },
        );
        assert_eq!(manifest["hook_attributes"]["active"], true);
        assert_eq!(
            manifest["hook_attributes"]["url"],
            "https://donkeyspace.example/webhooks/github"
        );
    }

    #[test]
    fn browser_launch_is_skipped_on_headless_linux() {
        assert!(!graphical_browser_available(false, false, false));
        assert!(graphical_browser_available(false, true, false));
        assert!(graphical_browser_available(false, false, true));
        assert!(graphical_browser_available(true, false, false));
    }

    #[test]
    fn parses_mocked_manifest_conversion_without_logging_secrets() {
        let conversion = parse_manifest_conversion(
            br#"{"id":42,"slug":"donkeyspace-test","pem":"private-key-value","webhook_secret":"hook-value"}"#,
        )
        .unwrap();
        assert_eq!(conversion.app_id, 42);
        assert_eq!(conversion.slug, "donkeyspace-test");
        assert_eq!(conversion.private_key, b"private-key-value");
        assert_eq!(conversion.webhook_secret, b"hook-value");
    }

    #[test]
    fn rejects_incomplete_manifest_conversion() {
        for response in [
            br#"{"slug":"app","pem":"key","webhook_secret":"secret"}"#.as_slice(),
            br#"{"id":1,"pem":"key","webhook_secret":"secret"}"#.as_slice(),
            br#"{"id":1,"slug":"app","webhook_secret":"secret"}"#.as_slice(),
            br#"{"id":1,"slug":"app","pem":"key"}"#.as_slice(),
        ] {
            assert!(parse_manifest_conversion(response).is_err());
        }
    }

    #[test]
    fn instance_config_never_serializes_secret_values() {
        let config = InstanceConfig {
            schema_version: SCHEMA_VERSION,
            source_tree: "/src".into(),
            runtime_source: RuntimeSource::LocalBuild,
            api_port: 8080,
            web_port: 5173,
            codex_home: None,
            github: Some(GitHubInstanceConfig::Pat {
                token_file: "/config/secrets/github-pat".into(),
                repositories: vec!["owner/repo".into()],
                ingress: IngressMode::Polling,
            }),
            github_access: BTreeMap::from([("owner/repo".into(), Vec::new())]),
            plugins: BTreeMap::new(),
            active_plugin: None,
        };
        let serialized = serde_json::to_string(&config).unwrap();
        assert!(!serialized.contains("ghp_"));
        assert!(serialized.contains("github-pat"));
    }

    #[test]
    fn migrates_legacy_configurations_atomically() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        for schema_version in [1, 2, 3] {
            let directory =
                env::temp_dir().join(format!("donkeyspace-schema-test-{unique}-{schema_version}"));
            fs::create_dir_all(&directory).unwrap();
            fs::write(
                directory.join(CONFIG_FILE),
                format!(
                    r#"{{
                      "schema_version": {schema_version},
                      "source_tree": "/src",
                      "runtime_source": "local-build",
                      "api_port": 8080
                    }}"#
                ),
            )
            .unwrap();
            let instance = Instance::open(Some(directory.clone())).unwrap();
            let config = instance.config().unwrap();
            assert_eq!(config.schema_version, SCHEMA_VERSION);
            assert_eq!(config.web_port, 5173);
            assert!(config.plugins.is_empty());
            assert!(config.active_plugin.is_none());
            assert!(config.github_access.is_empty());
            let saved: Value =
                serde_json::from_slice(&fs::read(directory.join(CONFIG_FILE)).unwrap()).unwrap();
            assert_eq!(saved["schema_version"], SCHEMA_VERSION);
            assert_eq!(saved["web_port"], 5173);
            assert!(!directory.join(".instance.json.tmp").exists());
            fs::remove_dir_all(directory).unwrap();
        }
    }

    #[test]
    fn validates_and_suggests_distinct_ports() {
        assert!(validate_ports(8080, 5173).is_ok());
        assert!(validate_ports(0, 5173).is_err());
        assert!(validate_ports(8080, 8080).is_err());

        let suggestion =
            suggest_available_port_with(20_000, &[20_001], |port| port >= 20_001).unwrap();
        assert_eq!(suggestion, 20_002);
    }

    #[test]
    fn repository_access_is_reconciled_and_owner_scoped() {
        let mut config = InstanceConfig {
            schema_version: SCHEMA_VERSION,
            source_tree: "/src".into(),
            runtime_source: RuntimeSource::LocalBuild,
            api_port: 8080,
            web_port: 5173,
            codex_home: None,
            github: Some(GitHubInstanceConfig::Pat {
                token_file: "/secret".into(),
                repositories: vec!["acme/rtl".into(), "acme/dv".into()],
                ingress: IngressMode::Polling,
            }),
            github_access: BTreeMap::from([(
                "old/repo".into(),
                vec![GitHubAccessSubject::User {
                    login: "stale".into(),
                }],
            )]),
            plugins: BTreeMap::new(),
            active_plugin: None,
        };
        reconcile_github_access(&mut config);
        assert_eq!(config.github_access.len(), 2);
        assert!(config.github_access["acme/rtl"].is_empty());
        assert!(config.github_access["acme/dv"].is_empty());
        assert!(
            validate_subject_owner(
                "acme",
                &GitHubAccessSubject::Team {
                    organization: "other".into(),
                    team_slug: "maintainers".into(),
                },
            )
            .is_err()
        );
    }

    #[test]
    fn startup_port_validation_distinguishes_stack_services_from_conflicts() {
        let config = InstanceConfig {
            schema_version: SCHEMA_VERSION,
            source_tree: "/src".into(),
            runtime_source: RuntimeSource::LocalBuild,
            api_port: 8080,
            web_port: 5173,
            codex_home: None,
            github: None,
            github_access: BTreeMap::new(),
            plugins: BTreeMap::new(),
            active_plugin: None,
        };
        let stopped = DeploymentStatus { services: vec![] };
        let error = validate_start_ports(&config, &stopped, |port| port == 8081).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("donkeyspace configure ports --api-port 8081")
        );

        let partial = DeploymentStatus {
            services: vec![ServiceStatus {
                name: "api".into(),
                state: "running".into(),
                health: None,
            }],
        };
        assert!(validate_start_ports(&config, &partial, |port| port == 5173).is_ok());
    }

    #[test]
    fn compose_environment_contains_both_host_ports() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let directory = env::temp_dir().join(format!("donkeyspace-port-env-test-{unique}"));
        let instance = Instance {
            directory: directory.clone(),
            config: Some(InstanceConfig {
                schema_version: SCHEMA_VERSION,
                source_tree: "/src".into(),
                runtime_source: RuntimeSource::LocalBuild,
                api_port: 18_080,
                web_port: 15_173,
                codex_home: None,
                github: None,
                github_access: BTreeMap::new(),
                plugins: BTreeMap::new(),
                active_plugin: None,
            }),
        };
        fs::create_dir_all(&directory).unwrap();
        instance
            .write_compose_env(instance.config().unwrap())
            .unwrap();
        let environment = fs::read_to_string(directory.join(GENERATED_ENV)).unwrap();
        assert!(environment.contains("DONKEYSPACE_API_PORT=18080\n"));
        assert!(environment.contains("DONKEYSPACE_WEB_PORT=15173\n"));
        assert!(environment.contains("DONKEYSPACE_POLICY_SOURCE="));
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn failed_command_description_redacts_environment_values() {
        let mut command = Command::new("docker");
        command
            .args(["compose", "ps"])
            .env("DONKEYSPACE_GITHUB_TOKEN", "ghp_do_not_log");
        let description = describe_command(&command);
        assert_eq!(description, "docker compose ps");
        assert!(!description.contains("ghp_do_not_log"));
    }

    #[test]
    fn parses_structured_compose_status() {
        let status = parse_compose_status(
            br#"[{"Service":"worker","State":"running","Health":""},{"Service":"postgres","State":"running","Health":"healthy"}]"#,
        )
        .unwrap();
        assert!(!status.running());
        assert_eq!(status.services[0].name, "postgres");
        assert_eq!(status.services[0].health.as_deref(), Some("healthy"));
        assert_eq!(status.services[1].name, "worker");
        assert_eq!(status.services[1].health, None);
    }

    #[test]
    fn parses_newline_delimited_compose_status() {
        let status = parse_compose_status(
            b"{\"Service\":\"worker\",\"State\":\"running\",\"Health\":\"\"}\n{\"Service\":\"postgres\",\"State\":\"running\",\"Health\":\"healthy\"}\n",
        )
        .unwrap();
        assert_eq!(status.services.len(), 2);
        assert_eq!(status.services[0].name, "postgres");
        assert_eq!(status.services[1].name, "worker");
    }

    #[test]
    fn deployment_is_running_only_when_every_core_service_is_running() {
        let status = DeploymentStatus {
            services: ["postgres", "api", "worker", "web"]
                .into_iter()
                .map(|name| ServiceStatus {
                    name: name.into(),
                    state: "running".into(),
                    health: None,
                })
                .collect(),
        };
        assert!(status.running());
    }

    #[test]
    fn doctor_report_only_fails_on_required_checks() {
        let report = DoctorReport {
            checks: vec![
                DoctorCheck {
                    name: "polling".into(),
                    level: CheckLevel::Warning,
                    detail: "delayed".into(),
                },
                DoctorCheck {
                    name: "docker".into(),
                    level: CheckLevel::Pass,
                    detail: "available".into(),
                },
            ],
        };
        assert!(report.passed());
    }

    #[test]
    fn validates_webhook_ingress_and_owner() {
        assert!(validate_owner("valid-owner").is_ok());
        assert!(validate_owner("invalid/owner").is_err());
        assert!(
            validate_ingress(&IngressMode::Webhook {
                public_url: "https://example.test".into()
            })
            .is_ok()
        );
        assert!(
            validate_ingress(&IngressMode::Webhook {
                public_url: "http://example.test".into()
            })
            .is_err()
        );
    }

    #[test]
    fn init_is_resumable_and_secret_files_are_private() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let directory = env::temp_dir().join(format!("donkeyspace-cli-test-{unique}"));
        let source_tree = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .unwrap()
            .parent()
            .unwrap()
            .to_path_buf();
        let mut instance = Instance::open(Some(directory.clone())).unwrap();
        instance
            .init_with_ports(
                source_tree.clone(),
                RuntimeSource::LocalBuild,
                Some(18_080),
                Some(15_173),
            )
            .unwrap();
        instance.config.as_mut().unwrap().codex_home = Some("/tmp/codex-test".into());
        instance.save().unwrap();
        instance
            .init(source_tree, RuntimeSource::LocalBuild)
            .unwrap();
        assert_eq!(instance.config.as_ref().unwrap().api_port, 18_080);
        assert_eq!(instance.config.as_ref().unwrap().web_port, 15_173);
        let replacement_web_port = 15_174;
        instance
            .configure_ports_with_availability(None, Some(replacement_web_port), |_| true)
            .unwrap();
        assert_eq!(instance.config.as_ref().unwrap().api_port, 18_080);
        assert_eq!(
            instance.config.as_ref().unwrap().web_port,
            replacement_web_port
        );
        assert!(instance.configure_ports(None, None).is_err());
        assert_eq!(
            instance.config.as_ref().unwrap().codex_home.as_deref(),
            Some(Path::new("/tmp/codex-test"))
        );

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                fs::metadata(instance.config_path())
                    .unwrap()
                    .permissions()
                    .mode()
                    & 0o777,
                0o600
            );
            assert_eq!(
                fs::metadata(&directory).unwrap().permissions().mode() & 0o777,
                0o700
            );
        }

        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn pending_github_registration_resumes_and_discards_privately() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let directory = env::temp_dir().join(format!("donkeyspace-pending-test-{unique}"));
        let private_key_file = directory.join("secrets/pending.pem");
        let webhook_secret_file = directory.join("secrets/pending-webhook");
        write_secret(&private_key_file, b"fixture key").unwrap();
        write_secret(&webhook_secret_file, b"fixture webhook").unwrap();
        let pending = PendingGitHubApp {
            app_id: 42,
            slug: "donkeyspace-test".into(),
            owner: "example".into(),
            ingress: IngressMode::Polling,
            callback_port: 8787,
            organization: None,
            private_key_file: private_key_file.clone(),
            webhook_secret_file: webhook_secret_file.clone(),
        };
        write_secret(
            &directory.join(PENDING_GITHUB_FILE),
            &serde_json::to_vec(&pending).unwrap(),
        )
        .unwrap();

        let instance = Instance::open(Some(directory.clone())).unwrap();
        assert_eq!(instance.pending_github_app().unwrap(), Some(pending));
        instance.discard_pending_github_app().unwrap();
        assert!(instance.pending_github_app().unwrap().is_none());
        assert!(!private_key_file.exists());
        assert!(!webhook_secret_file.exists());

        fs::remove_dir_all(directory).unwrap();
    }
}
