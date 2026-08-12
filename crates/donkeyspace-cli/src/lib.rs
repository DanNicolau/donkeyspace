use donkeyspace_github::{
    GitHubAuthConfig, GitHubCredentialProvider, GitHubRepository, discover_installation_id,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    env, fs,
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    path::{Path, PathBuf},
    process::{Command, Stdio},
};
use thiserror::Error;

pub mod tui;

const SCHEMA_VERSION: u32 = 1;
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub codex_home: Option<PathBuf>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub github: Option<GitHubInstanceConfig>,
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
        let config = if path.exists() {
            let config: InstanceConfig = serde_json::from_slice(&fs::read(&path)?)?;
            if config.schema_version != SCHEMA_VERSION {
                return Err(SetupError::Config(format!(
                    "unsupported schema_version {}; expected {SCHEMA_VERSION}",
                    config.schema_version
                )));
            }
            Some(config)
        } else {
            None
        };
        Ok(Self { directory, config })
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
        if runtime_source == RuntimeSource::RegistryImage {
            return Err(SetupError::Config(
                "registry-image is reserved for a future release backend".into(),
            ));
        }
        fs::create_dir_all(&self.directory)?;
        set_directory_mode(&self.directory)?;
        let source_tree = fs::canonicalize(source_tree)?;
        let config = match self.config.take() {
            Some(mut existing) => {
                existing.source_tree = source_tree;
                existing.runtime_source = runtime_source;
                existing
            }
            None => InstanceConfig {
                schema_version: SCHEMA_VERSION,
                source_tree,
                runtime_source,
                api_port: 8080,
                codex_home: None,
                github: None,
            },
        };
        self.config = Some(config);
        self.save()
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
        let deployment = self.deployment_status().unwrap_or(DeploymentStatus {
            services: Vec::new(),
        });
        for (name, port) in [("API port", config.api_port), ("Dashboard port", 5173)] {
            let available = std::net::TcpListener::bind(("127.0.0.1", port)).is_ok();
            checks.push(DoctorCheck {
                name: name.into(),
                level: if available || deployment.running() {
                    CheckLevel::Pass
                } else {
                    CheckLevel::Warning
                },
                detail: if available {
                    format!("127.0.0.1:{port} is available")
                } else if deployment.running() {
                    format!("127.0.0.1:{port} is used by the running stack")
                } else {
                    format!("127.0.0.1:{port} is already in use")
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
        self.compose(&["up", "-d", "--build"], false)
    }

    pub fn down(&self) -> Result<(), SetupError> {
        self.compose(&["down"], false)
    }

    pub fn status(&self) -> Result<(), SetupError> {
        self.compose(&["ps"], false)
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

    fn compose_command(&self, arguments: &[&str]) -> Result<Command, SetupError> {
        let config = self.require_config()?;
        self.write_compose_env(config)?;
        let mut command = Command::new("docker");
        command
            .current_dir(&config.source_tree)
            .args(["compose", "--env-file"])
            .arg(self.directory.join(GENERATED_ENV))
            .args(arguments);
        if let Some(GitHubInstanceConfig::Pat { token_file, .. }) = &config.github {
            command.env(
                "DONKEYSPACE_GITHUB_TOKEN",
                fs::read_to_string(token_file)?.trim(),
            );
        }
        Ok(command)
    }

    fn write_compose_env(&self, config: &InstanceConfig) -> Result<(), SetupError> {
        let mut lines = vec![format!("DONKEYSPACE_API_PORT={}", config.api_port)];
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
        write_secret(&self.config_path(), &bytes)
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
    println!("Open {local_url} to create the private GitHub App.");
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
            "metadata": "read", "contents": "write", "issues": "write", "pull_requests": "write"
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
    let values = match serde_json::from_str::<Value>(&text)? {
        Value::Array(values) => values,
        value @ Value::Object(_) => vec![value],
        _ => {
            return Err(SetupError::Config(
                "docker compose status was not JSON objects".into(),
            ));
        }
    };
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
            schema_version: 1,
            source_tree: "/src".into(),
            runtime_source: RuntimeSource::LocalBuild,
            api_port: 8080,
            codex_home: None,
            github: Some(GitHubInstanceConfig::Pat {
                token_file: "/config/secrets/github-pat".into(),
                repositories: vec!["owner/repo".into()],
                ingress: IngressMode::Polling,
            }),
        };
        let serialized = serde_json::to_string(&config).unwrap();
        assert!(!serialized.contains("ghp_"));
        assert!(serialized.contains("github-pat"));
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
            .init(source_tree.clone(), RuntimeSource::LocalBuild)
            .unwrap();
        instance.config.as_mut().unwrap().codex_home = Some("/tmp/codex-test".into());
        instance.save().unwrap();
        instance
            .init(source_tree, RuntimeSource::LocalBuild)
            .unwrap();
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
