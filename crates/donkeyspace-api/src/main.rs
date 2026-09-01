use axum::{
    Json, Router,
    body::Bytes,
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    routing::{get, post},
};
use chrono::{DateTime, Utc};
use donkeyspace_core::{
    AgentRole, DeploymentMode, EngagementGate, EngagementSelector, LabelState, PluginManifest,
    Policy, WorkflowState, normalize_workflow_labels,
};
use donkeyspace_db::{
    DbConfig, EngagementDecisionInput, JobRecord, LifecycleEventInput, PgPool, PullRequestInput,
    RepositoryInput, WorkflowItemInput, acquire_job_lease, active_job_exists_for_workflow_item,
    apply_migrations, connect, create_job, create_retry_job, get_job, get_workflow_by_issue,
    get_workflow_item_by_issue_number, get_workflow_item_state, github_ingress_delivery_stats,
    github_managed_resource_exists, latest_workflow_job_input, list_agent_publications_for_run,
    list_job_command_results, list_job_outbound_actions, list_job_transitions, list_jobs,
    list_jobs_for_repository, list_jobs_for_workflow_item, list_lifecycle_events,
    list_open_managed_pull_requests_for_base, list_recent_engagement_decisions,
    list_recent_outbound_actions, list_recent_outbound_actions_for_repository, list_workflows,
    pending_outbound_comment_exists, record_engagement_decision, record_lifecycle_event,
    record_state_transition, record_webhook_delivery, repair_job_exists_for_pr_base,
    resume_latest_paused_job, retry_agent_publication, reviewer_job_exists_for_pr_head,
    upsert_pull_request, upsert_repository, upsert_workflow_item, webhook_delivery_exists,
};
use donkeyspace_github::{
    GitHubAuthMode, GitHubClient, GitHubClientError, GitHubCredentialProvider,
};
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, HashMap},
    env, fs,
    net::SocketAddr,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::{Mutex, Notify};
use tower_http::trace::TraceLayer;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};
use uuid::Uuid;

#[derive(Debug, Clone)]
struct AppState {
    configuration: EffectiveConfigurationResponse,
    webhook_secret: Option<String>,
    configured_ingress_mode: String,
    github_auth: Option<GitHubCredentialProvider>,
    pool: Option<PgPool>,
    policy: Policy,
    github_token_owner: Option<String>,
    configured_repositories: Vec<PolledRepository>,
    verification_cache: Arc<Mutex<HashMap<String, (Instant, String)>>>,
    github_poller: GitHubPollController,
}

#[derive(Debug, Clone, Serialize)]
struct HealthResponse {
    status: &'static str,
    service: &'static str,
    deployment_mode: DeploymentMode,
    capabilities: Vec<String>,
    warnings: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
struct FacadeResponse {
    display_name: String,
    tagline: String,
    issue_command: String,
    branch_prefix: String,
}

#[derive(Debug, Clone, Serialize)]
struct EffectiveConfigurationResponse {
    deployment_mode: DeploymentMode,
    policy_source: String,
    facade: FacadeResponse,
    github: EffectiveGitHubConfiguration,
    plugin: Option<EffectivePluginConfiguration>,
    capabilities: Vec<String>,
    warnings: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
struct EffectiveGitHubConfiguration {
    auth_mode: &'static str,
    ingress_mode: String,
    repositories: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
struct EffectivePluginConfiguration {
    id: String,
    flow: String,
}

struct EffectiveConfigurationInput<'a> {
    policy_source: &'a str,
    github_auth: Option<&'a GitHubCredentialProvider>,
    ingress_mode: &'a str,
    configured_repositories: &'a [PolledRepository],
    polling_repositories: &'a [PolledRepository],
    webhook_enabled: bool,
}

#[derive(Debug, Deserialize)]
struct RepositoryQuery {
    repository: Option<String>,
}

#[derive(Debug, Deserialize)]
struct WorkflowEventsQuery {
    before_id: Option<i64>,
    #[serde(default = "default_event_level")]
    level: String,
    limit: Option<i64>,
}

fn default_event_level() -> String {
    "milestone".into()
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PolledRepository {
    owner: String,
    name: String,
}

#[derive(Debug, Clone)]
struct GitHubPollConfig {
    repositories: Vec<PolledRepository>,
    interval: Duration,
    max_pages: usize,
}

const GITHUB_POLL_REPOSITORY_TIMEOUT: Duration = Duration::from_secs(60);
const GITHUB_POLL_MAX_BACKOFF: Duration = Duration::from_secs(300);

#[derive(Debug, Clone)]
struct GitHubPollController {
    config: GitHubPollConfig,
    runtime: Arc<Mutex<GitHubPollRuntime>>,
    notify: Arc<Notify>,
}

#[derive(Debug)]
struct GitHubPollRuntime {
    running: bool,
    pending_manual: bool,
    last_started_at: Option<DateTime<Utc>>,
    last_completed_at: Option<DateTime<Utc>>,
    last_success_at: Option<DateTime<Utc>>,
    last_error: Option<String>,
    repositories: BTreeMap<String, GitHubRepositoryPollRuntime>,
}

#[derive(Debug)]
struct GitHubRepositoryPollRuntime {
    etag: Option<String>,
    server_interval: Option<Duration>,
    next_eligible: Instant,
    next_eligible_at: DateTime<Utc>,
    last_polled_at: Option<DateTime<Utc>>,
    last_success_at: Option<DateTime<Utc>>,
    last_error: Option<String>,
    consecutive_failures: u32,
}

#[derive(Debug, Serialize)]
struct GitHubPollStatusResponse {
    enabled: bool,
    running: bool,
    pending_manual: bool,
    configured_interval_seconds: u64,
    last_started_at: Option<DateTime<Utc>>,
    last_completed_at: Option<DateTime<Utc>>,
    last_success_at: Option<DateTime<Utc>>,
    last_error: Option<String>,
    consecutive_failures: u32,
    next_poll_at: Option<DateTime<Utc>>,
    repositories: Vec<GitHubRepositoryPollStatus>,
}

#[derive(Debug, Serialize)]
struct GitHubRepositoryPollStatus {
    full_name: String,
    server_interval_seconds: Option<u64>,
    next_eligible_at: DateTime<Utc>,
    last_polled_at: Option<DateTime<Utc>>,
    last_success_at: Option<DateTime<Utc>>,
    last_error: Option<String>,
    consecutive_failures: u32,
}

#[derive(Debug, Serialize)]
struct GitHubPollTriggerResponse {
    status: &'static str,
    already_pending: bool,
}

const REQUIRED_GITHUB_WEBHOOK_EVENTS: [&str; 4] =
    ["issue_comment", "issues", "pull_request", "push"];

#[derive(Debug, Serialize)]
struct GitHubIngressStatusResponse {
    configured_mode: String,
    webhook: GitHubWebhookStatusResponse,
    polling: GitHubPollStatusResponse,
    poll_deliveries: GitHubPollDeliveryStatusResponse,
}

#[derive(Debug, Serialize)]
struct GitHubWebhookStatusResponse {
    endpoint_enabled: bool,
    last_received_at: Option<DateTime<Utc>>,
    last_event: Option<String>,
    last_delivery_id: Option<String>,
    deliveries_24h: i64,
    app: Option<GitHubAppWebhookStatusResponse>,
    app_error: Option<String>,
}

#[derive(Debug, Serialize)]
struct GitHubAppWebhookStatusResponse {
    url: Option<String>,
    content_type: Option<String>,
    subscribed_events: Vec<String>,
    missing_events: Vec<String>,
    deliveries: Vec<donkeyspace_github::GitHubAppWebhookDelivery>,
}

#[derive(Debug, Serialize)]
struct GitHubPollDeliveryStatusResponse {
    last_received_at: Option<DateTime<Utc>>,
    last_event: Option<String>,
    deliveries_24h: i64,
}

#[derive(Debug)]
struct GitHubIngressEvent {
    event_name: &'static str,
    delivery_id: String,
    payload: Value,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(tag = "action", rename_all = "snake_case")]
enum HumanApprovalAction {
    Approve {
        target: Option<String>,
    },
    Revise {
        target: Option<String>,
        feedback: String,
    },
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    init_tracing();

    let deployment_mode = DeploymentMode::from_environment()?;
    let policy = load_policy()?;
    let _ = lifecycle_start_role(&policy)?;
    let database_url = env::var("DONKEYSPACE_DATABASE_URL").ok();
    let pool = if let Some(database_url) = database_url {
        let pool = connect(&DbConfig::from_database_url(database_url)).await?;
        apply_migrations(&pool).await?;
        tracing::info!("database connection verified");
        Some(pool)
    } else {
        tracing::warn!("DONKEYSPACE_DATABASE_URL is unset; starting without database check");
        None
    };

    let github_auth = GitHubCredentialProvider::from_env()?;
    let github_token_owner = match github_auth.as_ref() {
        Some(provider) if provider.installation_id().is_none() => {
            Some(provider.client().authenticated_login().await?)
        }
        _ => None,
    };
    let configured_repositories = parse_polled_repositories(
        &env::var("DONKEYSPACE_GITHUB_REPOSITORIES").unwrap_or_default(),
    )?;
    let github_poller = GitHubPollController::new(GitHubPollConfig::from_env()?);
    let webhook_secret = load_optional_secret(
        "DONKEYSPACE_WEBHOOK_SECRET",
        "DONKEYSPACE_WEBHOOK_SECRET_FILE",
    )?;
    let configured_ingress_mode = configured_ingress_mode(
        !github_poller.config.repositories.is_empty(),
        webhook_secret.is_some(),
    )?;
    let policy_source = env::var("DONKEYSPACE_POLICY_PATH")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .ok_or("DONKEYSPACE_POLICY_PATH must name the active policy file")?;
    let configuration = effective_configuration(
        deployment_mode,
        &policy,
        EffectiveConfigurationInput {
            policy_source: &policy_source,
            github_auth: github_auth.as_ref(),
            ingress_mode: &configured_ingress_mode,
            configured_repositories: &configured_repositories,
            polling_repositories: &github_poller.config.repositories,
            webhook_enabled: webhook_secret.is_some(),
        },
    )?;
    tracing::info!(
        deployment_mode = %configuration.deployment_mode,
        github_auth = configuration.github.auth_mode,
        github_ingress = %configuration.github.ingress_mode,
        repositories = configuration.github.repositories.len(),
        plugin = configuration.plugin.as_ref().map(|plugin| plugin.id.as_str()).unwrap_or("disabled"),
        "effective runtime configuration validated"
    );
    let state = Arc::new(AppState {
        configuration,
        webhook_secret,
        configured_ingress_mode,
        github_auth,
        pool,
        policy,
        github_token_owner,
        configured_repositories,
        verification_cache: Arc::new(Mutex::new(HashMap::new())),
        github_poller,
    });

    start_github_poller(state.clone())?;

    let app = Router::new()
        .route("/healthz", get(healthz))
        .route("/readyz", get(healthz))
        .route("/api/configuration", get(api_configuration))
        .route("/api/facade", get(api_facade))
        .route("/api/repositories", get(api_repositories))
        .route("/api/runs", get(api_runs))
        .route("/api/workflows", get(api_workflows))
        .route(
            "/api/workflows/{owner}/{repo}/issues/{number}",
            get(api_workflow),
        )
        .route(
            "/api/workflows/{owner}/{repo}/issues/{number}/events",
            get(api_workflow_events),
        )
        .route("/api/outbound-actions", get(api_outbound_actions))
        .route("/api/engagement-decisions", get(api_engagement_decisions))
        .route("/api/github-poll/status", get(api_github_poll_status))
        .route("/api/github-poll/trigger", post(api_github_poll_trigger))
        .route("/api/github-ingress/status", get(api_github_ingress_status))
        .route("/api/runs/{id}", get(api_run))
        .route("/api/runs/{id}/transitions", get(api_run_transitions))
        .route("/api/runs/{id}/lease", post(api_lease_run))
        .route("/api/runs/{id}/retry", post(api_retry_run))
        .route("/api/publications/{id}/retry", post(api_retry_publication))
        .route("/webhooks/github", post(github_webhook))
        .layer(TraceLayer::new_for_http())
        .with_state(state);

    let addr: SocketAddr = env::var("DONKEYSPACE_BIND_ADDR")
        .unwrap_or_else(|_| "0.0.0.0:8080".to_string())
        .parse()?;
    let listener = tokio::net::TcpListener::bind(addr).await?;

    tracing::info!(%addr, "donkeyspace api listening");
    axum::serve(listener, app).await?;
    Ok(())
}

fn start_github_poller(state: Arc<AppState>) -> Result<(), Box<dyn std::error::Error>> {
    let config = state.github_poller.config.clone();
    if config.repositories.is_empty() {
        return Ok(());
    }
    if state.pool.is_none() {
        return Err("github polling requires DONKEYSPACE_DATABASE_URL".into());
    }
    let client = state
        .github_auth
        .as_ref()
        .ok_or("github polling requires configured GitHub authentication")?
        .client();

    tracing::info!(
        repositories = config.repositories.len(),
        interval_seconds = config.interval.as_secs(),
        max_pages = config.max_pages,
        "github event poller enabled"
    );
    let controller = state.github_poller.clone();
    tokio::spawn(async move {
        loop {
            let child_state = state.clone();
            let child_client = client.clone();
            let child_controller = controller.clone();
            let task = tokio::spawn(async move {
                run_github_poller(child_state, child_client, child_controller).await
            });
            match task.await {
                Ok(()) => tracing::error!("github event poller exited unexpectedly"),
                Err(error) => tracing::error!(%error, "github event poller crashed"),
            }
            {
                let mut runtime = controller.runtime.lock().await;
                runtime.running = false;
                runtime.last_error = Some("polling task restarted after an unexpected exit".into());
            }
            tokio::time::sleep(config.interval).await;
        }
    });
    Ok(())
}

impl GitHubPollConfig {
    fn from_env() -> Result<Self, Box<dyn std::error::Error>> {
        let repositories = parse_polled_repositories(
            &env::var("DONKEYSPACE_GITHUB_POLL_REPOSITORIES").unwrap_or_default(),
        )?;
        let interval_seconds = env::var("DONKEYSPACE_GITHUB_POLL_INTERVAL_SECONDS")
            .unwrap_or_else(|_| "60".to_string())
            .parse::<u64>()?;
        if !(5..=3600).contains(&interval_seconds) {
            return Err(
                "DONKEYSPACE_GITHUB_POLL_INTERVAL_SECONDS must be between 5 and 3600".into(),
            );
        }
        let max_pages = env::var("DONKEYSPACE_GITHUB_POLL_MAX_PAGES")
            .unwrap_or_else(|_| "2".to_string())
            .parse::<usize>()?
            .max(1);
        Ok(Self {
            repositories,
            interval: Duration::from_secs(interval_seconds),
            max_pages,
        })
    }
}

impl GitHubPollController {
    fn new(config: GitHubPollConfig) -> Self {
        let now = Utc::now();
        let repositories = config
            .repositories
            .iter()
            .map(|repository| {
                (
                    repository.full_name(),
                    GitHubRepositoryPollRuntime {
                        etag: None,
                        server_interval: None,
                        next_eligible: Instant::now(),
                        next_eligible_at: now,
                        last_polled_at: None,
                        last_success_at: None,
                        last_error: None,
                        consecutive_failures: 0,
                    },
                )
            })
            .collect();
        Self {
            config,
            runtime: Arc::new(Mutex::new(GitHubPollRuntime {
                running: false,
                pending_manual: false,
                last_started_at: None,
                last_completed_at: None,
                last_success_at: None,
                last_error: None,
                repositories,
            })),
            notify: Arc::new(Notify::new()),
        }
    }

    async fn status(&self) -> GitHubPollStatusResponse {
        let runtime = self.runtime.lock().await;
        let repositories = runtime
            .repositories
            .iter()
            .map(|(full_name, repository)| GitHubRepositoryPollStatus {
                full_name: full_name.clone(),
                server_interval_seconds: repository.server_interval.map(|value| value.as_secs()),
                next_eligible_at: repository.next_eligible_at,
                last_polled_at: repository.last_polled_at,
                last_success_at: repository.last_success_at,
                last_error: repository.last_error.clone(),
                consecutive_failures: repository.consecutive_failures,
            })
            .collect::<Vec<_>>();
        GitHubPollStatusResponse {
            enabled: !self.config.repositories.is_empty(),
            running: runtime.running,
            pending_manual: runtime.pending_manual,
            configured_interval_seconds: self.config.interval.as_secs(),
            last_started_at: runtime.last_started_at,
            last_completed_at: runtime.last_completed_at,
            last_success_at: runtime.last_success_at,
            last_error: runtime.last_error.clone(),
            consecutive_failures: runtime
                .repositories
                .values()
                .map(|repository| repository.consecutive_failures)
                .max()
                .unwrap_or(0),
            next_poll_at: repositories
                .iter()
                .map(|repository| repository.next_eligible_at)
                .min(),
            repositories,
        }
    }

    async fn trigger(&self) -> bool {
        let already_pending = {
            let mut runtime = self.runtime.lock().await;
            let already_pending = runtime.pending_manual;
            runtime.pending_manual = true;
            already_pending
        };
        self.notify.notify_one();
        already_pending
    }
}

impl PolledRepository {
    fn full_name(&self) -> String {
        format!("{}/{}", self.owner, self.name)
    }
}

async fn api_github_poll_status(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    Json(state.github_poller.status().await)
}

async fn api_github_ingress_status(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let Some(pool) = &state.pool else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!(ApiError::new("database is not configured"))),
        )
            .into_response();
    };
    let stats = match github_ingress_delivery_stats(pool).await {
        Ok(stats) => stats,
        Err(error) => {
            tracing::error!(%error, "failed to load github ingress delivery statistics");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!(ApiError::new("failed to load github ingress status"))),
            )
                .into_response();
        }
    };
    let (app, app_error) = match &state.github_auth {
        Some(provider) => match provider.app_webhook_status().await {
            Ok(status) => (status.map(github_app_webhook_status_response), None),
            Err(error) => {
                tracing::warn!(%error, "failed to inspect github app webhook status");
                (None, Some(error.to_string()))
            }
        },
        None => (None, None),
    };
    Json(GitHubIngressStatusResponse {
        configured_mode: state.configured_ingress_mode.clone(),
        webhook: GitHubWebhookStatusResponse {
            endpoint_enabled: state.webhook_secret.is_some(),
            last_received_at: stats.webhook_last_received_at,
            last_event: stats.webhook_last_event,
            last_delivery_id: stats.webhook_last_delivery_id,
            deliveries_24h: stats.webhook_deliveries_24h,
            app,
            app_error,
        },
        polling: state.github_poller.status().await,
        poll_deliveries: GitHubPollDeliveryStatusResponse {
            last_received_at: stats.poll_last_received_at,
            last_event: stats.poll_last_event,
            deliveries_24h: stats.poll_deliveries_24h,
        },
    })
    .into_response()
}

fn github_app_webhook_status_response(
    status: donkeyspace_github::GitHubAppWebhookStatus,
) -> GitHubAppWebhookStatusResponse {
    let missing_events = REQUIRED_GITHUB_WEBHOOK_EVENTS
        .iter()
        .filter(|required| {
            !status
                .subscribed_events
                .iter()
                .any(|event| event == **required)
        })
        .map(|event| (*event).to_string())
        .collect();
    GitHubAppWebhookStatusResponse {
        url: status.url,
        content_type: status.content_type,
        subscribed_events: status.subscribed_events,
        missing_events,
        deliveries: status.deliveries,
    }
}

async fn api_github_poll_trigger(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    if state.github_poller.config.repositories.is_empty() {
        return (
            StatusCode::CONFLICT,
            Json(json!(ApiError::new("github polling is disabled"))),
        );
    }
    let already_pending = state.github_poller.trigger().await;
    (
        StatusCode::ACCEPTED,
        Json(json!(GitHubPollTriggerResponse {
            status: "requested",
            already_pending,
        })),
    )
}

async fn run_github_poller(
    state: Arc<AppState>,
    client: GitHubClient,
    controller: GitHubPollController,
) {
    loop {
        let wait = {
            let runtime = controller.runtime.lock().await;
            runtime
                .repositories
                .values()
                .map(|repository| {
                    repository
                        .next_eligible
                        .saturating_duration_since(Instant::now())
                })
                .min()
                .unwrap_or(controller.config.interval)
        };
        if !wait.is_zero() {
            tokio::select! {
                () = tokio::time::sleep(wait) => {}
                () = controller.notify.notified() => {}
            }
        }

        let eligible = {
            let mut runtime = controller.runtime.lock().await;
            let now = Instant::now();
            let eligible = controller
                .config
                .repositories
                .iter()
                .filter(|repository| {
                    runtime.repositories[&repository.full_name()].next_eligible <= now
                })
                .cloned()
                .collect::<Vec<_>>();
            if !eligible.is_empty() {
                runtime.running = true;
                runtime.pending_manual = false;
                runtime.last_started_at = Some(Utc::now());
            }
            eligible
        };
        if eligible.is_empty() {
            continue;
        }

        let mut cycle_errors = Vec::new();
        for repository in eligible {
            let full_name = repository.full_name();
            let etag = {
                controller.runtime.lock().await.repositories[&full_name]
                    .etag
                    .clone()
            };
            let result = tokio::time::timeout(
                GITHUB_POLL_REPOSITORY_TIMEOUT,
                poll_github_repository(
                    &state,
                    &client,
                    &controller.config,
                    &repository,
                    etag.as_deref(),
                ),
            )
            .await;
            let completed_at = Utc::now();
            let mut runtime = controller.runtime.lock().await;
            let repository_runtime = runtime
                .repositories
                .get_mut(&full_name)
                .expect("configured polling repository has runtime state");
            repository_runtime.last_polled_at = Some(completed_at);
            match result {
                Ok(Ok(outcome)) => {
                    repository_runtime.etag = outcome.etag;
                    repository_runtime.server_interval = outcome
                        .server_interval
                        .or(repository_runtime.server_interval);
                    repository_runtime.last_success_at = Some(completed_at);
                    repository_runtime.last_error = None;
                    repository_runtime.consecutive_failures = 0;
                    let delay = repository_runtime
                        .server_interval
                        .unwrap_or_default()
                        .max(controller.config.interval);
                    schedule_repository(repository_runtime, delay, completed_at);
                }
                Ok(Err(error)) => {
                    let error_text = error.to_string();
                    repository_runtime.consecutive_failures =
                        repository_runtime.consecutive_failures.saturating_add(1);
                    repository_runtime.last_error = Some(error_text.clone());
                    let delay = error
                        .downcast_ref::<GitHubClientError>()
                        .and_then(GitHubClientError::retry_after_seconds)
                        .map(Duration::from_secs)
                        .unwrap_or_else(|| {
                            poll_backoff(
                                controller.config.interval,
                                repository_runtime.consecutive_failures,
                            )
                        })
                        .max(repository_runtime.server_interval.unwrap_or_default());
                    schedule_repository(repository_runtime, delay, completed_at);
                    cycle_errors.push(format!("{full_name}: {error_text}"));
                }
                Err(_) => {
                    repository_runtime.consecutive_failures =
                        repository_runtime.consecutive_failures.saturating_add(1);
                    let error_text = format!(
                        "repository poll exceeded {} seconds",
                        GITHUB_POLL_REPOSITORY_TIMEOUT.as_secs()
                    );
                    repository_runtime.last_error = Some(error_text.clone());
                    let delay = poll_backoff(
                        controller.config.interval,
                        repository_runtime.consecutive_failures,
                    )
                    .max(repository_runtime.server_interval.unwrap_or_default());
                    schedule_repository(repository_runtime, delay, completed_at);
                    cycle_errors.push(format!("{full_name}: {error_text}"));
                }
            }
        }
        let mut runtime = controller.runtime.lock().await;
        runtime.running = false;
        runtime.last_completed_at = Some(Utc::now());
        if cycle_errors.is_empty() {
            runtime.last_success_at = Some(Utc::now());
        }
        runtime.last_error = (!cycle_errors.is_empty()).then(|| cycle_errors.join("; "));
        if let Some(error) = &runtime.last_error {
            tracing::error!(%error, "github event poll cycle failed");
        }
    }
}

fn schedule_repository(
    repository: &mut GitHubRepositoryPollRuntime,
    delay: Duration,
    completed_at: DateTime<Utc>,
) {
    repository.next_eligible = Instant::now() + delay;
    repository.next_eligible_at = completed_at
        + chrono::Duration::from_std(delay).unwrap_or_else(|_| chrono::Duration::hours(1));
}

fn poll_backoff(interval: Duration, consecutive_failures: u32) -> Duration {
    let exponent = consecutive_failures.saturating_sub(1).min(10);
    interval
        .saturating_mul(2_u32.saturating_pow(exponent))
        .min(GITHUB_POLL_MAX_BACKOFF)
}

struct GitHubRepositoryPollOutcome {
    etag: Option<String>,
    server_interval: Option<Duration>,
}

fn parse_polled_repositories(value: &str) -> Result<Vec<PolledRepository>, String> {
    value
        .split(',')
        .map(str::trim)
        .filter(|entry| !entry.is_empty())
        .map(|entry| {
            let (owner, name) = entry.split_once('/').ok_or_else(|| {
                format!("invalid github polling repository `{entry}`; expected owner/name")
            })?;
            if owner.is_empty() || name.is_empty() || name.contains('/') {
                return Err(format!(
                    "invalid github polling repository `{entry}`; expected owner/name"
                ));
            }
            Ok(PolledRepository {
                owner: owner.to_string(),
                name: name.to_string(),
            })
        })
        .collect()
}

fn parse_repository_query(value: Option<&str>) -> Result<Option<(String, String)>, String> {
    let Some(value) = value.map(str::trim).filter(|value| !value.is_empty()) else {
        return Ok(None);
    };
    let repositories = parse_polled_repositories(value)?;
    if repositories.len() != 1 {
        return Err("repository filter must select exactly one owner/name".into());
    }
    let repository = repositories
        .into_iter()
        .next()
        .expect("checked one repository");
    Ok(Some((repository.owner, repository.name)))
}

async fn poll_github_repository(
    state: &AppState,
    client: &GitHubClient,
    config: &GitHubPollConfig,
    repository: &PolledRepository,
    etag: Option<&str>,
) -> Result<GitHubRepositoryPollOutcome, Box<dyn std::error::Error + Send + Sync>> {
    let pool = state.pool.as_ref().ok_or("database is not configured")?;
    let repository_payload = client
        .repository(&repository.owner, &repository.name)
        .await?;
    let repository_input = polled_repository_input(
        &repository_payload,
        state
            .github_auth
            .as_ref()
            .and_then(GitHubCredentialProvider::installation_id),
    )?;
    let repository_id = upsert_repository(pool, &repository_input).await?;
    tracing::debug!(
        repository_id,
        repository = format_args!("{}/{}", repository_input.owner, repository_input.name),
        "registered polled github repository"
    );
    let poll = client
        .repository_events_conditional(&repository.owner, &repository.name, config.max_pages, etag)
        .await?;
    let outcome = GitHubRepositoryPollOutcome {
        etag: poll.etag,
        server_interval: poll.poll_interval_seconds.map(Duration::from_secs),
    };
    let mut events = poll.events;
    events.reverse();

    for mut event in events {
        let event_id = event
            .get("id")
            .and_then(Value::as_str)
            .ok_or("github polling event is missing its id")?;
        let delivery_id = format!(
            "github-poll:{}/{}:{event_id}",
            repository.owner, repository.name
        );
        if webhook_delivery_exists(pool, &delivery_id).await? {
            continue;
        }
        if event.get("type").and_then(Value::as_str) == Some("PullRequestEvent") {
            let pull_request_number = event
                .pointer("/payload/number")
                .and_then(Value::as_u64)
                .ok_or("github pull request event is missing its number")?;
            let pull_request = client
                .pull_request(&repository.owner, &repository.name, pull_request_number)
                .await?;
            event["payload"]["pull_request"] = pull_request;
        }
        let Some(mut ingress) = github_poll_event_to_ingress(
            &repository.owner,
            &repository.name,
            &repository_payload,
            &event,
        ) else {
            continue;
        };
        if let Some(installation_id) = state
            .github_auth
            .as_ref()
            .and_then(GitHubCredentialProvider::installation_id)
            && let Value::Object(payload) = &mut ingress.payload
        {
            payload.insert("installation".into(), json!({"id": installation_id}));
        }
        let body = serde_json::to_vec(&ingress.payload)?;
        match persist_github_webhook(pool, state, ingress.event_name, &ingress.delivery_id, &body)
            .await
            .map_err(|error| std::io::Error::other(error.to_string()))?
        {
            WebhookPersistOutcome::Queued(job) => tracing::info!(
                job_id = %job.id,
                event = ingress.event_name,
                delivery = ingress.delivery_id,
                "queued github polling job"
            ),
            WebhookPersistOutcome::Ignored | WebhookPersistOutcome::Duplicate => {}
        }
    }
    Ok(outcome)
}

fn polled_repository_input(
    repository: &Value,
    installation_id: Option<u64>,
) -> Result<RepositoryInput, String> {
    let owner = repository
        .pointer("/owner/login")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or("polled github repository omitted owner.login")?
        .to_string();
    let name = repository
        .get("name")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or("polled github repository omitted name")?
        .to_string();
    let default_branch = repository
        .get("default_branch")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or("polled github repository omitted default_branch")?
        .to_string();

    Ok(RepositoryInput {
        installation_external_id: installation_id.map(|value| value.to_string()),
        installation_account_login: installation_id.map(|_| owner.clone()),
        provider: "github".to_string(),
        owner,
        name,
        default_branch,
    })
}

fn github_poll_event_to_ingress(
    owner: &str,
    repo: &str,
    repository: &Value,
    event: &Value,
) -> Option<GitHubIngressEvent> {
    let event_id = event.get("id")?.as_str()?;
    let event_type = event.get("type")?.as_str()?;
    let source = event.get("payload")?;
    let repository = json!({
        "name": repository.get("name")?,
        "default_branch": repository.get("default_branch")?,
        "owner": {
            "login": repository.pointer("/owner/login")?,
            "type": repository.pointer("/owner/type").cloned().unwrap_or(Value::Null),
        },
    });
    let sender = polled_event_sender(event)?;
    let (event_name, payload) = match event_type {
        "IssuesEvent" => (
            "issues",
            json!({
                "action": source.get("action")?,
                "repository": repository,
                "issue": source.get("issue")?,
                "label": source.get("label").cloned().unwrap_or(Value::Null),
                "sender": sender,
            }),
        ),
        "IssueCommentEvent" => (
            "issue_comment",
            json!({
                "action": source.get("action")?,
                "repository": repository,
                "issue": source.get("issue")?,
                "comment": source.get("comment")?,
                "sender": sender,
            }),
        ),
        "PullRequestEvent" => (
            "pull_request",
            json!({
                "action": source.get("action")?,
                "repository": repository,
                "pull_request": source.get("pull_request")?,
            }),
        ),
        "PushEvent" => (
            "push",
            json!({
                "ref": source.get("ref")?,
                "after": source.get("head")?,
                "repository": repository,
            }),
        ),
        _ => return None,
    };

    Some(GitHubIngressEvent {
        event_name,
        delivery_id: format!("github-poll:{owner}/{repo}:{event_id}"),
        payload,
    })
}

fn polled_event_sender(event: &Value) -> Option<Value> {
    let mut sender = event.get("actor")?.clone();
    if sender.get("type").and_then(Value::as_str).is_some() {
        return Some(sender);
    }

    let matching_content_actor = [
        event.pointer("/payload/comment/user"),
        event.pointer("/payload/issue/user"),
        event.pointer("/payload/pull_request/user"),
    ]
    .into_iter()
    .flatten()
    .find(|candidate| github_identities_match(&sender, candidate));

    if let Some(kind) = matching_content_actor
        .and_then(|actor| actor.get("type"))
        .and_then(Value::as_str)
        && let Value::Object(sender) = &mut sender
    {
        sender.insert("type".into(), Value::String(kind.to_string()));
    }

    Some(sender)
}

fn github_identities_match(left: &Value, right: &Value) -> bool {
    match (
        left.get("id").and_then(Value::as_u64),
        right.get("id").and_then(Value::as_u64),
    ) {
        (Some(left), Some(right)) => left == right,
        _ => left
            .get("login")
            .and_then(Value::as_str)
            .zip(right.get("login").and_then(Value::as_str))
            .is_some_and(|(left, right)| left.eq_ignore_ascii_case(right)),
    }
}

fn configured_ingress_mode(polling_enabled: bool, webhook_enabled: bool) -> Result<String, String> {
    match env::var("DONKEYSPACE_GITHUB_INGRESS_MODE") {
        Ok(value)
            if matches!(
                value.as_str(),
                "disabled" | "polling" | "webhook" | "hybrid"
            ) =>
        {
            Ok(value)
        }
        Ok(value) if !value.trim().is_empty() => Err(format!(
            "DONKEYSPACE_GITHUB_INGRESS_MODE must be `disabled`, `polling`, `webhook`, or `hybrid`, got `{value}`"
        )),
        _ if polling_enabled && webhook_enabled => Ok("hybrid".into()),
        _ if polling_enabled => Ok("polling".into()),
        _ if webhook_enabled => Ok("webhook".into()),
        _ => Ok("disabled".into()),
    }
}

fn effective_configuration(
    deployment_mode: DeploymentMode,
    policy: &Policy,
    input: EffectiveConfigurationInput<'_>,
) -> Result<EffectiveConfigurationResponse, String> {
    let mut repositories = input
        .configured_repositories
        .iter()
        .map(PolledRepository::full_name)
        .collect::<Vec<_>>();
    repositories.sort_unstable_by_key(|repository| repository.to_ascii_lowercase());
    repositories.dedup_by(|left, right| left.eq_ignore_ascii_case(right));
    let mut polled = input
        .polling_repositories
        .iter()
        .map(PolledRepository::full_name)
        .collect::<Vec<_>>();
    polled.sort_unstable_by_key(|repository| repository.to_ascii_lowercase());
    polled.dedup_by(|left, right| left.eq_ignore_ascii_case(right));

    let plugin_selection = policy.lifecycle.plugin.as_ref().or(policy
        .agents
        .developer
        .plugin
        .as_ref());
    if deployment_mode == DeploymentMode::Minimal
        && (input.github_auth.is_some()
            || input.ingress_mode != "disabled"
            || !repositories.is_empty()
            || !polled.is_empty()
            || input.webhook_enabled
            || plugin_selection.is_some())
    {
        return Err(
            "minimal deployment mode cannot enable GitHub or plugin configuration; use the generated deployment entry point"
                .into(),
        );
    }

    if input.github_auth.is_none()
        && (input.ingress_mode != "disabled" || !repositories.is_empty() || !polled.is_empty())
    {
        return Err(
            "GitHub ingress or repositories were selected without GitHub authentication".into(),
        );
    }
    if input.github_auth.is_some() && repositories.is_empty() {
        return Err(
            "GitHub authentication is configured but DONKEYSPACE_GITHUB_REPOSITORIES is empty"
                .into(),
        );
    }
    if input.github_auth.is_some() && input.ingress_mode == "disabled" {
        return Err("GitHub authentication is configured but GitHub ingress is disabled".into());
    }
    if matches!(input.ingress_mode, "polling" | "hybrid") && polled != repositories {
        return Err(
            "polling ingress requires DONKEYSPACE_GITHUB_POLL_REPOSITORIES to match DONKEYSPACE_GITHUB_REPOSITORIES"
                .into(),
        );
    }
    if input.ingress_mode == "webhook" && !polled.is_empty() {
        return Err("webhook ingress cannot configure polling repositories".into());
    }
    if matches!(input.ingress_mode, "webhook" | "hybrid") && !input.webhook_enabled {
        return Err("webhook ingress requires a configured webhook secret".into());
    }

    let auth_mode = match input.github_auth.map(GitHubCredentialProvider::mode) {
        Some(GitHubAuthMode::App) => "app",
        Some(GitHubAuthMode::Pat) => "pat",
        None => "disabled",
    };
    let plugin = plugin_selection
        .map(|selection| {
            let manifest = PluginManifest::from_path(&selection.manifest_path)
                .map_err(|error| format!("configured plugin manifest is unavailable: {error}"))?;
            Ok::<_, String>(EffectivePluginConfiguration {
                id: manifest.id,
                flow: selection.flow.clone(),
            })
        })
        .transpose()?;
    let mut capabilities = vec!["api".into(), "dashboard".into()];
    if input.github_auth.is_some() {
        capabilities.push("github".into());
    }
    if input.ingress_mode == "polling" || input.ingress_mode == "hybrid" {
        capabilities.push("github_polling".into());
    }
    if input.ingress_mode == "webhook" || input.ingress_mode == "hybrid" {
        capabilities.push("github_webhooks".into());
    }
    if plugin.is_some() {
        capabilities.push("plugin".into());
    }
    let warnings = if deployment_mode == DeploymentMode::Minimal {
        vec!["Intentional minimal mode: GitHub ingress and plugins are disabled.".into()]
    } else if input.github_auth.is_none() {
        vec!["GitHub is not connected; repository automation is disabled.".into()]
    } else {
        Vec::new()
    };
    let facade = policy.facade.resolve();
    let issue_command = facade.issue_command();
    Ok(EffectiveConfigurationResponse {
        deployment_mode,
        policy_source: input.policy_source.into(),
        facade: FacadeResponse {
            display_name: facade.display_name,
            tagline: facade.tagline,
            issue_command,
            branch_prefix: facade.branch_prefix,
        },
        github: EffectiveGitHubConfiguration {
            auth_mode,
            ingress_mode: input.ingress_mode.into(),
            repositories,
        },
        plugin,
        capabilities,
        warnings,
    })
}

async fn healthz(State(state): State<Arc<AppState>>) -> Json<HealthResponse> {
    Json(HealthResponse {
        status: if state.configuration.warnings.is_empty() {
            "ok"
        } else {
            "degraded"
        },
        service: "donkeyspace-api",
        deployment_mode: state.configuration.deployment_mode,
        capabilities: state.configuration.capabilities.clone(),
        warnings: state.configuration.warnings.clone(),
    })
}

async fn api_configuration(
    State(state): State<Arc<AppState>>,
) -> Json<EffectiveConfigurationResponse> {
    Json(state.configuration.clone())
}

async fn api_facade(State(state): State<Arc<AppState>>) -> Json<FacadeResponse> {
    let facade = state.policy.facade.resolve();
    let issue_command = facade.issue_command();
    Json(FacadeResponse {
        display_name: facade.display_name,
        tagline: facade.tagline,
        issue_command,
        branch_prefix: facade.branch_prefix,
    })
}

async fn api_repositories(State(state): State<Arc<AppState>>) -> Json<Vec<String>> {
    let mut repositories = state
        .configured_repositories
        .iter()
        .map(PolledRepository::full_name)
        .collect::<Vec<_>>();
    repositories.sort_unstable_by_key(|repository| repository.to_ascii_lowercase());
    repositories.dedup_by(|left, right| left.eq_ignore_ascii_case(right));
    Json(repositories)
}

async fn api_runs(
    State(state): State<Arc<AppState>>,
    Query(query): Query<RepositoryQuery>,
) -> impl IntoResponse {
    let Some(pool) = &state.pool else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ApiError::new("database is not configured")),
        )
            .into_response();
    };

    let repository = match parse_repository_query(query.repository.as_deref()) {
        Ok(repository) => repository,
        Err(error) => return (StatusCode::BAD_REQUEST, Json(ApiError::new(error))).into_response(),
    };
    let jobs = match repository {
        Some((owner, repo)) => list_jobs_for_repository(pool, &owner, &repo, 50).await,
        None => list_jobs(pool, 50).await,
    };
    match jobs {
        Ok(jobs) => Json(jobs).into_response(),
        Err(error) => {
            tracing::error!(%error, "failed to list jobs");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiError::new("failed to list runs")),
            )
                .into_response()
        }
    }
}

async fn api_workflows(
    State(state): State<Arc<AppState>>,
    Query(query): Query<RepositoryQuery>,
) -> impl IntoResponse {
    let Some(pool) = &state.pool else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ApiError::new("database is not configured")),
        )
            .into_response();
    };
    let repository = match parse_repository_query(query.repository.as_deref()) {
        Ok(repository) => repository,
        Err(error) => return (StatusCode::BAD_REQUEST, Json(ApiError::new(error))).into_response(),
    };
    let workflows = match list_workflows(
        pool,
        repository
            .as_ref()
            .map(|(owner, repo)| (owner.as_str(), repo.as_str())),
        50,
    )
    .await
    {
        Ok(workflows) => workflows,
        Err(error) => {
            tracing::error!(%error, "failed to list workflows");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiError::new("failed to list workflows")),
            )
                .into_response();
        }
    };
    let mut summaries = Vec::with_capacity(workflows.len());
    for workflow in workflows {
        match workflow_summary(pool, workflow).await {
            Ok(summary) => summaries.push(summary),
            Err(error) => {
                tracing::error!(%error, "failed to build workflow summary");
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ApiError::new("failed to build workflow summary")),
                )
                    .into_response();
            }
        }
    }
    Json(summaries).into_response()
}

async fn api_workflow(
    State(state): State<Arc<AppState>>,
    Path((owner, repo, number)): Path<(String, String, i64)>,
) -> impl IntoResponse {
    let Some(pool) = &state.pool else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ApiError::new("database is not configured")),
        )
            .into_response();
    };
    let Some(workflow) = (match get_workflow_by_issue(pool, &owner, &repo, number).await {
        Ok(workflow) => workflow,
        Err(error) => {
            tracing::error!(%error, "failed to fetch workflow");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiError::new("failed to fetch workflow")),
            )
                .into_response();
        }
    }) else {
        return (
            StatusCode::NOT_FOUND,
            Json(ApiError::new("workflow not found")),
        )
            .into_response();
    };
    match workflow_summary(pool, workflow).await {
        Ok(summary) => Json(summary).into_response(),
        Err(error) => {
            tracing::error!(%error, "failed to build workflow detail");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiError::new("failed to build workflow detail")),
            )
                .into_response()
        }
    }
}

async fn api_workflow_events(
    State(state): State<Arc<AppState>>,
    Path((owner, repo, number)): Path<(String, String, i64)>,
    Query(query): Query<WorkflowEventsQuery>,
) -> impl IntoResponse {
    let Some(pool) = &state.pool else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ApiError::new("database is not configured")),
        )
            .into_response();
    };
    if !matches!(query.level.as_str(), "milestone" | "all") {
        return (
            StatusCode::BAD_REQUEST,
            Json(ApiError::new("event level must be milestone or all")),
        )
            .into_response();
    }
    let workflow = match get_workflow_by_issue(pool, &owner, &repo, number).await {
        Ok(Some(workflow)) => workflow,
        Ok(None) => {
            return (
                StatusCode::NOT_FOUND,
                Json(ApiError::new("workflow not found")),
            )
                .into_response();
        }
        Err(error) => {
            tracing::error!(%error, "failed to resolve workflow timeline");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiError::new("failed to resolve workflow timeline")),
            )
                .into_response();
        }
    };
    let limit = query.limit.unwrap_or(100).clamp(1, 200);
    match list_lifecycle_events(
        pool,
        workflow.id,
        query.before_id,
        query.level == "milestone",
        limit,
    )
    .await
    {
        Ok(events) => {
            let next_before_id = (events.len() as i64 == limit)
                .then(|| events.last().map(|event| event.id))
                .flatten();
            Json(LifecycleEventPage {
                events,
                next_before_id,
            })
            .into_response()
        }
        Err(error) => {
            tracing::error!(%error, "failed to fetch workflow timeline");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiError::new("failed to fetch workflow timeline")),
            )
                .into_response()
        }
    }
}

async fn workflow_summary(
    pool: &PgPool,
    workflow: donkeyspace_db::WorkflowOverviewRecord,
) -> Result<WorkflowSummary, donkeyspace_db::DbError> {
    let jobs = list_jobs_for_workflow_item(pool, workflow.id).await?;
    let coordinator_id = workflow.latest_job_id;
    let publication_pr_url = if let Some(coordinator_id) = coordinator_id {
        list_agent_publications_for_run(pool, coordinator_id, None)
            .await?
            .into_iter()
            .find_map(|publication| {
                publication
                    .metadata
                    .get("pull_request_url")
                    .and_then(Value::as_str)
                    .map(str::to_owned)
            })
    } else {
        None
    };
    let pull_request_url = workflow.pull_request_url.clone().or(publication_pr_url);
    let mut latest = BTreeMap::<(String, String), &JobRecord>::new();
    for job in &jobs {
        if job
            .input
            .pointer("/plugin_execution/coordinator_run_id")
            .and_then(Value::as_str)
            != coordinator_id.map(|id| id.to_string()).as_deref()
        {
            continue;
        }
        let Some(task) = job
            .input
            .pointer("/plugin_execution/task")
            .and_then(Value::as_str)
        else {
            continue;
        };
        let work_item = job
            .input
            .pointer("/plugin_execution/work_item/id")
            .and_then(Value::as_str)
            .unwrap_or("");
        latest.insert((task.into(), work_item.into()), job);
    }
    let tasks = latest
        .into_values()
        .map(|job| WorkflowTaskSummary {
            job_id: job.id,
            role: job.role.clone(),
            role_display_name: job
                .input
                .pointer("/plugin_execution/role_display_name")
                .and_then(Value::as_str)
                .unwrap_or(&job.role)
                .into(),
            task: job
                .input
                .pointer("/plugin_execution/task")
                .and_then(Value::as_str)
                .unwrap_or(&job.role)
                .into(),
            task_display_name: job
                .input
                .pointer("/plugin_execution/task_display_name")
                .and_then(Value::as_str)
                .or_else(|| {
                    job.input
                        .pointer("/plugin_execution/task")
                        .and_then(Value::as_str)
                })
                .unwrap_or(&job.role)
                .into(),
            work_item: job
                .input
                .pointer("/plugin_execution/work_item/id")
                .and_then(Value::as_str)
                .map(Into::into),
            status: job.status.clone(),
            outcome: job
                .result
                .as_ref()
                .and_then(|result| result.get("outcome"))
                .and_then(Value::as_str)
                .map(Into::into),
            summary: job
                .result
                .as_ref()
                .and_then(|result| result.get("summary"))
                .and_then(Value::as_str)
                .map(Into::into),
            updated_at: job.updated_at,
        })
        .collect::<Vec<_>>();
    let pending_approval: Option<String> = jobs
        .iter()
        .filter(|job| Some(job.id) == coordinator_id)
        .filter_map(|job| job.result.as_ref())
        .find_map(|result| {
            result
                .get("human_review_reason")
                .and_then(Value::as_str)
                .map(str::to_owned)
        });
    let no_pr_reason = if pull_request_url.is_some() {
        None
    } else if let Some(reason) = &pending_approval {
        Some(first_paragraph(reason))
    } else if let Some(task) = tasks.iter().find(|task| task.status == "running") {
        Some(format!("Waiting for {} to finish.", task.task_display_name))
    } else if let Some(task) = tasks.iter().find(|task| task.status == "waiting") {
        Some(format!(
            "{} is waiting for its dependencies.",
            task.task_display_name
        ))
    } else if workflow.current_state.as_deref() == Some("blocked") {
        Some(
            workflow
                .latest_summary
                .clone()
                .unwrap_or_else(|| "The workflow is blocked.".into()),
        )
    } else {
        Some("The workflow has not completed all required work yet.".into())
    };
    Ok(WorkflowSummary {
        id: workflow.id,
        owner: workflow.owner.clone(),
        repository: workflow.repository.clone(),
        issue_number: workflow.issue_number,
        issue_title: workflow
            .issue_title
            .unwrap_or_else(|| "Untitled issue".into()),
        issue_url: format!(
            "https://github.com/{}/{}/issues/{}",
            workflow.owner, workflow.repository, workflow.issue_number
        ),
        provider_state: workflow.provider_state,
        current_state: workflow.current_state,
        current_labels: workflow.current_labels,
        coordinator_job_id: coordinator_id,
        coordinator_status: workflow.latest_job_status,
        outcome: workflow.latest_outcome,
        summary: workflow.latest_summary,
        pending_approval,
        tasks,
        pull_request_number: workflow.pull_request_number,
        pull_request_url,
        pull_request_state: workflow.pull_request_state,
        no_pr_reason,
        updated_at: workflow.updated_at,
        created_at: workflow.created_at,
    })
}

fn first_paragraph(value: &str) -> String {
    value
        .split("\n\n")
        .next()
        .unwrap_or(value)
        .trim()
        .chars()
        .take(400)
        .collect()
}

async fn api_run(State(state): State<Arc<AppState>>, Path(id): Path<Uuid>) -> impl IntoResponse {
    let Some(pool) = &state.pool else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ApiError::new("database is not configured")),
        )
            .into_response();
    };

    match get_job(pool, id).await {
        Ok(Some(job)) => {
            let transitions = match list_job_transitions(pool, id).await {
                Ok(transitions) => transitions,
                Err(error) => {
                    tracing::error!(%error, %id, "failed to fetch job transitions");
                    return (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(ApiError::new("failed to fetch run transitions")),
                    )
                        .into_response();
                }
            };

            let outbound_actions = match list_job_outbound_actions(pool, id).await {
                Ok(actions) => actions,
                Err(error) => {
                    tracing::error!(%error, %id, "failed to fetch outbound actions");
                    return (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(ApiError::new("failed to fetch run outbound actions")),
                    )
                        .into_response();
                }
            };

            let command_results = match list_job_command_results(pool, id).await {
                Ok(results) => results,
                Err(error) => {
                    tracing::error!(%error, %id, "failed to fetch command results");
                    return (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(ApiError::new("failed to fetch run command results")),
                    )
                        .into_response();
                }
            };

            let coordinator_job_id = job
                .input
                .pointer("/plugin_execution/coordinator_run_id")
                .and_then(Value::as_str)
                .and_then(|value| Uuid::parse_str(value).ok());
            let publications =
                match list_agent_publications_for_run(pool, id, coordinator_job_id).await {
                    Ok(publications) => publications,
                    Err(error) => {
                        tracing::error!(%error, %id, "failed to fetch agent publications");
                        return (
                            StatusCode::INTERNAL_SERVER_ERROR,
                            Json(ApiError::new("failed to fetch run publications")),
                        )
                            .into_response();
                    }
                };

            Json(RunDetail {
                job,
                transitions,
                outbound_actions,
                command_results,
                publications,
            })
            .into_response()
        }
        Ok(None) => (StatusCode::NOT_FOUND, Json(ApiError::new("run not found"))).into_response(),
        Err(error) => {
            tracing::error!(%error, %id, "failed to fetch job");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiError::new("failed to fetch run")),
            )
                .into_response()
        }
    }
}

async fn api_outbound_actions(
    State(state): State<Arc<AppState>>,
    Query(query): Query<RepositoryQuery>,
) -> impl IntoResponse {
    let Some(pool) = &state.pool else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ApiError::new("database is not configured")),
        )
            .into_response();
    };

    let repository = match parse_repository_query(query.repository.as_deref()) {
        Ok(repository) => repository,
        Err(error) => return (StatusCode::BAD_REQUEST, Json(ApiError::new(error))).into_response(),
    };
    let actions = match repository {
        Some((owner, repo)) => {
            list_recent_outbound_actions_for_repository(pool, &owner, &repo, 50).await
        }
        None => list_recent_outbound_actions(pool, 50).await,
    };
    match actions {
        Ok(actions) => Json(actions).into_response(),
        Err(error) => {
            tracing::error!(%error, "failed to list outbound actions");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiError::new("failed to list outbound actions")),
            )
                .into_response()
        }
    }
}

async fn api_engagement_decisions(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let Some(pool) = &state.pool else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ApiError::new("database is not configured")),
        )
            .into_response();
    };

    match list_recent_engagement_decisions(pool, 100).await {
        Ok(decisions) => Json(decisions).into_response(),
        Err(error) => {
            tracing::error!(%error, "failed to list engagement decisions");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiError::new("failed to list engagement decisions")),
            )
                .into_response()
        }
    }
}

async fn api_run_transitions(
    State(state): State<Arc<AppState>>,
    Path(id): Path<Uuid>,
) -> impl IntoResponse {
    let Some(pool) = &state.pool else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ApiError::new("database is not configured")),
        )
            .into_response();
    };

    match list_job_transitions(pool, id).await {
        Ok(transitions) => Json(transitions).into_response(),
        Err(error) => {
            tracing::error!(%error, %id, "failed to fetch run transitions");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiError::new("failed to fetch run transitions")),
            )
                .into_response()
        }
    }
}

async fn api_lease_run(
    State(state): State<Arc<AppState>>,
    Path(id): Path<Uuid>,
    Json(request): Json<LeaseRequest>,
) -> impl IntoResponse {
    let Some(pool) = &state.pool else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ApiError::new("database is not configured")),
        )
            .into_response();
    };

    let lease_seconds = request.lease_seconds.unwrap_or(300).clamp(30, 3600);

    match acquire_job_lease(pool, id, &request.lease_owner, lease_seconds).await {
        Ok(Some(job)) => Json(job).into_response(),
        Ok(None) => (
            StatusCode::CONFLICT,
            Json(ApiError::new("run is not available for lease")),
        )
            .into_response(),
        Err(error) => {
            tracing::error!(%error, %id, "failed to lease job");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiError::new("failed to lease run")),
            )
                .into_response()
        }
    }
}

async fn api_retry_run(
    State(state): State<Arc<AppState>>,
    Path(id): Path<Uuid>,
) -> impl IntoResponse {
    let Some(pool) = &state.pool else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ApiError::new("database is not configured")),
        )
            .into_response();
    };

    if !state.policy.dashboard.allow_retry {
        return (
            StatusCode::FORBIDDEN,
            Json(ApiError::new("run retries are disabled by policy")),
        )
            .into_response();
    }

    let job = match get_job(pool, id).await {
        Ok(Some(job)) => job,
        Ok(None) => {
            return (StatusCode::NOT_FOUND, Json(ApiError::new("run not found"))).into_response();
        }
        Err(error) => {
            tracing::error!(%error, %id, "failed to fetch run before retry");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiError::new("failed to retry run")),
            )
                .into_response();
        }
    };

    if !can_retry_job(&job) {
        return (
            StatusCode::CONFLICT,
            Json(ApiError::new("run is not eligible for retry")),
        )
            .into_response();
    }

    let Some(workflow_item_id) = job.workflow_item_id else {
        return (
            StatusCode::CONFLICT,
            Json(ApiError::new("run is not linked to a workflow item")),
        )
            .into_response();
    };

    match create_retry_job(pool, Some(workflow_item_id), job.id, &job.role, &job.input).await {
        Ok(retry_job) => Json(retry_job).into_response(),
        Err(error) => {
            tracing::error!(%error, %id, "failed to create retry job");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiError::new("failed to retry run")),
            )
                .into_response()
        }
    }
}

async fn api_retry_publication(
    State(state): State<Arc<AppState>>,
    Path(id): Path<i64>,
) -> impl IntoResponse {
    let Some(pool) = &state.pool else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ApiError::new("database is not configured")),
        )
            .into_response();
    };
    if !state.policy.dashboard.allow_retry {
        return (
            StatusCode::FORBIDDEN,
            Json(ApiError::new("publication retries are disabled by policy")),
        )
            .into_response();
    }
    match retry_agent_publication(pool, id).await {
        Ok(true) => StatusCode::ACCEPTED.into_response(),
        Ok(false) => (
            StatusCode::CONFLICT,
            Json(ApiError::new("only failed publications can be retried")),
        )
            .into_response(),
        Err(error) => {
            tracing::error!(%error, publication_id = id, "failed to retry publication");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiError::new("failed to retry publication")),
            )
                .into_response()
        }
    }
}

async fn github_webhook(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: Bytes,
) -> impl IntoResponse {
    if let Some(secret) = &state.webhook_secret {
        let signature = headers
            .get("x-hub-signature-256")
            .and_then(|value| value.to_str().ok());

        if let Err(error) = donkeyspace_github::verify_signature(secret, &body, signature) {
            tracing::warn!(%error, "github webhook signature rejected");
            return StatusCode::UNAUTHORIZED;
        }
    }

    let event = headers
        .get("x-github-event")
        .and_then(|value| value.to_str().ok())
        .unwrap_or("unknown");
    let delivery = headers
        .get("x-github-delivery")
        .and_then(|value| value.to_str().ok())
        .unwrap_or("unknown");

    if let Some(expected) = state
        .github_auth
        .as_ref()
        .and_then(GitHubCredentialProvider::installation_id)
        && !webhook_installation_matches(expected, &body)
    {
        let actual = serde_json::from_slice::<Value>(&body)
            .ok()
            .and_then(|payload| payload.pointer("/installation/id").and_then(Value::as_u64));
        tracing::warn!(expected, ?actual, "github webhook installation rejected");
        return StatusCode::FORBIDDEN;
    }

    if !webhook_repository_allowed(&state.configured_repositories, &body) {
        tracing::warn!("github webhook repository is not configured");
        return StatusCode::FORBIDDEN;
    }

    let Some(pool) = &state.pool else {
        tracing::info!(
            event,
            delivery,
            bytes = body.len(),
            "accepted github webhook without database"
        );
        return StatusCode::ACCEPTED;
    };

    match persist_github_webhook(pool, &state, event, delivery, &body).await {
        Ok(WebhookPersistOutcome::Ignored) => StatusCode::ACCEPTED,
        Ok(WebhookPersistOutcome::Duplicate) => StatusCode::OK,
        Ok(WebhookPersistOutcome::Queued(job)) => {
            tracing::info!(job_id = %job.id, event, delivery, "queued webhook job");
            StatusCode::ACCEPTED
        }
        Err(error) => {
            tracing::error!(%error, event, delivery, "failed to process github webhook");
            StatusCode::INTERNAL_SERVER_ERROR
        }
    }
}

fn webhook_installation_matches(expected: u64, body: &[u8]) -> bool {
    serde_json::from_slice::<Value>(body)
        .ok()
        .and_then(|payload| payload.pointer("/installation/id").and_then(Value::as_u64))
        == Some(expected)
}

fn webhook_repository_allowed(configured: &[PolledRepository], body: &[u8]) -> bool {
    if configured.is_empty() {
        return true;
    }
    let Ok(payload) = serde_json::from_slice::<Value>(body) else {
        return false;
    };
    let owner = payload
        .pointer("/repository/owner/login")
        .and_then(Value::as_str);
    let name = payload.pointer("/repository/name").and_then(Value::as_str);
    configured.iter().any(|repository| {
        owner.is_some_and(|owner| repository.owner.eq_ignore_ascii_case(owner))
            && name.is_some_and(|name| repository.name.eq_ignore_ascii_case(name))
    })
}

fn init_tracing() {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "donkeyspace=info,tower_http=info".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();
}

fn load_optional_secret(
    value_name: &str,
    file_name: &str,
) -> Result<Option<String>, Box<dyn std::error::Error>> {
    let value = env::var(value_name)
        .ok()
        .filter(|value| !value.trim().is_empty());
    let file = env::var(file_name)
        .ok()
        .filter(|value| !value.trim().is_empty());
    match (value, file) {
        (Some(_), Some(_)) => {
            Err(format!("configure only one of {value_name} and {file_name}").into())
        }
        (Some(value), None) => Ok(Some(value)),
        (None, Some(path)) => Ok(Some(fs::read_to_string(path)?.trim_end().to_string())),
        (None, None) => Ok(None),
    }
}

fn load_policy() -> Result<Policy, Box<dyn std::error::Error>> {
    let path =
        env::var("DONKEYSPACE_POLICY_PATH").unwrap_or_else(|_| ".donkeyspace/policy.yml".into());
    let raw = fs::read_to_string(&path)?;
    let mut policy = Policy::from_yaml(&raw)?;
    if let Some(selection) = policy.lifecycle.plugin.as_ref().or(policy
        .agents
        .developer
        .plugin
        .as_ref())
    {
        let manifest = PluginManifest::from_path(&selection.manifest_path)?;
        policy.facade = manifest.facade.overlay(&policy.facade);
    }
    Ok(policy)
}

fn lifecycle_start_role(policy: &Policy) -> Result<Option<String>, Box<dyn std::error::Error>> {
    let Some(selection) = &policy.lifecycle.plugin else {
        return Ok(None);
    };
    let manifest = PluginManifest::from_path(&selection.manifest_path)?;
    let flow = manifest
        .flows
        .get(&selection.flow)
        .ok_or_else(|| format!("plugin `{}` has no flow `{}`", manifest.id, selection.flow))?;
    if !flow.replaces_default_lifecycle {
        return Err(format!(
            "plugin flow `{}` must declare replaces_default_lifecycle to be selected as a lifecycle",
            selection.flow
        )
        .into());
    }
    Ok(Some(flow.tasks[&flow.start].role.clone()))
}

async fn persist_github_webhook(
    pool: &PgPool,
    state: &AppState,
    event: &str,
    delivery: &str,
    body: &[u8],
) -> Result<WebhookPersistOutcome, Box<dyn std::error::Error>> {
    match event {
        "issues" | "issue_comment" => {
            persist_issue_webhook(pool, state, event, delivery, body).await
        }
        "pull_request" => {
            persist_pull_request_webhook(pool, &state.policy, event, delivery, body).await
        }
        "push" => persist_push_webhook(pool, &state.policy, event, delivery, body).await,
        _ => {
            let payload: Value = serde_json::from_slice(body)?;
            let inserted = record_webhook_delivery(pool, None, delivery, event, &payload).await?;
            Ok(if inserted.is_some() {
                WebhookPersistOutcome::Ignored
            } else {
                WebhookPersistOutcome::Duplicate
            })
        }
    }
}

async fn persist_issue_webhook(
    pool: &PgPool,
    app_state: &AppState,
    event: &str,
    delivery: &str,
    body: &[u8],
) -> Result<WebhookPersistOutcome, Box<dyn std::error::Error>> {
    let policy = &app_state.policy;
    let payload: GitHubIssueWebhook = serde_json::from_slice(body)?;
    let mut payload_value: Value = serde_json::from_slice(body)?;
    let repository_id = upsert_repository(
        pool,
        &RepositoryInput {
            installation_external_id: payload
                .installation
                .as_ref()
                .map(|value| value.id.to_string()),
            installation_account_login: payload
                .installation
                .as_ref()
                .map(|_| payload.repository.owner.login.clone()),
            provider: "github".to_string(),
            owner: payload.repository.owner.login.clone(),
            name: payload.repository.name.clone(),
            default_branch: payload.repository.default_branch.clone(),
        },
    )
    .await?;

    let inserted =
        record_webhook_delivery(pool, Some(repository_id), delivery, event, &payload_value).await?;
    let Some(webhook_delivery_id) = inserted else {
        return Ok(WebhookPersistOutcome::Duplicate);
    };

    let labels = payload
        .issue
        .labels
        .iter()
        .map(|label| label.name.clone())
        .collect::<Vec<_>>();
    let label_state = normalize_workflow_labels(&labels, &policy.workflow.state_labels);
    let label_state_name = match &label_state {
        LabelState::None => None,
        LabelState::One(label) => Some(label.state.to_string()),
        LabelState::Conflict(_) => Some(WorkflowState::NeedsHuman.to_string()),
    };
    let previous_state =
        get_workflow_item_state(pool, repository_id, &payload.issue.id.to_string()).await?;
    let current_state = label_state_name.or(previous_state);

    let workflow_item_id = upsert_workflow_item(
        pool,
        &WorkflowItemInput {
            repository_id,
            provider_issue_id: payload.issue.id.to_string(),
            issue_number: payload.issue.number,
            provider_state: payload.issue.state.clone(),
            current_state: current_state.clone(),
            current_labels: labels.clone(),
        },
    )
    .await?;

    if matches!(label_state, LabelState::Conflict(_)) {
        record_state_transition(
            pool,
            workflow_item_id,
            None,
            None,
            WorkflowState::NeedsHuman.as_str(),
            "conflicting ai workflow labels detected",
        )
        .await?;
        return Ok(WebhookPersistOutcome::Ignored);
    }

    if payload.issue.state == "closed" {
        tracing::info!(
            event,
            action = payload.action,
            issue_number = payload.issue.number,
            "closed issue did not queue agent work"
        );
        return Ok(WebhookPersistOutcome::Ignored);
    }

    let facade_command = policy.facade.resolve().command;
    if !should_queue_triage(
        event,
        &payload.action,
        &payload.issue.state,
        current_state.as_deref(),
        payload.comment.as_ref(),
        payload.label.as_ref().map(|label| label.name.as_str()),
        (&policy.workflow.allow_labels, &facade_command),
    ) {
        tracing::info!(
            event,
            action = payload.action,
            issue_number = payload.issue.number,
            current_state = current_state.as_deref().unwrap_or("none"),
            "webhook did not queue triage"
        );
        return Ok(WebhookPersistOutcome::Ignored);
    }

    let human_approval = if current_state.as_deref() == Some("needs_human") {
        payload.comment.as_ref().and_then(|comment| {
            parse_human_approval_command(&comment.body, &policy.facade.resolve().command)
        })
    } else {
        None
    };

    let gate = engagement_gate(event, current_state.as_deref())
        .ok_or("queueable github event has no engagement gate")?;
    let managed_resource = if let Some(comment) = &payload.comment {
        let registered = match comment.id {
            Some(comment_id) => {
                github_managed_resource_exists(
                    pool,
                    repository_id,
                    "issue_comment",
                    &comment_id.to_string(),
                )
                .await?
            }
            None => false,
        };
        let pending = match payload_value
            .pointer("/comment/body")
            .and_then(Value::as_str)
        {
            Some(body) => pending_outbound_comment_exists(pool, workflow_item_id, body).await?,
            None => false,
        };
        registered || pending
    } else {
        let created_by_this_app = app_state
            .github_auth
            .as_ref()
            .and_then(GitHubCredentialProvider::app_id)
            .zip(payload.issue.performed_via_github_app.as_ref())
            .is_some_and(|(configured, actual)| configured == actual.id);
        (is_projected_work_item(&payload.issue.body) && created_by_this_app)
            || github_managed_resource_exists(
                pool,
                repository_id,
                "issue",
                &payload.issue.id.to_string(),
            )
            .await?
    };
    if managed_resource {
        record_engagement_decision(
            pool,
            &EngagementDecisionInput {
                webhook_delivery_id,
                workflow_item_id: Some(workflow_item_id),
                gate: gate.as_str().into(),
                disposition: "system_generated".into(),
                actor: payload
                    .sender
                    .as_ref()
                    .and_then(|actor| serde_json::to_value(actor).ok()),
                matched_selector: None,
                reason: "platform-managed GitHub resource cannot trigger agent work".into(),
            },
        )
        .await?;
        return Ok(WebhookPersistOutcome::Ignored);
    }

    let automation_decision = policy.automation_decision_for_labels(&labels);
    if !automation_decision.is_allowed() {
        record_engagement_decision(
            pool,
            &EngagementDecisionInput {
                webhook_delivery_id,
                workflow_item_id: Some(workflow_item_id),
                gate: gate.as_str().into(),
                disposition: "denied".into(),
                actor: payload
                    .sender
                    .as_ref()
                    .and_then(|actor| serde_json::to_value(actor).ok()),
                matched_selector: None,
                reason: automation_decision.reason(),
            },
        )
        .await?;
        return Ok(WebhookPersistOutcome::Ignored);
    }

    let authorization = authorize_engagement(app_state, gate, &labels, &payload).await;
    let audit = record_engagement_decision(
        pool,
        &EngagementDecisionInput {
            webhook_delivery_id,
            workflow_item_id: Some(workflow_item_id),
            gate: gate.as_str().into(),
            disposition: if authorization.allowed {
                "allowed"
            } else {
                "denied"
            }
            .into(),
            actor: payload
                .sender
                .as_ref()
                .and_then(|actor| serde_json::to_value(actor).ok()),
            matched_selector: authorization.matched_selector.clone(),
            reason: authorization.reason.clone(),
        },
    )
    .await?;
    if !authorization.allowed {
        tracing::info!(
            event,
            action = payload.action,
            issue_number = payload.issue.number,
            reason = authorization.reason,
            "engagement authorization denied agent work"
        );
        return Ok(WebhookPersistOutcome::Ignored);
    }
    if let Value::Object(map) = &mut payload_value {
        map.insert(
            "donkeyspace_ingress".into(),
            json!({
                "delivery_id": delivery,
                "source": ingress_source(delivery),
                "event": event,
            }),
        );
        map.insert(
            "donkeyspace_engagement".into(),
            json!({
                "decision_id": audit.id,
                "gate": gate.as_str(),
                "actor": payload.sender,
                "matched_selector": authorization.matched_selector,
                "reason": authorization.reason,
            }),
        );
        if let Some(action) = human_approval.clone() {
            map.insert(
                "donkeyspace_human_decision".into(),
                serde_json::to_value(action).expect("approval action serializes"),
            );
        }
    }

    if policy.lifecycle.plugin.is_some()
        && let Value::Object(map) = &mut payload_value
    {
        map.insert(
            "donkeyspace_lifecycle_coordinator".into(),
            Value::Bool(true),
        );
    }

    if active_job_exists_for_workflow_item(pool, workflow_item_id).await? {
        tracing::info!(
            event,
            action = payload.action,
            issue_number = payload.issue.number,
            "active workflow job already exists; duplicate trigger ignored"
        );
        return Ok(WebhookPersistOutcome::Ignored);
    }

    if gate == EngagementGate::NeedsHumanResume
        && let Some(job) = resume_latest_paused_job(pool, workflow_item_id, &payload_value).await?
    {
        let (event_type, summary) = match human_approval.as_ref() {
            Some(HumanApprovalAction::Approve { target }) => (
                "approval_received",
                format!(
                    "Approval accepted{}.",
                    target
                        .as_deref()
                        .map(|target| format!(" for {target}"))
                        .unwrap_or_default()
                ),
            ),
            Some(HumanApprovalAction::Revise { target, .. }) => (
                "revision_received",
                format!(
                    "Revision requested{}.",
                    target
                        .as_deref()
                        .map(|target| format!(" for {target}"))
                        .unwrap_or_default()
                ),
            ),
            None => (
                "workflow_resumed",
                "Authorized feedback resumed the workflow.".into(),
            ),
        };
        record_ingress_lifecycle_event(
            pool,
            workflow_item_id,
            Some(job.id),
            delivery,
            event_type,
            &summary,
            payload.sender.as_ref().map(|sender| sender.login.as_str()),
        )
        .await?;
        record_state_transition(
            pool,
            workflow_item_id,
            Some(job.id),
            current_state.as_deref(),
            "lifecycle_resumed",
            &format!(
                "resumed paused plugin lifecycle from authorized github event; engagement decision {}",
                audit.id
            ),
        )
        .await?;
        return Ok(WebhookPersistOutcome::Queued(Box::new(job)));
    }

    let lifecycle_role = lifecycle_start_role(policy)?;
    let initial_role = lifecycle_role.unwrap_or_else(|| AgentRole::Triage.as_str().to_string());
    let job = create_job(pool, Some(workflow_item_id), &initial_role, &payload_value).await?;
    record_ingress_lifecycle_event(
        pool,
        workflow_item_id,
        Some(job.id),
        delivery,
        "issue_received",
        "Issue accepted for agent work.",
        payload.sender.as_ref().map(|sender| sender.login.as_str()),
    )
    .await?;
    record_state_transition(
        pool,
        workflow_item_id,
        Some(job.id),
        current_state.as_deref(),
        &format!("{initial_role}_queued"),
        &format!(
            "queued {initial_role} job from github webhook; engagement decision {}",
            audit.id
        ),
    )
    .await?;

    Ok(WebhookPersistOutcome::Queued(Box::new(job)))
}

fn ingress_source(delivery: &str) -> &'static str {
    if delivery.starts_with("github-poll:") {
        "poll"
    } else {
        "webhook"
    }
}

async fn record_ingress_lifecycle_event(
    pool: &PgPool,
    workflow_item_id: i64,
    coordinator_job_id: Option<Uuid>,
    delivery: &str,
    event_type: &str,
    summary: &str,
    actor: Option<&str>,
) -> Result<(), donkeyspace_db::DbError> {
    record_lifecycle_event(
        pool,
        &LifecycleEventInput {
            workflow_item_id,
            coordinator_job_id,
            job_id: coordinator_job_id,
            dedupe_key: Some(format!("ingress:{delivery}:{event_type}")),
            event_type: event_type.into(),
            level: "milestone".into(),
            source: ingress_source(delivery).into(),
            actor: actor.map(Into::into),
            wave: None,
            attempt: None,
            role: None,
            role_display_name: None,
            task: None,
            task_display_name: None,
            work_item: None,
            status: None,
            outcome: None,
            summary: summary.into(),
            reason: None,
            handoff_target: None,
            links: json!([]),
        },
    )
    .await?;
    Ok(())
}

fn is_projected_work_item(body: &str) -> bool {
    body.contains("<!-- donkeyspace-work-item -->")
}

async fn persist_pull_request_webhook(
    pool: &PgPool,
    policy: &Policy,
    event: &str,
    delivery: &str,
    body: &[u8],
) -> Result<WebhookPersistOutcome, Box<dyn std::error::Error>> {
    let payload: GitHubPullRequestWebhook = serde_json::from_slice(body)?;
    let payload_value: Value = serde_json::from_slice(body)?;
    let repository_id = upsert_repository(
        pool,
        &RepositoryInput {
            installation_external_id: payload
                .installation
                .as_ref()
                .map(|value| value.id.to_string()),
            installation_account_login: payload
                .installation
                .as_ref()
                .map(|_| payload.repository.owner.login.clone()),
            provider: "github".to_string(),
            owner: payload.repository.owner.login.clone(),
            name: payload.repository.name.clone(),
            default_branch: payload.repository.default_branch.clone(),
        },
    )
    .await?;

    let inserted =
        record_webhook_delivery(pool, Some(repository_id), delivery, event, &payload_value).await?;
    if inserted.is_none() {
        return Ok(WebhookPersistOutcome::Duplicate);
    }

    let branch_prefix = &policy.facade.resolve().branch_prefix;
    let managed = pull_request_is_managed(&payload.pull_request, branch_prefix);
    let linked_issue_number = payload
        .pull_request
        .body
        .as_deref()
        .and_then(extract_linked_issue_number)
        .or_else(|| {
            issue_number_from_managed_branch(&payload.pull_request.head.ref_name, branch_prefix)
        });
    let workflow_item = match linked_issue_number {
        Some(issue_number) => {
            get_workflow_item_by_issue_number(pool, repository_id, issue_number).await?
        }
        None => None,
    };

    upsert_pull_request(
        pool,
        &PullRequestInput {
            repository_id,
            workflow_item_id: workflow_item.as_ref().map(|item| item.id),
            provider_pr_id: payload.pull_request.id.to_string(),
            pr_number: payload.pull_request.number,
            title: payload.pull_request.title.clone(),
            html_url: payload.pull_request.html_url.clone(),
            state: payload.pull_request.state.clone(),
            head_ref: payload.pull_request.head.ref_name.clone(),
            head_sha: Some(payload.pull_request.head.sha.clone()),
            base_ref: payload.pull_request.base.ref_name.clone(),
            base_sha: Some(payload.pull_request.base.sha.clone()),
            managed_by_donkeyspace: managed,
        },
    )
    .await?;

    let Some(workflow_item) = workflow_item else {
        tracing::info!(
            action = payload.action,
            pr_number = payload.pull_request.number,
            "pull request webhook did not match a known workflow item"
        );
        return Ok(WebhookPersistOutcome::Ignored);
    };

    if policy.lifecycle.plugin.is_some()
        || !should_queue_reviewer(
            &payload.action,
            &payload.pull_request.state,
            payload.pull_request.draft,
            managed,
            policy.agents.reviewer.enabled,
        )
    {
        tracing::info!(
            action = payload.action,
            pr_number = payload.pull_request.number,
            managed,
            "pull request webhook did not queue reviewer"
        );
        return Ok(WebhookPersistOutcome::Ignored);
    }

    if reviewer_job_exists_for_pr_head(
        pool,
        workflow_item.id,
        payload.pull_request.number,
        Some(&payload.pull_request.head.sha),
    )
    .await?
    {
        tracing::info!(
            pr_number = payload.pull_request.number,
            head_sha = payload.pull_request.head.sha,
            "reviewer job already exists for pull request head"
        );
        return Ok(WebhookPersistOutcome::Ignored);
    }

    let Some(mut job_input) = latest_workflow_job_input(pool, workflow_item.id).await? else {
        tracing::info!(
            pr_number = payload.pull_request.number,
            "pull request webhook found workflow item without reusable job input"
        );
        return Ok(WebhookPersistOutcome::Ignored);
    };
    attach_pull_request_input(&mut job_input, payload_value["pull_request"].clone());

    let job = create_job(
        pool,
        Some(workflow_item.id),
        AgentRole::Reviewer.as_str(),
        &job_input,
    )
    .await?;
    record_state_transition(
        pool,
        workflow_item.id,
        Some(job.id),
        workflow_item.current_state.as_deref(),
        "reviewer_queued",
        "queued reviewer job from pull request webhook",
    )
    .await?;

    Ok(WebhookPersistOutcome::Queued(Box::new(job)))
}

async fn persist_push_webhook(
    pool: &PgPool,
    policy: &Policy,
    event: &str,
    delivery: &str,
    body: &[u8],
) -> Result<WebhookPersistOutcome, Box<dyn std::error::Error>> {
    let payload: GitHubPushWebhook = serde_json::from_slice(body)?;
    let payload_value: Value = serde_json::from_slice(body)?;
    let repository_id = upsert_repository(
        pool,
        &RepositoryInput {
            installation_external_id: payload
                .installation
                .as_ref()
                .map(|value| value.id.to_string()),
            installation_account_login: payload
                .installation
                .as_ref()
                .map(|_| payload.repository.owner.login.clone()),
            provider: "github".to_string(),
            owner: payload.repository.owner.login,
            name: payload.repository.name,
            default_branch: payload.repository.default_branch.clone(),
        },
    )
    .await?;

    let inserted =
        record_webhook_delivery(pool, Some(repository_id), delivery, event, &payload_value).await?;
    if inserted.is_none() {
        return Ok(WebhookPersistOutcome::Duplicate);
    }
    if policy.lifecycle.plugin.is_some() {
        return Ok(WebhookPersistOutcome::Ignored);
    }

    let Some(branch) = payload.git_ref.strip_prefix("refs/heads/") else {
        return Ok(WebhookPersistOutcome::Ignored);
    };
    if branch != payload.repository.default_branch {
        tracing::info!(
            branch,
            default_branch = payload.repository.default_branch,
            "push webhook ignored for non-default branch"
        );
        return Ok(WebhookPersistOutcome::Ignored);
    }

    let candidates = list_open_managed_pull_requests_for_base(pool, repository_id, branch).await?;
    let mut queued = None;

    for pull_request in candidates {
        if repair_job_exists_for_pr_base(
            pool,
            pull_request.workflow_item_id,
            pull_request.pr_number,
            pull_request.head_sha.as_deref(),
            Some(&payload.after),
        )
        .await?
        {
            continue;
        }

        let Some(mut job_input) =
            latest_workflow_job_input(pool, pull_request.workflow_item_id).await?
        else {
            continue;
        };
        attach_pull_request_input(
            &mut job_input,
            json!({
                "number": pull_request.pr_number,
                "title": pull_request.title,
                "body": null,
                "html_url": pull_request.html_url,
                "state": pull_request.state,
                "draft": false,
                "head": {
                    "ref": pull_request.head_ref,
                    "sha": pull_request.head_sha,
                },
                "base": {
                    "ref": pull_request.base_ref,
                    "sha": payload.after,
                },
            }),
        );

        let job = create_job(
            pool,
            Some(pull_request.workflow_item_id),
            AgentRole::Repair.as_str(),
            &job_input,
        )
        .await?;
        record_state_transition(
            pool,
            pull_request.workflow_item_id,
            Some(job.id),
            Some(WorkflowState::PrOpen.as_str()),
            "repair_queued",
            "queued repair check after base branch push",
        )
        .await?;
        queued.get_or_insert(job);
    }

    Ok(queued
        .map(|job| WebhookPersistOutcome::Queued(Box::new(job)))
        .unwrap_or(WebhookPersistOutcome::Ignored))
}

fn should_queue_triage(
    event: &str,
    action: &str,
    issue_state: &str,
    current_state: Option<&str>,
    comment: Option<&GitHubComment>,
    changed_label: Option<&str>,
    workflow: (&[String], &str),
) -> bool {
    let (allow_labels, facade_command) = workflow;
    if issue_state == "closed" {
        return false;
    }

    if current_state == Some("needs_human") {
        return event == "issue_comment"
            && action == "created"
            && comment.is_some_and(|comment| {
                parse_human_approval_command(&comment.body, facade_command).is_some()
            });
    }

    match (event, action) {
        ("issues", "opened" | "edited" | "reopened") => true,
        ("issues", "labeled") => {
            changed_label
                .map(|label| allow_labels.iter().any(|allowed| allowed == label))
                .unwrap_or(false)
                && matches!(current_state, None | Some("needs_info" | "blocked"))
        }
        ("issue_comment", "created" | "edited") => {
            matches!(
                current_state,
                Some(state) if matches!(state, "needs_info" | "blocked")
            ) && comment.is_some()
        }
        _ => false,
    }
}

fn parse_human_approval_command(body: &str, facade_command: &str) -> Option<HumanApprovalAction> {
    let mut lines = body.lines();
    let first = lines.find(|line| !line.trim().is_empty())?.trim();
    let mut parts = first.split_whitespace();
    if parts.next()? != format!("/{facade_command}") {
        return None;
    }
    let action = parts.next()?;
    let target = parts.next().map(str::to_string);
    if parts.next().is_some()
        || target
            .as_deref()
            .is_some_and(|value| !valid_approval_target(value))
    {
        return None;
    }
    match action {
        "approve" => Some(HumanApprovalAction::Approve { target }),
        "revise" => {
            let feedback = lines.collect::<Vec<_>>().join("\n").trim().to_string();
            (!feedback.is_empty()).then_some(HumanApprovalAction::Revise { target, feedback })
        }
        _ => None,
    }
}

fn valid_approval_target(value: &str) -> bool {
    value == "all"
        || (!value.is_empty()
            && value.matches('/').count() <= 1
            && value.chars().all(|character| {
                character.is_ascii_alphanumeric() || matches!(character, '-' | '_' | '.' | '/')
            }))
}

fn engagement_gate(event: &str, current_state: Option<&str>) -> Option<EngagementGate> {
    match current_state {
        Some("needs_info") => Some(EngagementGate::NeedsInfoResume),
        Some("blocked") => Some(EngagementGate::BlockedResume),
        Some("needs_human") => Some(EngagementGate::NeedsHumanResume),
        _ if event == "issues" => Some(EngagementGate::Initial),
        _ => None,
    }
}

#[derive(Debug)]
struct AuthorizationDecision {
    allowed: bool,
    reason: String,
    matched_selector: Option<Value>,
}

async fn authorize_engagement(
    state: &AppState,
    gate: EngagementGate,
    labels: &[String],
    payload: &GitHubIssueWebhook,
) -> AuthorizationDecision {
    let Some(actor) = payload.sender.as_ref() else {
        return AuthorizationDecision {
            allowed: false,
            reason: "github event is missing sender identity".into(),
            matched_selector: None,
        };
    };
    if actor.login.trim().is_empty() {
        return AuthorizationDecision {
            allowed: false,
            reason: "github event sender identity has no login".into(),
            matched_selector: None,
        };
    }
    if payload
        .comment
        .as_ref()
        .is_some_and(|comment| comment.id.is_none())
    {
        return AuthorizationDecision {
            allowed: false,
            reason: "github comment event is missing comment identity".into(),
            matched_selector: None,
        };
    }
    let repository = format!(
        "{}/{}",
        payload.repository.owner.login, payload.repository.name
    );
    let rule = state
        .policy
        .workflow
        .engagement
        .rule(gate, Some(&repository));
    let missing_labels = rule
        .required_labels
        .iter()
        .filter(|required| !labels.iter().any(|label| label == *required))
        .cloned()
        .collect::<Vec<_>>();
    if !missing_labels.is_empty() {
        return AuthorizationDecision {
            allowed: false,
            reason: format!(
                "missing required engagement labels: {}",
                missing_labels.join(", ")
            ),
            matched_selector: None,
        };
    }

    let (content_actor, content_association) = match payload.comment.as_ref() {
        Some(comment) => (comment.user.as_ref(), comment.author_association.as_deref()),
        None => (
            payload.issue.user.as_ref(),
            payload.issue.author_association.as_deref(),
        ),
    };
    let author_association = if content_actor
        .is_some_and(|content_actor| actor.login.eq_ignore_ascii_case(&content_actor.login))
    {
        content_association
    } else {
        None
    };
    let performed_app = payload
        .comment
        .as_ref()
        .and_then(|comment| comment.performed_via_github_app.as_ref())
        .or(payload.issue.performed_via_github_app.as_ref());
    let mut failures = Vec::new();

    for selector in &rule.allow {
        let result: Result<bool, String> = match selector {
            EngagementSelector::TokenOwner => Ok(state
                .github_token_owner
                .as_ref()
                .map(|login| login.eq_ignore_ascii_case(&actor.login))
                .unwrap_or(false)),
            EngagementSelector::AnyUser => Ok(actor.kind.as_deref() == Some("User")),
            EngagementSelector::User { login } => Ok(
                actor.kind.as_deref() == Some("User") && actor.login.eq_ignore_ascii_case(login)
            ),
            EngagementSelector::IssueAuthor => Ok(payload
                .issue
                .user
                .as_ref()
                .is_some_and(|author| actor.login.eq_ignore_ascii_case(&author.login))),
            EngagementSelector::RepositoryOwner => Ok(payload.repository.owner.kind.as_deref()
                != Some("Organization")
                && actor
                    .login
                    .eq_ignore_ascii_case(&payload.repository.owner.login)),
            EngagementSelector::RepositoryOrganizationMember => {
                if payload.repository.owner.kind.as_deref() != Some("Organization") {
                    Ok(false)
                } else {
                    verify_organization_member(state, &payload.repository.owner.login, &actor.login)
                        .await
                }
            }
            EngagementSelector::OrganizationMember { organization } => {
                verify_organization_member(state, organization, &actor.login).await
            }
            EngagementSelector::TeamMember {
                organization,
                team_slug,
            } => verify_team_member(state, organization, team_slug, &actor.login).await,
            EngagementSelector::AuthorAssociation { association } => {
                Ok(author_association == Some(association.as_str()))
            }
            EngagementSelector::CollaboratorPermission { minimum } => {
                verify_collaborator_permission(
                    state,
                    &payload.repository.owner.login,
                    &payload.repository.name,
                    &actor.login,
                )
                .await
                .map(|actual| permission_rank(&actual) >= permission_rank(minimum))
            }
            EngagementSelector::Bot { login } => {
                Ok(actor.kind.as_deref() == Some("Bot") && actor.login.eq_ignore_ascii_case(login))
            }
            EngagementSelector::GitHubApp { id, slug } => Ok(performed_app
                .map(|app| {
                    id.map(|expected| app.id == expected).unwrap_or(false)
                        || slug
                            .as_ref()
                            .map(|expected| app.slug.eq_ignore_ascii_case(expected))
                            .unwrap_or(false)
                })
                .unwrap_or(false)),
        };

        match result {
            Ok(true) => {
                return AuthorizationDecision {
                    allowed: true,
                    reason: format!("actor matched engagement selector `{selector:?}`"),
                    matched_selector: serde_json::to_value(selector).ok(),
                };
            }
            Ok(false) => failures.push(format!("`{selector:?}` did not match")),
            Err(error) => failures.push(format!("`{selector:?}` could not be verified: {error}")),
        }
    }

    AuthorizationDecision {
        allowed: false,
        reason: if failures.is_empty() {
            "engagement rule has no allowed identities".into()
        } else {
            failures.join("; ")
        },
        matched_selector: None,
    }
}

async fn verify_organization_member(
    state: &AppState,
    organization: &str,
    actor: &str,
) -> Result<bool, String> {
    let key = format!("org:{organization}:{actor}").to_ascii_lowercase();
    if let Some(value) = verification_cache_get(state, &key).await {
        return Ok(value == "true");
    }
    let result = match &state.github_auth {
        Some(provider) => provider
            .client()
            .organization_member(organization, actor)
            .await
            .map_err(|error| error.to_string()),
        None => Err("github credentials are unavailable".into()),
    }?;
    verification_cache_put(state, key, result.to_string()).await;
    Ok(result)
}

async fn verify_team_member(
    state: &AppState,
    organization: &str,
    team_slug: &str,
    actor: &str,
) -> Result<bool, String> {
    let key = format!("team:{organization}:{team_slug}:{actor}").to_ascii_lowercase();
    if let Some(value) = verification_cache_get(state, &key).await {
        return Ok(value == "true");
    }
    let result = match &state.github_auth {
        Some(provider) => provider
            .client()
            .team_member(organization, team_slug, actor)
            .await
            .map_err(|error| error.to_string()),
        None => Err("github credentials are unavailable".into()),
    }?;
    verification_cache_put(state, key, result.to_string()).await;
    Ok(result)
}

async fn verify_collaborator_permission(
    state: &AppState,
    owner: &str,
    repo: &str,
    actor: &str,
) -> Result<String, String> {
    let key = format!("permission:{owner}:{repo}:{actor}").to_ascii_lowercase();
    if let Some(value) = verification_cache_get(state, &key).await {
        return Ok(value);
    }
    let result = match &state.github_auth {
        Some(provider) => provider
            .client()
            .collaborator_permission(owner, repo, actor)
            .await
            .map_err(|error| error.to_string()),
        None => Err("github credentials are unavailable".into()),
    }?;
    verification_cache_put(state, key, result.clone()).await;
    Ok(result)
}

async fn verification_cache_get(state: &AppState, key: &str) -> Option<String> {
    let cache = state.verification_cache.lock().await;
    cache.get(key).and_then(|(created_at, value)| {
        (created_at.elapsed() < Duration::from_secs(300)).then(|| value.clone())
    })
}

async fn verification_cache_put(state: &AppState, key: String, value: String) {
    let mut cache = state.verification_cache.lock().await;
    if cache.len() >= 1_024 {
        cache.retain(|_, (created_at, _)| created_at.elapsed() < Duration::from_secs(300));
        if cache.len() >= 1_024
            && let Some(oldest) = cache
                .iter()
                .min_by_key(|(_, (created_at, _))| *created_at)
                .map(|(key, _)| key.clone())
        {
            cache.remove(&oldest);
        }
    }
    cache.insert(key, (Instant::now(), value));
}

fn permission_rank(permission: &str) -> u8 {
    match permission {
        "admin" => 5,
        "maintain" => 4,
        "write" | "push" => 3,
        "triage" => 2,
        "read" | "pull" => 1,
        _ => 0,
    }
}

fn should_queue_reviewer(
    action: &str,
    pr_state: &str,
    draft: bool,
    managed: bool,
    reviewer_enabled: bool,
) -> bool {
    reviewer_enabled
        && managed
        && pr_state == "open"
        && !draft
        && matches!(
            action,
            "opened" | "synchronize" | "reopened" | "ready_for_review"
        )
}

fn can_retry_job(job: &JobRecord) -> bool {
    if job.status != "failed" {
        return false;
    }

    !matches!(
        job.result
            .as_ref()
            .and_then(|result| result.get("outcome").and_then(Value::as_str)),
        Some("blocked" | "needs_human")
    )
}

fn pull_request_is_managed(pull_request: &GitHubPullRequest, branch_prefix: &str) -> bool {
    pull_request
        .head
        .ref_name
        .starts_with(&format!("{branch_prefix}/issue-"))
        || pull_request
            .body
            .as_deref()
            .map(|body| {
                body.contains("<!-- donkeyspace-generated -->")
                    || body.contains("Generated by donkeyspace developer job")
            })
            .unwrap_or(false)
}

fn extract_linked_issue_number(value: &str) -> Option<i64> {
    for token in
        value.split(|character: char| !character.is_ascii_alphanumeric() && character != '#')
    {
        if let Some(number) = token.strip_prefix('#').and_then(parse_positive_i64) {
            return Some(number);
        }
    }

    None
}

fn issue_number_from_managed_branch(branch: &str, branch_prefix: &str) -> Option<i64> {
    let prefix = format!("{branch_prefix}/issue-");
    let suffix = branch.strip_prefix(&prefix)?;
    let number = suffix.split('-').next()?;
    parse_positive_i64(number)
}

fn parse_positive_i64(value: &str) -> Option<i64> {
    let number = value.parse::<i64>().ok()?;
    (number > 0).then_some(number)
}

fn attach_pull_request_input(input: &mut Value, pull_request: Value) {
    if let Value::Object(map) = input {
        map.insert("pull_request".to_string(), pull_request);
    }
}

enum WebhookPersistOutcome {
    Ignored,
    Duplicate,
    Queued(Box<JobRecord>),
}

#[derive(Debug, Deserialize)]
struct GitHubIssueWebhook {
    action: String,
    repository: GitHubRepository,
    issue: GitHubIssue,
    #[serde(default)]
    comment: Option<GitHubComment>,
    #[serde(default)]
    label: Option<GitHubLabel>,
    #[serde(default)]
    installation: Option<GitHubInstallation>,
    #[serde(default)]
    sender: Option<GitHubActor>,
}

#[derive(Debug, Deserialize)]
struct GitHubPullRequestWebhook {
    action: String,
    repository: GitHubRepository,
    pull_request: GitHubPullRequest,
    #[serde(default)]
    installation: Option<GitHubInstallation>,
}

#[derive(Debug, Deserialize)]
struct GitHubPushWebhook {
    #[serde(rename = "ref")]
    git_ref: String,
    after: String,
    repository: GitHubRepository,
    #[serde(default)]
    installation: Option<GitHubInstallation>,
}

#[derive(Debug, Deserialize)]
struct GitHubInstallation {
    id: u64,
}

#[derive(Debug, Deserialize)]
struct GitHubRepository {
    name: String,
    default_branch: String,
    owner: GitHubOwner,
}

#[derive(Debug, Deserialize)]
struct GitHubOwner {
    login: String,
    #[serde(rename = "type", default)]
    kind: Option<String>,
}

#[derive(Debug, Deserialize)]
struct GitHubIssue {
    id: i64,
    number: i64,
    state: String,
    #[serde(default, deserialize_with = "deserialize_null_default")]
    body: String,
    labels: Vec<GitHubLabel>,
    #[serde(default)]
    user: Option<GitHubActor>,
    #[serde(default)]
    author_association: Option<String>,
    #[serde(default)]
    performed_via_github_app: Option<GitHubAppIdentity>,
}

fn deserialize_null_default<'de, D, T>(deserializer: D) -> Result<T, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de> + Default,
{
    Option::<T>::deserialize(deserializer).map(Option::unwrap_or_default)
}

#[derive(Debug, Deserialize)]
struct GitHubLabel {
    name: String,
}

#[derive(Debug, Deserialize)]
struct GitHubComment {
    #[serde(default)]
    id: Option<i64>,
    #[serde(default)]
    user: Option<GitHubActor>,
    #[serde(default)]
    author_association: Option<String>,
    #[serde(default)]
    performed_via_github_app: Option<GitHubAppIdentity>,
    #[serde(default, deserialize_with = "deserialize_null_default")]
    body: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct GitHubActor {
    #[serde(default)]
    login: String,
    #[serde(default)]
    id: Option<u64>,
    #[serde(rename = "type", default)]
    kind: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct GitHubAppIdentity {
    id: u64,
    slug: String,
}

#[derive(Debug, Deserialize)]
struct GitHubPullRequest {
    id: i64,
    number: i64,
    title: String,
    body: Option<String>,
    html_url: String,
    state: String,
    #[serde(default)]
    draft: bool,
    head: GitHubPullRequestRef,
    base: GitHubPullRequestRef,
}

#[derive(Debug, Deserialize)]
struct GitHubPullRequestRef {
    #[serde(rename = "ref")]
    ref_name: String,
    sha: String,
}

#[derive(Debug, Deserialize)]
struct LeaseRequest {
    lease_owner: String,
    lease_seconds: Option<i32>,
}

#[derive(Debug, Serialize)]
struct RunDetail {
    job: JobRecord,
    transitions: Vec<donkeyspace_db::StateTransitionRecord>,
    outbound_actions: Vec<donkeyspace_db::OutboundActionRecord>,
    command_results: Vec<donkeyspace_db::CommandResultRecord>,
    publications: Vec<donkeyspace_db::AgentPublicationRecord>,
}

#[derive(Debug, Serialize)]
struct WorkflowTaskSummary {
    job_id: Uuid,
    role: String,
    role_display_name: String,
    task: String,
    task_display_name: String,
    work_item: Option<String>,
    status: String,
    outcome: Option<String>,
    summary: Option<String>,
    updated_at: DateTime<Utc>,
}

#[derive(Debug, Serialize)]
struct WorkflowSummary {
    id: i64,
    owner: String,
    repository: String,
    issue_number: i64,
    issue_title: String,
    issue_url: String,
    provider_state: String,
    current_state: Option<String>,
    current_labels: Value,
    coordinator_job_id: Option<Uuid>,
    coordinator_status: Option<String>,
    outcome: Option<String>,
    summary: Option<String>,
    pending_approval: Option<String>,
    tasks: Vec<WorkflowTaskSummary>,
    pull_request_number: Option<i64>,
    pull_request_url: Option<String>,
    pull_request_state: Option<String>,
    no_pr_reason: Option<String>,
    updated_at: DateTime<Utc>,
    created_at: DateTime<Utc>,
}

#[derive(Debug, Serialize)]
struct LifecycleEventPage {
    events: Vec<donkeyspace_db::LifecycleEventRecord>,
    next_before_id: Option<i64>,
}

#[derive(Debug, Serialize)]
struct ApiError {
    error: String,
}

impl ApiError {
    fn new(message: impl Into<String>) -> Self {
        Self {
            error: message.into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        AppState, EffectiveConfigurationInput, EffectiveConfigurationResponse,
        EffectiveGitHubConfiguration, FacadeResponse, GitHubActor, GitHubAppIdentity,
        GitHubComment, GitHubIssue, GitHubIssueWebhook, GitHubLabel, GitHubOwner, GitHubPollConfig,
        GitHubPollController, GitHubRepository, HumanApprovalAction, JobRecord, PolledRepository,
        authorize_engagement, can_retry_job, effective_configuration, engagement_gate,
        extract_linked_issue_number, github_app_webhook_status_response,
        github_poll_event_to_ingress, is_projected_work_item, issue_number_from_managed_branch,
        parse_human_approval_command, parse_polled_repositories, parse_repository_query,
        permission_rank, poll_backoff, polled_event_sender, polled_repository_input,
        should_queue_reviewer, should_queue_triage, webhook_installation_matches,
        webhook_repository_allowed,
    };
    use chrono::{DateTime, Utc};
    use donkeyspace_core::{DeploymentMode, EngagementGate, EngagementSelector, Policy};
    use donkeyspace_github::{GitHubAppWebhookStatus, GitHubAuthConfig, GitHubCredentialProvider};
    use serde_json::json;
    use std::{collections::HashMap, sync::Arc};
    use tokio::sync::Mutex;
    use uuid::Uuid;

    fn comment() -> GitHubComment {
        GitHubComment {
            id: Some(1),
            user: Some(GitHubActor {
                login: "human".into(),
                id: Some(1),
                kind: Some("User".into()),
            }),
            author_association: Some("MEMBER".into()),
            performed_via_github_app: None,
            body: "ordinary response".into(),
        }
    }

    #[test]
    fn github_app_status_reports_required_missing_events() {
        let response = github_app_webhook_status_response(GitHubAppWebhookStatus {
            url: Some("https://hooks.example/webhooks/github".into()),
            content_type: Some("json".into()),
            subscribed_events: vec!["issues".into(), "push".into()],
            deliveries: Vec::new(),
        });
        assert_eq!(
            response.missing_events,
            vec!["issue_comment", "pull_request"]
        );
    }

    fn engagement_state(selectors: Vec<EngagementSelector>) -> AppState {
        let mut policy =
            Policy::from_yaml(include_str!("../../../docs/policy.example.yml")).unwrap();
        policy.workflow.engagement.default.allow = selectors;
        policy.workflow.engagement.initial = None;
        let facade = policy.facade.resolve();
        let issue_command = facade.issue_command();
        AppState {
            configuration: EffectiveConfigurationResponse {
                deployment_mode: DeploymentMode::Minimal,
                policy_source: "/policy.yml".into(),
                facade: FacadeResponse {
                    display_name: facade.display_name,
                    tagline: facade.tagline,
                    issue_command,
                    branch_prefix: facade.branch_prefix,
                },
                github: EffectiveGitHubConfiguration {
                    auth_mode: "disabled",
                    ingress_mode: "disabled".into(),
                    repositories: Vec::new(),
                },
                plugin: None,
                capabilities: vec!["api".into(), "dashboard".into()],
                warnings: Vec::new(),
            },
            webhook_secret: None,
            configured_ingress_mode: "disabled".into(),
            github_auth: None,
            pool: None,
            policy,
            github_token_owner: Some("maintainer".into()),
            configured_repositories: Vec::new(),
            verification_cache: Arc::new(Mutex::new(HashMap::new())),
            github_poller: GitHubPollController::new(GitHubPollConfig {
                repositories: Vec::new(),
                interval: std::time::Duration::from_secs(8),
                max_pages: 2,
            }),
        }
    }

    #[test]
    fn minimal_configuration_is_explicit_and_redacted() {
        let policy = Policy::from_yaml(include_str!("../../../docs/policy.example.yml")).unwrap();
        let configuration = effective_configuration(
            DeploymentMode::Minimal,
            &policy,
            EffectiveConfigurationInput {
                policy_source: "/run/donkeyspace/policy.yml",
                github_auth: None,
                ingress_mode: "disabled",
                configured_repositories: &[],
                polling_repositories: &[],
                webhook_enabled: false,
            },
        )
        .unwrap();
        assert_eq!(configuration.deployment_mode, DeploymentMode::Minimal);
        assert_eq!(configuration.github.auth_mode, "disabled");
        assert_eq!(configuration.capabilities, ["api", "dashboard"]);
        assert_eq!(configuration.warnings.len(), 1);
    }

    #[test]
    fn rejects_ingress_without_authentication() {
        let policy = Policy::from_yaml(include_str!("../../../docs/policy.example.yml")).unwrap();
        let repositories = vec![PolledRepository {
            owner: "acme".into(),
            name: "rtl".into(),
        }];
        let error = effective_configuration(
            DeploymentMode::Generated,
            &policy,
            EffectiveConfigurationInput {
                policy_source: "/run/donkeyspace/policy.yml",
                github_auth: None,
                ingress_mode: "polling",
                configured_repositories: &repositories,
                polling_repositories: &repositories,
                webhook_enabled: false,
            },
        )
        .unwrap_err();
        assert!(error.contains("without GitHub authentication"));
    }

    #[tokio::test]
    async fn reports_valid_generated_configuration_without_exposing_credentials() {
        let policy = Policy::from_yaml(include_str!("../../../docs/policy.example.yml")).unwrap();
        let repositories = vec![PolledRepository {
            owner: "acme".into(),
            name: "rtl".into(),
        }];
        let auth = GitHubCredentialProvider::new(GitHubAuthConfig::Pat {
            token: "super-secret-token".into(),
        })
        .unwrap();
        let configuration = effective_configuration(
            DeploymentMode::Generated,
            &policy,
            EffectiveConfigurationInput {
                policy_source: "/run/donkeyspace/policy.yml",
                github_auth: Some(&auth),
                ingress_mode: "polling",
                configured_repositories: &repositories,
                polling_repositories: &repositories,
                webhook_enabled: false,
            },
        )
        .unwrap();
        let encoded = serde_json::to_string(&configuration).unwrap();
        assert_eq!(configuration.github.auth_mode, "pat");
        assert_eq!(configuration.github.repositories, ["acme/rtl"]);
        assert!(
            configuration
                .capabilities
                .contains(&"github_polling".into())
        );
        assert!(!encoded.contains("super-secret-token"));
    }

    #[test]
    fn rejects_missing_selected_plugin_manifest() {
        let mut policy =
            Policy::from_yaml(include_str!("../../../docs/policy.example.yml")).unwrap();
        policy.lifecycle.plugin = Some(donkeyspace_core::PluginFlowSelection {
            manifest_path: "/missing/plugin/donkeyspace-plugin.yml".into(),
            flow: "implementation".into(),
            max_handoffs_per_edge: None,
            environment: Default::default(),
            parameters: Default::default(),
            task_access_overrides: Default::default(),
        });
        let error = effective_configuration(
            DeploymentMode::Generated,
            &policy,
            EffectiveConfigurationInput {
                policy_source: "/run/donkeyspace/policy.yml",
                github_auth: None,
                ingress_mode: "disabled",
                configured_repositories: &[],
                polling_repositories: &[],
                webhook_enabled: false,
            },
        )
        .unwrap_err();
        assert!(error.contains("configured plugin manifest is unavailable"));
    }

    fn engagement_payload(sender: GitHubActor) -> GitHubIssueWebhook {
        GitHubIssueWebhook {
            action: "opened".into(),
            repository: GitHubRepository {
                name: "repo".into(),
                default_branch: "main".into(),
                owner: GitHubOwner {
                    login: "acme".into(),
                    kind: Some("Organization".into()),
                },
            },
            issue: GitHubIssue {
                id: 7,
                number: 7,
                state: "open".into(),
                body: "work".into(),
                labels: vec![GitHubLabel { name: "ai".into() }],
                user: Some(GitHubActor {
                    login: "author".into(),
                    id: Some(2),
                    kind: Some("User".into()),
                }),
                author_association: Some("NONE".into()),
                performed_via_github_app: None,
            },
            comment: None,
            label: None,
            installation: None,
            sender: Some(sender),
        }
    }

    #[tokio::test]
    async fn secure_default_allows_only_authenticated_token_owner() {
        let state = engagement_state(vec![EngagementSelector::TokenOwner]);
        let allowed = authorize_engagement(
            &state,
            EngagementGate::Initial,
            &["ai".into()],
            &engagement_payload(GitHubActor {
                login: "maintainer".into(),
                id: Some(1),
                kind: Some("User".into()),
            }),
        )
        .await;
        let denied = authorize_engagement(
            &state,
            EngagementGate::Initial,
            &["ai".into()],
            &engagement_payload(GitHubActor {
                login: "outsider".into(),
                id: Some(3),
                kind: Some("User".into()),
            }),
        )
        .await;
        let mut missing_actor = engagement_payload(GitHubActor {
            login: "unused".into(),
            id: None,
            kind: None,
        });
        missing_actor.sender = None;
        let missing_actor = authorize_engagement(
            &state,
            EngagementGate::Initial,
            &["ai".into()],
            &missing_actor,
        )
        .await;

        assert!(allowed.allowed);
        assert!(!denied.allowed);
        assert!(!missing_actor.allowed);
        assert!(missing_actor.reason.contains("missing sender"));
    }

    #[tokio::test]
    async fn github_app_selector_uses_verified_app_metadata() {
        let state = engagement_state(vec![EngagementSelector::GitHubApp {
            id: None,
            slug: Some("trusted-app".into()),
        }]);
        let mut payload = engagement_payload(GitHubActor {
            login: "trusted-app[bot]".into(),
            id: Some(4),
            kind: Some("Bot".into()),
        });
        payload.issue.performed_via_github_app = Some(GitHubAppIdentity {
            id: 10,
            slug: "trusted-app".into(),
        });

        assert!(
            authorize_engagement(&state, EngagementGate::Initial, &["ai".into()], &payload)
                .await
                .allowed
        );
    }

    #[tokio::test]
    async fn selectors_requiring_content_authors_fail_closed_when_they_are_missing() {
        let issue_author_state = engagement_state(vec![EngagementSelector::IssueAuthor]);
        let mut issue_payload = engagement_payload(GitHubActor {
            login: "author".into(),
            id: Some(2),
            kind: Some("User".into()),
        });
        issue_payload.issue.user = None;
        assert!(
            !authorize_engagement(
                &issue_author_state,
                EngagementGate::Initial,
                &["ai".into()],
                &issue_payload,
            )
            .await
            .allowed
        );

        let association_state = engagement_state(vec![EngagementSelector::AuthorAssociation {
            association: "OWNER".into(),
        }]);
        let mut comment_payload = engagement_payload(GitHubActor {
            login: "commenter".into(),
            id: Some(3),
            kind: Some("User".into()),
        });
        comment_payload.comment = Some(GitHubComment {
            id: Some(9),
            user: None,
            author_association: Some("OWNER".into()),
            performed_via_github_app: None,
            body: "clarification".into(),
        });
        assert!(
            !authorize_engagement(
                &association_state,
                EngagementGate::NeedsInfoResume,
                &["ai".into()],
                &comment_payload,
            )
            .await
            .allowed
        );
    }

    #[test]
    fn state_and_permission_ordering_are_explicit() {
        assert_eq!(
            engagement_gate("issue_comment", Some("needs_human")),
            Some(EngagementGate::NeedsHumanResume)
        );
        assert_eq!(
            engagement_gate("issues", Some("blocked")),
            Some(EngagementGate::BlockedResume)
        );
        assert!(permission_rank("maintain") > permission_rank("write"));
        assert!(permission_rank("triage") > permission_rank("read"));
    }

    #[test]
    fn parses_polled_repository_list() {
        let repositories = parse_polled_repositories("acme/rtl, acme/dv").unwrap();
        assert_eq!(repositories.len(), 2);
        assert_eq!(repositories[0].owner, "acme");
        assert_eq!(repositories[0].name, "rtl");
        assert!(parse_polled_repositories("missing-owner-separator").is_err());
        assert!(parse_polled_repositories("acme/too/many").is_err());
    }

    #[test]
    fn repository_query_selects_zero_or_one_repository() {
        assert_eq!(parse_repository_query(None).unwrap(), None);
        assert_eq!(parse_repository_query(Some("  ")).unwrap(), None);
        assert_eq!(
            parse_repository_query(Some("acme/rtl")).unwrap(),
            Some(("acme".into(), "rtl".into()))
        );
        assert!(parse_repository_query(Some("acme/rtl,acme/dv")).is_err());
        assert!(parse_repository_query(Some("invalid")).is_err());
    }

    #[test]
    fn polling_backoff_is_exponential_and_bounded() {
        let interval = std::time::Duration::from_secs(8);
        assert_eq!(poll_backoff(interval, 1).as_secs(), 8);
        assert_eq!(poll_backoff(interval, 2).as_secs(), 16);
        assert_eq!(poll_backoff(interval, 10).as_secs(), 300);
    }

    #[tokio::test]
    async fn manual_poll_requests_are_coalesced() {
        let controller = GitHubPollController::new(GitHubPollConfig {
            repositories: vec![PolledRepository {
                owner: "acme".into(),
                name: "rtl".into(),
            }],
            interval: std::time::Duration::from_secs(8),
            max_pages: 2,
        });
        assert!(!controller.trigger().await);
        assert!(controller.trigger().await);
        let status = controller.status().await;
        assert!(status.pending_manual);
        assert_eq!(status.repositories.len(), 1);
    }

    #[test]
    fn registers_polled_repository_for_configured_installation() {
        let repository = json!({
            "name": "empty-repository",
            "default_branch": "main",
            "owner": {"login": "example-owner"}
        });

        let input = polled_repository_input(&repository, Some(42)).unwrap();

        assert_eq!(input.provider, "github");
        assert_eq!(input.owner, "example-owner");
        assert_eq!(input.name, "empty-repository");
        assert_eq!(input.default_branch, "main");
        assert_eq!(input.installation_external_id.as_deref(), Some("42"));
        assert_eq!(
            input.installation_account_login.as_deref(),
            Some("example-owner")
        );
    }

    #[test]
    fn rejects_incomplete_polled_repository_metadata() {
        let repository = json!({
            "name": "empty-repository",
            "owner": {"login": "example-owner"}
        });

        assert_eq!(
            polled_repository_input(&repository, Some(42)).unwrap_err(),
            "polled github repository omitted default_branch"
        );
    }

    #[test]
    fn webhook_installation_must_match_configured_app() {
        assert!(webhook_installation_matches(
            42,
            br#"{"installation":{"id":42}}"#
        ));
        assert!(!webhook_installation_matches(
            42,
            br#"{"installation":{"id":7}}"#
        ));
        assert!(!webhook_installation_matches(42, br#"{}"#));
    }

    #[test]
    fn webhook_repository_must_be_selected_when_scope_is_configured() {
        let configured = vec![PolledRepository {
            owner: "acme".into(),
            name: "rtl".into(),
        }];
        assert!(webhook_repository_allowed(
            &configured,
            br#"{"repository":{"owner":{"login":"ACME"},"name":"RTL"}}"#
        ));
        assert!(!webhook_repository_allowed(
            &configured,
            br#"{"repository":{"owner":{"login":"acme"},"name":"other"}}"#
        ));
        assert!(webhook_repository_allowed(&[], br#"{}"#));
    }

    #[test]
    fn adapts_polled_issue_event_to_webhook_shape() {
        let repository = json!({
            "name": "rtl",
            "default_branch": "main",
            "owner": {"login": "acme"}
        });
        let event = json!({
            "id": "12345",
            "type": "IssuesEvent",
            "actor": {"login": "alice", "id": 1, "type": "User"},
            "payload": {
                "action": "opened",
                "issue": {"id": 7, "number": 3, "state": "open", "labels": []}
            }
        });

        let ingress = github_poll_event_to_ingress("acme", "rtl", &repository, &event).unwrap();
        assert_eq!(ingress.event_name, "issues");
        assert_eq!(ingress.delivery_id, "github-poll:acme/rtl:12345");
        assert_eq!(ingress.payload["repository"]["default_branch"], "main");
        assert_eq!(ingress.payload["issue"]["number"], 3);
        assert_eq!(ingress.payload["sender"]["login"], "alice");
    }

    #[test]
    fn restores_missing_polled_sender_type_from_matching_issue_author() {
        let repository = json!({
            "name": "rtl",
            "default_branch": "main",
            "owner": {"login": "acme", "type": "Organization"}
        });
        let event = json!({
            "id": "12346",
            "type": "IssuesEvent",
            "actor": {"login": "alice", "id": 1},
            "payload": {
                "action": "labeled",
                "issue": {
                    "id": 7,
                    "number": 3,
                    "state": "open",
                    "labels": [{"name": "ai"}],
                    "user": {"login": "alice", "id": 1, "type": "User"}
                },
                "label": {"name": "ai"}
            }
        });

        let ingress = github_poll_event_to_ingress("acme", "rtl", &repository, &event).unwrap();

        assert_eq!(ingress.payload["sender"]["type"], "User");
    }

    #[test]
    fn does_not_copy_sender_type_from_a_different_issue_author() {
        let event = json!({
            "actor": {"login": "maintainer", "id": 1},
            "payload": {
                "issue": {
                    "user": {"login": "author", "id": 2, "type": "User"}
                }
            }
        });

        let sender = polled_event_sender(&event).unwrap();

        assert!(sender.get("type").is_none());
    }

    #[test]
    fn accepts_null_issue_body_from_github() {
        let payload: GitHubIssueWebhook = serde_json::from_value(json!({
            "action": "opened",
            "repository": {
                "name": "rtl",
                "default_branch": "main",
                "owner": {"login": "acme"}
            },
            "issue": {
                "id": 7,
                "number": 34,
                "state": "open",
                "body": null,
                "labels": [{"name": "ai"}]
            }
        }))
        .unwrap();
        assert_eq!(payload.issue.body, "");
    }

    #[test]
    fn preserves_changed_label_in_polled_issue_event() {
        let repository = json!({
            "name": "rtl",
            "default_branch": "main",
            "owner": {"login": "acme"}
        });
        let event = json!({
            "id": "12346",
            "type": "IssuesEvent",
            "actor": {"login": "alice", "id": 1, "type": "User"},
            "payload": {
                "action": "labeled",
                "issue": {"id": 7, "number": 3, "state": "open", "labels": [{"name": "ai"}]},
                "label": {"name": "ai"}
            }
        });

        let ingress = github_poll_event_to_ingress("acme", "rtl", &repository, &event).unwrap();
        assert_eq!(ingress.payload["label"]["name"], "ai");
    }

    #[test]
    fn adapts_polled_push_head_to_webhook_after_sha() {
        let repository = json!({
            "name": "rtl",
            "default_branch": "main",
            "owner": {"login": "acme"}
        });
        let event = json!({
            "id": "67890",
            "type": "PushEvent",
            "actor": {"login": "alice", "id": 1, "type": "User"},
            "payload": {"ref": "refs/heads/main", "head": "abc123"}
        });

        let ingress = github_poll_event_to_ingress("acme", "rtl", &repository, &event).unwrap();
        assert_eq!(ingress.event_name, "push");
        assert_eq!(ingress.payload["after"], "abc123");
    }

    fn queue_triage(
        event: &str,
        action: &str,
        issue_state: &str,
        current_state: Option<&str>,
        comment: Option<&GitHubComment>,
    ) -> bool {
        should_queue_triage(
            event,
            action,
            issue_state,
            current_state,
            comment,
            None,
            (&[], "donkeyspace"),
        )
    }

    #[test]
    fn issue_opened_queues_triage() {
        assert!(queue_triage("issues", "opened", "open", None, None));
    }

    #[test]
    fn closed_issue_does_not_queue_triage() {
        assert!(!queue_triage(
            "issues",
            "edited",
            "closed",
            Some("ready"),
            None
        ));
    }

    #[test]
    fn non_allow_label_events_do_not_queue_triage() {
        assert!(!should_queue_triage(
            "issues",
            "labeled",
            "open",
            Some("needs_info"),
            None,
            Some("bug"),
            (&["ai".to_string()], "donkeyspace"),
        ));
    }

    #[test]
    fn allow_label_event_queues_triage_for_unstarted_issue() {
        assert!(should_queue_triage(
            "issues",
            "labeled",
            "open",
            None,
            None,
            Some("ai"),
            (&["ai".to_string()], "donkeyspace"),
        ));
    }

    #[test]
    fn allow_label_event_does_not_queue_triage_for_active_issue() {
        assert!(!should_queue_triage(
            "issues",
            "labeled",
            "open",
            Some("pr_open"),
            None,
            Some("ai"),
            (&["ai".to_string()], "donkeyspace"),
        ));
    }

    #[test]
    fn human_comment_on_needs_info_queues_triage() {
        assert!(queue_triage(
            "issue_comment",
            "created",
            "open",
            Some("needs_info"),
            Some(&comment()),
        ));
    }

    #[test]
    fn human_comment_on_blocked_queues_triage() {
        assert!(queue_triage(
            "issue_comment",
            "created",
            "open",
            Some("blocked"),
            Some(&comment()),
        ));
    }

    #[test]
    fn explicit_approval_on_needs_human_queues_triage() {
        let mut approval = comment();
        approval.body = "/donkeyspace approve".into();
        assert!(queue_triage(
            "issue_comment",
            "created",
            "open",
            Some("needs_human"),
            Some(&approval),
        ));
    }

    #[test]
    fn ordinary_or_edited_comment_on_needs_human_does_not_resume() {
        assert!(!queue_triage(
            "issue_comment",
            "created",
            "open",
            Some("needs_human"),
            Some(&comment()),
        ));
        let mut approval = comment();
        approval.body = "/donkeyspace approve".into();
        assert!(!queue_triage(
            "issue_comment",
            "edited",
            "open",
            Some("needs_human"),
            Some(&approval),
        ));
    }

    #[test]
    fn parses_explicit_approval_commands() {
        assert_eq!(
            parse_human_approval_command(" /donkeyspace approve rtl/storage ", "donkeyspace"),
            Some(HumanApprovalAction::Approve {
                target: Some("rtl/storage".into())
            })
        );
        assert_eq!(
            parse_human_approval_command(
                "/donkeyspace revise architect\nSplit the register file from decode.",
                "donkeyspace",
            ),
            Some(HumanApprovalAction::Revise {
                target: Some("architect".into()),
                feedback: "Split the register file from decode.".into()
            })
        );
        assert!(
            parse_human_approval_command("/donkeyspace revise architect", "donkeyspace").is_none()
        );
        assert!(
            parse_human_approval_command("please /donkeyspace approve", "donkeyspace").is_none()
        );
        assert!(
            parse_human_approval_command("/donkeyspace approve too many", "donkeyspace").is_none()
        );
        assert!(parse_human_approval_command("/example-agent approve", "example-agent").is_some());
        assert!(parse_human_approval_command("/donkeyspace approve", "example-agent").is_none());
    }

    #[test]
    fn edited_human_comment_on_blocked_queues_triage() {
        assert!(queue_triage(
            "issue_comment",
            "edited",
            "open",
            Some("blocked"),
            Some(&comment()),
        ));
    }

    #[test]
    fn human_comment_without_retriable_state_does_not_queue_triage() {
        assert!(!queue_triage(
            "issue_comment",
            "created",
            "open",
            Some("ready"),
            Some(&comment()),
        ));
    }

    #[test]
    fn missing_comment_payload_does_not_queue_triage() {
        assert!(!queue_triage(
            "issue_comment",
            "created",
            "open",
            Some("blocked"),
            None,
        ));
    }

    #[test]
    fn comment_body_prefix_is_not_used_for_trigger_classification() {
        assert!(queue_triage(
            "issue_comment",
            "created",
            "open",
            Some("blocked"),
            Some(&comment()),
        ));
    }

    #[test]
    fn projected_work_item_marker_prevents_recursive_lifecycle() {
        assert!(is_projected_work_item(
            "<!-- donkeyspace-work-item -->\n\nBlock specification"
        ));
        assert!(!is_projected_work_item("ordinary issue body"));
    }

    #[test]
    fn pull_request_webhook_queues_reviewer_for_managed_open_pr() {
        assert!(should_queue_reviewer(
            "synchronize",
            "open",
            false,
            true,
            true
        ));
    }

    #[test]
    fn pull_request_webhook_skips_unmanaged_or_draft_pr() {
        assert!(!should_queue_reviewer(
            "synchronize",
            "open",
            false,
            false,
            true
        ));
        assert!(!should_queue_reviewer("opened", "open", true, true, true));
    }

    #[test]
    fn extracts_linked_issue_from_pr_text_and_branch() {
        assert_eq!(extract_linked_issue_number("Closes #12"), Some(12));
        assert_eq!(
            issue_number_from_managed_branch("example-agent/issue-12-019e399e", "example-agent"),
            Some(12)
        );
    }

    fn job_with_status_and_outcome(status: &str, outcome: Option<&str>) -> JobRecord {
        JobRecord {
            id: Uuid::now_v7(),
            workflow_item_id: Some(1),
            retry_of_job_id: None,
            role: "developer".to_string(),
            status: status.to_string(),
            lease_owner: None,
            lease_expires_at: None,
            input: json!({}),
            result: outcome.map(|outcome| json!({ "outcome": outcome })),
            created_at: DateTime::<Utc>::from_timestamp(0, 0).expect("timestamp"),
            updated_at: DateTime::<Utc>::from_timestamp(0, 0).expect("timestamp"),
        }
    }

    #[test]
    fn failed_jobs_can_be_retried_when_not_blocked_or_needs_human() {
        assert!(can_retry_job(&job_with_status_and_outcome(
            "failed",
            Some("failed")
        )));
        assert!(can_retry_job(&job_with_status_and_outcome("failed", None)));
    }

    #[test]
    fn blocked_or_needs_human_jobs_are_not_retried() {
        assert!(!can_retry_job(&job_with_status_and_outcome(
            "failed",
            Some("blocked")
        )));
        assert!(!can_retry_job(&job_with_status_and_outcome(
            "failed",
            Some("needs_human")
        )));
    }
}
