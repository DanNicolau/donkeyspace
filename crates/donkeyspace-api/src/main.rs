use axum::{
    Json, Router,
    body::Bytes,
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    routing::{get, post},
};
use donkeyspace_core::{
    AgentRole, LabelState, PluginManifest, Policy, WorkflowState, normalize_workflow_labels,
};
use donkeyspace_db::{
    DbConfig, JobRecord, PgPool, PullRequestInput, RepositoryInput, WorkflowItemInput,
    acquire_job_lease, active_job_exists_for_workflow_item, apply_migrations, connect, create_job,
    create_retry_job, get_job, get_workflow_item_by_issue_number, get_workflow_item_state,
    latest_workflow_job_input, list_job_command_results, list_job_outbound_actions,
    list_job_transitions, list_jobs, list_open_managed_pull_requests_for_base,
    list_recent_outbound_actions, record_state_transition, record_webhook_delivery,
    repair_job_exists_for_pr_base, resume_latest_paused_job, reviewer_job_exists_for_pr_head,
    upsert_pull_request, upsert_repository, upsert_workflow_item,
};
use donkeyspace_github::{GitHubClient, GitHubCredentialProvider};
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::{Value, json};
use std::{env, fs, net::SocketAddr, sync::Arc, time::Duration};
use tower_http::trace::TraceLayer;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};
use uuid::Uuid;

#[derive(Debug, Clone)]
struct AppState {
    webhook_secret: Option<String>,
    github_auth: Option<GitHubCredentialProvider>,
    pool: Option<PgPool>,
    policy: Policy,
}

#[derive(Debug, Serialize)]
struct HealthResponse {
    status: &'static str,
    service: &'static str,
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

#[derive(Debug)]
struct GitHubIngressEvent {
    event_name: &'static str,
    delivery_id: String,
    payload: Value,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    init_tracing();

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
    let state = Arc::new(AppState {
        webhook_secret: load_optional_secret(
            "DONKEYSPACE_WEBHOOK_SECRET",
            "DONKEYSPACE_WEBHOOK_SECRET_FILE",
        )?,
        github_auth,
        pool,
        policy,
    });

    start_github_poller(state.clone())?;

    let app = Router::new()
        .route("/healthz", get(healthz))
        .route("/api/runs", get(api_runs))
        .route("/api/outbound-actions", get(api_outbound_actions))
        .route("/api/runs/{id}", get(api_run))
        .route("/api/runs/{id}/transitions", get(api_run_transitions))
        .route("/api/runs/{id}/lease", post(api_lease_run))
        .route("/api/runs/{id}/retry", post(api_retry_run))
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
    let config = GitHubPollConfig::from_env()?;
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
    tokio::spawn(async move {
        loop {
            if let Err(error) = poll_github_events(&state, &client, &config).await {
                tracing::error!(%error, "github event poll failed");
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
            .parse::<u64>()?
            .max(5);
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

async fn poll_github_events(
    state: &AppState,
    client: &GitHubClient,
    config: &GitHubPollConfig,
) -> Result<(), Box<dyn std::error::Error>> {
    let pool = state.pool.as_ref().ok_or("database is not configured")?;
    for repository in &config.repositories {
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
        let mut events = client
            .repository_events(&repository.owner, &repository.name, config.max_pages)
            .await?;
        events.reverse();

        for mut event in events {
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
            match persist_github_webhook(
                pool,
                &state.policy,
                ingress.event_name,
                &ingress.delivery_id,
                &body,
            )
            .await?
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
    }
    Ok(())
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
        "owner": {"login": repository.pointer("/owner/login")?},
    });
    let (event_name, payload) = match event_type {
        "IssuesEvent" => (
            "issues",
            json!({
                "action": source.get("action")?,
                "repository": repository,
                "issue": source.get("issue")?,
                "label": source.get("label").cloned().unwrap_or(Value::Null),
            }),
        ),
        "IssueCommentEvent" => (
            "issue_comment",
            json!({
                "action": source.get("action")?,
                "repository": repository,
                "issue": source.get("issue")?,
                "comment": source.get("comment")?,
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

async fn healthz() -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "ok",
        service: "donkeyspace-api",
    })
}

async fn api_runs(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let Some(pool) = &state.pool else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ApiError::new("database is not configured")),
        )
            .into_response();
    };

    match list_jobs(pool, 50).await {
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

            Json(RunDetail {
                job,
                transitions,
                outbound_actions,
                command_results,
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

async fn api_outbound_actions(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let Some(pool) = &state.pool else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ApiError::new("database is not configured")),
        )
            .into_response();
    };

    match list_recent_outbound_actions(pool, 50).await {
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
    {
        if !webhook_installation_matches(expected, &body) {
            let actual = serde_json::from_slice::<Value>(&body)
                .ok()
                .and_then(|payload| payload.pointer("/installation/id").and_then(Value::as_u64));
            tracing::warn!(expected, ?actual, "github webhook installation rejected");
            return StatusCode::FORBIDDEN;
        }
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

    match persist_github_webhook(pool, &state.policy, event, delivery, &body).await {
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
    Ok(Policy::from_yaml(&raw)?)
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
    policy: &Policy,
    event: &str,
    delivery: &str,
    body: &[u8],
) -> Result<WebhookPersistOutcome, Box<dyn std::error::Error>> {
    match event {
        "issues" | "issue_comment" => {
            persist_issue_webhook(pool, policy, event, delivery, body).await
        }
        "pull_request" => persist_pull_request_webhook(pool, policy, event, delivery, body).await,
        "push" => persist_push_webhook(pool, policy, event, delivery, body).await,
        _ => {
            let payload: Value = serde_json::from_slice(body)?;
            let inserted = record_webhook_delivery(pool, None, delivery, event, &payload).await?;
            Ok(if inserted {
                WebhookPersistOutcome::Ignored
            } else {
                WebhookPersistOutcome::Duplicate
            })
        }
    }
}

async fn persist_issue_webhook(
    pool: &PgPool,
    policy: &Policy,
    event: &str,
    delivery: &str,
    body: &[u8],
) -> Result<WebhookPersistOutcome, Box<dyn std::error::Error>> {
    let payload: GitHubIssueWebhook = serde_json::from_slice(body)?;
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
            default_branch: payload.repository.default_branch,
        },
    )
    .await?;

    let inserted =
        record_webhook_delivery(pool, Some(repository_id), delivery, event, &payload_value).await?;
    if !inserted {
        return Ok(WebhookPersistOutcome::Duplicate);
    }

    let labels = payload
        .issue
        .labels
        .into_iter()
        .map(|label| label.name)
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

    if is_projected_work_item(&payload.issue.body) {
        tracing::info!(
            issue_number = payload.issue.number,
            "projected plugin work-item issue did not queue an independent lifecycle"
        );
        return Ok(WebhookPersistOutcome::Ignored);
    }

    let automation_decision = policy.automation_decision_for_labels(&labels);
    if !automation_decision.is_allowed() {
        tracing::info!(
            event,
            action = payload.action,
            issue_number = payload.issue.number,
            reason = automation_decision.reason(),
            "policy did not allow agent work"
        );
        return Ok(WebhookPersistOutcome::Ignored);
    }

    if !should_queue_triage(
        event,
        &payload.action,
        &payload.issue.state,
        current_state.as_deref(),
        payload.comment.as_ref(),
        payload.label.as_ref().map(|label| label.name.as_str()),
        &policy.workflow.allow_labels,
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

    if active_job_exists_for_workflow_item(pool, workflow_item_id).await? {
        tracing::info!(
            event,
            action = payload.action,
            issue_number = payload.issue.number,
            "active workflow job already exists; duplicate trigger ignored"
        );
        return Ok(WebhookPersistOutcome::Ignored);
    }

    if event == "issue_comment"
        && current_state.as_deref() == Some(WorkflowState::NeedsHuman.as_str())
        && payload
            .comment
            .as_ref()
            .map(is_human_comment)
            .unwrap_or(false)
        && let Some(job) = resume_latest_paused_job(pool, workflow_item_id, &payload_value).await?
    {
        record_state_transition(
            pool,
            workflow_item_id,
            Some(job.id),
            current_state.as_deref(),
            "lifecycle_resumed",
            "resumed paused plugin lifecycle from human comment",
        )
        .await?;
        return Ok(WebhookPersistOutcome::Queued(job));
    }

    let initial_role =
        lifecycle_start_role(policy)?.unwrap_or_else(|| AgentRole::Triage.as_str().to_string());
    let job = create_job(pool, Some(workflow_item_id), &initial_role, &payload_value).await?;
    record_state_transition(
        pool,
        workflow_item_id,
        Some(job.id),
        current_state.as_deref(),
        &format!("{initial_role}_queued"),
        &format!("queued {initial_role} job from github webhook"),
    )
    .await?;

    Ok(WebhookPersistOutcome::Queued(job))
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
    if !inserted {
        return Ok(WebhookPersistOutcome::Duplicate);
    }

    let managed = pull_request_is_managed(&payload.pull_request);
    let linked_issue_number = payload
        .pull_request
        .body
        .as_deref()
        .and_then(extract_linked_issue_number)
        .or_else(|| issue_number_from_donkeyspace_branch(&payload.pull_request.head.ref_name));
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

    Ok(WebhookPersistOutcome::Queued(job))
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
    if !inserted {
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
        .map(WebhookPersistOutcome::Queued)
        .unwrap_or(WebhookPersistOutcome::Ignored))
}

fn should_queue_triage(
    event: &str,
    action: &str,
    issue_state: &str,
    current_state: Option<&str>,
    comment: Option<&GitHubComment>,
    changed_label: Option<&str>,
    allow_labels: &[String],
) -> bool {
    if issue_state == "closed" {
        return false;
    }

    match (event, action) {
        ("issues", "opened" | "edited" | "reopened") => true,
        ("issues", "labeled") => {
            changed_label
                .map(|label| allow_labels.iter().any(|allowed| allowed == label))
                .unwrap_or(false)
                && matches!(
                    current_state,
                    None | Some("needs_info" | "needs_human" | "blocked")
                )
        }
        ("issue_comment", "created" | "edited") => {
            matches!(
                current_state,
                Some(state)
                    if matches!(
                        state,
                        "needs_info" | "needs_human" | "blocked"
                    )
            ) && comment.map(is_human_comment).unwrap_or(false)
        }
        _ => false,
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

fn is_human_comment(comment: &GitHubComment) -> bool {
    !comment_is_from_donkeyspace(comment)
}

fn comment_is_from_donkeyspace(comment: &GitHubComment) -> bool {
    comment.body.trim_start().starts_with("donkeyspace ")
}

fn pull_request_is_managed(pull_request: &GitHubPullRequest) -> bool {
    pull_request.head.ref_name.starts_with("donkeyspace/issue-")
        || pull_request
            .body
            .as_deref()
            .map(|body| body.contains("Generated by donkeyspace developer job"))
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

fn issue_number_from_donkeyspace_branch(branch: &str) -> Option<i64> {
    let suffix = branch.strip_prefix("donkeyspace/issue-")?;
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
    Queued(JobRecord),
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
}

#[derive(Debug, Deserialize)]
struct GitHubIssue {
    id: i64,
    number: i64,
    state: String,
    #[serde(default, deserialize_with = "deserialize_null_default")]
    body: String,
    labels: Vec<GitHubLabel>,
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
    body: String,
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
        GitHubComment, GitHubIssueWebhook, JobRecord, can_retry_job, comment_is_from_donkeyspace,
        extract_linked_issue_number, github_poll_event_to_ingress, is_projected_work_item,
        issue_number_from_donkeyspace_branch, parse_polled_repositories, polled_repository_input,
        should_queue_reviewer, should_queue_triage, webhook_installation_matches,
    };
    use chrono::{DateTime, Utc};
    use serde_json::json;
    use uuid::Uuid;

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
    fn adapts_polled_issue_event_to_webhook_shape() {
        let repository = json!({
            "name": "rtl",
            "default_branch": "main",
            "owner": {"login": "acme"}
        });
        let event = json!({
            "id": "12345",
            "type": "IssuesEvent",
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
            &[],
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
            &["ai".to_string()],
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
            &["ai".to_string()],
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
            &["ai".to_string()],
        ));
    }

    #[test]
    fn human_comment_on_needs_info_queues_triage() {
        assert!(queue_triage(
            "issue_comment",
            "created",
            "open",
            Some("needs_info"),
            Some(&GitHubComment {
                body: "Here are the reproduction steps.".to_string(),
            }),
        ));
    }

    #[test]
    fn human_comment_on_blocked_queues_triage() {
        assert!(queue_triage(
            "issue_comment",
            "created",
            "open",
            Some("blocked"),
            Some(&GitHubComment {
                body: "I added the missing detail.".to_string(),
            }),
        ));
    }

    #[test]
    fn human_comment_on_needs_human_queues_triage() {
        assert!(queue_triage(
            "issue_comment",
            "created",
            "open",
            Some("needs_human"),
            Some(&GitHubComment {
                body: "N+2 is acceptable and makes sense.".to_string(),
            }),
        ));
    }

    #[test]
    fn edited_human_comment_on_blocked_queues_triage() {
        assert!(queue_triage(
            "issue_comment",
            "edited",
            "open",
            Some("blocked"),
            Some(&GitHubComment {
                body: "Updated with more details.".to_string(),
            }),
        ));
    }

    #[test]
    fn human_comment_without_retriable_state_does_not_queue_triage() {
        assert!(!queue_triage(
            "issue_comment",
            "created",
            "open",
            Some("ready"),
            Some(&GitHubComment {
                body: "Looks good.".to_string(),
            }),
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
    fn donkeyspace_comment_does_not_queue_triage() {
        assert!(!queue_triage(
            "issue_comment",
            "created",
            "open",
            Some("blocked"),
            Some(&GitHubComment {
                body: "donkeyspace triage needs clarification before this issue can move to implementation.".to_string(),
            }),
        ));
    }

    #[test]
    fn detects_donkeyspace_generated_comment_after_whitespace() {
        assert!(comment_is_from_donkeyspace(&GitHubComment {
            body: "\n  donkeyspace marked this issue ready for agent implementation.".to_string(),
        }));
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
            issue_number_from_donkeyspace_branch("donkeyspace/issue-12-019e399e"),
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
