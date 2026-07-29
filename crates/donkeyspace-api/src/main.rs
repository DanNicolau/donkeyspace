use axum::{
    Json, Router,
    body::Bytes,
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    routing::{get, post},
};
use donkeyspace_core::{
    AgentRole, EngagementRule, LabelState, PluginManifest, Policy, WorkflowState,
    normalize_workflow_labels,
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
use donkeyspace_github::GitHubClient;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{env, fs, net::SocketAddr, sync::Arc, time::Duration};
use tower_http::trace::TraceLayer;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};
use uuid::Uuid;

#[derive(Debug, Clone)]
struct AppState {
    webhook_secret: Option<String>,
    pool: Option<PgPool>,
    policy: Policy,
    github_token_owner: Option<String>,
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
    let github_token_owner = resolve_github_token_owner(&policy).await?;
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

    let state = Arc::new(AppState {
        webhook_secret: env::var("DONKEYSPACE_WEBHOOK_SECRET").ok(),
        pool,
        policy,
        github_token_owner,
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

async fn resolve_github_token_owner(
    policy: &Policy,
) -> Result<Option<String>, Box<dyn std::error::Error>> {
    if !policy.workflow.engagement.requires_token_owner() {
        return Ok(None);
    }
    let token = env::var("DONKEYSPACE_GITHUB_TOKEN")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .ok_or(
            "workflow engagement policy uses token_owner but DONKEYSPACE_GITHUB_TOKEN is unset",
        )?;
    let login = GitHubClient::new(token)?
        .authenticated_login()
        .await
        .map_err(|error| {
            format!("workflow engagement policy could not resolve token_owner: {error}")
        })?;
    tracing::info!(
        github_login = login,
        "resolved authorized GitHub token owner"
    );
    Ok(Some(login))
}

fn start_github_poller(state: Arc<AppState>) -> Result<(), Box<dyn std::error::Error>> {
    let config = GitHubPollConfig::from_env()?;
    if config.repositories.is_empty() {
        return Ok(());
    }
    if state.pool.is_none() {
        return Err("github polling requires DONKEYSPACE_DATABASE_URL".into());
    }
    let token = env::var("DONKEYSPACE_GITHUB_TOKEN")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .ok_or("github polling requires DONKEYSPACE_GITHUB_TOKEN")?;
    let client = GitHubClient::new(token)?;

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
            let Some(ingress) = github_poll_event_to_ingress(
                &repository.owner,
                &repository.name,
                &repository_payload,
                &event,
            ) else {
                continue;
            };
            let body = serde_json::to_vec(&ingress.payload)?;
            match persist_github_webhook(
                pool,
                &state.policy,
                state.github_token_owner.as_deref(),
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
                "sender": event.get("actor")?,
            }),
        ),
        "IssueCommentEvent" => (
            "issue_comment",
            json!({
                "action": source.get("action")?,
                "repository": repository,
                "issue": source.get("issue")?,
                "comment": source.get("comment")?,
                "sender": event.get("actor")?,
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

    let Some(pool) = &state.pool else {
        tracing::info!(
            event,
            delivery,
            bytes = body.len(),
            "accepted github webhook without database"
        );
        return StatusCode::ACCEPTED;
    };

    match persist_github_webhook(
        pool,
        &state.policy,
        state.github_token_owner.as_deref(),
        event,
        delivery,
        &body,
    )
    .await
    {
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

fn init_tracing() {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "donkeyspace=info,tower_http=info".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();
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
    token_owner: Option<&str>,
    event: &str,
    delivery: &str,
    body: &[u8],
) -> Result<WebhookPersistOutcome, Box<dyn std::error::Error>> {
    match event {
        "issues" | "issue_comment" => {
            persist_issue_webhook(pool, policy, token_owner, event, delivery, body).await
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
    token_owner: Option<&str>,
    event: &str,
    delivery: &str,
    body: &[u8],
) -> Result<WebhookPersistOutcome, Box<dyn std::error::Error>> {
    let payload: GitHubIssueWebhook = serde_json::from_slice(body)?;
    let payload_value: Value = serde_json::from_slice(body)?;
    let repository_id = upsert_repository(
        pool,
        &RepositoryInput {
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

    let authorization =
        engagement_authorization(policy, current_state.as_deref(), &payload, token_owner);
    if !authorization.allowed {
        let audit_state = current_state.as_deref().unwrap_or("unstarted");
        record_state_transition(
            pool,
            workflow_item_id,
            None,
            current_state.as_deref(),
            audit_state,
            &format!("AI engagement denied: {}", authorization.reason),
        )
        .await?;
        tracing::info!(
            event,
            action = payload.action,
            issue_number = payload.issue.number,
            reason = authorization.reason,
            "actor was not authorized to trigger agent work"
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
            &format!(
                "resumed paused plugin lifecycle from human comment; {}",
                authorization.reason
            ),
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
        &format!(
            "queued {initial_role} job from github webhook; {}",
            authorization.reason
        ),
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

struct EngagementAuthorization {
    allowed: bool,
    reason: String,
}

fn engagement_authorization(
    policy: &Policy,
    current_state: Option<&str>,
    payload: &GitHubIssueWebhook,
    token_owner: Option<&str>,
) -> EngagementAuthorization {
    let (path, rule) = match current_state {
        Some("needs_info") => ("clarification", &policy.workflow.engagement.clarification),
        Some("blocked") => ("blocked_resume", &policy.workflow.engagement.blocked_resume),
        Some("needs_human") => (
            "human_authorization",
            &policy.workflow.engagement.human_authorization,
        ),
        _ => ("initial", &policy.workflow.engagement.initial),
    };
    authorize_actor(path, rule, payload, token_owner)
}

fn authorize_actor(
    path: &str,
    rule: &EngagementRule,
    payload: &GitHubIssueWebhook,
    token_owner: Option<&str>,
) -> EngagementAuthorization {
    let Some(actor) = payload.sender.as_ref() else {
        return EngagementAuthorization {
            allowed: false,
            reason: format!("{path} policy requires actor metadata, but sender is missing"),
        };
    };
    let association = payload
        .comment
        .as_ref()
        .and_then(|comment| comment.author_association.as_deref())
        .or(payload.issue.author_association.as_deref());
    let is_bot = matches!(actor.actor_type.as_str(), "Bot" | "App");
    let matches = (rule.any && !is_bot)
        || (rule.token_owner
            && token_owner.is_some_and(|login| login.eq_ignore_ascii_case(&actor.login)))
        || (!is_bot
            && rule.issue_author
            && payload
                .issue
                .user
                .as_ref()
                .is_some_and(|author| author.login.eq_ignore_ascii_case(&actor.login)))
        || (!is_bot
            && rule
                .users
                .iter()
                .any(|login| login.eq_ignore_ascii_case(&actor.login)))
        || (is_bot
            && rule
                .trusted_bots
                .iter()
                .any(|login| login.eq_ignore_ascii_case(&actor.login)))
        || (!is_bot
            && association.is_some_and(|value| {
                rule.author_associations
                    .iter()
                    .any(|allowed| allowed.eq_ignore_ascii_case(value))
            }));

    EngagementAuthorization {
        allowed: matches,
        reason: if matches {
            format!("{path} engagement authorized actor `{}`", actor.login)
        } else {
            format!("{path} policy does not authorize actor `{}`", actor.login)
        },
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
    comment.body.contains("<!-- donkeyspace-generated -->")
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
    sender: Option<GitHubActor>,
}

#[derive(Debug, Deserialize)]
struct GitHubPullRequestWebhook {
    action: String,
    repository: GitHubRepository,
    pull_request: GitHubPullRequest,
}

#[derive(Debug, Deserialize)]
struct GitHubPushWebhook {
    #[serde(rename = "ref")]
    git_ref: String,
    after: String,
    repository: GitHubRepository,
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
    #[serde(default)]
    body: String,
    labels: Vec<GitHubLabel>,
    #[serde(default)]
    user: Option<GitHubActor>,
    #[serde(default)]
    author_association: Option<String>,
}

#[derive(Debug, Deserialize)]
struct GitHubLabel {
    name: String,
}

#[derive(Debug, Default, Deserialize)]
struct GitHubComment {
    body: String,
    #[serde(default)]
    author_association: Option<String>,
}

#[derive(Debug, Deserialize)]
struct GitHubActor {
    login: String,
    #[serde(default, rename = "type")]
    actor_type: String,
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
        engagement_authorization, extract_linked_issue_number, github_poll_event_to_ingress,
        is_projected_work_item, issue_number_from_donkeyspace_branch, parse_polled_repositories,
        should_queue_reviewer, should_queue_triage,
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
    fn adapts_polled_issue_event_to_webhook_shape() {
        let repository = json!({
            "name": "rtl",
            "default_branch": "main",
            "owner": {"login": "acme"}
        });
        let event = json!({
            "id": "12345",
            "type": "IssuesEvent",
            "actor": {"login": "octocat", "type": "User"},
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
        assert_eq!(ingress.payload["sender"]["login"], "octocat");
    }

    #[test]
    fn token_owner_is_the_default_authorized_actor() {
        let policy =
            donkeyspace_core::Policy::from_yaml(include_str!("../../../docs/policy.example.yml"))
                .unwrap();
        let payload: GitHubIssueWebhook = serde_json::from_value(json!({
            "action": "labeled",
            "repository": {"name": "repo", "default_branch": "main", "owner": {"login": "owner"}},
            "issue": {"id": 1, "number": 1, "state": "open", "body": "", "labels": [], "user": {"login": "author", "type": "User"}},
            "sender": {"login": "octocat", "type": "User"}
        }))
        .unwrap();

        assert!(engagement_authorization(&policy, None, &payload, Some("octocat")).allowed);
        assert!(!engagement_authorization(&policy, None, &payload, Some("someone-else")).allowed);
    }

    #[test]
    fn missing_actor_metadata_fails_closed() {
        let policy =
            donkeyspace_core::Policy::from_yaml(include_str!("../../../docs/policy.example.yml"))
                .unwrap();
        let payload: GitHubIssueWebhook = serde_json::from_value(json!({
            "action": "opened",
            "repository": {"name": "repo", "default_branch": "main", "owner": {"login": "owner"}},
            "issue": {"id": 1, "number": 1, "state": "open", "body": "", "labels": []}
        }))
        .unwrap();
        assert!(!engagement_authorization(&policy, None, &payload, Some("octocat")).allowed);
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
            "actor": {"login": "octocat", "type": "User"},
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
                ..Default::default()
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
                ..Default::default()
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
                ..Default::default()
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
                ..Default::default()
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
                ..Default::default()
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
                body: "donkeyspace triage needs clarification.\n\n<!-- donkeyspace-generated -->"
                    .to_string(),
                ..Default::default()
            }),
        ));
    }

    #[test]
    fn detects_donkeyspace_generated_comment_after_whitespace() {
        assert!(comment_is_from_donkeyspace(&GitHubComment {
            body: "\n  donkeyspace marked this issue ready.\n\n<!-- donkeyspace-generated -->"
                .to_string(),
            ..Default::default()
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
