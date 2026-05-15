use axum::{
    Json, Router,
    body::Bytes,
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    routing::{get, post},
};
use donkeyspace_core::{AgentRole, LabelState, Policy, WorkflowState, normalize_workflow_labels};
use donkeyspace_db::{
    DbConfig, JobRecord, PgPool, RepositoryInput, WorkflowItemInput, acquire_job_lease,
    apply_migrations, connect, create_job, get_job, list_job_outbound_actions,
    list_job_transitions, list_jobs, list_recent_outbound_actions, record_state_transition,
    record_webhook_delivery, upsert_repository, upsert_workflow_item,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{env, fs, net::SocketAddr, sync::Arc};
use tower_http::trace::TraceLayer;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};
use uuid::Uuid;

#[derive(Debug, Clone)]
struct AppState {
    webhook_secret: Option<String>,
    pool: Option<PgPool>,
    policy: Policy,
}

#[derive(Debug, Serialize)]
struct HealthResponse {
    status: &'static str,
    service: &'static str,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    init_tracing();

    let policy = load_policy()?;
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
    });

    let app = Router::new()
        .route("/healthz", get(healthz))
        .route("/api/runs", get(api_runs))
        .route("/api/outbound-actions", get(api_outbound_actions))
        .route("/api/runs/{id}", get(api_run))
        .route("/api/runs/{id}/transitions", get(api_run_transitions))
        .route("/api/runs/{id}/lease", post(api_lease_run))
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

            Json(RunDetail {
                job,
                transitions,
                outbound_actions,
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

    match persist_issue_webhook(pool, &state.policy, event, delivery, &body).await {
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

async fn persist_issue_webhook(
    pool: &PgPool,
    policy: &Policy,
    event: &str,
    delivery: &str,
    body: &[u8],
) -> Result<WebhookPersistOutcome, Box<dyn std::error::Error>> {
    if !matches!(event, "issues" | "issue_comment") {
        let payload: Value = serde_json::from_slice(body)?;
        let inserted = record_webhook_delivery(pool, None, delivery, event, &payload).await?;
        return Ok(if inserted {
            WebhookPersistOutcome::Ignored
        } else {
            WebhookPersistOutcome::Duplicate
        });
    }

    let payload: GitHubIssueWebhook = serde_json::from_slice(body)?;
    let payload_value: Value = serde_json::from_slice(body)?;
    let repository_id = upsert_repository(
        pool,
        &RepositoryInput {
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
    let current_state = match &label_state {
        LabelState::None => None,
        LabelState::One(label) => Some(label.state.to_string()),
        LabelState::Conflict(_) => Some(WorkflowState::NeedsHuman.to_string()),
    };

    let workflow_item_id = upsert_workflow_item(
        pool,
        &WorkflowItemInput {
            repository_id,
            provider_issue_id: payload.issue.id.to_string(),
            issue_number: payload.issue.number,
            current_state: current_state.clone(),
            current_labels: labels,
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

    if !should_queue_triage(event, &payload.action) {
        return Ok(WebhookPersistOutcome::Ignored);
    }

    let job = create_job(
        pool,
        Some(workflow_item_id),
        AgentRole::Triage.as_str(),
        &payload_value,
    )
    .await?;
    record_state_transition(
        pool,
        workflow_item_id,
        Some(job.id),
        current_state.as_deref(),
        "triage_queued",
        "queued triage job from github webhook",
    )
    .await?;

    Ok(WebhookPersistOutcome::Queued(job))
}

fn should_queue_triage(event: &str, action: &str) -> bool {
    matches!(
        (event, action),
        ("issues", "opened")
            | ("issues", "edited")
            | ("issues", "reopened")
            | ("issues", "labeled")
            | ("issues", "unlabeled")
            | ("issue_comment", "created")
    )
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
    labels: Vec<GitHubLabel>,
}

#[derive(Debug, Deserialize)]
struct GitHubLabel {
    name: String,
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
