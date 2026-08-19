use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
pub use sqlx::PgPool;
use sqlx::{FromRow, postgres::PgPoolOptions};
use std::time::Duration;
use thiserror::Error;
use uuid::Uuid;

#[derive(Debug, Error)]
pub enum DbError {
    #[error("database url is empty")]
    EmptyDatabaseUrl,
    #[error("database error: {0}")]
    Sqlx(#[from] sqlx::Error),
}

#[derive(Debug, Clone)]
pub struct DbConfig {
    pub database_url: String,
    pub max_connections: u32,
}

impl DbConfig {
    pub fn from_database_url(database_url: impl Into<String>) -> Self {
        Self {
            database_url: database_url.into(),
            max_connections: 5,
        }
    }
}

pub async fn connect(config: &DbConfig) -> Result<PgPool, DbError> {
    if config.database_url.trim().is_empty() {
        return Err(DbError::EmptyDatabaseUrl);
    }

    Ok(PgPoolOptions::new()
        .max_connections(config.max_connections)
        .acquire_timeout(Duration::from_secs(5))
        .connect(&config.database_url)
        .await?)
}

pub async fn apply_migrations(pool: &PgPool) -> Result<(), DbError> {
    const MIGRATION_LOCK_ID: i64 = 0x0D05_0001;

    let mut transaction = pool.begin().await?;
    sqlx::query("SELECT pg_advisory_xact_lock($1)")
        .bind(MIGRATION_LOCK_ID)
        .execute(&mut *transaction)
        .await?;
    sqlx::raw_sql(include_str!("../../../migrations/0001_init.sql"))
        .execute(&mut *transaction)
        .await?;
    transaction.commit().await?;
    Ok(())
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RepositoryInput {
    pub installation_external_id: Option<String>,
    pub installation_account_login: Option<String>,
    pub provider: String,
    pub owner: String,
    pub name: String,
    pub default_branch: String,
}

#[derive(Debug, Clone, FromRow, Serialize, Deserialize)]
pub struct RepositoryRecord {
    pub id: i64,
    pub provider: String,
    pub owner: String,
    pub name: String,
    pub default_branch: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowItemInput {
    pub repository_id: i64,
    pub provider_issue_id: String,
    pub issue_number: i64,
    pub provider_state: String,
    pub current_state: Option<String>,
    pub current_labels: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PullRequestInput {
    pub repository_id: i64,
    pub workflow_item_id: Option<i64>,
    pub provider_pr_id: String,
    pub pr_number: i64,
    pub title: String,
    pub html_url: String,
    pub state: String,
    pub head_ref: String,
    pub head_sha: Option<String>,
    pub base_ref: String,
    pub base_sha: Option<String>,
    pub managed_by_donkeyspace: bool,
}

#[derive(Debug, Clone, FromRow, Serialize, Deserialize)]
pub struct JobRecord {
    pub id: Uuid,
    pub workflow_item_id: Option<i64>,
    pub retry_of_job_id: Option<Uuid>,
    pub role: String,
    pub status: String,
    pub lease_owner: Option<String>,
    pub lease_expires_at: Option<DateTime<Utc>>,
    pub input: Value,
    pub result: Option<Value>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, FromRow, Serialize, Deserialize)]
pub struct ReadyDeveloperCandidate {
    pub workflow_item_id: i64,
    pub current_labels: Value,
    pub input: Value,
}

#[derive(Debug, Clone, FromRow, Serialize, Deserialize)]
pub struct RepairCandidate {
    pub workflow_item_id: i64,
    pub pr_number: i64,
    pub title: String,
    pub html_url: String,
    pub state: String,
    pub head_ref: String,
    pub head_sha: Option<String>,
    pub base_ref: String,
    pub base_sha: Option<String>,
    pub input: Value,
}

#[derive(Debug, Clone, FromRow, Serialize, Deserialize)]
pub struct WorkflowItemIssueRecord {
    pub id: i64,
    pub current_state: Option<String>,
}

#[derive(Debug, Clone, FromRow, Serialize, Deserialize)]
pub struct StateTransitionRecord {
    pub id: i64,
    pub workflow_item_id: i64,
    pub job_id: Option<Uuid>,
    pub from_state: Option<String>,
    pub to_state: String,
    pub reason: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OutboundActionInput {
    pub workflow_item_id: i64,
    pub job_id: Option<Uuid>,
    pub provider: String,
    pub action_type: String,
    pub payload: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommandResultInput {
    pub job_id: Uuid,
    pub name: String,
    pub command: Vec<String>,
    pub status: String,
    pub exit_code: Option<i32>,
    pub summary: Option<String>,
}

#[derive(Debug, Clone, FromRow, Serialize, Deserialize)]
pub struct CommandResultRecord {
    pub id: i64,
    pub job_id: Uuid,
    pub name: String,
    pub command: Value,
    pub status: String,
    pub exit_code: Option<i32>,
    pub summary: Option<String>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, FromRow, Serialize, Deserialize)]
pub struct OutboundActionRecord {
    pub id: i64,
    pub workflow_item_id: i64,
    pub job_id: Option<Uuid>,
    pub provider: String,
    pub action_type: String,
    pub status: String,
    pub payload: Value,
    pub last_error: Option<String>,
    pub provider_resource_id: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EngagementDecisionInput {
    pub webhook_delivery_id: i64,
    pub workflow_item_id: Option<i64>,
    pub gate: String,
    pub disposition: String,
    pub actor: Option<Value>,
    pub matched_selector: Option<Value>,
    pub reason: String,
}

#[derive(Debug, Clone, FromRow, Serialize, Deserialize)]
pub struct EngagementDecisionRecord {
    pub id: i64,
    pub webhook_delivery_id: i64,
    pub workflow_item_id: Option<i64>,
    pub gate: String,
    pub disposition: String,
    pub actor: Option<Value>,
    pub matched_selector: Option<Value>,
    pub reason: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, FromRow, Serialize, Deserialize)]
pub struct ManagedPullRequestRecord {
    pub workflow_item_id: i64,
    pub pr_number: i64,
    pub title: String,
    pub html_url: String,
    pub state: String,
    pub head_ref: String,
    pub head_sha: Option<String>,
    pub base_ref: String,
    pub base_sha: Option<String>,
}

pub async fn upsert_repository(pool: &PgPool, input: &RepositoryInput) -> Result<i64, DbError> {
    let installation_id = if let (Some(external_id), Some(account_login)) = (
        input.installation_external_id.as_deref(),
        input.installation_account_login.as_deref(),
    ) {
        Some(
            sqlx::query_scalar::<_, i64>(
                r#"
                INSERT INTO installations (provider, external_id, account_login)
                VALUES ($1, $2, $3)
                ON CONFLICT (provider, external_id)
                DO UPDATE SET account_login = EXCLUDED.account_login
                RETURNING id
                "#,
            )
            .bind(&input.provider)
            .bind(external_id)
            .bind(account_login)
            .fetch_one(pool)
            .await?,
        )
    } else {
        None
    };
    let id = sqlx::query_scalar::<_, i64>(
        r#"
        INSERT INTO repositories (installation_id, provider, owner, name, default_branch)
        VALUES ($1, $2, $3, $4, $5)
        ON CONFLICT (provider, owner, name)
        DO UPDATE SET
            installation_id = COALESCE(EXCLUDED.installation_id, repositories.installation_id),
            default_branch = EXCLUDED.default_branch
        RETURNING id
        "#,
    )
    .bind(installation_id)
    .bind(&input.provider)
    .bind(&input.owner)
    .bind(&input.name)
    .bind(&input.default_branch)
    .fetch_one(pool)
    .await?;

    Ok(id)
}

pub async fn list_github_repositories(pool: &PgPool) -> Result<Vec<RepositoryRecord>, DbError> {
    let repositories = sqlx::query_as::<_, RepositoryRecord>(
        r#"
        SELECT id, provider, owner, name, default_branch
        FROM repositories
        WHERE provider = 'github'
        ORDER BY owner ASC, name ASC
        "#,
    )
    .fetch_all(pool)
    .await?;

    Ok(repositories)
}

pub async fn list_github_repositories_for_installation(
    pool: &PgPool,
    installation_external_id: &str,
) -> Result<Vec<RepositoryRecord>, DbError> {
    let repositories = sqlx::query_as::<_, RepositoryRecord>(
        r#"
        SELECT r.id, r.provider, r.owner, r.name, r.default_branch
        FROM repositories r
        JOIN installations i ON i.id = r.installation_id
        WHERE r.provider = 'github'
          AND i.provider = 'github'
          AND i.external_id = $1
        ORDER BY r.owner ASC, r.name ASC
        "#,
    )
    .bind(installation_external_id)
    .fetch_all(pool)
    .await?;

    Ok(repositories)
}

pub async fn upsert_workflow_item(
    pool: &PgPool,
    input: &WorkflowItemInput,
) -> Result<i64, DbError> {
    let labels = serde_json::to_value(&input.current_labels).expect("labels serialize");
    let id = sqlx::query_scalar::<_, i64>(
        r#"
        INSERT INTO workflow_items (
            repository_id,
            provider_issue_id,
            issue_number,
            provider_state,
            current_state,
            current_labels
        )
        VALUES ($1, $2, $3, $4, $5, $6)
        ON CONFLICT (repository_id, provider_issue_id)
        DO UPDATE SET
            issue_number = EXCLUDED.issue_number,
            provider_state = EXCLUDED.provider_state,
            current_state = COALESCE(EXCLUDED.current_state, workflow_items.current_state),
            current_labels = EXCLUDED.current_labels,
            updated_at = now()
        RETURNING id
        "#,
    )
    .bind(input.repository_id)
    .bind(&input.provider_issue_id)
    .bind(input.issue_number)
    .bind(&input.provider_state)
    .bind(&input.current_state)
    .bind(labels)
    .fetch_one(pool)
    .await?;

    Ok(id)
}

pub async fn get_workflow_item_state(
    pool: &PgPool,
    repository_id: i64,
    provider_issue_id: &str,
) -> Result<Option<String>, DbError> {
    let state = sqlx::query_scalar::<_, Option<String>>(
        r#"
        SELECT COALESCE(
            workflow_items.current_state,
            CASE
                WHEN latest_job.status = 'failed' THEN 'blocked'
                WHEN latest_job.result->>'outcome' IN ('blocked', 'failed') THEN 'blocked'
                WHEN latest_job.result->>'outcome' = 'needs_info' THEN 'needs_info'
                WHEN latest_job.result->>'outcome' = 'needs_human' THEN 'needs_human'
                WHEN latest_job.result->>'outcome' = 'ready' THEN 'ready'
                ELSE NULL
            END
        )
        FROM workflow_items
        LEFT JOIN LATERAL (
            SELECT status, result
            FROM jobs
            WHERE jobs.workflow_item_id = workflow_items.id
              AND jobs.role = 'triage'
            ORDER BY jobs.created_at DESC
            LIMIT 1
        ) AS latest_job ON true
        WHERE workflow_items.repository_id = $1
          AND workflow_items.provider_issue_id = $2
        "#,
    )
    .bind(repository_id)
    .bind(provider_issue_id)
    .fetch_optional(pool)
    .await?
    .flatten();

    Ok(state)
}

pub async fn get_workflow_item_by_issue_number(
    pool: &PgPool,
    repository_id: i64,
    issue_number: i64,
) -> Result<Option<WorkflowItemIssueRecord>, DbError> {
    let item = sqlx::query_as::<_, WorkflowItemIssueRecord>(
        r#"
        SELECT id, current_state
        FROM workflow_items
        WHERE repository_id = $1
          AND issue_number = $2
        "#,
    )
    .bind(repository_id)
    .bind(issue_number)
    .fetch_optional(pool)
    .await?;

    Ok(item)
}

pub async fn latest_workflow_job_input(
    pool: &PgPool,
    workflow_item_id: i64,
) -> Result<Option<Value>, DbError> {
    let input = sqlx::query_scalar::<_, Value>(
        r#"
        SELECT input
        FROM jobs
        WHERE workflow_item_id = $1
          AND role IN ('developer', 'triage')
        ORDER BY
          CASE WHEN role = 'developer' THEN 0 ELSE 1 END,
          created_at DESC
        LIMIT 1
        "#,
    )
    .bind(workflow_item_id)
    .fetch_optional(pool)
    .await?;

    Ok(input)
}

pub async fn upsert_pull_request(pool: &PgPool, input: &PullRequestInput) -> Result<i64, DbError> {
    let id = sqlx::query_scalar::<_, i64>(
        r#"
        INSERT INTO pull_requests (
            repository_id,
            workflow_item_id,
            provider_pr_id,
            pr_number,
            title,
            html_url,
            state,
            head_ref,
            head_sha,
            base_ref,
            base_sha,
            managed_by_donkeyspace
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
        ON CONFLICT (repository_id, provider_pr_id)
        DO UPDATE SET
            workflow_item_id = COALESCE(EXCLUDED.workflow_item_id, pull_requests.workflow_item_id),
            pr_number = EXCLUDED.pr_number,
            title = EXCLUDED.title,
            html_url = EXCLUDED.html_url,
            state = EXCLUDED.state,
            head_ref = EXCLUDED.head_ref,
            head_sha = EXCLUDED.head_sha,
            base_ref = EXCLUDED.base_ref,
            base_sha = EXCLUDED.base_sha,
            managed_by_donkeyspace = EXCLUDED.managed_by_donkeyspace OR pull_requests.managed_by_donkeyspace,
            updated_at = now()
        RETURNING id
        "#,
    )
    .bind(input.repository_id)
    .bind(input.workflow_item_id)
    .bind(&input.provider_pr_id)
    .bind(input.pr_number)
    .bind(&input.title)
    .bind(&input.html_url)
    .bind(&input.state)
    .bind(&input.head_ref)
    .bind(&input.head_sha)
    .bind(&input.base_ref)
    .bind(&input.base_sha)
    .bind(input.managed_by_donkeyspace)
    .fetch_one(pool)
    .await?;

    Ok(id)
}

pub async fn list_open_managed_pull_requests_for_base(
    pool: &PgPool,
    repository_id: i64,
    base_ref: &str,
) -> Result<Vec<ManagedPullRequestRecord>, DbError> {
    let rows = sqlx::query_as::<_, ManagedPullRequestRecord>(
        r#"
        SELECT
            workflow_item_id,
            pr_number,
            title,
            html_url,
            state,
            head_ref,
            head_sha,
            base_ref,
            base_sha
        FROM pull_requests
        WHERE repository_id = $1
          AND base_ref = $2
          AND state = 'open'
          AND managed_by_donkeyspace = true
          AND workflow_item_id IS NOT NULL
        ORDER BY updated_at ASC
        "#,
    )
    .bind(repository_id)
    .bind(base_ref)
    .fetch_all(pool)
    .await?;

    Ok(rows)
}

pub async fn repair_job_exists_for_pr_base(
    pool: &PgPool,
    workflow_item_id: i64,
    pr_number: i64,
    head_sha: Option<&str>,
    base_sha: Option<&str>,
) -> Result<bool, DbError> {
    let exists = sqlx::query_scalar::<_, bool>(
        r#"
        SELECT EXISTS (
            SELECT 1
            FROM jobs
            WHERE workflow_item_id = $1
              AND role = 'repair'
              AND input #>> '{pull_request,number}' = $2
              AND (
                $3::text IS NULL
                OR input #>> '{pull_request,head,sha}' = $3
              )
              AND (
                $4::text IS NULL
                OR input #>> '{pull_request,base,sha}' = $4
              )
              AND status IN ('queued', 'leased', 'running', 'completed')
        )
        "#,
    )
    .bind(workflow_item_id)
    .bind(pr_number.to_string())
    .bind(head_sha)
    .bind(base_sha)
    .fetch_one(pool)
    .await?;

    Ok(exists)
}

pub async fn reviewer_job_exists_for_pr_head(
    pool: &PgPool,
    workflow_item_id: i64,
    pr_number: i64,
    head_sha: Option<&str>,
) -> Result<bool, DbError> {
    let exists = sqlx::query_scalar::<_, bool>(
        r#"
        SELECT EXISTS (
            SELECT 1
            FROM jobs
            WHERE workflow_item_id = $1
              AND role = 'reviewer'
              AND input #>> '{pull_request,number}' = $2
              AND (
                $3::text IS NULL
                OR input #>> '{pull_request,head,sha}' = $3
              )
              AND status IN ('queued', 'leased', 'running', 'completed')
        )
        "#,
    )
    .bind(workflow_item_id)
    .bind(pr_number.to_string())
    .bind(head_sha)
    .fetch_one(pool)
    .await?;

    Ok(exists)
}

pub async fn record_webhook_delivery(
    pool: &PgPool,
    repository_id: Option<i64>,
    delivery_id: &str,
    event_name: &str,
    payload: &Value,
) -> Result<Option<i64>, DbError> {
    let inserted = sqlx::query_scalar::<_, i64>(
        r#"
        INSERT INTO webhook_deliveries (repository_id, delivery_id, event_name, payload)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (delivery_id) DO NOTHING
        RETURNING id
        "#,
    )
    .bind(repository_id)
    .bind(delivery_id)
    .bind(event_name)
    .bind(payload)
    .fetch_optional(pool)
    .await?;

    Ok(inserted)
}

pub async fn record_engagement_decision(
    pool: &PgPool,
    input: &EngagementDecisionInput,
) -> Result<EngagementDecisionRecord, DbError> {
    Ok(sqlx::query_as::<_, EngagementDecisionRecord>(
        r#"
        INSERT INTO engagement_decisions (
            webhook_delivery_id, workflow_item_id, gate, disposition,
            actor, matched_selector, reason
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        ON CONFLICT (webhook_delivery_id) DO UPDATE SET
            workflow_item_id = EXCLUDED.workflow_item_id,
            gate = EXCLUDED.gate,
            disposition = EXCLUDED.disposition,
            actor = EXCLUDED.actor,
            matched_selector = EXCLUDED.matched_selector,
            reason = EXCLUDED.reason
        RETURNING *
        "#,
    )
    .bind(input.webhook_delivery_id)
    .bind(input.workflow_item_id)
    .bind(&input.gate)
    .bind(&input.disposition)
    .bind(&input.actor)
    .bind(&input.matched_selector)
    .bind(&input.reason)
    .fetch_one(pool)
    .await?)
}

pub async fn list_recent_engagement_decisions(
    pool: &PgPool,
    limit: i64,
) -> Result<Vec<EngagementDecisionRecord>, DbError> {
    Ok(sqlx::query_as::<_, EngagementDecisionRecord>(
        "SELECT * FROM engagement_decisions ORDER BY created_at DESC LIMIT $1",
    )
    .bind(limit)
    .fetch_all(pool)
    .await?)
}

pub async fn record_github_managed_resource_for_workflow_item(
    pool: &PgPool,
    workflow_item_id: i64,
    resource_kind: &str,
    provider_id: &str,
    metadata: &Value,
) -> Result<(), DbError> {
    sqlx::query(
        r#"
        INSERT INTO github_managed_resources (
            repository_id, workflow_item_id, resource_kind, provider_id, metadata
        )
        SELECT repository_id, id, $2, $3, $4
        FROM workflow_items WHERE id = $1
        ON CONFLICT (repository_id, resource_kind, provider_id) DO NOTHING
        "#,
    )
    .bind(workflow_item_id)
    .bind(resource_kind)
    .bind(provider_id)
    .bind(metadata)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn github_managed_resource_exists(
    pool: &PgPool,
    repository_id: i64,
    resource_kind: &str,
    provider_id: &str,
) -> Result<bool, DbError> {
    Ok(sqlx::query_scalar::<_, bool>(
        r#"
        SELECT EXISTS (
            SELECT 1 FROM github_managed_resources
            WHERE repository_id = $1 AND resource_kind = $2 AND provider_id = $3
        )
        "#,
    )
    .bind(repository_id)
    .bind(resource_kind)
    .bind(provider_id)
    .fetch_one(pool)
    .await?)
}

pub async fn pending_outbound_comment_exists(
    pool: &PgPool,
    workflow_item_id: i64,
    body: &str,
) -> Result<bool, DbError> {
    Ok(sqlx::query_scalar::<_, bool>(
        r#"
        SELECT EXISTS (
            SELECT 1 FROM outbound_actions
            WHERE workflow_item_id = $1
              AND action_type = 'issue.create_comment'
              AND status = 'pending'
              AND payload ->> 'body' = $2
        )
        "#,
    )
    .bind(workflow_item_id)
    .bind(body)
    .fetch_one(pool)
    .await?)
}

pub async fn create_job(
    pool: &PgPool,
    workflow_item_id: Option<i64>,
    role: &str,
    input: &Value,
) -> Result<JobRecord, DbError> {
    create_job_with_retry_of(pool, workflow_item_id, None, role, input).await
}

pub async fn active_job_exists_for_workflow_item(
    pool: &PgPool,
    workflow_item_id: i64,
) -> Result<bool, DbError> {
    Ok(sqlx::query_scalar::<_, bool>(
        r#"
        SELECT EXISTS (
            SELECT 1
            FROM jobs
            WHERE workflow_item_id = $1
              AND status IN ('queued', 'leased', 'running')
        )
        "#,
    )
    .bind(workflow_item_id)
    .fetch_one(pool)
    .await?)
}

/// Requeue the most recent paused lifecycle coordinator for a workflow item.
/// The coordinator keeps its UUID so its durable workspace and checkpoint can
/// be reused. The new webhook payload becomes the run input and is marked as a
/// resume so the worker does not replace the retained checkout.
pub async fn resume_latest_paused_job(
    pool: &PgPool,
    workflow_item_id: i64,
    input: &Value,
) -> Result<Option<JobRecord>, DbError> {
    let mut resumed_input = input.clone();
    if let Value::Object(map) = &mut resumed_input {
        map.insert("donkeyspace_resume".to_string(), Value::Bool(true));
    }

    Ok(sqlx::query_as::<_, JobRecord>(
        r#"
        UPDATE jobs
        SET
            status = 'queued',
            input = $2,
            result = NULL,
            lease_owner = NULL,
            lease_expires_at = NULL,
            updated_at = now()
        WHERE id = (
            SELECT id
            FROM jobs
            WHERE workflow_item_id = $1
              AND status = 'paused'
            ORDER BY updated_at DESC
            LIMIT 1
            FOR UPDATE SKIP LOCKED
        )
        RETURNING *
        "#,
    )
    .bind(workflow_item_id)
    .bind(resumed_input)
    .fetch_optional(pool)
    .await?)
}

/// Create a child task that is durably visible but cannot be leased by the
/// general worker loop. A lifecycle coordinator starts it once dependencies
/// are satisfied.
pub async fn create_waiting_job(
    pool: &PgPool,
    workflow_item_id: Option<i64>,
    role: &str,
    input: &Value,
) -> Result<JobRecord, DbError> {
    let id = Uuid::now_v7();
    Ok(sqlx::query_as::<_, JobRecord>(
        r#"
        INSERT INTO jobs (id, workflow_item_id, role, status, input)
        VALUES ($1, $2, $3, 'waiting', $4)
        RETURNING *
        "#,
    )
    .bind(id)
    .bind(workflow_item_id)
    .bind(role)
    .bind(input)
    .fetch_one(pool)
    .await?)
}

pub async fn start_waiting_job(pool: &PgPool, id: Uuid) -> Result<Option<JobRecord>, DbError> {
    Ok(sqlx::query_as::<_, JobRecord>(
        r#"
        UPDATE jobs
        SET status = 'running', updated_at = now()
        WHERE id = $1 AND status = 'waiting'
        RETURNING *
        "#,
    )
    .bind(id)
    .fetch_optional(pool)
    .await?)
}

pub async fn create_retry_job(
    pool: &PgPool,
    workflow_item_id: Option<i64>,
    retry_of_job_id: Uuid,
    role: &str,
    input: &Value,
) -> Result<JobRecord, DbError> {
    create_job_with_retry_of(pool, workflow_item_id, Some(retry_of_job_id), role, input).await
}

async fn create_job_with_retry_of(
    pool: &PgPool,
    workflow_item_id: Option<i64>,
    retry_of_job_id: Option<Uuid>,
    role: &str,
    input: &Value,
) -> Result<JobRecord, DbError> {
    let id = Uuid::now_v7();
    let job = sqlx::query_as::<_, JobRecord>(
        r#"
        INSERT INTO jobs (id, workflow_item_id, retry_of_job_id, role, status, input)
        VALUES ($1, $2, $3, $4, 'queued', $5)
        RETURNING *
        "#,
    )
    .bind(id)
    .bind(workflow_item_id)
    .bind(retry_of_job_id)
    .bind(role)
    .bind(input)
    .fetch_one(pool)
    .await?;

    Ok(job)
}

pub async fn list_ready_developer_candidates(
    pool: &PgPool,
    limit: i64,
) -> Result<Vec<ReadyDeveloperCandidate>, DbError> {
    let candidates = sqlx::query_as::<_, ReadyDeveloperCandidate>(
        r#"
        SELECT
            workflow_items.id AS workflow_item_id,
            workflow_items.current_labels AS current_labels,
            latest_triage.input AS input
        FROM workflow_items
        JOIN LATERAL (
            SELECT jobs.input
            FROM jobs
            WHERE jobs.workflow_item_id = workflow_items.id
              AND jobs.role = 'triage'
              AND jobs.status = 'completed'
            ORDER BY jobs.created_at DESC
            LIMIT 1
        ) AS latest_triage ON true
        WHERE workflow_items.current_state = 'ready'
          AND workflow_items.provider_state <> 'closed'
          AND COALESCE(latest_triage.input #>> '{issue,state}', 'open') <> 'closed'
          AND NOT EXISTS (
              SELECT 1
              FROM jobs developer_jobs
              WHERE developer_jobs.workflow_item_id = workflow_items.id
                AND developer_jobs.role = 'developer'
                AND (
                    developer_jobs.status IN ('queued', 'leased', 'running')
                    OR developer_jobs.result->>'blocked_reason' = 'closed issues are not eligible for agent work'
                )
          )
        ORDER BY workflow_items.updated_at ASC
        LIMIT $1
        "#,
    )
    .bind(limit)
    .fetch_all(pool)
    .await?;

    Ok(candidates)
}

pub async fn list_repair_candidates(
    pool: &PgPool,
    limit: i64,
) -> Result<Vec<RepairCandidate>, DbError> {
    let candidates = sqlx::query_as::<_, RepairCandidate>(
        r#"
        SELECT
            pull_requests.workflow_item_id AS workflow_item_id,
            pull_requests.pr_number,
            pull_requests.title,
            pull_requests.html_url,
            pull_requests.state,
            pull_requests.head_ref,
            pull_requests.head_sha,
            pull_requests.base_ref,
            pull_requests.base_sha,
            latest_input.input AS input
        FROM pull_requests
        JOIN LATERAL (
            SELECT jobs.input
            FROM jobs
            WHERE jobs.workflow_item_id = pull_requests.workflow_item_id
              AND jobs.role IN ('developer', 'triage')
            ORDER BY
              CASE WHEN role = 'developer' THEN 0 ELSE 1 END,
              created_at DESC
            LIMIT 1
        ) AS latest_input ON true
        WHERE pull_requests.workflow_item_id IS NOT NULL
          AND pull_requests.state = 'open'
          AND pull_requests.managed_by_donkeyspace = true
          AND NOT EXISTS (
              SELECT 1
              FROM jobs repair_jobs
              WHERE repair_jobs.workflow_item_id = pull_requests.workflow_item_id
                AND repair_jobs.role = 'repair'
                AND repair_jobs.input #>> '{pull_request,number}' = pull_requests.pr_number::text
                AND (
                    pull_requests.head_sha IS NULL
                    OR repair_jobs.input #>> '{pull_request,head,sha}' = pull_requests.head_sha
                )
                AND (
                    pull_requests.base_sha IS NULL
                    OR repair_jobs.input #>> '{pull_request,base,sha}' = pull_requests.base_sha
                )
                AND repair_jobs.status IN ('queued', 'leased', 'running', 'completed')
          )
        ORDER BY pull_requests.updated_at ASC
        LIMIT $1
        "#,
    )
    .bind(limit)
    .fetch_all(pool)
    .await?;

    Ok(candidates)
}

pub async fn record_state_transition(
    pool: &PgPool,
    workflow_item_id: i64,
    job_id: Option<Uuid>,
    from_state: Option<&str>,
    to_state: &str,
    reason: &str,
) -> Result<(), DbError> {
    sqlx::query(
        r#"
        INSERT INTO state_transitions (workflow_item_id, job_id, from_state, to_state, reason)
        VALUES ($1, $2, $3, $4, $5)
        "#,
    )
    .bind(workflow_item_id)
    .bind(job_id)
    .bind(from_state)
    .bind(to_state)
    .bind(reason)
    .execute(pool)
    .await?;

    Ok(())
}

pub async fn create_outbound_action(
    pool: &PgPool,
    input: &OutboundActionInput,
) -> Result<OutboundActionRecord, DbError> {
    let action = sqlx::query_as::<_, OutboundActionRecord>(
        r#"
        INSERT INTO outbound_actions (
            workflow_item_id,
            job_id,
            provider,
            action_type,
            payload
        )
        VALUES ($1, $2, $3, $4, $5)
        RETURNING *
        "#,
    )
    .bind(input.workflow_item_id)
    .bind(input.job_id)
    .bind(&input.provider)
    .bind(&input.action_type)
    .bind(&input.payload)
    .fetch_one(pool)
    .await?;

    Ok(action)
}

pub async fn create_command_result(
    pool: &PgPool,
    input: &CommandResultInput,
) -> Result<(), DbError> {
    let command = serde_json::to_value(&input.command).expect("command serializes");
    sqlx::query(
        r#"
        INSERT INTO command_results (
            job_id,
            name,
            command,
            status,
            exit_code,
            summary
        )
        VALUES ($1, $2, $3, $4, $5, $6)
        "#,
    )
    .bind(input.job_id)
    .bind(&input.name)
    .bind(&command)
    .bind(&input.status)
    .bind(input.exit_code)
    .bind(&input.summary)
    .execute(pool)
    .await?;

    Ok(())
}

pub async fn list_job_command_results(
    pool: &PgPool,
    job_id: Uuid,
) -> Result<Vec<CommandResultRecord>, DbError> {
    let results = sqlx::query_as::<_, CommandResultRecord>(
        r#"
        SELECT *
        FROM command_results
        WHERE job_id = $1
        ORDER BY created_at ASC
        "#,
    )
    .bind(job_id)
    .fetch_all(pool)
    .await?;

    Ok(results)
}

pub async fn list_recent_outbound_actions(
    pool: &PgPool,
    limit: i64,
) -> Result<Vec<OutboundActionRecord>, DbError> {
    let actions = sqlx::query_as::<_, OutboundActionRecord>(
        r#"
        SELECT *
        FROM outbound_actions
        ORDER BY created_at DESC
        LIMIT $1
        "#,
    )
    .bind(limit)
    .fetch_all(pool)
    .await?;

    Ok(actions)
}

pub async fn list_pending_outbound_actions(
    pool: &PgPool,
    limit: i64,
) -> Result<Vec<OutboundActionRecord>, DbError> {
    let actions = sqlx::query_as::<_, OutboundActionRecord>(
        r#"
        SELECT *
        FROM outbound_actions
        WHERE status = 'pending'
        ORDER BY created_at ASC
        LIMIT $1
        "#,
    )
    .bind(limit)
    .fetch_all(pool)
    .await?;

    Ok(actions)
}

pub async fn mark_outbound_action_completed(
    pool: &PgPool,
    id: i64,
    provider_resource_id: Option<&str>,
) -> Result<(), DbError> {
    let mut transaction = pool.begin().await?;
    sqlx::query(
        r#"
        UPDATE outbound_actions
        SET status = 'completed',
            last_error = NULL,
            provider_resource_id = $2,
            updated_at = now()
        WHERE id = $1
        "#,
    )
    .bind(id)
    .bind(provider_resource_id)
    .execute(&mut *transaction)
    .await?;

    if let Some(provider_id) = provider_resource_id {
        sqlx::query(
            r#"
            INSERT INTO github_managed_resources (
                repository_id, workflow_item_id, outbound_action_id,
                resource_kind, provider_id, metadata
            )
            SELECT workflow_items.repository_id, outbound_actions.workflow_item_id,
                   outbound_actions.id, 'issue_comment', $2, '{}'::jsonb
            FROM outbound_actions
            JOIN workflow_items ON workflow_items.id = outbound_actions.workflow_item_id
            WHERE outbound_actions.id = $1
            ON CONFLICT (repository_id, resource_kind, provider_id) DO NOTHING
            "#,
        )
        .bind(id)
        .bind(provider_id)
        .execute(&mut *transaction)
        .await?;
    }

    transaction.commit().await?;

    Ok(())
}

pub async fn mark_outbound_action_failed(
    pool: &PgPool,
    id: i64,
    error: &str,
) -> Result<(), DbError> {
    sqlx::query(
        r#"
        UPDATE outbound_actions
        SET status = 'failed',
            last_error = $2,
            updated_at = now()
        WHERE id = $1
        "#,
    )
    .bind(id)
    .bind(error)
    .execute(pool)
    .await?;

    Ok(())
}

pub async fn list_job_outbound_actions(
    pool: &PgPool,
    job_id: Uuid,
) -> Result<Vec<OutboundActionRecord>, DbError> {
    let actions = sqlx::query_as::<_, OutboundActionRecord>(
        r#"
        SELECT *
        FROM outbound_actions
        WHERE job_id = $1
        ORDER BY created_at ASC
        "#,
    )
    .bind(job_id)
    .fetch_all(pool)
    .await?;

    Ok(actions)
}

pub async fn list_jobs(pool: &PgPool, limit: i64) -> Result<Vec<JobRecord>, DbError> {
    let jobs = sqlx::query_as::<_, JobRecord>(
        r#"
        SELECT *
        FROM jobs
        ORDER BY created_at DESC
        LIMIT $1
        "#,
    )
    .bind(limit)
    .fetch_all(pool)
    .await?;

    Ok(jobs)
}

pub async fn get_job(pool: &PgPool, id: Uuid) -> Result<Option<JobRecord>, DbError> {
    let job = sqlx::query_as::<_, JobRecord>("SELECT * FROM jobs WHERE id = $1")
        .bind(id)
        .fetch_optional(pool)
        .await?;

    Ok(job)
}

pub async fn mark_job_running(pool: &PgPool, id: Uuid) -> Result<Option<JobRecord>, DbError> {
    let job = sqlx::query_as::<_, JobRecord>(
        r#"
        UPDATE jobs
        SET
            status = 'running',
            updated_at = now()
        WHERE id = $1
          AND status = 'leased'
          AND lease_expires_at > now()
        RETURNING *
        "#,
    )
    .bind(id)
    .fetch_optional(pool)
    .await?;

    Ok(job)
}

pub async fn complete_job(
    pool: &PgPool,
    id: Uuid,
    result: &Value,
) -> Result<Option<JobRecord>, DbError> {
    let job = sqlx::query_as::<_, JobRecord>(
        r#"
        UPDATE jobs
        SET
            status = 'completed',
            result = $2,
            lease_owner = NULL,
            lease_expires_at = NULL,
            updated_at = now()
        WHERE id = $1
          AND status = 'running'
        RETURNING *
        "#,
    )
    .bind(id)
    .bind(result)
    .fetch_optional(pool)
    .await?;

    Ok(job)
}

pub async fn pause_job(
    pool: &PgPool,
    id: Uuid,
    result: &Value,
) -> Result<Option<JobRecord>, DbError> {
    let job = sqlx::query_as::<_, JobRecord>(
        r#"
        UPDATE jobs
        SET
            status = 'paused',
            result = $2,
            lease_owner = NULL,
            lease_expires_at = NULL,
            updated_at = now()
        WHERE id = $1
          AND status = 'running'
        RETURNING *
        "#,
    )
    .bind(id)
    .bind(result)
    .fetch_optional(pool)
    .await?;

    Ok(job)
}

pub async fn fail_job(
    pool: &PgPool,
    id: Uuid,
    result: &Value,
) -> Result<Option<JobRecord>, DbError> {
    let job = sqlx::query_as::<_, JobRecord>(
        r#"
        UPDATE jobs
        SET
            status = 'failed',
            result = $2,
            lease_owner = NULL,
            lease_expires_at = NULL,
            updated_at = now()
        WHERE id = $1
        RETURNING *
        "#,
    )
    .bind(id)
    .bind(result)
    .fetch_optional(pool)
    .await?;

    Ok(job)
}

pub async fn update_workflow_item_state(
    pool: &PgPool,
    workflow_item_id: i64,
    state: &str,
) -> Result<(), DbError> {
    sqlx::query(
        r#"
        UPDATE workflow_items
        SET current_state = $2,
            updated_at = now()
        WHERE id = $1
        "#,
    )
    .bind(workflow_item_id)
    .bind(state)
    .execute(pool)
    .await?;

    Ok(())
}

pub async fn list_job_transitions(
    pool: &PgPool,
    job_id: Uuid,
) -> Result<Vec<StateTransitionRecord>, DbError> {
    let transitions = sqlx::query_as::<_, StateTransitionRecord>(
        r#"
        SELECT *
        FROM state_transitions
        WHERE job_id = $1
        ORDER BY created_at ASC
        "#,
    )
    .bind(job_id)
    .fetch_all(pool)
    .await?;

    Ok(transitions)
}

pub async fn acquire_job_lease(
    pool: &PgPool,
    id: Uuid,
    lease_owner: &str,
    lease_seconds: i32,
) -> Result<Option<JobRecord>, DbError> {
    let job = sqlx::query_as::<_, JobRecord>(
        r#"
        UPDATE jobs
        SET
            status = 'leased',
            lease_owner = $2,
            lease_expires_at = now() + ($3::text || ' seconds')::interval,
            updated_at = now()
        WHERE id = $1
          AND status IN ('queued', 'leased')
          AND (lease_expires_at IS NULL OR lease_expires_at < now())
        RETURNING *
        "#,
    )
    .bind(id)
    .bind(lease_owner)
    .bind(lease_seconds)
    .fetch_optional(pool)
    .await?;

    Ok(job)
}

pub async fn acquire_next_queued_job(
    pool: &PgPool,
    lease_owner: &str,
    lease_seconds: i32,
) -> Result<Option<JobRecord>, DbError> {
    let job = sqlx::query_as::<_, JobRecord>(
        r#"
        WITH candidate AS (
            SELECT id
            FROM jobs
            WHERE status = 'queued'
               OR (status = 'leased' AND lease_expires_at < now())
            ORDER BY created_at ASC
            LIMIT 1
            FOR UPDATE SKIP LOCKED
        )
        UPDATE jobs
        SET
            status = 'leased',
            lease_owner = $1,
            lease_expires_at = now() + ($2::text || ' seconds')::interval,
            updated_at = now()
        WHERE id = (SELECT id FROM candidate)
        RETURNING *
        "#,
    )
    .bind(lease_owner)
    .bind(lease_seconds)
    .fetch_optional(pool)
    .await?;

    Ok(job)
}
