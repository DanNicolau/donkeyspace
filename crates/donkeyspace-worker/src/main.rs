use clap::Parser;
use donkeyspace_core::{
    Policy, fake_triage_issue, triage_github_issue_actions, workflow_state_for_outcome,
};
use donkeyspace_db::{
    DbConfig, JobRecord, OutboundActionInput, OutboundActionRecord, acquire_next_queued_job,
    apply_migrations, complete_job, connect, create_outbound_action, fail_job,
    list_pending_outbound_actions, mark_job_running, mark_outbound_action_completed,
    mark_outbound_action_failed, record_state_transition, update_workflow_item_state,
};
use donkeyspace_github::GitHubClient;
use serde::Deserialize;
use serde_json::json;
use std::{env, fs, time::Duration};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[derive(Debug, Parser)]
#[command(name = "donkeyspace-worker")]
struct Args {
    #[arg(long, env = "DONKEYSPACE_DATABASE_URL")]
    database_url: Option<String>,
    #[arg(long, env = "DONKEYSPACE_WORKER_ID", default_value = "worker-local")]
    worker_id: String,
    #[arg(long, env = "DONKEYSPACE_LEASE_SECONDS", default_value_t = 300)]
    lease_seconds: i32,
    #[arg(long, default_value_t = false)]
    once: bool,
    #[arg(long, env = "DONKEYSPACE_GITHUB_TOKEN")]
    github_token: Option<String>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    init_tracing();
    let args = Args::parse();
    let policy = load_policy()?;

    let pool = if let Some(database_url) = args.database_url {
        let pool = connect(&DbConfig::from_database_url(database_url)).await?;
        apply_migrations(&pool).await?;
        tracing::info!("database connection verified");
        Some(pool)
    } else {
        tracing::warn!("DONKEYSPACE_DATABASE_URL is unset; starting without database check");
        None
    };

    tracing::info!("donkeyspace worker started");

    if args.once {
        if let Some(pool) = &pool {
            poll_once(
                pool,
                &policy,
                args.github_token.as_deref(),
                &args.worker_id,
                args.lease_seconds,
            )
            .await?;
        }
        tracing::info!("worker once mode completed");
        return Ok(());
    }

    loop {
        if let Some(pool) = &pool {
            poll_once(
                pool,
                &policy,
                args.github_token.as_deref(),
                &args.worker_id,
                args.lease_seconds,
            )
            .await?;
        }
        tokio::time::sleep(Duration::from_secs(30)).await;
        tracing::debug!("worker heartbeat");
    }
}

async fn poll_once(
    pool: &donkeyspace_db::PgPool,
    policy: &Policy,
    github_token: Option<&str>,
    worker_id: &str,
    lease_seconds: i32,
) -> Result<(), Box<dyn std::error::Error>> {
    match acquire_next_queued_job(pool, worker_id, lease_seconds).await? {
        Some(job) => {
            tracing::info!(
                job_id = %job.id,
                role = job.role,
                "leased queued job"
            );
            execute_job(pool, policy, job).await?;
        }
        None => tracing::debug!("no queued jobs available"),
    }

    if let Some(github_token) = github_token.filter(|token| !token.trim().is_empty()) {
        let client = GitHubClient::new(github_token)?;
        process_outbound_actions(pool, &client).await?;
    } else {
        tracing::debug!("DONKEYSPACE_GITHUB_TOKEN is unset; outbound actions remain pending");
    }

    Ok(())
}

async fn execute_job(
    pool: &donkeyspace_db::PgPool,
    policy: &Policy,
    job: JobRecord,
) -> Result<(), Box<dyn std::error::Error>> {
    let Some(running_job) = mark_job_running(pool, job.id).await? else {
        tracing::warn!(job_id = %job.id, "leased job was not available to mark running");
        return Ok(());
    };

    match running_job.role.as_str() {
        "triage" => {
            if input_is_donkeyspace_comment(&running_job.input) {
                let result_value = json!({
                    "outcome": "blocked",
                    "summary": "Ignored donkeyspace-generated comment webhook.",
                    "confidence": "high",
                    "risk": "unknown",
                    "questions": [],
                    "tests": [],
                    "changed_files": [],
                    "human_review_reason": null,
                    "blocked_reason": "donkeyspace-generated comments do not trigger triage",
                });
                complete_job(pool, running_job.id, &result_value).await?;
                tracing::info!(
                    job_id = %running_job.id,
                    "ignored donkeyspace-generated comment job"
                );
                return Ok(());
            }

            let result = fake_triage_issue(&running_job.input);
            result.validate_for_orchestration()?;
            let result_value = serde_json::to_value(&result)?;
            let workflow_state = workflow_state_for_outcome(result.outcome);
            complete_job(pool, running_job.id, &result_value).await?;

            if let Some(workflow_item_id) = running_job.workflow_item_id {
                update_workflow_item_state(pool, workflow_item_id, workflow_state.as_str()).await?;
                record_state_transition(
                    pool,
                    workflow_item_id,
                    Some(running_job.id),
                    None,
                    workflow_state.as_str(),
                    "fake triage agent completed",
                )
                .await?;

                for action in
                    triage_github_issue_actions(policy, &running_job.input, &result, workflow_state)
                {
                    create_outbound_action(
                        pool,
                        &OutboundActionInput {
                            workflow_item_id,
                            job_id: Some(running_job.id),
                            provider: "github".to_string(),
                            action_type: action.action_type,
                            payload: action.payload,
                        },
                    )
                    .await?;
                }
            }

            tracing::info!(
                job_id = %running_job.id,
                outcome = ?result.outcome,
                workflow_state = workflow_state.as_str(),
                "job completed"
            );
        }
        unsupported => {
            let result_value = json!({
                "outcome": "failed",
                "summary": "Job execution failed.",
                "confidence": "low",
                "risk": "unknown",
                "questions": [],
                "tests": [],
                "changed_files": [],
                "human_review_reason": null,
                "blocked_reason": format!("unsupported agent role: {unsupported}"),
            });
            fail_job(pool, running_job.id, &result_value).await?;

            if let Some(workflow_item_id) = running_job.workflow_item_id {
                update_workflow_item_state(pool, workflow_item_id, "blocked").await?;
                record_state_transition(
                    pool,
                    workflow_item_id,
                    Some(running_job.id),
                    None,
                    "blocked",
                    "job execution failed",
                )
                .await?;
            }

            tracing::warn!(job_id = %running_job.id, "job failed");
        }
    }

    Ok(())
}

fn input_is_donkeyspace_comment(input: &serde_json::Value) -> bool {
    input.pointer("/action").and_then(serde_json::Value::as_str) == Some("created")
        && input
            .pointer("/comment/body")
            .and_then(serde_json::Value::as_str)
            .map(|body| body.trim_start().starts_with("donkeyspace "))
            .unwrap_or(false)
}

async fn process_outbound_actions(
    pool: &donkeyspace_db::PgPool,
    client: &GitHubClient,
) -> Result<(), Box<dyn std::error::Error>> {
    for action in list_pending_outbound_actions(pool, 20).await? {
        match execute_outbound_action(client, &action).await {
            Ok(()) => {
                mark_outbound_action_completed(pool, action.id).await?;
                tracing::info!(
                    action_id = action.id,
                    action_type = action.action_type,
                    "outbound github action completed"
                );
            }
            Err(error) => {
                let error = error.to_string();
                mark_outbound_action_failed(pool, action.id, &error).await?;
                tracing::warn!(
                    action_id = action.id,
                    action_type = action.action_type,
                    %error,
                    "outbound github action failed"
                );
            }
        }
    }

    Ok(())
}

async fn execute_outbound_action(
    client: &GitHubClient,
    action: &OutboundActionRecord,
) -> Result<(), Box<dyn std::error::Error>> {
    match action.action_type.as_str() {
        "issue.add_label" => {
            let payload: AddLabelPayload = serde_json::from_value(action.payload.clone())?;
            client
                .add_issue_label(
                    &payload.owner,
                    &payload.repo,
                    payload.issue_number,
                    &payload.label,
                )
                .await?;
        }
        "issue.remove_labels" => {
            let payload: RemoveLabelsPayload = serde_json::from_value(action.payload.clone())?;
            for label in payload.labels {
                client
                    .remove_issue_label(&payload.owner, &payload.repo, payload.issue_number, &label)
                    .await?;
            }
        }
        "issue.create_comment" => {
            let payload: CreateCommentPayload = serde_json::from_value(action.payload.clone())?;
            client
                .create_issue_comment(
                    &payload.owner,
                    &payload.repo,
                    payload.issue_number,
                    &payload.body,
                )
                .await?;
        }
        unsupported => {
            return Err(format!("unsupported outbound action type: {unsupported}").into());
        }
    }

    Ok(())
}

#[derive(Debug, Deserialize)]
struct AddLabelPayload {
    owner: String,
    repo: String,
    issue_number: i64,
    label: String,
}

#[derive(Debug, Deserialize)]
struct RemoveLabelsPayload {
    owner: String,
    repo: String,
    issue_number: i64,
    labels: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct CreateCommentPayload {
    owner: String,
    repo: String,
    issue_number: i64,
    body: String,
}

#[cfg(test)]
mod tests {
    use super::input_is_donkeyspace_comment;
    use serde_json::json;

    #[test]
    fn detects_generated_comment_job() {
        assert!(input_is_donkeyspace_comment(&json!({
            "action": "created",
            "comment": {
                "body": "donkeyspace triage needs clarification before this issue can move to implementation."
            }
        })));
    }

    #[test]
    fn human_comment_job_is_not_generated() {
        assert!(!input_is_donkeyspace_comment(&json!({
            "action": "created",
            "comment": {
                "body": "Here are the reproduction steps."
            }
        })));
    }
}

fn init_tracing() {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "donkeyspace=info".into()),
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
