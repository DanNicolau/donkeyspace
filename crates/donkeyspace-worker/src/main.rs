use clap::Parser;
use donkeyspace_core::{
    Policy, fake_triage_issue, triage_github_issue_actions, workflow_state_for_outcome,
};
use donkeyspace_db::{
    DbConfig, JobRecord, OutboundActionInput, acquire_next_queued_job, apply_migrations,
    complete_job, connect, create_outbound_action, fail_job, mark_job_running,
    record_state_transition, update_workflow_item_state,
};
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
            poll_once(pool, &policy, &args.worker_id, args.lease_seconds).await?;
        }
        tracing::info!("worker once mode completed");
        return Ok(());
    }

    loop {
        if let Some(pool) = &pool {
            poll_once(pool, &policy, &args.worker_id, args.lease_seconds).await?;
        }
        tokio::time::sleep(Duration::from_secs(30)).await;
        tracing::debug!("worker heartbeat");
    }
}

async fn poll_once(
    pool: &donkeyspace_db::PgPool,
    policy: &Policy,
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
