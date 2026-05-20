use clap::Parser;
use donkeyspace_core::policy::RequiredCommand;
use donkeyspace_core::{
    Outcome, Policy, RunResult, TestResult, TestStatus, WorkflowState, fake_triage_issue,
    triage_github_issue_actions, workflow_state_for_outcome,
};
use donkeyspace_db::{
    CommandResultInput, DbConfig, JobRecord, OutboundActionInput, OutboundActionRecord,
    acquire_next_queued_job, apply_migrations, complete_job, connect, create_command_result,
    create_job, create_outbound_action, fail_job, list_pending_outbound_actions,
    list_ready_developer_candidates, mark_job_running, mark_outbound_action_completed,
    mark_outbound_action_failed, record_state_transition, update_workflow_item_state,
};
use donkeyspace_github::GitHubClient;
use donkeyspace_runner::{AgentCommand, AgentCommandStatus, read_run_result, run_agent_command};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    env, fs,
    path::{Path, PathBuf},
    process::Stdio,
    time::Duration,
};
use tokio::fs as tokio_fs;
use tokio::process::Command;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

mod llm_triage;
mod repo_context;

use llm_triage::{LlmTriageConfig, OpenAiTriageClient, TriageProvider};
use repo_context::{
    RepoContextConfig, build_repository_context, cleanup_repository_context,
    enrich_input_with_repository_context, workspace_path, write_askpass_script,
};

#[derive(Debug, Parser)]
#[command(name = "donkeyspace-worker")]
struct Args {
    #[arg(long, env = "DONKEYSPACE_DATABASE_URL", hide_env_values = true)]
    database_url: Option<String>,
    #[arg(long, env = "DONKEYSPACE_WORKER_ID", default_value = "worker-local")]
    worker_id: String,
    #[arg(long, env = "DONKEYSPACE_LEASE_SECONDS", default_value_t = 300)]
    lease_seconds: i32,
    #[arg(long, default_value_t = false)]
    once: bool,
    #[arg(long, env = "DONKEYSPACE_GITHUB_TOKEN", hide_env_values = true)]
    github_token: Option<String>,
    #[arg(long, env = "DONKEYSPACE_TRIAGE_PROVIDER", default_value = "auto")]
    triage_provider: String,
    #[arg(
        long,
        env = "DONKEYSPACE_LLM_BASE_URL",
        default_value = "https://openrouter.ai/api/v1"
    )]
    llm_base_url: String,
    #[arg(long, env = "DONKEYSPACE_LLM_MODEL", default_value = "openrouter/free")]
    llm_model: String,
    #[arg(long, env = "DONKEYSPACE_LLM_API_KEY", hide_env_values = true)]
    llm_api_key: Option<String>,
    #[arg(
        long,
        env = "DONKEYSPACE_WORKSPACE_ROOT",
        default_value = "/tmp/donkeyspace/workspaces"
    )]
    workspace_root: PathBuf,
    #[arg(
        long,
        env = "DONKEYSPACE_REPO_CONTEXT_MAX_BYTES",
        default_value_t = 20_000
    )]
    repo_context_max_bytes: usize,
    #[arg(
        long,
        env = "DONKEYSPACE_REPO_CONTEXT_MAX_FILE_BYTES",
        default_value_t = 4_000
    )]
    repo_context_max_file_bytes: usize,
    #[arg(long, env = "DONKEYSPACE_REPO_CONTEXT_MAX_FILES", default_value_t = 12)]
    repo_context_max_files: usize,
    #[arg(long, env = "DONKEYSPACE_READY_RECONCILE_LIMIT", default_value_t = 1)]
    ready_reconcile_limit: i64,
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

    let triage_config = LlmTriageConfig {
        provider: TriageProvider::parse(&args.triage_provider),
        base_url: args.llm_base_url.clone(),
        api_key: non_empty_string(args.llm_api_key.clone())
            .or_else(|| non_empty_string(env::var("OPENROUTER_API_KEY").ok())),
        model: args.llm_model.clone(),
    };
    let triage_client = OpenAiTriageClient::new(&triage_config)?;
    if triage_client.is_some() {
        tracing::info!(
            provider = "openai-compatible",
            model = triage_config.model,
            base_url = triage_config.base_url,
            "llm triage enabled"
        );
    } else if triage_config.provider == TriageProvider::Agent {
        tracing::info!("external agent triage enabled");
    } else {
        tracing::info!("deterministic triage enabled");
    }

    let repo_context_config = RepoContextConfig::new(
        args.workspace_root.clone(),
        args.repo_context_max_bytes,
        args.repo_context_max_file_bytes,
        args.repo_context_max_files,
    );

    tracing::info!("donkeyspace worker started");

    if args.once {
        if let Some(pool) = &pool {
            poll_once(
                pool,
                &policy,
                &triage_config.provider,
                triage_client.as_ref(),
                &repo_context_config,
                args.github_token.as_deref(),
                &args.worker_id,
                args.lease_seconds,
                args.ready_reconcile_limit,
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
                &triage_config.provider,
                triage_client.as_ref(),
                &repo_context_config,
                args.github_token.as_deref(),
                &args.worker_id,
                args.lease_seconds,
                args.ready_reconcile_limit,
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
    triage_provider: &TriageProvider,
    triage_client: Option<&OpenAiTriageClient>,
    repo_context_config: &RepoContextConfig,
    github_token: Option<&str>,
    worker_id: &str,
    lease_seconds: i32,
    ready_reconcile_limit: i64,
) -> Result<(), Box<dyn std::error::Error>> {
    reconcile_ready_developer_jobs(pool, policy, ready_reconcile_limit).await?;

    match acquire_next_queued_job(pool, worker_id, lease_seconds).await? {
        Some(job) => {
            tracing::info!(
                job_id = %job.id,
                role = job.role,
                "leased queued job"
            );
            execute_job(
                pool,
                policy,
                triage_provider,
                triage_client,
                repo_context_config,
                github_token,
                job,
            )
            .await?;
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

async fn reconcile_ready_developer_jobs(
    pool: &donkeyspace_db::PgPool,
    policy: &Policy,
    limit: i64,
) -> Result<(), Box<dyn std::error::Error>> {
    if !policy.agents.developer.enabled {
        return Ok(());
    }

    let limit = limit.clamp(0, 50);
    if limit == 0 {
        return Ok(());
    }

    for candidate in list_ready_developer_candidates(pool, limit).await? {
        let job = create_job(
            pool,
            Some(candidate.workflow_item_id),
            "developer",
            &candidate.input,
        )
        .await?;
        record_state_transition(
            pool,
            candidate.workflow_item_id,
            Some(job.id),
            Some(WorkflowState::Ready.as_str()),
            "developer_queued",
            "queued developer job during ready issue reconciliation",
        )
        .await?;

        tracing::info!(
            workflow_item_id = candidate.workflow_item_id,
            developer_job_id = %job.id,
            "queued missing developer job for ready issue"
        );
    }

    Ok(())
}

async fn execute_job(
    pool: &donkeyspace_db::PgPool,
    policy: &Policy,
    triage_provider: &TriageProvider,
    triage_client: Option<&OpenAiTriageClient>,
    repo_context_config: &RepoContextConfig,
    github_token: Option<&str>,
    job: JobRecord,
) -> Result<(), Box<dyn std::error::Error>> {
    let Some(running_job) = mark_job_running(pool, job.id).await? else {
        tracing::warn!(job_id = %job.id, "leased job was not available to mark running");
        return Ok(());
    };

    if input_issue_is_closed(&running_job.input) {
        complete_ignored_closed_issue_job(pool, &running_job).await?;
        tracing::info!(
            job_id = %running_job.id,
            role = running_job.role,
            "ignored closed issue job"
        );
        return Ok(());
    }

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

            let repository_context = match build_repository_context(
                &running_job.input,
                running_job.id,
                github_token,
                repo_context_config,
            )
            .await
            {
                Ok(context) => context,
                Err(error) => {
                    fail_triage_job(
                        pool,
                        &running_job,
                        "Repository checkout context failed.",
                        &error.to_string(),
                    )
                    .await?;
                    let _ = cleanup_repository_context(running_job.id, repo_context_config);
                    tracing::warn!(
                        job_id = %running_job.id,
                        "repository context failed"
                    );
                    return Ok(());
                }
            };
            let enriched_input =
                enrich_input_with_repository_context(&running_job.input, repository_context);

            let triage_result = if *triage_provider == TriageProvider::Agent {
                run_agent_triage(
                    pool,
                    policy,
                    &running_job,
                    &enriched_input,
                    repo_context_config,
                )
                .await
            } else {
                run_triage(triage_client, &enriched_input).await
            };

            let (result, transition_reason) = match triage_result {
                Ok(result) => result,
                Err(error) => {
                    fail_triage_job(
                        pool,
                        &running_job,
                        "Triage execution failed.",
                        &error.to_string(),
                    )
                    .await?;
                    let _ = cleanup_repository_context(running_job.id, repo_context_config);
                    tracing::warn!(
                        job_id = %running_job.id,
                        "triage job failed"
                    );
                    return Ok(());
                }
            };
            let result_value = serde_json::to_value(&result)?;
            let workflow_state = workflow_state_for_outcome(result.outcome);
            complete_job(pool, running_job.id, &result_value).await?;
            let _ = cleanup_repository_context(running_job.id, repo_context_config);

            if let Some(workflow_item_id) = running_job.workflow_item_id {
                update_workflow_item_state(pool, workflow_item_id, workflow_state.as_str()).await?;
                record_state_transition(
                    pool,
                    workflow_item_id,
                    Some(running_job.id),
                    None,
                    workflow_state.as_str(),
                    transition_reason,
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

                if result.outcome == Outcome::Ready && policy.agents.developer.enabled {
                    let developer_job = create_job(
                        pool,
                        Some(workflow_item_id),
                        "developer",
                        &running_job.input,
                    )
                    .await?;
                    record_state_transition(
                        pool,
                        workflow_item_id,
                        Some(developer_job.id),
                        Some(WorkflowState::Ready.as_str()),
                        "developer_queued",
                        "queued developer job after ready triage",
                    )
                    .await?;
                    tracing::info!(
                        triage_job_id = %running_job.id,
                        developer_job_id = %developer_job.id,
                        "queued developer job for ready issue"
                    );
                }
            }

            tracing::info!(
                job_id = %running_job.id,
                outcome = ?result.outcome,
                workflow_state = workflow_state.as_str(),
                "job completed"
            );
        }
        "developer" => {
            execute_developer_job(pool, policy, repo_context_config, github_token, running_job)
                .await?;
        }
        "reviewer" => {
            execute_reviewer_job(pool, policy, repo_context_config, github_token, running_job)
                .await?;
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

async fn fail_triage_job(
    pool: &donkeyspace_db::PgPool,
    running_job: &JobRecord,
    summary: &str,
    blocked_reason: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let result_value = json!({
        "outcome": "failed",
        "summary": summary,
        "confidence": "low",
        "risk": "unknown",
        "questions": [],
        "tests": [],
        "changed_files": [],
        "human_review_reason": null,
        "blocked_reason": blocked_reason,
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
            "triage execution failed",
        )
        .await?;
    }

    Ok(())
}

async fn complete_ignored_closed_issue_job(
    pool: &donkeyspace_db::PgPool,
    running_job: &JobRecord,
) -> Result<(), Box<dyn std::error::Error>> {
    let result_value = json!({
        "outcome": "blocked",
        "summary": "Ignored closed GitHub issue.",
        "confidence": "high",
        "risk": "unknown",
        "questions": [],
        "tests": [],
        "changed_files": [],
        "human_review_reason": null,
        "blocked_reason": "closed issues are not eligible for agent work",
    });
    complete_job(pool, running_job.id, &result_value).await?;
    Ok(())
}

async fn execute_developer_job(
    pool: &donkeyspace_db::PgPool,
    policy: &Policy,
    repo_context_config: &RepoContextConfig,
    github_token: Option<&str>,
    running_job: JobRecord,
) -> Result<(), Box<dyn std::error::Error>> {
    if !policy.agents.developer.enabled {
        fail_role_job(
            pool,
            &running_job,
            "Developer execution failed.",
            "developer agent is disabled by policy",
            "developer execution failed",
        )
        .await?;
        return Ok(());
    }
    if policy.agents.developer.command.is_empty() {
        fail_role_job(
            pool,
            &running_job,
            "Developer execution failed.",
            "developer agent command is empty",
            "developer execution failed",
        )
        .await?;
        return Ok(());
    }

    match current_github_issue_is_closed(&running_job.input, github_token).await {
        Ok(true) => {
            complete_ignored_closed_issue_job(pool, &running_job).await?;
            tracing::info!(
                job_id = %running_job.id,
                "ignored developer job because github issue is closed"
            );
            return Ok(());
        }
        Ok(false) => {}
        Err(error) => {
            tracing::warn!(
                job_id = %running_job.id,
                %error,
                "could not verify github issue state before developer execution"
            );
        }
    }

    let repository_context = match build_repository_context(
        &running_job.input,
        running_job.id,
        github_token,
        repo_context_config,
    )
    .await
    {
        Ok(context) => context,
        Err(error) => {
            fail_role_job(
                pool,
                &running_job,
                "Repository checkout context failed.",
                &error.to_string(),
                "developer execution failed",
            )
            .await?;
            let _ = cleanup_repository_context(running_job.id, repo_context_config);
            tracing::warn!(job_id = %running_job.id, "developer repository context failed");
            return Ok(());
        }
    };

    if let Some(workflow_item_id) = running_job.workflow_item_id {
        update_workflow_item_state(pool, workflow_item_id, WorkflowState::InProgress.as_str())
            .await?;
        record_state_transition(
            pool,
            workflow_item_id,
            Some(running_job.id),
            Some(WorkflowState::Ready.as_str()),
            WorkflowState::InProgress.as_str(),
            "developer agent started",
        )
        .await?;
    }

    let enriched_input =
        enrich_input_with_repository_context(&running_job.input, repository_context.clone());
    let developer_result = run_agent_developer(
        pool,
        policy,
        &running_job,
        &enriched_input,
        repo_context_config,
    )
    .await;

    let mut result = match developer_result {
        Ok(result) => result,
        Err(error) => {
            fail_role_job(
                pool,
                &running_job,
                "Developer execution failed.",
                &error.to_string(),
                "developer execution failed",
            )
            .await?;
            let _ = cleanup_repository_context(running_job.id, repo_context_config);
            tracing::warn!(job_id = %running_job.id, "developer job failed");
            return Ok(());
        }
    };

    if result.outcome != Outcome::Implemented {
        let result_value = serde_json::to_value(&result)?;
        let workflow_state = workflow_state_for_outcome(result.outcome);
        complete_job(pool, running_job.id, &result_value).await?;
        let _ = cleanup_repository_context(running_job.id, repo_context_config);

        if let Some(workflow_item_id) = running_job.workflow_item_id {
            update_workflow_item_state(pool, workflow_item_id, workflow_state.as_str()).await?;
            record_state_transition(
                pool,
                workflow_item_id,
                Some(running_job.id),
                Some(WorkflowState::InProgress.as_str()),
                workflow_state.as_str(),
                "developer agent completed without implementation",
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
            "developer job completed without implementation"
        );
        return Ok(());
    }

    let repo_path = repository_checkout_path(&repository_context)?;
    let changed_files = git_changed_files(&repo_path).await?;
    if changed_files.is_empty() {
        fail_role_job(
            pool,
            &running_job,
            "Developer execution failed.",
            "developer agent returned implemented but did not modify the repository checkout",
            "developer execution failed",
        )
        .await?;
        let _ = cleanup_repository_context(running_job.id, repo_context_config);
        return Ok(());
    }

    result.changed_files = changed_files.clone();
    let required_check_results =
        run_required_commands(pool, policy, running_job.id, &repo_path).await?;
    let required_checks_failed = required_check_results
        .iter()
        .any(|check| check.status == TestStatus::Failed);
    result.tests.extend(required_check_results);
    if required_checks_failed {
        result.outcome = Outcome::Failed;
        result.summary = "Developer implementation failed required checks.".to_string();
        result.blocked_reason = Some(required_check_failure_summary(&result.tests));
        let result_value = serde_json::to_value(&result)?;
        fail_job(pool, running_job.id, &result_value).await?;
        let _ = cleanup_repository_context(running_job.id, repo_context_config);

        if let Some(workflow_item_id) = running_job.workflow_item_id {
            update_workflow_item_state(pool, workflow_item_id, WorkflowState::Blocked.as_str())
                .await?;
            record_state_transition(
                pool,
                workflow_item_id,
                Some(running_job.id),
                Some(WorkflowState::InProgress.as_str()),
                WorkflowState::Blocked.as_str(),
                "developer required checks failed",
            )
            .await?;

            for action in triage_github_issue_actions(
                policy,
                &running_job.input,
                &result,
                WorkflowState::Blocked,
            ) {
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

        tracing::warn!(
            job_id = %running_job.id,
            "developer job blocked by required checks"
        );
        return Ok(());
    }

    let issue_num = issue_number(&running_job.input).unwrap_or(0);
    let branch_name = developer_branch_name(issue_num, running_job.id);
    let commit_title = conventional_commit_title(&running_job.input, &changed_files);
    let commit_body = developer_commit_body(&running_job, &result, &changed_files);
    let base_branch = repository_default_branch(&running_job.input);
    let workspace = workspace_path(running_job.id, repo_context_config);
    if let Err(error) = push_developer_branch(
        &repo_path,
        &workspace,
        github_token,
        &branch_name,
        &commit_title,
        &commit_body,
    )
    .await
    {
        fail_role_job(
            pool,
            &running_job,
            "Developer execution failed.",
            &error.to_string(),
            "developer execution failed",
        )
        .await?;
        let _ = cleanup_repository_context(running_job.id, repo_context_config);
        return Ok(());
    }

    let pull_request = async {
        let owner = repository_owner(&running_job.input)?;
        let repo = repository_name(&running_job.input)?;
        let pull_request_body = developer_pull_request_body(&running_job, &result, &changed_files);
        let github_token = github_token
            .ok_or("DONKEYSPACE_GITHUB_TOKEN is required to open developer pull requests")?;
        let github_client = GitHubClient::new(github_token)?;
        let pull_request_url = github_client
            .create_pull_request(
                &owner,
                &repo,
                &commit_title,
                &branch_name,
                &base_branch,
                &pull_request_body,
            )
            .await?;
        Ok::<_, Box<dyn std::error::Error>>((owner, repo, pull_request_url))
    }
    .await;

    let (owner, repo, pull_request_url) = match pull_request {
        Ok(pull_request) => pull_request,
        Err(error) => {
            fail_role_job(
                pool,
                &running_job,
                "Developer execution failed.",
                &error.to_string(),
                "developer execution failed",
            )
            .await?;
            let _ = cleanup_repository_context(running_job.id, repo_context_config);
            return Ok(());
        }
    };

    let result_value = serde_json::to_value(&result)?;
    complete_job(pool, running_job.id, &result_value).await?;
    let _ = cleanup_repository_context(running_job.id, repo_context_config);

    if let Some(workflow_item_id) = running_job.workflow_item_id {
        update_workflow_item_state(pool, workflow_item_id, WorkflowState::PrOpen.as_str()).await?;
        record_state_transition(
            pool,
            workflow_item_id,
            Some(running_job.id),
            Some(WorkflowState::InProgress.as_str()),
            WorkflowState::PrOpen.as_str(),
            "developer agent opened pull request",
        )
        .await?;

        for action in
            triage_github_issue_actions(policy, &running_job.input, &result, WorkflowState::PrOpen)
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

        if let Some(issue_number) = issue_number(&running_job.input) {
            create_outbound_action(
                pool,
                &OutboundActionInput {
                    workflow_item_id,
                    job_id: Some(running_job.id),
                    provider: "github".to_string(),
                    action_type: "issue.create_comment".to_string(),
                    payload: json!({
                        "owner": owner,
                        "repo": repo,
                        "issue_number": issue_number,
                        "body": format!("donkeyspace developer opened a pull request: {pull_request_url}"),
                    }),
                },
            )
            .await?;
        }
    }

    tracing::info!(
        job_id = %running_job.id,
        branch = branch_name,
        pull_request_url,
        "developer job completed"
    );

    Ok(())
}

async fn current_github_issue_is_closed(
    input: &Value,
    github_token: Option<&str>,
) -> Result<bool, Box<dyn std::error::Error>> {
    let Some(token) = github_token.filter(|token| !token.trim().is_empty()) else {
        return Ok(false);
    };
    let Some(issue_number) = issue_number(input) else {
        return Ok(false);
    };

    let client = GitHubClient::new(token)?;
    Ok(client
        .issue_is_closed(
            &repository_owner(input)?,
            &repository_name(input)?,
            issue_number,
        )
        .await?)
}

async fn execute_reviewer_job(
    pool: &donkeyspace_db::PgPool,
    policy: &Policy,
    repo_context_config: &RepoContextConfig,
    github_token: Option<&str>,
    running_job: JobRecord,
) -> Result<(), Box<dyn std::error::Error>> {
    if !policy.agents.reviewer.enabled {
        fail_role_job(
            pool,
            &running_job,
            "Reviewer execution failed.",
            "reviewer agent is disabled by policy",
            "reviewer execution failed",
        )
        .await?;
        return Ok(());
    }
    if policy.agents.reviewer.command.is_empty() {
        fail_role_job(
            pool,
            &running_job,
            "Reviewer execution failed.",
            "reviewer agent command is empty",
            "reviewer execution failed",
        )
        .await?;
        return Ok(());
    }

    let (enriched_input, repository_context) = match prepare_reviewer_input(
        &running_job.input,
        running_job.id,
        github_token,
        repo_context_config,
    )
    .await
    {
        Ok(input) => input,
        Err(error) => {
            fail_role_job(
                pool,
                &running_job,
                "Reviewer repository context failed.",
                &error.to_string(),
                "reviewer execution failed",
            )
            .await?;
            let _ = cleanup_repository_context(running_job.id, repo_context_config);
            tracing::warn!(job_id = %running_job.id, "reviewer repository context failed");
            return Ok(());
        }
    };

    let reviewer_result = run_agent_reviewer(
        pool,
        policy,
        &running_job,
        &enriched_input,
        repo_context_config,
    )
    .await;
    let mut result = match reviewer_result {
        Ok(result) => normalize_reviewer_result(result),
        Err(error) => {
            fail_role_job(
                pool,
                &running_job,
                "Reviewer execution failed.",
                &error.to_string(),
                "reviewer execution failed",
            )
            .await?;
            let _ = cleanup_repository_context(running_job.id, repo_context_config);
            tracing::warn!(job_id = %running_job.id, "reviewer job failed");
            return Ok(());
        }
    };

    result.changed_files = reviewer_changed_files(&enriched_input);
    let workflow_state = workflow_state_for_outcome(result.outcome);
    let result_value = serde_json::to_value(&result)?;
    if result.outcome == Outcome::Failed {
        fail_job(pool, running_job.id, &result_value).await?;
    } else {
        complete_job(pool, running_job.id, &result_value).await?;
    }
    let _ = cleanup_repository_context(running_job.id, repo_context_config);

    if let Some(workflow_item_id) = running_job.workflow_item_id {
        update_workflow_item_state(pool, workflow_item_id, workflow_state.as_str()).await?;
        record_state_transition(
            pool,
            workflow_item_id,
            Some(running_job.id),
            Some(WorkflowState::PrOpen.as_str()),
            workflow_state.as_str(),
            "reviewer agent completed",
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

        create_outbound_action(
            pool,
            &OutboundActionInput {
                workflow_item_id,
                job_id: Some(running_job.id),
                provider: "github".to_string(),
                action_type: "issue.create_comment".to_string(),
                payload: json!({
                    "owner": repository_owner(&running_job.input)?,
                    "repo": repository_name(&running_job.input)?,
                    "issue_number": pull_request_number(&running_job.input).unwrap_or_else(|| issue_number(&running_job.input).unwrap_or(0)),
                    "body": reviewer_comment_body(&result, running_job.id, &repository_context),
                }),
            },
        )
        .await?;
    }

    tracing::info!(
        job_id = %running_job.id,
        outcome = ?result.outcome,
        "reviewer job completed"
    );

    Ok(())
}

async fn fail_role_job(
    pool: &donkeyspace_db::PgPool,
    running_job: &JobRecord,
    summary: &str,
    blocked_reason: &str,
    transition_reason: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let result_value = json!({
        "outcome": "failed",
        "summary": summary,
        "confidence": "low",
        "risk": "unknown",
        "questions": [],
        "tests": [],
        "changed_files": [],
        "human_review_reason": null,
        "blocked_reason": blocked_reason,
    });
    fail_job(pool, running_job.id, &result_value).await?;

    if let Some(workflow_item_id) = running_job.workflow_item_id {
        update_workflow_item_state(pool, workflow_item_id, WorkflowState::Blocked.as_str()).await?;
        record_state_transition(
            pool,
            workflow_item_id,
            Some(running_job.id),
            None,
            WorkflowState::Blocked.as_str(),
            transition_reason,
        )
        .await?;
    }

    Ok(())
}

async fn run_triage(
    triage_client: Option<&OpenAiTriageClient>,
    input: &serde_json::Value,
) -> Result<(RunResult, &'static str), Box<dyn std::error::Error>> {
    let deterministic_result = fake_triage_issue(input);
    if repository_context_short_circuits_triage(input, &deterministic_result) {
        deterministic_result.validate_for_orchestration()?;
        return Ok((deterministic_result, "repository context triage completed"));
    }

    let result = match triage_client {
        Some(client) => (
            client.triage_issue(input).await?,
            "llm triage agent completed",
        ),
        None => (
            fake_triage_issue(input),
            "deterministic triage agent completed",
        ),
    };
    result.0.validate_for_orchestration()?;
    Ok(result)
}

async fn run_agent_triage(
    pool: &donkeyspace_db::PgPool,
    policy: &Policy,
    running_job: &JobRecord,
    input: &Value,
    repo_context_config: &RepoContextConfig,
) -> Result<(RunResult, &'static str), Box<dyn std::error::Error>> {
    if !policy.agents.triage.enabled {
        return Err("triage agent is disabled by policy".into());
    }
    if policy.agents.triage.command.is_empty() {
        return Err("triage agent command is empty".into());
    }

    let workspace_path = workspace_path(running_job.id, repo_context_config);
    let donkeyspace_path = workspace_path.join(".donkeyspace");
    let input_path = donkeyspace_path.join("run-input.json");
    let result_path = donkeyspace_path.join("run-result.json");
    let contract_result_path = ".donkeyspace/run-result.json";
    tokio_fs::create_dir_all(&donkeyspace_path).await?;
    let run_input = agent_run_input(
        running_job.id,
        &running_job.role,
        input,
        contract_result_path,
    );
    tokio_fs::write(&input_path, serde_json::to_vec_pretty(&run_input)?).await?;

    let command =
        AgentCommand::from_parts(&policy.agents.triage.command, &workspace_path, &result_path)?;
    let command_result = run_agent_command(&command).await?;
    record_agent_command_result(pool, running_job.id, "triage agent", &command_result).await?;

    if command_result.status != AgentCommandStatus::Passed {
        return Err(format!(
            "triage agent exited unsuccessfully with code {:?}",
            command_result.exit_code
        )
        .into());
    }

    Ok((
        read_run_result(&result_path).await?,
        "external triage agent completed",
    ))
}

async fn run_agent_developer(
    pool: &donkeyspace_db::PgPool,
    policy: &Policy,
    running_job: &JobRecord,
    input: &Value,
    repo_context_config: &RepoContextConfig,
) -> Result<RunResult, Box<dyn std::error::Error>> {
    let workspace_path = workspace_path(running_job.id, repo_context_config);
    let donkeyspace_path = workspace_path.join(".donkeyspace");
    let input_path = donkeyspace_path.join("run-input.json");
    let result_path = donkeyspace_path.join("run-result.json");
    let contract_result_path = ".donkeyspace/run-result.json";
    tokio_fs::create_dir_all(&donkeyspace_path).await?;
    let run_input = agent_run_input(
        running_job.id,
        &running_job.role,
        input,
        contract_result_path,
    );
    tokio_fs::write(&input_path, serde_json::to_vec_pretty(&run_input)?).await?;

    let command = AgentCommand::from_parts(
        &policy.agents.developer.command,
        &workspace_path,
        &result_path,
    )?;
    let command_result = run_agent_command(&command).await?;
    record_agent_command_result(pool, running_job.id, "developer agent", &command_result).await?;

    if command_result.status != AgentCommandStatus::Passed {
        return Err(format!(
            "developer agent exited unsuccessfully with code {:?}",
            command_result.exit_code
        )
        .into());
    }

    Ok(read_run_result(&result_path).await?)
}

async fn run_agent_reviewer(
    pool: &donkeyspace_db::PgPool,
    policy: &Policy,
    running_job: &JobRecord,
    input: &Value,
    repo_context_config: &RepoContextConfig,
) -> Result<RunResult, Box<dyn std::error::Error>> {
    let workspace_path = workspace_path(running_job.id, repo_context_config);
    let donkeyspace_path = workspace_path.join(".donkeyspace");
    let input_path = donkeyspace_path.join("run-input.json");
    let result_path = donkeyspace_path.join("run-result.json");
    let contract_result_path = ".donkeyspace/run-result.json";
    tokio_fs::create_dir_all(&donkeyspace_path).await?;
    let run_input = agent_run_input(
        running_job.id,
        &running_job.role,
        input,
        contract_result_path,
    );
    tokio_fs::write(&input_path, serde_json::to_vec_pretty(&run_input)?).await?;

    let command = AgentCommand::from_parts(
        &policy.agents.reviewer.command,
        &workspace_path,
        &result_path,
    )?;
    let command_result = run_agent_command(&command).await?;
    record_agent_command_result(pool, running_job.id, "reviewer agent", &command_result).await?;

    if command_result.status != AgentCommandStatus::Passed {
        return Err(format!(
            "reviewer agent exited unsuccessfully with code {:?}",
            command_result.exit_code
        )
        .into());
    }

    Ok(read_run_result(&result_path).await?)
}

async fn prepare_reviewer_input(
    input: &Value,
    job_id: uuid::Uuid,
    github_token: Option<&str>,
    repo_context_config: &RepoContextConfig,
) -> Result<(Value, Value), Box<dyn std::error::Error>> {
    let mut repository_context =
        build_repository_context(input, job_id, github_token, repo_context_config).await?;
    let repo_path = repository_checkout_path(&repository_context)?;
    checkout_pull_request_head(input, job_id, &repo_path, github_token, repo_context_config)
        .await?;

    if let Value::Object(map) = &mut repository_context {
        map.insert("checkout_ref".to_string(), json!("pull_request_head"));
    }

    let mut enriched = enrich_input_with_repository_context(input, repository_context.clone());
    let base_ref = pull_request_base_ref(input).unwrap_or_else(|| repository_default_branch(input));
    let changed_files = git_diff_name_only(&repo_path, &base_ref).await?;
    let diff_summary = git_diff_summary(&repo_path, &base_ref).await?;
    let diff = git_diff_patch(&repo_path, &base_ref).await?;

    if let Some(pull_request) = enriched
        .pointer_mut("/pull_request")
        .and_then(Value::as_object_mut)
    {
        pull_request.insert("changed_files".to_string(), json!(changed_files));
        pull_request.insert("diff_summary".to_string(), json!(diff_summary));
        pull_request.insert("diff".to_string(), json!(truncate_chars(&diff, 20_000)));
    }

    Ok((enriched, repository_context))
}

async fn checkout_pull_request_head(
    input: &Value,
    job_id: uuid::Uuid,
    repo_path: &Path,
    github_token: Option<&str>,
    repo_context_config: &RepoContextConfig,
) -> Result<(), Box<dyn std::error::Error>> {
    let pr_number =
        pull_request_number(input).ok_or("reviewer input is missing pull request number")?;
    let workspace = workspace_path(job_id, repo_context_config);
    let askpass_path = workspace.join("git-askpass.sh");
    let token = github_token.filter(|token| !token.trim().is_empty());
    if token.is_some() {
        write_askpass_script(&askpass_path)?;
    }
    let askpass = token.map(|_| askpass_path.as_path());
    let pr_ref = format!("pull/{pr_number}/head");
    run_git(repo_path, &["fetch", "origin", &pr_ref], token, askpass).await?;
    run_git(
        repo_path,
        &["checkout", "--detach", "FETCH_HEAD"],
        None,
        None,
    )
    .await?;
    Ok(())
}

async fn git_diff_name_only(
    repo_path: &Path,
    base_ref: &str,
) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let base = format!("origin/{base_ref}");
    let output = run_git(
        repo_path,
        &["diff", "--name-only", &base, "HEAD"],
        None,
        None,
    )
    .await?;
    Ok(output
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(ToString::to_string)
        .collect())
}

async fn git_diff_summary(
    repo_path: &Path,
    base_ref: &str,
) -> Result<String, Box<dyn std::error::Error>> {
    let base = format!("origin/{base_ref}");
    run_git(repo_path, &["diff", "--stat", &base, "HEAD"], None, None).await
}

async fn git_diff_patch(
    repo_path: &Path,
    base_ref: &str,
) -> Result<String, Box<dyn std::error::Error>> {
    let base = format!("origin/{base_ref}");
    run_git(
        repo_path,
        &["diff", "--no-ext-diff", "--unified=80", &base, "HEAD"],
        None,
        None,
    )
    .await
}

async fn run_required_commands(
    pool: &donkeyspace_db::PgPool,
    policy: &Policy,
    job_id: uuid::Uuid,
    repo_path: &Path,
) -> Result<Vec<TestResult>, Box<dyn std::error::Error>> {
    let mut results = Vec::new();

    for required in &policy.checks.required_commands {
        let result = run_required_command(pool, job_id, repo_path, required).await?;
        results.push(result);
    }

    Ok(results)
}

async fn run_required_command(
    pool: &donkeyspace_db::PgPool,
    job_id: uuid::Uuid,
    repo_path: &Path,
    required: &RequiredCommand,
) -> Result<TestResult, Box<dyn std::error::Error>> {
    if required.command.is_empty() {
        let summary = "required command is empty".to_string();
        create_command_result(
            pool,
            &CommandResultInput {
                job_id,
                name: required.name.clone(),
                command: Vec::new(),
                status: "failed".to_string(),
                exit_code: None,
                summary: Some(summary.clone()),
            },
        )
        .await?;
        return Ok(TestResult {
            name: required.name.clone(),
            command: Vec::new(),
            status: TestStatus::Failed,
            exit_code: None,
            summary: Some(summary),
        });
    }

    let command =
        AgentCommand::from_parts(&required.command, repo_path, repo_path.join(".unused"))?;
    let output = match run_agent_command(&command).await {
        Ok(output) => output,
        Err(error) => {
            let summary = format!("required command failed to start or complete: {error}");
            create_command_result(
                pool,
                &CommandResultInput {
                    job_id,
                    name: required.name.clone(),
                    command: required.command.clone(),
                    status: "failed".to_string(),
                    exit_code: None,
                    summary: Some(summary.clone()),
                },
            )
            .await?;
            return Ok(TestResult {
                name: required.name.clone(),
                command: required.command.clone(),
                status: TestStatus::Failed,
                exit_code: None,
                summary: Some(summary),
            });
        }
    };

    record_agent_command_result(pool, job_id, &required.name, &output).await?;
    let status = match output.status {
        AgentCommandStatus::Passed => TestStatus::Passed,
        AgentCommandStatus::Failed => TestStatus::Failed,
    };
    let summary = command_summary(&output.stdout, &output.stderr);

    Ok(TestResult {
        name: required.name.clone(),
        command: output.command,
        status,
        exit_code: output.exit_code,
        summary,
    })
}

fn required_check_failure_summary(tests: &[TestResult]) -> String {
    let failures = tests
        .iter()
        .filter(|test| test.status == TestStatus::Failed)
        .map(|test| {
            let command = if test.command.is_empty() {
                "<empty>".to_string()
            } else {
                test.command.join(" ")
            };
            match &test.summary {
                Some(summary) if !summary.trim().is_empty() => {
                    format!("{} (`{}`): {}", test.name, command, summary.trim())
                }
                _ => format!("{} (`{}`) failed", test.name, command),
            }
        })
        .collect::<Vec<_>>();

    if failures.is_empty() {
        "required checks failed".to_string()
    } else {
        truncate_chars(&failures.join("\n\n"), 4_000)
    }
}

fn repository_checkout_path(context: &Value) -> Result<PathBuf, Box<dyn std::error::Error>> {
    let path = context
        .pointer("/checkout_path")
        .and_then(Value::as_str)
        .ok_or("repository context is missing checkout_path")?;
    Ok(PathBuf::from(path))
}

async fn git_changed_files(repo_path: &Path) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let output = run_git(repo_path, &["status", "--porcelain"], None, None).await?;
    Ok(parse_porcelain_status(&output))
}

async fn push_developer_branch(
    repo_path: &Path,
    workspace_path: &Path,
    github_token: Option<&str>,
    branch_name: &str,
    commit_title: &str,
    commit_body: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let token = github_token
        .filter(|token| !token.trim().is_empty())
        .ok_or("DONKEYSPACE_GITHUB_TOKEN is required to push developer branches")?;
    let askpass_path = workspace_path.join("git-askpass.sh");
    write_askpass_script(&askpass_path)?;

    run_git(
        repo_path,
        &["config", "user.name", "donkeyspace"],
        None,
        None,
    )
    .await?;
    run_git(
        repo_path,
        &["config", "user.email", "donkeyspace@example.invalid"],
        None,
        None,
    )
    .await?;
    run_git(repo_path, &["checkout", "-b", branch_name], None, None).await?;
    run_git(repo_path, &["add", "-A"], None, None).await?;
    run_git(
        repo_path,
        &["commit", "-m", commit_title, "-m", commit_body],
        None,
        None,
    )
    .await?;
    let push_ref = format!("HEAD:refs/heads/{branch_name}");
    run_git(
        repo_path,
        &["push", "origin", &push_ref],
        Some(token),
        Some(&askpass_path),
    )
    .await?;

    Ok(())
}

async fn run_git(
    repo_path: &Path,
    args: &[&str],
    github_token: Option<&str>,
    askpass_path: Option<&Path>,
) -> Result<String, Box<dyn std::error::Error>> {
    let mut command = Command::new("git");
    command
        .args(args)
        .current_dir(repo_path)
        .env("GIT_TERMINAL_PROMPT", "0")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());

    if let Some(token) = github_token {
        command.env("DONKEYSPACE_GIT_TOKEN", token);
    }
    if let Some(path) = askpass_path {
        command.env("GIT_ASKPASS", path);
    }

    let output = command.output().await?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(format!("git {:?} failed: {}", args, stderr.trim()).into());
    }

    Ok(String::from_utf8_lossy(&output.stdout).to_string())
}

fn parse_porcelain_status(output: &str) -> Vec<String> {
    output
        .lines()
        .filter_map(|line| {
            let path = line.get(3..)?.trim();
            let path = path.rsplit_once(" -> ").map(|(_, new)| new).unwrap_or(path);
            (!path.is_empty()).then(|| path.to_string())
        })
        .collect()
}

fn developer_branch_name(issue_number: i64, job_id: uuid::Uuid) -> String {
    let short_id = job_id.to_string().chars().take(8).collect::<String>();
    format!("donkeyspace/issue-{issue_number}-{short_id}")
}

fn conventional_commit_title(input: &Value, changed_files: &[String]) -> String {
    let issue_number = issue_number(input).unwrap_or(0);
    let issue_text = format!(
        "{}\n{}",
        input
            .pointer("/issue/title")
            .and_then(Value::as_str)
            .unwrap_or_default(),
        input
            .pointer("/issue/body")
            .and_then(Value::as_str)
            .unwrap_or_default()
    )
    .to_ascii_lowercase();

    if changed_files.iter().all(is_documentation_path) {
        if changed_files.iter().any(|path| {
            path.eq_ignore_ascii_case("README.md") || path.eq_ignore_ascii_case("README")
        }) {
            return format!("docs: update README for issue #{issue_number}");
        }
        return format!("docs: implement issue #{issue_number}");
    }

    if contains_any(
        &issue_text,
        &["bug", "fix", "broken", "error", "fail", "failing"],
    ) {
        return format!("fix: implement issue #{issue_number}");
    }

    if contains_any(
        &issue_text,
        &["feature", "add", "create", "implement", "support"],
    ) {
        return format!("feat: implement issue #{issue_number}");
    }

    format!("chore: implement issue #{issue_number}")
}

fn is_documentation_path(path: &String) -> bool {
    let lower = path.to_ascii_lowercase();
    lower == "readme"
        || lower.starts_with("readme.")
        || lower.ends_with(".md")
        || lower.starts_with("docs/")
}

fn contains_any(haystack: &str, needles: &[&str]) -> bool {
    needles.iter().any(|needle| haystack.contains(needle))
}

fn developer_commit_body(
    running_job: &JobRecord,
    result: &RunResult,
    changed_files: &[String],
) -> String {
    format!(
        "Implements issue #{}.\n\n{}\n\nChanged files:\n{}\n\nGenerated by donkeyspace job {}.",
        issue_number(&running_job.input).unwrap_or(0),
        result.summary,
        changed_files
            .iter()
            .map(|path| format!("- {path}"))
            .collect::<Vec<_>>()
            .join("\n"),
        running_job.id
    )
}

fn developer_pull_request_body(
    running_job: &JobRecord,
    result: &RunResult,
    changed_files: &[String],
) -> String {
    let tests = if result.tests.is_empty() {
        "- Not reported".to_string()
    } else {
        result
            .tests
            .iter()
            .map(|test| {
                format!(
                    "- `{}`: {}{}",
                    test.command.join(" "),
                    test_status_text(test.status),
                    test.summary
                        .as_ref()
                        .map(|summary| format!(" - {summary}"))
                        .unwrap_or_default()
                )
            })
            .collect::<Vec<_>>()
            .join("\n")
    };

    format!(
        "Closes #{}\n\n## Summary\n{}\n\n## Changed Files\n{}\n\n## Tests\n{}\n\nGenerated by donkeyspace developer job `{}`.",
        issue_number(&running_job.input).unwrap_or(0),
        result.summary,
        changed_files
            .iter()
            .map(|path| format!("- `{path}`"))
            .collect::<Vec<_>>()
            .join("\n"),
        tests,
        running_job.id
    )
}

fn normalize_reviewer_result(mut result: RunResult) -> RunResult {
    if matches!(
        result.outcome,
        Outcome::NeedsChanges | Outcome::NeedsHuman | Outcome::Blocked | Outcome::Failed
    ) {
        return result;
    }

    result.summary = format!(
        "Reviewer returned unsupported outcome `{:?}`; routing to human review.",
        result.outcome
    );
    result.outcome = Outcome::NeedsHuman;
    result.confidence = donkeyspace_core::Confidence::Low;
    result.human_review_reason =
        Some("Reviewer returned an unsupported outcome for the reviewer role.".to_string());
    result
}

fn reviewer_comment_body(
    result: &RunResult,
    job_id: uuid::Uuid,
    repository_context: &Value,
) -> String {
    let reason = result
        .human_review_reason
        .as_ref()
        .or(result.blocked_reason.as_ref())
        .map(|value| format!("\n\nReason:\n{value}"))
        .unwrap_or_default();
    let changed_files = result.changed_files.join(", ");
    let changed_files = if changed_files.is_empty() {
        "Not reported".to_string()
    } else {
        changed_files
    };
    let diff_note = repository_context
        .pointer("/truncated")
        .and_then(Value::as_bool)
        .filter(|truncated| *truncated)
        .map(|_| "\n\nNote: repository context was truncated.")
        .unwrap_or_default();

    format!(
        "donkeyspace reviewer result: {}\n\nSummary:\n{}\n\nRisk: {}\nConfidence: {}\nChanged files: {}{}{}\n\nRun: `{}`",
        outcome_text(result.outcome),
        result.summary,
        risk_text(result.risk),
        confidence_text(result.confidence),
        changed_files,
        reason,
        diff_note,
        job_id
    )
}

fn reviewer_changed_files(input: &Value) -> Vec<String> {
    input
        .pointer("/pull_request/changed_files")
        .and_then(Value::as_array)
        .map(|files| {
            files
                .iter()
                .filter_map(Value::as_str)
                .map(ToString::to_string)
                .collect()
        })
        .unwrap_or_default()
}

fn outcome_text(outcome: Outcome) -> &'static str {
    match outcome {
        Outcome::Ready => "ready",
        Outcome::NeedsInfo => "needs_info",
        Outcome::Implemented => "implemented",
        Outcome::NeedsChanges => "needs_changes",
        Outcome::NeedsHuman => "needs_human",
        Outcome::Blocked => "blocked",
        Outcome::Failed => "failed",
    }
}

fn confidence_text(confidence: donkeyspace_core::Confidence) -> &'static str {
    match confidence {
        donkeyspace_core::Confidence::Low => "low",
        donkeyspace_core::Confidence::Medium => "medium",
        donkeyspace_core::Confidence::High => "high",
    }
}

fn risk_text(risk: donkeyspace_core::Risk) -> &'static str {
    match risk {
        donkeyspace_core::Risk::Low => "low",
        donkeyspace_core::Risk::Medium => "medium",
        donkeyspace_core::Risk::High => "high",
        donkeyspace_core::Risk::Unknown => "unknown",
    }
}

fn test_status_text(status: TestStatus) -> &'static str {
    match status {
        TestStatus::Passed => "passed",
        TestStatus::Failed => "failed",
        TestStatus::Skipped => "skipped",
        TestStatus::NotRun => "not_run",
    }
}

fn issue_number(input: &Value) -> Option<i64> {
    input.pointer("/issue/number").and_then(Value::as_i64)
}

fn pull_request_number(input: &Value) -> Option<i64> {
    input
        .pointer("/pull_request/number")
        .and_then(Value::as_i64)
}

fn pull_request_base_ref(input: &Value) -> Option<String> {
    input
        .pointer("/pull_request/base/ref")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .map(ToString::to_string)
}

fn repository_owner(input: &Value) -> Result<String, Box<dyn std::error::Error>> {
    input
        .pointer("/repository/owner/login")
        .and_then(Value::as_str)
        .map(ToString::to_string)
        .ok_or_else(|| "webhook payload is missing repository owner".into())
}

fn repository_name(input: &Value) -> Result<String, Box<dyn std::error::Error>> {
    input
        .pointer("/repository/name")
        .and_then(Value::as_str)
        .map(ToString::to_string)
        .ok_or_else(|| "webhook payload is missing repository name".into())
}

fn repository_default_branch(input: &Value) -> String {
    input
        .pointer("/repository/default_branch")
        .and_then(Value::as_str)
        .filter(|branch| !branch.trim().is_empty())
        .unwrap_or("main")
        .to_string()
}

async fn record_agent_command_result(
    pool: &donkeyspace_db::PgPool,
    job_id: uuid::Uuid,
    name: &str,
    output: &donkeyspace_runner::AgentCommandResult,
) -> Result<(), Box<dyn std::error::Error>> {
    create_command_result(
        pool,
        &CommandResultInput {
            job_id,
            name: name.to_string(),
            command: output.command.clone(),
            status: output.status.as_str().to_string(),
            exit_code: output.exit_code,
            summary: command_summary(&output.stdout, &output.stderr),
        },
    )
    .await?;

    Ok(())
}

fn command_summary(stdout: &str, stderr: &str) -> Option<String> {
    let mut parts = Vec::new();
    if !stdout.trim().is_empty() {
        parts.push(format!("stdout:\n{}", stdout.trim()));
    }
    if !stderr.trim().is_empty() {
        parts.push(format!("stderr:\n{}", stderr.trim()));
    }
    let summary = parts.join("\n\n");
    if summary.is_empty() {
        None
    } else {
        Some(truncate_chars(&summary, 4_000))
    }
}

fn truncate_chars(value: &str, max_chars: usize) -> String {
    if value.chars().count() <= max_chars {
        return value.to_string();
    }

    let mut truncated = value.chars().take(max_chars).collect::<String>();
    truncated.push_str("\n[truncated]");
    truncated
}

#[derive(Debug, Serialize)]
struct AgentRunInput {
    run_id: String,
    role: String,
    repository: AgentRepositoryInput,
    issue: AgentIssueInput,
    pull_request: Option<Value>,
    policy: AgentPolicyInput,
    workspace: AgentWorkspaceInput,
    repository_context: Option<Value>,
}

#[derive(Debug, Serialize)]
struct AgentRepositoryInput {
    provider: String,
    owner: String,
    name: String,
    default_branch: String,
}

#[derive(Debug, Serialize)]
struct AgentIssueInput {
    number: Option<i64>,
    title: String,
    body: String,
    labels: Vec<String>,
    comments: Vec<AgentCommentInput>,
}

#[derive(Debug, Serialize)]
struct AgentCommentInput {
    body: String,
}

#[derive(Debug, Serialize)]
struct AgentPolicyInput {
    path: String,
    snapshot_id: Option<String>,
}

#[derive(Debug, Serialize)]
struct AgentWorkspaceInput {
    repo_path: String,
    result_path: String,
}

fn agent_run_input(
    run_id: uuid::Uuid,
    role: &str,
    input: &Value,
    result_path: &str,
) -> AgentRunInput {
    let issue = input.pointer("/issue").unwrap_or(input);
    let repository_context = input.pointer("/repository_context").cloned();
    let repo_path = repository_context
        .as_ref()
        .and_then(|context| context.pointer("/checkout_path"))
        .and_then(Value::as_str)
        .unwrap_or("repo")
        .to_string();

    AgentRunInput {
        run_id: run_id.to_string(),
        role: role.to_string(),
        repository: AgentRepositoryInput {
            provider: "github".to_string(),
            owner: input
                .pointer("/repository/owner/login")
                .and_then(Value::as_str)
                .unwrap_or("unknown")
                .to_string(),
            name: input
                .pointer("/repository/name")
                .and_then(Value::as_str)
                .unwrap_or("unknown")
                .to_string(),
            default_branch: input
                .pointer("/repository/default_branch")
                .and_then(Value::as_str)
                .unwrap_or("main")
                .to_string(),
        },
        issue: AgentIssueInput {
            number: issue.pointer("/number").and_then(Value::as_i64),
            title: issue
                .pointer("/title")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string(),
            body: issue
                .pointer("/body")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string(),
            labels: issue_labels(issue),
            comments: latest_comment(input)
                .map(|body| vec![AgentCommentInput { body }])
                .unwrap_or_default(),
        },
        pull_request: input.pointer("/pull_request").cloned(),
        policy: AgentPolicyInput {
            path: env::var("DONKEYSPACE_POLICY_PATH")
                .unwrap_or_else(|_| ".donkeyspace/policy.yml".to_string()),
            snapshot_id: None,
        },
        workspace: AgentWorkspaceInput {
            repo_path,
            result_path: result_path.to_string(),
        },
        repository_context,
    }
}

fn issue_labels(issue: &Value) -> Vec<String> {
    issue
        .pointer("/labels")
        .and_then(Value::as_array)
        .map(|labels| {
            labels
                .iter()
                .filter_map(|label| {
                    label
                        .pointer("/name")
                        .and_then(Value::as_str)
                        .or_else(|| label.as_str())
                })
                .map(ToString::to_string)
                .collect()
        })
        .unwrap_or_default()
}

fn latest_comment(input: &Value) -> Option<String> {
    input
        .pointer("/comment/body")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|body| !body.is_empty())
        .map(ToString::to_string)
}

fn repository_context_short_circuits_triage(input: &serde_json::Value, result: &RunResult) -> bool {
    result.outcome == Outcome::Ready
        && input.pointer("/repository_context").is_some()
        && meaningful_word_count(&format!(
            "{}\n{}",
            input
                .pointer("/issue/title")
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default(),
            input
                .pointer("/issue/body")
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default()
        )) < 8
}

fn meaningful_word_count(value: &str) -> usize {
    value
        .split_whitespace()
        .filter(|word| word.chars().any(char::is_alphanumeric))
        .count()
}

fn input_is_donkeyspace_comment(input: &serde_json::Value) -> bool {
    input.pointer("/action").and_then(serde_json::Value::as_str) == Some("created")
        && input
            .pointer("/comment/body")
            .and_then(serde_json::Value::as_str)
            .map(|body| body.trim_start().starts_with("donkeyspace "))
            .unwrap_or(false)
}

fn input_issue_is_closed(input: &serde_json::Value) -> bool {
    input
        .pointer("/issue/state")
        .and_then(serde_json::Value::as_str)
        == Some("closed")
}

fn non_empty_string(value: Option<String>) -> Option<String> {
    value
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
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
    use super::{
        agent_run_input, command_summary, conventional_commit_title, input_is_donkeyspace_comment,
        input_issue_is_closed, non_empty_string, normalize_reviewer_result, parse_porcelain_status,
        repository_context_short_circuits_triage, required_check_failure_summary,
        reviewer_changed_files, reviewer_comment_body,
    };
    use donkeyspace_core::{Confidence, Outcome, Risk, RunResult, TestResult, TestStatus};
    use serde_json::json;
    use uuid::Uuid;

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

    #[test]
    fn closed_issue_input_is_not_eligible_for_agent_work() {
        assert!(input_issue_is_closed(&json!({
            "issue": {"state": "closed"}
        })));
        assert!(!input_issue_is_closed(&json!({
            "issue": {"state": "open"}
        })));
    }

    #[test]
    fn empty_env_values_are_treated_as_missing() {
        assert_eq!(non_empty_string(Some("".to_string())), None);
        assert_eq!(non_empty_string(Some("   ".to_string())), None);
        assert_eq!(
            non_empty_string(Some(" key ".to_string())),
            Some("key".to_string())
        );
    }

    #[test]
    fn short_ready_repo_context_can_skip_llm() {
        let result = RunResult {
            outcome: Outcome::Ready,
            summary: "Ready".to_string(),
            confidence: Confidence::Medium,
            risk: Risk::Low,
            questions: Vec::new(),
            tests: Vec::new(),
            changed_files: Vec::new(),
            human_review_reason: None,
            blocked_reason: None,
        };

        assert!(repository_context_short_circuits_triage(
            &json!({
                "issue": {"title": "Capitize D and S in README", "body": null},
                "repository_context": {"file_tree": ["README.md"]}
            }),
            &result
        ));
    }

    #[test]
    fn agent_run_input_uses_issue_and_workspace_context() {
        let input = json!({
            "repository": {
                "owner": {"login": "example-owner"},
                "name": "example-repo",
                "default_branch": "main"
            },
            "issue": {
                "number": 10,
                "title": "Create src directory",
                "body": "Add a hello world Rust project.",
                "labels": [{"name": "ai:needs-info"}]
            },
            "comment": {"body": "Use cargo."},
            "repository_context": {
                "checkout_path": "/tmp/donkeyspace/workspaces/run/repo",
                "file_tree": ["README.md"]
            }
        });

        let run_input = agent_run_input(
            Uuid::nil(),
            "triage",
            &input,
            ".donkeyspace/run-result.json",
        );

        assert_eq!(run_input.repository.owner, "example-owner");
        assert_eq!(run_input.issue.number, Some(10));
        assert_eq!(run_input.issue.labels, vec!["ai:needs-info"]);
        assert_eq!(run_input.issue.comments[0].body, "Use cargo.");
        assert_eq!(
            run_input.workspace.repo_path,
            "/tmp/donkeyspace/workspaces/run/repo"
        );
        assert_eq!(
            run_input.workspace.result_path,
            ".donkeyspace/run-result.json"
        );
    }

    #[test]
    fn agent_run_input_includes_pull_request_context() {
        let input = json!({
            "repository": {
                "owner": {"login": "example-owner"},
                "name": "example-repo",
                "default_branch": "main"
            },
            "issue": {"number": 12, "title": "Update README", "body": ""},
            "pull_request": {
                "number": 13,
                "title": "docs: update README",
                "changed_files": ["README.md"]
            }
        });

        let run_input = agent_run_input(
            Uuid::nil(),
            "reviewer",
            &input,
            ".donkeyspace/run-result.json",
        );

        assert_eq!(run_input.pull_request.unwrap()["number"], 13);
    }

    #[test]
    fn command_summary_includes_stdout_and_stderr() {
        let summary = command_summary("ok", "warning").unwrap();

        assert!(summary.contains("stdout:\nok"));
        assert!(summary.contains("stderr:\nwarning"));
    }

    #[test]
    fn required_check_failure_summary_lists_failed_checks() {
        let summary = required_check_failure_summary(&[
            TestResult {
                name: "format".to_string(),
                command: vec!["cargo".to_string(), "fmt".to_string()],
                status: TestStatus::Passed,
                exit_code: Some(0),
                summary: None,
            },
            TestResult {
                name: "tests".to_string(),
                command: vec!["cargo".to_string(), "test".to_string()],
                status: TestStatus::Failed,
                exit_code: Some(101),
                summary: Some("stderr:\nfailed".to_string()),
            },
        ]);

        assert!(summary.contains("tests (`cargo test`)"));
        assert!(summary.contains("stderr:\nfailed"));
        assert!(!summary.contains("format"));
    }

    #[test]
    fn reviewer_helpers_report_findings_and_changed_files() {
        let files = reviewer_changed_files(&json!({
            "pull_request": {"changed_files": ["README.md", "src/main.rs"]}
        }));
        assert_eq!(files, vec!["README.md", "src/main.rs"]);

        let result = RunResult {
            outcome: Outcome::NeedsChanges,
            summary: "README wording should be clearer.".to_string(),
            confidence: Confidence::High,
            risk: Risk::Low,
            questions: Vec::new(),
            tests: Vec::new(),
            changed_files: files,
            human_review_reason: None,
            blocked_reason: None,
        };
        let body = reviewer_comment_body(&result, Uuid::nil(), &json!({"truncated": false}));

        assert!(body.contains("donkeyspace reviewer result: needs_changes"));
        assert!(body.contains("README wording should be clearer."));
        assert!(body.contains("README.md, src/main.rs"));
    }

    #[test]
    fn reviewer_unsupported_outcome_routes_to_human() {
        let normalized = normalize_reviewer_result(RunResult {
            outcome: Outcome::Ready,
            summary: "Looks good.".to_string(),
            confidence: Confidence::High,
            risk: Risk::Low,
            questions: Vec::new(),
            tests: Vec::new(),
            changed_files: Vec::new(),
            human_review_reason: None,
            blocked_reason: None,
        });

        assert_eq!(normalized.outcome, Outcome::NeedsHuman);
        assert!(normalized.human_review_reason.is_some());
    }

    #[test]
    fn conventional_commit_title_uses_docs_for_readme_changes() {
        let title = conventional_commit_title(
            &json!({
                "issue": {
                    "number": 12,
                    "title": "Add description to README",
                    "body": ""
                }
            }),
            &["README.md".to_string()],
        );

        assert_eq!(title, "docs: update README for issue #12");
    }

    #[test]
    fn conventional_commit_title_uses_fix_for_bug_language() {
        let title = conventional_commit_title(
            &json!({
                "issue": {
                    "number": 13,
                    "title": "Fix failing login",
                    "body": ""
                }
            }),
            &["src/auth.rs".to_string()],
        );

        assert_eq!(title, "fix: implement issue #13");
    }

    #[test]
    fn conventional_commit_title_uses_feat_for_add_language() {
        let title = conventional_commit_title(
            &json!({
                "issue": {
                    "number": 14,
                    "title": "Add webhook retry endpoint",
                    "body": ""
                }
            }),
            &["src/routes.rs".to_string()],
        );

        assert_eq!(title, "feat: implement issue #14");
    }

    #[test]
    fn parses_git_porcelain_status_paths() {
        let paths = parse_porcelain_status(" M README.md\n?? src/main.rs\nR  old.md -> new.md\n");

        assert_eq!(
            paths,
            vec![
                "README.md".to_string(),
                "src/main.rs".to_string(),
                "new.md".to_string()
            ]
        );
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
