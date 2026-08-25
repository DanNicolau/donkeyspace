use clap::Parser;
use donkeyspace_core::policy::RequiredCommand;
use donkeyspace_core::{
    Confidence, Outcome, PluginManifest, Policy, Risk, RunResult, TestResult, TestStatus,
    WorkflowState, triage_github_issue_actions, workflow_state_for_outcome,
};
use donkeyspace_db::{
    CommandResultInput, DbConfig, JobRecord, OutboundActionInput, OutboundActionRecord,
    acquire_next_queued_job, apply_migrations, complete_job, connect, create_command_result,
    create_job, create_outbound_action, fail_job, get_job, list_github_repositories,
    list_github_repositories_for_installation, list_pending_agent_publications,
    list_pending_outbound_actions, list_ready_developer_candidates, list_repair_candidates,
    mark_job_running, mark_outbound_action_completed, mark_outbound_action_failed, pause_job,
    record_state_transition, set_checkpoint_pull_request, unpublished_agent_publications_exist,
    update_workflow_item_state,
};
use donkeyspace_github::{
    GitHubAuthConfig, GitHubAuthMode, GitHubClient, GitHubCredentialProvider,
};
use donkeyspace_runner::{AgentCommand, AgentCommandStatus, read_run_result, run_agent_command};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::{BTreeSet, HashSet},
    env, fs,
    path::{Path, PathBuf},
    process::Stdio,
    sync::OnceLock,
    time::Duration,
};
use tokio::fs as tokio_fs;
use tokio::process::Command;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

mod llm_triage;
mod plugin_flow;
mod plugin_task_graph;
mod publication;
mod repo_context;

use llm_triage::{LlmTriageConfig, OpenAiTriageClient, TriageProvider};
use publication::{
    AttemptPublication, PublicationContext, publish_attempt, publish_checkpoint,
    push_existing_publication, queue_publication_status,
};
use repo_context::{
    RepoContextConfig, build_repository_context, cleanup_repository_context,
    enrich_input_with_repository_context, workspace_path, write_askpass_script,
};

static GITHUB_AUTH: OnceLock<GitHubCredentialProvider> = OnceLock::new();

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
    #[arg(long, env = "DONKEYSPACE_REPAIR_RECONCILE_LIMIT", default_value_t = 1)]
    repair_reconcile_limit: i64,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    init_tracing();
    let args = Args::parse();
    let policy = load_policy()?;
    let _ = lifecycle_start_role(&policy)?;

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
        tracing::info!("llm triage unavailable; token usage block result enabled");
    }

    let repo_context_config = RepoContextConfig::new(
        args.workspace_root.clone(),
        args.repo_context_max_bytes,
        args.repo_context_max_file_bytes,
        args.repo_context_max_files,
    );

    tracing::info!("donkeyspace worker started");

    let legacy_pat = non_empty_string(args.github_token.clone());
    let github_auth = match (GitHubCredentialProvider::from_env()?, legacy_pat) {
        (Some(provider), Some(_)) if provider.mode() == GitHubAuthMode::App => {
            return Err("App and PAT credentials cannot be configured together".into());
        }
        (Some(provider), _) => Some(provider),
        (None, Some(token)) if !token.trim().is_empty() => {
            Some(GitHubCredentialProvider::new(GitHubAuthConfig::Pat {
                token,
            })?)
        }
        _ => None,
    };
    if let Some(provider) = &github_auth {
        let _ = GITHUB_AUTH.set(provider.clone());
    }

    let mut label_synced_repositories = HashSet::new();

    if args.once {
        if let Some(pool) = &pool {
            let github_token = match &github_auth {
                Some(provider) => Some(provider.token().await?),
                None => None,
            };
            poll_once(
                pool,
                &policy,
                &triage_config.provider,
                triage_client.as_ref(),
                &repo_context_config,
                github_token.as_deref(),
                &mut label_synced_repositories,
                &args.worker_id,
                args.lease_seconds,
                args.ready_reconcile_limit,
                args.repair_reconcile_limit,
            )
            .await?;
        }
        tracing::info!("worker once mode completed");
        return Ok(());
    }

    loop {
        if let Some(pool) = &pool {
            let github_token = match &github_auth {
                Some(provider) => Some(provider.token().await?),
                None => None,
            };
            poll_once(
                pool,
                &policy,
                &triage_config.provider,
                triage_client.as_ref(),
                &repo_context_config,
                github_token.as_deref(),
                &mut label_synced_repositories,
                &args.worker_id,
                args.lease_seconds,
                args.ready_reconcile_limit,
                args.repair_reconcile_limit,
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
    label_synced_repositories: &mut HashSet<String>,
    worker_id: &str,
    lease_seconds: i32,
    ready_reconcile_limit: i64,
    repair_reconcile_limit: i64,
) -> Result<(), Box<dyn std::error::Error>> {
    reconcile_ready_developer_jobs(pool, policy, ready_reconcile_limit).await?;
    reconcile_pr_repair_jobs(pool, policy, repair_reconcile_limit).await?;

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
        process_pending_publications(pool, github_token, repo_context_config).await?;
        let client = configured_github_client(github_token)?;
        ensure_policy_labels(pool, policy, &client, label_synced_repositories).await?;
        process_outbound_actions(pool, &client).await?;
    } else {
        tracing::debug!("DONKEYSPACE_GITHUB_TOKEN is unset; outbound actions remain pending");
    }

    Ok(())
}

async fn process_pending_publications(
    pool: &donkeyspace_db::PgPool,
    github_token: &str,
    repo_context_config: &RepoContextConfig,
) -> Result<(), Box<dyn std::error::Error>> {
    for publication in list_pending_agent_publications(pool, 20).await? {
        let workspace = workspace_path(publication.coordinator_job_id, repo_context_config);
        let push_succeeded = if let Err(error) =
            push_existing_publication(pool, Some(github_token), &workspace, &publication).await
        {
            tracing::warn!(publication_id = publication.id, %error, "agent publication failed");
            false
        } else {
            true
        };
        if let Err(error) = queue_publication_status(pool, &publication).await {
            tracing::warn!(publication_id = publication.id, %error, "publication status comment queue failed");
        }
        if push_succeeded
            && !unpublished_agent_publications_exist(pool, publication.coordinator_job_id).await?
            && get_job(pool, publication.coordinator_job_id)
                .await?
                .is_some_and(|job| matches!(job.status.as_str(), "completed" | "failed"))
        {
            let _ = cleanup_repository_context(publication.coordinator_job_id, repo_context_config);
        }
    }
    Ok(())
}

async fn cleanup_published_workspace(
    pool: &donkeyspace_db::PgPool,
    coordinator_job_id: uuid::Uuid,
    repo_context_config: &RepoContextConfig,
) {
    match unpublished_agent_publications_exist(pool, coordinator_job_id).await {
        Ok(false) => {
            let _ = cleanup_repository_context(coordinator_job_id, repo_context_config);
        }
        Ok(true) => tracing::warn!(
            job_id = %coordinator_job_id,
            "preserving workspace for failed agent publication retry"
        ),
        Err(error) => tracing::warn!(
            job_id = %coordinator_job_id,
            %error,
            "could not determine publication cleanup state; preserving workspace"
        ),
    }
}

async fn ensure_policy_labels(
    pool: &donkeyspace_db::PgPool,
    policy: &Policy,
    client: &GitHubClient,
    label_synced_repositories: &mut HashSet<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    let labels = policy_managed_labels(policy);
    if labels.is_empty() {
        return Ok(());
    }

    let repositories = match env::var("DONKEYSPACE_GITHUB_INSTALLATION_ID") {
        Ok(installation_id) if !installation_id.trim().is_empty() => {
            list_github_repositories_for_installation(pool, &installation_id).await?
        }
        _ => list_github_repositories(pool).await?,
    };
    for repository in repositories {
        let sync_key = format!("{}/{}", repository.owner, repository.name);
        if label_synced_repositories.contains(&sync_key) {
            continue;
        }

        client
            .ensure_labels(&repository.owner, &repository.name, &labels)
            .await?;
        label_synced_repositories.insert(sync_key.clone());
        tracing::info!(
            repository = sync_key,
            label_count = labels.len(),
            "ensured donkeyspace github labels"
        );
    }

    Ok(())
}

fn policy_managed_labels(policy: &Policy) -> Vec<String> {
    policy
        .workflow
        .state_labels
        .values()
        .chain(policy.workflow.allow_labels.iter())
        .chain(policy.workflow.block_labels.iter())
        .filter(|label| !label.trim().is_empty())
        .cloned()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

async fn reconcile_ready_developer_jobs(
    pool: &donkeyspace_db::PgPool,
    policy: &Policy,
    limit: i64,
) -> Result<(), Box<dyn std::error::Error>> {
    if policy.lifecycle.plugin.is_some() {
        return Ok(());
    }
    if !policy.agents.developer.enabled {
        return Ok(());
    }

    let limit = limit.clamp(0, 50);
    if limit == 0 {
        return Ok(());
    }

    for candidate in list_ready_developer_candidates(pool, limit).await? {
        let current_labels =
            serde_json::from_value::<Vec<String>>(candidate.current_labels.clone())
                .unwrap_or_default();
        let automation_decision = policy.automation_decision_for_labels(&current_labels);
        if !automation_decision.is_allowed() {
            tracing::debug!(
                workflow_item_id = candidate.workflow_item_id,
                reason = automation_decision.reason(),
                "policy skipped ready issue reconciliation"
            );
            continue;
        }

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

async fn reconcile_pr_repair_jobs(
    pool: &donkeyspace_db::PgPool,
    policy: &Policy,
    limit: i64,
) -> Result<(), Box<dyn std::error::Error>> {
    if policy.lifecycle.plugin.is_some() {
        return Ok(());
    }
    if !policy.agents.repair.enabled {
        return Ok(());
    }

    let limit = limit.clamp(0, 50);
    if limit == 0 {
        return Ok(());
    }

    for candidate in list_repair_candidates(pool, limit).await? {
        let mut input = candidate.input;
        attach_repair_pull_request_input(
            &mut input,
            candidate.pr_number,
            candidate.title,
            candidate.html_url,
            candidate.state,
            candidate.head_ref,
            candidate.head_sha,
            candidate.base_ref,
            candidate.base_sha,
        );

        let job = create_job(pool, Some(candidate.workflow_item_id), "repair", &input).await?;
        record_state_transition(
            pool,
            candidate.workflow_item_id,
            Some(job.id),
            Some(WorkflowState::PrOpen.as_str()),
            "repair_queued",
            "queued repair check during PR reconciliation",
        )
        .await?;

        tracing::info!(
            workflow_item_id = candidate.workflow_item_id,
            repair_job_id = %job.id,
            "queued missing repair check for managed pull request"
        );
    }

    Ok(())
}

fn attach_repair_pull_request_input(
    input: &mut Value,
    pr_number: i64,
    title: String,
    html_url: String,
    state: String,
    head_ref: String,
    head_sha: Option<String>,
    base_ref: String,
    base_sha: Option<String>,
) {
    if let Value::Object(map) = input {
        map.insert(
            "pull_request".to_string(),
            json!({
                "number": pr_number,
                "title": title,
                "body": null,
                "html_url": html_url,
                "state": state,
                "draft": false,
                "head": {
                    "ref": head_ref,
                    "sha": head_sha,
                },
                "base": {
                    "ref": base_ref,
                    "sha": base_sha,
                },
            }),
        );
    }
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

    let lifecycle_role = lifecycle_start_role(policy)?;
    if lifecycle_role.as_deref() == Some(running_job.role.as_str()) {
        execute_developer_job(pool, policy, repo_context_config, github_token, running_job).await?;
        return Ok(());
    }

    match running_job.role.as_str() {
        "triage" => {
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

            let (mut result, transition_reason) = match triage_result {
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
            let mut transition_reason = transition_reason.to_string();
            if let Some(policy_reason) = policy.apply_result_routing(&mut result) {
                transition_reason = policy_reason;
            }
            let result_value = serde_json::to_value(&result)?;
            let workflow_state = workflow_state_for_outcome(result.outcome);
            complete_job(pool, running_job.id, &result_value).await?;
            cleanup_published_workspace(pool, running_job.id, repo_context_config).await;

            if let Some(workflow_item_id) = running_job.workflow_item_id {
                update_workflow_item_state(pool, workflow_item_id, workflow_state.as_str()).await?;
                record_state_transition(
                    pool,
                    workflow_item_id,
                    Some(running_job.id),
                    None,
                    workflow_state.as_str(),
                    &transition_reason,
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
        "repair" => {
            execute_repair_job(pool, policy, repo_context_config, github_token, running_job)
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
    let lifecycle_selection = policy.lifecycle.plugin.as_ref();
    if lifecycle_selection.is_none() && !policy.agents.developer.enabled {
        fail_role_job(
            pool,
            &running_job,
            "Implementation execution failed.",
            "developer agent is disabled by policy",
            "developer execution failed",
        )
        .await?;
        return Ok(());
    }
    if lifecycle_selection.is_none()
        && policy.agents.developer.command.is_empty()
        && policy.agents.developer.plugin.is_none()
    {
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
                "ignored implementation job because github issue is closed"
            );
            return Ok(());
        }
        Ok(false) => {}
        Err(error) => {
            tracing::warn!(
                job_id = %running_job.id,
                %error,
                "could not verify github issue state before implementation execution"
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
                "implementation execution failed",
            )
            .await?;
            cleanup_published_workspace(pool, running_job.id, repo_context_config).await;
            tracing::warn!(job_id = %running_job.id, "implementation repository context failed");
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
            if lifecycle_selection.is_some() {
                None
            } else {
                Some(WorkflowState::Ready.as_str())
            },
            WorkflowState::InProgress.as_str(),
            "implementation lifecycle started",
        )
        .await?;

        let state_result = RunResult {
            outcome: Outcome::Implemented,
            summary: "Implementation lifecycle is running.".to_string(),
            confidence: Confidence::High,
            risk: Risk::Low,
            questions: Vec::new(),
            tests: Vec::new(),
            changed_files: Vec::new(),
            human_review_reason: None,
            blocked_reason: None,
        };
        for action in triage_github_issue_actions(
            policy,
            &running_job.input,
            &state_result,
            WorkflowState::InProgress,
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

    let enriched_input =
        enrich_input_with_repository_context(&running_job.input, repository_context.clone());
    let publication_owner = repository_owner(&running_job.input)?;
    let publication_repo = repository_name(&running_job.input)?;
    let publication_workspace = workspace_path(running_job.id, repo_context_config);
    let publication_context = PublicationContext {
        pool,
        coordinator_job_id: running_job.id,
        workflow_item_id: running_job.workflow_item_id,
        issue_number: issue_number(&running_job.input).unwrap_or(0),
        owner: &publication_owner,
        repo: &publication_repo,
        workspace_path: &publication_workspace,
        token: github_token,
    };
    if let Err(error) = publish_checkpoint(
        &publication_context,
        &repository_checkout_path(&repository_context)?,
        &format!(
            "chore(donkeyspace): start issue #{}",
            publication_context.issue_number
        ),
    )
    .await
    {
        tracing::warn!(job_id = %running_job.id, %error, "initial issue branch publication failed");
    }
    let plugin_github_client = github_token
        .filter(|token| !token.trim().is_empty())
        .map(configured_github_client)
        .transpose()?;
    let developer_result =
        if let Some(selection) = lifecycle_selection.or(policy.agents.developer.plugin.as_ref()) {
            plugin_flow::run(
                selection,
                &repository_checkout_path(&repository_context)?,
                &workspace_path(running_job.id, repo_context_config),
                &enriched_input,
                Some(plugin_flow::LifecycleTracking {
                    pool,
                    coordinator: &running_job,
                    github: plugin_github_client.as_ref(),
                    publication: Some(publication_context),
                }),
            )
            .await
        } else {
            run_agent_developer(
                pool,
                policy,
                &running_job,
                &enriched_input,
                repo_context_config,
            )
            .await
        };

    let mut result = match developer_result {
        Ok(result) => result,
        Err(error) => {
            if lifecycle_selection.is_none() {
                publish_job_attempt(
                    &publication_context,
                    &repository_checkout_path(&repository_context)?,
                    &publication_workspace,
                    &running_job,
                    "developer",
                    None,
                    &error.to_string(),
                )
                .await;
            }
            fail_role_job(
                pool,
                &running_job,
                "Implementation execution failed.",
                &error.to_string(),
                "implementation execution failed",
            )
            .await?;
            cleanup_published_workspace(pool, running_job.id, repo_context_config).await;
            tracing::warn!(job_id = %running_job.id, "implementation job failed");
            return Ok(());
        }
    };

    if result.outcome != Outcome::Implemented {
        if lifecycle_selection.is_none() {
            publish_job_attempt(
                &publication_context,
                &repository_checkout_path(&repository_context)?,
                &publication_workspace,
                &running_job,
                "developer",
                Some(result.outcome),
                &result.summary,
            )
            .await;
        }
        let result_value = serde_json::to_value(&result)?;
        let workflow_state = workflow_state_for_outcome(result.outcome);
        let paused = result.outcome == Outcome::NeedsHuman && lifecycle_selection.is_some();
        if paused {
            pause_job(pool, running_job.id, &result_value).await?;
        } else {
            complete_job(pool, running_job.id, &result_value).await?;
            cleanup_published_workspace(pool, running_job.id, repo_context_config).await;
        }

        if let Some(workflow_item_id) = running_job.workflow_item_id {
            update_workflow_item_state(pool, workflow_item_id, workflow_state.as_str()).await?;
            record_state_transition(
                pool,
                workflow_item_id,
                Some(running_job.id),
                Some(WorkflowState::InProgress.as_str()),
                workflow_state.as_str(),
                "implementation lifecycle completed without implementation",
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
            paused,
            "implementation job stopped without implementation"
        );
        return Ok(());
    }

    let repo_path = repository_checkout_path(&repository_context)?;
    let base_branch = repository_default_branch(&running_job.input);
    let changed_files =
        git_changed_files_since(&repo_path, &format!("origin/{base_branch}")).await?;
    if changed_files.is_empty() {
        publish_job_attempt(
            &publication_context,
            &repo_path,
            &publication_workspace,
            &running_job,
            "developer",
            Some(Outcome::Failed),
            "implementation returned implemented without repository changes",
        )
        .await;
        fail_role_job(
            pool,
            &running_job,
            "Implementation execution failed.",
            "implementation lifecycle returned implemented but did not modify the repository checkout",
            "implementation execution failed",
        )
        .await?;
        cleanup_published_workspace(pool, running_job.id, repo_context_config).await;
        return Ok(());
    }

    result.changed_files = changed_files.clone();
    let required_check_results =
        run_required_commands(pool, policy, running_job.id, &repo_path).await?;
    write_required_check_diagnostics(&publication_workspace, &required_check_results).await?;
    let required_checks_failed = required_check_results
        .iter()
        .any(|check| check.status == TestStatus::Failed);
    result.tests.extend(required_check_results);
    if required_checks_failed {
        result.outcome = Outcome::Failed;
        result.summary = "Implementation failed required checks.".to_string();
        result.blocked_reason = Some(required_check_failure_summary(&result.tests));
        publish_job_attempt(
            &publication_context,
            &repo_path,
            &publication_workspace,
            &running_job,
            "required-checks",
            Some(Outcome::Failed),
            result
                .blocked_reason
                .as_deref()
                .unwrap_or("required checks failed"),
        )
        .await;
        let result_value = serde_json::to_value(&result)?;
        fail_job(pool, running_job.id, &result_value).await?;
        cleanup_published_workspace(pool, running_job.id, repo_context_config).await;

        if let Some(workflow_item_id) = running_job.workflow_item_id {
            update_workflow_item_state(pool, workflow_item_id, WorkflowState::Blocked.as_str())
                .await?;
            record_state_transition(
                pool,
                workflow_item_id,
                Some(running_job.id),
                Some(WorkflowState::InProgress.as_str()),
                WorkflowState::Blocked.as_str(),
                "implementation required checks failed",
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
            "implementation job blocked by required checks"
        );
        return Ok(());
    }

    let policy_routing_reason = policy.apply_result_routing(&mut result);
    let issue_num = issue_number(&running_job.input).unwrap_or(0);
    let branch_name = developer_branch_name(issue_num, running_job.id);
    let commit_title = conventional_commit_title(&running_job.input, &changed_files);
    let commit_body = developer_commit_body(&running_job, &result, &changed_files);
    let workspace = workspace_path(running_job.id, repo_context_config);
    if let Err(error) = push_developer_branch(
        &repo_path,
        &workspace,
        github_token,
        &changed_files,
        &branch_name,
        &commit_title,
        &commit_body,
    )
    .await
    {
        publish_job_attempt(
            &publication_context,
            &repo_path,
            &publication_workspace,
            &running_job,
            "developer-push",
            Some(Outcome::Failed),
            &error.to_string(),
        )
        .await;
        fail_role_job(
            pool,
            &running_job,
            "Implementation execution failed.",
            &error.to_string(),
            "implementation execution failed",
        )
        .await?;
        cleanup_published_workspace(pool, running_job.id, repo_context_config).await;
        return Ok(());
    }
    if let Err(error) = publish_checkpoint(&publication_context, &repo_path, &commit_title).await {
        tracing::warn!(job_id = %running_job.id, %error, "final issue branch publication record failed");
    }

    let pull_request = async {
        let owner = repository_owner(&running_job.input)?;
        let repo = repository_name(&running_job.input)?;
        let pull_request_body = developer_pull_request_body(&running_job, &result, &changed_files);
        let github_token = github_token.ok_or(
            "configured GitHub authentication is required to open implementation pull requests",
        )?;
        let github_client = configured_github_client(github_token)?;
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
                "Implementation execution failed.",
                &error.to_string(),
                "implementation execution failed",
            )
            .await?;
            cleanup_published_workspace(pool, running_job.id, repo_context_config).await;
            return Ok(());
        }
    };

    if let Some(publication) =
        set_checkpoint_pull_request(pool, running_job.id, &pull_request_url).await?
        && let Err(error) = queue_publication_status(pool, &publication).await
    {
        tracing::warn!(job_id = %running_job.id, %error, "final publication status queue failed");
    }

    let result_value = serde_json::to_value(&result)?;
    complete_job(pool, running_job.id, &result_value).await?;
    cleanup_published_workspace(pool, running_job.id, repo_context_config).await;

    if let Some(workflow_item_id) = running_job.workflow_item_id {
        let workflow_state = workflow_state_for_outcome(result.outcome);
        update_workflow_item_state(pool, workflow_item_id, workflow_state.as_str()).await?;
        record_state_transition(
            pool,
            workflow_item_id,
            Some(running_job.id),
            Some(WorkflowState::InProgress.as_str()),
            workflow_state.as_str(),
            policy_routing_reason
                .as_deref()
                .unwrap_or("implementation lifecycle opened pull request"),
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
                        "body": format!("donkeyspace implementation lifecycle opened a pull request: {pull_request_url}\n\n<!-- donkeyspace-generated -->"),
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
        "implementation job completed"
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

    let client = configured_github_client(token)?;
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
                    "body": format!(
                        "{}\n\n<!-- donkeyspace-generated -->",
                        reviewer_comment_body(&result, running_job.id, &repository_context)
                    ),
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

async fn execute_repair_job(
    pool: &donkeyspace_db::PgPool,
    policy: &Policy,
    repo_context_config: &RepoContextConfig,
    github_token: Option<&str>,
    running_job: JobRecord,
) -> Result<(), Box<dyn std::error::Error>> {
    if !policy.agents.repair.enabled {
        fail_role_job(
            pool,
            &running_job,
            "Repair execution failed.",
            "repair agent is disabled by policy",
            "repair execution failed",
        )
        .await?;
        return Ok(());
    }
    if policy.agents.repair.command.is_empty() {
        fail_role_job(
            pool,
            &running_job,
            "Repair execution failed.",
            "repair agent command is empty",
            "repair execution failed",
        )
        .await?;
        return Ok(());
    }

    let repair_input = match prepare_repair_input(
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
                "Repair repository context failed.",
                &error.to_string(),
                "repair execution failed",
            )
            .await?;
            let _ = cleanup_repository_context(running_job.id, repo_context_config);
            tracing::warn!(job_id = %running_job.id, "repair repository context failed");
            return Ok(());
        }
    };

    if repair_input.conflicted_files.is_empty() {
        let result = RunResult {
            outcome: Outcome::Reviewed,
            summary: "Pull request branch merged the current base branch without conflicts."
                .to_string(),
            confidence: Confidence::High,
            risk: Risk::Low,
            questions: Vec::new(),
            tests: Vec::new(),
            changed_files: Vec::new(),
            human_review_reason: None,
            blocked_reason: None,
        };
        let result_value = serde_json::to_value(&result)?;
        complete_job(pool, running_job.id, &result_value).await?;
        cleanup_published_workspace(pool, running_job.id, repo_context_config).await;

        if let Some(workflow_item_id) = running_job.workflow_item_id {
            update_workflow_item_state(pool, workflow_item_id, WorkflowState::PrOpen.as_str())
                .await?;
            record_state_transition(
                pool,
                workflow_item_id,
                Some(running_job.id),
                Some(WorkflowState::PrOpen.as_str()),
                WorkflowState::PrOpen.as_str(),
                "repair check found no merge conflict",
            )
            .await?;
        }

        tracing::info!(job_id = %running_job.id, "repair job found no conflict");
        return Ok(());
    }

    if let Some(workflow_item_id) = running_job.workflow_item_id {
        update_workflow_item_state(pool, workflow_item_id, WorkflowState::InProgress.as_str())
            .await?;
        record_state_transition(
            pool,
            workflow_item_id,
            Some(running_job.id),
            Some(WorkflowState::PrOpen.as_str()),
            WorkflowState::InProgress.as_str(),
            "repair agent started",
        )
        .await?;
    }

    let repair_owner = repository_owner(&running_job.input)?;
    let repair_repo_name = repository_name(&running_job.input)?;
    let repair_publication = PublicationContext {
        pool,
        coordinator_job_id: running_job.id,
        workflow_item_id: running_job.workflow_item_id,
        issue_number: issue_number(&running_job.input).unwrap_or(0),
        owner: &repair_owner,
        repo: &repair_repo_name,
        workspace_path: &repair_input.workspace_path,
        token: github_token,
    };
    let repair_repo_path = repository_checkout_path(&repair_input.repository_context)?;

    let repair_result = run_agent_repair(
        pool,
        policy,
        &running_job,
        &repair_input.enriched_input,
        repo_context_config,
    )
    .await;
    let mut result = match repair_result {
        Ok(result) => result,
        Err(error) => {
            publish_job_attempt(
                &repair_publication,
                &repair_repo_path,
                &repair_input.workspace_path,
                &running_job,
                "repair",
                None,
                &error.to_string(),
            )
            .await;
            fail_role_job(
                pool,
                &running_job,
                "Repair execution failed.",
                &error.to_string(),
                "repair execution failed",
            )
            .await?;
            cleanup_published_workspace(pool, running_job.id, repo_context_config).await;
            tracing::warn!(job_id = %running_job.id, "repair job failed");
            return Ok(());
        }
    };

    if result.outcome != Outcome::Implemented {
        publish_job_attempt(
            &repair_publication,
            &repair_repo_path,
            &repair_input.workspace_path,
            &running_job,
            "repair",
            Some(result.outcome),
            &result.summary,
        )
        .await;
        let workflow_state = workflow_state_for_outcome(result.outcome);
        let result_value = serde_json::to_value(&result)?;
        complete_job(pool, running_job.id, &result_value).await?;
        cleanup_published_workspace(pool, running_job.id, repo_context_config).await;

        if let Some(workflow_item_id) = running_job.workflow_item_id {
            update_workflow_item_state(pool, workflow_item_id, workflow_state.as_str()).await?;
            record_state_transition(
                pool,
                workflow_item_id,
                Some(running_job.id),
                Some(WorkflowState::InProgress.as_str()),
                workflow_state.as_str(),
                "repair agent completed without repair",
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

            create_repair_comment_action(pool, workflow_item_id, &running_job, &result).await?;
        }

        return Ok(());
    }

    let repo_path = repository_checkout_path(&repair_input.repository_context)?;
    if conflict_markers_present(&repo_path)? {
        publish_job_attempt(
            &repair_publication,
            &repo_path,
            &repair_input.workspace_path,
            &running_job,
            "repair",
            Some(Outcome::Failed),
            "repair agent left merge conflict markers in the checkout",
        )
        .await;
        fail_role_job(
            pool,
            &running_job,
            "Repair execution failed.",
            "repair agent left merge conflict markers in the checkout",
            "repair conflict markers remain",
        )
        .await?;
        cleanup_published_workspace(pool, running_job.id, repo_context_config).await;
        return Ok(());
    }

    let changed_files = git_changed_files(&repo_path).await?;
    if changed_files.is_empty() {
        publish_job_attempt(
            &repair_publication,
            &repo_path,
            &repair_input.workspace_path,
            &running_job,
            "repair",
            Some(Outcome::Failed),
            "repair agent returned implemented without repository changes",
        )
        .await;
        fail_role_job(
            pool,
            &running_job,
            "Repair execution failed.",
            "repair agent returned implemented but did not modify the repository checkout",
            "repair execution failed",
        )
        .await?;
        cleanup_published_workspace(pool, running_job.id, repo_context_config).await;
        return Ok(());
    }

    result.changed_files = changed_files.clone();
    let required_check_results =
        run_required_commands(pool, policy, running_job.id, &repo_path).await?;
    write_required_check_diagnostics(&repair_input.workspace_path, &required_check_results).await?;
    let required_checks_failed = required_check_results
        .iter()
        .any(|check| check.status == TestStatus::Failed);
    result.tests.extend(required_check_results);
    if required_checks_failed {
        result.outcome = Outcome::Failed;
        result.summary = "Repair failed required checks.".to_string();
        result.blocked_reason = Some(required_check_failure_summary(&result.tests));
        publish_job_attempt(
            &repair_publication,
            &repo_path,
            &repair_input.workspace_path,
            &running_job,
            "repair-required-checks",
            Some(Outcome::Failed),
            result
                .blocked_reason
                .as_deref()
                .unwrap_or("required checks failed"),
        )
        .await;
        let result_value = serde_json::to_value(&result)?;
        fail_job(pool, running_job.id, &result_value).await?;
        cleanup_published_workspace(pool, running_job.id, repo_context_config).await;

        if let Some(workflow_item_id) = running_job.workflow_item_id {
            update_workflow_item_state(pool, workflow_item_id, WorkflowState::Blocked.as_str())
                .await?;
            record_state_transition(
                pool,
                workflow_item_id,
                Some(running_job.id),
                Some(WorkflowState::InProgress.as_str()),
                WorkflowState::Blocked.as_str(),
                "repair required checks failed",
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
            create_repair_comment_action(pool, workflow_item_id, &running_job, &result).await?;
        }

        return Ok(());
    }

    let commit_title = repair_commit_title(&running_job.input);
    let commit_body = repair_commit_body(&running_job, &result, &changed_files);
    if let Err(error) = push_repair_branch(
        &repo_path,
        &repair_input.workspace_path,
        github_token,
        &changed_files,
        &pull_request_head_ref(&running_job.input)?,
        &commit_title,
        &commit_body,
    )
    .await
    {
        publish_job_attempt(
            &repair_publication,
            &repo_path,
            &repair_input.workspace_path,
            &running_job,
            "repair-push",
            Some(Outcome::Failed),
            &error.to_string(),
        )
        .await;
        fail_role_job(
            pool,
            &running_job,
            "Repair push failed.",
            &error.to_string(),
            "repair push failed",
        )
        .await?;
        cleanup_published_workspace(pool, running_job.id, repo_context_config).await;
        tracing::warn!(job_id = %running_job.id, "repair push failed");
        return Ok(());
    }

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
            "repair agent pushed conflict resolution",
        )
        .await?;
        create_repair_comment_action(pool, workflow_item_id, &running_job, &result).await?;
    }

    tracing::info!(job_id = %running_job.id, "repair job completed");
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
    let Some(client) = triage_client else {
        let result = token_usage_exceeded_triage_result();
        result.validate_for_orchestration()?;
        return Ok((result, "llm triage token usage exceeded"));
    };

    match client.triage_issue(input).await {
        Ok(result) => {
            result.validate_for_orchestration()?;
            Ok((result, "llm triage agent completed"))
        }
        Err(error) => {
            tracing::warn!(%error, "llm triage failed without deterministic fallback");
            let result = token_usage_exceeded_triage_result();
            result.validate_for_orchestration()?;
            Ok((result, "llm triage token usage exceeded"))
        }
    }
}

fn token_usage_exceeded_triage_result() -> RunResult {
    RunResult {
        outcome: Outcome::Blocked,
        summary: "LLM triage token usage exceeded.".to_string(),
        confidence: Confidence::High,
        risk: Risk::Unknown,
        questions: Vec::new(),
        tests: Vec::new(),
        changed_files: Vec::new(),
        human_review_reason: None,
        blocked_reason: Some(
            "LLM triage could not run because token usage or provider quota was exceeded. Donkeyspace did not use deterministic fallback triage for this issue."
                .to_string(),
        ),
    }
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
    write_command_logs(&donkeyspace_path, &command_result).await?;

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

async fn run_agent_repair(
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

    let command =
        AgentCommand::from_parts(&policy.agents.repair.command, &workspace_path, &result_path)?;
    let command_result = run_agent_command(&command).await?;
    record_agent_command_result(pool, running_job.id, "repair agent", &command_result).await?;
    write_command_logs(&donkeyspace_path, &command_result).await?;

    if command_result.status != AgentCommandStatus::Passed {
        return Err(format!(
            "repair agent exited unsuccessfully with code {:?}",
            command_result.exit_code
        )
        .into());
    }

    Ok(read_run_result(&result_path).await?)
}

async fn write_command_logs(
    donkeyspace_path: &Path,
    result: &donkeyspace_runner::AgentCommandResult,
) -> Result<(), Box<dyn std::error::Error>> {
    tokio_fs::write(
        donkeyspace_path.join("agent.stdout.log"),
        truncate_chars(&result.stdout, 1_000_000),
    )
    .await?;
    tokio_fs::write(
        donkeyspace_path.join("agent.stderr.log"),
        truncate_chars(&result.stderr, 1_000_000),
    )
    .await?;
    Ok(())
}

async fn write_required_check_diagnostics(
    workspace_path: &Path,
    results: &[TestResult],
) -> Result<(), Box<dyn std::error::Error>> {
    let donkeyspace_path = workspace_path.join(".donkeyspace");
    tokio_fs::create_dir_all(&donkeyspace_path).await?;
    tokio_fs::write(
        donkeyspace_path.join("required-checks.json"),
        serde_json::to_vec_pretty(results)?,
    )
    .await?;
    Ok(())
}

struct RepairInput {
    enriched_input: Value,
    repository_context: Value,
    workspace_path: PathBuf,
    conflicted_files: Vec<String>,
}

async fn prepare_repair_input(
    input: &Value,
    job_id: uuid::Uuid,
    github_token: Option<&str>,
    repo_context_config: &RepoContextConfig,
) -> Result<RepairInput, Box<dyn std::error::Error>> {
    let mut repository_context =
        build_repository_context(input, job_id, github_token, repo_context_config).await?;
    let repo_path = repository_checkout_path(&repository_context)?;
    checkout_pull_request_branch(input, job_id, &repo_path, github_token, repo_context_config)
        .await?;

    if let Value::Object(map) = &mut repository_context {
        map.insert("checkout_ref".to_string(), json!("pull_request_head"));
    }

    let base_ref = pull_request_base_ref(input).unwrap_or_else(|| repository_default_branch(input));
    fetch_base_branch(
        &repo_path,
        &workspace_path(job_id, repo_context_config),
        &base_ref,
        github_token,
    )
    .await?;
    configure_git_author(&repo_path).await?;
    let merge = attempt_base_merge(&repo_path, &base_ref, false).await?;
    let merge = if merge_refused_unrelated_histories(&merge.stderr) {
        attempt_base_merge(&repo_path, &base_ref, true).await?
    } else {
        merge
    };
    let conflicted_files = if merge.success {
        Vec::new()
    } else {
        let files = git_unmerged_files(&repo_path).await?;
        if files.is_empty() {
            return Err(
                format!("base merge failed without unmerged files: {}", merge.stderr).into(),
            );
        }
        files
    };

    let mut enriched = enrich_input_with_repository_context(input, repository_context.clone());
    if let Some(pull_request) = enriched
        .pointer_mut("/pull_request")
        .and_then(Value::as_object_mut)
    {
        pull_request.insert(
            "merge_conflict".to_string(),
            json!({
                "base_ref": base_ref,
                "head_ref": pull_request_head_ref(input)?,
                "conflicted": !conflicted_files.is_empty(),
                "conflicted_files": conflicted_files.clone(),
                "merge_stderr": truncate_chars(&merge.stderr, 4_000),
            }),
        );
    }

    Ok(RepairInput {
        enriched_input: enriched,
        repository_context,
        workspace_path: workspace_path(job_id, repo_context_config),
        conflicted_files,
    })
}

async fn fetch_base_branch(
    repo_path: &Path,
    workspace_path: &Path,
    base_ref: &str,
    github_token: Option<&str>,
) -> Result<(), Box<dyn std::error::Error>> {
    let askpass_path = workspace_path.join("git-askpass.sh");
    let current_token = current_github_token(github_token).await?;
    let token = current_token
        .as_deref()
        .filter(|token| !token.trim().is_empty());
    if token.is_some() {
        write_askpass_script(&askpass_path)?;
    }
    let askpass = token.map(|_| askpass_path.as_path());
    run_git(repo_path, &["fetch", "origin", base_ref], token, askpass).await?;
    Ok(())
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
    let current_token = current_github_token(github_token).await?;
    let token = current_token
        .as_deref()
        .filter(|token| !token.trim().is_empty());
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

async fn checkout_pull_request_branch(
    input: &Value,
    job_id: uuid::Uuid,
    repo_path: &Path,
    github_token: Option<&str>,
    repo_context_config: &RepoContextConfig,
) -> Result<(), Box<dyn std::error::Error>> {
    let head_ref = pull_request_head_ref(input)?;
    let workspace = workspace_path(job_id, repo_context_config);
    let askpass_path = workspace.join("git-askpass.sh");
    let current_token = current_github_token(github_token).await?;
    let token = current_token
        .as_deref()
        .filter(|token| !token.trim().is_empty());
    if token.is_some() {
        write_askpass_script(&askpass_path)?;
    }
    let askpass = token.map(|_| askpass_path.as_path());
    let head_refspec = format!("refs/heads/{head_ref}");
    run_git(
        repo_path,
        &["fetch", "origin", &head_refspec],
        token,
        askpass,
    )
    .await?;
    run_git(
        repo_path,
        &["checkout", "-B", &head_ref, "FETCH_HEAD"],
        None,
        None,
    )
    .await?;
    Ok(())
}

struct MergeAttempt {
    success: bool,
    stderr: String,
}

async fn attempt_base_merge(
    repo_path: &Path,
    base_ref: &str,
    allow_unrelated_histories: bool,
) -> Result<MergeAttempt, Box<dyn std::error::Error>> {
    let base = format!("origin/{base_ref}");
    let mut args = vec!["merge", "--no-commit", "--no-ff"];
    if allow_unrelated_histories {
        args.push("--allow-unrelated-histories");
    }
    args.push(&base);

    let output = Command::new("git")
        .args(args)
        .current_dir(repo_path)
        .env("GIT_TERMINAL_PROMPT", "0")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .await?;

    Ok(MergeAttempt {
        success: output.status.success(),
        stderr: String::from_utf8_lossy(&output.stderr).to_string(),
    })
}

fn merge_refused_unrelated_histories(stderr: &str) -> bool {
    stderr.contains("refusing to merge unrelated histories")
}

async fn git_unmerged_files(repo_path: &Path) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let output = run_git(
        repo_path,
        &["diff", "--name-only", "--diff-filter=U"],
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

async fn git_changed_files_since(
    repo_path: &Path,
    base_ref: &str,
) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let diff = run_git(
        repo_path,
        &[
            "diff",
            "--name-only",
            "--no-renames",
            "--diff-filter=ACDMRTUXB",
            base_ref,
            "--",
        ],
        None,
        None,
    )
    .await?;
    let status = run_git(repo_path, &["status", "--porcelain"], None, None).await?;
    let mut files = diff
        .lines()
        .map(str::trim)
        .filter(|path| !path.is_empty())
        .map(str::to_string)
        .chain(parse_porcelain_status(&status))
        .collect::<Vec<_>>();
    files.sort();
    files.dedup();
    Ok(files)
}

async fn publish_job_attempt(
    context: &PublicationContext<'_>,
    repo_path: &Path,
    workspace_path: &Path,
    job: &JobRecord,
    task: &str,
    outcome: Option<Outcome>,
    reason: &str,
) {
    let changed_files = git_changed_files(repo_path).await.unwrap_or_default();
    if let Err(error) = publish_attempt(
        context,
        repo_path,
        &AttemptPublication {
            job_id: Some(job.id),
            task,
            work_item: None,
            attempt: 1,
            outcome,
            task_root: workspace_path,
            write_roots: &changed_files,
            diagnostics: &[],
            reason,
            related_issue_number: None,
            redactions: &[],
        },
    )
    .await
    {
        tracing::warn!(job_id = %job.id, %error, "job forensic publication failed");
    }
}

async fn push_developer_branch(
    repo_path: &Path,
    workspace_path: &Path,
    github_token: Option<&str>,
    changed_files: &[String],
    branch_name: &str,
    commit_title: &str,
    commit_body: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let current_token = current_github_token(github_token).await?;
    let token = current_token
        .as_deref()
        .ok_or("configured GitHub authentication is required to push developer branches")?;
    let askpass_path = workspace_path.join("git-askpass.sh");
    write_askpass_script(&askpass_path)?;

    configure_git_author(repo_path).await?;
    let current_branch = run_git(repo_path, &["branch", "--show-current"], None, None).await?;
    if current_branch.trim() != branch_name {
        run_git(repo_path, &["checkout", "-b", branch_name], None, None).await?;
    }
    stage_changed_files(repo_path, changed_files).await?;
    let staged = run_git(repo_path, &["diff", "--cached", "--name-only"], None, None).await?;
    if !staged.trim().is_empty() {
        run_git(
            repo_path,
            &["commit", "-m", commit_title, "-m", commit_body],
            None,
            None,
        )
        .await?;
    }
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

async fn push_repair_branch(
    repo_path: &Path,
    workspace_path: &Path,
    github_token: Option<&str>,
    changed_files: &[String],
    branch_name: &str,
    commit_title: &str,
    commit_body: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let current_token = current_github_token(github_token).await?;
    let token = current_token
        .as_deref()
        .ok_or("configured GitHub authentication is required to push repaired branches")?;
    let askpass_path = workspace_path.join("git-askpass.sh");
    write_askpass_script(&askpass_path)?;

    configure_git_author(repo_path).await?;
    stage_changed_files(repo_path, changed_files).await?;
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

async fn stage_changed_files(
    repo_path: &Path,
    changed_files: &[String],
) -> Result<(), Box<dyn std::error::Error>> {
    if changed_files.is_empty() {
        return Err("cannot stage an empty implementation change set".into());
    }

    let mut args = vec!["add", "-A", "--"];
    args.extend(changed_files.iter().map(String::as_str));
    run_git(repo_path, &args, None, None).await?;
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

async fn configure_git_author(repo_path: &Path) -> Result<(), Box<dyn std::error::Error>> {
    run_git(
        repo_path,
        &["config", "user.name", "donkeyspace[bot]"],
        None,
        None,
    )
    .await?;
    run_git(
        repo_path,
        &[
            "config",
            "user.email",
            "donkeyspace-bot@users.noreply.github.com",
        ],
        None,
        None,
    )
    .await?;
    Ok(())
}

fn configured_github_client(
    token: &str,
) -> Result<GitHubClient, donkeyspace_github::GitHubClientError> {
    match GITHUB_AUTH.get() {
        Some(provider) => Ok(provider.client()),
        None => GitHubClient::new(token),
    }
}

pub(crate) async fn current_github_token(
    fallback: Option<&str>,
) -> Result<Option<String>, donkeyspace_github::GitHubClientError> {
    match GITHUB_AUTH.get() {
        Some(provider) => Ok(Some(provider.token().await?)),
        None => Ok(fallback
            .filter(|token| !token.trim().is_empty())
            .map(str::to_string)),
    }
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

fn conflict_markers_present(repo_path: &Path) -> Result<bool, Box<dyn std::error::Error>> {
    conflict_markers_present_in_dir(repo_path, repo_path)
}

fn conflict_markers_present_in_dir(
    repo_path: &Path,
    dir: &Path,
) -> Result<bool, Box<dyn std::error::Error>> {
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        let file_name = entry.file_name();
        if file_name.to_string_lossy() == ".git" {
            continue;
        }

        if path.is_dir() {
            if conflict_markers_present_in_dir(repo_path, &path)? {
                return Ok(true);
            }
        } else if path.is_file() {
            let raw = fs::read(&path)?;
            if raw.contains(&0) {
                continue;
            }
            let content = String::from_utf8_lossy(&raw);
            if content.contains("<<<<<<<") || content.contains(">>>>>>>") {
                tracing::warn!(
                    path = %path.strip_prefix(repo_path).unwrap_or(&path).display(),
                    "repair checkout still contains conflict marker text"
                );
                return Ok(true);
            }
        }
    }
    Ok(false)
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
        "Closes #{}\n\n## Summary\n{}\n\n## Changed Files\n{}\n\n## Tests\n{}\n\nGenerated by donkeyspace implementation job `{}`.",
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

fn repair_commit_title(input: &Value) -> String {
    format!(
        "chore: repair pull request #{}",
        pull_request_number(input).unwrap_or(0)
    )
}

fn repair_commit_body(
    running_job: &JobRecord,
    result: &RunResult,
    changed_files: &[String],
) -> String {
    format!(
        "Repairs merge conflicts for pull request #{}.\n\n{}\n\nChanged files:\n{}\n\nGenerated by donkeyspace repair job {}.",
        pull_request_number(&running_job.input).unwrap_or(0),
        result.summary,
        changed_files
            .iter()
            .map(|path| format!("- {path}"))
            .collect::<Vec<_>>()
            .join("\n"),
        running_job.id
    )
}

async fn create_repair_comment_action(
    pool: &donkeyspace_db::PgPool,
    workflow_item_id: i64,
    running_job: &JobRecord,
    result: &RunResult,
) -> Result<(), Box<dyn std::error::Error>> {
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
                "body": format!(
                    "{}\n\n<!-- donkeyspace-generated -->",
                    repair_comment_body(result, running_job.id)
                ),
            }),
        },
    )
    .await?;
    Ok(())
}

fn repair_comment_body(result: &RunResult, job_id: uuid::Uuid) -> String {
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

    format!(
        "donkeyspace repair result: {}\n\nSummary:\n{}\n\nRisk: {}\nConfidence: {}\nChanged files: {}{}\n\nRun: `{}`",
        outcome_text(result.outcome),
        result.summary,
        risk_text(result.risk),
        confidence_text(result.confidence),
        changed_files,
        reason,
        job_id
    )
}

fn normalize_reviewer_result(mut result: RunResult) -> RunResult {
    if matches!(
        result.outcome,
        Outcome::Reviewed
            | Outcome::NeedsChanges
            | Outcome::NeedsHuman
            | Outcome::Blocked
            | Outcome::Failed
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
        Outcome::Reviewed => "reviewed",
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

fn pull_request_head_ref(input: &Value) -> Result<String, Box<dyn std::error::Error>> {
    input
        .pointer("/pull_request/head/ref")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .map(ToString::to_string)
        .ok_or_else(|| "repair input is missing pull request head ref".into())
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
            Ok(provider_resource_id) => {
                mark_outbound_action_completed(pool, action.id, provider_resource_id.as_deref())
                    .await?;
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
) -> Result<Option<String>, Box<dyn std::error::Error>> {
    let provider_resource_id = match action.action_type.as_str() {
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
            None
        }
        "issue.remove_labels" => {
            let payload: RemoveLabelsPayload = serde_json::from_value(action.payload.clone())?;
            for label in payload.labels {
                client
                    .remove_issue_label(&payload.owner, &payload.repo, payload.issue_number, &label)
                    .await?;
            }
            None
        }
        "issue.create_comment" => {
            let payload: CreateCommentPayload = serde_json::from_value(action.payload.clone())?;
            let comment_id = client
                .create_issue_comment(
                    &payload.owner,
                    &payload.repo,
                    payload.issue_number,
                    &payload.body,
                )
                .await?;
            Some(comment_id)
        }
        "issue.upsert_comment" => {
            let payload: UpsertCommentPayload = serde_json::from_value(action.payload.clone())?;
            let comment_id = client
                .upsert_issue_comment(
                    &payload.owner,
                    &payload.repo,
                    payload.issue_number,
                    &payload.marker,
                    &payload.body,
                )
                .await?;
            Some(comment_id)
        }
        unsupported => {
            return Err(format!("unsupported outbound action type: {unsupported}").into());
        }
    };

    Ok(provider_resource_id)
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

#[derive(Debug, Deserialize)]
struct UpsertCommentPayload {
    owner: String,
    repo: String,
    issue_number: i64,
    marker: String,
    body: String,
}

#[cfg(test)]
mod tests {
    use super::{
        agent_run_input, command_summary, conventional_commit_title, git_changed_files_since,
        input_issue_is_closed, merge_refused_unrelated_histories, non_empty_string,
        normalize_reviewer_result, parse_porcelain_status, policy_managed_labels,
        required_check_failure_summary, reviewer_changed_files, reviewer_comment_body, run_git,
        stage_changed_files, token_usage_exceeded_triage_result,
    };
    use donkeyspace_core::{Confidence, Outcome, Policy, Risk, RunResult, TestResult, TestStatus};
    use serde_json::json;
    use uuid::Uuid;

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
    fn policy_managed_labels_include_state_allow_and_block_labels() {
        let policy = Policy::from_yaml(include_str!("../../../docs/policy.example.yml")).unwrap();
        let labels = policy_managed_labels(&policy);

        assert!(labels.contains(&"ai".to_string()));
        assert!(labels.contains(&"ai:disabled".to_string()));
        assert!(labels.contains(&"ai:ready".to_string()));
        assert_eq!(
            labels.iter().filter(|label| *label == "ai:ready").count(),
            1
        );
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
    fn token_usage_exceeded_result_blocks_triage_without_fallback() {
        let result = token_usage_exceeded_triage_result();

        assert_eq!(result.outcome, Outcome::Blocked);
        assert_eq!(result.summary, "LLM triage token usage exceeded.");
        assert!(
            result
                .blocked_reason
                .unwrap()
                .contains("deterministic fallback")
        );
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
    fn reviewer_reviewed_outcome_is_supported() {
        let result = RunResult {
            outcome: Outcome::Reviewed,
            summary: "No actionable findings.".to_string(),
            confidence: Confidence::High,
            risk: Risk::Low,
            questions: Vec::new(),
            tests: Vec::new(),
            changed_files: vec!["README.md".to_string()],
            human_review_reason: None,
            blocked_reason: None,
        };

        let normalized = normalize_reviewer_result(result.clone());
        assert_eq!(normalized.outcome, Outcome::Reviewed);

        let body = reviewer_comment_body(&result, Uuid::nil(), &json!({"truncated": false}));
        assert!(body.contains("donkeyspace reviewer result: reviewed"));
        assert!(body.contains("No actionable findings."));
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

    #[tokio::test]
    async fn committed_checkpoint_changes_are_visible_to_pr_handoff() {
        let repo_path =
            std::env::temp_dir().join(format!("donkeyspace-checkpoint-diff-{}", Uuid::now_v7()));
        std::fs::create_dir_all(&repo_path).unwrap();

        run_git(&repo_path, &["init"], None, None).await.unwrap();
        run_git(
            &repo_path,
            &["config", "user.name", "Donkeyspace Test"],
            None,
            None,
        )
        .await
        .unwrap();
        run_git(
            &repo_path,
            &["config", "user.email", "test@example.invalid"],
            None,
            None,
        )
        .await
        .unwrap();
        std::fs::write(repo_path.join("README.md"), "base\n").unwrap();
        run_git(&repo_path, &["add", "README.md"], None, None)
            .await
            .unwrap();
        run_git(&repo_path, &["commit", "-m", "initial"], None, None)
            .await
            .unwrap();
        run_git(
            &repo_path,
            &["update-ref", "refs/remotes/origin/main", "HEAD"],
            None,
            None,
        )
        .await
        .unwrap();
        run_git(
            &repo_path,
            &["checkout", "-b", "donkeyspace/issue-35-test"],
            None,
            None,
        )
        .await
        .unwrap();

        std::fs::write(repo_path.join("README.md"), "implemented\n").unwrap();
        run_git(&repo_path, &["add", "README.md"], None, None)
            .await
            .unwrap();
        run_git(&repo_path, &["commit", "-m", "checkpoint"], None, None)
            .await
            .unwrap();
        assert!(
            run_git(&repo_path, &["status", "--porcelain"], None, None)
                .await
                .unwrap()
                .trim()
                .is_empty()
        );

        let changed = git_changed_files_since(&repo_path, "origin/main")
            .await
            .unwrap();
        assert_eq!(changed, vec!["README.md".to_string()]);

        std::fs::remove_dir_all(repo_path).unwrap();
    }

    #[tokio::test]
    async fn publication_stages_only_the_pre_validation_change_set() {
        let repo_path =
            std::env::temp_dir().join(format!("donkeyspace-stage-changes-{}", Uuid::now_v7()));
        std::fs::create_dir_all(&repo_path).unwrap();

        run_git(&repo_path, &["init"], None, None).await.unwrap();
        run_git(
            &repo_path,
            &["config", "user.name", "Donkeyspace Test"],
            None,
            None,
        )
        .await
        .unwrap();
        run_git(
            &repo_path,
            &["config", "user.email", "test@example.invalid"],
            None,
            None,
        )
        .await
        .unwrap();
        std::fs::write(repo_path.join("README.md"), "before\n").unwrap();
        run_git(&repo_path, &["add", "README.md"], None, None)
            .await
            .unwrap();
        run_git(&repo_path, &["commit", "-m", "initial"], None, None)
            .await
            .unwrap();

        std::fs::write(repo_path.join("README.md"), "after\n").unwrap();
        std::fs::create_dir_all(repo_path.join("target")).unwrap();
        std::fs::write(repo_path.join("target/generated"), "validation artifact\n").unwrap();
        stage_changed_files(&repo_path, &["README.md".to_string()])
            .await
            .unwrap();

        let staged = run_git(&repo_path, &["diff", "--cached", "--name-only"], None, None)
            .await
            .unwrap();
        assert_eq!(staged.trim(), "README.md");
        assert!(repo_path.join("target/generated").exists());

        std::fs::remove_dir_all(repo_path).unwrap();
    }

    #[test]
    fn detects_unrelated_history_merge_refusal() {
        assert!(merge_refused_unrelated_histories(
            "fatal: refusing to merge unrelated histories\n"
        ));
        assert!(!merge_refused_unrelated_histories(
            "CONFLICT (content): Merge conflict in README.md\n"
        ));
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
