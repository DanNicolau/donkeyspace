use clap::Parser;
use donkeyspace_core::{
    Outcome, Policy, RunResult, fake_triage_issue, triage_github_issue_actions,
    workflow_state_for_outcome,
};
use donkeyspace_db::{
    CommandResultInput, DbConfig, JobRecord, OutboundActionInput, OutboundActionRecord,
    acquire_next_queued_job, apply_migrations, complete_job, connect, create_command_result,
    create_outbound_action, fail_job, list_pending_outbound_actions, mark_job_running,
    mark_outbound_action_completed, mark_outbound_action_failed, record_state_transition,
    update_workflow_item_state,
};
use donkeyspace_github::GitHubClient;
use donkeyspace_runner::{AgentCommand, AgentCommandStatus, read_run_result, run_agent_command};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{env, fs, path::PathBuf, time::Duration};
use tokio::fs as tokio_fs;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

mod llm_triage;
mod repo_context;

use llm_triage::{LlmTriageConfig, OpenAiTriageClient, TriageProvider};
use repo_context::{
    RepoContextConfig, build_repository_context, cleanup_repository_context,
    enrich_input_with_repository_context, workspace_path,
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
) -> Result<(), Box<dyn std::error::Error>> {
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
        pull_request: None,
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
        agent_run_input, command_summary, input_is_donkeyspace_comment, non_empty_string,
        repository_context_short_circuits_triage,
    };
    use donkeyspace_core::{Confidence, Outcome, Risk, RunResult};
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
    fn command_summary_includes_stdout_and_stderr() {
        let summary = command_summary("ok", "warning").unwrap();

        assert!(summary.contains("stdout:\nok"));
        assert!(summary.contains("stderr:\nwarning"));
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
