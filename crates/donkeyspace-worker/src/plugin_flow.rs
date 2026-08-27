use donkeyspace_core::{
    Confidence, Outcome, PluginApprovalMode, PluginArtifact, PluginArtifactType, PluginFlow,
    PluginFlowSelection, PluginManifest, PluginParameter, PluginResourceAssignment,
    PluginResourceSource, PluginTask, PluginTaskResult, PluginTaskScope, PluginValidator,
    PluginWorkItem, PluginWorkItemRegistry, Risk, RunResult, TestResult, TestStatus,
};
use donkeyspace_db::{
    JobRecord, PgPool, complete_job, create_waiting_job, fail_job,
    record_github_managed_resource_for_workflow_item, start_waiting_job,
};
use donkeyspace_github::{GitHubClient, GitHubWorkItem};
use futures::future::join_all;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    env, fs,
    path::{Component, Path, PathBuf},
    process::Stdio,
};
use tokio::process::Command;
use uuid::Uuid;

use crate::active_facade;
use crate::plugin_task_graph::{TaskGraph, TaskKey};
use crate::publication::{
    AttemptPublication, PublicationContext, publish_attempt, publish_checkpoint,
};

pub struct LifecycleTracking<'a> {
    pub pool: &'a PgPool,
    pub coordinator: &'a JobRecord,
    pub github: Option<&'a GitHubClient>,
    pub publication: Option<PublicationContext<'a>>,
}

const CHECKPOINT_VERSION: u32 = 3;
const MAX_RESOURCE_FILES: usize = 1_024;
const MAX_RESOURCE_BYTES: u64 = 32 * 1024 * 1024;

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
struct MaterializedResource {
    id: String,
    source: PluginResourceSource,
    source_path: String,
    root: String,
    available: bool,
    inventory: Vec<String>,
    digest: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct TrackedJobCheckpoint {
    key: TaskKey,
    job_id: Uuid,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct HandoffCheckpoint {
    work_item: Option<String>,
    from: String,
    to: String,
    count: u32,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum ApprovalTrigger {
    Required,
    AgentRequested,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct PendingApproval {
    key: TaskKey,
    trigger: ApprovalTrigger,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case")]
enum HumanDecision {
    Approve {
        target: Option<String>,
    },
    Revise {
        target: Option<String>,
        feedback: String,
    },
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct LifecycleCheckpoint {
    version: u32,
    attempt: u32,
    accumulated_tests: Vec<TestResult>,
    previous: Vec<Value>,
    aggregate_risk: Risk,
    aggregate_confidence: Confidence,
    last_result: RunResult,
    completed_keys: Vec<TaskKey>,
    tracked_jobs: Vec<TrackedJobCheckpoint>,
    handoffs: Vec<HandoffCheckpoint>,
    projected_issues: BTreeMap<String, i64>,
    closed_projected_issues: BTreeSet<String>,
    resume_target: TaskKey,
    #[serde(default)]
    pending_approvals: Vec<PendingApproval>,
    #[serde(default)]
    start_approved: bool,
    #[serde(default)]
    revision_targets: Vec<TaskKey>,
    #[serde(default)]
    active_work_items: Vec<String>,
}

pub async fn run(
    selection: &PluginFlowSelection,
    repo_path: &Path,
    workspace_path: &Path,
    issue_input: &Value,
    tracking: Option<LifecycleTracking<'_>>,
) -> Result<RunResult, Box<dyn std::error::Error>> {
    let manifest = PluginManifest::from_path(&selection.manifest_path)?;
    let parameters = resolve_parameters(&manifest, selection)?;
    let plugin_root = Path::new(&selection.manifest_path)
        .parent()
        .unwrap_or_else(|| Path::new("."));
    let flow = manifest
        .flows
        .get(&selection.flow)
        .ok_or_else(|| format!("plugin `{}` has no flow `{}`", manifest.id, selection.flow))?;
    if flow.replaces_default_lifecycle {
        return run_work_item_lifecycle(
            selection,
            &manifest,
            flow,
            repo_path,
            workspace_path,
            issue_input,
            tracking,
            &parameters,
            plugin_root,
        )
        .await;
    }
    let max_handoffs = selection
        .max_handoffs_per_edge
        .unwrap_or(flow.max_handoffs_per_edge);
    let mut handoffs = BTreeMap::<(String, String), u32>::new();
    let mut stage_name = flow.start.clone();
    let mut attempt = 0u32;
    let mut accumulated_tests = Vec::<TestResult>::new();
    let mut previous = Vec::<Value>::new();

    loop {
        attempt += 1;
        if attempt > 64 {
            return Err("plugin flow exceeded 64 stage attempts".into());
        }
        let stage = flow
            .tasks
            .get(&stage_name)
            .ok_or("plugin stage disappeared after validation")?;
        let agent = manifest
            .roles
            .get(&stage.role)
            .ok_or("plugin agent disappeared after validation")?;
        let stage_root = workspace_path
            .join("plugin-stages")
            .join(format!("{attempt:02}-{stage_name}"));
        let stage_repo = stage_root.join("repo");
        fs::create_dir_all(&stage_repo)?;
        let declared_read = expand_templates(&stage.read, &parameters, None)?;
        let declared_write = expand_templates(&stage.write, &parameters, None)?;
        let (read_roots, write_roots) = resolve_access(
            selection,
            &stage_name,
            &declared_read,
            &declared_write,
            &parameters,
            &manifest.parameters,
        )?;
        let diagnostics = expand_artifacts(&stage.diagnostics, &parameters, None)?;
        if let Some(diagnostic) = diagnostics.iter().find(|diagnostic| {
            !covered(&diagnostic.path, &read_roots) && !covered(&diagnostic.path, &write_roots)
        }) {
            return Err(format!(
                "stage `{stage_name}` diagnostic `{}` is outside its declared roots",
                diagnostic.path
            )
            .into());
        }
        for root in read_roots.iter().chain(&write_roots) {
            copy_root(repo_path, &stage_repo, root)?;
        }

        let donkeyspace = stage_root.join(".donkeyspace");
        fs::create_dir_all(&donkeyspace)?;
        let input_path = donkeyspace.join("run-input.json");
        let result_path = donkeyspace.join("run-result.json");
        let resources = materialize_resources(
            &manifest,
            &stage.role,
            stage,
            plugin_root,
            repo_path,
            &stage_root,
            &parameters,
        )?;
        let selected_mcp = agent
            .mcp_servers
            .iter()
            .filter_map(|name| manifest.mcp_servers.get(name).map(|server| (name, server)))
            .collect::<BTreeMap<_, _>>();
        fs::write(
            &input_path,
            serde_json::to_vec_pretty(&json!({
                "run_id": issue_input.pointer("/run_id"),
                "role": stage.role,
                "plugin": {"id": manifest.id, "flow": selection.flow, "task": stage_name, "attempt": attempt},
                "issue": issue_input.pointer("/issue").unwrap_or(issue_input),
                "repository": issue_input.pointer("/repository"),
                "workspace": {"repo_path": "repo", "result_path": ".donkeyspace/run-result.json", "read": read_roots, "write": write_roots},
                "parameters": parameters,
                "resources": resources,
                "previous_stages": previous,
                "mcp_servers": selected_mcp,
            }))?,
        )?;

        let image = agent
            .image
            .as_deref()
            .unwrap_or(&manifest.runtime.default_image);
        let output = match run_container(
            image,
            &agent.command,
            &stage_root,
            &selection.environment,
            &agent.environment,
        )
        .await
        {
            Ok(output) => output,
            Err(error) => {
                publish_serial_stage_attempt(
                    tracking.as_ref(),
                    selection,
                    &stage_name,
                    attempt,
                    &stage_root,
                    repo_path,
                    &write_roots,
                    &diagnostics,
                    None,
                    &error.to_string(),
                )
                .await;
                return Err(error);
            }
        };
        write_agent_log(&donkeyspace.join("agent.stdout.log"), &output.stdout)?;
        write_agent_log(&donkeyspace.join("agent.stderr.log"), &output.stderr)?;
        let stage_execution = async {
            if !output.status.success() {
                return Err(format!(
                    "plugin stage `{stage_name}` exited {:?}: {}",
                    output.status.code(),
                    String::from_utf8_lossy(&output.stderr).trim()
                )
                .into());
            }
            let raw = fs::read_to_string(&result_path)?;
            let mut stage_result: PluginTaskResult = serde_json::from_str(&raw)?;
            validate_resources_used(&stage_result.resources_used, &resources)?;
            validate_changed_files(&stage_result.result.changed_files, &write_roots)?;
            if is_publishable(stage_result.result.outcome) {
                verify_resources(&stage_root, &resources)?;
                validate_artifacts(
                    &stage_repo,
                    &expand_artifacts(&stage.artifacts, &parameters, None)?,
                    &write_roots,
                )?;
                let validator_results = run_validators(
                    &stage.validators,
                    image,
                    &stage_root,
                    &selection.environment,
                    &agent.environment,
                )
                .await?;
                apply_validator_results(&mut stage_result.result, validator_results);
            }
            stage_result.result.validate_for_orchestration()?;
            Ok::<_, Box<dyn std::error::Error>>(stage_result)
        }
        .await;
        let stage_result = match stage_execution {
            Ok(stage_result) => stage_result,
            Err(error) => {
                publish_serial_stage_attempt(
                    tracking.as_ref(),
                    selection,
                    &stage_name,
                    attempt,
                    &stage_root,
                    repo_path,
                    &write_roots,
                    &diagnostics,
                    None,
                    &error.to_string(),
                )
                .await;
                return Err(error);
            }
        };
        if is_publishable(stage_result.result.outcome) {
            for root in &write_roots {
                replace_root(&stage_repo, repo_path, root)?;
            }
            if let Some(publication) = tracking
                .as_ref()
                .and_then(|tracking| tracking.publication.as_ref())
                && let Err(error) = publish_checkpoint(
                    publication,
                    repo_path,
                    &format!(
                        "chore({}): checkpoint {} for issue #{}",
                        active_facade().command,
                        stage_name,
                        publication.issue_number
                    ),
                )
                .await
            {
                tracing::warn!(%error, stage = stage_name, "plugin stage checkpoint failed");
            }
            if diagnostics_present_at(&stage_repo, &diagnostics) {
                publish_serial_stage_attempt(
                    tracking.as_ref(),
                    selection,
                    &stage_name,
                    attempt,
                    &stage_root,
                    repo_path,
                    &write_roots,
                    &diagnostics,
                    Some(stage_result.result.outcome),
                    &stage_result.result.summary,
                )
                .await;
            }
        } else {
            publish_serial_stage_attempt(
                tracking.as_ref(),
                selection,
                &stage_name,
                attempt,
                &stage_root,
                repo_path,
                &write_roots,
                &diagnostics,
                Some(stage_result.result.outcome),
                &stage_result.result.summary,
            )
            .await;
        }
        accumulated_tests.extend(stage_result.result.tests.clone());
        previous.push(json!({"stage": stage_name, "attempt": attempt, "outcome": stage_result.result.outcome, "summary": stage_result.result.summary}));

        match stage_result.result.outcome {
            Outcome::Implemented if stage.terminal => {
                let mut result = stage_result.result;
                result.tests = accumulated_tests;
                result.summary = flow_summary(&previous, &result.summary);
                if result.tests.is_empty() {
                    return Err("terminal plugin result did not report tests".into());
                }
                return Ok(result);
            }
            Outcome::Implemented => {
                stage_name = stage
                    .transitions
                    .get("implemented")
                    .cloned()
                    .ok_or_else(|| format!("stage `{stage_name}` has no implemented transition"))?;
            }
            Outcome::NeedsChanges => {
                let handoff = stage_result.handoff.ok_or_else(|| {
                    format!("stage `{stage_name}` returned needs_changes without a handoff")
                })?;
                if !stage.allowed_handoffs.contains(&handoff.target) {
                    return Err(format!(
                        "stage `{stage_name}` cannot hand off to `{}`",
                        handoff.target
                    )
                    .into());
                }
                let key = (stage_name.clone(), handoff.target.clone());
                let count = handoffs.entry(key).or_default();
                *count += 1;
                if *count > max_handoffs {
                    let mut result = stage_result.result;
                    result.outcome = Outcome::NeedsHuman;
                    result.human_review_reason = Some(format!(
                        "handoff from `{stage_name}` to `{}` exceeded policy limit {max_handoffs}: {}",
                        handoff.target, handoff.reason
                    ));
                    result.tests = accumulated_tests;
                    result.summary = flow_summary(&previous, &result.summary);
                    return Ok(result);
                }
                previous.push(json!({"handoff": {"from": stage_name, "to": handoff.target, "reason": handoff.reason}}));
                stage_name = handoff.target;
            }
            _ => {
                let mut result = stage_result.result;
                result.tests = accumulated_tests;
                result.summary = flow_summary(&previous, &result.summary);
                return Ok(result);
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_work_item_lifecycle(
    selection: &PluginFlowSelection,
    manifest: &PluginManifest,
    flow: &PluginFlow,
    repo_path: &Path,
    workspace_path: &Path,
    issue_input: &Value,
    tracking: Option<LifecycleTracking<'_>>,
    parameters: &BTreeMap<String, Value>,
    plugin_root: &Path,
) -> Result<RunResult, Box<dyn std::error::Error>> {
    let checkpoint_path = workspace_path
        .join(".donkeyspace")
        .join("lifecycle-checkpoint.json");
    let is_resume = issue_input
        .pointer("/donkeyspace_resume")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let mut checkpoint = if is_resume {
        if checkpoint_path.is_file() {
            let checkpoint: LifecycleCheckpoint =
                serde_json::from_str(&fs::read_to_string(&checkpoint_path)?)?;
            if !matches!(checkpoint.version, 1 | 2 | CHECKPOINT_VERSION) {
                return Err(format!(
                    "unsupported lifecycle checkpoint version {}",
                    checkpoint.version
                )
                .into());
            }
            Some(checkpoint)
        } else {
            // A planner can request human input before a work-item graph
            // exists. In that case the retained workspace is authoritative
            // and the planner is rerun in-place with the human response.
            None
        }
    } else {
        None
    };

    let mut rerun_start = false;
    if let Some(saved) = checkpoint.as_mut()
        && !saved.pending_approvals.is_empty()
    {
        let decision_value = issue_input
            .pointer("/donkeyspace_human_decision")
            .cloned()
            .ok_or("resumed approval checkpoint is missing a human decision")?;
        let decision = serde_json::from_value::<HumanDecision>(decision_value)?;
        let selected = match select_pending_approvals(&saved.pending_approvals, &decision) {
            Ok(selected) => selected,
            Err(error) => {
                let result = pending_approval_result(
                    &saved.pending_approvals,
                    &saved.projected_issues,
                    &format!("The approval command was not applied: {error}"),
                );
                saved.version = CHECKPOINT_VERSION;
                saved.last_result = result.clone();
                write_lifecycle_checkpoint(&checkpoint_path, saved)?;
                return Ok(finish_result(
                    result,
                    saved.accumulated_tests.clone(),
                    &saved.previous,
                ));
            }
        };
        let selected_keys = selected
            .iter()
            .map(|approval| approval.key.clone())
            .collect::<BTreeSet<_>>();
        let feedback = match &decision {
            HumanDecision::Approve { .. } => "approved".to_string(),
            HumanDecision::Revise { feedback, .. } => feedback.trim().to_string(),
        };
        for approval in &selected {
            let is_start = approval.key.work_item.is_none() && approval.key.task == flow.start;
            match (&decision, approval.trigger, is_start) {
                (HumanDecision::Approve { .. }, ApprovalTrigger::Required, true) => {
                    saved.start_approved = true;
                }
                (HumanDecision::Approve { .. }, ApprovalTrigger::Required, false) => {
                    if !saved.completed_keys.contains(&approval.key) {
                        saved.completed_keys.push(approval.key.clone());
                    }
                }
                (HumanDecision::Revise { .. }, _, true) => {
                    saved.start_approved = false;
                    rerun_start = true;
                }
                (HumanDecision::Revise { .. }, _, false)
                | (HumanDecision::Approve { .. }, ApprovalTrigger::AgentRequested, false) => {
                    if !saved.revision_targets.contains(&approval.key) {
                        saved.revision_targets.push(approval.key.clone());
                    }
                }
                (HumanDecision::Approve { .. }, ApprovalTrigger::AgentRequested, true) => {
                    rerun_start = true;
                }
            }
            saved.previous.push(json!({
                "human_response": feedback,
                "human_decision": issue_input.pointer("/donkeyspace_human_decision"),
                "resume_target": approval.key,
            }));
        }
        saved
            .pending_approvals
            .retain(|approval| !selected_keys.contains(&approval.key));
        if !saved.pending_approvals.is_empty() {
            saved.version = CHECKPOINT_VERSION;
            let result = pending_approval_result(
                &saved.pending_approvals,
                &saved.projected_issues,
                "Some approvals remain pending.",
            );
            saved.last_result = result.clone();
            write_lifecycle_checkpoint(&checkpoint_path, saved)?;
            return Ok(finish_result(
                result,
                saved.accumulated_tests.clone(),
                &saved.previous,
            ));
        }
    }
    let revision_targets = checkpoint
        .as_ref()
        .map(|checkpoint| checkpoint.revision_targets.clone())
        .unwrap_or_default();

    let (
        mut previous,
        mut accumulated_tests,
        mut attempt,
        mut aggregate_risk,
        mut aggregate_confidence,
        mut last_result,
        requested_work_items,
    ) = if let Some(checkpoint) = &checkpoint
        && !rerun_start
    {
        let mut previous = checkpoint.previous.clone();
        if checkpoint.version == 1 {
            previous.push(json!({
                "human_response": issue_input.pointer("/comment/body").and_then(Value::as_str),
                "human_decision": issue_input.pointer("/donkeyspace_human_decision"),
                "resume_target": checkpoint.resume_target,
            }));
        }
        (
            previous,
            checkpoint.accumulated_tests.clone(),
            checkpoint.attempt,
            checkpoint.aggregate_risk,
            checkpoint.aggregate_confidence,
            checkpoint.last_result.clone(),
            (!checkpoint.active_work_items.is_empty())
                .then(|| checkpoint.active_work_items.clone()),
        )
    } else {
        let mut previous = checkpoint
            .as_ref()
            .map(|checkpoint| checkpoint.previous.clone())
            .unwrap_or_default();
        if is_resume {
            previous.push(json!({
                "human_response": issue_input.pointer("/comment/body").and_then(Value::as_str),
                "human_decision": issue_input.pointer("/donkeyspace_human_decision"),
                "resume_target": {"work_item": null, "task": flow.start},
            }));
        }
        let mut accumulated_tests = Vec::<TestResult>::new();
        let attempt = 1u32;
        let planner_execution = execute_task(
            selection,
            manifest,
            &flow.start,
            &flow.tasks[&flow.start],
            None,
            attempt,
            repo_path,
            workspace_path,
            issue_input,
            &previous,
            parameters,
            plugin_root,
        )
        .await;
        let planner = match planner_execution {
            Ok(planner) => planner,
            Err(error) => {
                let reason = error.to_string();
                publish_task_attempt(
                    tracking.as_ref(),
                    selection,
                    &flow.start,
                    &flow.tasks[&flow.start],
                    None,
                    attempt,
                    workspace_path,
                    repo_path,
                    parameters,
                    &manifest.parameters,
                    tracking.as_ref().map(|tracking| tracking.coordinator.id),
                    None,
                    &reason,
                    None,
                )
                .await;
                return Err(error);
            }
        };
        accumulated_tests.extend(planner.result.tests.clone());
        let requested_work_items = planner.work_items.clone();
        let aggregate_risk = planner.result.risk;
        let aggregate_confidence = planner.result.confidence;
        previous.push(task_summary(&flow.start, None, attempt, &planner.result));
        if planner.result.outcome == Outcome::Implemented
            && let Some(publication) = tracking
                .as_ref()
                .and_then(|tracking| tracking.publication.as_ref())
            && let Err(error) = publish_checkpoint(
                publication,
                repo_path,
                &format!(
                    "chore({}): checkpoint {} for issue #{}",
                    active_facade().command,
                    flow.start,
                    publication.issue_number
                ),
            )
            .await
        {
            tracing::warn!(%error, task = flow.start, "plugin checkpoint publication failed");
        }
        if planner.result.outcome != Outcome::Implemented {
            publish_task_attempt(
                tracking.as_ref(),
                selection,
                &flow.start,
                &flow.tasks[&flow.start],
                None,
                attempt,
                workspace_path,
                repo_path,
                parameters,
                &manifest.parameters,
                tracking.as_ref().map(|tracking| tracking.coordinator.id),
                Some(planner.result.outcome),
                &planner.result.summary,
                None,
            )
            .await;
            return Ok(finish_result(planner.result, accumulated_tests, &previous));
        }
        (
            previous,
            accumulated_tests,
            attempt,
            aggregate_risk,
            aggregate_confidence,
            planner.result,
            requested_work_items,
        )
    };

    let registry_template = flow
        .work_items_path
        .as_deref()
        .ok_or("lifecycle flow is missing work_items_path")?;
    let registry_path = expand_template(registry_template, parameters)?;
    let registry: PluginWorkItemRegistry =
        serde_json::from_str(&fs::read_to_string(repo_path.join(&registry_path))?)?;
    validate_work_items(&registry.work_items)?;
    let work_items =
        select_lifecycle_work_items(&registry.work_items, requested_work_items.as_deref())?;
    if let Some(item) = work_items
        .iter()
        .find(|item| !repo_path.join(&item.spec).is_file())
    {
        return Err(format!(
            "architect work item `{}` references missing specification `{}`",
            item.id, item.spec
        )
        .into());
    }
    let github_coordinates = (
        issue_input
            .pointer("/repository/owner/login")
            .and_then(Value::as_str),
        issue_input
            .pointer("/repository/name")
            .and_then(Value::as_str),
        issue_input.pointer("/issue/number").and_then(Value::as_i64),
    );
    let mut graph = TaskGraph::for_work_items(flow, &work_items);
    let mut projected_issues = checkpoint
        .as_ref()
        .map(|checkpoint| checkpoint.projected_issues.clone())
        .unwrap_or_default();
    let mut tracked_jobs = checkpoint
        .as_ref()
        .map(|checkpoint| {
            checkpoint
                .tracked_jobs
                .iter()
                .map(|entry| (entry.key.clone(), entry.job_id))
                .collect::<BTreeMap<_, _>>()
        })
        .unwrap_or_default();
    let mut finished_jobs = checkpoint
        .as_ref()
        .map(|checkpoint| checkpoint.completed_keys.iter().cloned().collect())
        .unwrap_or_default();
    if let Some(checkpoint) = &checkpoint {
        graph.restore_completed(&checkpoint.completed_keys)?;
    }
    let mut revision_keys = BTreeSet::new();
    for target in &revision_targets {
        revision_keys.extend(graph.restart_from(target)?);
    }
    if checkpoint.is_none() || rerun_start {
        if rerun_start
            && let Some(github) = tracking.as_ref().and_then(|tracking| tracking.github)
            && let (Some(owner), Some(repo), _) = github_coordinates
        {
            let active_ids = work_items
                .iter()
                .map(|item| item.id.as_str())
                .collect::<BTreeSet<_>>();
            let removed = projected_issues
                .iter()
                .filter(|(id, _)| !active_ids.contains(id.as_str()))
                .map(|(id, issue)| (id.clone(), *issue))
                .collect::<Vec<_>>();
            for (id, issue_number) in removed {
                if let Err(error) = github.close_issue(owner, repo, issue_number).await {
                    tracing::warn!(%error, issue_number, "failed to close removed projected issue");
                }
                projected_issues.remove(&id);
            }
        }
        if flow.project_github_issues
            && let Some(github) = tracking.as_ref().and_then(|tracking| tracking.github)
            && let (Some(owner), Some(repo), Some(parent_issue_number)) = github_coordinates
        {
            let github_work_items = work_items
                .iter()
                .map(|item| GitHubWorkItem {
                    id: item.id.clone(),
                    spec: item.spec.clone(),
                    body: fs::read_to_string(repo_path.join(&item.spec))
                        .unwrap_or_default()
                        .chars()
                        .take(50_000)
                        .collect(),
                    depends_on: item.depends_on.clone(),
                })
                .collect::<Vec<_>>();

            if rerun_start {
                for item in &github_work_items {
                    let Some(issue_number) = projected_issues.get(&item.id) else {
                        continue;
                    };
                    if let Err(error) = github
                        .update_projected_work_item(
                            owner,
                            repo,
                            parent_issue_number,
                            *issue_number,
                            item,
                        )
                        .await
                    {
                        tracing::warn!(
                            %error,
                            issue_number,
                            work_item = item.id,
                            "failed to update revised projected issue"
                        );
                    }
                }
            }

            let github_work_items = github_work_items
                .into_iter()
                .filter(|item| !projected_issues.contains_key(&item.id))
                .collect::<Vec<_>>();
            match github
                .project_work_items(owner, repo, parent_issue_number, &github_work_items)
                .await
            {
                Ok(issues) => {
                    if let Some(tracking) = &tracking
                        && let Some(workflow_item_id) = tracking.coordinator.workflow_item_id
                    {
                        for (work_item, issue) in &issues {
                            record_github_managed_resource_for_workflow_item(
                                tracking.pool,
                                workflow_item_id,
                                "issue",
                                &issue.id.to_string(),
                                &json!({"work_item": work_item, "issue_number": issue.number}),
                            )
                            .await?;
                        }
                    }
                    projected_issues.extend(
                        issues
                            .into_iter()
                            .map(|(work_item, issue)| (work_item, issue.number)),
                    );
                }
                Err(error) => tracing::warn!(%error, "github work-item projection failed"),
            }
        }
    }

    let start_approved = checkpoint
        .as_ref()
        .is_some_and(|checkpoint| checkpoint.start_approved)
        && !rerun_start;
    if flow.tasks[&flow.start].approval == PluginApprovalMode::Required && !start_approved {
        let pending = vec![PendingApproval {
            key: TaskKey {
                work_item: None,
                task: flow.start.clone(),
            },
            trigger: ApprovalTrigger::Required,
        }];
        let result = pending_approval_result(
            &pending,
            &projected_issues,
            "The lifecycle start task completed successfully and requires approval before downstream work begins.",
        );
        write_lifecycle_checkpoint(
            &checkpoint_path,
            &LifecycleCheckpoint {
                version: CHECKPOINT_VERSION,
                attempt,
                accumulated_tests: accumulated_tests.clone(),
                previous: previous.clone(),
                aggregate_risk,
                aggregate_confidence,
                last_result: result.clone(),
                completed_keys: Vec::new(),
                tracked_jobs: Vec::new(),
                handoffs: Vec::new(),
                projected_issues: projected_issues.clone(),
                closed_projected_issues: BTreeSet::new(),
                resume_target: pending[0].key.clone(),
                pending_approvals: pending,
                start_approved: false,
                revision_targets: Vec::new(),
                active_work_items: work_items.iter().map(|item| item.id.clone()).collect(),
            },
        )?;
        return Ok(finish_result(result, accumulated_tests, &previous));
    }

    if let Some(tracking) = &tracking {
        for key in graph.keys() {
            if !graph.is_completed(key)
                && (!tracked_jobs.contains_key(key) || revision_keys.contains(key))
            {
                let job = create_tracked_job(
                    tracking,
                    manifest,
                    &selection.flow,
                    flow,
                    key,
                    &work_items,
                    issue_input,
                )
                .await?;
                tracked_jobs.insert(key.clone(), job.id);
            }
        }
    }
    let max_handoffs = selection
        .max_handoffs_per_edge
        .unwrap_or(flow.max_handoffs_per_edge);
    let mut handoffs = checkpoint
        .as_ref()
        .map(|checkpoint| {
            checkpoint
                .handoffs
                .iter()
                .map(|entry| {
                    (
                        (
                            entry.work_item.clone(),
                            entry.from.clone(),
                            entry.to.clone(),
                        ),
                        entry.count,
                    )
                })
                .collect::<BTreeMap<_, _>>()
        })
        .unwrap_or_default();
    let mut closed_projected_issues = checkpoint
        .as_ref()
        .map(|checkpoint| checkpoint.closed_projected_issues.clone())
        .unwrap_or_default();

    while !graph.is_complete() {
        let ready = graph
            .ready()?
            .into_iter()
            .take(flow.max_parallel_tasks)
            .collect::<Vec<_>>();
        if ready.is_empty() {
            if let Some(tracking) = &tracking {
                fail_unfinished_tracked_jobs(
                    tracking,
                    &tracked_jobs,
                    &mut finished_jobs,
                    "plugin task graph had no runnable tasks",
                )
                .await?;
            }
            return Err("plugin task graph has no runnable tasks".into());
        }
        for key in &ready {
            graph.mark_running(key)?;
            if let Some(tracking) = &tracking {
                start_waiting_job(tracking.pool, tracked_jobs[key])
                    .await?
                    .ok_or("plugin child job was not waiting")?;
            }
        }
        attempt += 1;
        if attempt > 64 {
            if let Some(tracking) = &tracking {
                fail_unfinished_tracked_jobs(
                    tracking,
                    &tracked_jobs,
                    &mut finished_jobs,
                    "plugin lifecycle exceeded 64 task waves",
                )
                .await?;
            }
            return Err("plugin lifecycle exceeded 64 task waves".into());
        }
        let task_attempts = ready
            .iter()
            .enumerate()
            .map(|(offset, key)| (key.clone(), attempt * 100 + offset as u32))
            .collect::<BTreeMap<_, _>>();
        let executions = join_all(ready.iter().enumerate().map(|(offset, key)| {
            let work_item = key
                .work_item
                .as_deref()
                .and_then(|id| work_items.iter().find(|item| item.id == id));
            execute_task(
                selection,
                manifest,
                &key.task,
                &flow.tasks[&key.task],
                work_item,
                attempt * 100 + offset as u32,
                repo_path,
                workspace_path,
                issue_input,
                &previous,
                parameters,
                plugin_root,
            )
        }))
        .await;

        let mut successful_executions = Vec::new();
        let mut execution_errors = Vec::new();
        for (offset, (key, execution)) in ready.into_iter().zip(executions).enumerate() {
            match execution {
                Ok(execution) => {
                    if let Some(tracking) = &tracking {
                        complete_job(
                            tracking.pool,
                            tracked_jobs[&key],
                            &serde_json::to_value(&execution)?,
                        )
                        .await?;
                        finished_jobs.insert(key.clone());
                    }
                    successful_executions.push((key, execution));
                }
                Err(error) => {
                    let reason = error.to_string();
                    if let Some(tracking) = &tracking {
                        let result = failed_task_result(reason.clone());
                        fail_job(tracking.pool, tracked_jobs[&key], &result).await?;
                        finished_jobs.insert(key.clone());
                    }
                    execution_errors.push((key, attempt * 100 + offset as u32, reason));
                }
            }
        }
        if successful_executions
            .iter()
            .any(|(_, execution)| execution.result.outcome == Outcome::Implemented)
            && let Some(publication) = tracking
                .as_ref()
                .and_then(|tracking| tracking.publication.as_ref())
            && let Err(error) = publish_checkpoint(
                publication,
                repo_path,
                &format!(
                    "chore({}): checkpoint task wave for issue #{}",
                    active_facade().command,
                    publication.issue_number
                ),
            )
            .await
        {
            tracing::warn!(%error, "plugin task-wave checkpoint publication failed");
        }
        for (key, execution) in &successful_executions {
            let work_item = key
                .work_item
                .as_deref()
                .and_then(|id| work_items.iter().find(|item| item.id == id));
            if execution.result.outcome == Outcome::Implemented
                && !declared_diagnostics_present(
                    &flow.tasks[&key.task],
                    workspace_path,
                    &key.task,
                    work_item,
                    task_attempts[key],
                    parameters,
                )
            {
                continue;
            }
            publish_task_attempt(
                tracking.as_ref(),
                selection,
                &key.task,
                &flow.tasks[&key.task],
                work_item,
                task_attempts[key],
                workspace_path,
                repo_path,
                parameters,
                &manifest.parameters,
                tracking.as_ref().map(|_| tracked_jobs[key]),
                Some(execution.result.outcome),
                &execution.result.summary,
                key.work_item
                    .as_deref()
                    .and_then(|work_item| projected_issues.get(work_item).copied()),
            )
            .await;
        }
        for (key, task_attempt, reason) in &execution_errors {
            let work_item = key
                .work_item
                .as_deref()
                .and_then(|id| work_items.iter().find(|item| item.id == id));
            publish_task_attempt(
                tracking.as_ref(),
                selection,
                &key.task,
                &flow.tasks[&key.task],
                work_item,
                *task_attempt,
                workspace_path,
                repo_path,
                parameters,
                &manifest.parameters,
                tracking.as_ref().map(|_| tracked_jobs[key]),
                None,
                reason,
                key.work_item
                    .as_deref()
                    .and_then(|work_item| projected_issues.get(work_item).copied()),
            )
            .await;
        }
        if let Some((_, _, error)) = execution_errors.into_iter().next() {
            if let Some(tracking) = &tracking {
                fail_unfinished_tracked_jobs(
                    tracking,
                    &tracked_jobs,
                    &mut finished_jobs,
                    &format!("plugin lifecycle stopped after a parallel task failed: {error}"),
                )
                .await?;
            }
            return Err(error.into());
        }

        let mut feedback = Vec::new();
        let mut required_approvals = successful_executions
            .iter()
            .filter(|(key, execution)| {
                execution.result.outcome == Outcome::Implemented
                    && flow.tasks[&key.task].approval == PluginApprovalMode::Required
            })
            .map(|(key, _)| PendingApproval {
                key: key.clone(),
                trigger: ApprovalTrigger::Required,
            })
            .collect::<Vec<_>>();
        for (key, execution) in &successful_executions {
            accumulated_tests.extend(execution.result.tests.clone());
            aggregate_risk = max_risk(aggregate_risk, execution.result.risk);
            aggregate_confidence =
                min_confidence(aggregate_confidence, execution.result.confidence);
            previous.push(task_summary(
                &key.task,
                key.work_item.as_deref(),
                attempt,
                &execution.result,
            ));
            last_result = execution.result.clone();
        }
        // Record successful siblings before processing feedback from this
        // parallel wave. A pause in one task must not discard independent work
        // that completed at the same time.
        for (key, execution) in &successful_executions {
            if execution.result.outcome == Outcome::Implemented
                && flow.tasks[&key.task].approval != PluginApprovalMode::Required
            {
                graph.mark_completed(key)?;
            }
        }
        for (key, execution) in successful_executions {
            match execution.result.outcome {
                Outcome::Implemented => {}
                Outcome::NeedsChanges => {
                    let Some(handoff) = execution.handoff else {
                        let reason = format!(
                            "task `{}` returned needs_changes without a handoff",
                            key.task
                        );
                        if let Some(tracking) = &tracking {
                            fail_unfinished_tracked_jobs(
                                tracking,
                                &tracked_jobs,
                                &mut finished_jobs,
                                &reason,
                            )
                            .await?;
                        }
                        return Err(reason.into());
                    };
                    let task = &flow.tasks[&key.task];
                    if !task.allowed_handoffs.contains(&handoff.target) {
                        let reason = format!(
                            "task `{}` cannot hand off to `{}`",
                            key.task, handoff.target
                        );
                        if let Some(tracking) = &tracking {
                            fail_unfinished_tracked_jobs(
                                tracking,
                                &tracked_jobs,
                                &mut finished_jobs,
                                &reason,
                            )
                            .await?;
                        }
                        return Err(reason.into());
                    }
                    let edge = (
                        key.work_item.clone(),
                        key.task.clone(),
                        handoff.target.clone(),
                    );
                    let count = handoffs.entry(edge.clone()).or_default();
                    *count += 1;
                    if *count > max_handoffs {
                        let resume_target = normalize_handoff_target(flow, &key, &handoff.target)?;
                        graph.restart_from(&resume_target)?;
                        if let Some(tracking) = &tracking {
                            let pending_keys = graph
                                .keys()
                                .filter(|key| !graph.is_completed(key))
                                .cloned()
                                .collect::<Vec<_>>();
                            for pending_key in pending_keys {
                                if !required_approvals
                                    .iter()
                                    .any(|approval| approval.key == pending_key)
                                    && (finished_jobs.remove(&pending_key)
                                        || !tracked_jobs.contains_key(&pending_key))
                                {
                                    let job = create_tracked_job(
                                        tracking,
                                        manifest,
                                        &selection.flow,
                                        flow,
                                        &pending_key,
                                        &work_items,
                                        issue_input,
                                    )
                                    .await?;
                                    tracked_jobs.insert(pending_key, job.id);
                                }
                            }
                        }
                        // A human decision authorizes a fresh bounded feedback
                        // cycle on the edge that caused the pause.
                        handoffs.insert(edge, 0);
                        let lead = format!(
                            "handoff from `{}` to `{}` exceeded policy limit {max_handoffs}: {}\n\nPreserved checkpoint:\n- {} completed task(s) remain valid.\n- Existing block issues and workspace changes will be reused.",
                            key.task,
                            handoff.target,
                            handoff.reason,
                            graph.completed_keys().count()
                        );
                        required_approvals.push(PendingApproval {
                            key: resume_target.clone(),
                            trigger: ApprovalTrigger::AgentRequested,
                        });
                        let result =
                            pending_approval_result(&required_approvals, &projected_issues, &lead);
                        write_lifecycle_checkpoint(
                            &checkpoint_path,
                            &LifecycleCheckpoint {
                                version: CHECKPOINT_VERSION,
                                attempt,
                                accumulated_tests: accumulated_tests.clone(),
                                previous: previous.clone(),
                                aggregate_risk,
                                aggregate_confidence,
                                last_result: result.clone(),
                                completed_keys: graph.completed_keys().cloned().collect(),
                                tracked_jobs: tracked_jobs
                                    .iter()
                                    .map(|(key, job_id)| TrackedJobCheckpoint {
                                        key: key.clone(),
                                        job_id: *job_id,
                                    })
                                    .collect(),
                                handoffs: handoffs
                                    .iter()
                                    .map(|((work_item, from, to), count)| HandoffCheckpoint {
                                        work_item: work_item.clone(),
                                        from: from.clone(),
                                        to: to.clone(),
                                        count: *count,
                                    })
                                    .collect(),
                                projected_issues: projected_issues.clone(),
                                closed_projected_issues: closed_projected_issues.clone(),
                                resume_target: resume_target.clone(),
                                pending_approvals: required_approvals,
                                start_approved: true,
                                revision_targets: Vec::new(),
                                active_work_items: work_items
                                    .iter()
                                    .map(|item| item.id.clone())
                                    .collect(),
                            },
                        )?;
                        return Ok(finish_result(result, accumulated_tests, &previous));
                    }
                    feedback.push(normalize_handoff_target(flow, &key, &handoff.target)?);
                }
                Outcome::NeedsHuman => {
                    let resume_target = key.clone();
                    graph.restart_from(&resume_target)?;
                    if let Some(tracking) = &tracking {
                        let pending_keys = graph
                            .keys()
                            .filter(|key| !graph.is_completed(key))
                            .cloned()
                            .collect::<Vec<_>>();
                        for pending_key in pending_keys {
                            if !required_approvals
                                .iter()
                                .any(|approval| approval.key == pending_key)
                                && (finished_jobs.remove(&pending_key)
                                    || !tracked_jobs.contains_key(&pending_key))
                            {
                                let job = create_tracked_job(
                                    tracking,
                                    manifest,
                                    &selection.flow,
                                    flow,
                                    &pending_key,
                                    &work_items,
                                    issue_input,
                                )
                                .await?;
                                tracked_jobs.insert(pending_key, job.id);
                            }
                        }
                    }
                    let original_reason = execution
                        .result
                        .human_review_reason
                        .as_deref()
                        .unwrap_or("task requested human judgment");
                    let lead = format!(
                        "{original_reason}\n\nPreserved checkpoint:\n- {} completed task(s) remain valid.\n- Existing block issues and workspace changes will be reused.",
                        graph.completed_keys().count()
                    );
                    required_approvals.push(PendingApproval {
                        key: resume_target.clone(),
                        trigger: ApprovalTrigger::AgentRequested,
                    });
                    let result =
                        pending_approval_result(&required_approvals, &projected_issues, &lead);
                    write_lifecycle_checkpoint(
                        &checkpoint_path,
                        &LifecycleCheckpoint {
                            version: CHECKPOINT_VERSION,
                            attempt,
                            accumulated_tests: accumulated_tests.clone(),
                            previous: previous.clone(),
                            aggregate_risk,
                            aggregate_confidence,
                            last_result: result.clone(),
                            completed_keys: graph.completed_keys().cloned().collect(),
                            tracked_jobs: tracked_jobs
                                .iter()
                                .map(|(key, job_id)| TrackedJobCheckpoint {
                                    key: key.clone(),
                                    job_id: *job_id,
                                })
                                .collect(),
                            handoffs: handoffs
                                .iter()
                                .map(|((work_item, from, to), count)| HandoffCheckpoint {
                                    work_item: work_item.clone(),
                                    from: from.clone(),
                                    to: to.clone(),
                                    count: *count,
                                })
                                .collect(),
                            projected_issues: projected_issues.clone(),
                            closed_projected_issues: closed_projected_issues.clone(),
                            resume_target: resume_target.clone(),
                            pending_approvals: required_approvals,
                            start_approved: true,
                            revision_targets: Vec::new(),
                            active_work_items: work_items
                                .iter()
                                .map(|item| item.id.clone())
                                .collect(),
                        },
                    )?;
                    return Ok(finish_result(result, accumulated_tests, &previous));
                }
                _ => {
                    if let Some(tracking) = &tracking {
                        fail_unfinished_tracked_jobs(
                            tracking,
                            &tracked_jobs,
                            &mut finished_jobs,
                            &format!(
                                "plugin lifecycle stopped after task `{}` returned {:?}",
                                key.task, execution.result.outcome
                            ),
                        )
                        .await?;
                    }
                    return Ok(finish_result(
                        execution.result,
                        accumulated_tests,
                        &previous,
                    ));
                }
            }
        }
        for target in feedback {
            let invalidated = graph.restart_from(&target)?;
            required_approvals.retain(|approval| !invalidated.contains(&approval.key));
            if let Some(tracking) = &tracking {
                for invalidated_key in invalidated {
                    if finished_jobs.remove(&invalidated_key)
                        || !tracked_jobs.contains_key(&invalidated_key)
                    {
                        let job = create_tracked_job(
                            tracking,
                            manifest,
                            &selection.flow,
                            flow,
                            &invalidated_key,
                            &work_items,
                            issue_input,
                        )
                        .await?;
                        tracked_jobs.insert(invalidated_key, job.id);
                    }
                }
            }
        }
        if !required_approvals.is_empty() {
            let result = pending_approval_result(
                &required_approvals,
                &projected_issues,
                "The configured tasks completed successfully and require approval before their dependents can run.",
            );
            write_lifecycle_checkpoint(
                &checkpoint_path,
                &LifecycleCheckpoint {
                    version: CHECKPOINT_VERSION,
                    attempt,
                    accumulated_tests: accumulated_tests.clone(),
                    previous: previous.clone(),
                    aggregate_risk,
                    aggregate_confidence,
                    last_result: result.clone(),
                    completed_keys: graph.completed_keys().cloned().collect(),
                    tracked_jobs: tracked_jobs
                        .iter()
                        .map(|(key, job_id)| TrackedJobCheckpoint {
                            key: key.clone(),
                            job_id: *job_id,
                        })
                        .collect(),
                    handoffs: handoffs
                        .iter()
                        .map(|((work_item, from, to), count)| HandoffCheckpoint {
                            work_item: work_item.clone(),
                            from: from.clone(),
                            to: to.clone(),
                            count: *count,
                        })
                        .collect(),
                    projected_issues: projected_issues.clone(),
                    closed_projected_issues: closed_projected_issues.clone(),
                    resume_target: required_approvals[0].key.clone(),
                    pending_approvals: required_approvals,
                    start_approved: true,
                    revision_targets: Vec::new(),
                    active_work_items: work_items.iter().map(|item| item.id.clone()).collect(),
                },
            )?;
            return Ok(finish_result(result, accumulated_tests, &previous));
        }
        if let Some(github) = tracking.as_ref().and_then(|tracking| tracking.github)
            && let (Some(owner), Some(repo), _) = github_coordinates
        {
            for item in &work_items {
                if graph.work_item_is_complete(&item.id)
                    && closed_projected_issues.insert(item.id.clone())
                    && let Some(issue_number) = projected_issues.get(&item.id)
                    && let Err(error) = github.close_issue(owner, repo, *issue_number).await
                {
                    tracing::warn!(
                        %error,
                        work_item = item.id,
                        "failed to close projected github work-item issue"
                    );
                }
            }
        }
    }

    last_result.outcome = Outcome::Implemented;
    last_result.risk = aggregate_risk;
    last_result.confidence = aggregate_confidence;
    last_result.summary = format!(
        "Completed {} block work item(s) across {} task execution(s).",
        work_items.len(),
        previous.len()
    );
    if checkpoint_path.exists() {
        fs::remove_file(&checkpoint_path)?;
    }
    Ok(finish_result(last_result, accumulated_tests, &previous))
}

fn declared_diagnostics_present(
    task: &PluginTask,
    workspace_path: &Path,
    task_name: &str,
    work_item: Option<&PluginWorkItem>,
    attempt: u32,
    parameters: &BTreeMap<String, Value>,
) -> bool {
    let Ok(diagnostics) = expand_artifacts(&task.diagnostics, parameters, work_item) else {
        return false;
    };
    let repo = task_attempt_root(
        workspace_path,
        task_name,
        work_item.map(|item| item.id.as_str()),
        attempt,
    )
    .join("repo");
    diagnostics_present_at(&repo, &diagnostics)
}

fn diagnostics_present_at(repo: &Path, diagnostics: &[PluginArtifact]) -> bool {
    diagnostics.iter().any(|diagnostic| {
        let path = repo.join(&diagnostic.path);
        match diagnostic.kind {
            PluginArtifactType::File => path.metadata().is_ok_and(|metadata| metadata.len() > 0),
            PluginArtifactType::Directory => path
                .read_dir()
                .is_ok_and(|mut entries| entries.next().is_some()),
        }
    })
}

fn write_lifecycle_checkpoint(
    path: &Path,
    checkpoint: &LifecycleCheckpoint,
) -> Result<(), Box<dyn std::error::Error>> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(path, serde_json::to_vec_pretty(checkpoint)?)?;
    Ok(())
}

fn approval_target(key: &TaskKey) -> String {
    key.work_item
        .as_ref()
        .map(|work_item| format!("{}/{work_item}", key.task))
        .unwrap_or_else(|| key.task.clone())
}

fn normalize_handoff_target(
    flow: &PluginFlow,
    source: &TaskKey,
    target: &str,
) -> Result<TaskKey, Box<dyn std::error::Error>> {
    let task = flow
        .tasks
        .get(target)
        .ok_or_else(|| format!("handoff targets unknown task `{target}`"))?;
    let work_item = match task.scope {
        PluginTaskScope::Workflow => None,
        PluginTaskScope::WorkItem => Some(
            source
                .work_item
                .clone()
                .ok_or_else(|| format!("workflow task `{}` cannot hand off to work-item task `{target}` without a work item", source.task))?,
        ),
    };
    Ok(TaskKey {
        work_item,
        task: target.to_string(),
    })
}

fn select_pending_approvals(
    pending: &[PendingApproval],
    decision: &HumanDecision,
) -> Result<Vec<PendingApproval>, Box<dyn std::error::Error>> {
    let target = match decision {
        HumanDecision::Approve { target } | HumanDecision::Revise { target, .. } => {
            target.as_deref()
        }
    };
    if target == Some("all") {
        if matches!(decision, HumanDecision::Revise { .. }) {
            return Err("revision feedback must target one task".into());
        }
        return Ok(pending.to_vec());
    }
    if let Some(target) = target {
        return pending
            .iter()
            .find(|approval| approval_target(&approval.key) == target)
            .cloned()
            .map(|approval| vec![approval])
            .ok_or_else(|| format!("no pending approval matches `{target}`").into());
    }
    if pending.len() == 1 {
        return Ok(pending.to_vec());
    }
    Err("an approval target is required when multiple tasks are pending".into())
}

fn pending_approval_result(
    pending: &[PendingApproval],
    projected_issues: &BTreeMap<String, i64>,
    lead: &str,
) -> RunResult {
    let targets = pending
        .iter()
        .map(|approval| format!("- `{}`", approval_target(&approval.key)))
        .collect::<Vec<_>>()
        .join("\n");
    let review_issues = if projected_issues.is_empty() {
        String::new()
    } else {
        format!(
            "\n\nReview the projected work-item issues:\n{}",
            projected_issues
                .iter()
                .map(|(item, number)| format!("- `{item}`: #{number}"))
                .collect::<Vec<_>>()
                .join("\n")
        )
    };
    let single = (pending.len() == 1).then(|| approval_target(&pending[0].key));
    let commands = match single {
        Some(target) => format!(
            "`{0} approve {target}`\n\nOr request changes with `{0} revise {target}` followed by feedback on subsequent lines.",
            active_facade().issue_command()
        ),
        None => format!(
            "Approve everything with `{0} approve all`, approve one target with `{0} approve <task>`, or revise one target with `{0} revise <task>` followed by feedback on subsequent lines.",
            active_facade().issue_command()
        ),
    };
    RunResult {
        outcome: Outcome::NeedsHuman,
        summary: format!("Awaiting approval for {} task(s).", pending.len()),
        confidence: Confidence::High,
        risk: Risk::Unknown,
        questions: Vec::new(),
        tests: Vec::new(),
        changed_files: Vec::new(),
        human_review_reason: Some(format!(
            "{lead}\n\nPending approvals:\n{targets}{review_issues}\n\nWhat to do:\n{commands}"
        )),
        blocked_reason: None,
    }
}

fn failed_task_result(reason: impl Into<String>) -> Value {
    json!({
        "outcome": "failed",
        "summary": "Plugin task execution failed.",
        "confidence": "low",
        "risk": "unknown",
        "questions": [],
        "tests": [],
        "changed_files": [],
        "human_review_reason": null,
        "blocked_reason": reason.into(),
    })
}

fn unfinished_tracked_keys(
    tracked_jobs: &BTreeMap<TaskKey, uuid::Uuid>,
    finished_jobs: &BTreeSet<TaskKey>,
) -> Vec<TaskKey> {
    tracked_jobs
        .keys()
        .filter(|key| !finished_jobs.contains(*key))
        .cloned()
        .collect()
}

async fn fail_unfinished_tracked_jobs(
    tracking: &LifecycleTracking<'_>,
    tracked_jobs: &BTreeMap<TaskKey, uuid::Uuid>,
    finished_jobs: &mut BTreeSet<TaskKey>,
    reason: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    for key in unfinished_tracked_keys(tracked_jobs, finished_jobs) {
        fail_job(
            tracking.pool,
            tracked_jobs[&key],
            &failed_task_result(reason),
        )
        .await?;
        finished_jobs.insert(key);
    }
    Ok(())
}

fn max_risk(left: Risk, right: Risk) -> Risk {
    fn rank(risk: Risk) -> u8 {
        match risk {
            Risk::Low => 0,
            Risk::Medium => 1,
            Risk::Unknown => 2,
            Risk::High => 3,
        }
    }
    if rank(left) >= rank(right) {
        left
    } else {
        right
    }
}

fn min_confidence(left: Confidence, right: Confidence) -> Confidence {
    fn rank(confidence: Confidence) -> u8 {
        match confidence {
            Confidence::Low => 0,
            Confidence::Medium => 1,
            Confidence::High => 2,
        }
    }
    if rank(left) <= rank(right) {
        left
    } else {
        right
    }
}

async fn create_tracked_job(
    tracking: &LifecycleTracking<'_>,
    manifest: &PluginManifest,
    flow_name: &str,
    flow: &PluginFlow,
    key: &TaskKey,
    work_items: &[PluginWorkItem],
    issue_input: &Value,
) -> Result<JobRecord, Box<dyn std::error::Error>> {
    let mut input = issue_input.clone();
    let work_item = key
        .work_item
        .as_deref()
        .and_then(|id| work_items.iter().find(|item| item.id == id));
    if let Value::Object(map) = &mut input {
        map.insert(
            "plugin_execution".into(),
            json!({
                "coordinator_run_id": tracking.coordinator.id,
                "plugin_id": manifest.id,
                "flow": flow_name,
                "task": key.task,
                "work_item": work_item,
                "dependencies": flow.tasks[&key.task].dependencies,
            }),
        );
    }
    Ok(create_waiting_job(
        tracking.pool,
        tracking.coordinator.workflow_item_id,
        &flow.tasks[&key.task].role,
        &input,
    )
    .await?)
}

#[allow(clippy::too_many_arguments)]
async fn execute_task(
    selection: &PluginFlowSelection,
    manifest: &PluginManifest,
    task_name: &str,
    task: &PluginTask,
    work_item: Option<&PluginWorkItem>,
    attempt: u32,
    repo_path: &Path,
    workspace_path: &Path,
    issue_input: &Value,
    previous: &[Value],
    parameters: &BTreeMap<String, Value>,
    plugin_root: &Path,
) -> Result<PluginTaskResult, Box<dyn std::error::Error>> {
    let role = &manifest.roles[&task.role];
    let task_root = task_attempt_root(
        workspace_path,
        task_name,
        work_item.map(|item| item.id.as_str()),
        attempt,
    );
    let task_repo = task_root.join("repo");
    fs::create_dir_all(&task_repo)?;
    let declared_read = expand_templates(&task.read, parameters, work_item)?;
    let declared_write = expand_templates(&task.write, parameters, work_item)?;
    let (read_roots, write_roots) = resolve_access(
        selection,
        task_name,
        &declared_read,
        &declared_write,
        parameters,
        &manifest.parameters,
    )?;
    let diagnostics = expand_artifacts(&task.diagnostics, parameters, work_item)?;
    if let Some(diagnostic) = diagnostics.iter().find(|diagnostic| {
        !covered(&diagnostic.path, &read_roots) && !covered(&diagnostic.path, &write_roots)
    }) {
        return Err(format!(
            "task `{task_name}` diagnostic `{}` is outside its declared roots",
            diagnostic.path
        )
        .into());
    }
    for root in read_roots.iter().chain(&write_roots) {
        copy_root(repo_path, &task_repo, root)?;
    }
    let donkeyspace = task_root.join(".donkeyspace");
    fs::create_dir_all(&donkeyspace)?;
    let result_path = donkeyspace.join("run-result.json");
    let resources = materialize_resources(
        manifest,
        &task.role,
        task,
        plugin_root,
        repo_path,
        &task_root,
        parameters,
    )?;
    let selected_mcp = role
        .mcp_servers
        .iter()
        .filter_map(|name| manifest.mcp_servers.get(name).map(|server| (name, server)))
        .collect::<BTreeMap<_, _>>();
    fs::write(
        donkeyspace.join("run-input.json"),
        serde_json::to_vec_pretty(&json!({
            "run_id": issue_input.pointer("/run_id"),
            "role": task.role,
            "plugin": {"id": manifest.id, "flow": selection.flow, "task": task_name, "attempt": attempt},
            "work_item": work_item,
            "issue": issue_input.pointer("/issue").unwrap_or(issue_input),
            "repository": issue_input.pointer("/repository"),
            "workspace": {"repo_path": "repo", "result_path": ".donkeyspace/run-result.json", "read": read_roots, "write": write_roots},
            "parameters": parameters,
            "resources": resources,
            "previous_tasks": previous.iter().rev().take(64).rev().collect::<Vec<_>>(),
            "mcp_servers": selected_mcp,
        }))?,
    )?;
    let image = role
        .image
        .as_deref()
        .unwrap_or(&manifest.runtime.default_image);
    let output = run_container(
        image,
        &role.command,
        &task_root,
        &selection.environment,
        &role.environment,
    )
    .await?;
    write_agent_log(&donkeyspace.join("agent.stdout.log"), &output.stdout)?;
    write_agent_log(&donkeyspace.join("agent.stderr.log"), &output.stderr)?;
    if !output.status.success() {
        return Err(format!(
            "plugin task `{task_name}` exited {:?}: {}",
            output.status.code(),
            String::from_utf8_lossy(&output.stderr).trim()
        )
        .into());
    }
    let mut task_result: PluginTaskResult =
        serde_json::from_str(&fs::read_to_string(result_path)?)?;
    validate_resources_used(&task_result.resources_used, &resources)?;
    validate_changed_files(&task_result.result.changed_files, &write_roots)?;
    if is_publishable(task_result.result.outcome) {
        verify_resources(&task_root, &resources)?;
        let artifacts = expand_artifacts(&task.artifacts, parameters, work_item)?;
        validate_artifacts(&task_repo, &artifacts, &write_roots)?;
        let validator_results = run_validators(
            &task.validators,
            image,
            &task_root,
            &selection.environment,
            &role.environment,
        )
        .await?;
        apply_validator_results(&mut task_result.result, validator_results);
    }
    task_result.result.validate_for_orchestration()?;
    if is_publishable(task_result.result.outcome) {
        for root in &write_roots {
            replace_root(&task_repo, repo_path, root)?;
        }
    }
    Ok(task_result)
}

#[allow(clippy::too_many_arguments)]
async fn publish_task_attempt(
    tracking: Option<&LifecycleTracking<'_>>,
    selection: &PluginFlowSelection,
    task_name: &str,
    task: &PluginTask,
    work_item: Option<&PluginWorkItem>,
    attempt: u32,
    workspace_path: &Path,
    aggregate_repo: &Path,
    parameters: &BTreeMap<String, Value>,
    parameter_definitions: &BTreeMap<String, PluginParameter>,
    job_id: Option<Uuid>,
    outcome: Option<Outcome>,
    reason: &str,
    related_issue_number: Option<i64>,
) {
    let Some(publication) = tracking.and_then(|tracking| tracking.publication.as_ref()) else {
        return;
    };
    let result = async {
        let declared_read = expand_templates(&task.read, parameters, work_item)?;
        let declared_write = expand_templates(&task.write, parameters, work_item)?;
        let (_, write_roots) = resolve_access(
            selection,
            task_name,
            &declared_read,
            &declared_write,
            parameters,
            parameter_definitions,
        )?;
        let diagnostics = expand_artifacts(&task.diagnostics, parameters, work_item)?;
        let redactions = selection
            .environment
            .values()
            .filter_map(|source| {
                if Path::new(source).is_absolute() {
                    fs::read_to_string(source).ok()
                } else {
                    env::var(source).ok()
                }
            })
            .map(|value| value.trim_end().to_string())
            .filter(|value| !value.is_empty())
            .collect::<Vec<_>>();
        let task_root = task_attempt_root(
            workspace_path,
            task_name,
            work_item.map(|item| item.id.as_str()),
            attempt,
        );
        publish_attempt(
            publication,
            aggregate_repo,
            &AttemptPublication {
                job_id,
                task: task_name,
                work_item: work_item.map(|item| item.id.as_str()),
                attempt,
                outcome,
                task_root: &task_root,
                write_roots: &write_roots,
                diagnostics: &diagnostics,
                reason,
                related_issue_number,
                redactions: &redactions,
            },
        )
        .await?;
        Ok::<_, Box<dyn std::error::Error>>(())
    }
    .await;
    if let Err(error) = result {
        tracing::warn!(%error, task = task_name, ?outcome, "forensic attempt publication failed");
    }
}

#[allow(clippy::too_many_arguments)]
async fn publish_serial_stage_attempt(
    tracking: Option<&LifecycleTracking<'_>>,
    selection: &PluginFlowSelection,
    stage_name: &str,
    attempt: u32,
    stage_root: &Path,
    aggregate_repo: &Path,
    write_roots: &[String],
    diagnostics: &[PluginArtifact],
    outcome: Option<Outcome>,
    reason: &str,
) {
    let Some(publication) = tracking.and_then(|tracking| tracking.publication.as_ref()) else {
        return;
    };
    let redactions = configured_environment_redactions(selection);
    if let Err(error) = publish_attempt(
        publication,
        aggregate_repo,
        &AttemptPublication {
            job_id: tracking.map(|tracking| tracking.coordinator.id),
            task: stage_name,
            work_item: None,
            attempt,
            outcome,
            task_root: stage_root,
            write_roots,
            diagnostics,
            reason,
            related_issue_number: None,
            redactions: &redactions,
        },
    )
    .await
    {
        tracing::warn!(%error, stage = stage_name, ?outcome, "plugin stage forensic publication failed");
    }
}

fn configured_environment_redactions(selection: &PluginFlowSelection) -> Vec<String> {
    selection
        .environment
        .values()
        .filter_map(|source| {
            if Path::new(source).is_absolute() {
                fs::read_to_string(source).ok()
            } else {
                env::var(source).ok()
            }
        })
        .map(|value| value.trim_end().to_string())
        .filter(|value| !value.is_empty())
        .collect()
}

fn task_attempt_root(
    workspace_path: &Path,
    task_name: &str,
    work_item: Option<&str>,
    attempt: u32,
) -> PathBuf {
    let item_suffix = work_item.map(|item| format!("-{item}")).unwrap_or_default();
    workspace_path
        .join("plugin-tasks")
        .join(format!("{attempt:04}-{task_name}{item_suffix}"))
}

fn write_agent_log(path: &Path, bytes: &[u8]) -> Result<(), Box<dyn std::error::Error>> {
    const MAX_LOG_CHARS: usize = 1_000_000;
    let mut value = String::from_utf8_lossy(bytes)
        .chars()
        .take(MAX_LOG_CHARS)
        .collect::<String>();
    if bytes.len() > value.len() {
        value.push_str("\n[truncated]\n");
    }
    fs::write(path, value)?;
    Ok(())
}

fn expand_templates(
    values: &[String],
    parameters: &BTreeMap<String, Value>,
    work_item: Option<&PluginWorkItem>,
) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    values
        .iter()
        .map(|value| {
            let expanded = expand_template(value, parameters)?;
            Ok(match work_item {
                Some(item) => expanded.replace("{work_item}", &item.id),
                None => expanded,
            })
        })
        .collect()
}

fn validate_work_items(items: &[PluginWorkItem]) -> Result<(), Box<dyn std::error::Error>> {
    if items.is_empty() {
        return Err("architect produced an empty work item registry".into());
    }
    let ids = items
        .iter()
        .map(|item| item.id.as_str())
        .collect::<BTreeSet<_>>();
    if ids.len() != items.len() {
        return Err("architect produced duplicate work item ids".into());
    }
    for item in items {
        if item.id.is_empty()
            || !item.id.chars().all(|character| {
                character.is_ascii_alphanumeric() || matches!(character, '-' | '_')
            })
        {
            return Err(format!("unsafe work item id `{}`", item.id).into());
        }
        let spec = Path::new(&item.spec);
        if spec.is_absolute()
            || spec
                .components()
                .any(|component| matches!(component, Component::ParentDir))
        {
            return Err(format!("unsafe work item spec path `{}`", item.spec).into());
        }
        if let Some(dependency) = item.depends_on.iter().find(|id| !ids.contains(id.as_str())) {
            return Err(format!(
                "work item `{}` depends on unknown work item `{dependency}`",
                item.id
            )
            .into());
        }
    }
    fn visit<'a>(
        id: &'a str,
        items: &'a [PluginWorkItem],
        visiting: &mut BTreeSet<&'a str>,
        visited: &mut BTreeSet<&'a str>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if visited.contains(id) {
            return Ok(());
        }
        if !visiting.insert(id) {
            return Err(format!("work item dependency cycle contains `{id}`").into());
        }
        let item = items
            .iter()
            .find(|item| item.id == id)
            .expect("validated id");
        for dependency in &item.depends_on {
            visit(dependency, items, visiting, visited)?;
        }
        visiting.remove(id);
        visited.insert(id);
        Ok(())
    }
    let mut visiting = BTreeSet::new();
    let mut visited = BTreeSet::new();
    for item in items {
        visit(&item.id, items, &mut visiting, &mut visited)?;
    }
    Ok(())
}

fn select_lifecycle_work_items(
    catalog: &[PluginWorkItem],
    requested: Option<&[String]>,
) -> Result<Vec<PluginWorkItem>, Box<dyn std::error::Error>> {
    let requested = requested.ok_or(
        "architect result is missing `work_items`; list only the repository block ids participating in this lifecycle",
    )?;
    if requested.is_empty() {
        return Err("architect selected no work items for this lifecycle".into());
    }
    let ids = requested
        .iter()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    if ids.len() != requested.len() {
        return Err("architect selected duplicate lifecycle work item ids".into());
    }
    if let Some(id) = requested
        .iter()
        .find(|id| !catalog.iter().any(|item| &item.id == *id))
    {
        return Err(format!("architect selected unknown lifecycle work item `{id}`").into());
    }
    Ok(requested
        .iter()
        .filter_map(|id| catalog.iter().find(|item| &item.id == id).cloned())
        .collect())
}

fn task_summary(task: &str, work_item: Option<&str>, attempt: u32, result: &RunResult) -> Value {
    json!({
        "task": task,
        "work_item": work_item,
        "attempt": attempt,
        "outcome": result.outcome,
        "summary": result.summary.chars().take(2_000).collect::<String>(),
    })
}

fn finish_result(
    mut result: RunResult,
    accumulated_tests: Vec<TestResult>,
    previous: &[Value],
) -> RunResult {
    result.tests = accumulated_tests;
    result.summary = flow_summary(previous, &result.summary);
    result
}

fn flow_summary(previous: &[Value], final_summary: &str) -> String {
    let mut lines = previous
        .iter()
        .rev()
        .take(64)
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .filter_map(|entry| {
            let stage = entry.get("stage").or_else(|| entry.get("task"))?.as_str()?;
            let summary = entry.get("summary")?.as_str()?;
            Some(format!("{stage}: {summary}"))
        })
        .collect::<Vec<_>>();
    if lines
        .last()
        .is_none_or(|line| !line.ends_with(final_summary))
    {
        lines.push(final_summary.to_string());
    }
    lines.join("\n")
}

fn resolve_parameters(
    manifest: &PluginManifest,
    selection: &PluginFlowSelection,
) -> Result<BTreeMap<String, Value>, Box<dyn std::error::Error>> {
    if let Some(name) = selection
        .parameters
        .keys()
        .find(|name| !manifest.parameters.contains_key(*name))
    {
        return Err(format!("unknown plugin parameter `{name}`").into());
    }
    let mut resolved = BTreeMap::new();
    for (name, definition) in &manifest.parameters {
        let selected = selection.parameters.get(name);
        if let Some(selected) = selected {
            let valid = match definition {
                PluginParameter::Path { .. }
                | PluginParameter::Enum { .. }
                | PluginParameter::String { .. } => selected.is_string(),
                PluginParameter::Integer { .. } => selected.is_i64(),
                PluginParameter::Boolean { .. } => selected.is_boolean(),
            };
            if !valid {
                return Err(format!("invalid type for plugin parameter `{name}`").into());
            }
        }
        let value = match definition {
            PluginParameter::Path { default } => {
                let value = selected
                    .and_then(Value::as_str)
                    .map(str::to_owned)
                    .or_else(|| default.clone())
                    .ok_or_else(|| format!("missing plugin parameter `{name}`"))?;
                validate_runtime_path(&value)?;
                Value::String(value)
            }
            PluginParameter::Enum { values, default } => {
                let value = selected
                    .and_then(Value::as_str)
                    .map(str::to_owned)
                    .or_else(|| default.clone())
                    .ok_or_else(|| format!("missing plugin parameter `{name}`"))?;
                if !values.contains(&value) {
                    return Err(format!("invalid value for enum parameter `{name}`").into());
                }
                Value::String(value)
            }
            PluginParameter::String { default } => Value::String(
                selected
                    .and_then(Value::as_str)
                    .map(str::to_owned)
                    .or_else(|| default.clone())
                    .ok_or_else(|| format!("missing plugin parameter `{name}`"))?,
            ),
            PluginParameter::Integer { default } => Value::Number(
                selected
                    .and_then(Value::as_i64)
                    .or(*default)
                    .ok_or_else(|| format!("missing plugin parameter `{name}`"))?
                    .into(),
            ),
            PluginParameter::Boolean { default } => Value::Bool(
                selected
                    .and_then(Value::as_bool)
                    .or(*default)
                    .ok_or_else(|| format!("missing plugin parameter `{name}`"))?,
            ),
        };
        resolved.insert(name.clone(), value);
    }
    Ok(resolved)
}

fn expand_template(
    template: &str,
    parameters: &BTreeMap<String, Value>,
) -> Result<String, Box<dyn std::error::Error>> {
    let mut expanded = template.to_string();
    for (name, value) in parameters {
        if let Some(value) = value.as_str() {
            expanded = expanded.replace(&format!("{{{name}}}"), value);
        }
    }
    let remainder = expanded.replace("{work_item}", "item");
    if remainder.contains('{') || remainder.contains('}') {
        return Err(format!("unknown placeholder in filesystem field `{template}`").into());
    }
    validate_runtime_path(&remainder)?;
    Ok(expanded)
}

fn validate_runtime_path(value: &str) -> Result<(), Box<dyn std::error::Error>> {
    let path = Path::new(value);
    if value.trim().is_empty()
        || value.contains(['{', '}'])
        || path.is_absolute()
        || path.components().any(|component| {
            matches!(
                component,
                Component::ParentDir | Component::RootDir | Component::Prefix(_)
            )
        })
    {
        return Err(format!("unsafe plugin path `{value}`").into());
    }
    Ok(())
}

fn merged_resource_assignments(
    role: &[PluginResourceAssignment],
    task: &[PluginResourceAssignment],
) -> BTreeMap<String, bool> {
    let mut merged = BTreeMap::new();
    for assignment in role.iter().chain(task) {
        merged
            .entry(assignment.id.clone())
            .and_modify(|required| *required |= assignment.required)
            .or_insert(assignment.required);
    }
    merged
}

#[allow(clippy::too_many_arguments)]
fn materialize_resources(
    manifest: &PluginManifest,
    role_name: &str,
    task: &PluginTask,
    plugin_root: &Path,
    repo_root: &Path,
    attempt_root: &Path,
    parameters: &BTreeMap<String, Value>,
) -> Result<Vec<MaterializedResource>, Box<dyn std::error::Error>> {
    let assignments =
        merged_resource_assignments(&manifest.roles[role_name].resources, &task.resources);
    let mut result = Vec::new();
    for (id, required) in assignments {
        let definition = &manifest.resources[&id];
        let source_path = expand_template(&definition.path, parameters)?;
        let source_root = match definition.source {
            PluginResourceSource::Plugin => plugin_root,
            PluginResourceSource::Repository => repo_root,
        };
        let source = source_root.join(&source_path);
        let relative_root = format!(".donkeyspace/resources/{id}");
        let target = attempt_root.join(&relative_root);
        let metadata = match fs::symlink_metadata(&source) {
            Ok(metadata) => Some(metadata),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
            Err(error) => return Err(error.into()),
        };
        let Some(metadata) = metadata else {
            if required {
                return Err(
                    format!("required resource `{id}` is missing at `{source_path}`").into(),
                );
            }
            result.push(MaterializedResource {
                id,
                source: definition.source,
                source_path,
                root: relative_root,
                available: false,
                inventory: Vec::new(),
                digest: None,
            });
            continue;
        };
        if metadata.file_type().is_symlink() {
            return Err(format!("resource `{id}` may not be a symlink").into());
        }
        fs::create_dir_all(&target)?;
        if metadata.is_file() {
            let basename = source
                .file_name()
                .ok_or_else(|| format!("resource `{id}` has no basename"))?;
            copy_resource_entry(&source, &target.join(basename))?;
        } else if metadata.is_dir() {
            copy_resource_directory(&source, &target)?;
        } else {
            return Err(format!("resource `{id}` is not a regular file or directory").into());
        }
        let (inventory, digest) = digest_resource_tree(&target)?;
        result.push(MaterializedResource {
            id,
            source: definition.source,
            source_path,
            root: relative_root,
            available: true,
            inventory,
            digest: Some(digest),
        });
    }
    Ok(result)
}

fn copy_resource_directory(source: &Path, target: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let mut entries = fs::read_dir(source)?.collect::<Result<Vec<_>, _>>()?;
    entries.sort_by_key(|entry| entry.file_name());
    for entry in entries {
        let metadata = fs::symlink_metadata(entry.path())?;
        if metadata.file_type().is_symlink() {
            return Err(format!("resource contains symlink `{}`", entry.path().display()).into());
        }
        let destination = target.join(entry.file_name());
        if metadata.is_dir() {
            fs::create_dir_all(&destination)?;
            copy_resource_directory(&entry.path(), &destination)?;
        } else if metadata.is_file() {
            copy_resource_entry(&entry.path(), &destination)?;
        } else {
            return Err(format!(
                "resource contains special file `{}`",
                entry.path().display()
            )
            .into());
        }
    }
    Ok(())
}

fn copy_resource_entry(source: &Path, target: &Path) -> Result<(), Box<dyn std::error::Error>> {
    if let Some(parent) = target.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::copy(source, target)?;
    Ok(())
}

fn digest_resource_tree(root: &Path) -> Result<(Vec<String>, String), Box<dyn std::error::Error>> {
    let root_metadata = fs::symlink_metadata(root)?;
    if root_metadata.file_type().is_symlink() || !root_metadata.is_dir() {
        return Err("materialized resource root is not a regular directory".into());
    }
    fn visit(directory: &Path, files: &mut Vec<PathBuf>) -> Result<(), Box<dyn std::error::Error>> {
        let mut entries = fs::read_dir(directory)?.collect::<Result<Vec<_>, _>>()?;
        entries.sort_by_key(|entry| entry.file_name());
        for entry in entries {
            let metadata = fs::symlink_metadata(entry.path())?;
            if metadata.file_type().is_symlink() {
                return Err("materialized resource contains a symlink".into());
            }
            if metadata.is_dir() {
                visit(&entry.path(), files)?;
            } else if metadata.is_file() {
                files.push(entry.path());
            } else {
                return Err("materialized resource contains a special file".into());
            }
        }
        Ok(())
    }
    let mut files = Vec::new();
    visit(root, &mut files)?;
    if files.len() > MAX_RESOURCE_FILES {
        return Err(format!("resource exceeds {MAX_RESOURCE_FILES} files").into());
    }
    let mut inventory = Vec::with_capacity(files.len());
    let mut hasher = Sha256::new();
    let mut total = 0u64;
    for file in files {
        let relative = file
            .strip_prefix(root)?
            .to_string_lossy()
            .replace('\\', "/");
        let contents = fs::read(&file)?;
        total = total.saturating_add(contents.len() as u64);
        if total > MAX_RESOURCE_BYTES {
            return Err(format!("resource exceeds {MAX_RESOURCE_BYTES} bytes").into());
        }
        hasher.update((relative.len() as u64).to_be_bytes());
        hasher.update(relative.as_bytes());
        hasher.update((contents.len() as u64).to_be_bytes());
        hasher.update(&contents);
        inventory.push(relative);
    }
    Ok((inventory, format!("sha256:{:x}", hasher.finalize())))
}

fn verify_resources(
    attempt_root: &Path,
    resources: &[MaterializedResource],
) -> Result<(), Box<dyn std::error::Error>> {
    for resource in resources.iter().filter(|resource| resource.available) {
        let (inventory, digest) = digest_resource_tree(&attempt_root.join(&resource.root))?;
        if inventory != resource.inventory || Some(digest) != resource.digest {
            return Err(format!("resource `{}` was modified during execution", resource.id).into());
        }
    }
    Ok(())
}

fn validate_resources_used(
    used: &[String],
    resources: &[MaterializedResource],
) -> Result<(), Box<dyn std::error::Error>> {
    let supplied = resources
        .iter()
        .filter(|resource| resource.available)
        .map(|resource| resource.id.as_str())
        .collect::<BTreeSet<_>>();
    if let Some(id) = used.iter().find(|id| !supplied.contains(id.as_str())) {
        return Err(format!("plugin reported unsupplied resource `{id}`").into());
    }
    Ok(())
}

fn expand_artifacts(
    artifacts: &[PluginArtifact],
    parameters: &BTreeMap<String, Value>,
    work_item: Option<&PluginWorkItem>,
) -> Result<Vec<PluginArtifact>, Box<dyn std::error::Error>> {
    artifacts
        .iter()
        .map(|artifact| {
            let mut artifact = artifact.clone();
            artifact.path = expand_template(&artifact.path, parameters)?;
            if let Some(item) = work_item {
                artifact.path = artifact.path.replace("{work_item}", &item.id);
            }
            Ok(artifact)
        })
        .collect()
}

fn validate_artifacts(
    repo: &Path,
    artifacts: &[PluginArtifact],
    write_roots: &[String],
) -> Result<(), Box<dyn std::error::Error>> {
    for artifact in artifacts {
        if !covered(&artifact.path, write_roots) {
            return Err(format!("artifact `{}` is outside task write roots", artifact.path).into());
        }
        let path = repo.join(&artifact.path);
        let metadata = match fs::symlink_metadata(&path) {
            Ok(metadata) => Some(metadata),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
            Err(error) => return Err(error.into()),
        };
        let Some(metadata) = metadata else {
            if artifact.required {
                return Err(format!("required artifact `{}` is missing", artifact.path).into());
            }
            continue;
        };
        if metadata.file_type().is_symlink()
            || match artifact.kind {
                PluginArtifactType::File => !metadata.is_file(),
                PluginArtifactType::Directory => !metadata.is_dir(),
            }
        {
            return Err(format!("artifact `{}` has the wrong type", artifact.path).into());
        }
    }
    Ok(())
}

fn is_publishable(outcome: Outcome) -> bool {
    outcome == Outcome::Implemented
}

fn apply_validator_results(result: &mut RunResult, validator_results: Vec<TestResult>) {
    let validators_passed = validator_results
        .iter()
        .all(|result| result.status == TestStatus::Passed);
    result.tests.extend(validator_results);
    if !validators_passed {
        result.outcome = Outcome::Failed;
        result.blocked_reason = Some("plugin validator failed".into());
    }
}

async fn run_validators(
    validators: &[PluginValidator],
    image: &str,
    task_root: &Path,
    configured: &BTreeMap<String, String>,
    allowed: &[String],
) -> Result<Vec<TestResult>, Box<dyn std::error::Error>> {
    let mut results = Vec::new();
    for validator in validators {
        let output =
            run_container(image, &validator.command, task_root, configured, allowed).await?;
        results.push(TestResult {
            name: validator.name.clone(),
            command: validator.command.clone(),
            status: if output.status.success() {
                TestStatus::Passed
            } else {
                TestStatus::Failed
            },
            exit_code: output.status.code(),
            summary: Some(if output.status.success() {
                String::from_utf8_lossy(&output.stdout)
                    .trim()
                    .chars()
                    .take(2_000)
                    .collect()
            } else {
                String::from_utf8_lossy(&output.stderr)
                    .trim()
                    .chars()
                    .take(2_000)
                    .collect()
            }),
        });
    }
    Ok(results)
}

async fn run_container(
    image: &str,
    command: &[String],
    stage_root: &Path,
    configured: &BTreeMap<String, String>,
    allowed: &[String],
) -> Result<std::process::Output, Box<dyn std::error::Error>> {
    let mut docker = Command::new("docker");
    docker.arg("run").arg("--rm").arg("--network").arg("bridge");
    if let Ok(volume) = env::var("DONKEYSPACE_WORKSPACE_VOLUME") {
        let workspace_root =
            env::var("DONKEYSPACE_WORKSPACE_ROOT").unwrap_or_else(|_| "/workspaces".into());
        docker.args([
            "--mount",
            &format!("type=volume,src={volume},dst={workspace_root}"),
        ]);
        docker.args(["--workdir", &stage_root.display().to_string()]);
    } else {
        docker.args([
            "--mount",
            &format!("type=bind,src={},dst=/workspace", stage_root.display()),
        ]);
        docker.args(["--workdir", "/workspace"]);
    }
    if let Ok(volume) = env::var("DONKEYSPACE_CODEX_VOLUME") {
        docker.args([
            "--mount",
            &format!("type=volume,src={volume},dst=/root/.codex"),
        ]);
    }
    if let Ok(source) = env::var("DONKEYSPACE_OSS_TOOLS_PATH") {
        let source = source.trim();
        if !source.is_empty() {
            let source_path = Path::new(source);
            if !source_path.is_absolute() || source.contains(',') {
                return Err(
                    "DONKEYSPACE_OSS_TOOLS_PATH must be an absolute path without commas".into(),
                );
            }
            docker.args([
                "--mount",
                &format!("type=bind,src={source},dst=/mnt/oss-tools,readonly"),
            ]);
        }
    }
    if let Ok(source) = env::var("DONKEYSPACE_TECH_PATH") {
        let source = source.trim();
        if !source.is_empty() {
            let source_path = Path::new(source);
            if !source_path.is_absolute() || source.contains(',') {
                return Err("DONKEYSPACE_TECH_PATH must be an absolute path without commas".into());
            }
            docker.args([
                "--mount",
                &format!("type=bind,src={source},dst=/mnt/tech,readonly"),
            ]);
        }
    }
    for name in allowed {
        if let Some(source) = configured.get(name) {
            let value = if Path::new(source).is_absolute() {
                std::fs::read_to_string(source)
                    .map(|value| value.trim_end().to_string())
                    .map_err(|_| {
                        format!("required plugin environment file `{source}` is unreadable")
                    })?
            } else {
                env::var(source).map_err(|_| {
                    format!("required plugin environment source `{source}` is unset")
                })?
            };
            // Pass only the variable name on Docker's command line. The value
            // is inherited from this worker process and is never exposed in
            // process listings or command diagnostics.
            docker.arg("--env").arg(name).env(name, value);
        }
    }
    docker
        .arg(image)
        .args(command)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    Ok(docker.output().await?)
}

fn resolve_access(
    selection: &PluginFlowSelection,
    stage: &str,
    declared_read: &[String],
    declared_write: &[String],
    parameters: &BTreeMap<String, Value>,
    parameter_definitions: &BTreeMap<String, PluginParameter>,
) -> Result<(Vec<String>, Vec<String>), Box<dyn std::error::Error>> {
    let Some(overrides) = selection.task_access_overrides.get(stage) else {
        return Ok((declared_read.to_vec(), declared_write.to_vec()));
    };
    let read = overrides
        .read
        .clone()
        .map(|values| expand_policy_roots(&values, parameters, parameter_definitions))
        .transpose()?
        .unwrap_or_else(|| declared_read.to_vec());
    let write = overrides
        .write
        .clone()
        .map(|values| expand_policy_roots(&values, parameters, parameter_definitions))
        .transpose()?
        .unwrap_or_else(|| declared_write.to_vec());
    if !read.iter().all(|path| covered(path, declared_read))
        || !write.iter().all(|path| covered(path, declared_write))
    {
        return Err(
            format!("policy access override widens plugin task `{stage}` permissions").into(),
        );
    }
    Ok((read, write))
}

fn expand_policy_roots(
    values: &[String],
    parameters: &BTreeMap<String, Value>,
    definitions: &BTreeMap<String, PluginParameter>,
) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    for value in values {
        for segment in value.split('{').skip(1) {
            let name = segment
                .split_once('}')
                .map(|(name, _)| name)
                .ok_or_else(|| format!("unclosed placeholder in policy path `{value}`"))?;
            if !matches!(
                definitions.get(name),
                Some(PluginParameter::Path { .. } | PluginParameter::Enum { .. })
            ) {
                return Err(format!(
                    "parameter `{name}` cannot be used in a policy filesystem field"
                )
                .into());
            }
        }
    }
    expand_templates(values, parameters, None)
}

fn covered(path: &str, roots: &[String]) -> bool {
    roots.iter().any(|root| {
        path == root
            || path
                .strip_prefix(root)
                .is_some_and(|rest| rest.starts_with('/'))
    })
}

fn validate_changed_files(
    files: &[String],
    roots: &[String],
) -> Result<(), Box<dyn std::error::Error>> {
    if let Some(path) = files
        .iter()
        .find(|path| validate_runtime_path(path).is_err() || !covered(path, roots))
    {
        return Err(format!("plugin reported change outside task write roots: `{path}`").into());
    }
    Ok(())
}

fn copy_root(
    source_repo: &Path,
    target_repo: &Path,
    root: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let source = source_repo.join(root);
    if !source.exists() {
        return Ok(());
    }
    copy_entry(&source, &target_repo.join(root))
}

fn copy_entry(source: &Path, target: &Path) -> Result<(), Box<dyn std::error::Error>> {
    if source.is_dir() {
        fs::create_dir_all(target)?;
        for entry in fs::read_dir(source)? {
            let entry = entry?;
            copy_entry(&entry.path(), &target.join(entry.file_name()))?;
        }
    } else {
        if let Some(parent) = target.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::copy(source, target)?;
    }
    Ok(())
}

fn replace_root(
    source_repo: &Path,
    target_repo: &Path,
    root: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let source = source_repo.join(root);
    let target = target_repo.join(root);
    if target.is_dir() {
        fs::remove_dir_all(&target)?;
    } else if target.exists() {
        fs::remove_file(&target)?;
    }
    if source.exists() {
        copy_entry(&source, &target)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_manifest(input: &str) -> PluginManifest {
        let manifest: PluginManifest = serde_yaml::from_str(input).unwrap();
        manifest.validate().unwrap();
        manifest
    }

    fn test_selection(input: &str) -> PluginFlowSelection {
        serde_yaml::from_str(input).unwrap()
    }

    fn temporary_root() -> PathBuf {
        env::temp_dir().join(format!("donkeyspace-plugin-test-{}", uuid::Uuid::now_v7()))
    }

    #[test]
    fn path_coverage_is_segment_aware() {
        assert!(covered("rtl/core.sv", &["rtl".into()]));
        assert!(!covered("rtl-secret/a", &["rtl".into()]));
    }

    #[test]
    fn aggregate_result_preserves_highest_risk_and_lowest_confidence() {
        assert_eq!(max_risk(Risk::Medium, Risk::High), Risk::High);
        assert_eq!(max_risk(Risk::Low, Risk::Unknown), Risk::Unknown);
        assert_eq!(
            min_confidence(Confidence::High, Confidence::Low),
            Confidence::Low
        );
    }

    #[test]
    fn lifecycle_selection_excludes_unrelated_catalog_items() {
        let catalog = vec![
            PluginWorkItem {
                id: "existing".into(),
                spec: "docs/existing/spec.md".into(),
                depends_on: Vec::new(),
                metadata: BTreeMap::new(),
            },
            PluginWorkItem {
                id: "requested".into(),
                spec: "docs/requested/spec.md".into(),
                depends_on: vec!["existing".into()],
                metadata: BTreeMap::new(),
            },
        ];

        let selected = select_lifecycle_work_items(&catalog, Some(&["requested".into()])).unwrap();
        assert_eq!(selected.len(), 1);
        assert_eq!(selected[0].id, "requested");
        assert_eq!(selected[0].depends_on, ["existing"]);
        assert!(select_lifecycle_work_items(&catalog, None).is_err());
        assert!(select_lifecycle_work_items(&catalog, Some(&["missing".into()])).is_err());
    }

    #[test]
    fn handoff_target_uses_the_target_tasks_scope() {
        let manifest = test_manifest(
            r#"
api_version: 1
id: example
runtime: { default_image: image }
roles:
  architect: { command: [run] }
  dv: { command: [run] }
flows:
  blocks:
    start: architect
    replaces_default_lifecycle: true
    work_items_path: docs/index.json
    tasks:
      architect: { role: architect }
      dv: { role: dv, scope: work_item, dependencies: [architect] }
"#,
        );
        let flow = &manifest.flows["blocks"];
        let source = TaskKey {
            work_item: Some("fifo".into()),
            task: "dv".into(),
        };

        assert_eq!(
            normalize_handoff_target(flow, &source, "architect").unwrap(),
            TaskKey {
                work_item: None,
                task: "architect".into(),
            }
        );
        assert!(normalize_handoff_target(flow, &source, "unknown").is_err());
    }

    #[test]
    fn successful_diagnostics_require_nonempty_declared_output() {
        let root = temporary_root();
        fs::create_dir_all(root.join("logs")).unwrap();
        let diagnostics = vec![PluginArtifact {
            path: "logs".into(),
            kind: PluginArtifactType::Directory,
            required: false,
        }];

        assert!(!diagnostics_present_at(&root, &diagnostics));
        fs::write(root.join("logs/synthesis.log"), "success").unwrap();
        assert!(diagnostics_present_at(&root, &diagnostics));
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn unfinished_tracked_keys_excludes_every_terminal_sibling() {
        let first = TaskKey {
            work_item: Some("first".into()),
            task: "rtl".into(),
        };
        let second = TaskKey {
            work_item: Some("second".into()),
            task: "dv_prepare".into(),
        };
        let third = TaskKey {
            work_item: Some("third".into()),
            task: "synthesis".into(),
        };
        let tracked = BTreeMap::from([
            (first.clone(), uuid::Uuid::now_v7()),
            (second.clone(), uuid::Uuid::now_v7()),
            (third.clone(), uuid::Uuid::now_v7()),
        ]);
        let finished = BTreeSet::from([first, third]);

        assert_eq!(unfinished_tracked_keys(&tracked, &finished), vec![second]);
    }

    #[test]
    fn filtered_view_does_not_copy_hidden_dv_files() {
        let root = temporary_root();
        let source = root.join("source");
        let target = root.join("target");
        fs::create_dir_all(source.join("rtl")).unwrap();
        fs::create_dir_all(source.join("dv")).unwrap();
        fs::write(source.join("rtl/design.sv"), "module design; endmodule").unwrap();
        fs::write(
            source.join("dv/hidden_tb.sv"),
            "module hidden_tb; endmodule",
        )
        .unwrap();

        copy_root(&source, &target, "rtl").unwrap();

        assert!(target.join("rtl/design.sv").exists());
        assert!(!target.join("dv/hidden_tb.sv").exists());
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn resolves_typed_parameters_and_rejects_invalid_values() {
        let manifest = test_manifest(
            r#"
api_version: 1
id: example
runtime: { default_image: image }
parameters:
  root: { type: path, default: src }
  extension: { type: enum, values: [rs, txt], default: rs }
  label: { type: string, default: default }
  count: { type: integer, default: 2 }
  enabled: { type: boolean, default: true }
roles: { developer: { command: [run] } }
flows:
  default:
    start: develop
    tasks: { develop: { role: developer } }
"#,
        );
        let selection = test_selection(
            r#"
manifest_path: plugin.yml
flow: default
parameters: { root: lib, extension: txt, label: selected, count: 3, enabled: false }
"#,
        );
        let resolved = resolve_parameters(&manifest, &selection).unwrap();
        assert_eq!(resolved["root"], json!("lib"));
        assert_eq!(resolved["count"], json!(3));
        assert_eq!(resolved["enabled"], json!(false));

        let traversal = test_selection(
            r#"
manifest_path: plugin.yml
flow: default
parameters: { root: ../secret }
"#,
        );
        assert!(resolve_parameters(&manifest, &traversal).is_err());
        let wrong_type = test_selection(
            r#"
manifest_path: plugin.yml
flow: default
parameters: { count: "3" }
"#,
        );
        assert!(resolve_parameters(&manifest, &wrong_type).is_err());
        let unknown = test_selection(
            r#"
manifest_path: plugin.yml
flow: default
parameters: { surprise: true }
"#,
        );
        assert!(resolve_parameters(&manifest, &unknown).is_err());
    }

    #[test]
    fn materializes_files_directories_and_optional_missing_resources() {
        let root = temporary_root();
        let plugin = root.join("plugin");
        let repo = root.join("repo");
        let attempt = root.join("attempt");
        fs::create_dir_all(plugin.join("resources/library/nested")).unwrap();
        fs::create_dir_all(plugin.join("resources/empty")).unwrap();
        fs::create_dir_all(&repo).unwrap();
        fs::write(plugin.join("resources/standards.md"), "standards").unwrap();
        fs::write(
            plugin.join("resources/library/nested/reference.txt"),
            "reference",
        )
        .unwrap();
        let manifest = test_manifest(
            r#"
api_version: 1
id: example
runtime: { default_image: image }
resources:
  standards: { source: plugin, path: resources/standards.md }
  library: { source: plugin, path: resources/library }
  empty: { source: plugin, path: resources/empty }
  absent: { source: repository, path: missing }
roles:
  developer:
    command: [run]
    resources:
      - { id: standards, required: true }
      - { id: library, required: false }
      - { id: empty, required: true }
flows:
  default:
    start: develop
    tasks:
      develop:
        role: developer
        resources: [{ id: absent, required: false }]
"#,
        );
        let task = &manifest.flows["default"].tasks["develop"];
        let resources = materialize_resources(
            &manifest,
            "developer",
            task,
            &plugin,
            &repo,
            &attempt,
            &BTreeMap::new(),
        )
        .unwrap();

        assert!(
            attempt
                .join(".donkeyspace/resources/standards/standards.md")
                .is_file()
        );
        assert!(
            attempt
                .join(".donkeyspace/resources/library/nested/reference.txt")
                .is_file()
        );
        assert!(attempt.join(".donkeyspace/resources/empty").is_dir());
        assert_eq!(
            resources
                .iter()
                .find(|item| item.id == "empty")
                .unwrap()
                .inventory,
            Vec::<String>::new()
        );
        assert!(
            !resources
                .iter()
                .find(|item| item.id == "absent")
                .unwrap()
                .available
        );
        assert_eq!(
            resources
                .iter()
                .find(|item| item.id == "library")
                .unwrap()
                .inventory,
            vec!["nested/reference.txt"]
        );
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn snapshots_new_directory_files_and_detects_mutation() {
        let root = temporary_root();
        let plugin = root.join("plugin");
        let repo = root.join("repo");
        fs::create_dir_all(plugin.join("resources/library")).unwrap();
        fs::create_dir_all(&repo).unwrap();
        fs::write(plugin.join("resources/library/a.txt"), "a").unwrap();
        let manifest = test_manifest(
            r#"
api_version: 1
id: example
runtime: { default_image: image }
resources: { library: { source: plugin, path: resources/library } }
roles: { developer: { command: [run], resources: [{ id: library, required: true }] } }
flows:
  default:
    start: develop
    tasks: { develop: { role: developer } }
"#,
        );
        let task = &manifest.flows["default"].tasks["develop"];
        let first_attempt = root.join("first");
        let first = materialize_resources(
            &manifest,
            "developer",
            task,
            &plugin,
            &repo,
            &first_attempt,
            &BTreeMap::new(),
        )
        .unwrap();
        fs::write(plugin.join("resources/library/b.txt"), "b").unwrap();
        let second_attempt = root.join("second");
        let second = materialize_resources(
            &manifest,
            "developer",
            task,
            &plugin,
            &repo,
            &second_attempt,
            &BTreeMap::new(),
        )
        .unwrap();
        assert_eq!(second[0].inventory, vec!["a.txt", "b.txt"]);
        assert_ne!(first[0].digest, second[0].digest);

        fs::write(
            second_attempt.join(".donkeyspace/resources/library/a.txt"),
            "changed",
        )
        .unwrap();
        assert!(verify_resources(&second_attempt, &second).is_err());
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn tree_digest_is_deterministic_and_enforces_limits() {
        let root = temporary_root();
        let first = root.join("first");
        let second = root.join("second");
        fs::create_dir_all(&first).unwrap();
        fs::create_dir_all(&second).unwrap();
        fs::write(first.join("b"), "two").unwrap();
        fs::write(first.join("a"), "one").unwrap();
        fs::write(second.join("a"), "one").unwrap();
        fs::write(second.join("b"), "two").unwrap();
        assert_eq!(
            digest_resource_tree(&first).unwrap(),
            digest_resource_tree(&second).unwrap()
        );

        let too_many = root.join("too-many");
        fs::create_dir_all(&too_many).unwrap();
        for index in 0..=MAX_RESOURCE_FILES {
            fs::write(too_many.join(format!("{index:04}")), []).unwrap();
        }
        assert!(
            digest_resource_tree(&too_many)
                .unwrap_err()
                .to_string()
                .contains("files")
        );

        let too_large = root.join("too-large");
        fs::create_dir_all(&too_large).unwrap();
        let file = fs::File::create(too_large.join("large")).unwrap();
        file.set_len(MAX_RESOURCE_BYTES + 1).unwrap();
        assert!(
            digest_resource_tree(&too_large)
                .unwrap_err()
                .to_string()
                .contains("bytes")
        );
        fs::remove_dir_all(root).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn rejects_resource_symlinks_and_special_files() {
        use std::os::unix::fs::symlink;

        let root = temporary_root();
        let source = root.join("source");
        let target = root.join("target");
        fs::create_dir_all(&source).unwrap();
        fs::write(source.join("regular"), "contents").unwrap();
        symlink(source.join("regular"), source.join("link")).unwrap();
        assert!(copy_resource_directory(&source, &target).is_err());
        fs::remove_file(source.join("link")).unwrap();

        let manifest = test_manifest(
            r#"
api_version: 1
id: example
runtime: { default_image: image }
resources: { special: { source: plugin, path: dev/null } }
roles: { developer: { command: [run], resources: [{ id: special, required: true }] } }
flows:
  default:
    start: develop
    tasks: { develop: { role: developer } }
"#,
        );
        let task = &manifest.flows["default"].tasks["develop"];
        assert!(
            materialize_resources(
                &manifest,
                "developer",
                task,
                Path::new("/"),
                &source,
                &root.join("attempt"),
                &BTreeMap::new(),
            )
            .is_err()
        );
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn validates_artifact_presence_type_and_write_scope() {
        let root = temporary_root();
        fs::create_dir_all(root.join("src")).unwrap();
        fs::write(root.join("src/output.txt"), "output").unwrap();
        let file = PluginArtifact {
            path: "src/output.txt".into(),
            kind: PluginArtifactType::File,
            required: true,
        };
        assert!(validate_artifacts(&root, std::slice::from_ref(&file), &["src".into()]).is_ok());
        let wrong_type = PluginArtifact {
            kind: PluginArtifactType::Directory,
            ..file.clone()
        };
        assert!(validate_artifacts(&root, &[wrong_type], &["src".into()]).is_err());
        let missing = PluginArtifact {
            path: "src/missing".into(),
            ..file.clone()
        };
        assert!(validate_artifacts(&root, &[missing], &["src".into()]).is_err());
        assert!(validate_artifacts(&root, &[file], &["other".into()]).is_err());
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn validator_failure_is_persisted_and_gates_publication() {
        assert!(is_publishable(Outcome::Implemented));
        assert!(!is_publishable(Outcome::NeedsChanges));

        let mut result = RunResult {
            outcome: Outcome::Implemented,
            summary: "generated output".into(),
            confidence: Confidence::High,
            risk: Risk::Low,
            questions: Vec::new(),
            tests: Vec::new(),
            changed_files: vec!["src/output.txt".into()],
            human_review_reason: None,
            blocked_reason: None,
        };
        apply_validator_results(
            &mut result,
            vec![TestResult {
                name: "source validation".into(),
                command: vec!["validate".into()],
                status: TestStatus::Failed,
                exit_code: Some(1),
                summary: Some("invalid output".into()),
            }],
        );

        assert_eq!(result.outcome, Outcome::Failed);
        assert!(!is_publishable(result.outcome));
        assert_eq!(result.tests[0].status, TestStatus::Failed);
        let persisted = serde_json::to_value(PluginTaskResult {
            result,
            handoff: None,
            resources_used: Vec::new(),
            work_items: None,
        })
        .unwrap();
        assert_eq!(
            persisted.pointer("/tests/0/name"),
            Some(&json!("source validation"))
        );
    }

    #[test]
    fn required_assignment_wins_and_usage_must_be_supplied() {
        let merged = merged_resource_assignments(
            &[PluginResourceAssignment {
                id: "guide".into(),
                required: false,
            }],
            &[PluginResourceAssignment {
                id: "guide".into(),
                required: true,
            }],
        );
        assert!(merged["guide"]);
        let resources = vec![MaterializedResource {
            id: "guide".into(),
            source: PluginResourceSource::Plugin,
            source_path: "guide.md".into(),
            root: ".donkeyspace/resources/guide".into(),
            available: true,
            inventory: vec!["guide.md".into()],
            digest: Some("sha256:test".into()),
        }];
        assert!(validate_resources_used(&["guide".into()], &resources).is_ok());
        assert!(validate_resources_used(&["missing".into()], &resources).is_err());
    }

    #[test]
    fn approval_selection_requires_a_target_for_parallel_tasks() {
        let pending = vec![
            PendingApproval {
                key: TaskKey {
                    task: "rtl".into(),
                    work_item: Some("fifo".into()),
                },
                trigger: ApprovalTrigger::Required,
            },
            PendingApproval {
                key: TaskKey {
                    task: "rtl".into(),
                    work_item: Some("storage".into()),
                },
                trigger: ApprovalTrigger::Required,
            },
        ];
        assert!(
            select_pending_approvals(&pending, &HumanDecision::Approve { target: None }).is_err()
        );
        let selected = select_pending_approvals(
            &pending,
            &HumanDecision::Approve {
                target: Some("rtl/storage".into()),
            },
        )
        .unwrap();
        assert_eq!(approval_target(&selected[0].key), "rtl/storage");
        assert_eq!(
            select_pending_approvals(
                &pending,
                &HumanDecision::Approve {
                    target: Some("all".into())
                }
            )
            .unwrap()
            .len(),
            2
        );
    }

    #[test]
    fn revision_requires_feedback_and_one_target() {
        let pending = vec![PendingApproval {
            key: TaskKey {
                task: "architect".into(),
                work_item: None,
            },
            trigger: ApprovalTrigger::Required,
        }];
        assert!(
            select_pending_approvals(
                &pending,
                &HumanDecision::Revise {
                    target: Some("all".into()),
                    feedback: "change it".into(),
                }
            )
            .is_err()
        );
        let result = pending_approval_result(&pending, &BTreeMap::new(), "Review required.");
        assert_eq!(result.outcome, Outcome::NeedsHuman);
        assert!(
            result
                .human_review_reason
                .unwrap()
                .contains("/donkeyspace approve architect")
        );
    }
}
