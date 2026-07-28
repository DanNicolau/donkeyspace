use donkeyspace_core::{
    Confidence, Outcome, PluginFlow, PluginFlowSelection, PluginManifest, PluginTask,
    PluginTaskResult, PluginWorkItem, PluginWorkItemRegistry, Risk, RunResult, TestResult,
};
use donkeyspace_db::{
    JobRecord, PgPool, complete_job, create_waiting_job, fail_job, start_waiting_job,
};
use donkeyspace_github::{GitHubClient, GitHubWorkItem};
use futures::future::join_all;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    env, fs,
    path::{Component, Path},
    process::Stdio,
};
use tokio::process::Command;
use uuid::Uuid;

use crate::plugin_task_graph::{TaskGraph, TaskKey};

pub struct LifecycleTracking<'a> {
    pub pool: &'a PgPool,
    pub coordinator: &'a JobRecord,
    pub github: Option<&'a GitHubClient>,
}

const CHECKPOINT_VERSION: u32 = 1;

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
}

pub async fn run(
    selection: &PluginFlowSelection,
    repo_path: &Path,
    workspace_path: &Path,
    issue_input: &Value,
    tracking: Option<LifecycleTracking<'_>>,
) -> Result<RunResult, Box<dyn std::error::Error>> {
    let manifest = PluginManifest::from_path(&selection.manifest_path)?;
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
        let (read_roots, write_roots) =
            resolve_access(selection, &stage_name, &stage.read, &stage.write)?;
        for root in read_roots.iter().chain(&write_roots) {
            copy_root(repo_path, &stage_repo, root)?;
        }

        let donkeyspace = stage_root.join(".donkeyspace");
        fs::create_dir_all(&donkeyspace)?;
        let input_path = donkeyspace.join("run-input.json");
        let result_path = donkeyspace.join("run-result.json");
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
                "previous_stages": previous,
                "mcp_servers": selected_mcp,
            }))?,
        )?;

        let image = agent
            .image
            .as_deref()
            .unwrap_or(&manifest.runtime.default_image);
        let output = run_container(
            image,
            &agent.command,
            &stage_root,
            &selection.environment,
            &agent.environment,
        )
        .await?;
        if !output.status.success() {
            return Err(format!(
                "plugin stage `{stage_name}` exited {:?}: {}",
                output.status.code(),
                String::from_utf8_lossy(&output.stderr).trim()
            )
            .into());
        }
        let raw = fs::read_to_string(&result_path)?;
        let stage_result: PluginTaskResult = serde_json::from_str(&raw)?;
        stage_result.result.validate_for_orchestration()?;
        validate_changed_files(&stage_result.result.changed_files, &write_roots)?;
        let publish_changes = matches!(
            stage_result.result.outcome,
            Outcome::Implemented | Outcome::NeedsChanges
        );
        if publish_changes {
            for root in &write_roots {
                replace_root(&stage_repo, repo_path, root)?;
            }
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

async fn run_work_item_lifecycle(
    selection: &PluginFlowSelection,
    manifest: &PluginManifest,
    flow: &PluginFlow,
    repo_path: &Path,
    workspace_path: &Path,
    issue_input: &Value,
    tracking: Option<LifecycleTracking<'_>>,
) -> Result<RunResult, Box<dyn std::error::Error>> {
    let checkpoint_path = workspace_path
        .join(".donkeyspace")
        .join("lifecycle-checkpoint.json");
    let is_resume = issue_input
        .pointer("/donkeyspace_resume")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let checkpoint = if is_resume {
        if checkpoint_path.is_file() {
            let checkpoint: LifecycleCheckpoint =
                serde_json::from_str(&fs::read_to_string(&checkpoint_path)?)?;
            if checkpoint.version != CHECKPOINT_VERSION {
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

    let (
        mut previous,
        mut accumulated_tests,
        mut attempt,
        mut aggregate_risk,
        mut aggregate_confidence,
        mut last_result,
    ) = if let Some(checkpoint) = &checkpoint {
        let mut previous = checkpoint.previous.clone();
        previous.push(json!({
            "human_response": issue_input.pointer("/comment/body").and_then(Value::as_str),
            "resume_target": checkpoint.resume_target,
        }));
        (
            previous,
            checkpoint.accumulated_tests.clone(),
            checkpoint.attempt,
            checkpoint.aggregate_risk,
            checkpoint.aggregate_confidence,
            checkpoint.last_result.clone(),
        )
    } else {
        let mut previous = Vec::<Value>::new();
        if is_resume {
            previous.push(json!({
                "human_response": issue_input.pointer("/comment/body").and_then(Value::as_str),
                "resume_target": {"work_item": null, "task": flow.start},
            }));
        }
        let mut accumulated_tests = Vec::<TestResult>::new();
        let attempt = 1u32;
        let planner = execute_task(
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
        )
        .await?;
        accumulated_tests.extend(planner.result.tests.clone());
        let aggregate_risk = planner.result.risk;
        let aggregate_confidence = planner.result.confidence;
        previous.push(task_summary(&flow.start, None, attempt, &planner.result));
        if planner.result.outcome != Outcome::Implemented {
            return Ok(finish_result(planner.result, accumulated_tests, &previous));
        }
        (
            previous,
            accumulated_tests,
            attempt,
            aggregate_risk,
            aggregate_confidence,
            planner.result,
        )
    };

    let registry_path = flow
        .work_items_path
        .as_deref()
        .ok_or("lifecycle flow is missing work_items_path")?;
    let registry: PluginWorkItemRegistry =
        serde_json::from_str(&fs::read_to_string(repo_path.join(registry_path))?)?;
    validate_work_items(&registry.work_items)?;
    if let Some(item) = registry
        .work_items
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
    let mut graph = TaskGraph::for_work_items(flow, &registry.work_items);
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
        graph.restore_completed(&checkpoint.completed_keys);
    } else {
        if flow.project_github_issues
            && let Some(github) = tracking.as_ref().and_then(|tracking| tracking.github)
            && let (Some(owner), Some(repo), Some(parent_issue_number)) = github_coordinates
        {
            let work_items = registry
                .work_items
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
            match github
                .project_work_items(owner, repo, parent_issue_number, &work_items)
                .await
            {
                Ok(issues) => projected_issues = issues,
                Err(error) => tracing::warn!(%error, "github work-item projection failed"),
            }
        }
        if let Some(tracking) = &tracking {
            for key in graph.keys() {
                let job = create_tracked_job(
                    tracking,
                    manifest,
                    &selection.flow,
                    flow,
                    key,
                    &registry.work_items,
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
            .ready()
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
            graph.mark_running(key);
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
        let executions = join_all(ready.iter().enumerate().map(|(offset, key)| {
            let work_item = key
                .work_item
                .as_deref()
                .and_then(|id| registry.work_items.iter().find(|item| item.id == id));
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
            )
        }))
        .await;

        let mut successful_executions = Vec::new();
        let mut first_execution_error = None;
        for (key, execution) in ready.into_iter().zip(executions) {
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
                    if let Some(tracking) = &tracking {
                        let result = failed_task_result(error.to_string());
                        fail_job(tracking.pool, tracked_jobs[&key], &result).await?;
                        finished_jobs.insert(key);
                    }
                    if first_execution_error.is_none() {
                        first_execution_error = Some(error);
                    }
                }
            }
        }
        if let Some(error) = first_execution_error {
            if let Some(tracking) = &tracking {
                fail_unfinished_tracked_jobs(
                    tracking,
                    &tracked_jobs,
                    &mut finished_jobs,
                    &format!("plugin lifecycle stopped after a parallel task failed: {error}"),
                )
                .await?;
            }
            return Err(error);
        }

        let mut feedback = Vec::new();
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
            if execution.result.outcome == Outcome::Implemented {
                graph.mark_completed(key);
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
                        let resume_target = TaskKey {
                            work_item: key.work_item.clone(),
                            task: handoff.target.clone(),
                        };
                        graph.restart_from(&resume_target);
                        if let Some(tracking) = &tracking {
                            let pending_keys = graph
                                .keys()
                                .filter(|key| !graph.is_completed(key))
                                .cloned()
                                .collect::<Vec<_>>();
                            for pending_key in pending_keys {
                                if finished_jobs.remove(&pending_key) {
                                    let job = create_tracked_job(
                                        tracking,
                                        manifest,
                                        &selection.flow,
                                        flow,
                                        &pending_key,
                                        &registry.work_items,
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
                        let mut result = execution.result;
                        result.outcome = Outcome::NeedsHuman;
                        result.human_review_reason = Some(format!(
                            "handoff from `{}` to `{}` exceeded policy limit {max_handoffs}: {}\n\nPreserved checkpoint:\n- {} completed task(s) remain valid.\n- Existing block issues and workspace changes will be reused.\n- After a human comment, resume at `{}` for block `{}` and invalidate only that task and its dependents.",
                            key.task,
                            handoff.target,
                            handoff.reason,
                            graph.completed_keys().count(),
                            resume_target.task,
                            resume_target.work_item.as_deref().unwrap_or("workflow")
                        ));
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
                                resume_target,
                            },
                        )?;
                        return Ok(finish_result(result, accumulated_tests, &previous));
                    }
                    feedback.push((key.work_item.clone(), handoff.target));
                }
                Outcome::NeedsHuman => {
                    let resume_target = key.clone();
                    graph.restart_from(&resume_target);
                    if let Some(tracking) = &tracking {
                        let pending_keys = graph
                            .keys()
                            .filter(|key| !graph.is_completed(key))
                            .cloned()
                            .collect::<Vec<_>>();
                        for pending_key in pending_keys {
                            if finished_jobs.remove(&pending_key) {
                                let job = create_tracked_job(
                                    tracking,
                                    manifest,
                                    &selection.flow,
                                    flow,
                                    &pending_key,
                                    &registry.work_items,
                                    issue_input,
                                )
                                .await?;
                                tracked_jobs.insert(pending_key, job.id);
                            }
                        }
                    }
                    let mut result = execution.result;
                    let original_reason = result
                        .human_review_reason
                        .take()
                        .unwrap_or_else(|| "task requested human judgment".to_string());
                    result.human_review_reason = Some(format!(
                        "{original_reason}\n\nPreserved checkpoint:\n- {} completed task(s) remain valid.\n- Existing block issues and workspace changes will be reused.\n- After a human comment, resume `{}` for block `{}` and invalidate only that task and its dependents.",
                        graph.completed_keys().count(),
                        resume_target.task,
                        resume_target.work_item.as_deref().unwrap_or("workflow")
                    ));
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
                            resume_target,
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
        for (work_item, target) in feedback {
            let invalidated = graph.restart_from(&TaskKey {
                work_item,
                task: target,
            });
            if let Some(tracking) = &tracking {
                for invalidated_key in invalidated {
                    if finished_jobs.remove(&invalidated_key) {
                        let job = create_tracked_job(
                            tracking,
                            manifest,
                            &selection.flow,
                            flow,
                            &invalidated_key,
                            &registry.work_items,
                            issue_input,
                        )
                        .await?;
                        tracked_jobs.insert(invalidated_key, job.id);
                    }
                }
            }
        }
        if let Some(github) = tracking.as_ref().and_then(|tracking| tracking.github)
            && let (Some(owner), Some(repo), _) = github_coordinates
        {
            for item in &registry.work_items {
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
        registry.work_items.len(),
        previous.len()
    );
    if checkpoint_path.exists() {
        fs::remove_file(&checkpoint_path)?;
    }
    Ok(finish_result(last_result, accumulated_tests, &previous))
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
) -> Result<PluginTaskResult, Box<dyn std::error::Error>> {
    let role = &manifest.roles[&task.role];
    let item_suffix = work_item
        .map(|item| format!("-{}", item.id))
        .unwrap_or_default();
    let task_root = workspace_path
        .join("plugin-tasks")
        .join(format!("{attempt:04}-{task_name}{item_suffix}"));
    let task_repo = task_root.join("repo");
    fs::create_dir_all(&task_repo)?;
    let declared_read = expand_roots(&task.read, work_item);
    let declared_write = expand_roots(&task.write, work_item);
    let (read_roots, write_roots) =
        resolve_access(selection, task_name, &declared_read, &declared_write)?;
    for root in read_roots.iter().chain(&write_roots) {
        copy_root(repo_path, &task_repo, root)?;
    }
    let donkeyspace = task_root.join(".donkeyspace");
    fs::create_dir_all(&donkeyspace)?;
    let result_path = donkeyspace.join("run-result.json");
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
    if !output.status.success() {
        return Err(format!(
            "plugin task `{task_name}` exited {:?}: {}",
            output.status.code(),
            String::from_utf8_lossy(&output.stderr).trim()
        )
        .into());
    }
    let task_result: PluginTaskResult = serde_json::from_str(&fs::read_to_string(result_path)?)?;
    task_result.result.validate_for_orchestration()?;
    validate_changed_files(&task_result.result.changed_files, &write_roots)?;
    if matches!(
        task_result.result.outcome,
        Outcome::Implemented | Outcome::NeedsChanges
    ) {
        for root in &write_roots {
            replace_root(&task_repo, repo_path, root)?;
        }
    }
    Ok(task_result)
}

fn expand_roots(roots: &[String], work_item: Option<&PluginWorkItem>) -> Vec<String> {
    roots
        .iter()
        .map(|root| match work_item {
            Some(item) => root.replace("{work_item}", &item.id),
            None => root.clone(),
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
    for name in allowed {
        if let Some(source) = configured.get(name) {
            let value = env::var(source)
                .map_err(|_| format!("required plugin environment source `{source}` is unset"))?;
            docker.arg("--env").arg(format!("{name}={value}"));
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
) -> Result<(Vec<String>, Vec<String>), Box<dyn std::error::Error>> {
    let Some(overrides) = selection.task_access_overrides.get(stage) else {
        return Ok((declared_read.to_vec(), declared_write.to_vec()));
    };
    let read = overrides
        .read
        .clone()
        .unwrap_or_else(|| declared_read.to_vec());
    let write = overrides
        .write
        .clone()
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
    if let Some(path) = files.iter().find(|path| !covered(path, roots)) {
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
        let root =
            env::temp_dir().join(format!("donkeyspace-plugin-test-{}", uuid::Uuid::now_v7()));
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
}
