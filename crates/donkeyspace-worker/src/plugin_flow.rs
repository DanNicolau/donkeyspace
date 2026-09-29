#[cfg(test)]
mod clarification_tests;
#[cfg(test)]
mod lifecycle_tests;
mod projection;
#[cfg(test)]
mod retention_tests;
#[cfg(test)]
mod revision_tests;
#[cfg(test)]
mod shared_repair_tests;
use projection::*;
mod execution;
use execution::*;
mod approvals;
use approvals::*;
mod checkpoint;
use checkpoint::*;
mod tracking;
use tracking::*;
mod publication;
use publication::*;

use donkeyspace_core::{
    Confidence, Outcome, PluginApprovalMode, PluginArtifact, PluginArtifactType, PluginFlow,
    PluginFlowSelection, PluginManifest, PluginParameter, PluginResourceAssignment,
    PluginResourceSource, PluginTask, PluginTaskResult, PluginTaskScope, PluginValidator,
    PluginWorkItem, PluginWorkItemRegistry, Risk, RunResult, TestResult, TestStatus,
};
use donkeyspace_db::{
    ApprovalRequestInput, JobRecord, LifecycleEventInput, PgPool, ProjectedWorkItemInput, fail_job,
    get_job, list_agent_publications_for_run, list_projected_work_items_for_run,
    mark_projected_work_item_applied, record_github_managed_resource_for_workflow_item,
    record_lifecycle_event, supersede_job, upsert_projected_work_item,
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
};
use uuid::Uuid;

use crate::active_facade;
use crate::plugin_container::run_container;
use crate::plugin_task_graph::{TaskGraph, TaskKey};
use crate::publication::{
    AttemptPublication, PublicationContext, publish_attempt, publish_checkpoint,
};

pub struct LifecycleTracking<'a> {
    pub pool: &'a PgPool,
    pub policy: &'a donkeyspace_core::Policy,
    pub coordinator: &'a JobRecord,
    pub github: Option<&'a GitHubClient>,
    pub publication: Option<PublicationContext<'a>>,
}

pub fn configured_pull_request_title(
    selection: &PluginFlowSelection,
    issue_input: &Value,
    issue_number: i64,
) -> Result<Option<String>, Box<dyn std::error::Error>> {
    let manifest = PluginManifest::from_path(&selection.manifest_path)?;
    let flow = manifest
        .flows
        .get(&selection.flow)
        .ok_or_else(|| format!("plugin `{}` has no flow `{}`", manifest.id, selection.flow))?;
    let Some(template) = flow.pull_request_title.as_deref() else {
        return Ok(None);
    };
    let title = template
        .replace("{issue_number}", &issue_number.to_string())
        .replace("{issue_title}", &publication_issue_title(issue_input));
    Ok(Some(limit_publication_title(&title)))
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
    let enriched_input = crate::plugin_input::with_issue_comments(
        issue_input,
        tracking.as_ref().and_then(|tracking| tracking.github),
    )
    .await?;
    let issue_input = &enriched_input;
    run_work_item_lifecycle(
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
    .await
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
    let is_resume = issue_input
        .pointer("/donkeyspace_resume")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let mut store = CheckpointStore::new(workspace_path, tracking.as_ref());
    let mut checkpoint = store.load(is_resume, issue_input, flow).await?;
    if let Some(saved) = checkpoint.as_mut()
        && saved.last_result.outcome == Outcome::NeedsInfo
        && saved.start_action != StartAction::Revise
    {
        saved.previous.push(json!({
            "human_response": issue_input.pointer("/comment/body").and_then(Value::as_str),
            "resume_targets": saved.revision_targets,
        }));
    }
    if let Some(saved) = checkpoint.as_mut()
        && !saved.pending_approvals.is_empty()
        && let Some(result) =
            resume_human_decision(saved, flow, issue_input, tracking.as_ref(), &mut store).await?
    {
        return Ok(result);
    }
    let rerun_start = checkpoint
        .as_ref()
        .is_some_and(|saved| saved.start_action == StartAction::Revise);
    let accept_start_projection = checkpoint
        .as_ref()
        .is_some_and(|saved| saved.start_action == StartAction::Accept);
    let revision_targets = checkpoint
        .as_ref()
        .map(|checkpoint| checkpoint.revision_targets.clone())
        .unwrap_or_default();

    let (
        previous,
        accumulated_tests,
        attempt,
        aggregate_risk,
        aggregate_confidence,
        last_result,
        requested_work_items,
    ) = if let Some(checkpoint) = &checkpoint
        && !rerun_start
    {
        let previous = checkpoint.previous.clone();
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
        let mut accumulated_tests = checkpoint
            .as_ref()
            .map(|saved| saved.accumulated_tests.clone())
            .unwrap_or_default();
        let attempt = checkpoint.as_ref().map_or(1, |saved| saved.attempt + 1);
        let planner_key = TaskKey {
            work_item: None,
            task: flow.start.clone(),
        };
        record_plugin_task_event(
            tracking.as_ref(),
            manifest,
            flow,
            &planner_key,
            tracking.as_ref().map(|tracking| tracking.coordinator.id),
            "task_started",
            "milestone",
            Some("running"),
            None,
            "Planning started.",
            None,
            None,
            None,
            Some(attempt),
        )
        .await?;
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
                record_plugin_task_event(
                    tracking.as_ref(),
                    manifest,
                    flow,
                    &planner_key,
                    tracking.as_ref().map(|tracking| tracking.coordinator.id),
                    "task_failed",
                    "milestone",
                    Some("failed"),
                    Some("failed"),
                    "Planning failed.",
                    Some(&reason),
                    None,
                    None,
                    Some(attempt),
                )
                .await?;
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
        record_plugin_task_event(
            tracking.as_ref(),
            manifest,
            flow,
            &planner_key,
            tracking.as_ref().map(|tracking| tracking.coordinator.id),
            "task_completed",
            "milestone",
            Some("completed"),
            Some(outcome_name(planner.result.outcome)),
            &planner.result.summary,
            task_result_reason(&planner.result).as_deref(),
            None,
            None,
            Some(attempt),
        )
        .await?;
        accumulated_tests.extend(planner.result.tests.clone());
        let requested_work_items = planner.work_items.clone();
        let aggregate_risk = checkpoint.as_ref().map_or(planner.result.risk, |saved| {
            max_risk(saved.aggregate_risk, planner.result.risk)
        });
        let aggregate_confidence = checkpoint
            .as_ref()
            .map_or(planner.result.confidence, |saved| {
                min_confidence(saved.aggregate_confidence, planner.result.confidence)
            });
        previous.push(task_summary(&flow.start, None, attempt, &planner));
        if retains_output(&flow.tasks[&flow.start], planner.result.outcome)
            && let Some(publication) = tracking
                .as_ref()
                .and_then(|tracking| tracking.publication.as_ref())
        {
            publish_checkpoint(
                publication,
                repo_path,
                &checkpoint_commit_title(
                    flow,
                    std::slice::from_ref(&planner_key),
                    issue_input,
                    publication.issue_number,
                )
                .unwrap_or_else(|| {
                    format!(
                        "chore({}): checkpoint {} for issue #{}",
                        active_facade().command,
                        flow.start,
                        publication.issue_number
                    )
                }),
                &expand_artifacts(
                    &flow.tasks[&flow.start].preserve_on_success,
                    parameters,
                    None,
                )?,
            )
            .await?;
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
            if matches!(
                planner.result.outcome,
                Outcome::NeedsHuman | Outcome::NeedsInfo
            ) {
                let needs_info = planner.result.outcome == Outcome::NeedsInfo;
                let pending = if needs_info {
                    Vec::new()
                } else {
                    vec![PendingApproval {
                        key: planner_key.clone(),
                        trigger: ApprovalTrigger::AgentRequested,
                    }]
                };
                let result = if needs_info {
                    planner.result.clone()
                } else {
                    pending_approval_result(
                        &pending,
                        &BTreeMap::new(),
                        flow,
                        planner
                            .result
                            .human_review_reason
                            .as_deref()
                            .unwrap_or(&planner.result.summary),
                    )
                };
                let mut saved = checkpoint.clone().unwrap_or_else(|| {
                    LifecycleCheckpoint::capture(
                        attempt,
                        &accumulated_tests,
                        &previous,
                        aggregate_risk,
                        aggregate_confidence,
                        &result,
                        &TaskGraph::for_work_items(flow, &[]),
                        &BTreeMap::new(),
                        &BTreeMap::new(),
                        &BTreeMap::new(),
                        &BTreeSet::new(),
                        pending.clone(),
                        false,
                        &[],
                    )
                });
                // Replanning can itself ask for clarification. Keep unrelated
                // completed work and projected issue identities through that pause.
                saved.attempt = attempt;
                saved.accumulated_tests = accumulated_tests.clone();
                saved.previous = previous.clone();
                saved.aggregate_risk = aggregate_risk;
                saved.aggregate_confidence = aggregate_confidence;
                saved.last_result = result.clone();
                saved.pending_approvals = pending;
                saved.start_approved = false;
                saved.revision_options.clear();
                if needs_info {
                    // Clarification resumes this coordinator and its exact draft;
                    // it is not acceptance of the eventual completed proposal.
                    saved.start_action = StartAction::Revise;
                }
                store.save(&saved, flow, true, false).await?;
                return Ok(finish_result(result, accumulated_tests, &previous));
            }
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
    let tracked_jobs = checkpoint
        .as_ref()
        .map(|checkpoint| {
            checkpoint
                .tracked_jobs
                .iter()
                .map(|entry| (entry.key.clone(), entry.job_id))
                .collect::<BTreeMap<_, _>>()
        })
        .unwrap_or_default();
    let finished_jobs = checkpoint
        .as_ref()
        .map(|checkpoint| checkpoint.completed_keys.iter().cloned().collect())
        .unwrap_or_default();
    if let Some(checkpoint) = &checkpoint {
        // A revised plan may remove work items. Preserve only unaffected
        // completed tasks that still exist in the reconciled graph.
        let retained = checkpoint
            .completed_keys
            .iter()
            .filter(|key| !rerun_start || graph.keys().any(|current| current == *key))
            .cloned()
            .collect::<Vec<_>>();
        graph.restore_completed(&retained)?;
    }
    for target in &revision_targets {
        graph.restart_from(target)?;
    }
    if checkpoint.is_none() || rerun_start || accept_start_projection {
        let phase = if accept_start_projection {
            ProjectionPhase::Accepted
        } else if rerun_start {
            ProjectionPhase::Proposed
        } else {
            ProjectionPhase::Initial
        };
        synchronize_work_items(
            tracking.as_ref(),
            flow,
            github_coordinates,
            repo_path,
            &work_items,
            &mut projected_issues,
            checkpoint
                .as_ref()
                .map(|saved| saved.tracked_jobs.as_slice())
                .unwrap_or_default(),
            phase,
        )
        .await?;
    }

    let handoffs = checkpoint
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
    let closed_projected_issues = checkpoint
        .as_ref()
        .map(|checkpoint| checkpoint.closed_projected_issues.clone())
        .unwrap_or_default();

    let mut state = LifecycleState {
        attempt,
        accumulated_tests,
        previous,
        aggregate_risk,
        aggregate_confidence,
        last_result,
        graph,
        tracked_jobs,
        finished_jobs,
        handoffs,
        projected_issues,
        closed_projected_issues,
        work_items,
    };
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
            &state.projected_issues,
            flow,
            "The lifecycle start task completed successfully and requires approval before downstream work begins.",
        );
        store
            .save(&state.snapshot(&result, pending, false), flow, true, false)
            .await?;
        return Ok(finish_result(
            result,
            state.accumulated_tests,
            &state.previous,
        ));
    }

    if let Some(tracking) = &tracking {
        let pending_keys = state
            .graph
            .keys()
            .filter(|key| !state.graph.is_completed(key))
            .cloned()
            .collect::<Vec<_>>();
        for key in pending_keys {
            ensure_waiting_tracked_job(
                tracking,
                &mut store.effects,
                &mut state.tracked_jobs,
                &mut state.finished_jobs,
                manifest,
                &selection.flow,
                flow,
                &key,
                &state.work_items,
                issue_input,
            )
            .await?;
        }
    }
    let max_handoffs = selection
        .max_handoffs_per_edge
        .unwrap_or(flow.max_handoffs_per_edge);

    while !state.graph.is_complete() {
        let ready = state
            .graph
            .ready()?
            .into_iter()
            .take(flow.max_parallel_tasks)
            .collect::<Vec<_>>();
        if ready.is_empty() {
            if let Some(tracking) = &tracking {
                fail_unfinished_tracked_jobs(
                    tracking,
                    &state.tracked_jobs,
                    &mut state.finished_jobs,
                    "plugin task graph had no runnable tasks",
                )
                .await?;
            }
            return Err("plugin task graph has no runnable tasks".into());
        }
        let wave = state.attempt;
        let released = ready
            .iter()
            .map(TaskKey::target)
            .collect::<Vec<_>>()
            .join(", ");
        stage_flow_event(
            tracking.as_ref(),
            &mut store.effects,
            "wave_started",
            "milestone",
            &format!("Wave {wave} started: {released}."),
            None,
            Some(wave),
            &wave.to_string(),
        );
        for key in &ready {
            if let Some(tracking) = &tracking {
                let job_id = ensure_waiting_tracked_job(
                    tracking,
                    &mut store.effects,
                    &mut state.tracked_jobs,
                    &mut state.finished_jobs,
                    manifest,
                    &selection.flow,
                    flow,
                    key,
                    &state.work_items,
                    issue_input,
                )
                .await?;
                store.effects.starting_children.push(job_id);
                if let Some(event) = plugin_task_event(
                    Some(tracking),
                    manifest,
                    flow,
                    key,
                    Some(job_id),
                    "task_started",
                    "milestone",
                    Some("running"),
                    None,
                    "Agent task started.",
                    None,
                    None,
                    Some(wave),
                    Some(wave),
                ) {
                    store.effects.events.push(event);
                }
            }
            state.graph.mark_running(key)?;
        }
        state.attempt += 1;
        if state.attempt > 64 {
            if let Some(tracking) = &tracking {
                fail_unfinished_tracked_jobs(
                    tracking,
                    &state.tracked_jobs,
                    &mut state.finished_jobs,
                    "plugin lifecycle exceeded 64 task waves",
                )
                .await?;
            }
            return Err("plugin lifecycle exceeded 64 task waves".into());
        }
        store
            .save(
                &state.snapshot(&state.last_result, Vec::new(), true),
                flow,
                false,
                false,
            )
            .await?;
        let task_attempts = ready
            .iter()
            .enumerate()
            .map(|(offset, key)| (key.clone(), state.attempt * 100 + offset as u32))
            .collect::<BTreeMap<_, _>>();
        let executions = join_all(ready.iter().enumerate().map(|(offset, key)| {
            let work_item = key
                .work_item
                .as_deref()
                .and_then(|id| state.work_items.iter().find(|item| item.id == id));
            execute_task(
                selection,
                manifest,
                &key.task,
                &flow.tasks[&key.task],
                work_item,
                state.attempt * 100 + offset as u32,
                repo_path,
                workspace_path,
                issue_input,
                &state.previous,
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
                    if tracking.is_some() {
                        store
                            .effects
                            .child_results
                            .push((state.tracked_jobs[&key], serde_json::to_value(&execution)?));
                        state.finished_jobs.insert(key.clone());
                    }
                    if let Some(event) = plugin_task_event(
                        tracking.as_ref(),
                        manifest,
                        flow,
                        &key,
                        tracking.as_ref().map(|_| state.tracked_jobs[&key]),
                        "task_completed",
                        "milestone",
                        Some("completed"),
                        Some(outcome_name(execution.result.outcome)),
                        &execution.result.summary,
                        task_result_reason(&execution.result).as_deref(),
                        execution
                            .handoff
                            .as_ref()
                            .map(|handoff| handoff.target.as_str()),
                        Some(wave),
                        Some(wave),
                    ) {
                        store.effects.events.push(event);
                    }
                    successful_executions.push((key, execution));
                }
                Err(error) => {
                    let reason = error.to_string();
                    if tracking.is_some() {
                        let result = failed_task_result(reason.clone());
                        store
                            .effects
                            .failed_children
                            .push((state.tracked_jobs[&key], result));
                        state.finished_jobs.insert(key.clone());
                    }
                    if let Some(event) = plugin_task_event(
                        tracking.as_ref(),
                        manifest,
                        flow,
                        &key,
                        tracking.as_ref().map(|_| state.tracked_jobs[&key]),
                        "task_failed",
                        "milestone",
                        Some("failed"),
                        Some("failed"),
                        "Agent task failed.",
                        Some(&reason),
                        None,
                        Some(wave),
                        Some(wave),
                    ) {
                        store.effects.events.push(event);
                    }
                    execution_errors.push((key, state.attempt * 100 + offset as u32, reason));
                }
            }
        }
        for (key, execution) in &successful_executions {
            state
                .accumulated_tests
                .extend(execution.result.tests.clone());
            state.aggregate_risk = max_risk(state.aggregate_risk, execution.result.risk);
            state.aggregate_confidence =
                min_confidence(state.aggregate_confidence, execution.result.confidence);
            state.previous.push(task_summary(
                &key.task,
                key.work_item.as_deref(),
                state.attempt,
                execution,
            ));
            state.last_result = execution.result.clone();
        }
        // Record successful siblings before processing feedback from this
        // parallel wave. A pause in one task must not discard independent work
        // that completed at the same time.
        for (key, execution) in &successful_executions {
            if execution.result.outcome == Outcome::Implemented
                && flow.tasks[&key.task].approval != PluginApprovalMode::Required
            {
                state.graph.mark_completed(key)?;
            }
        }
        if successful_executions.iter().any(|(key, execution)| {
            retains_output(&flow.tasks[&key.task], execution.result.outcome)
        }) && let Some(publication) = tracking
            .as_ref()
            .and_then(|tracking| tracking.publication.as_ref())
        {
            let checkpoint_keys = successful_executions
                .iter()
                .filter(|(key, execution)| {
                    retains_output(&flow.tasks[&key.task], execution.result.outcome)
                })
                .map(|(key, _)| key.clone())
                .collect::<Vec<_>>();
            let commit_title = checkpoint_commit_title(
                flow,
                &checkpoint_keys,
                issue_input,
                publication.issue_number,
            )
            .unwrap_or_else(|| {
                format!(
                    "chore({}): checkpoint task wave for issue #{}",
                    active_facade().command,
                    publication.issue_number
                )
            });
            let mut retained = Vec::new();
            for (key, _) in &successful_executions {
                let item = state
                    .work_items
                    .iter()
                    .find(|item| Some(&item.id) == key.work_item.as_ref());
                retained.extend(expand_artifacts(
                    &flow.tasks[&key.task].preserve_on_success,
                    parameters,
                    item,
                )?);
            }
            publish_checkpoint(publication, repo_path, &commit_title, &retained).await?;
        }
        // Task execution has copied retained output into the coordinator tree.
        // Commit it before recording completed tasks and immutable provenance;
        // otherwise capture rejects legitimate output as an uncommitted change.
        store
            .save(
                &state.snapshot(&state.last_result, Vec::new(), true),
                flow,
                false,
                false,
            )
            .await?;
        if let Some((_, architect)) = successful_executions.iter().find(|(key, execution)| {
            key.task == flow.start && execution.result.outcome == Outcome::Implemented
        }) {
            let revised_registry: PluginWorkItemRegistry =
                serde_json::from_str(&fs::read_to_string(repo_path.join(&registry_path))?)?;
            validate_work_items(&revised_registry.work_items)?;
            let revised_work_items = select_lifecycle_work_items(
                &revised_registry.work_items,
                architect.work_items.as_deref(),
            )?;
            if let Some(item) = revised_work_items
                .iter()
                .find(|item| !repo_path.join(&item.spec).is_file())
            {
                return Err(format!(
                    "architect work item `{}` references missing specification `{}`",
                    item.id, item.spec
                )
                .into());
            }
            synchronize_work_items(
                tracking.as_ref(),
                flow,
                github_coordinates,
                repo_path,
                &revised_work_items,
                &mut state.projected_issues,
                &[],
                ProjectionPhase::Proposed,
            )
            .await?;
            let completed = state.graph.completed_keys().cloned().collect::<Vec<_>>();
            let mut revised_graph = TaskGraph::for_work_items(flow, &revised_work_items);
            let valid = revised_graph.keys().cloned().collect::<BTreeSet<_>>();
            revised_graph.restore_completed(
                &completed
                    .into_iter()
                    .filter(|key| valid.contains(key))
                    .collect::<Vec<_>>(),
            )?;
            state.graph = revised_graph;
            state.work_items = revised_work_items;
        }
        for (key, execution) in &successful_executions {
            let work_item = key
                .work_item
                .as_deref()
                .and_then(|id| state.work_items.iter().find(|item| item.id == id));
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
                tracking.as_ref().map(|_| state.tracked_jobs[key]),
                Some(execution.result.outcome),
                &execution.result.summary,
                key.work_item
                    .as_deref()
                    .and_then(|work_item| state.projected_issues.get(work_item).copied()),
            )
            .await;
        }
        for (key, task_attempt, reason) in &execution_errors {
            let work_item = key
                .work_item
                .as_deref()
                .and_then(|id| state.work_items.iter().find(|item| item.id == id));
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
                tracking.as_ref().map(|_| state.tracked_jobs[key]),
                None,
                reason,
                key.work_item
                    .as_deref()
                    .and_then(|work_item| state.projected_issues.get(work_item).copied()),
            )
            .await;
        }
        if let Some((_, _, error)) = execution_errors.into_iter().next() {
            if let Some(tracking) = &tracking {
                fail_unfinished_tracked_jobs(
                    tracking,
                    &state.tracked_jobs,
                    &mut state.finished_jobs,
                    &format!("plugin lifecycle stopped after a parallel task failed: {error}"),
                )
                .await?;
            }
            return Err(error.into());
        }

        let mut feedback = Vec::new();
        let mut pause_reasons = Vec::new();
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
        let mut clarifications = Vec::new();
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
                                &state.tracked_jobs,
                                &mut state.finished_jobs,
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
                                &state.tracked_jobs,
                                &mut state.finished_jobs,
                                &reason,
                            )
                            .await?;
                        }
                        return Err(reason.into());
                    }
                    let handoff_summary = task
                        .handoff_descriptions
                        .get(&handoff.target)
                        .map(String::as_str)
                        .unwrap_or("An agent requested changes from another task.");
                    if let Some(event) = plugin_task_event(
                        tracking.as_ref(),
                        manifest,
                        flow,
                        &key,
                        tracking.as_ref().map(|_| state.tracked_jobs[&key]),
                        "handoff_requested",
                        "milestone",
                        None,
                        Some("needs_changes"),
                        handoff_summary,
                        Some(&handoff.reason),
                        Some(&handoff.target),
                        Some(wave),
                        Some(wave),
                    ) {
                        store.effects.events.push(event);
                    }
                    let edge = (
                        key.work_item.clone(),
                        key.task.clone(),
                        handoff.target.clone(),
                    );
                    let count = state.handoffs.entry(edge.clone()).or_default();
                    *count += 1;
                    if *count > max_handoffs {
                        let resume_target = normalize_handoff_target(flow, &key, &handoff.target)?;
                        state.graph.restart_from(&resume_target)?;
                        // A human decision authorizes a fresh bounded feedback
                        // cycle on the edge that caused the pause.
                        state.handoffs.insert(edge, 0);
                        pause_reasons.push(format!(
                            "handoff from `{}` to `{}` exceeded policy limit {max_handoffs}: {}",
                            key.task, handoff.target, handoff.reason
                        ));
                        if !required_approvals
                            .iter()
                            .any(|approval| approval.key == resume_target)
                        {
                            required_approvals.push(PendingApproval {
                                key: resume_target,
                                trigger: ApprovalTrigger::AgentRequested,
                            });
                        }
                        continue;
                    }
                    feedback.push(normalize_handoff_target(flow, &key, &handoff.target)?);
                }
                Outcome::NeedsInfo => {
                    state.graph.restart_from(&key)?;
                    clarifications.push((key, execution.result.questions));
                }
                Outcome::NeedsHuman => {
                    let resume_target = key.clone();
                    state.graph.restart_from(&resume_target)?;
                    let reason = execution
                        .result
                        .human_review_reason
                        .as_deref()
                        .unwrap_or("task requested human judgment");
                    pause_reasons.push(format!("{}: {reason}", key.target()));
                    if !required_approvals
                        .iter()
                        .any(|approval| approval.key == resume_target)
                    {
                        required_approvals.push(PendingApproval {
                            key: resume_target,
                            trigger: ApprovalTrigger::AgentRequested,
                        });
                    }
                }
                _ => {
                    if let Some(tracking) = &tracking {
                        fail_unfinished_tracked_jobs(
                            tracking,
                            &state.tracked_jobs,
                            &mut state.finished_jobs,
                            &format!(
                                "plugin lifecycle stopped after task `{}` returned {:?}",
                                key.task, execution.result.outcome
                            ),
                        )
                        .await?;
                    }
                    return Ok(finish_result(
                        execution.result,
                        state.accumulated_tests,
                        &state.previous,
                    ));
                }
            }
        }
        for target in feedback {
            let invalidated = state.graph.restart_from(&target)?;
            let invalidated_names = invalidated
                .iter()
                .map(TaskKey::target)
                .collect::<Vec<_>>()
                .join(", ");
            stage_flow_event(
                tracking.as_ref(),
                &mut store.effects,
                "repair_wave_created",
                "milestone",
                &format!("Repair work was released for {invalidated_names}."),
                Some("A task handoff invalidated its target and dependent results."),
                Some(wave + 1),
                &format!("{}:{}", wave + 1, target.target()),
            );
            required_approvals.retain(|approval| {
                matches!(approval.trigger, ApprovalTrigger::AgentRequested)
                    || !invalidated.contains(&approval.key)
            });
            if let Some(tracking) = &tracking {
                for invalidated_key in invalidated {
                    ensure_waiting_tracked_job(
                        tracking,
                        &mut store.effects,
                        &mut state.tracked_jobs,
                        &mut state.finished_jobs,
                        manifest,
                        &selection.flow,
                        flow,
                        &invalidated_key,
                        &state.work_items,
                        issue_input,
                    )
                    .await?;
                }
            }
        }
        let questions = clarifications
            .iter()
            .flat_map(|(key, questions)| {
                questions
                    .iter()
                    .map(|question| format!("{}: {question}", key.target()))
            })
            .collect::<Vec<_>>();
        if !required_approvals.is_empty() {
            // A clarification reply cannot bypass another task's approval.
            // Mixed waves keep the stricter gate and expose each question.
            for (key, _) in &clarifications {
                if !required_approvals
                    .iter()
                    .any(|approval| &approval.key == key)
                {
                    required_approvals.push(PendingApproval {
                        key: key.clone(),
                        trigger: ApprovalTrigger::AgentRequested,
                    });
                }
            }
            pause_reasons.extend(questions.clone());
        }
        if !required_approvals.is_empty() || !clarifications.is_empty() {
            // Collect every blocked sibling before pausing. Returning on the
            // first needs_human result loses other targets and reruns them when
            // an unrelated target is approved.
            if let Some(tracking) = &tracking {
                let waiting = state
                    .graph
                    .keys()
                    .filter(|key| {
                        !state.graph.is_completed(key)
                            && !required_approvals.iter().any(|approval| {
                                &approval.key == *key
                                    && matches!(approval.trigger, ApprovalTrigger::Required)
                            })
                    })
                    .cloned()
                    .collect::<Vec<_>>();
                for key in waiting {
                    ensure_waiting_tracked_job(
                        tracking,
                        &mut store.effects,
                        &mut state.tracked_jobs,
                        &mut state.finished_jobs,
                        manifest,
                        &selection.flow,
                        flow,
                        &key,
                        &state.work_items,
                        issue_input,
                    )
                    .await?;
                }
            }
            let lead = if pause_reasons.is_empty() {
                "The configured tasks completed successfully and require approval before their dependents can run.".to_string()
            } else {
                format!(
                    "{}\n\nPreserved checkpoint:\n- {} completed task(s) remain valid.\n- Existing block issues and workspace changes will be reused.",
                    pause_reasons.join("\n\n"),
                    state.graph.completed_keys().count()
                )
            };
            let mut result = if required_approvals.is_empty() {
                RunResult {
                    outcome: Outcome::NeedsInfo,
                    summary: "Awaiting answers before continuing the retained workflow.".into(),
                    confidence: state.aggregate_confidence,
                    risk: state.aggregate_risk,
                    questions: Vec::new(),
                    tests: Vec::new(),
                    changed_files: Vec::new(),
                    human_review_reason: None,
                    blocked_reason: None,
                }
            } else {
                pending_approval_result(&required_approvals, &state.projected_issues, flow, &lead)
            };
            result.questions = questions;
            let mut saved = state.snapshot(&result, required_approvals, true);
            if result.outcome == Outcome::NeedsInfo {
                saved.revision_targets = clarifications.into_iter().map(|(key, _)| key).collect();
            }
            store.save(&saved, flow, true, false).await?;
            return Ok(finish_result(
                result,
                state.accumulated_tests,
                &state.previous,
            ));
        }
        store
            .save(
                &state.snapshot(&state.last_result, Vec::new(), true),
                flow,
                false,
                false,
            )
            .await?;
        if let Some(github) = tracking.as_ref().and_then(|tracking| tracking.github)
            && let (Some(owner), Some(repo), _) = github_coordinates
        {
            let tracking_ref = tracking
                .as_ref()
                .expect("GitHub projection requires tracking");
            crate::cancellation::side_effect(
                tracking_ref.pool,
                tracking_ref.coordinator.id,
                async {
                    for item in &state.work_items {
                        if state.graph.work_item_is_complete(&item.id)
                            && state.closed_projected_issues.insert(item.id.clone())
                            && let Some(issue_number) = state.projected_issues.get(&item.id)
                            && let Err(error) = github.close_issue(owner, repo, *issue_number).await
                        {
                            tracing::warn!(
                                %error,
                                work_item = item.id,
                                "failed to close projected github work-item issue"
                            );
                        }
                    }
                    Ok::<_, Box<dyn std::error::Error>>(())
                },
            )
            .await?;
        }
    }

    state.last_result.outcome = Outcome::Implemented;
    state.last_result.risk = state.aggregate_risk;
    state.last_result.confidence = state.aggregate_confidence;
    state.last_result.summary = format!(
        "Completed {} block work item(s) across {} task execution(s).",
        state.work_items.len(),
        state.previous.len()
    );
    store
        .save(
            &state.snapshot(&state.last_result, Vec::new(), true),
            flow,
            false,
            true,
        )
        .await?;
    Ok(finish_result(
        state.last_result,
        state.accumulated_tests,
        &state.previous,
    ))
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

fn task_summary(
    task: &str,
    work_item: Option<&str>,
    attempt: u32,
    execution: &PluginTaskResult,
) -> Value {
    let result = &execution.result;
    json!({
        "task": task,
        "work_item": work_item,
        "attempt": attempt,
        "outcome": result.outcome,
        "summary": result.summary.chars().take(2_000).collect::<String>(),
        "questions": result.questions,
        "human_review_reason": result.human_review_reason,
        "blocked_reason": result.blocked_reason,
        "handoff": execution.handoff,
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
            Some(format!(
                "{stage}{}: {summary}",
                if entry.get("superseded").and_then(Value::as_bool) == Some(true) {
                    " (superseded)"
                } else {
                    ""
                }
            ))
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

    #[test]
    fn checkpoint_titles_use_plugin_tags_and_work_items() {
        let manifest = test_manifest(
            r#"
api_version: 1
id: example.hardware
runtime: { default_image: example:dev }
roles:
  rtl: { command: [run, rtl] }
  dv: { command: [run, dv] }
flows:
  blocks:
    start: rtl
    replaces_default_lifecycle: true
    work_items_path: work-items.json
    tasks:
      rtl: { role: rtl, display_name: RTL implementation, publication_tag: RTL }
      dv: { role: dv, display_name: Design verification, publication_tag: DV }
"#,
        );
        let keys = vec![
            TaskKey {
                work_item: Some("counter_detect".into()),
                task: "rtl".into(),
            },
            TaskKey {
                work_item: Some("counter_detect".into()),
                task: "dv".into(),
            },
        ];
        assert_eq!(
            checkpoint_commit_title(
                &manifest.flows["blocks"],
                &keys,
                &json!({"issue": {"title": "Counter Detect"}}),
                11,
            )
            .as_deref(),
            Some("[RTL][DV] RTL implementation + Design verification: counter_detect (#11)")
        );
    }

    fn approval_manifest() -> PluginManifest {
        test_manifest(
            r#"
api_version: 1
id: test.approvals
runtime: { default_image: test:dev }
roles:
  architect: { command: [true] }
  dv: { command: [true] }
  rtl: { command: [true] }
flows:
  blocks:
    start: architect
    replaces_default_lifecycle: true
    work_items_path: docs/index.json
    tasks:
      architect: { role: architect, approval: required }
      rtl: { role: rtl }
      dv: { role: dv, allowed_handoffs: [rtl] }
"#,
        )
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
            display_name: None,
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
    fn repeated_handoff_replaces_terminal_approval_job_without_duplicating_active_work() {
        // Approval-required tasks have a terminal child row before their graph
        // key is recorded as completed. A later handoff must trust the row's
        // status and replace it even when the checkpoint's completed set did
        // not contain the task.
        assert_eq!(
            tracked_job_disposition(Some("completed")),
            TrackedJobDisposition::ReplaceTerminal
        );
        assert_eq!(
            tracked_job_disposition(Some("failed")),
            TrackedJobDisposition::ReplaceTerminal
        );
        assert_eq!(
            tracked_job_disposition(None),
            TrackedJobDisposition::ReplaceTerminal
        );
        assert_eq!(
            tracked_job_disposition(Some("waiting")),
            TrackedJobDisposition::KeepWaiting
        );
        assert_eq!(
            tracked_job_disposition(Some("running")),
            TrackedJobDisposition::RejectActive
        );
        assert_eq!(
            tracked_job_disposition(Some("paused")),
            TrackedJobDisposition::RejectActive
        );
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
    replaces_default_lifecycle: true
    work_items_path: work-items.json
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
    replaces_default_lifecycle: true
    work_items_path: work-items.json
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
    replaces_default_lifecycle: true
    work_items_path: work-items.json
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
    replaces_default_lifecycle: true
    work_items_path: work-items.json
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
            display_name: None,
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
        assert_eq!(selected[0].key.target(), "rtl/storage");
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
        let manifest = approval_manifest();
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
        let result = pending_approval_result(
            &pending,
            &BTreeMap::from([("counter_detect".into(), 3)]),
            &manifest.flows["blocks"],
            "Review required.",
        );
        assert_eq!(result.outcome, Outcome::NeedsHuman);
        let reason = result.human_review_reason.unwrap();
        assert!(reason.contains("Approval subjects:"));
        assert!(reason.contains("the proposed lifecycle plan and block specifications"));
        assert!(reason.contains("`counter_detect`: #3"));
        assert!(reason.contains("Approving accepts this output as the current checkpoint"));
        assert!(reason.contains("Revising keeps those dependents blocked"));
        assert!(reason.contains("/donkeyspace approve architect"));
        assert!(reason.contains("<describe the required changes>"));
    }

    #[test]
    fn agent_requested_approval_explains_that_it_authorizes_a_rerun() {
        let manifest = approval_manifest();
        let pending = vec![PendingApproval {
            key: TaskKey {
                task: "dv".into(),
                work_item: Some("counter_detect".into()),
            },
            trigger: ApprovalTrigger::AgentRequested,
        }];
        let result = pending_approval_result(
            &pending,
            &BTreeMap::from([("counter_detect".into(), 3)]),
            &manifest.flows["blocks"],
            "DV needs a human decision.",
        );
        let reason = result.human_review_reason.unwrap();

        assert!(reason.contains("Review the completed `dv` output for work item `counter_detect`"));
        assert!(reason.contains("Approving authorizes this target to rerun"));
        assert!(reason.contains("Revising reruns it with the feedback you provide"));
    }
}
