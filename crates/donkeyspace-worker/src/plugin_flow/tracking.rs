use super::*;

#[allow(clippy::too_many_arguments)]
pub(super) async fn record_plugin_task_event(
    tracking: Option<&LifecycleTracking<'_>>,
    manifest: &PluginManifest,
    flow: &PluginFlow,
    key: &TaskKey,
    job_id: Option<Uuid>,
    event_type: &str,
    level: &str,
    status: Option<&str>,
    outcome: Option<&str>,
    summary: &str,
    reason: Option<&str>,
    handoff_target: Option<&str>,
    wave: Option<u32>,
    attempt: Option<u32>,
) -> Result<(), Box<dyn std::error::Error>> {
    if let Some(input) = plugin_task_event(
        tracking,
        manifest,
        flow,
        key,
        job_id,
        event_type,
        level,
        status,
        outcome,
        summary,
        reason,
        handoff_target,
        wave,
        attempt,
    ) {
        record_lifecycle_event(tracking.expect("event has tracking").pool, &input).await?;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub(super) fn plugin_task_event(
    tracking: Option<&LifecycleTracking<'_>>,
    manifest: &PluginManifest,
    flow: &PluginFlow,
    key: &TaskKey,
    job_id: Option<Uuid>,
    event_type: &str,
    level: &str,
    status: Option<&str>,
    outcome: Option<&str>,
    summary: &str,
    reason: Option<&str>,
    handoff_target: Option<&str>,
    wave: Option<u32>,
    attempt: Option<u32>,
) -> Option<LifecycleEventInput> {
    let tracking = tracking?;
    let workflow_item_id = tracking.coordinator.workflow_item_id?;
    let task = &flow.tasks[&key.task];
    let role = &manifest.roles[&task.role];
    let identity = format!(
        "{}:{}:{}:{}:{}:{}:{}",
        tracking.coordinator.id,
        event_type,
        key.task,
        key.work_item.as_deref().unwrap_or("workflow"),
        job_id.map_or_else(|| "-".into(), |value| value.to_string()),
        wave.map_or_else(|| "-".into(), |value| value.to_string()),
        attempt.map_or_else(|| "-".into(), |value| value.to_string()),
    );
    Some(LifecycleEventInput {
        workflow_item_id,
        coordinator_job_id: Some(tracking.coordinator.id),
        job_id,
        dedupe_key: Some(identity),
        event_type: event_type.into(),
        level: level.into(),
        source: "worker".into(),
        actor: None,
        wave: wave.map(|value| value as i32),
        attempt: attempt.map(|value| value as i32),
        role: Some(task.role.clone()),
        role_display_name: role
            .display_name
            .clone()
            .or_else(|| Some(task.role.clone())),
        task: Some(key.task.clone()),
        task_display_name: task.display_name.clone().or_else(|| Some(key.task.clone())),
        work_item: key.work_item.clone(),
        status: status.map(Into::into),
        outcome: outcome.map(Into::into),
        summary: concise_event_text(summary),
        reason: reason.map(|reason| {
            if event_type == "task_completed" {
                reason.to_owned()
            } else {
                concise_event_text(reason)
            }
        }),
        handoff_target: handoff_target.map(Into::into),
        links: json!([]),
    })
}

pub(super) fn concise_event_text(value: &str) -> String {
    let value = value.split("\n\n").next().unwrap_or(value).trim();
    let mut shortened = value.chars().take(600).collect::<String>();
    if value.chars().count() > 600 {
        shortened.push('…');
    }
    shortened
}

pub(super) fn task_result_reason(result: &RunResult) -> Option<String> {
    let mut details = result
        .human_review_reason
        .iter()
        .chain(&result.blocked_reason)
        .cloned()
        .collect::<Vec<_>>();
    details.extend(
        result
            .questions
            .iter()
            .map(|question| format!("- {question}")),
    );
    (!details.is_empty()).then(|| details.join("\n\n"))
}

pub(super) fn outcome_name(outcome: Outcome) -> &'static str {
    match outcome {
        Outcome::Ready => "ready",
        Outcome::Implemented => "implemented",
        Outcome::Reviewed => "reviewed",
        Outcome::NeedsInfo => "needs_info",
        Outcome::NeedsChanges => "needs_changes",
        Outcome::NeedsHuman => "needs_human",
        Outcome::Blocked => "blocked",
        Outcome::Failed => "failed",
    }
}

pub(super) fn stage_flow_event(
    tracking: Option<&LifecycleTracking<'_>>,
    effects: &mut donkeyspace_db::lifecycle_checkpoints::Effects,
    event_type: &str,
    level: &str,
    summary: &str,
    reason: Option<&str>,
    wave: Option<u32>,
    dedupe_suffix: &str,
) {
    let Some(tracking) = tracking else {
        return;
    };
    let Some(workflow_item_id) = tracking.coordinator.workflow_item_id else {
        return;
    };
    effects.events.push(LifecycleEventInput {
        workflow_item_id,
        coordinator_job_id: Some(tracking.coordinator.id),
        job_id: Some(tracking.coordinator.id),
        dedupe_key: Some(format!(
            "{}:{event_type}:{dedupe_suffix}",
            tracking.coordinator.id
        )),
        event_type: event_type.into(),
        level: level.into(),
        source: "worker".into(),
        actor: None,
        wave: wave.map(|value| value as i32),
        attempt: None,
        role: None,
        role_display_name: None,
        task: None,
        task_display_name: None,
        work_item: None,
        status: None,
        outcome: None,
        summary: concise_event_text(summary),
        reason: reason.map(concise_event_text),
        handoff_target: None,
        links: json!([]),
    });
}

pub(super) fn failed_task_result(reason: impl Into<String>) -> Value {
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

pub(super) fn unfinished_tracked_keys(
    tracked_jobs: &BTreeMap<TaskKey, uuid::Uuid>,
    finished_jobs: &BTreeSet<TaskKey>,
) -> Vec<TaskKey> {
    tracked_jobs
        .keys()
        .filter(|key| !finished_jobs.contains(*key))
        .cloned()
        .collect()
}

pub(super) async fn fail_unfinished_tracked_jobs(
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum TrackedJobDisposition {
    KeepWaiting,
    ReplaceTerminal,
    RejectActive,
}

pub(super) fn tracked_job_disposition(status: Option<&str>) -> TrackedJobDisposition {
    match status {
        Some("waiting") => TrackedJobDisposition::KeepWaiting,
        Some("completed" | "failed" | "superseded") | None => {
            TrackedJobDisposition::ReplaceTerminal
        }
        Some(_) => TrackedJobDisposition::RejectActive,
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn ensure_waiting_tracked_job(
    tracking: &LifecycleTracking<'_>,
    effects: &mut donkeyspace_db::lifecycle_checkpoints::Effects,
    tracked_jobs: &mut BTreeMap<TaskKey, Uuid>,
    finished_jobs: &mut BTreeSet<TaskKey>,
    manifest: &PluginManifest,
    flow_name: &str,
    flow: &PluginFlow,
    key: &TaskKey,
    work_items: &[PluginWorkItem],
    issue_input: &Value,
) -> Result<Uuid, Box<dyn std::error::Error>> {
    if let Some(id) = tracked_jobs.get(key)
        && effects.waiting_children.iter().any(|child| child.id == *id)
    {
        return Ok(*id);
    }
    let existing = match tracked_jobs.get(key).copied() {
        Some(job_id) => get_job(tracking.pool, job_id).await?,
        None => None,
    };
    let disposition = if finished_jobs.contains(key) {
        TrackedJobDisposition::ReplaceTerminal
    } else {
        tracked_job_disposition(existing.as_ref().map(|job| job.status.as_str()))
    };
    match disposition {
        TrackedJobDisposition::KeepWaiting => {
            finished_jobs.remove(key);
            Ok(existing.expect("waiting disposition requires a job").id)
        }
        TrackedJobDisposition::ReplaceTerminal => {
            let job_id = stage_tracked_job(
                tracking,
                effects,
                manifest,
                flow_name,
                flow,
                key,
                work_items,
                issue_input,
            );
            tracked_jobs.insert(key.clone(), job_id);
            finished_jobs.remove(key);
            Ok(job_id)
        }
        TrackedJobDisposition::RejectActive => {
            let job = existing.expect("active disposition requires a job");
            Err(format!(
                "plugin task `{}` already has active child job `{}` in `{}` state",
                key.target(),
                job.id,
                job.status
            )
            .into())
        }
    }
}

fn stage_tracked_job(
    tracking: &LifecycleTracking<'_>,
    effects: &mut donkeyspace_db::lifecycle_checkpoints::Effects,
    manifest: &PluginManifest,
    flow_name: &str,
    flow: &PluginFlow,
    key: &TaskKey,
    work_items: &[PluginWorkItem],
    issue_input: &Value,
) -> Uuid {
    let mut input = issue_input.clone();
    let work_item = key
        .work_item
        .as_deref()
        .and_then(|id| work_items.iter().find(|item| item.id == id));
    if let Value::Object(map) = &mut input {
        let task = &flow.tasks[&key.task];
        let role = &manifest.roles[&task.role];
        map.insert(
            "plugin_execution".into(),
            json!({
                "coordinator_run_id": tracking.coordinator.id,
                "plugin_id": manifest.id,
                "flow": flow_name,
                "task": key.task,
                "task_display_name": task.display_name.as_deref().unwrap_or(&key.task),
                "role_display_name": role.display_name.as_deref().unwrap_or(&task.role),
                "work_item": work_item,
                "dependencies": task.dependencies,
            }),
        );
    }
    let job_id = Uuid::now_v7();
    effects
        .waiting_children
        .push(donkeyspace_db::lifecycle_checkpoints::WaitingChild {
            id: job_id,
            role: flow.tasks[&key.task].role.clone(),
            input,
        });
    if let Some(event) = plugin_task_event(
        Some(tracking),
        manifest,
        flow,
        key,
        Some(job_id),
        "task_waiting",
        "detail",
        Some("waiting"),
        None,
        "Waiting for dependencies.",
        None,
        None,
        None,
        None,
    ) {
        effects.events.push(event);
    }
    job_id
}
