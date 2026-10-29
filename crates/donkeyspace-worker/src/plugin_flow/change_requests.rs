//! Interpret requests against a retained checkpoint before invalidating work.
use super::*;
use donkeyspace_core::{ChangeProposal, ExecutionPlan};

type Error = Box<dyn std::error::Error>;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub(super) struct PendingChange {
    pub revision: u32,
    pub request: String,
    pub target_hint: Option<String>,
    pub base_digest: String,
    pub flow_digest: String,
    pub proposal: Option<ChangeProposal>,
    pub plan: Option<ExecutionPlan>,
    pub affected: Vec<TaskKey>,
}

impl PendingChange {
    pub fn key(&self) -> TaskKey {
        TaskKey {
            task: "change".into(),
            work_item: Some(self.revision.to_string()),
        }
    }
}

pub(super) enum ChangeResume {
    Unhandled,
    Paused(RunResult),
    Continue,
}

fn base_digest(saved: &LifecycleCheckpoint) -> Result<String, Error> {
    let value = serde_json::to_value(saved)?;
    let approvals = saved
        .pending_approvals
        .iter()
        .filter(|a| a.key.task != "change")
        .collect::<Vec<_>>();
    let value = json!({
        "tree": value.pointer("/repository_snapshot/tree_sha"),
        "plan": saved.execution_plan, "completed": saved.completed_keys,
        "approvals": approvals, "start_approved": saved.start_approved,
        "start_action": saved.start_action, "revision_targets": saved.revision_targets,
        "work_items": saved.active_work_items,
    });
    Ok(format!("{:x}", Sha256::digest(serde_json::to_vec(&value)?)))
}

fn proposal_graph(flow: &PluginFlow, items: &[PluginWorkItem]) -> TaskGraph {
    TaskGraph::for_work_items(flow, items)
}

fn validate_proposal(
    saved: &LifecycleCheckpoint,
    flow: &PluginFlow,
    items: &[PluginWorkItem],
    proposal: &ChangeProposal,
) -> Result<(Option<ExecutionPlan>, Vec<TaskKey>), Error> {
    if proposal.summary.trim().is_empty() {
        return Err("change proposal must explain the requested change".into());
    }
    let plan = proposal
        .execution_plan
        .as_ref()
        .map(|settings| {
            ExecutionPlan::resolve(
                flow,
                Some(settings.clone()),
                saved.active_work_items.clone(),
                saved.execution_plan.as_ref().map_or(1, |p| p.revision + 1),
            )
        })
        .transpose()?;
    let effective = plan
        .as_ref()
        .or(saved.execution_plan.as_ref())
        .map(|p| p.apply(flow))
        .transpose()?;
    let graph = proposal_graph(effective.as_ref().unwrap_or(flow), items);
    let mut seen = BTreeSet::new();
    let mut affected = BTreeSet::new();
    // Evidence invalidated by human edits to a PR must be revalidated even if
    // the accompanying request only changes execution settings.
    for key in &saved.revision_targets {
        if graph.keys().any(|candidate| candidate == key) {
            affected.extend(graph.affected_by(key)?);
        }
    }
    for revision in &proposal.revisions {
        if revision.instruction.trim().is_empty() || !seen.insert(&revision.target) {
            return Err("each revision needs a unique target and specific instructions".into());
        }
        let key = graph
            .keys()
            .find(|key| key.target() == revision.target)
            .ok_or_else(|| format!("unknown or excluded revision target `{}`", revision.target))?;
        affected.extend(graph.affected_by(key)?);
    }
    Ok((plan, affected.into_iter().collect()))
}

fn apply_proposal(
    saved: &mut LifecycleCheckpoint,
    flow: &PluginFlow,
    items: &[PluginWorkItem],
    decision_id: Option<&str>,
) -> Result<ResumeDecision, Error> {
    let pending = saved
        .pending_change
        .clone()
        .ok_or("no change is awaiting approval")?;
    if pending.base_digest != base_digest(saved)? || pending.flow_digest != flow_fingerprint(flow)?
    {
        return Err("the checkpoint or configured flow changed; submit the request again to review an updated proposal".into());
    }
    let proposal = pending
        .proposal
        .as_ref()
        .ok_or("answer the clarification or revise the request before approving")?;
    let (resolved, affected) = validate_proposal(saved, flow, items, proposal)?;
    if resolved != pending.plan || affected != pending.affected {
        return Err("the proposed impact changed; submit the request again".into());
    }
    let mut invalidated = BTreeSet::new();
    if let Some(mut plan) = resolved {
        plan.approved = true;
        invalidated.extend(super::plan_controls::install_plan(
            saved,
            flow,
            plan.clone(),
        )?);
        saved.previous.push(json!({"event":"execution_plan_approved", "execution_plan":plan, "human_decision_id":decision_id}));
    }
    invalidated.extend(affected);
    saved.completed_keys.retain(|k| !invalidated.contains(k));
    saved
        .pending_approvals
        .retain(|a| a.key.task != "change" && !invalidated.contains(&a.key));
    saved.revision_targets.retain(|k| !invalidated.contains(k));
    saved.revision_options.clear();
    saved.closed_projected_issues.retain(|item| {
        !invalidated
            .iter()
            .any(|k| k.work_item.as_ref() == Some(item))
    });
    for entry in &mut saved.previous {
        if invalidated.iter().any(|k| {
            entry.get("task").and_then(Value::as_str) == Some(&k.task)
                && entry.get("work_item").and_then(Value::as_str) == k.work_item.as_deref()
        }) {
            entry["superseded"] = json!(true);
        }
    }
    let effective = saved
        .execution_plan
        .as_ref()
        .map(|p| p.apply(flow))
        .transpose()?;
    let graph = proposal_graph(effective.as_ref().unwrap_or(flow), items);
    for revision in &proposal.revisions {
        let key = graph
            .keys()
            .find(|k| k.target() == revision.target)
            .unwrap()
            .clone();
        if key.task == flow.start {
            saved.start_approved = false;
            saved.start_action = StartAction::Revise;
        } else {
            saved.revision_targets.push(key.clone());
        }
        saved.previous.push(
            json!({"human_response": revision.instruction, "resume_target":key,
            "human_decision_id":decision_id, "superseded_results":invalidated}),
        );
    }
    saved.previous.push(
        json!({"event":"change_approved", "change":pending, "human_decision_id":decision_id}),
    );
    saved.pending_change = None;
    saved.flow_fingerprint = Some(flow_fingerprint(effective.as_ref().unwrap_or(flow))?);
    Ok(ResumeDecision {
        decisions: vec![(pending.key(), "approved".into())],
        invalidated: invalidated.into_iter().collect(),
    })
}

fn request_command() -> String {
    format!("{} revise", active_facade().issue_command())
}

pub(super) fn change_review(saved: &LifecycleCheckpoint) -> RunResult {
    let change = saved.pending_change.as_ref().expect("pending change");
    let proposal = change.proposal.as_ref().expect("validated change proposal");
    let mut lines = vec![format!(
        "**Proposed change {}:** {}",
        change.revision,
        proposal.summary.trim()
    )];
    if let Some(plan) = &change.plan {
        let mut changed = Vec::new();
        for (name, task) in &plan.tasks {
            let old = saved
                .execution_plan
                .as_ref()
                .and_then(|p| p.tasks.get(name));
            if old.is_none_or(|t| t.included != task.included || t.approval != task.approval) {
                changed.push(format!(
                    "- {}: {}{}.",
                    task.display_name,
                    if task.included {
                        "included"
                    } else {
                        "excluded; not validated by this run"
                    },
                    if task.included && task.approval == PluginApprovalMode::Required {
                        "; approval required afterward"
                    } else {
                        ""
                    }
                ));
            }
        }
        if changed.is_empty() {
            changed.push("- Execution-stage selection and approval gates stay the same.".into());
        }
        lines.push(changed.join("\n"));
    }
    if change.affected.is_empty() {
        lines.push("**No completed task needs to rerun.** Current artifacts and their review state are retained.".into());
    } else {
        lines.push(format!(
            "**Run or rerun after approval:** {}.",
            change
                .affected
                .iter()
                .map(|k| format!("`{}`", k.target()))
                .collect::<Vec<_>>()
                .join(", ")
        ));
        let retained = saved
            .completed_keys
            .iter()
            .chain(saved.pending_approvals.iter().map(|a| &a.key))
            .filter(|k| k.task != "change" && !change.affected.contains(k))
            .map(|k| format!("`{}`", k.target()))
            .collect::<BTreeSet<_>>();
        if !retained.is_empty() {
            lines.push(format!(
                "**Retained:** {}.",
                retained.into_iter().collect::<Vec<_>>().join(", ")
            ));
        }
    }
    let waiting = saved
        .pending_approvals
        .iter()
        .filter(|a| a.key.task != "change" && !change.affected.contains(&a.key))
        .filter(|a| {
            change
                .plan
                .as_ref()
                .is_none_or(|p| p.tasks.get(&a.key.task).is_none_or(|t| t.included))
        })
        .map(|a| format!("`{}`", a.key.target()))
        .collect::<Vec<_>>();
    if !waiting.is_empty() {
        lines.push(format!(
            "Approving this change leaves these reviews pending: {}.",
            waiting.join(", ")
        ));
    }
    lines.push(format!("To accept this exact proposal, comment:\n```text\n{}\n```\nTo change it, describe what you want:\n```text\n{}\nDescribe the desired change.\n```", change.key().approval_commands(active_facade()).approve, request_command()));
    RunResult {
        outcome: Outcome::NeedsHuman,
        summary: format!(
            "Review proposed change {}: {}",
            change.revision,
            proposal.summary.trim()
        ),
        confidence: Confidence::High,
        risk: saved.aggregate_risk,
        questions: vec![],
        tests: vec![],
        changed_files: vec![],
        human_review_reason: Some(lines.join("\n\n")),
        blocked_reason: None,
    }
}

fn rejected_review(saved: &LifecycleCheckpoint, message: &str) -> RunResult {
    let mut result = if saved
        .pending_change
        .as_ref()
        .is_some_and(|c| c.proposal.is_some())
    {
        change_review(saved)
    } else {
        RunResult {
            outcome: Outcome::NeedsHuman,
            summary: "The requested change needs clarification.".into(),
            confidence: Confidence::High,
            risk: saved.aggregate_risk,
            questions: vec![],
            tests: vec![],
            changed_files: vec![],
            human_review_reason: Some(format!(
                "Your retained work is unchanged. Submit a new request with `{}` followed by what you want to change.",
                request_command()
            )),
            blocked_reason: None,
        }
    };
    result.human_review_reason = Some(format!(
        "{message}\n\n{}",
        result.human_review_reason.unwrap_or_default()
    ));
    result
}

fn decision_id(input: &Value) -> Option<String> {
    input
        .pointer("/comment/id")
        .map(|id| format!("comment:{id}"))
        .or_else(|| {
            input
                .pointer("/donkeyspace_ingress/delivery_id")
                .and_then(Value::as_str)
                .map(|id| format!("delivery:{id}"))
        })
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn resume(
    saved: &mut LifecycleCheckpoint,
    selection: &PluginFlowSelection,
    manifest: &PluginManifest,
    flow: &PluginFlow,
    repo: &Path,
    workspace: &Path,
    input: &Value,
    tracking: Option<&LifecycleTracking<'_>>,
    parameters: &BTreeMap<String, Value>,
    plugin_root: &Path,
    store: &mut CheckpointStore<'_, '_>,
) -> Result<ChangeResume, Error> {
    if !flow.change_intake {
        if saved.pending_change.is_some() {
            return Err("change intake is disabled while a proposal is pending; restore the configured flow".into());
        }
        return Ok(ChangeResume::Unhandled);
    }
    let decision = input
        .get("donkeyspace_human_decision")
        .map(|v| serde_json::from_value::<HumanDecision>(v.clone()))
        .transpose()?;
    let clarification = saved
        .pending_change
        .as_ref()
        .is_some_and(|c| c.proposal.is_none());
    let is_revision = matches!(&decision, Some(HumanDecision::Revise { .. }));
    let handles_approval = saved.pending_change.is_some()
        || matches!(&decision,
        Some(HumanDecision::Approve { target: Some(target) }) if target.starts_with("change/"));
    if !is_revision && !handles_approval && !(clarification && decision.is_none()) {
        return Ok(ChangeResume::Unhandled);
    }
    let id = decision_id(input);
    if id.as_ref().is_some_and(|id| {
        saved
            .previous
            .iter()
            .any(|v| v.get("human_decision_id").and_then(Value::as_str) == Some(id.as_str()))
    }) {
        if let Some(tracking) = tracking {
            crate::stop_role_job(
                tracking.pool,
                tracking.coordinator,
                &saved.last_result,
                true,
                "duplicate change decision ignored",
            )
            .await?;
        }
        return Ok(ChangeResume::Paused(finish_result(
            saved.last_result.clone(),
            saved.accumulated_tests.clone(),
            &saved.previous,
        )));
    }
    let registry: PluginWorkItemRegistry = serde_json::from_str(&fs::read_to_string(
        repo.join(expand_template(
            flow.work_items_path
                .as_deref()
                .ok_or("missing work-item registry")?,
            parameters,
        )?),
    )?)?;
    validate_work_items(&registry.work_items)?;
    let items = select_lifecycle_work_items(&registry.work_items, Some(&saved.active_work_items))?;
    if let Some(HumanDecision::Approve { target }) = &decision {
        let expected = saved.pending_change.as_ref().map(|p| p.key().target());
        let applied = if target.is_some() && target == &expected {
            apply_proposal(saved, flow, &items, id.as_deref())
        } else {
            Err("approve the displayed change proposal explicitly; artifact approvals remain separate".into())
        };
        match applied {
            Ok(mut effects) => {
                store.effects.decisions.append(&mut effects.decisions);
                store
                    .effects
                    .invalidated_tasks
                    .append(&mut effects.invalidated);
                let effective = saved
                    .execution_plan
                    .as_ref()
                    .map(|p| p.apply(flow))
                    .transpose()?;
                let effective = effective.as_ref().unwrap_or(flow);
                if saved.pending_approvals.is_empty() {
                    saved.last_result = RunResult {
                        outcome: Outcome::Implemented,
                        summary: "Approved change applied.".into(),
                        confidence: saved.aggregate_confidence,
                        risk: saved.aggregate_risk,
                        questions: vec![],
                        tests: vec![],
                        changed_files: vec![],
                        human_review_reason: None,
                        blocked_reason: None,
                    };
                    store.save(saved, effective, false, false).await?;
                    return Ok(ChangeResume::Continue);
                }
                saved.last_result = pending_approval_result(
                    &saved.pending_approvals,
                    &saved.projected_issues,
                    effective,
                    "Change approved. Existing artifact reviews remain pending.",
                );
            }
            Err(error) => {
                saved.last_result =
                    if saved.pending_change.is_none() && !saved.pending_approvals.is_empty() {
                        pending_approval_result(
                            &saved.pending_approvals,
                            &saved.projected_issues,
                            flow,
                            &format!("The decision was not applied: {error}"),
                        )
                    } else {
                        rejected_review(saved, &format!("The decision was not applied: {error}"))
                    };
            }
        }
    } else {
        let (target, feedback) = match decision {
            Some(HumanDecision::Revise { target, feedback }) if !feedback.trim().is_empty() => {
                (target, feedback)
            }
            None if clarification => {
                let prior = saved.pending_change.as_ref().unwrap();
                (
                    prior.target_hint.clone(),
                    format!(
                        "{}\n\nAdditional answer:\n{}",
                        prior.request,
                        input
                            .pointer("/comment/body")
                            .and_then(Value::as_str)
                            .unwrap_or("")
                    ),
                )
            }
            _ => return Ok(ChangeResume::Unhandled),
        };
        let current = saved
            .execution_plan
            .as_ref()
            .map(|p| p.apply(flow))
            .transpose()?;
        let current = current.as_ref().unwrap_or(flow);
        let graph = proposal_graph(flow, &items);
        let targets = graph.keys().map(|key| json!({"target":key.target(), "task":key.task, "work_item":key.work_item,
            "included":current.tasks.contains_key(&key.task), "completed":saved.completed_keys.contains(key), "awaiting_approval":saved.pending_approvals.iter().any(|a| a.key == *key)})).collect::<Vec<_>>();
        let mut intake_input = input.clone();
        intake_input["donkeyspace_change_request"] = json!({"request":feedback,"target_hint":target,
            "execution_plan":saved.execution_plan,"specification_approved":saved.start_approved,
            "available_targets":targets,"work_items":items,"pending_reviews":saved.pending_approvals,
            "previous_proposal":saved.pending_change.as_ref().and_then(|c| c.proposal.as_ref())});
        let mut task = flow.tasks[&flow.start].clone();
        task.read.extend(task.write.clone());
        task.write.clear();
        task.artifacts.clear();
        task.preserve_on_success.clear();
        task.validators.clear();
        task.diagnostics.clear();
        task.allowed_handoffs.clear();
        saved.attempt += 1;
        let execution = execute_task(
            selection,
            manifest,
            &flow.start,
            &task,
            None,
            saved.attempt,
            repo,
            workspace,
            &intake_input,
            &saved.previous,
            parameters,
            plugin_root,
        )
        .await;
        if let Some(old) = saved.pending_change.take() {
            store.effects.invalidated_tasks.push(old.key());
        }
        saved.pending_approvals.retain(|a| a.key.task != "change");
        saved.change_sequence += 1;
        let mut pending = PendingChange {
            revision: saved.change_sequence,
            request: feedback,
            target_hint: target,
            base_digest: base_digest(saved)?,
            flow_digest: flow_fingerprint(flow)?,
            proposal: None,
            plan: None,
            affected: vec![],
        };
        let result = match execution {
            Ok(result) if result.result.outcome == Outcome::Implemented => {
                match result
                    .change_proposal
                    .ok_or_else(|| "intake did not return a change proposal".into())
                    .and_then(|proposal| {
                        validate_proposal(saved, flow, &items, &proposal)
                            .map(|(plan, affected)| (proposal, plan, affected))
                    }) {
                    Ok((proposal, plan, affected)) => {
                        pending.proposal = Some(proposal);
                        pending.plan = plan;
                        pending.affected = affected;
                        None
                    }
                    Err(error) => Some(rejected_review(
                        saved,
                        &format!("The proposed change could not be validated: {error}"),
                    )),
                }
            }
            Ok(result) if result.result.outcome == Outcome::NeedsInfo => Some(result.result),
            Ok(result) => Some(rejected_review(
                saved,
                result
                    .result
                    .human_review_reason
                    .as_deref()
                    .or(result.result.blocked_reason.as_deref())
                    .unwrap_or(&result.result.summary),
            )),
            Err(error) => Some(rejected_review(
                saved,
                &format!("Change intake could not complete: {error}"),
            )),
        };
        if pending.proposal.is_some() {
            saved.pending_approvals.push(PendingApproval {
                key: pending.key(),
                trigger: ApprovalTrigger::Required,
            });
        }
        saved
            .previous
            .push(json!({"event":"change_proposed", "change":pending, "human_decision_id":id}));
        saved.pending_change = Some(pending);
        saved.last_result = result.unwrap_or_else(|| change_review(saved));
    }
    let effective = saved
        .execution_plan
        .as_ref()
        .map(|p| p.apply(flow))
        .transpose()?;
    store
        .save(saved, effective.as_ref().unwrap_or(flow), true, false)
        .await?;
    Ok(ChangeResume::Paused(finish_result(
        saved.last_result.clone(),
        saved.accumulated_tests.clone(),
        &saved.previous,
    )))
}

#[cfg(test)]
mod tests;
