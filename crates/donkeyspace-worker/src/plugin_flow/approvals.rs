use super::*;

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(super) enum ApprovalTrigger {
    Required,
    AgentRequested,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub(super) struct PendingApproval {
    pub(super) key: TaskKey,
    pub(super) trigger: ApprovalTrigger,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case")]
pub(super) enum HumanDecision {
    Approve {
        target: Option<String>,
    },
    Revise {
        target: Option<String>,
        feedback: String,
    },
}

pub(super) fn normalize_handoff_target(
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

pub(super) fn select_pending_approvals(
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
            .find(|approval| approval.key.target() == target)
            .cloned()
            .map(|approval| vec![approval])
            .ok_or_else(|| format!("no pending approval matches `{target}`").into());
    }
    if pending.len() == 1 {
        return Ok(pending.to_vec());
    }
    Err("an approval target is required when multiple tasks are pending".into())
}

pub(super) async fn approval_requests(
    tracking: Option<&LifecycleTracking<'_>>,
    pending: &[PendingApproval],
    projected_issues: &BTreeMap<String, i64>,
    flow: &PluginFlow,
    result: &RunResult,
) -> Result<Vec<ApprovalRequestInput>, Box<dyn std::error::Error>> {
    let Some(tracking) = tracking else {
        return Ok(Vec::new());
    };
    let Some(workflow_item_id) = tracking.coordinator.workflow_item_id else {
        return Ok(Vec::new());
    };
    let publication = list_agent_publications_for_run(
        tracking.pool,
        tracking.coordinator.id,
        Some(tracking.coordinator.id),
    )
    .await?
    .into_iter()
    .filter(|publication| publication.kind == "checkpoint" && publication.status == "published")
    .max_by_key(|publication| publication.id);
    let projected = projected_issues
        .iter()
        .map(|(work_item, number)| json!({"work_item": work_item, "number": number}))
        .collect::<Vec<_>>();
    let mut requests = Vec::new();
    for approval in pending {
        let task = flow
            .tasks
            .get(&approval.key.task)
            .unwrap_or(&flow.tasks[&flow.start]);
        let downstream = flow
            .tasks
            .iter()
            .filter(|(_, candidate)| candidate.dependencies.contains(&approval.key.task))
            .map(|(name, _)| name)
            .collect::<Vec<_>>();
        requests.push(ApprovalRequestInput {
            workflow_item_id,
            coordinator_job_id: tracking.coordinator.id,
            target_task: approval.key.task.clone(),
            target_work_item: approval.key.work_item.clone(),
            purpose: "accept_result".into(),
            trigger: match approval.trigger {
                ApprovalTrigger::Required => "required",
                ApprovalTrigger::AgentRequested => "agent_requested",
            }
            .into(),
            approval_subject: if approval.key.task == "change" && flow.change_intake {
                "proposed change and displayed rerun scope; other artifact approvals remain separate".into()
            } else if approval.key.task == "execution-plan" {
                "execution plan only; existing specification approval is retained".into()
            } else {
                task.approval_subject.clone().unwrap_or_else(|| {
                    approval.key.work_item.as_ref().map_or_else(
                        || format!("{} result", approval.key.task),
                        |work_item| format!("{} result for {work_item}", approval.key.task),
                    )
                })
            },
            result_summary: result.summary.clone(),
            changed_files: publication.as_ref().map_or_else(
                || json!([]),
                |publication| publication.changed_files.clone(),
            ),
            proposed_publication_id: publication.as_ref().map(|publication| publication.id),
            projected_issues: json!(projected),
            downstream_tasks: json!(downstream),
        });
    }
    Ok(requests)
}

pub(super) fn pending_approval_result(
    pending: &[PendingApproval],
    projected_issues: &BTreeMap<String, i64>,
    flow: &PluginFlow,
    lead: &str,
) -> RunResult {
    if flow.change_intake {
        let prefix = active_facade().issue_command();
        let mut lines = vec![lead.to_string()];
        for approval in pending {
            let target = approval.key.target();
            let label = if approval.key.task == "execution-plan" {
                "execution plan".to_string()
            } else {
                flow.tasks
                    .get(&approval.key.task)
                    .and_then(|t| t.display_name.clone())
                    .unwrap_or_else(|| approval.key.task.clone())
            };
            let item = approval
                .key
                .work_item
                .as_deref()
                .map(|item| format!(" for `{item}`"))
                .unwrap_or_default();
            let effect = if matches!(approval.trigger, ApprovalTrigger::AgentRequested) {
                "retry this task"
            } else {
                "accept this output and release its dependent work"
            };
            lines.push(format!(
                "- **{label}{item}:** `{prefix} approve {target}` to {effect}."
            ));
        }
        let relevant = projected_issues
            .iter()
            .filter(|(item, _)| {
                pending
                    .iter()
                    .any(|a| a.key.work_item.as_ref() == Some(item) || a.key.task == flow.start)
            })
            .map(|(item, number)| format!("`{item}`: #{number}"))
            .collect::<Vec<_>>();
        if !relevant.is_empty() {
            lines.push(format!("Review: {}.", relevant.join(", ")));
        }
        lines.push(format!("**Want a change?** Describe changes to the plan, implementation, or requirements:\n```text\n{prefix} revise\nDescribe what you want to change.\n```\nThe planner will propose the scope before completed work is rerun."));
        return RunResult {
            outcome: Outcome::NeedsHuman,
            summary: format!("Awaiting {} review(s).", pending.len()),
            confidence: Confidence::High,
            risk: Risk::Unknown,
            questions: vec![],
            tests: vec![],
            changed_files: vec![],
            human_review_reason: Some(lines.join("\n\n")),
            blocked_reason: None,
        };
    }
    let start_task = &flow.start;
    let subjects = pending
        .iter()
        .map(|approval| {
            let target = approval.key.target();
            if approval.key.task == "execution-plan" {
                return "- `execution-plan`: Review the execution plan. Approving accepts these scheduling choices; the existing specification approval is retained. Revising changes only this plan.".into();
            }
            let configured_subject = flow.tasks[&approval.key.task]
                .approval_subject
                .as_deref();
            let artifact = match (&approval.key.work_item, configured_subject) {
                (Some(work_item), Some(subject)) => {
                    format!("the {subject} for work item `{work_item}`")
                }
                (None, Some(subject)) => format!("the {subject}"),
                (Some(work_item), None) => format!(
                    "the completed `{}` output for work item `{work_item}`",
                    approval.key.task
                ),
                (None, None) if approval.key.task == *start_task && !projected_issues.is_empty() => {
                    "the proposed lifecycle plan and block specifications in the projected work-item issues listed below".to_string()
                }
                (None, None) => format!("the completed workflow-level `{}` output", approval.key.task),
            };
            let consequence = match approval.trigger {
                ApprovalTrigger::Required => {
                    "Approving accepts this output as the current checkpoint and authorizes its dependent agent tasks to run. Revising keeps those dependents blocked and reruns this target with your feedback."
                }
                ApprovalTrigger::AgentRequested => {
                    "Approving authorizes this target to rerun from the preserved checkpoint without additional feedback. Revising reruns it with the feedback you provide."
                }
            };
            format!("- `{target}`: Review {artifact}. {consequence}")
        })
        .collect::<Vec<_>>()
        .join("\n");
    let include_all_issues = pending
        .iter()
        .any(|approval| approval.key.work_item.is_none() && approval.key.task == *start_task);
    let pending_work_items = pending
        .iter()
        .filter_map(|approval| approval.key.work_item.as_deref())
        .collect::<BTreeSet<_>>();
    let relevant_issues = projected_issues
        .iter()
        .filter(|(item, _)| include_all_issues || pending_work_items.contains(item.as_str()))
        .collect::<Vec<_>>();
    let review_issues = if relevant_issues.is_empty() {
        String::new()
    } else {
        format!(
            "\n\nReview the projected work-item issues:\n{}",
            relevant_issues
                .into_iter()
                .map(|(item, number)| format!("- `{item}`: #{number}"))
                .collect::<Vec<_>>()
                .join("\n")
        )
    };
    let mut commands = pending.iter().map(|approval| {
        let commands = approval.key.approval_commands(active_facade());
        if approval.key.task == "execution-plan" {
            return format!("To accept this execution plan, comment:\n`{}`\n\nTo change it, use `{}` with YAML fields `excluded_tasks`, `approval_tasks`, and `reason` on subsequent lines. Other artifact approvals remain separate.", commands.approve, commands.revise);
        }
        format!("To accept, comment:\n`{}`\n\nTo request changes, comment and put specific feedback on the following lines:\n`{}`\n`<describe the required changes>`", commands.approve, commands.revise)
    }).collect::<Vec<_>>().join("\n\n");
    if pending.len() > 1 && !pending.iter().any(|a| a.key.task == "execution-plan") {
        commands.push_str(&format!(
            "\n\nAccept every approval subject with `{} approve all`.",
            active_facade().issue_command()
        ));
    }
    RunResult {
        outcome: Outcome::NeedsHuman,
        summary: format!("Awaiting approval for {} task(s).", pending.len()),
        confidence: Confidence::High,
        risk: Risk::Unknown,
        questions: Vec::new(),
        tests: Vec::new(),
        changed_files: Vec::new(),
        human_review_reason: Some(format!(
            "{lead}\n\nApproval subjects:\n{subjects}{review_issues}\n\nDecision instructions:\n{commands}"
        )),
        blocked_reason: None,
    }
}

/// Pure transition: selecting one target cannot invalidate completed siblings.
#[derive(Debug, Default)]
pub(super) struct ResumeDecision {
    pub(super) decisions: Vec<(TaskKey, String)>,
    pub(super) invalidated: Vec<TaskKey>,
}

pub(super) fn flow_fingerprint(flow: &PluginFlow) -> Result<String, serde_json::Error> {
    Ok(format!("{:x}", Sha256::digest(serde_json::to_vec(flow)?)))
}

pub(super) fn apply_human_decision(
    saved: &mut LifecycleCheckpoint,
    flow: &PluginFlow,
    decision_value: &Value,
) -> Result<ResumeDecision, Box<dyn std::error::Error>> {
    let base_flow = flow;
    let decision: HumanDecision = serde_json::from_value(decision_value.clone())?;
    if matches!(&decision, HumanDecision::Revise { feedback, .. } if feedback.trim().is_empty()) {
        return Err("revision feedback must not be empty".into());
    }
    let decision_id = decision_value.get("decision_id").and_then(Value::as_str);
    if decision_id.is_some_and(|id| {
        saved
            .previous
            .iter()
            .any(|entry| entry.get("human_decision_id").and_then(Value::as_str) == Some(id))
    }) {
        return Ok(ResumeDecision::default());
    }
    if let Some(result) =
        super::plan_controls::apply_plan_decision(saved, base_flow, &decision, decision_id)?
    {
        return Ok(result);
    }
    let effective = saved
        .execution_plan
        .as_ref()
        .map(|p| p.apply(base_flow))
        .transpose()?;
    let flow = effective.as_ref().unwrap_or(base_flow);
    let mut invalidated = Vec::new();
    let selected = match select_pending_approvals(&saved.pending_approvals, &decision) {
        Ok(selected) => selected,
        Err(error) => {
            let HumanDecision::Revise {
                target: Some(target),
                ..
            } = &decision
            else {
                return Err(error);
            };
            if matches!(
                saved.last_result.outcome,
                Outcome::Failed | Outcome::Blocked
            ) {
                let items = saved
                    .active_work_items
                    .iter()
                    .map(|id| PluginWorkItem {
                        id: id.clone(),
                        spec: String::new(),
                        depends_on: vec![],
                        metadata: BTreeMap::new(),
                    })
                    .collect::<Vec<_>>();
                let graph = TaskGraph::for_work_items(flow, &items);
                let key = graph
                    .keys()
                    .find(|key| key.target() == *target)
                    .cloned()
                    .ok_or("unknown revision target")?;
                saved.pending_approvals.push(PendingApproval {
                    key,
                    trigger: ApprovalTrigger::AgentRequested,
                });
                saved.last_result.outcome = Outcome::NeedsHuman;
                return apply_human_decision(saved, base_flow, decision_value);
            }
            if saved.last_result.outcome != Outcome::NeedsHuman {
                return Err(
                    "completed work can only be revised from a human decision pause".into(),
                );
            }
            if saved.flow_fingerprint.as_deref() != Some(flow_fingerprint(flow)?.as_str()) {
                return Err("the saved revision graph does not match the active flow; restore the original plugin or start a new approved run".into());
            }
            let option = saved
                .revision_options
                .iter()
                .find(|option| option.target.target() == *target)
                .ok_or_else(|| {
                    format!(
                        "`{target}` is not a completed upstream task relevant to the current pause"
                    )
                })?;
            if !saved.completed_keys.contains(&option.target)
                || !saved
                    .pending_approvals
                    .iter()
                    .any(|approval| option.affected.contains(&approval.key))
            {
                return Err(
                    "the requested upstream revision is no longer relevant to this pause".into(),
                );
            }
            invalidated = option.affected.clone();
            vec![PendingApproval {
                key: option.target.clone(),
                trigger: ApprovalTrigger::Required,
            }]
        }
    };
    let mut decisions = Vec::new();
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
                saved.start_action = StartAction::Accept;
                if let Some(plan) = &mut saved.execution_plan {
                    plan.approved = true;
                    saved
                        .previous
                        .push(json!({"event":"execution_plan_approved", "execution_plan":plan}));
                }
            }
            (HumanDecision::Approve { .. }, ApprovalTrigger::Required, false) => {
                if !saved.completed_keys.contains(&approval.key) {
                    saved.completed_keys.push(approval.key.clone());
                }
            }
            (HumanDecision::Revise { .. }, _, true) => {
                saved.start_approved = false;
                saved.start_action = StartAction::Revise;
            }
            (HumanDecision::Revise { .. }, _, false)
            | (HumanDecision::Approve { .. }, ApprovalTrigger::AgentRequested, false) => {
                if !saved.revision_targets.contains(&approval.key) {
                    saved.revision_targets.push(approval.key.clone());
                }
            }
            (HumanDecision::Approve { .. }, ApprovalTrigger::AgentRequested, true) => {
                saved.start_action = StartAction::Revise;
            }
        }
        saved.previous.push(json!({
            "human_response": feedback,
            "human_decision": decision_value,
            "human_decision_id": decision_id,
            "resume_target": approval.key,
            "superseded_results": invalidated,
        }));
        decisions.push((
            approval.key.clone(),
            match decision {
                HumanDecision::Approve { .. } => "approved".into(),
                HumanDecision::Revise { .. } => "revised".into(),
            },
        ));
    }
    saved.pending_approvals.retain(|approval| {
        !selected_keys.contains(&approval.key) && !invalidated.contains(&approval.key)
    });
    saved
        .completed_keys
        .retain(|key| !invalidated.contains(key));
    saved
        .revision_targets
        .retain(|key| !invalidated.contains(key) || selected_keys.contains(key));
    for entry in &mut saved.previous {
        if invalidated.iter().any(|key| {
            entry.get("task").and_then(Value::as_str) == Some(&key.task)
                && entry.get("work_item").and_then(Value::as_str) == key.work_item.as_deref()
        }) {
            entry["superseded"] = json!(true);
        }
    }
    saved.closed_projected_issues.retain(|item| {
        !invalidated
            .iter()
            .any(|key| key.work_item.as_ref() == Some(item))
    });
    saved.revision_options.retain(|option| {
        !invalidated.contains(&option.target)
            && saved
                .pending_approvals
                .iter()
                .any(|approval| option.affected.contains(&approval.key))
    });

    Ok(ResumeDecision {
        decisions,
        invalidated,
    })
}

/// Apply or reject one authorized decision before executing any task. Rejections
/// and redeliveries pause without rewriting the authoritative checkpoint.
pub(super) async fn resume_human_decision(
    saved: &mut LifecycleCheckpoint,
    flow: &PluginFlow,
    issue_input: &Value,
    tracking: Option<&LifecycleTracking<'_>>,
    store: &mut CheckpointStore<'_, '_>,
) -> Result<Option<RunResult>, Box<dyn std::error::Error>> {
    let mut decision = issue_input
        .pointer("/donkeyspace_human_decision")
        .ok_or("resumed approval checkpoint is missing a human decision")?
        .clone();
    let decision_id = issue_input
        .pointer("/comment/id")
        .map(|id| format!("comment:{id}"))
        .or_else(|| {
            issue_input
                .pointer("/donkeyspace_ingress/delivery_id")
                .and_then(Value::as_str)
                .map(|id| format!("delivery:{id}"))
        });
    if let Some(id) = &decision_id {
        decision["decision_id"] = json!(id);
    }
    let (mut resume, lead) = match apply_human_decision(saved, flow, &decision) {
        Ok(applied) => {
            if applied.decisions.is_empty() {
                if let Some(tracking) = tracking {
                    crate::stop_role_job(
                        tracking.pool,
                        tracking.coordinator,
                        &saved.last_result,
                        true,
                        "duplicate human decision ignored; checkpoint unchanged",
                    )
                    .await?;
                }
                return Ok(Some(finish_result(
                    saved.last_result.clone(),
                    saved.accumulated_tests.clone(),
                    &saved.previous,
                )));
            }
            (applied, "Some approvals remain pending.".to_string())
        }
        Err(error) => {
            let result = pending_approval_result(
                &saved.pending_approvals,
                &saved.projected_issues,
                flow,
                &format!("The decision was not applied: {error}"),
            );
            if let Some(tracking) = tracking {
                crate::stop_role_job(
                    tracking.pool,
                    tracking.coordinator,
                    &result,
                    true,
                    "human decision rejected; checkpoint unchanged",
                )
                .await?;
            }
            return Ok(Some(finish_result(
                result,
                saved.accumulated_tests.clone(),
                &saved.previous,
            )));
        }
    };
    store.effects.decisions.append(&mut resume.decisions);
    if !resume.invalidated.is_empty() {
        stage_flow_event(
            tracking,
            &mut store.effects,
            "upstream_revision_applied",
            "milestone",
            &format!(
                "Human revision superseded: {}.",
                resume
                    .invalidated
                    .iter()
                    .map(TaskKey::target)
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
            decision.get("feedback").and_then(Value::as_str),
            Some(saved.attempt),
            decision_id
                .as_deref()
                .unwrap_or(&format!("revision-{}", saved.attempt)),
        );
        if let Some(event) = store.effects.events.last_mut() {
            event.actor = issue_input
                .pointer("/donkeyspace_engagement/actor/login")
                .and_then(Value::as_str)
                .map(str::to_owned);
        }
        store
            .effects
            .invalidated_tasks
            .append(&mut resume.invalidated);
    }
    let effective = saved
        .execution_plan
        .as_ref()
        .map(|p| p.apply(flow))
        .transpose()?;
    let flow = effective.as_ref().unwrap_or(flow);
    if !saved.pending_approvals.is_empty() {
        saved.last_result = pending_approval_result(
            &saved.pending_approvals,
            &saved.projected_issues,
            flow,
            &lead,
        );
        store.save(saved, flow, true, false).await?;
        return Ok(Some(finish_result(
            saved.last_result.clone(),
            saved.accumulated_tests.clone(),
            &saved.previous,
        )));
    }
    store.save(saved, flow, false, false).await?;
    Ok(None)
}

#[cfg(test)]
mod revision_tests {
    use super::*;

    fn paused(pending_sibling: bool) -> (PluginFlow, LifecycleCheckpoint) {
        let flow: PluginFlow =
            serde_json::from_value(json!({"start":"plan", "replaces_default_lifecycle":true,
            "work_items_path":"items.json", "tasks":{
                "plan":{"role":"plan", "approval":"required"},
                "build":{"role":"build","scope":"work_item","dependencies":["plan"]},
                "check":{"role":"check","scope":"work_item","dependencies":["build"]},
                "unrelated":{"role":"other"}}}))
            .unwrap();
        let items: Vec<PluginWorkItem> = serde_json::from_value(json!([
            {"id":"left","spec":"left.md"},{"id":"right","spec":"right.md"}]))
        .unwrap();
        let mut graph = TaskGraph::for_work_items(&flow, &items);
        let pending = TaskKey {
            task: "check".into(),
            work_item: Some("left".into()),
        };
        for key in graph.keys().cloned().collect::<Vec<_>>() {
            if key != pending && !(pending_sibling && key.target() == "check/right") {
                graph.mark_completed(&key).unwrap();
            }
        }
        let result = crate::role_failure_result("review", "specification needs changing");
        let mut saved = LifecycleCheckpoint::capture(
            3,
            &[],
            &[],
            Risk::Low,
            Confidence::High,
            &RunResult {
                outcome: Outcome::NeedsHuman,
                ..result
            },
            &graph,
            &BTreeMap::new(),
            &BTreeMap::new(),
            &BTreeMap::new(),
            &BTreeSet::new(),
            if pending_sibling {
                vec![
                    PendingApproval {
                        key: pending,
                        trigger: ApprovalTrigger::AgentRequested,
                    },
                    PendingApproval {
                        key: TaskKey {
                            task: "check".into(),
                            work_item: Some("right".into()),
                        },
                        trigger: ApprovalTrigger::Required,
                    },
                ]
            } else {
                vec![PendingApproval {
                    key: pending,
                    trigger: ApprovalTrigger::AgentRequested,
                }]
            },
            true,
            &items,
        );
        saved.flow_fingerprint = Some(flow_fingerprint(&flow).unwrap());
        (flow, saved)
    }

    #[test]
    fn completed_revision_preserves_siblings_and_rejects_invalid_commands_without_mutation() {
        let (flow, mut saved) = paused(false);
        assert_eq!(
            saved
                .revision_options
                .iter()
                .map(|option| option.target.target())
                .collect::<Vec<_>>(),
            ["plan", "build/left"]
        );
        for decision in [
            json!({"action":"approve","target":"plan"}),
            json!({"action":"revise","target":"build","feedback":"fix"}),
            json!({"action":"revise","target":"plan/left","feedback":"fix"}),
            json!({"action":"revise","target":"unrelated","feedback":"fix"}),
            json!({"action":"revise","target":"build/right","feedback":"fix"}),
            json!({"action":"revise","target":"missing","feedback":"fix"}),
            json!({"action":"revise","target":"all","feedback":"fix"}),
            json!({"action":"revise","target":"plan","feedback":"  "}),
        ] {
            let before = serde_json::to_value(&saved).unwrap();
            assert!(
                apply_human_decision(&mut saved, &flow, &decision).is_err(),
                "{decision}"
            );
            assert_eq!(serde_json::to_value(&saved).unwrap(), before);
        }
        let decision = json!({"action":"revise","target":"build/left","feedback":"repair the implementation","decision_id":"comment:1"});
        let applied = apply_human_decision(&mut saved, &flow, &decision).unwrap();
        assert_eq!(
            applied
                .invalidated
                .iter()
                .map(TaskKey::target)
                .collect::<Vec<_>>(),
            ["build/left", "check/left"]
        );
        assert!(
            saved
                .completed_keys
                .iter()
                .any(|key| key.target() == "check/right")
        );
        assert!(saved.start_approved);
        assert!(saved.pending_approvals.is_empty());
        assert_eq!(saved.revision_targets[0].target(), "build/left");
        let after = serde_json::to_value(&saved).unwrap();
        assert!(
            apply_human_decision(&mut saved, &flow, &decision)
                .unwrap()
                .invalidated
                .is_empty()
        );
        assert_eq!(serde_json::to_value(&saved).unwrap(), after);
    }

    #[test]
    fn upstream_revision_preserves_an_unrelated_pending_approval() {
        let (flow, mut saved) = paused(true);
        apply_human_decision(
            &mut saved,
            &flow,
            &json!({"action":"revise","target":"build/left","feedback":"repair"}),
        )
        .unwrap();
        assert_eq!(saved.pending_approvals.len(), 1);
        assert_eq!(saved.pending_approvals[0].key.target(), "check/right");
        assert!(
            saved
                .completed_keys
                .iter()
                .any(|key| key.target() == "build/right")
        );
        assert_eq!(saved.revision_targets[0].target(), "build/left");
    }

    #[test]
    fn completed_plan_revision_renews_approval_and_preserves_unrelated_work() {
        let (flow, mut saved) = paused(false);
        let decision = json!({"action":"revise","target":"plan","feedback":"change the contract"});
        let mut changed = flow.clone();
        changed.tasks.get_mut("build").unwrap().dependencies.clear();
        assert!(apply_human_decision(&mut saved, &changed, &decision).is_err());
        let applied = apply_human_decision(&mut saved, &flow, &decision).unwrap();
        assert_eq!(applied.invalidated.len(), 5);
        assert_eq!(
            saved
                .completed_keys
                .iter()
                .map(TaskKey::target)
                .collect::<Vec<_>>(),
            ["unrelated"]
        );
        assert!(!saved.start_approved);
        assert_eq!(saved.start_action, StartAction::Revise);
        assert!(saved.pending_approvals.is_empty());
    }

    #[test]
    fn non_opted_in_flow_can_approve_a_real_execution_plan_task() {
        let (mut flow, mut saved) = paused(false);
        let mut task = flow.tasks["check"].clone();
        task.scope = PluginTaskScope::Workflow;
        flow.tasks.insert("execution-plan".into(), task);
        let key = TaskKey {
            task: "execution-plan".into(),
            work_item: None,
        };
        saved.pending_approvals = vec![PendingApproval {
            key: key.clone(),
            trigger: ApprovalTrigger::Required,
        }];
        apply_human_decision(
            &mut saved,
            &flow,
            &json!({"action":"approve","target":"execution-plan"}),
        )
        .unwrap();
        assert!(saved.pending_approvals.is_empty());
        assert!(saved.completed_keys.contains(&key));
        assert!(saved.execution_plan.is_none());
    }
}
