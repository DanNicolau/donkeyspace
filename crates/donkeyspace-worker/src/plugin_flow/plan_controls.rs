use super::*;

pub(super) fn apply_plan_decision(
    saved: &mut LifecycleCheckpoint,
    base_flow: &PluginFlow,
    decision: &HumanDecision,
    decision_id: Option<&str>,
) -> Result<Option<ResumeDecision>, Box<dyn std::error::Error>> {
    // The virtual target belongs only to opted-in flows. Other plugins may
    // already have a real task with this name.
    if base_flow.execution_plan.is_none() {
        return Ok(None);
    }
    if let HumanDecision::Revise {
        target: Some(target),
        feedback,
    } = &decision
        && target == "execution-plan"
    {
        let proposal: donkeyspace_core::ExecutionPlanProposal = serde_yaml::from_str(feedback)
            .map_err(|e| {
                format!("Use excluded_tasks: [task], approval_tasks: [task], and reason: text. {e}")
            })?;
        let revision = saved.execution_plan.as_ref().map_or(1, |p| p.revision + 1);
        let plan = donkeyspace_core::ExecutionPlan::resolve(
            base_flow,
            Some(proposal),
            saved.active_work_items.clone(),
            revision,
        )?;
        let effective = plan.apply(base_flow)?;
        let removed: Vec<_> = saved
            .tracked_jobs
            .iter()
            .map(|t| &t.key)
            .filter(|key| !effective.tasks.contains_key(&key.task))
            .cloned()
            .collect();
        saved
            .completed_keys
            .retain(|key| effective.tasks.contains_key(&key.task));
        saved
            .pending_approvals
            .retain(|a| effective.tasks.contains_key(&a.key.task));
        saved
            .revision_targets
            .retain(|key| effective.tasks.contains_key(&key.task));
        // Newly requested review also gates an already implemented artifact.
        let review_keys: Vec<_> = saved
            .completed_keys
            .iter()
            .filter(|key| {
                key.task != base_flow.start
                    && effective.tasks[&key.task].approval == PluginApprovalMode::Required
                    && saved.execution_plan.as_ref().is_none_or(|old| {
                        old.tasks
                            .get(&key.task)
                            .is_none_or(|t| t.approval != PluginApprovalMode::Required)
                    })
            })
            .cloned()
            .collect();
        for key in review_keys {
            saved.completed_keys.retain(|k| k != &key);
            saved.pending_approvals.push(PendingApproval {
                key,
                trigger: ApprovalTrigger::Required,
            });
        }
        saved.pending_approvals.push(PendingApproval {
            key: TaskKey {
                task: "execution-plan".into(),
                work_item: None,
            },
            trigger: ApprovalTrigger::Required,
        });
        saved.previous.push(json!({"event":"execution_plan_proposed", "execution_plan":plan, "human_decision_id":decision_id}));
        saved.execution_plan = Some(plan);
        saved.flow_fingerprint = Some(flow_fingerprint(&effective)?);
        return Ok(Some(ResumeDecision {
            decisions: vec![(
                TaskKey {
                    task: "execution-plan".into(),
                    work_item: None,
                },
                "revised".into(),
            )],
            invalidated: removed,
        }));
    }
    if let HumanDecision::Approve {
        target: Some(target),
    } = &decision
        && target == "execution-plan"
    {
        if !saved
            .pending_approvals
            .iter()
            .any(|a| a.key.task == "execution-plan")
        {
            return Err("no execution plan is awaiting approval".into());
        }
        let plan = saved
            .execution_plan
            .as_mut()
            .ok_or("execution plan is missing")?;
        plan.apply(base_flow)?;
        plan.approved = true;
        saved
            .pending_approvals
            .retain(|a| a.key.task != "execution-plan");
        saved.previous.push(json!({"event":"execution_plan_approved", "execution_plan":plan, "human_decision_id":decision_id}));
        return Ok(Some(ResumeDecision {
            decisions: vec![(
                TaskKey {
                    task: "execution-plan".into(),
                    work_item: None,
                },
                "approved".into(),
            )],
            invalidated: vec![],
        }));
    }
    if matches!(&decision, HumanDecision::Approve { target } if target.is_none() || target.as_deref() == Some("all"))
        && saved
            .pending_approvals
            .iter()
            .any(|a| a.key.task == "execution-plan")
    {
        return Err("approve execution-plan explicitly before approving other work".into());
    }
    Ok(None)
}
