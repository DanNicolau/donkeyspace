//! Human-approved scheduling choices. Plugins supply task names and defaults;
//! the scheduler, never an agent's prose, enforces the resolved plan.
use crate::{PluginApprovalMode, PluginFlow};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ExecutionPlanPolicy {
    #[serde(default)]
    pub optional_tasks: Vec<String>,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ExecutionPlanProposal {
    #[serde(default)]
    pub excluded_tasks: Vec<String>,
    #[serde(default)]
    pub approval_tasks: Vec<String>,
    #[serde(default)]
    pub reason: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ExecutionPlanTask {
    pub display_name: String,
    pub included: bool,
    pub approval: PluginApprovalMode,
    pub dependencies: Vec<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ExecutionPlan {
    pub revision: u32,
    pub proposal: ExecutionPlanProposal,
    pub work_items: Vec<String>,
    pub tasks: BTreeMap<String, ExecutionPlanTask>,
    #[serde(default)]
    pub approved: bool,
}

impl ExecutionPlan {
    pub fn resolve(
        flow: &PluginFlow,
        proposal: Option<ExecutionPlanProposal>,
        work_items: Vec<String>,
        revision: u32,
    ) -> Result<Self, String> {
        let policy = flow
            .execution_plan
            .as_ref()
            .ok_or("flow does not support execution plans")?;
        let proposal = proposal.unwrap_or_default();
        for (values, kind) in [
            (&proposal.excluded_tasks, "excluded"),
            (&proposal.approval_tasks, "approval"),
        ] {
            let mut seen = BTreeSet::new();
            for name in values {
                if !flow.tasks.contains_key(name) || !seen.insert(name) {
                    return Err(format!("unknown or duplicate {kind} task `{name}`"));
                }
            }
        }
        for name in &proposal.excluded_tasks {
            if name == &flow.start || !policy.optional_tasks.contains(name) {
                return Err(format!("task `{name}` cannot be excluded"));
            }
            if proposal.approval_tasks.contains(name) {
                return Err(format!("excluded task `{name}` cannot require approval"));
            }
        }
        let tasks = flow
            .tasks
            .iter()
            .map(|(name, task)| {
                let included = !proposal.excluded_tasks.contains(name);
                if included
                    && task
                        .dependencies
                        .iter()
                        .any(|d| proposal.excluded_tasks.contains(d))
                {
                    return Err(format!(
                        "included task `{name}` depends on an excluded task"
                    ));
                }
                Ok((
                    name.clone(),
                    ExecutionPlanTask {
                        display_name: task.display_name.clone().unwrap_or_else(|| name.clone()),
                        included,
                        approval: if name == &flow.start || proposal.approval_tasks.contains(name) {
                            PluginApprovalMode::Required
                        } else {
                            task.approval
                        },
                        dependencies: task.dependencies.clone(),
                    },
                ))
            })
            .collect::<Result<_, String>>()?;
        Ok(Self {
            revision,
            proposal,
            work_items,
            tasks,
            approved: false,
        })
    }

    /// Revalidate on every continuation. Changed task topology/default approvals
    /// must not silently change the accepted plan; access-root refreshes may.
    pub fn apply(&self, flow: &PluginFlow) -> Result<PluginFlow, String> {
        let resolved = Self::resolve(
            flow,
            Some(self.proposal.clone()),
            self.work_items.clone(),
            self.revision,
        )?;
        if self.tasks != resolved.tasks {
            return Err("execution plan no longer matches the configured flow; restore that flow or request a revised plan".into());
        }
        let mut effective = flow.clone();
        effective.tasks.retain(|name, _| self.tasks[name].included);
        for (name, task) in &mut effective.tasks {
            task.approval = self.tasks[name].approval;
            // Handoffs remain declared so an excluded target can be reported
            // as a human decision rather than silently executed.
        }
        Ok(effective)
    }

    pub fn markdown(&self) -> String {
        let status = if self.approved {
            "approved"
        } else {
            "awaiting approval"
        };
        let reason = if !self.proposal.reason.trim().is_empty() {
            self.proposal.reason.trim()
        } else if self.proposal.excluded_tasks.is_empty() && self.proposal.approval_tasks.is_empty()
        {
            "Default workflow; no execution preferences supplied."
        } else {
            "Execution settings proposed for approval."
        };
        let escape = |s: &str| s.replace('|', "\\|").replace(['\n', '\r'], " ");
        let mut lines = vec![
            format!("### Execution plan v{} — {status}", self.revision),
            format!("Scope: {}", self.work_items.join(", ")),
            escape(reason),
            "\n| Task | Included | Approval afterward | Depends on |\n| --- | --- | --- | --- |"
                .into(),
        ];
        let mut rows = Vec::new();
        let mut remaining = self.tasks.iter().collect::<Vec<_>>();
        let mut ordered = Vec::new();
        let mut seen = BTreeSet::new();
        while !remaining.is_empty() {
            let next = remaining
                .iter()
                .position(|(_, task)| task.dependencies.iter().all(|d| seen.contains(d)))
                .unwrap_or(0);
            let (name, task) = remaining.remove(next);
            seen.insert(name.clone());
            ordered.push((name, task));
        }
        for (name, task) in ordered {
            rows.push(format!(
                "| {} (`{name}`) | {} | {} | {} |",
                escape(&task.display_name),
                if task.included {
                    "Yes"
                } else {
                    "No — not run"
                },
                if task.included && task.approval == PluginApprovalMode::Required {
                    "Required"
                } else {
                    "—"
                },
                task.dependencies.join(", ")
            ));
        }
        lines
            .last_mut()
            .unwrap()
            .push_str(&format!("\n{}", rows.join("\n")));
        lines.push("\nIncluded tasks run when their dependencies and approvals are satisfied. Completion requires their required checks; excluded tasks are not validated. Approval of the initial proposal accepts both the specification and this execution plan.".into());
        lines.join("\n\n")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn flow() -> PluginFlow {
        serde_json::from_value(serde_json::json!({"start":"plan","execution_plan":{"optional_tasks":["package"]},"tasks":{"plan":{"role":"planner"},"build":{"role":"writer","dependencies":["plan"]},"check":{"role":"checker","dependencies":["build"]},"package":{"role":"publisher","dependencies":["build"]}}})).unwrap()
    }
    #[test]
    fn defaults_are_a_full_unapproved_plan() {
        let p = ExecutionPlan::resolve(&flow(), None, vec!["one".into()], 1).unwrap();
        assert!(!p.approved);
        assert!(p.tasks.values().all(|t| t.included));
        assert_eq!(p.tasks["plan"].approval, PluginApprovalMode::Required);
    }
    #[test]
    fn optional_tasks_can_be_excluded_and_build_can_pause() {
        let p = ExecutionPlan::resolve(
            &flow(),
            Some(ExecutionPlanProposal {
                excluded_tasks: vec!["package".into()],
                approval_tasks: vec!["build".into()],
                reason: "User requests review".into(),
            }),
            vec!["one".into()],
            1,
        )
        .unwrap();
        let f = p.apply(&flow()).unwrap();
        assert!(!f.tasks.contains_key("package"));
        assert_eq!(f.tasks["build"].approval, PluginApprovalMode::Required);
        assert!(p.markdown().contains("No — not run"));
    }
    #[test]
    fn rejects_unknown_required_conflicting_and_dependency_breaking_choices() {
        for excluded in ["unknown", "plan", "build"] {
            assert!(
                ExecutionPlan::resolve(
                    &flow(),
                    Some(ExecutionPlanProposal {
                        excluded_tasks: vec![excluded.into()],
                        ..Default::default()
                    }),
                    vec![],
                    1
                )
                .is_err()
            );
        }
        let mut f = flow();
        f.execution_plan
            .as_mut()
            .unwrap()
            .optional_tasks
            .push("build".into());
        assert!(
            ExecutionPlan::resolve(
                &f,
                Some(ExecutionPlanProposal {
                    excluded_tasks: vec!["build".into()],
                    ..Default::default()
                }),
                vec![],
                1
            )
            .is_err()
        );
        assert!(
            ExecutionPlan::resolve(
                &f,
                Some(ExecutionPlanProposal {
                    excluded_tasks: vec!["package".into()],
                    approval_tasks: vec!["package".into()],
                    ..Default::default()
                }),
                vec![],
                1
            )
            .is_err()
        );
    }
    #[test]
    fn accepted_plan_cannot_silently_change_with_manifest() {
        let mut f = flow();
        let p = ExecutionPlan::resolve(&f, None, vec![], 1).unwrap();
        f.tasks.get_mut("build").unwrap().approval = PluginApprovalMode::Required;
        assert!(p.apply(&f).is_err());
    }
}
