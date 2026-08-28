use crate::{Outcome, Policy, RunResult, WorkflowState};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct GitHubIssueAction {
    pub action_type: String,
    pub payload: Value,
}

pub fn triage_github_issue_actions(
    policy: &Policy,
    input: &Value,
    result: &RunResult,
    target_state: WorkflowState,
) -> Vec<GitHubIssueAction> {
    let Some(issue_number) = input.pointer("/issue/number").and_then(Value::as_i64) else {
        return Vec::new();
    };
    let Some(owner) = input
        .pointer("/repository/owner/login")
        .and_then(Value::as_str)
    else {
        return Vec::new();
    };
    let Some(repo) = input.pointer("/repository/name").and_then(Value::as_str) else {
        return Vec::new();
    };

    let mut actions = Vec::new();

    if let Some(target_label) = policy.workflow.state_labels.get(target_state.as_str()) {
        let stale_labels = policy
            .workflow
            .state_labels
            .values()
            .filter(|label| *label != target_label)
            .cloned()
            .collect::<Vec<_>>();

        if !stale_labels.is_empty() {
            actions.push(GitHubIssueAction {
                action_type: "issue.remove_labels".to_string(),
                payload: serde_json::json!({
                    "owner": owner,
                    "repo": repo,
                    "issue_number": issue_number,
                    "labels": stale_labels,
                }),
            });
        }

        // GitHub events are snapshots and can be stale by the time an async
        // workflow transition is published. Adding an existing label is
        // idempotent, so always re-apply the desired label after removing all
        // stale state labels to make the issue converge on the target state.
        actions.push(GitHubIssueAction {
            action_type: "issue.add_label".to_string(),
            payload: serde_json::json!({
                "owner": owner,
                "repo": repo,
                "issue_number": issue_number,
                "label": target_label,
                "state": target_state.as_str(),
            }),
        });
    }

    if let Some(body) = triage_comment_body(policy, result, target_state) {
        actions.push(GitHubIssueAction {
            action_type: "issue.create_comment".to_string(),
            payload: serde_json::json!({
                "owner": owner,
                "repo": repo,
                "issue_number": issue_number,
                "body": body,
            }),
        });
    }

    actions
}

pub fn triage_comment_body(
    policy: &Policy,
    result: &RunResult,
    target_state: WorkflowState,
) -> Option<String> {
    let facade = policy.facade.resolve();
    let name = &facade.display_name;
    let command = facade.issue_command();
    let body = match result.outcome {
        Outcome::NeedsInfo => {
            let questions = result
                .questions
                .iter()
                .map(|question| format!("- {question}"))
                .collect::<Vec<_>>()
                .join("\n");

            Some(format!(
                "{name} triage needs clarification before this issue can move to implementation.\n\nQuestions:\n{questions}\n\nCurrent state: `{}`",
                workflow_label_text(target_state)
            ))
        }
        Outcome::Ready => Some(format!(
            "{name} marked this issue ready for agent implementation.\n\nReason:\n{}\n\nCurrent state: `{}`",
            result.summary,
            workflow_label_text(target_state)
        )),
        Outcome::NeedsHuman => {
            let reason = result
                .human_review_reason
                .as_deref()
                .unwrap_or("The workflow requires a human decision before it can continue.");
            let failed_checks = result
                .tests
                .iter()
                .filter(|test| test.status == crate::TestStatus::Failed)
                .map(|test| {
                    format!(
                        "- `{}`: {}",
                        test.name,
                        test.summary.as_deref().unwrap_or("No details reported.")
                    )
                })
                .collect::<Vec<_>>();
            let evidence = if failed_checks.is_empty() {
                String::new()
            } else {
                format!("\n\nFailed verification:\n{}", failed_checks.join("\n"))
            };

            Some(format!(
                "{name} needs human input before this workflow can continue.\n\nDecision required:\n{reason}\n\nLatest result:\n{}{evidence}\n\nBefore responding:\nReview every approval subject and linked artifact listed above. Approval accepts only the described checkpoint and authorizes the stated next work. Revision keeps dependent work blocked and must include specific requested changes. Use the exact target-specific command shown above; if none is shown, use `{command} approve` or `{command} revise` followed by feedback. Only a newly created command comment from an authorized approver can requeue the workflow.\n\nCurrent state: `{}`",
                result.summary,
                workflow_label_text(target_state)
            ))
        }
        _ => None,
    }?;

    Some(format!("{body}\n\n<!-- donkeyspace-generated -->"))
}

fn workflow_label_text(state: WorkflowState) -> &'static str {
    match state {
        WorkflowState::NeedsInfo => "ai:needs-info",
        WorkflowState::Ready => "ai:ready",
        WorkflowState::InProgress => "ai:in-progress",
        WorkflowState::PrOpen => "ai:pr-open",
        WorkflowState::NeedsHuman => "ai:needs-human",
        WorkflowState::Blocked => "ai:blocked",
    }
}

#[cfg(test)]
mod tests {
    use super::{triage_comment_body, triage_github_issue_actions};
    use crate::{Confidence, Outcome, Policy, Risk, RunResult, WorkflowState};
    use serde_json::json;

    fn policy() -> Policy {
        Policy::from_yaml(include_str!("../../../docs/policy.example.yml")).unwrap()
    }

    #[test]
    fn ready_triage_adds_ready_label_and_comment() {
        let result = RunResult {
            outcome: Outcome::Ready,
            summary: "Enough context exists.".to_string(),
            confidence: Confidence::Medium,
            risk: Risk::Unknown,
            questions: Vec::new(),
            tests: Vec::new(),
            changed_files: Vec::new(),
            human_review_reason: None,
            blocked_reason: None,
        };
        let actions = triage_github_issue_actions(
            &policy(),
            &json!({
                "repository": {
                    "name": "repo",
                    "owner": {"login": "owner"}
                },
                "issue": {
                    "number": 42,
                    "labels": [{"name": "bug"}]
                }
            }),
            &result,
            WorkflowState::Ready,
        );

        assert_eq!(actions.len(), 3);
        assert_eq!(actions[0].action_type, "issue.remove_labels");
        assert_eq!(actions[1].action_type, "issue.add_label");
        assert_eq!(actions[1].payload["owner"], "owner");
        assert_eq!(actions[1].payload["repo"], "repo");
        assert_eq!(actions[1].payload["label"], "ai:ready");
        assert_eq!(actions[2].action_type, "issue.create_comment");
    }

    #[test]
    fn stale_workflow_labels_are_removed_before_target_label_is_added() {
        let result = RunResult {
            outcome: Outcome::NeedsInfo,
            summary: "Missing context.".to_string(),
            confidence: Confidence::High,
            risk: Risk::Unknown,
            questions: vec!["What should change?".to_string()],
            tests: Vec::new(),
            changed_files: Vec::new(),
            human_review_reason: None,
            blocked_reason: None,
        };
        let actions = triage_github_issue_actions(
            &policy(),
            &json!({
                "repository": {
                    "name": "repo",
                    "owner": {"login": "owner"}
                },
                "issue": {
                    "number": 42,
                    "labels": [{"name": "ai:ready"}]
                }
            }),
            &result,
            WorkflowState::NeedsInfo,
        );

        assert_eq!(actions.len(), 3);
        assert_eq!(actions[0].action_type, "issue.remove_labels");
        assert_eq!(
            actions[0].payload["labels"],
            json!([
                "ai:blocked",
                "ai:in-progress",
                "ai:needs-human",
                "ai:pr-open",
                "ai:ready"
            ])
        );
        assert_eq!(actions[1].action_type, "issue.add_label");
        assert_eq!(actions[1].payload["label"], "ai:needs-info");
    }

    #[test]
    fn target_label_is_reapplied_when_event_snapshot_already_contains_it() {
        let result = RunResult {
            outcome: Outcome::NeedsHuman,
            summary: "Approval required.".to_string(),
            confidence: Confidence::High,
            risk: Risk::Low,
            questions: Vec::new(),
            tests: Vec::new(),
            changed_files: Vec::new(),
            human_review_reason: Some("Approve the checkpoint.".to_string()),
            blocked_reason: None,
        };
        let actions = triage_github_issue_actions(
            &policy(),
            &json!({
                "repository": {
                    "name": "repo",
                    "owner": {"login": "owner"}
                },
                "issue": {
                    "number": 42,
                    "labels": [{"name": "ai:needs-human"}]
                }
            }),
            &result,
            WorkflowState::NeedsHuman,
        );

        assert_eq!(actions[0].action_type, "issue.remove_labels");
        assert_eq!(actions[1].action_type, "issue.add_label");
        assert_eq!(actions[1].payload["label"], "ai:needs-human");
    }

    #[test]
    fn needs_info_comment_includes_questions() {
        let body = triage_comment_body(
            &policy(),
            &RunResult {
                outcome: Outcome::NeedsInfo,
                summary: "Missing context.".to_string(),
                confidence: Confidence::High,
                risk: Risk::Unknown,
                questions: vec!["How should this be verified?".to_string()],
                tests: Vec::new(),
                changed_files: Vec::new(),
                human_review_reason: None,
                blocked_reason: None,
            },
            WorkflowState::NeedsInfo,
        )
        .unwrap();

        assert!(body.contains("- How should this be verified?"));
        assert!(body.contains("Current state: `ai:needs-info`"));
        assert!(body.contains("<!-- donkeyspace-generated -->"));
    }

    #[test]
    fn needs_human_comment_explains_required_action_and_failed_checks() {
        let mut policy = policy();
        policy.facade.display_name = Some("Example Agent Platform".into());
        policy.facade.command = Some("example-agent".into());
        let body = triage_comment_body(
            &policy,
            &RunResult {
                outcome: Outcome::NeedsHuman,
                summary: "DV and RTL disagree about output latency.".to_string(),
                confidence: Confidence::High,
                risk: Risk::High,
                questions: Vec::new(),
                tests: vec![crate::TestResult {
                    name: "top-level simulation".to_string(),
                    command: vec!["make".to_string(), "test".to_string()],
                    status: crate::TestStatus::Failed,
                    exit_code: Some(1),
                    summary: Some("31 cycle-alignment mismatches.".to_string()),
                }],
                changed_files: Vec::new(),
                human_review_reason: Some(
                    "The DV-to-RTL handoff exceeded the policy limit.".to_string(),
                ),
                blocked_reason: None,
            },
            WorkflowState::NeedsHuman,
        )
        .unwrap();

        assert!(body.contains("Decision required:"));
        assert!(body.contains("handoff exceeded the policy limit"));
        assert!(body.contains("Failed verification:"));
        assert!(body.contains("`top-level simulation`: 31 cycle-alignment mismatches."));
        assert!(body.contains("Example Agent Platform needs human input"));
        assert!(body.contains("Review every approval subject and linked artifact"));
        assert!(body.contains("Revision keeps dependent work blocked"));
        assert!(body.contains("/example-agent approve"));
        assert!(!body.contains("/donkeyspace approve"));
        assert!(body.contains("Current state: `ai:needs-human`"));
    }
}
