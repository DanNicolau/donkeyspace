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

    let current_labels = issue_labels(input);
    let mut actions = Vec::new();

    if let Some(target_label) = policy.workflow.state_labels.get(target_state.as_str()) {
        let stale_labels = policy
            .workflow
            .state_labels
            .values()
            .filter(|label| *label != target_label)
            .filter(|label| current_labels.iter().any(|current| current == *label))
            .cloned()
            .collect::<Vec<_>>();

        if !stale_labels.is_empty() {
            actions.push(GitHubIssueAction {
                action_type: "issue.remove_labels".to_string(),
                payload: serde_json::json!({
                    "issue_number": issue_number,
                    "labels": stale_labels,
                }),
            });
        }

        if !current_labels.iter().any(|label| label == target_label) {
            actions.push(GitHubIssueAction {
                action_type: "issue.add_label".to_string(),
                payload: serde_json::json!({
                    "issue_number": issue_number,
                    "label": target_label,
                    "state": target_state.as_str(),
                }),
            });
        }
    }

    if let Some(body) = triage_comment_body(result, target_state) {
        actions.push(GitHubIssueAction {
            action_type: "issue.create_comment".to_string(),
            payload: serde_json::json!({
                "issue_number": issue_number,
                "body": body,
            }),
        });
    }

    actions
}

pub fn triage_comment_body(result: &RunResult, target_state: WorkflowState) -> Option<String> {
    match result.outcome {
        Outcome::NeedsInfo => {
            let questions = result
                .questions
                .iter()
                .map(|question| format!("- {question}"))
                .collect::<Vec<_>>()
                .join("\n");

            Some(format!(
                "donkeyspace triage needs clarification before this issue can move to implementation.\n\nQuestions:\n{questions}\n\nCurrent state: `{}`",
                workflow_label_text(target_state)
            ))
        }
        Outcome::Ready => Some(format!(
            "donkeyspace marked this issue ready for agent implementation.\n\nReason:\n{}\n\nCurrent state: `{}`",
            result.summary,
            workflow_label_text(target_state)
        )),
        _ => None,
    }
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

fn issue_labels(input: &Value) -> Vec<String> {
    input
        .pointer("/issue/labels")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|label| label.pointer("/name").and_then(Value::as_str))
        .map(ToString::to_string)
        .collect()
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
                "issue": {
                    "number": 42,
                    "labels": [{"name": "bug"}]
                }
            }),
            &result,
            WorkflowState::Ready,
        );

        assert_eq!(actions.len(), 2);
        assert_eq!(actions[0].action_type, "issue.add_label");
        assert_eq!(actions[0].payload["label"], "ai:ready");
        assert_eq!(actions[1].action_type, "issue.create_comment");
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
        assert_eq!(actions[0].payload["labels"], json!(["ai:ready"]));
        assert_eq!(actions[1].action_type, "issue.add_label");
        assert_eq!(actions[1].payload["label"], "ai:needs-info");
    }

    #[test]
    fn needs_info_comment_includes_questions() {
        let body = triage_comment_body(
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
    }
}
