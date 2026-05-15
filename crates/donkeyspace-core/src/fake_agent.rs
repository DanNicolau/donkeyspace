use crate::{Confidence, Outcome, Risk, RunResult, WorkflowState};
use serde_json::Value;

pub fn fake_triage_issue(input: &Value) -> RunResult {
    let title = input
        .pointer("/issue/title")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .trim();
    let body = input
        .pointer("/issue/body")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .trim();

    if title.is_empty() || meaningful_word_count(body) < 8 {
        return RunResult {
            outcome: Outcome::NeedsInfo,
            summary: "The issue needs clearer acceptance criteria before implementation."
                .to_string(),
            confidence: Confidence::High,
            risk: Risk::Unknown,
            questions: vec![
                "What behavior should change, and how should we verify it?".to_string(),
                "Are there relevant logs, screenshots, reproduction steps, or affected files?"
                    .to_string(),
            ],
            tests: Vec::new(),
            changed_files: Vec::new(),
            human_review_reason: None,
            blocked_reason: None,
        };
    }

    RunResult {
        outcome: Outcome::Ready,
        summary: "The issue has enough context for an implementation agent to start.".to_string(),
        confidence: Confidence::Medium,
        risk: Risk::Unknown,
        questions: Vec::new(),
        tests: Vec::new(),
        changed_files: Vec::new(),
        human_review_reason: None,
        blocked_reason: None,
    }
}

pub fn workflow_state_for_outcome(outcome: Outcome) -> WorkflowState {
    match outcome {
        Outcome::Ready => WorkflowState::Ready,
        Outcome::NeedsInfo => WorkflowState::NeedsInfo,
        Outcome::Implemented => WorkflowState::PrOpen,
        Outcome::NeedsChanges => WorkflowState::PrOpen,
        Outcome::NeedsHuman => WorkflowState::NeedsHuman,
        Outcome::Blocked | Outcome::Failed => WorkflowState::Blocked,
    }
}

fn meaningful_word_count(value: &str) -> usize {
    value
        .split_whitespace()
        .filter(|word| word.chars().any(char::is_alphanumeric))
        .count()
}

#[cfg(test)]
mod tests {
    use super::{fake_triage_issue, workflow_state_for_outcome};
    use crate::{Outcome, WorkflowState};
    use serde_json::json;

    #[test]
    fn unclear_issue_needs_info() {
        let result = fake_triage_issue(&json!({
            "issue": {
                "title": "broken",
                "body": "fails"
            }
        }));

        assert_eq!(result.outcome, Outcome::NeedsInfo);
        assert!(!result.questions.is_empty());
    }

    #[test]
    fn clear_issue_is_ready() {
        let result = fake_triage_issue(&json!({
            "issue": {
                "title": "Fix invoice export",
                "body": "The CSV export fails for accounts with many invoices. Reproduce by exporting an account with more than 100 invoices and verify the download succeeds."
            }
        }));

        assert_eq!(result.outcome, Outcome::Ready);
    }

    #[test]
    fn maps_result_to_workflow_state() {
        assert_eq!(
            workflow_state_for_outcome(Outcome::NeedsInfo),
            WorkflowState::NeedsInfo
        );
        assert_eq!(
            workflow_state_for_outcome(Outcome::Ready),
            WorkflowState::Ready
        );
    }
}
