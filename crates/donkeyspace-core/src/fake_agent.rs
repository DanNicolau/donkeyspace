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
    let latest_comment = input
        .pointer("/comment/body")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .trim();
    let repository_context = input
        .pointer("/repository_context")
        .map(Value::to_string)
        .unwrap_or_default();

    if short_issue_has_unique_file_context(title, body, &repository_context) {
        return RunResult {
            outcome: Outcome::Ready,
            summary: "The issue is short, but repository context identifies the target file."
                .to_string(),
            confidence: Confidence::Medium,
            risk: Risk::Low,
            questions: Vec::new(),
            tests: Vec::new(),
            changed_files: Vec::new(),
            human_review_reason: None,
            blocked_reason: None,
        };
    }

    if title.is_empty() || meaningful_word_count(&format!("{body}\n{latest_comment}")) < 8 {
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
        Outcome::Reviewed => WorkflowState::PrOpen,
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

fn short_issue_has_unique_file_context(title: &str, body: &str, repository_context: &str) -> bool {
    let issue_text = format!("{title}\n{body}").to_ascii_lowercase();
    issue_text.contains("readme") && repository_context.to_ascii_lowercase().contains("readme")
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
    fn human_clarification_comment_can_make_issue_ready() {
        let result = fake_triage_issue(&json!({
            "issue": {
                "title": "Fix invoice export",
                "body": "Export fails"
            },
            "comment": {
                "body": "Reproduce by exporting an account with more than 100 invoices. The expected result is a downloadable CSV with all invoice rows."
            }
        }));

        assert_eq!(result.outcome, Outcome::Ready);
    }

    #[test]
    fn short_readme_issue_is_ready_when_repo_context_has_readme() {
        let result = fake_triage_issue(&json!({
            "issue": {
                "title": "Capitize D and S in README",
                "body": null
            },
            "repository_context": {
                "file_tree": ["README.md"],
                "excerpts": [{"path": "README.md", "content": "# example-repo", "truncated": false}]
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
        assert_eq!(
            workflow_state_for_outcome(Outcome::Reviewed),
            WorkflowState::PrOpen
        );
    }
}
