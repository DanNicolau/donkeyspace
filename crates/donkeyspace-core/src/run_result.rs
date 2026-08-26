use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum RunResultError {
    #[error("needs_info outcome requires at least one question")]
    MissingQuestion,
    #[error("needs_human outcome requires human_review_reason")]
    MissingHumanReviewReason,
    #[error("{0:?} outcome requires blocked_reason")]
    MissingBlockedReason(Outcome),
    #[error("implemented outcome requires at least one test result")]
    MissingTests,
    #[error("low-confidence ready result must be routed to humans")]
    LowConfidenceReady,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct RunResult {
    pub outcome: Outcome,
    pub summary: String,
    pub confidence: Confidence,
    pub risk: Risk,
    #[serde(default)]
    pub questions: Vec<String>,
    #[serde(default)]
    pub tests: Vec<TestResult>,
    #[serde(default)]
    pub changed_files: Vec<String>,
    pub human_review_reason: Option<String>,
    pub blocked_reason: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct AgentHandoff {
    pub target: String,
    pub reason: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct PluginTaskResult {
    #[serde(flatten)]
    pub result: RunResult,
    #[serde(default)]
    pub handoff: Option<AgentHandoff>,
    #[serde(default)]
    pub resources_used: Vec<String>,
    /// Repository work-item ids selected for this lifecycle. Planner tasks use
    /// this to distinguish the current issue's work from the persistent block
    /// catalog; non-planner tasks leave it unset.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub work_items: Option<Vec<String>>,
}

pub type PluginStageResult = PluginTaskResult;

impl RunResult {
    pub fn validate_for_orchestration(&self) -> Result<(), RunResultError> {
        match self.outcome {
            Outcome::NeedsInfo if self.questions.is_empty() => Err(RunResultError::MissingQuestion),
            Outcome::NeedsHuman if self.human_review_reason_is_empty() => {
                Err(RunResultError::MissingHumanReviewReason)
            }
            Outcome::Blocked | Outcome::Failed if self.blocked_reason_is_empty() => {
                Err(RunResultError::MissingBlockedReason(self.outcome))
            }
            Outcome::Implemented if self.tests.is_empty() => Err(RunResultError::MissingTests),
            Outcome::Ready if self.confidence == Confidence::Low => {
                Err(RunResultError::LowConfidenceReady)
            }
            _ => Ok(()),
        }
    }

    fn human_review_reason_is_empty(&self) -> bool {
        self.human_review_reason
            .as_ref()
            .map(|value| value.trim().is_empty())
            .unwrap_or(true)
    }

    fn blocked_reason_is_empty(&self) -> bool {
        self.blocked_reason
            .as_ref()
            .map(|value| value.trim().is_empty())
            .unwrap_or(true)
    }
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Outcome {
    Ready,
    NeedsInfo,
    Implemented,
    Reviewed,
    NeedsChanges,
    NeedsHuman,
    Blocked,
    Failed,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Confidence {
    Low,
    Medium,
    High,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Risk {
    Low,
    Medium,
    High,
    Unknown,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct TestResult {
    pub name: String,
    pub command: Vec<String>,
    pub status: TestStatus,
    pub exit_code: Option<i32>,
    pub summary: Option<String>,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TestStatus {
    Passed,
    Failed,
    Skipped,
    NotRun,
}

#[cfg(test)]
mod tests {
    use super::{Confidence, Outcome, Risk, RunResult, RunResultError};

    #[test]
    fn needs_info_requires_question() {
        let result = RunResult {
            outcome: Outcome::NeedsInfo,
            summary: "Missing context".to_string(),
            confidence: Confidence::Medium,
            risk: Risk::Unknown,
            questions: Vec::new(),
            tests: Vec::new(),
            changed_files: Vec::new(),
            human_review_reason: None,
            blocked_reason: None,
        };

        assert_eq!(
            result.validate_for_orchestration(),
            Err(RunResultError::MissingQuestion)
        );
    }
}
