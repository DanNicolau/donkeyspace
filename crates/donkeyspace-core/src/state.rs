use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, fmt};

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AgentRole {
    Triage,
    Developer,
    Reviewer,
}

impl AgentRole {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Triage => "triage",
            Self::Developer => "developer",
            Self::Reviewer => "reviewer",
        }
    }
}

impl fmt::Display for AgentRole {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum WorkflowState {
    NeedsInfo,
    Ready,
    InProgress,
    PrOpen,
    NeedsHuman,
    Blocked,
}

impl WorkflowState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::NeedsInfo => "needs_info",
            Self::Ready => "ready",
            Self::InProgress => "in_progress",
            Self::PrOpen => "pr_open",
            Self::NeedsHuman => "needs_human",
            Self::Blocked => "blocked",
        }
    }
}

impl fmt::Display for WorkflowState {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct WorkflowLabel {
    pub state: WorkflowState,
    pub label: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LabelState {
    None,
    One(WorkflowLabel),
    Conflict(Vec<WorkflowLabel>),
}

pub fn normalize_workflow_labels(
    labels: &[String],
    state_labels: &BTreeMap<String, String>,
) -> LabelState {
    let mut matches = Vec::new();

    for state in [
        WorkflowState::NeedsInfo,
        WorkflowState::Ready,
        WorkflowState::InProgress,
        WorkflowState::PrOpen,
        WorkflowState::NeedsHuman,
        WorkflowState::Blocked,
    ] {
        if let Some(label) = state_labels.get(state.as_str())
            && labels.iter().any(|candidate| candidate == label)
        {
            matches.push(WorkflowLabel {
                state,
                label: label.clone(),
            });
        }
    }

    match matches.len() {
        0 => LabelState::None,
        1 => LabelState::One(matches.remove(0)),
        _ => LabelState::Conflict(matches),
    }
}

#[cfg(test)]
mod tests {
    use super::{LabelState, WorkflowState, normalize_workflow_labels};
    use std::collections::BTreeMap;

    fn state_labels() -> BTreeMap<String, String> {
        BTreeMap::from([
            ("needs_info".to_string(), "ai:needs-info".to_string()),
            ("ready".to_string(), "ai:ready".to_string()),
            ("in_progress".to_string(), "ai:in-progress".to_string()),
            ("pr_open".to_string(), "ai:pr-open".to_string()),
            ("needs_human".to_string(), "ai:needs-human".to_string()),
            ("blocked".to_string(), "ai:blocked".to_string()),
        ])
    }

    #[test]
    fn detects_single_workflow_label() {
        let labels = vec!["bug".to_string(), "ai:ready".to_string()];
        let state = normalize_workflow_labels(&labels, &state_labels());

        assert!(matches!(
            state,
            LabelState::One(label) if label.state == WorkflowState::Ready
        ));
    }

    #[test]
    fn detects_conflicting_workflow_labels() {
        let labels = vec!["ai:ready".to_string(), "ai:blocked".to_string()];
        let state = normalize_workflow_labels(&labels, &state_labels());

        assert!(matches!(state, LabelState::Conflict(labels) if labels.len() == 2));
    }
}
