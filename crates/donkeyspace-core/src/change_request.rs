use crate::ExecutionPlanProposal;
use serde::{Deserialize, Serialize};

/// A planner interprets a human request; the coordinator validates and applies
/// the proposal against the retained checkpoint after explicit approval.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ChangeProposal {
    pub summary: String,
    pub execution_plan: Option<ExecutionPlanProposal>,
    pub revisions: Vec<TaskRevisionRequest>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct TaskRevisionRequest {
    pub target: String,
    pub instruction: String,
}
