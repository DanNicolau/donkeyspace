pub mod fake_agent;
pub mod github_workflow;
pub mod plugin;
pub mod policy;
pub mod run_result;
pub mod state;

pub use fake_agent::{fake_triage_issue, workflow_state_for_outcome};
pub use github_workflow::{GitHubIssueAction, triage_comment_body, triage_github_issue_actions};
pub use plugin::{
    McpServerDefinition, PluginAgent, PluginArtifact, PluginArtifactType, PluginError, PluginFlow,
    PluginManifest, PluginParameter, PluginResource, PluginResourceAssignment,
    PluginResourceSource, PluginRole, PluginRuntime, PluginStage, PluginTask, PluginTaskScope,
    PluginValidator, PluginWorkItem, PluginWorkItemRegistry,
};
pub use policy::{
    AgentConfig, AgentRoleConfig, AutomationDecision, EngagementGate, EngagementPolicy,
    EngagementRule, EngagementSelector, LifecyclePolicy, PluginFlowSelection, Policy, PolicyError,
    StageAccessOverride, TaskAccessOverride,
};
pub use run_result::{
    AgentHandoff, Confidence, Outcome, PluginStageResult, PluginTaskResult, Risk, RunResult,
    RunResultError, TestResult, TestStatus,
};
pub use state::{AgentRole, LabelState, WorkflowLabel, WorkflowState, normalize_workflow_labels};
