pub mod deployment;
pub mod facade;
pub mod github_workflow;
pub mod plugin;
pub mod policy;
pub mod repository;
pub mod run_result;
pub mod state;

pub use approval::{ApprovalCommands, RevisionOption, TaskKey};
pub use deployment::{DEPLOYMENT_MODE_ENV, DeploymentMode};
pub use facade::{Facade, FacadeConfig};
pub use github_workflow::{GitHubIssueAction, triage_comment_body, triage_github_issue_actions};
pub use plugin::{
    McpServerDefinition, PluginAgent, PluginApprovalMode, PluginArtifact, PluginArtifactType,
    PluginBuild, PluginEnvironmentVariable, PluginError, PluginFlow, PluginInstallation,
    PluginManifest, PluginParameter, PluginResource, PluginResourceAssignment,
    PluginResourceSource, PluginRole, PluginRuntime, PluginStage, PluginTask, PluginTaskScope,
    PluginValidator, PluginWorkItem, PluginWorkItemRegistry,
};
pub use policy::{
    AgentConfig, AgentRoleConfig, AutomationDecision, EngagementGate, EngagementPolicy,
    EngagementRule, EngagementSelector, LifecyclePolicy, PluginFlowSelection, Policy, PolicyError,
    RepositoryEngagementPolicy, StageAccessOverride, TaskAccessOverride,
};
pub use run_result::{
    AgentHandoff, Confidence, Outcome, PluginStageResult, PluginTaskResult, Risk, RunResult,
    RunResultError, TestResult, TestStatus,
};
pub use state::workflow_state_for_outcome;
pub use state::{AgentRole, LabelState, WorkflowLabel, WorkflowState, normalize_workflow_labels};
pub mod approval;
