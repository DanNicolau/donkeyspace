use crate::{Outcome, Risk, RunResult};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum PolicyError {
    #[error("failed to parse policy yaml: {0}")]
    Parse(#[from] serde_yaml::Error),
    #[error("invalid policy: {0}")]
    Invalid(String),
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct Policy {
    pub version: u32,
    pub workflow: WorkflowPolicy,
    #[serde(default)]
    pub lifecycle: LifecyclePolicy,
    #[serde(default)]
    pub agents: AgentConfig,
    pub checks: CheckPolicy,
    pub risk: RiskPolicy,
    pub automation: AutomationPolicy,
    #[serde(default)]
    pub dashboard: DashboardPolicy,
}

impl Policy {
    pub fn from_yaml(input: &str) -> Result<Self, PolicyError> {
        let policy: Self = serde_yaml::from_str(input)?;
        for (name, role) in [
            ("triage", &policy.agents.triage),
            ("developer", &policy.agents.developer),
            ("reviewer", &policy.agents.reviewer),
            ("repair", &policy.agents.repair),
        ] {
            if role.enabled && role.command.is_empty() && role.plugin.is_none() {
                return Err(PolicyError::Invalid(format!(
                    "enabled agent `{name}` requires command or plugin"
                )));
            }
            if !role.command.is_empty() && role.plugin.is_some() {
                return Err(PolicyError::Invalid(format!(
                    "agent `{name}` cannot specify both command and plugin"
                )));
            }
            if name != "developer" && role.plugin.is_some() {
                return Err(PolicyError::Invalid(
                    "plugin flows are currently supported only for developer".to_string(),
                ));
            }
        }
        if let Some(selection) = &policy.lifecycle.plugin
            && selection.flow.trim().is_empty()
        {
            return Err(PolicyError::Invalid(
                "lifecycle plugin flow cannot be empty".to_string(),
            ));
        }
        Ok(policy)
    }

    pub fn automation_decision_for_labels(&self, labels: &[String]) -> AutomationDecision {
        self.workflow.automation_decision_for_labels(labels)
    }

    pub fn apply_result_routing(&self, result: &mut RunResult) -> Option<String> {
        self.risk.apply_result_routing(result)
    }
}

#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq, Eq)]
pub struct LifecyclePolicy {
    /// An optional plugin flow that replaces the built-in
    /// triage/developer/reviewer/repair lifecycle.
    #[serde(default)]
    pub plugin: Option<PluginFlowSelection>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct WorkflowPolicy {
    pub state_labels: BTreeMap<String, String>,
    #[serde(default)]
    pub block_labels: Vec<String>,
    #[serde(default)]
    pub allow_labels: Vec<String>,
    #[serde(default)]
    pub engagement: EngagementPolicy,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct EngagementPolicy {
    #[serde(default = "EngagementRule::token_owner")]
    pub initial: EngagementRule,
    #[serde(default = "EngagementRule::token_owner")]
    pub clarification: EngagementRule,
    #[serde(default = "EngagementRule::token_owner")]
    pub blocked_resume: EngagementRule,
    #[serde(default = "EngagementRule::token_owner")]
    pub human_authorization: EngagementRule,
}

impl Default for EngagementPolicy {
    fn default() -> Self {
        Self {
            initial: EngagementRule::token_owner(),
            clarification: EngagementRule::token_owner(),
            blocked_resume: EngagementRule::token_owner(),
            human_authorization: EngagementRule::token_owner(),
        }
    }
}

#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq, Eq)]
pub struct EngagementRule {
    #[serde(default)]
    pub any: bool,
    #[serde(default)]
    pub token_owner: bool,
    #[serde(default)]
    pub issue_author: bool,
    #[serde(default)]
    pub users: Vec<String>,
    #[serde(default)]
    pub author_associations: Vec<String>,
    #[serde(default)]
    pub trusted_bots: Vec<String>,
}

impl EngagementRule {
    fn token_owner() -> Self {
        Self {
            token_owner: true,
            ..Self::default()
        }
    }

    pub fn requires_token_owner(&self) -> bool {
        self.token_owner
    }
}

impl EngagementPolicy {
    pub fn requires_token_owner(&self) -> bool {
        [
            &self.initial,
            &self.clarification,
            &self.blocked_resume,
            &self.human_authorization,
        ]
        .iter()
        .any(|rule| rule.requires_token_owner())
    }
}

impl WorkflowPolicy {
    pub fn automation_decision_for_labels(&self, labels: &[String]) -> AutomationDecision {
        if let Some(label) = labels
            .iter()
            .find(|label| self.block_labels.iter().any(|blocked| blocked == *label))
        {
            return AutomationDecision::BlockedByLabel(label.clone());
        }

        if !self.allow_labels.is_empty()
            && !labels
                .iter()
                .any(|label| self.allow_labels.iter().any(|allowed| allowed == label))
        {
            return AutomationDecision::MissingAllowLabel(self.allow_labels.clone());
        }

        AutomationDecision::Allowed
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AutomationDecision {
    Allowed,
    BlockedByLabel(String),
    MissingAllowLabel(Vec<String>),
}

impl AutomationDecision {
    pub fn is_allowed(&self) -> bool {
        matches!(self, Self::Allowed)
    }

    pub fn reason(&self) -> String {
        match self {
            Self::Allowed => "policy allows automation".to_string(),
            Self::BlockedByLabel(label) => format!("blocked by policy label `{label}`"),
            Self::MissingAllowLabel(labels) if labels.len() == 1 => {
                format!("missing required policy allow label `{}`", labels[0])
            }
            Self::MissingAllowLabel(labels) => {
                format!(
                    "missing one of required policy allow labels: {}",
                    labels.join(", ")
                )
            }
        }
    }
}

#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq, Eq)]
pub struct AgentConfig {
    #[serde(default)]
    pub triage: AgentRoleConfig,
    #[serde(default)]
    pub developer: AgentRoleConfig,
    #[serde(default)]
    pub reviewer: AgentRoleConfig,
    #[serde(default)]
    pub repair: AgentRoleConfig,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq, Eq)]
pub struct AgentRoleConfig {
    pub enabled: bool,
    #[serde(default)]
    pub command: Vec<String>,
    #[serde(default)]
    pub plugin: Option<PluginFlowSelection>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct PluginFlowSelection {
    pub manifest_path: String,
    pub flow: String,
    #[serde(default)]
    pub max_handoffs_per_edge: Option<u32>,
    #[serde(default)]
    pub environment: BTreeMap<String, String>,
    #[serde(default)]
    #[serde(alias = "stage_access_overrides")]
    pub task_access_overrides: BTreeMap<String, TaskAccessOverride>,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq, Eq)]
pub struct TaskAccessOverride {
    #[serde(default)]
    pub read: Option<Vec<String>>,
    #[serde(default)]
    pub write: Option<Vec<String>>,
}

pub type StageAccessOverride = TaskAccessOverride;

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct CheckPolicy {
    #[serde(default)]
    pub required_commands: Vec<RequiredCommand>,
    #[serde(default)]
    pub require_github_checks: bool,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct RequiredCommand {
    pub name: String,
    pub command: Vec<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct RiskPolicy {
    pub default: String,
    pub agent_classification: bool,
    pub route_unknown_to_human: bool,
    pub route_high_to_human: bool,
    #[serde(default)]
    pub human_review_paths: Vec<String>,
}

impl RiskPolicy {
    pub fn apply_result_routing(&self, result: &mut RunResult) -> Option<String> {
        if result.outcome != Outcome::Ready && result.outcome != Outcome::Implemented {
            return None;
        }

        let reason = if self.route_high_to_human && result.risk == Risk::High {
            Some("policy routes high-risk work to human review".to_string())
        } else if self.route_unknown_to_human && result.risk == Risk::Unknown {
            Some("policy routes unknown-risk work to human review".to_string())
        } else if let Some(pattern) = self.human_review_paths.iter().find(|pattern| {
            result
                .changed_files
                .iter()
                .any(|path| path_matches(pattern, path))
        }) {
            Some(format!(
                "policy requires human review for `{pattern}` changes"
            ))
        } else {
            None
        };

        if let Some(reason) = reason {
            result.outcome = Outcome::NeedsHuman;
            result.human_review_reason = Some(reason.clone());
            Some(reason)
        } else {
            None
        }
    }
}

fn path_matches(pattern: &str, path: &str) -> bool {
    let pattern = pattern.trim_matches('/');
    let path = path.trim_matches('/');

    if pattern == path {
        return true;
    }

    if let Some(suffix) = pattern.strip_prefix("**/") {
        if let Some(directory) = suffix.strip_suffix("/**") {
            return path == directory
                || path.starts_with(&format!("{directory}/"))
                || path.contains(&format!("/{directory}/"));
        }

        return path.ends_with(suffix);
    }

    if let Some(prefix) = pattern.strip_suffix("/**") {
        return path == prefix || path.starts_with(&format!("{prefix}/"));
    }

    false
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct AutomationPolicy {
    pub max_concurrent_jobs: u32,
    pub retry_failed_jobs: bool,
    pub auto_merge: bool,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq, Eq)]
pub struct DashboardPolicy {
    #[serde(default)]
    pub expose_policy: bool,
    #[serde(default)]
    pub allow_retry: bool,
    #[serde(default)]
    pub allow_cancel: bool,
}

#[cfg(test)]
mod tests {
    use super::Policy;
    use crate::{Confidence, Outcome, Risk, RunResult};

    #[test]
    fn parses_example_policy() {
        let policy = Policy::from_yaml(include_str!("../../../docs/policy.example.yml")).unwrap();

        assert_eq!(policy.version, 1);
        assert!(policy.agents.triage.enabled);
        assert!(policy.agents.repair.enabled);
        assert_eq!(policy.workflow.state_labels["ready"], "ai:ready");
        assert!(policy.workflow.engagement.initial.token_owner);
        assert!(policy.workflow.engagement.human_authorization.token_owner);
    }

    #[test]
    fn repair_agent_defaults_disabled_for_older_policy_files() {
        let policy = Policy::from_yaml(
            r#"
version: 1
workflow:
  state_labels:
    ready: "ai:ready"
agents:
  triage:
    enabled: true
    command: ["triage"]
  developer:
    enabled: true
    command: ["developer"]
  reviewer:
    enabled: true
    command: ["reviewer"]
checks: {}
risk:
  default: "unknown"
  agent_classification: true
  route_unknown_to_human: true
  route_high_to_human: true
automation:
  max_concurrent_jobs: 1
  retry_failed_jobs: false
  auto_merge: false
"#,
        )
        .unwrap();

        assert!(!policy.agents.repair.enabled);
        assert!(policy.agents.repair.command.is_empty());
    }

    #[test]
    fn automation_requires_allow_label_when_configured() {
        let policy = Policy::from_yaml(include_str!("../../../docs/policy.example.yml")).unwrap();

        assert!(
            !policy
                .automation_decision_for_labels(&["bug".to_string()])
                .is_allowed()
        );
        assert!(
            policy
                .automation_decision_for_labels(&["bug".to_string(), "ai".to_string()])
                .is_allowed()
        );
    }

    #[test]
    fn lifecycle_policy_can_omit_default_agents() {
        let policy =
            Policy::from_yaml(include_str!("../../../docs/policy.plugin.example.yml")).unwrap();

        assert_eq!(
            policy
                .lifecycle
                .plugin
                .as_ref()
                .map(|plugin| plugin.flow.as_str()),
            Some("work_items")
        );
        assert!(!policy.agents.triage.enabled);
        assert!(!policy.agents.developer.enabled);
    }

    #[test]
    fn block_label_wins_over_allow_label() {
        let policy = Policy::from_yaml(include_str!("../../../docs/policy.example.yml")).unwrap();
        let decision =
            policy.automation_decision_for_labels(&["ai".to_string(), "ai:disabled".to_string()]);

        assert!(!decision.is_allowed());
        assert!(decision.reason().contains("ai:disabled"));
    }

    #[test]
    fn unknown_ready_result_routes_to_human_when_enabled() {
        let policy = Policy::from_yaml(include_str!("../../../docs/policy.example.yml")).unwrap();
        let mut result = RunResult {
            outcome: Outcome::Ready,
            summary: "ready".to_string(),
            confidence: Confidence::High,
            risk: Risk::Unknown,
            questions: Vec::new(),
            tests: Vec::new(),
            changed_files: Vec::new(),
            human_review_reason: None,
            blocked_reason: None,
        };

        let reason = policy.apply_result_routing(&mut result).unwrap();

        assert_eq!(result.outcome, Outcome::NeedsHuman);
        assert!(reason.contains("unknown-risk"));
    }

    #[test]
    fn human_review_path_routes_implemented_result_to_human() {
        let policy = Policy::from_yaml(include_str!("../../../docs/policy.example.yml")).unwrap();
        let mut result = RunResult {
            outcome: Outcome::Implemented,
            summary: "implemented".to_string(),
            confidence: Confidence::High,
            risk: Risk::Low,
            questions: Vec::new(),
            tests: Vec::new(),
            changed_files: vec!["migrations/0002_add_column.sql".to_string()],
            human_review_reason: None,
            blocked_reason: None,
        };

        let reason = policy.apply_result_routing(&mut result).unwrap();

        assert_eq!(result.outcome, Outcome::NeedsHuman);
        assert!(reason.contains("migrations/**"));
    }

    #[test]
    fn nested_human_review_path_routes_implemented_result_to_human() {
        let policy = Policy::from_yaml(include_str!("../../../docs/policy.example.yml")).unwrap();
        let mut result = RunResult {
            outcome: Outcome::Implemented,
            summary: "implemented".to_string(),
            confidence: Confidence::High,
            risk: Risk::Low,
            questions: Vec::new(),
            tests: Vec::new(),
            changed_files: vec!["crates/api/src/security/token.rs".to_string()],
            human_review_reason: None,
            blocked_reason: None,
        };

        let reason = policy.apply_result_routing(&mut result).unwrap();

        assert_eq!(result.outcome, Outcome::NeedsHuman);
        assert!(reason.contains("**/security/**"));
    }
}
