use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum PolicyError {
    #[error("failed to parse policy yaml: {0}")]
    Parse(#[from] serde_yaml::Error),
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct Policy {
    pub version: u32,
    pub workflow: WorkflowPolicy,
    pub agents: AgentConfig,
    pub checks: CheckPolicy,
    pub risk: RiskPolicy,
    pub automation: AutomationPolicy,
    #[serde(default)]
    pub dashboard: DashboardPolicy,
}

impl Policy {
    pub fn from_yaml(input: &str) -> Result<Self, PolicyError> {
        Ok(serde_yaml::from_str(input)?)
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct WorkflowPolicy {
    pub state_labels: BTreeMap<String, String>,
    #[serde(default)]
    pub block_labels: Vec<String>,
    #[serde(default)]
    pub allow_labels: Vec<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct AgentConfig {
    pub triage: AgentRoleConfig,
    pub developer: AgentRoleConfig,
    pub reviewer: AgentRoleConfig,
    #[serde(default)]
    pub repair: AgentRoleConfig,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct AgentRoleConfig {
    pub enabled: bool,
    pub command: Vec<String>,
}

impl Default for AgentRoleConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            command: Vec::new(),
        }
    }
}

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

    #[test]
    fn parses_example_policy() {
        let policy = Policy::from_yaml(include_str!("../../../docs/policy.example.yml")).unwrap();

        assert_eq!(policy.version, 1);
        assert!(policy.agents.triage.enabled);
        assert!(policy.agents.repair.enabled);
        assert_eq!(policy.workflow.state_labels["ready"], "ai:ready");
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
}
