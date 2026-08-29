use crate::{FacadeConfig, Outcome, Risk, RunResult};
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
    #[serde(default)]
    pub facade: FacadeConfig,
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
        policy.facade.validate().map_err(PolicyError::Invalid)?;
        if let Some(public_url) = &policy.dashboard.public_url
            && !(public_url.starts_with("https://") || public_url.starts_with("http://localhost"))
        {
            return Err(PolicyError::Invalid(
                "dashboard.public_url must use HTTPS (or http://localhost for development)".into(),
            ));
        }
        policy.workflow.engagement.validate()?;
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
pub struct EngagementPolicy {
    #[serde(default)]
    pub default: EngagementRule,
    #[serde(default)]
    pub initial: Option<EngagementRule>,
    #[serde(default)]
    pub needs_info_resume: Option<EngagementRule>,
    #[serde(default)]
    pub blocked_resume: Option<EngagementRule>,
    #[serde(default)]
    pub needs_human_resume: Option<EngagementRule>,
    #[serde(default)]
    pub repositories: BTreeMap<String, RepositoryEngagementPolicy>,
}

impl EngagementPolicy {
    pub fn rule(&self, gate: EngagementGate, repository: Option<&str>) -> &EngagementRule {
        if let Some(repository) = repository
            && let Some(rules) = self
                .repositories
                .iter()
                .find_map(|(name, rules)| name.eq_ignore_ascii_case(repository).then_some(rules))
        {
            return rules.rule(gate);
        }
        self.global_rule(gate)
    }

    fn global_rule(&self, gate: EngagementGate) -> &EngagementRule {
        match gate {
            EngagementGate::Initial => self.initial.as_ref(),
            EngagementGate::NeedsInfoResume => self.needs_info_resume.as_ref(),
            EngagementGate::BlockedResume => self.blocked_resume.as_ref(),
            EngagementGate::NeedsHumanResume => self.needs_human_resume.as_ref(),
        }
        .unwrap_or(&self.default)
    }

    fn validate(&self) -> Result<(), PolicyError> {
        for rule in [
            Some(&self.default),
            self.initial.as_ref(),
            self.needs_info_resume.as_ref(),
            self.blocked_resume.as_ref(),
            self.needs_human_resume.as_ref(),
        ]
        .into_iter()
        .flatten()
        {
            rule.validate()?;
        }
        for (repository, rules) in &self.repositories {
            validate_repository_name(repository)?;
            rules.validate()?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq, Eq)]
pub struct RepositoryEngagementPolicy {
    #[serde(default)]
    pub default: EngagementRule,
    #[serde(default)]
    pub initial: Option<EngagementRule>,
    #[serde(default)]
    pub needs_info_resume: Option<EngagementRule>,
    #[serde(default)]
    pub blocked_resume: Option<EngagementRule>,
    #[serde(default)]
    pub needs_human_resume: Option<EngagementRule>,
}

impl RepositoryEngagementPolicy {
    fn rule(&self, gate: EngagementGate) -> &EngagementRule {
        match gate {
            EngagementGate::Initial => self.initial.as_ref(),
            EngagementGate::NeedsInfoResume => self.needs_info_resume.as_ref(),
            EngagementGate::BlockedResume => self.blocked_resume.as_ref(),
            EngagementGate::NeedsHumanResume => self.needs_human_resume.as_ref(),
        }
        .unwrap_or(&self.default)
    }

    fn validate(&self) -> Result<(), PolicyError> {
        for rule in [
            Some(&self.default),
            self.initial.as_ref(),
            self.needs_info_resume.as_ref(),
            self.blocked_resume.as_ref(),
            self.needs_human_resume.as_ref(),
        ]
        .into_iter()
        .flatten()
        {
            rule.validate()?;
        }
        Ok(())
    }
}

fn validate_repository_name(repository: &str) -> Result<(), PolicyError> {
    let valid = repository
        .split_once('/')
        .is_some_and(|(owner, name)| !owner.is_empty() && !name.is_empty() && !name.contains('/'));
    if valid {
        Ok(())
    } else {
        Err(PolicyError::Invalid(format!(
            "engagement repository `{repository}` must use owner/name"
        )))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EngagementGate {
    Initial,
    NeedsInfoResume,
    BlockedResume,
    NeedsHumanResume,
}

impl EngagementGate {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Initial => "initial",
            Self::NeedsInfoResume => "needs_info_resume",
            Self::BlockedResume => "blocked_resume",
            Self::NeedsHumanResume => "needs_human_resume",
        }
    }
}

#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq, Eq)]
pub struct EngagementRule {
    #[serde(default)]
    pub required_labels: Vec<String>,
    #[serde(default)]
    pub allow: Vec<EngagementSelector>,
}

impl EngagementRule {
    fn validate(&self) -> Result<(), PolicyError> {
        for selector in &self.allow {
            selector.validate()?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum EngagementSelector {
    TokenOwner,
    AnyUser,
    User {
        login: String,
    },
    IssueAuthor,
    RepositoryOwner,
    RepositoryOrganizationMember,
    OrganizationMember {
        organization: String,
    },
    TeamMember {
        organization: String,
        team_slug: String,
    },
    AuthorAssociation {
        association: String,
    },
    CollaboratorPermission {
        minimum: String,
    },
    Bot {
        login: String,
    },
    #[serde(rename = "github_app")]
    GitHubApp {
        id: Option<u64>,
        slug: Option<String>,
    },
}

impl EngagementSelector {
    fn validate(&self) -> Result<(), PolicyError> {
        let nonempty = |name: &str, value: &str| {
            if value.trim().is_empty() {
                Err(PolicyError::Invalid(format!(
                    "engagement selector `{name}` cannot be empty"
                )))
            } else {
                Ok(())
            }
        };
        match self {
            Self::User { login } | Self::Bot { login } => nonempty("login", login),
            Self::OrganizationMember { organization } => nonempty("organization", organization),
            Self::TeamMember {
                organization,
                team_slug,
            } => {
                nonempty("organization", organization)?;
                nonempty("team_slug", team_slug)
            }
            Self::AuthorAssociation { association } => {
                const ASSOCIATIONS: &[&str] = &[
                    "COLLABORATOR",
                    "CONTRIBUTOR",
                    "FIRST_TIMER",
                    "FIRST_TIME_CONTRIBUTOR",
                    "MANNEQUIN",
                    "MEMBER",
                    "NONE",
                    "OWNER",
                ];
                if ASSOCIATIONS.contains(&association.as_str()) {
                    Ok(())
                } else {
                    Err(PolicyError::Invalid(format!(
                        "unknown GitHub author association `{association}`"
                    )))
                }
            }
            Self::CollaboratorPermission { minimum } => {
                if ["read", "triage", "write", "maintain", "admin"].contains(&minimum.as_str()) {
                    Ok(())
                } else {
                    Err(PolicyError::Invalid(format!(
                        "unknown GitHub collaborator permission `{minimum}`"
                    )))
                }
            }
            Self::GitHubApp { id, slug } => match (id, slug) {
                (Some(_), None) => Ok(()),
                (None, Some(slug)) => nonempty("slug", slug),
                _ => Err(PolicyError::Invalid(
                    "github_app selector requires exactly one of `id` or `slug`".into(),
                )),
            },
            Self::TokenOwner
            | Self::AnyUser
            | Self::IssueAuthor
            | Self::RepositoryOwner
            | Self::RepositoryOrganizationMember => Ok(()),
        }
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
    pub parameters: BTreeMap<String, serde_json::Value>,
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
        } else {
            self.human_review_paths
                .iter()
                .find(|pattern| {
                    result
                        .changed_files
                        .iter()
                        .any(|path| path_matches(pattern, path))
                })
                .map(|pattern| format!("policy requires human review for `{pattern}` changes"))
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub public_url: Option<String>,
    #[serde(default)]
    pub expose_policy: bool,
    #[serde(default)]
    pub allow_retry: bool,
    #[serde(default)]
    pub allow_cancel: bool,
}

#[cfg(test)]
mod tests {
    use super::{EngagementGate, EngagementRule, EngagementSelector, Policy};
    use crate::{Confidence, Outcome, Risk, RunResult};

    #[test]
    fn parses_example_policy() {
        let policy = Policy::from_yaml(include_str!("../../../docs/policy.example.yml")).unwrap();

        assert_eq!(policy.version, 1);
        assert!(policy.agents.triage.enabled);
        assert!(policy.agents.repair.enabled);
        assert_eq!(policy.workflow.state_labels["ready"], "ai:ready");
    }

    #[test]
    fn omitted_engagement_denies_every_gate() {
        let policy = Policy::from_yaml(
            r#"
version: 1
workflow: { state_labels: {} }
checks: {}
risk:
  default: unknown
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

        for gate in [
            EngagementGate::Initial,
            EngagementGate::NeedsInfoResume,
            EngagementGate::BlockedResume,
            EngagementGate::NeedsHumanResume,
        ] {
            assert!(policy.workflow.engagement.rule(gate, None).allow.is_empty());
        }
    }

    #[test]
    fn engagement_gate_override_and_full_selectors_parse() {
        let mut yaml = include_str!("../../../docs/policy.example.yml").to_string();
        yaml = yaml.replace(
            "    repositories: {}",
            r#"    repositories:
      "acme/rtl":
        default:
          allow:
            - { type: user, login: alice }
        blocked_resume:
          allow: []
        needs_human_resume:
          allow:
            - { type: user, login: alice }
            - { type: collaborator_permission, minimum: write }"#,
        );
        let policy = Policy::from_yaml(&yaml).unwrap();

        assert!(
            policy
                .workflow
                .engagement
                .rule(EngagementGate::BlockedResume, Some("ACME/RTL"))
                .allow
                .is_empty()
        );
        assert!(matches!(
            policy
                .workflow
                .engagement
                .rule(EngagementGate::NeedsHumanResume, Some("acme/rtl"))
                .allow[1],
            EngagementSelector::CollaboratorPermission { .. }
        ));
    }

    #[test]
    fn all_engagement_selector_shapes_parse() {
        let rule: EngagementRule = serde_yaml::from_str(
            r#"
allow:
  - { type: token_owner }
  - { type: any_user }
  - { type: user, login: alice }
  - { type: issue_author }
  - { type: repository_owner }
  - { type: repository_organization_member }
  - { type: organization_member, organization: acme }
  - { type: team_member, organization: acme, team_slug: maintainers }
  - { type: author_association, association: OWNER }
  - { type: collaborator_permission, minimum: maintain }
  - { type: bot, login: "dependabot[bot]" }
  - { type: github_app, id: 123 }
  - { type: github_app, slug: dependabot }
"#,
        )
        .unwrap();

        assert_eq!(rule.allow.len(), 13);
        for selector in &rule.allow {
            selector.validate().unwrap();
        }
    }

    #[test]
    fn invalid_engagement_selector_fails_policy_loading() {
        let yaml = include_str!("../../../docs/policy.example.yml").replace(
            "    repositories: {}",
            r#"    repositories:
      "acme/rtl":
        default:
          allow:
            - { type: collaborator_permission, minimum: superuser }"#,
        );
        assert!(Policy::from_yaml(&yaml).is_err());
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
