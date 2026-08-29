use crate::FacadeConfig;
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, fs, path::Path};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum PluginError {
    #[error("failed to read plugin manifest: {0}")]
    Io(#[from] std::io::Error),
    #[error("failed to parse plugin manifest: {0}")]
    Parse(#[from] serde_yaml::Error),
    #[error("invalid plugin manifest: {0}")]
    Invalid(String),
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct PluginManifest {
    pub api_version: u32,
    pub id: String,
    #[serde(default)]
    pub facade: FacadeConfig,
    pub runtime: PluginRuntime,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub installation: Option<PluginInstallation>,
    #[serde(default)]
    pub parameters: BTreeMap<String, PluginParameter>,
    #[serde(default)]
    pub resources: BTreeMap<String, PluginResource>,
    #[serde(default)]
    pub mcp_servers: BTreeMap<String, McpServerDefinition>,
    #[serde(alias = "agents")]
    pub roles: BTreeMap<String, PluginRole>,
    pub flows: BTreeMap<String, PluginFlow>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct PluginRuntime {
    pub default_image: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct PluginInstallation {
    #[serde(default)]
    pub build: PluginBuild,
    #[serde(default)]
    pub environment: BTreeMap<String, PluginEnvironmentVariable>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct PluginBuild {
    #[serde(default = "default_build_context")]
    pub context: String,
    #[serde(default = "default_dockerfile")]
    pub dockerfile: String,
}

impl Default for PluginBuild {
    fn default() -> Self {
        Self {
            context: default_build_context(),
            dockerfile: default_dockerfile(),
        }
    }
}

fn default_build_context() -> String {
    ".".into()
}

fn default_dockerfile() -> String {
    "Dockerfile".into()
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct PluginEnvironmentVariable {
    pub description: String,
    #[serde(default)]
    pub required: bool,
    #[serde(default)]
    pub secret: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct PluginRole {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,
    pub command: Vec<String>,
    #[serde(default)]
    pub image: Option<String>,
    #[serde(default)]
    pub environment: Vec<String>,
    #[serde(default)]
    pub mcp_servers: Vec<String>,
    #[serde(default)]
    pub resources: Vec<PluginResourceAssignment>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum PluginParameter {
    Path {
        #[serde(default)]
        default: Option<String>,
    },
    Enum {
        values: Vec<String>,
        #[serde(default)]
        default: Option<String>,
    },
    String {
        #[serde(default)]
        default: Option<String>,
    },
    Integer {
        #[serde(default)]
        default: Option<i64>,
    },
    Boolean {
        #[serde(default)]
        default: Option<bool>,
    },
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PluginResource {
    pub source: PluginResourceSource,
    pub path: String,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PluginResourceSource {
    Plugin,
    Repository,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PluginResourceAssignment {
    pub id: String,
    #[serde(default)]
    pub required: bool,
}

/// Backward-compatible Rust name for integrations built against the serial
/// prototype. New manifests and code should use `PluginRole`.
pub type PluginAgent = PluginRole;

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct PluginFlow {
    pub start: String,
    #[serde(default)]
    pub replaces_default_lifecycle: bool,
    #[serde(default)]
    pub work_items_path: Option<String>,
    /// Project planner-created work items and dependencies into GitHub issues.
    /// Donkeyspace remains the scheduling source of truth.
    #[serde(default)]
    pub project_github_issues: bool,
    #[serde(default = "default_max_handoffs")]
    pub max_handoffs_per_edge: u32,
    #[serde(default = "default_max_parallel_tasks")]
    pub max_parallel_tasks: usize,
    #[serde(alias = "stages")]
    pub tasks: BTreeMap<String, PluginTask>,
}

fn default_max_handoffs() -> u32 {
    2
}

fn default_max_parallel_tasks() -> usize {
    4
}

#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PluginApprovalMode {
    #[default]
    None,
    Required,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct PluginTask {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub approval_subject: Option<String>,
    #[serde(alias = "agent")]
    pub role: String,
    #[serde(default)]
    pub scope: PluginTaskScope,
    #[serde(default)]
    pub dependencies: Vec<String>,
    #[serde(default)]
    pub depends_on_work_items: bool,
    #[serde(default)]
    pub read: Vec<String>,
    #[serde(default)]
    pub write: Vec<String>,
    #[serde(default)]
    pub resources: Vec<PluginResourceAssignment>,
    #[serde(default)]
    pub artifacts: Vec<PluginArtifact>,
    /// Optional text diagnostics to preserve on a forensic attempt branch.
    /// Unlike publishable artifacts, these never enter the aggregate checkout.
    #[serde(default)]
    pub diagnostics: Vec<PluginArtifact>,
    #[serde(default)]
    pub validators: Vec<PluginValidator>,
    #[serde(default)]
    pub allowed_handoffs: Vec<String>,
    #[serde(default)]
    pub handoff_descriptions: BTreeMap<String, String>,
    #[serde(default)]
    pub transitions: BTreeMap<String, String>,
    #[serde(default)]
    pub terminal: bool,
    /// Pause after a successful task result until an authorized human approves
    /// the published output. Agents may still request human input themselves
    /// by returning `needs_human` when this is `none`.
    #[serde(default)]
    pub approval: PluginApprovalMode,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PluginArtifact {
    pub path: String,
    #[serde(rename = "type")]
    pub kind: PluginArtifactType,
    #[serde(default)]
    pub required: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PluginArtifactType {
    File,
    Directory,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PluginValidator {
    pub name: String,
    pub command: Vec<String>,
}

/// Backward-compatible Rust name for the former stage abstraction.
pub type PluginStage = PluginTask;

#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PluginTaskScope {
    #[default]
    Workflow,
    WorkItem,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct PluginWorkItemRegistry {
    pub work_items: Vec<PluginWorkItem>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct PluginWorkItem {
    pub id: String,
    pub spec: String,
    #[serde(default)]
    pub depends_on: Vec<String>,
    #[serde(default)]
    pub metadata: BTreeMap<String, serde_json::Value>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(tag = "transport", rename_all = "snake_case")]
pub enum McpServerDefinition {
    Stdio {
        command: Vec<String>,
        #[serde(default)]
        environment: Vec<String>,
    },
    Http {
        url: String,
        #[serde(default)]
        environment: Vec<String>,
    },
}

impl PluginManifest {
    pub fn from_path(path: impl AsRef<Path>) -> Result<Self, PluginError> {
        let raw = fs::read_to_string(path)?;
        let manifest: Self = serde_yaml::from_str(&raw)?;
        manifest.validate()?;
        Ok(manifest)
    }

    pub fn validate(&self) -> Result<(), PluginError> {
        if self.api_version != 1 {
            return Err(PluginError::Invalid("api_version must be 1".into()));
        }
        if self.id.trim().is_empty() || self.runtime.default_image.is_empty() {
            return Err(PluginError::Invalid(
                "id and runtime.default_image are required".into(),
            ));
        }
        self.facade.validate().map_err(PluginError::Invalid)?;
        if let Some(installation) = &self.installation {
            validate_relative_path(&installation.build.context)?;
            validate_relative_path(&installation.build.dockerfile)?;
            for (name, variable) in &installation.environment {
                if !is_environment_name(name) {
                    return Err(PluginError::Invalid(format!(
                        "invalid installation environment name `{name}`"
                    )));
                }
                if variable.description.trim().is_empty() {
                    return Err(PluginError::Invalid(format!(
                        "installation environment `{name}` requires a description"
                    )));
                }
                if variable.secret && variable.default.is_some() {
                    return Err(PluginError::Invalid(format!(
                        "secret installation environment `{name}` cannot have a default"
                    )));
                }
                if !self
                    .roles
                    .values()
                    .any(|role| role.environment.iter().any(|allowed| allowed == name))
                {
                    return Err(PluginError::Invalid(format!(
                        "installation environment `{name}` is not allowed by any role"
                    )));
                }
            }
        }
        for (name, parameter) in &self.parameters {
            validate_safe_id("parameter", name)?;
            match parameter {
                PluginParameter::Path {
                    default: Some(value),
                } => validate_relative_path(value)?,
                PluginParameter::Enum { values, default } => {
                    if values.is_empty() || values.iter().any(|value| !is_safe_enum(value)) {
                        return Err(PluginError::Invalid(format!(
                            "parameter `{name}` has no safe enum values"
                        )));
                    }
                    if default
                        .as_ref()
                        .is_some_and(|value| !values.contains(value))
                    {
                        return Err(PluginError::Invalid(format!(
                            "parameter `{name}` default is not an allowed value"
                        )));
                    }
                }
                _ => {}
            }
        }
        for (id, resource) in &self.resources {
            validate_safe_id("resource", id)?;
            validate_filesystem_template(&resource.path, &self.parameters, false)?;
        }
        for (name, role) in &self.roles {
            if let Some(display_name) = &role.display_name {
                validate_display_text("role display_name", display_name)?;
            }
            if role.command.is_empty() {
                return Err(PluginError::Invalid(format!(
                    "role `{name}` command is empty"
                )));
            }
            for server in &role.mcp_servers {
                if !self.mcp_servers.contains_key(server) {
                    return Err(PluginError::Invalid(format!(
                        "role `{name}` references unknown MCP server `{server}`"
                    )));
                }
            }
            validate_resource_assignments(&role.resources, &self.resources, name)?;
        }
        for (flow_name, flow) in &self.flows {
            if !flow.tasks.contains_key(&flow.start) {
                return Err(PluginError::Invalid(format!(
                    "flow `{flow_name}` has unknown start task"
                )));
            }
            if flow.max_handoffs_per_edge == 0 {
                return Err(PluginError::Invalid(format!(
                    "flow `{flow_name}` must allow at least one handoff"
                )));
            }
            if flow.max_parallel_tasks == 0 {
                return Err(PluginError::Invalid(format!(
                    "flow `{flow_name}` must allow at least one parallel task"
                )));
            }
            if flow.replaces_default_lifecycle && flow.work_items_path.is_none() {
                return Err(PluginError::Invalid(format!(
                    "lifecycle-replacing flow `{flow_name}` requires work_items_path"
                )));
            }
            if flow.replaces_default_lifecycle
                && flow.tasks[&flow.start].scope != PluginTaskScope::Workflow
            {
                return Err(PluginError::Invalid(format!(
                    "lifecycle-replacing flow `{flow_name}` start task must have workflow scope"
                )));
            }
            if let Some(path) = &flow.work_items_path {
                validate_filesystem_template(path, &self.parameters, false)?;
            }
            for (task_name, task) in &flow.tasks {
                if let Some(display_name) = &task.display_name {
                    validate_display_text("task display_name", display_name)?;
                }
                if let Some(subject) = &task.approval_subject {
                    validate_display_text("task approval_subject", subject)?;
                }
                if !self.roles.contains_key(&task.role) {
                    return Err(PluginError::Invalid(format!(
                        "task `{task_name}` references unknown role `{}`",
                        task.role
                    )));
                }
                for path in task.read.iter().chain(&task.write) {
                    validate_filesystem_template(path, &self.parameters, true)?;
                }
                if task.scope == PluginTaskScope::WorkItem
                    && task.write.iter().any(|path| !path.contains("{work_item}"))
                {
                    return Err(PluginError::Invalid(format!(
                        "work-item task `{task_name}` write paths must contain `{{work_item}}`"
                    )));
                }
                validate_resource_assignments(&task.resources, &self.resources, task_name)?;
                for artifact in &task.artifacts {
                    if let Some(display_name) = &artifact.display_name {
                        validate_display_text("artifact display_name", display_name)?;
                    }
                    if artifact.path.contains(['*', '?', '[', ']']) {
                        return Err(PluginError::Invalid(format!(
                            "task `{task_name}` artifact paths must be exact"
                        )));
                    }
                    validate_filesystem_template(&artifact.path, &self.parameters, true)?;
                }
                for diagnostic in &task.diagnostics {
                    if let Some(display_name) = &diagnostic.display_name {
                        validate_display_text("diagnostic display_name", display_name)?;
                    }
                    if diagnostic.path.contains(['*', '?', '[', ']']) {
                        return Err(PluginError::Invalid(format!(
                            "task `{task_name}` diagnostic paths must be exact"
                        )));
                    }
                    validate_filesystem_template(&diagnostic.path, &self.parameters, true)?;
                }
                for validator in &task.validators {
                    if validator.name.trim().is_empty() || validator.command.is_empty() {
                        return Err(PluginError::Invalid(format!(
                            "task `{task_name}` has an invalid validator"
                        )));
                    }
                }
                for dependency in &task.dependencies {
                    if !flow.tasks.contains_key(dependency) || dependency == task_name {
                        return Err(PluginError::Invalid(format!(
                            "task `{task_name}` has invalid dependency `{dependency}`"
                        )));
                    }
                }
                for target in task.transitions.values().chain(&task.allowed_handoffs) {
                    if !flow.tasks.contains_key(target) {
                        return Err(PluginError::Invalid(format!(
                            "task `{task_name}` references unknown target `{target}`"
                        )));
                    }
                }
                for (target, description) in &task.handoff_descriptions {
                    if !task.allowed_handoffs.contains(target) {
                        return Err(PluginError::Invalid(format!(
                            "task `{task_name}` describes undeclared handoff `{target}`"
                        )));
                    }
                    validate_display_text("handoff description", description)?;
                }
            }
            validate_task_graph(flow_name, flow)?;
        }
        Ok(())
    }
}

fn validate_display_text(field: &str, value: &str) -> Result<(), PluginError> {
    if value.trim().is_empty() || value.len() > 160 || value.chars().any(char::is_control) {
        return Err(PluginError::Invalid(format!(
            "{field} must be nonempty single-line text no longer than 160 bytes"
        )));
    }
    Ok(())
}

fn is_environment_name(value: &str) -> bool {
    !value.is_empty()
        && value.chars().all(|character| {
            character.is_ascii_uppercase() || character.is_ascii_digit() || character == '_'
        })
        && value.as_bytes()[0].is_ascii_uppercase()
}

fn validate_resource_assignments(
    assignments: &[PluginResourceAssignment],
    resources: &BTreeMap<String, PluginResource>,
    owner: &str,
) -> Result<(), PluginError> {
    for assignment in assignments {
        if !resources.contains_key(&assignment.id) {
            return Err(PluginError::Invalid(format!(
                "`{owner}` references unknown resource `{}`",
                assignment.id
            )));
        }
    }
    Ok(())
}

fn validate_safe_id(kind: &str, value: &str) -> Result<(), PluginError> {
    if value.is_empty()
        || !value
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || matches!(character, '-' | '_'))
    {
        return Err(PluginError::Invalid(format!("unsafe {kind} id `{value}`")));
    }
    Ok(())
}

fn is_safe_enum(value: &str) -> bool {
    !value.is_empty()
        && value
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || matches!(character, '-' | '_'))
}

fn validate_filesystem_template(
    value: &str,
    parameters: &BTreeMap<String, PluginParameter>,
    allow_work_item: bool,
) -> Result<(), PluginError> {
    let mut expanded = value.to_string();
    for placeholder in placeholders(value)? {
        if allow_work_item && placeholder == "work_item" {
            expanded = expanded.replace("{work_item}", "item");
            continue;
        }
        let Some(parameter) = parameters.get(&placeholder) else {
            return Err(PluginError::Invalid(format!(
                "unknown filesystem placeholder `{{{placeholder}}}`"
            )));
        };
        if !matches!(
            parameter,
            PluginParameter::Path { .. } | PluginParameter::Enum { .. }
        ) {
            return Err(PluginError::Invalid(format!(
                "parameter `{placeholder}` cannot be used in a filesystem field"
            )));
        }
        expanded = expanded.replace(&format!("{{{placeholder}}}"), "value");
    }
    validate_relative_path(&expanded)
}

fn placeholders(value: &str) -> Result<Vec<String>, PluginError> {
    let mut result = Vec::new();
    let mut rest = value;
    while let Some(start) = rest.find('{') {
        let after = &rest[start + 1..];
        let Some(end) = after.find('}') else {
            return Err(PluginError::Invalid(format!(
                "unclosed placeholder in `{value}`"
            )));
        };
        let name = &after[..end];
        if name.is_empty() || name.contains('{') {
            return Err(PluginError::Invalid(format!(
                "invalid placeholder in `{value}`"
            )));
        }
        result.push(name.to_string());
        rest = &after[end + 1..];
    }
    if rest.contains('}') {
        return Err(PluginError::Invalid(format!(
            "unmatched placeholder in `{value}`"
        )));
    }
    Ok(result)
}

fn validate_task_graph(flow_name: &str, flow: &PluginFlow) -> Result<(), PluginError> {
    fn visit(
        name: &str,
        flow: &PluginFlow,
        visiting: &mut Vec<String>,
        visited: &mut Vec<String>,
    ) -> Result<(), PluginError> {
        if visited.iter().any(|item| item == name) {
            return Ok(());
        }
        if visiting.iter().any(|item| item == name) {
            return Err(PluginError::Invalid(format!(
                "contains a dependency cycle at task `{name}`"
            )));
        }
        visiting.push(name.to_string());
        for dependency in &flow.tasks[name].dependencies {
            visit(dependency, flow, visiting, visited)?;
        }
        visiting.retain(|item| item != name);
        visited.push(name.to_string());
        Ok(())
    }

    let mut visiting = Vec::new();
    let mut visited = Vec::new();
    for name in flow.tasks.keys() {
        visit(name, flow, &mut visiting, &mut visited).map_err(|error| match error {
            PluginError::Invalid(message) => {
                PluginError::Invalid(format!("flow `{flow_name}` {message}"))
            }
            other => other,
        })?;
    }
    Ok(())
}

fn validate_relative_path(value: &str) -> Result<(), PluginError> {
    let path = Path::new(value);
    if value.trim().is_empty()
        || value.contains(['{', '}'])
        || path.is_absolute()
        || path
            .components()
            .any(|part| matches!(part, std::path::Component::ParentDir))
    {
        return Err(PluginError::Invalid(format!("unsafe task path `{value}`")));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn rejects_path_traversal() {
        assert!(validate_relative_path("../dv").is_err());
        assert!(validate_relative_path("rtl").is_ok());
    }

    #[test]
    fn parses_and_validates_a_serial_flow() {
        let manifest: PluginManifest = serde_yaml::from_str(
            r#"
api_version: 1
id: example.rtl
facade: { display_name: Example Platform, tagline: Hardware agents, command: example-agent }
runtime: { default_image: example:dev }
agents:
  rtl: { command: [run-rtl] }
flows:
  rtl_module:
    start: rtl
    stages:
      rtl:
        agent: rtl
        read: [docs/design, rtl]
        write: [rtl]
        terminal: true
"#,
        )
        .unwrap();
        manifest.validate().unwrap();
        assert_eq!(manifest.facade.command.as_deref(), Some("example-agent"));
    }

    #[test]
    fn validates_installation_contract() {
        let manifest: PluginManifest = serde_yaml::from_str(
            r#"
api_version: 1
id: example.plugin
runtime: { default_image: example:dev }
installation:
  environment:
    EXAMPLE_MODE:
      description: Select the example execution mode.
      default: fake
roles:
  developer:
    command: [run]
    environment: [EXAMPLE_MODE]
flows:
  implementation:
    start: develop
    tasks:
      develop: { role: developer, terminal: true }
"#,
        )
        .unwrap();
        manifest.validate().unwrap();
        let installation = manifest.installation.unwrap();
        assert_eq!(installation.build.context, ".");
        assert_eq!(installation.build.dockerfile, "Dockerfile");
    }

    #[test]
    fn rejects_secret_installation_defaults() {
        let manifest: PluginManifest = serde_yaml::from_str(
            r#"
api_version: 1
id: example.plugin
runtime: { default_image: example:dev }
installation:
  environment:
    API_TOKEN: { description: API token, secret: true, default: unsafe }
roles:
  developer: { command: [run], environment: [API_TOKEN] }
flows:
  implementation:
    start: develop
    tasks:
      develop: { role: developer, terminal: true }
"#,
        )
        .unwrap();
        assert!(manifest.validate().is_err());
    }

    #[test]
    fn parses_parallel_work_item_tasks_with_custom_roles() {
        let manifest: PluginManifest = serde_yaml::from_str(
            r#"
api_version: 1
id: example.rtl
runtime: { default_image: example:dev }
roles:
  architect: { display_name: Design Architect, command: [run, architect] }
  rtl: { command: [run, rtl] }
  dv: { command: [run, dv] }
flows:
  blocks:
    start: architect
    replaces_default_lifecycle: true
    work_items_path: docs/design/blocks/index.json
    tasks:
      architect:
        role: architect
        display_name: Block specification
        approval_subject: generated block specification
        write: [docs/design]
        approval: required
      rtl:
        role: rtl
        scope: work_item
        read: [docs/design]
        write: ["rtl/{work_item}.sv"]
      dv_prepare:
        role: dv
        scope: work_item
        read: [docs/design]
        write: ["dv/{work_item}"]
      dv_verify:
        role: dv
        scope: work_item
        dependencies: [rtl, dv_prepare]
        read: [docs/design, rtl, dv]
        write: ["dv/{work_item}"]
        allowed_handoffs: [rtl]
        handoff_descriptions: { rtl: Verification found an RTL defect. }
"#,
        )
        .unwrap();
        manifest.validate().unwrap();
        assert!(manifest.flows["blocks"].replaces_default_lifecycle);
        assert_eq!(
            manifest.flows["blocks"].tasks["architect"].approval,
            PluginApprovalMode::Required
        );
        assert_eq!(
            manifest.flows["blocks"].tasks["rtl"].approval,
            PluginApprovalMode::None
        );
        assert_eq!(
            manifest.roles["architect"].display_name.as_deref(),
            Some("Design Architect")
        );
        assert_eq!(
            manifest.flows["blocks"].tasks["architect"]
                .approval_subject
                .as_deref(),
            Some("generated block specification")
        );
    }

    #[test]
    fn validates_resources_parameters_artifacts_and_validators() {
        let manifest: PluginManifest = serde_yaml::from_str(
            r#"
api_version: 1
id: example.generic
runtime: { default_image: example:dev }
parameters:
  source_root: { type: path, default: src }
  extension: { type: enum, values: [rs, txt], default: rs }
  label: { type: string, default: example }
  retries: { type: integer, default: 2 }
  enabled: { type: boolean, default: true }
resources:
  standards: { source: plugin, path: resources/standards.md }
  references: { source: repository, path: "{source_root}/references" }
roles:
  developer:
    command: [run]
    resources: [{ id: standards, required: true }]
flows:
  implementation:
    start: develop
    tasks:
      develop:
        role: developer
        resources: [{ id: references, required: false }]
        read: ["{source_root}"]
        write: ["{source_root}/{work_item}.{extension}"]
        artifacts:
          - { path: "{source_root}/{work_item}.{extension}", type: file, required: true }
        validators:
          - { name: source validation, command: [/plugin/validate] }
        terminal: true
"#,
        )
        .unwrap();

        manifest.validate().unwrap();
    }

    #[test]
    fn rejects_unknown_or_non_path_filesystem_placeholders() {
        let unknown: PluginManifest = serde_yaml::from_str(
            r#"
api_version: 1
id: example
runtime: { default_image: image }
roles: { developer: { command: [run] } }
flows:
  default:
    start: develop
    tasks:
      develop: { role: developer, read: ["{missing}"] }
"#,
        )
        .unwrap();
        assert!(unknown.validate().is_err());

        let string_path: PluginManifest = serde_yaml::from_str(
            r#"
api_version: 1
id: example
runtime: { default_image: image }
parameters: { location: { type: string, default: src } }
roles: { developer: { command: [run] } }
flows:
  default:
    start: develop
    tasks:
      develop: { role: developer, read: ["{location}"] }
"#,
        )
        .unwrap();
        assert!(string_path.validate().is_err());
    }

    #[test]
    fn rejects_unsafe_resource_ids_and_unknown_assignments() {
        let unsafe_id: PluginManifest = serde_yaml::from_str(
            r#"
api_version: 1
id: example
runtime: { default_image: image }
resources: { "../secret": { source: plugin, path: resource.txt } }
roles: { developer: { command: [run] } }
flows:
  default:
    start: develop
    tasks: { develop: { role: developer } }
"#,
        )
        .unwrap();
        assert!(unsafe_id.validate().is_err());

        let unknown: PluginManifest = serde_yaml::from_str(
            r#"
api_version: 1
id: example
runtime: { default_image: image }
roles:
  developer: { command: [run], resources: [{ id: missing }] }
flows:
  default:
    start: develop
    tasks: { develop: { role: developer } }
"#,
        )
        .unwrap();
        assert!(unknown.validate().is_err());
    }

    #[test]
    fn rejects_glob_artifact_paths() {
        let manifest: PluginManifest = serde_yaml::from_str(
            r#"
api_version: 1
id: example
runtime: { default_image: image }
roles: { developer: { command: [run] } }
flows:
  default:
    start: develop
    tasks:
      develop:
        role: developer
        artifacts: [{ path: "build/*.txt", type: file, required: true }]
"#,
        )
        .unwrap();
        assert!(manifest.validate().is_err());
    }
}
