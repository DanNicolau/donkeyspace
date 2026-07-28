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
    pub runtime: PluginRuntime,
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
pub struct PluginRole {
    pub command: Vec<String>,
    #[serde(default)]
    pub image: Option<String>,
    #[serde(default)]
    pub environment: Vec<String>,
    #[serde(default)]
    pub mcp_servers: Vec<String>,
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

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct PluginTask {
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
    pub allowed_handoffs: Vec<String>,
    #[serde(default)]
    pub transitions: BTreeMap<String, String>,
    #[serde(default)]
    pub terminal: bool,
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
        for (name, role) in &self.roles {
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
                validate_relative_path(path)?;
            }
            for (task_name, task) in &flow.tasks {
                if !self.roles.contains_key(&task.role) {
                    return Err(PluginError::Invalid(format!(
                        "task `{task_name}` references unknown role `{}`",
                        task.role
                    )));
                }
                for path in task.read.iter().chain(&task.write) {
                    validate_relative_path(path)?;
                }
                if task.scope == PluginTaskScope::WorkItem
                    && task.write.iter().any(|path| !path.contains("{work_item}"))
                {
                    return Err(PluginError::Invalid(format!(
                        "work-item task `{task_name}` write paths must contain `{{work_item}}`"
                    )));
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
            }
            validate_task_graph(flow_name, flow)?;
        }
        Ok(())
    }
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
    }

    #[test]
    fn parses_parallel_work_item_tasks_with_custom_roles() {
        let manifest: PluginManifest = serde_yaml::from_str(
            r#"
api_version: 1
id: example.rtl
runtime: { default_image: example:dev }
roles:
  architect: { command: [run, architect] }
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
        write: [docs/design]
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
"#,
        )
        .unwrap();
        manifest.validate().unwrap();
        assert!(manifest.flows["blocks"].replaces_default_lifecycle);
    }
}
