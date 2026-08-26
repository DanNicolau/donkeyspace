use serde::{Deserialize, Serialize};

pub const DEFAULT_DISPLAY_NAME: &str = "Donkeyspace";
pub const DEFAULT_TAGLINE: &str = "Agentic repository workflow harness";
pub const DEFAULT_COMMAND: &str = "donkeyspace";
pub const DEFAULT_BRANCH_PREFIX: &str = "donkeyspace";

#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct FacadeConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tagline: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub command: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub branch_prefix: Option<String>,
}

impl FacadeConfig {
    pub fn overlay(&self, higher_priority: &Self) -> Self {
        Self {
            display_name: higher_priority
                .display_name
                .clone()
                .or_else(|| self.display_name.clone()),
            tagline: higher_priority
                .tagline
                .clone()
                .or_else(|| self.tagline.clone()),
            command: higher_priority
                .command
                .clone()
                .or_else(|| self.command.clone()),
            branch_prefix: higher_priority
                .branch_prefix
                .clone()
                .or_else(|| self.branch_prefix.clone()),
        }
    }

    pub fn validate(&self) -> Result<(), String> {
        if let Some(value) = &self.display_name {
            validate_text("display_name", value)?;
        }
        if let Some(value) = &self.tagline {
            validate_text("tagline", value)?;
        }
        if let Some(value) = &self.command
            && !valid_command(value)
        {
            return Err(
                "facade command must contain 1-32 lowercase ASCII letters, digits, or hyphens; it must start and end with a letter or digit"
                    .into(),
            );
        }
        if let Some(value) = &self.branch_prefix
            && !valid_branch_prefix(value)
        {
            return Err(
                "facade branch_prefix must contain 1-64 lowercase ASCII letters, digits, or hyphens; it must start and end with a letter or digit"
                    .into(),
            );
        }
        Ok(())
    }

    pub fn resolve(&self) -> Facade {
        Facade {
            display_name: self
                .display_name
                .clone()
                .unwrap_or_else(|| DEFAULT_DISPLAY_NAME.into()),
            tagline: self
                .tagline
                .clone()
                .unwrap_or_else(|| DEFAULT_TAGLINE.into()),
            command: self
                .command
                .clone()
                .unwrap_or_else(|| DEFAULT_COMMAND.into()),
            branch_prefix: self
                .branch_prefix
                .clone()
                .unwrap_or_else(|| DEFAULT_BRANCH_PREFIX.into()),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct Facade {
    pub display_name: String,
    pub tagline: String,
    pub command: String,
    pub branch_prefix: String,
}

impl Default for Facade {
    fn default() -> Self {
        FacadeConfig::default().resolve()
    }
}

impl Facade {
    pub fn issue_command(&self) -> String {
        format!("/{}", self.command)
    }

    pub fn git_author_name(&self) -> String {
        format!("{}[bot]", self.display_name)
    }

    pub fn git_author_email(&self) -> String {
        format!("{}[bot]@users.noreply.github.com", self.command)
    }
}

fn validate_text(field: &str, value: &str) -> Result<(), String> {
    if value.trim().is_empty() || value.chars().any(char::is_control) {
        return Err(format!("facade {field} must be nonempty single-line text"));
    }
    Ok(())
}

fn valid_command(value: &str) -> bool {
    valid_slug(value, 32)
}

fn valid_branch_prefix(value: &str) -> bool {
    valid_slug(value, 64)
}

fn valid_slug(value: &str, max_len: usize) -> bool {
    let bytes = value.as_bytes();
    if bytes.is_empty() || bytes.len() > max_len {
        return false;
    }
    let alphanumeric = |byte: u8| byte.is_ascii_lowercase() || byte.is_ascii_digit();
    alphanumeric(bytes[0])
        && alphanumeric(bytes[bytes.len() - 1])
        && bytes
            .iter()
            .all(|byte| alphanumeric(*byte) || *byte == b'-')
}

#[cfg(test)]
mod tests {
    use super::{DEFAULT_COMMAND, FacadeConfig};

    #[test]
    fn resolves_defaults_and_fieldwise_overlays() {
        let plugin = FacadeConfig {
            display_name: Some("Plugin Name".into()),
            tagline: Some("Plugin tagline".into()),
            command: Some("plugin-agent".into()),
            branch_prefix: Some("plugin-work".into()),
        };
        let deployment = FacadeConfig {
            display_name: Some("Deployment Name".into()),
            ..Default::default()
        };
        let resolved = plugin.overlay(&deployment).resolve();
        assert_eq!(resolved.display_name, "Deployment Name");
        assert_eq!(resolved.tagline, "Plugin tagline");
        assert_eq!(resolved.issue_command(), "/plugin-agent");
        assert_eq!(resolved.branch_prefix, "plugin-work");
    }

    #[test]
    fn validates_command_and_text_fields() {
        assert!(FacadeConfig::default().validate().is_ok());
        assert_eq!(FacadeConfig::default().resolve().command, DEFAULT_COMMAND);
        for invalid in ["/agent", "Agent", "agent_1", "-agent", "agent-", ""] {
            assert!(
                FacadeConfig {
                    command: Some(invalid.into()),
                    ..Default::default()
                }
                .validate()
                .is_err()
            );
        }
        for invalid in [
            "example/agent",
            "Example-agent",
            "agent_1",
            "-agent",
            "agent-",
        ] {
            assert!(
                FacadeConfig {
                    branch_prefix: Some(invalid.into()),
                    ..Default::default()
                }
                .validate()
                .is_err()
            );
        }
        assert!(
            FacadeConfig {
                display_name: Some("two\nlines".into()),
                ..Default::default()
            }
            .validate()
            .is_err()
        );
    }
}
