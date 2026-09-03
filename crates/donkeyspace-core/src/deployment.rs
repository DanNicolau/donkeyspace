use serde::{Deserialize, Serialize};
use std::{fmt, str::FromStr};

pub const DEPLOYMENT_MODE_ENV: &str = "DONKEYSPACE_DEPLOYMENT_MODE";

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum DeploymentMode {
    Generated,
    Minimal,
}

impl DeploymentMode {
    pub fn from_environment() -> Result<Self, String> {
        std::env::var(DEPLOYMENT_MODE_ENV)
            .map_err(|_| missing_mode_message())?
            .parse()
    }
}

impl FromStr for DeploymentMode {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim() {
            "generated" => Ok(Self::Generated),
            "minimal" => Ok(Self::Minimal),
            value => Err(format!(
                "{DEPLOYMENT_MODE_ENV} must be `generated` or `minimal`, got `{value}`"
            )),
        }
    }
}

impl fmt::Display for DeploymentMode {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Generated => "generated",
            Self::Minimal => "minimal",
        })
    }
}

fn missing_mode_message() -> String {
    format!(
        "{DEPLOYMENT_MODE_ENV} is required; use the `donkeyspace up` command for a configured deployment, or explicitly set it to `minimal` for a local deployment without GitHub or plugins"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_supported_modes() {
        assert_eq!("generated".parse(), Ok(DeploymentMode::Generated));
        assert_eq!("minimal".parse(), Ok(DeploymentMode::Minimal));
    }

    #[test]
    fn rejects_empty_and_unknown_modes() {
        assert!(
            "".parse::<DeploymentMode>()
                .unwrap_err()
                .contains(DEPLOYMENT_MODE_ENV)
        );
        assert!("configured".parse::<DeploymentMode>().is_err());
    }
}
