use donkeyspace_core::RunResult;
use std::path::{Path, PathBuf};
use thiserror::Error;
use tokio::{fs, process::Command};

#[derive(Debug, Error)]
pub enum RunnerError {
    #[error("agent command cannot be empty")]
    EmptyCommand,
    #[error("agent command failed to start or complete: {0}")]
    Io(#[from] std::io::Error),
    #[error("agent result json is invalid: {0}")]
    Json(#[from] serde_json::Error),
    #[error("agent result failed orchestration validation: {0}")]
    InvalidResult(#[from] donkeyspace_core::RunResultError),
}

#[derive(Debug, Clone)]
pub struct AgentRunOutput {
    pub result: RunResult,
    pub command_result: AgentCommandResult,
}

#[derive(Debug, Clone)]
pub struct AgentCommandResult {
    pub command: Vec<String>,
    pub status: AgentCommandStatus,
    pub exit_code: Option<i32>,
    pub stdout: String,
    pub stderr: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AgentCommandStatus {
    Passed,
    Failed,
}

impl AgentCommandStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Passed => "passed",
            Self::Failed => "failed",
        }
    }
}

#[derive(Debug, Clone)]
pub struct AgentCommand {
    pub program: String,
    pub args: Vec<String>,
    pub working_dir: PathBuf,
    pub result_path: PathBuf,
}

impl AgentCommand {
    pub fn from_parts(
        command: &[String],
        working_dir: impl Into<PathBuf>,
        result_path: impl Into<PathBuf>,
    ) -> Result<Self, RunnerError> {
        let (program, args) = command.split_first().ok_or(RunnerError::EmptyCommand)?;

        Ok(Self {
            program: program.clone(),
            args: args.to_vec(),
            working_dir: working_dir.into(),
            result_path: result_path.into(),
        })
    }

    pub fn command_line(&self) -> Vec<String> {
        let mut command = Vec::with_capacity(self.args.len() + 1);
        command.push(self.program.clone());
        command.extend(self.args.clone());
        command
    }
}

pub async fn run_agent(command: &AgentCommand) -> Result<AgentRunOutput, RunnerError> {
    let command_result = run_agent_command(command).await?;
    let result = read_run_result(&command.result_path).await?;
    Ok(AgentRunOutput {
        result,
        command_result,
    })
}

pub async fn run_agent_command(command: &AgentCommand) -> Result<AgentCommandResult, RunnerError> {
    let output = Command::new(&command.program)
        .args(&command.args)
        .current_dir(&command.working_dir)
        .output()
        .await?;

    let command_result = AgentCommandResult {
        command: command.command_line(),
        status: if output.status.success() {
            AgentCommandStatus::Passed
        } else {
            AgentCommandStatus::Failed
        },
        exit_code: output.status.code(),
        stdout: String::from_utf8_lossy(&output.stdout).to_string(),
        stderr: String::from_utf8_lossy(&output.stderr).to_string(),
    };

    tracing::info!(status = ?output.status, "agent command completed");
    Ok(command_result)
}

pub async fn read_run_result(path: impl AsRef<Path>) -> Result<RunResult, RunnerError> {
    let raw = fs::read_to_string(path).await?;
    let result: RunResult = serde_json::from_str(&raw)?;
    result.validate_for_orchestration()?;
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::{AgentCommand, AgentCommandStatus, run_agent_command};
    use std::path::PathBuf;

    #[tokio::test]
    async fn run_agent_command_captures_output_and_status() {
        let command = AgentCommand {
            program: "sh".to_string(),
            args: vec![
                "-c".to_string(),
                "printf stdout; printf stderr >&2; exit 7".to_string(),
            ],
            working_dir: PathBuf::from("."),
            result_path: PathBuf::from("unused.json"),
        };

        let result = run_agent_command(&command).await.unwrap();

        assert_eq!(
            result.command,
            vec!["sh", "-c", "printf stdout; printf stderr >&2; exit 7"]
        );
        assert_eq!(result.status, AgentCommandStatus::Failed);
        assert_eq!(result.exit_code, Some(7));
        assert_eq!(result.stdout, "stdout");
        assert_eq!(result.stderr, "stderr");
    }
}
