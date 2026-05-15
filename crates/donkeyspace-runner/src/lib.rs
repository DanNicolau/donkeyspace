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
}

pub async fn run_agent(command: &AgentCommand) -> Result<RunResult, RunnerError> {
    let status = Command::new(&command.program)
        .args(&command.args)
        .current_dir(&command.working_dir)
        .status()
        .await?;

    tracing::info!(?status, "agent command completed");
    read_run_result(&command.result_path).await
}

pub async fn read_run_result(path: impl AsRef<Path>) -> Result<RunResult, RunnerError> {
    let raw = fs::read_to_string(path).await?;
    let result: RunResult = serde_json::from_str(&raw)?;
    result.validate_for_orchestration()?;
    Ok(result)
}
