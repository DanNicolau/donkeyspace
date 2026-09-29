use std::path::{Path, PathBuf};
use thiserror::Error;
use tokio::fs;

#[cfg(unix)]
pub mod process;

/// Shared file protocol for native agents and plugin containers.
pub struct RunFiles {
    directory: PathBuf,
    pub result: PathBuf,
}

impl RunFiles {
    pub async fn prepare(
        workspace: &Path,
        input: &impl serde::Serialize,
    ) -> Result<Self, RunnerError> {
        let directory = workspace.join(".donkeyspace");
        require_plain_path(&directory, true).await?;
        fs::create_dir_all(&directory).await?;
        let result = directory.join("run-result.json");
        match fs::remove_file(&result).await {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        write_run_metadata(workspace, "run-input.json", input).await?;
        Ok(Self { directory, result })
    }

    pub async fn write_logs(&self, stdout: &[u8], stderr: &[u8]) -> Result<(), RunnerError> {
        require_plain_path(&self.directory, true).await?;
        require_plain_path(&self.directory.join("agent.stdout.log"), false).await?;
        require_plain_path(&self.directory.join("agent.stderr.log"), false).await?;
        fs::write(self.directory.join("agent.stdout.log"), bounded_log(stdout)).await?;
        fs::write(self.directory.join("agent.stderr.log"), bounded_log(stderr)).await?;
        Ok(())
    }

    pub async fn read<T: serde::de::DeserializeOwned>(&self) -> Result<T, RunnerError> {
        require_plain_path(&self.directory, true).await?;
        require_plain_path(&self.result, false).await?;
        Ok(serde_json::from_slice(&fs::read(&self.result).await?)?)
    }
}

pub async fn write_run_metadata(
    workspace: &Path,
    name: &str,
    value: &impl serde::Serialize,
) -> Result<(), RunnerError> {
    if Path::new(name).components().count() != 1
        || !matches!(
            Path::new(name).components().next(),
            Some(std::path::Component::Normal(_))
        )
    {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "metadata name must be a filename",
        )
        .into());
    }
    let directory = workspace.join(".donkeyspace");
    require_plain_path(&directory, true).await?;
    fs::create_dir_all(&directory).await?;
    let path = directory.join(name);
    require_plain_path(&path, false).await?;
    fs::write(path, serde_json::to_vec_pretty(value)?).await?;
    Ok(())
}

// Execution has terminated before result/log handling. Its protocol directory
// is a pinned mount, and no other job mounts this workspace. Reject links and
// special files rather than following agent-controlled paths in the worker.
async fn require_plain_path(path: &Path, directory: bool) -> Result<(), std::io::Error> {
    match fs::symlink_metadata(path).await {
        Ok(metadata)
            if if directory {
                metadata.is_dir()
            } else {
                metadata.is_file()
            } =>
        {
            Ok(())
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
        Ok(_) => Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "agent protocol paths must not be symlinks or special files",
        )),
    }
}

fn bounded_log(bytes: &[u8]) -> String {
    let text = String::from_utf8_lossy(bytes);
    let mut chars = text.chars();
    let mut value = chars.by_ref().take(1_000_000).collect::<String>();
    if chars.next().is_some() {
        value.push_str("\n[truncated]\n");
    }
    value
}

#[derive(Debug, Error)]
pub enum RunnerError {
    #[error("agent run files could not be read or written: {0}")]
    Io(#[from] std::io::Error),
    #[error("agent result json is invalid: {0}")]
    Json(#[from] serde_json::Error),
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

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::os::unix::fs::symlink;

    #[tokio::test]
    async fn protocol_links_cannot_read_or_overwrite_files_outside_the_run() {
        let root = std::env::temp_dir().join(format!(
            "ds-protocol-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir_all(&root).await.unwrap();
        let secret = root.join("synthetic-secret");
        fs::write(&secret, b"{\"secret\":true}").await.unwrap();
        let workspace = root.join("workspace");
        let files = RunFiles::prepare(&workspace, &serde_json::json!({}))
            .await
            .unwrap();
        symlink(&secret, &files.result).unwrap();
        assert!(files.read::<serde_json::Value>().await.is_err());
        symlink(&secret, files.directory.join("agent.stdout.log")).unwrap();
        assert!(files.write_logs(b"overwrite", b"").await.is_err());
        symlink(&secret, files.directory.join("policy.json")).unwrap();
        assert!(
            write_run_metadata(&workspace, "policy.json", &serde_json::json!({}))
                .await
                .is_err()
        );
        assert!(
            write_run_metadata(&workspace, "../synthetic-secret", &serde_json::json!({}))
                .await
                .is_err()
        );
        fs::remove_file(files.directory.join("run-input.json"))
            .await
            .unwrap();
        symlink(&secret, files.directory.join("run-input.json")).unwrap();
        assert!(
            RunFiles::prepare(&workspace, &serde_json::json!({}))
                .await
                .is_err()
        );
        assert_eq!(fs::read(&secret).await.unwrap(), b"{\"secret\":true}");
        fs::remove_dir_all(&files.directory).await.unwrap();
        symlink(&root, &files.directory).unwrap();
        assert!(
            RunFiles::prepare(&workspace, &serde_json::json!({}))
                .await
                .is_err()
        );
        assert!(
            write_run_metadata(&workspace, "synthetic-secret", &serde_json::json!({}))
                .await
                .is_err()
        );
        fs::remove_dir_all(root).await.unwrap();
    }
}
