//! Owned plugin containers with supervised process execution and cleanup.
use sha2::{Digest, Sha256};
use std::{collections::BTreeMap, env, path::Path, process::Stdio};
use tokio::process::Command;
use uuid::Uuid;

pub(crate) async fn run_container(
    image: &str,
    command: &[String],
    stage_root: &Path,
    configured: &BTreeMap<String, String>,
    allowed: &[String],
) -> Result<std::process::Output, Box<dyn std::error::Error>> {
    run_container_until(
        image,
        command,
        stage_root,
        configured,
        allowed,
        std::future::pending(),
    )
    .await
}

pub(crate) async fn run_container_until(
    image: &str,
    command: &[String],
    stage_root: &Path,
    configured: &BTreeMap<String, String>,
    allowed: &[String],
    cancel: impl std::future::Future<Output = ()>,
) -> Result<std::process::Output, Box<dyn std::error::Error>> {
    // Each invocation owns its container name. A delayed cleanup cannot remove
    // a replacement attempt, while the stable scope label identifies the task
    // attempt across its agent and validator invocations.
    let container_name = format!("donkeyspace-execution-{}", Uuid::now_v7());
    let scope = format!(
        "{:x}",
        Sha256::digest(stage_root.as_os_str().as_encoded_bytes())
    );
    let mut docker = Command::new("docker");
    docker.args([
        "run",
        "--rm",
        "--name",
        &container_name,
        "--label",
        "donkeyspace.managed=true",
        "--label",
        &format!("donkeyspace.execution-scope={scope}"),
        "--network",
        "bridge",
    ]);
    if let Ok(volume) = env::var("DONKEYSPACE_WORKSPACE_VOLUME") {
        let workspace_root =
            env::var("DONKEYSPACE_WORKSPACE_ROOT").unwrap_or_else(|_| "/workspaces".into());
        docker.args([
            "--mount",
            &format!("type=volume,src={volume},dst={workspace_root}"),
        ]);
        docker.args(["--workdir", &stage_root.display().to_string()]);
    } else {
        docker.args([
            "--mount",
            &format!("type=bind,src={},dst=/workspace", stage_root.display()),
        ]);
        docker.args(["--workdir", "/workspace"]);
    }
    if let Ok(volume) = env::var("DONKEYSPACE_CODEX_VOLUME") {
        docker.args([
            "--mount",
            &format!("type=volume,src={volume},dst=/root/.codex"),
        ]);
    }
    if let Ok(source) = env::var("DONKEYSPACE_OSS_TOOLS_PATH") {
        let source = source.trim();
        if !source.is_empty() {
            let source_path = Path::new(source);
            if !source_path.is_absolute() || source.contains(',') {
                return Err(
                    "DONKEYSPACE_OSS_TOOLS_PATH must be an absolute path without commas".into(),
                );
            }
            docker.args([
                "--mount",
                &format!("type=bind,src={source},dst=/mnt/oss-tools,readonly"),
            ]);
        }
    }
    if let Ok(source) = env::var("DONKEYSPACE_TECH_PATH") {
        let source = source.trim();
        if !source.is_empty() {
            let source_path = Path::new(source);
            if !source_path.is_absolute() || source.contains(',') {
                return Err("DONKEYSPACE_TECH_PATH must be an absolute path without commas".into());
            }
            docker.args([
                "--mount",
                &format!("type=bind,src={source},dst=/mnt/tech,readonly"),
            ]);
        }
    }
    for name in allowed {
        if let Some(source) = configured.get(name) {
            let value = if Path::new(source).is_absolute() {
                std::fs::read_to_string(source)
                    .map(|value| value.trim_end().to_string())
                    .map_err(|_| {
                        format!("required plugin environment file `{source}` is unreadable")
                    })?
            } else {
                env::var(source).map_err(|_| {
                    format!("required plugin environment source `{source}` is unset")
                })?
            };
            // Pass only the variable name on Docker's command line. The value
            // is inherited from this worker process and is never exposed in
            // process listings or command diagnostics.
            docker.arg("--env").arg(name).env(name, value);
        }
    }
    docker
        .arg(image)
        .args(command)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    #[cfg(unix)]
    {
        // --rm covers normal exit, and forced removal also stops a container
        // surviving its CLI process. Docker rm --force is idempotent for an
        // absent name. The supervisor owns cleanup if this future is dropped.
        let mut cleanup = Command::new("docker");
        cleanup.args(["rm", "--force", &container_name]);
        let output =
            donkeyspace_runner::process::run_command_until(&mut docker, Some(cleanup), cancel)
                .await?;
        if output.cancelled {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Interrupted,
                "plugin execution cancelled",
            )
            .into());
        }
        Ok(output.output)
    }
    #[cfg(not(unix))]
    {
        let _ = cancel;
        let output = docker.output().await;
        let status = Command::new("docker")
            .args(["rm", "--force", &container_name])
            .status()
            .await?;
        if !status.success() {
            return Err("plugin container cleanup failed".into());
        }
        Ok(output?)
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::{path::PathBuf, time::Duration};

    struct Fixture {
        root: PathBuf,
        scope: String,
    }

    impl Fixture {
        async fn new() -> Self {
            // Never borrow an installed worker's credentials, volumes, or tools.
            for key in [
                "DONKEYSPACE_WORKSPACE_VOLUME",
                "DONKEYSPACE_CODEX_VOLUME",
                "DONKEYSPACE_OSS_TOOLS_PATH",
                "DONKEYSPACE_TECH_PATH",
            ] {
                assert!(
                    env::var_os(key).is_none(),
                    "unset {key} for isolated container tests"
                );
            }
            let source = env::var("DONKEYSPACE_TEST_REPO")
                .expect("set DONKEYSPACE_TEST_REPO to a temporary umbrella checkout");
            let root =
                env::temp_dir().join(format!("donkeyspace-container-test-{}", Uuid::now_v7()));
            std::fs::create_dir(&root).unwrap();
            let scope = format!("{:x}", Sha256::digest(root.as_os_str().as_encoded_bytes()));
            let fixture = Self { root, scope };
            let clone = Command::new("git")
                .args(["clone", "--local", "--no-hardlinks", "--quiet", &source])
                .arg(fixture.root.join("repo"))
                .output()
                .await
                .unwrap();
            assert!(clone.status.success(), "fixture checkout failed");
            fixture
        }

        async fn containers(&self) -> Vec<String> {
            let output = Command::new("docker")
                .args([
                    "ps",
                    "--all",
                    "--filter",
                    &format!("label=donkeyspace.execution-scope={}", self.scope),
                    "--format",
                    "{{.Names}}",
                ])
                .output()
                .await
                .unwrap();
            assert!(output.status.success());
            String::from_utf8(output.stdout)
                .unwrap()
                .lines()
                .map(str::to_owned)
                .collect()
        }

        async fn wait_for_container_count(&self, count: usize) {
            tokio::time::timeout(Duration::from_secs(20), async {
                while self.containers().await.len() != count {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            })
            .await
            .expect("unexpected surviving or missing fixture containers");
        }

        async fn ready(&self, marker: &str) {
            tokio::time::timeout(Duration::from_secs(10), async {
                while !self.root.join(marker).exists() {
                    tokio::time::sleep(Duration::from_millis(25)).await;
                }
            })
            .await
            .expect("container did not reach its startup checkpoint");
        }
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            // Failure cleanup is limited to this fixture's unguessable scope.
            if let Ok(output) = std::process::Command::new("docker")
                .args([
                    "ps",
                    "--all",
                    "--quiet",
                    "--filter",
                    &format!("label=donkeyspace.execution-scope={}", self.scope),
                ])
                .output()
            {
                for id in String::from_utf8_lossy(&output.stdout).lines() {
                    let _ = std::process::Command::new("docker")
                        .args(["rm", "--force", id])
                        .output();
                }
            }
            let _ = std::fs::remove_dir_all(&self.root);
        }
    }

    fn shell(script: &str) -> Vec<String> {
        vec!["sh".into(), "-c".into(), script.into()]
    }

    #[tokio::test]
    #[ignore = "requires Docker, busybox:latest, and DONKEYSPACE_TEST_REPO umbrella checkout"]
    async fn container_execution_cleanup_and_cancellation_in_umbrella_checkout() {
        let fixture = Fixture::new().await;
        let configured = BTreeMap::new();
        let passed = run_container(
            "busybox:latest",
            &shell("test -d repo/.git && printf passed"),
            &fixture.root,
            &configured,
            &[],
        )
        .await
        .unwrap();
        assert!(passed.status.success());
        assert_eq!(passed.stdout, b"passed");
        fixture.wait_for_container_count(0).await;
        let failed = run_container(
            "busybox:latest",
            &shell("printf failed >&2; exit 7"),
            &fixture.root,
            &configured,
            &[],
        )
        .await
        .unwrap();
        assert_eq!(failed.status.code(), Some(7));
        assert_eq!(failed.stderr, b"failed");
        fixture.wait_for_container_count(0).await;

        let root = fixture.root.clone();
        let first = tokio::spawn(async move {
            run_container(
                "busybox:latest",
                &shell("test -d repo/.git || exit 90; trap '' TERM; touch first-ready; sleep 60"),
                &root,
                &BTreeMap::new(),
                &[],
            )
            .await
            .map_err(|e| e.to_string())
        });
        fixture.ready("first-ready").await;
        let first_name = fixture.containers().await.pop().unwrap();
        let root = fixture.root.clone();
        let (cancel, cancelled) = tokio::sync::oneshot::channel::<()>();
        let second = tokio::spawn(async move {
            run_container_until(
                "busybox:latest",
                &shell("test -d repo/.git || exit 90; trap '' TERM; touch second-ready; sleep 60"),
                &root,
                &BTreeMap::new(),
                &[],
                async {
                    let _ = cancelled.await;
                },
            )
            .await
            .map_err(|e| e.to_string())
        });
        fixture.ready("second-ready").await;
        fixture.wait_for_container_count(2).await;
        first.abort();
        assert!(first.await.unwrap_err().is_cancelled());
        fixture.wait_for_container_count(1).await;
        assert_ne!(fixture.containers().await[0], first_name);
        assert!(
            !second.is_finished(),
            "old cleanup terminated a replacement invocation"
        );
        cancel.send(()).unwrap();
        let result = tokio::time::timeout(Duration::from_secs(20), second)
            .await
            .unwrap()
            .unwrap()
            .unwrap_err();
        assert!(result.contains("plugin execution cancelled"), "{result}");
        fixture.wait_for_container_count(0).await;
    }
}
