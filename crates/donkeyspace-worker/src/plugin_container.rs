//! Owned plugin containers with supervised process execution and cleanup.
use donkeyspace_db::{
    PgPool,
    container_executions::{ContainerExecution, register_container_execution},
};
use sha2::{Digest, Sha256};
use std::{collections::BTreeMap, env, future::Future, path::Path, process::Stdio};
use tokio::process::Command;
use uuid::Uuid;

tokio::task_local! {
    static EXECUTION_OWNER: (PgPool, Uuid, String);
}

// Like process supervision, this scope covers agents and validators polled in
// the coordinator task. A newly spawned task must explicitly enter the scope;
// an unscoped production container launch fails closed.
pub(crate) async fn with_execution_owner<F: Future>(
    pool: &PgPool,
    job: Uuid,
    owner: &str,
    work: F,
) -> F::Output {
    EXECUTION_OWNER
        .scope((pool.clone(), job, owner.to_owned()), work)
        .await
}

pub(crate) async fn run_container(
    image: &str,
    command: &[String],
    stage_root: &Path,
    configured: &BTreeMap<String, String>,
    allowed: &[String],
) -> Result<std::process::Output, Box<dyn std::error::Error>> {
    let (pool, coordinator, owner) = EXECUTION_OWNER
        .try_with(Clone::clone)
        .map_err(|_| "plugin container launch requires a coordinator execution scope")?;
    let scope = format!(
        "{:x}",
        Sha256::digest(stage_root.as_os_str().as_encoded_bytes())
    );
    let execution = register_container_execution(&pool, coordinator, &owner, &scope).await?;
    run_container_until(
        &execution,
        image,
        command,
        stage_root,
        configured,
        allowed,
        std::future::pending(),
    )
    .await
}

async fn run_container_until(
    execution: &ContainerExecution,
    image: &str,
    command: &[String],
    stage_root: &Path,
    configured: &BTreeMap<String, String>,
    allowed: &[String],
    cancel: impl std::future::Future<Output = ()>,
) -> Result<std::process::Output, Box<dyn std::error::Error>> {
    // The identity is already committed before any Docker request. Each agent
    // and validator invocation gets a distinct name, even in the same scope.
    let container_name = &execution.container_name;
    let mut docker = Command::new("docker");
    docker.args([
        "run",
        "--rm",
        "--name",
        container_name,
        "--label",
        "donkeyspace.managed=true",
        "--label",
        &format!("donkeyspace.execution-scope={}", execution.execution_scope),
        "--network",
        "bridge",
    ]);
    for (key, value) in execution_labels(execution) {
        docker.args(["--label", &format!("{key}={value}")]);
    }
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

fn execution_labels(execution: &ContainerExecution) -> Vec<(&'static str, String)> {
    let mut labels = vec![
        ("donkeyspace.execution-id", execution.id.to_string()),
        (
            "donkeyspace.coordinator-job-id",
            execution.coordinator_job_id.to_string(),
        ),
        (
            "donkeyspace.workflow-generation",
            execution.generation.to_string(),
        ),
    ];
    if let Some(workflow) = execution.workflow_item_id {
        labels.push(("donkeyspace.workflow-id", workflow.to_string()));
    }
    labels
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::{path::PathBuf, time::Duration};

    fn fixture_execution(root: &Path) -> ContainerExecution {
        let id = Uuid::now_v7();
        ContainerExecution {
            id,
            coordinator_job_id: Uuid::now_v7(),
            workflow_item_id: Some(7),
            generation: 2,
            lease_owner: "isolated-test".into(),
            container_name: format!("donkeyspace-execution-{id}"),
            execution_scope: format!("{:x}", Sha256::digest(root.as_os_str().as_encoded_bytes())),
        }
    }

    // Low-level supervision tests use synthetic identities. Production always
    // registers through run_container; the live harness verifies that path.
    async fn run_fixture_container(
        image: &str,
        command: &[String],
        root: &Path,
        configured: &BTreeMap<String, String>,
        allowed: &[String],
    ) -> Result<std::process::Output, Box<dyn std::error::Error>> {
        run_fixture_container_until(
            image,
            command,
            root,
            configured,
            allowed,
            std::future::pending(),
        )
        .await
    }

    async fn run_fixture_container_until(
        image: &str,
        command: &[String],
        root: &Path,
        configured: &BTreeMap<String, String>,
        allowed: &[String],
        cancel: impl Future<Output = ()>,
    ) -> Result<std::process::Output, Box<dyn std::error::Error>> {
        run_container_until(
            &fixture_execution(root),
            image,
            command,
            root,
            configured,
            allowed,
            cancel,
        )
        .await
    }

    #[tokio::test]
    async fn unscoped_container_launch_is_rejected_before_docker() {
        let error = run_container("unused", &[], Path::new("/unused"), &BTreeMap::new(), &[])
            .await
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("requires a coordinator execution scope")
        );
    }

    #[test]
    fn container_labels_use_persisted_execution_identity() {
        let mut execution = fixture_execution(Path::new("/unused"));
        let labels: BTreeMap<_, _> = execution_labels(&execution).into_iter().collect();
        assert_eq!(labels["donkeyspace.execution-id"], execution.id.to_string());
        assert_eq!(
            labels["donkeyspace.coordinator-job-id"],
            execution.coordinator_job_id.to_string()
        );
        assert_eq!(labels["donkeyspace.workflow-id"], "7");
        assert_eq!(labels["donkeyspace.workflow-generation"], "2");
        execution.workflow_item_id = None;
        assert!(
            !execution_labels(&execution)
                .iter()
                .any(|(name, _)| *name == "donkeyspace.workflow-id")
        );
    }

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
        let passed = run_fixture_container(
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
        let failed = run_fixture_container(
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
            run_fixture_container(
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
            run_fixture_container_until(
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
