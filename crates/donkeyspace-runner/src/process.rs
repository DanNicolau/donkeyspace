//! Process supervision for Unix workers. Cancellation is local to an execution;
//! callers remain responsible for workflow authorization and publication fencing.

use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use std::{future::Future, io, process::Output, process::Stdio, time::Duration};
use tokio::{
    process::Command,
    sync::{Notify, oneshot},
    task::JoinHandle,
};

tokio::task_local! {
    static EXECUTION_SUPERVISORS: Vec<Arc<Supervisors>>;
}

#[derive(Default)]
struct Supervisors {
    active: AtomicUsize,
    failed: AtomicBool,
    finished: Notify,
}

struct SupervisorRegistration(Vec<Arc<Supervisors>>);
impl SupervisorRegistration {
    fn failed(&self) {
        for supervisors in &self.0 {
            supervisors.failed.store(true, Ordering::SeqCst);
        }
    }
}

impl Drop for SupervisorRegistration {
    fn drop(&mut self) {
        for supervisors in &self.0 {
            supervisors.active.fetch_sub(1, Ordering::SeqCst);
            supervisors.finished.notify_one();
        }
    }
}

/// Drop the workflow future on cancellation, then wait for all process/container
/// supervisors it started to finish cleanup before reporting cancellation.
/// The workflow future must execute its commands in this task's scope.
pub async fn run_cancellable_execution<F: Future, C: Future<Output = ()>>(
    work: F,
    cancel: C,
) -> io::Result<Option<F::Output>> {
    let supervisors = Arc::new(Supervisors::default());
    let mut scopes = EXECUTION_SUPERVISORS
        .try_with(Clone::clone)
        .unwrap_or_default();
    scopes.push(supervisors.clone());
    let result = EXECUTION_SUPERVISORS
        .scope(scopes, async {
            tokio::pin!(work, cancel);
            tokio::select! {
                biased;
                _ = &mut cancel => None,
                result = &mut work => Some(result),
            }
        })
        .await;
    loop {
        let finished = supervisors.finished.notified();
        if supervisors.active.load(Ordering::SeqCst) == 0 {
            break;
        }
        finished.await;
    }
    if supervisors.failed.load(Ordering::SeqCst) {
        return Err(io::Error::other(
            "execution cleanup failed; reconciliation required",
        ));
    }
    Ok(result)
}

const TERMINATION_GRACE: Duration = Duration::from_secs(5);
const CLEANUP_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Debug)]
pub struct ProcessOutput {
    pub output: Output,
    /// Cancellation remains distinct even when a TERM handler exits successfully.
    pub cancelled: bool,
}

/// Run a command in a new process group, capturing stdout and stderr.
///
/// When `cancel` resolves, send TERM to the group, allow up to five seconds for
/// shutdown, then KILL survivors. Dropping this future also requests shutdown;
/// its supervisor continues cleanup while the Tokio runtime remains alive.
/// `cleanup`, when provided, runs after execution on success, failure, or
/// cancellation (for example, removing a Docker container). Cleanup failure is
/// returned rather than reporting a successful execution with leaked resources.
/// Normal command completion also terminates remaining group members before
/// cleanup; background processes cannot outlive their execution scope.
///
/// An abruptly stopped worker/runtime still needs external orphan reconciliation.
pub async fn run_command_until(
    command: &mut Command,
    cleanup: Option<Command>,
    cancel: impl Future<Output = ()>,
) -> io::Result<ProcessOutput> {
    run_command_with_grace(command, cleanup, cancel, TERMINATION_GRACE).await
}

async fn run_command_with_grace(
    command: &mut Command,
    cleanup: Option<Command>,
    cancel: impl Future<Output = ()>,
    grace: Duration,
) -> io::Result<ProcessOutput> {
    tokio::pin!(cancel);
    // Do not launch work when cancellation was already requested.
    tokio::select! {
        biased;
        _ = &mut cancel => return Err(io::Error::new(io::ErrorKind::Interrupted, "command cancelled before launch")),
        _ = std::future::ready(()) => {}
    }
    command.process_group(0).kill_on_drop(true);
    command.stdout(Stdio::piped()).stderr(Stdio::piped());
    let child = command.spawn()?;
    let group = ProcessGroup(child.id().expect("new child has a process id") as i32);
    let (request_cancel, cancellation) = oneshot::channel::<()>();
    let registration = EXECUTION_SUPERVISORS
        .try_with(|supervisors| {
            for scope in supervisors {
                scope.active.fetch_add(1, Ordering::SeqCst);
            }
            SupervisorRegistration(supervisors.clone())
        })
        .ok();
    let mut supervisor = tokio::spawn(async move {
        let registration = registration;
        // Keep this guard across awaits: aborting the supervisor kills the group.
        let mut group = group;
        let execution = async {
            let output = child.wait_with_output();
            tokio::pin!(output);
            tokio::select! {
                biased;
                _ = cancellation => {
                    // Continue draining pipes/reaping the leader while the
                    // whole process group shuts down.
                    let (captured, stopped) = tokio::join!(
                        tokio::time::timeout(grace, &mut output),
                        group.terminate(grace),
                    );
                    stopped?;
                    let output = match captured {
                        Ok(result) => result?,
                        Err(_) => tokio::time::timeout(Duration::from_secs(1), &mut output).await
                            .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "cancelled command output pipes did not close"))??,
                    };
                    Ok(ProcessOutput { output, cancelled: true })
                }
                result = &mut output => {
                    let output = result?;
                    // A completed command must not leave background work behind.
                    // Stop remaining descendants before releasing group ownership
                    // or starting cleanup, where cancellation can still arrive.
                    group.terminate(grace).await?;
                    Ok(ProcessOutput { output, cancelled: false })
                },
            }
        }.await;
        // The leader is reaped and remaining descendants have been terminated.
        // Disarm before cleanup to avoid signaling a subsequently reused group ID.
        if execution.is_err() {
            if let Some(registration) = &registration {
                registration.failed();
            }
            let _ = group.signal(libc::SIGKILL);
        }
        group.0 = 0;
        if let Err(error) = run_cleanup(cleanup).await {
            // A dropped caller cannot observe the returned error. Keep cleanup
            // failures visible to operators without logging command arguments.
            tracing::warn!(%error, "execution cleanup failed");
            if let Some(registration) = &registration {
                registration.failed();
            }
            return Err(error);
        }
        execution
    });
    let result = tokio::select! {
        biased;
        _ = &mut cancel => {
            drop(request_cancel);
            let mut result = join_supervisor(supervisor).await?;
            // Cancellation can arrive after exit, while resource cleanup is
            // still running. It must still fence acceptance of a success result.
            result.cancelled = true;
            Ok(result)
        }
        result = &mut supervisor => result.map_err(io::Error::other)?,
    };
    result
}

async fn join_supervisor(
    supervisor: JoinHandle<io::Result<ProcessOutput>>,
) -> io::Result<ProcessOutput> {
    supervisor.await.map_err(io::Error::other)?
}

async fn run_cleanup(cleanup: Option<Command>) -> io::Result<()> {
    let Some(mut command) = cleanup else {
        return Ok(());
    };
    command
        .kill_on_drop(true)
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    let status = tokio::time::timeout(CLEANUP_TIMEOUT, command.status())
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "execution cleanup timed out"))??;
    if !status.success() {
        return Err(io::Error::other(format!(
            "execution cleanup failed with {status}"
        )));
    }
    Ok(())
}

struct ProcessGroup(i32);

impl ProcessGroup {
    async fn terminate(&self, grace: Duration) -> io::Result<()> {
        if !self.exists()? {
            return Ok(());
        }
        self.signal(libc::SIGTERM)?;
        let stopped = tokio::time::timeout(grace, async {
            while self.exists()? {
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
            Ok::<_, io::Error>(())
        })
        .await;
        match stopped {
            Ok(result) => result,
            Err(_) => self.signal(libc::SIGKILL),
        }
    }

    fn signal(&self, signal: i32) -> io::Result<()> {
        // SAFETY: the negative ID addresses only the process group established
        // for this child by process_group(0); it never targets the worker group.
        let result = unsafe { libc::kill(-self.0, signal) };
        if result == 0 {
            return Ok(());
        }
        let error = io::Error::last_os_error();
        if error.raw_os_error() == Some(libc::ESRCH) {
            Ok(())
        } else {
            Err(error)
        }
    }

    fn exists(&self) -> io::Result<bool> {
        // SAFETY: signal zero only checks existence of this child's process group.
        if unsafe { libc::kill(-self.0, 0) } == 0 {
            return Ok(true);
        }
        let error = io::Error::last_os_error();
        if error.raw_os_error() == Some(libc::ESRCH) {
            Ok(false)
        } else {
            Err(error)
        }
    }
}

impl Drop for ProcessGroup {
    fn drop(&mut self) {
        if self.0 != 0 {
            let _ = self.signal(libc::SIGKILL);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        path::{Path, PathBuf},
        sync::atomic::{AtomicU64, Ordering},
    };

    struct Fixture(PathBuf);

    impl Fixture {
        fn new() -> Self {
            static NEXT: AtomicU64 = AtomicU64::new(0);
            let root = std::env::temp_dir().join(format!(
                "donkeyspace-process-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
            std::fs::create_dir(&root).unwrap();
            Self(root)
        }

        fn command(&self, script: &str) -> Command {
            let mut command = Command::new("sh");
            command.args(["-c", script]).current_dir(&self.0);
            command
        }

        fn cleanup(&self) -> Command {
            self.command("printf cleaned > cleaned")
        }
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    async fn wait_for_file(path: &Path) {
        tokio::time::timeout(Duration::from_secs(10), async {
            while !path.exists() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("command did not reach the expected checkpoint");
    }

    fn process_is_running(pid: i32) -> bool {
        #[cfg(target_os = "linux")]
        {
            // A killed descendant may remain a zombie until its system reaper
            // runs. That is not a live process executing agent work.
            let Ok(stat) = std::fs::read_to_string(format!("/proc/{pid}/stat")) else {
                return false;
            };
            !stat.rsplit_once(") ").unwrap().1.starts_with('Z')
        }
        #[cfg(not(target_os = "linux"))]
        {
            // SAFETY: signal zero only tests existence of a fixture process.
            unsafe { libc::kill(pid, 0) == 0 }
        }
    }

    #[tokio::test]
    async fn ready_cancellation_does_not_launch_a_command() {
        let fixture = Fixture::new();
        let result = run_command_until(
            &mut fixture.command("touch launched"),
            None,
            std::future::ready(()),
        )
        .await
        .unwrap_err();
        assert_eq!(result.kind(), io::ErrorKind::Interrupted);
        assert!(!fixture.0.join("launched").exists());
    }

    #[tokio::test]
    async fn graceful_cancellation_preserves_logs_and_does_not_become_success() {
        let fixture = Fixture::new();
        let output = run_command_until(
            &mut fixture.command(
                "trap 'printf stopped; exit 0' TERM; touch ready; while :; do sleep 0.05; done",
            ),
            Some(fixture.cleanup()),
            wait_for_file(&fixture.0.join("ready")),
        )
        .await
        .unwrap();
        assert!(output.cancelled);
        assert!(output.output.status.success());
        assert_eq!(output.output.stdout, b"stopped");
        assert!(fixture.0.join("cleaned").exists());
    }

    #[tokio::test]
    async fn cancellation_kills_term_ignoring_descendant_after_parent_exits() {
        let fixture = Fixture::new();
        let script = "trap 'exit 0' TERM; sh -c 'trap \"\" TERM; echo $$ > child; touch ready; exec sleep 60' >/dev/null 2>&1 & wait";
        let started = std::time::Instant::now();
        let output = run_command_until(
            &mut fixture.command(script),
            Some(fixture.cleanup()),
            wait_for_file(&fixture.0.join("ready")),
        )
        .await
        .unwrap();
        assert!(output.cancelled);
        assert!(started.elapsed() >= TERMINATION_GRACE);
        assert!(started.elapsed() < Duration::from_secs(9));
        let child = std::fs::read_to_string(fixture.0.join("child"))
            .unwrap()
            .trim()
            .parse()
            .unwrap();
        tokio::time::timeout(Duration::from_secs(1), async {
            while process_is_running(child) {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("TERM-ignoring descendant survived cancellation");
        assert!(fixture.0.join("cleaned").exists());
    }

    #[tokio::test]
    async fn dropping_execution_still_terminates_and_cleans_up() {
        let fixture = Fixture::new();
        let mut command =
            fixture.command("trap '' TERM; echo $$ > child; touch ready; exec sleep 60");
        let cleanup = fixture.cleanup();
        let caller = tokio::spawn(async move {
            run_command_with_grace(
                &mut command,
                Some(cleanup),
                std::future::pending(),
                Duration::from_millis(100),
            )
            .await
        });
        wait_for_file(&fixture.0.join("ready")).await;
        let child = std::fs::read_to_string(fixture.0.join("child"))
            .unwrap()
            .trim()
            .parse()
            .unwrap();
        caller.abort();
        assert!(caller.await.unwrap_err().is_cancelled());
        wait_for_file(&fixture.0.join("cleaned")).await;
        assert!(!process_is_running(child));
    }

    #[tokio::test]
    async fn cancellation_is_isolated_from_other_executions() {
        let first = Fixture::new();
        let second = Fixture::new();
        let (stop_second, stopped) = oneshot::channel();
        let mut other_command = second.command("touch ready; exec sleep 60");
        let other = tokio::spawn(async move {
            run_command_until(&mut other_command, None, async {
                let _ = stopped.await;
            })
            .await
        });
        wait_for_file(&second.0.join("ready")).await;
        run_command_until(
            &mut first.command("touch ready; exec sleep 60"),
            None,
            wait_for_file(&first.0.join("ready")),
        )
        .await
        .unwrap();
        assert!(!other.is_finished());
        stop_second.send(()).unwrap();
        assert!(other.await.unwrap().unwrap().cancelled);
    }

    #[tokio::test]
    async fn drains_both_pipes_and_runs_cleanup_after_command_failure() {
        let fixture = Fixture::new();
        let output = run_command_until(
            &mut fixture.command("head -c 131072 /dev/zero; head -c 131072 /dev/zero >&2; exit 7"),
            Some(fixture.cleanup()),
            std::future::pending(),
        )
        .await
        .unwrap();
        assert!(!output.cancelled);
        assert_eq!(output.output.status.code(), Some(7));
        assert_eq!(output.output.stdout.len(), 131072);
        assert_eq!(output.output.stderr.len(), 131072);
        assert!(fixture.0.join("cleaned").exists());
    }

    #[tokio::test]
    async fn cleanup_failure_is_reported_even_after_successful_command() {
        let fixture = Fixture::new();
        let error = run_command_until(
            &mut fixture.command("exit 0"),
            Some(fixture.command("exit 9")),
            std::future::pending(),
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("execution cleanup failed"));
    }

    #[tokio::test]
    async fn cancellation_during_cleanup_cannot_return_success() {
        let fixture = Fixture::new();
        let output = run_command_until(
            &mut fixture.command("exit 0"),
            Some(fixture.command("touch cleaning; sleep 0.1; touch cleaned")),
            wait_for_file(&fixture.0.join("cleaning")),
        )
        .await
        .unwrap();
        assert!(output.cancelled);
        assert!(output.output.status.success());
        assert!(fixture.0.join("cleaned").exists());
    }

    #[tokio::test]
    async fn cancellation_during_cleanup_does_not_leave_a_descendant_running() {
        let fixture = Fixture::new();
        let output = run_command_with_grace(
            &mut fixture.command(
                "sh -c 'trap \"\" TERM; echo $$ > child; touch ready; exec sleep 60' >/dev/null 2>&1 & while [ ! -f ready ]; do sleep 0.01; done; exit 0",
            ),
            Some(fixture.command("touch cleaning; sleep 0.1; touch cleaned")),
            wait_for_file(&fixture.0.join("cleaning")),
            Duration::from_millis(100),
        )
        .await
        .unwrap();
        let child: i32 = std::fs::read_to_string(fixture.0.join("child"))
            .unwrap()
            .trim()
            .parse()
            .unwrap();
        let stopped = tokio::time::timeout(Duration::from_secs(1), async {
            while process_is_running(child) {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .is_ok();
        // Clean up even when running this regression against the broken revision.
        if !stopped {
            let _ = Command::new("kill")
                .args(["-KILL", &child.to_string()])
                .status()
                .await;
        }
        assert!(output.cancelled);
        assert!(output.output.status.success());
        assert!(fixture.0.join("cleaned").exists());
        assert!(stopped, "cancelled execution left its descendant running");
    }

    #[tokio::test]
    async fn completed_commands_stop_descendants_before_cleanup() {
        for exit_code in [0, 7] {
            let fixture = Fixture::new();
            let output = run_command_with_grace(
                &mut fixture.command(&format!(
                    "sleep 60 >/dev/null 2>&1 & echo $! > child; printf stdout; printf stderr >&2; exit {exit_code}",
                )),
                Some(fixture.command("sleep 0.05; touch cleaned")),
                std::future::pending(),
                Duration::from_millis(100),
            ).await.unwrap();
            let child: i32 = std::fs::read_to_string(fixture.0.join("child"))
                .unwrap()
                .trim()
                .parse()
                .unwrap();
            let running = process_is_running(child);
            if running {
                let _ = Command::new("kill")
                    .args(["-KILL", &child.to_string()])
                    .status()
                    .await;
            }
            assert!(!running, "completed command left its descendant running");
            assert!(!output.cancelled);
            assert_eq!(output.output.status.code(), Some(exit_code));
            assert_eq!(output.output.stdout, b"stdout");
            assert_eq!(output.output.stderr, b"stderr");
            assert!(fixture.0.join("cleaned").exists());
        }
    }

    #[tokio::test]
    async fn workflow_cancellation_waits_for_nested_supervisor_cleanup() {
        let fixture = Fixture::new();
        let result = run_cancellable_execution(
            async {
                run_cancellable_execution(
                    async {
                        run_command_with_grace(
                            &mut fixture.command("trap '' TERM; touch ready; exec sleep 60"),
                            Some(fixture.command("sleep 0.1; touch cleaned")),
                            std::future::pending(),
                            Duration::from_millis(100),
                        )
                        .await
                        .unwrap();
                        std::fs::write(fixture.0.join("published"), "late result").unwrap();
                    },
                    std::future::pending(),
                )
                .await
                .unwrap();
            },
            wait_for_file(&fixture.0.join("ready")),
        )
        .await
        .unwrap();
        assert!(result.is_none());
        assert!(fixture.0.join("cleaned").exists());
        assert!(!fixture.0.join("published").exists());
    }

    #[tokio::test]
    async fn workflow_cancellation_reports_failed_cleanup() {
        let fixture = Fixture::new();
        let error = run_cancellable_execution(
            async {
                run_command_with_grace(
                    &mut fixture.command("touch ready; exec sleep 60"),
                    Some(fixture.command("exit 9")),
                    std::future::pending(),
                    Duration::from_millis(100),
                )
                .await
                .unwrap();
            },
            wait_for_file(&fixture.0.join("ready")),
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("cleanup failed"));
    }

    #[tokio::test]
    async fn workflow_completion_cannot_hide_failed_cleanup() {
        let fixture = Fixture::new();
        let error = run_cancellable_execution(
            async {
                // A workflow can handle a command error and return before the
                // cancellation monitor polls. Its scope must still report the
                // cleanup failure so the worker cannot acknowledge cancellation.
                let _ = run_command_with_grace(
                    &mut fixture.command("exit 0"),
                    Some(fixture.command("exit 9")),
                    std::future::pending(),
                    Duration::from_millis(100),
                )
                .await;
            },
            std::future::pending(),
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("cleanup failed"));
    }
}
