# Process cancellation foundation

On Unix, agent commands and required checks run in their own process groups.
`donkeyspace_runner::run_agent_command_until` accepts a cancellation future.
When it resolves, the runner sends TERM to the group, waits up to five seconds,
then sends KILL to survivors. A parent exiting does not bypass termination of
its descendants. Output is drained during shutdown, and cancellation is reported
separately from failure even if a TERM handler returns exit code zero.
Already-requested cancellation returns an interrupted error without launching
the command. Cancellation arriving during cleanup also prevents a success result.
Normal command completion also shuts down surviving members of its process group
before cleanup begins, using the same TERM/grace/KILL sequence. Background work
cannot outlive its command. This prevents cancellation during cleanup from leaving
a child running after the parent has exited, while allowing group ownership to be
released before the cleanup delay. The parent's output and exit status are retained.

The ordinary `run_agent_command` API uses the same supervisor. Dropping either
execution future requests cancellation; a supervisor task owns termination and
cleanup while the Tokio runtime remains alive. The caller must await explicit
cancellation when it needs confirmation that cleanup has completed. On abrupt
runtime shutdown, a guard force-kills the local process group, but it cannot
guarantee asynchronous Docker cleanup. Processes deliberately escaping their
group also require stronger isolation than this runner provides.

Plugin agent and validator containers retain Docker's `--rm` behavior and also
receive explicit `docker rm --force` cleanup. Each invocation gets a unique
`donkeyspace-execution-<UUID>` name so late cleanup cannot remove another attempt.
The `donkeyspace.managed=true` and `donkeyspace.execution-scope=<SHA256>` labels
identify managed containers and their task-attempt directory. The scope hashes
the directory path; it is not yet a persisted workflow/job identity. Container
names and labels contain no agent environment values. Cleanup is bounded to ten
seconds, and failure is logged and returned rather than hidden. A caller waiting
for cancellation therefore waits for termination and cleanup, not just a signal.

GitHub closure now connects to this supervisor through durable job cancellation.
See [closed workflows](closed-workflows.md) for the database migration, execution
fencing, live test, and remaining scope of
[issue #28](https://github.com/DanNicolau/donkeyspace/issues/28), including crash
reconciliation and manual cancellation API/UI.

## Validation

`cargo test -p donkeyspace-runner` exercises real Unix processes: successful and
failed output capture, full pipes, cooperative TERM, TERM-ignoring descendants,
caller abandonment, isolation between executions, cancellation before launch and
during cleanup, and cleanup failures.

The opt-in worker smoke test uses the production plugin container runner with
`busybox:latest` and a temporary clone of the authorized umbrella test repository:

```sh
gh repo clone EPIC-BLOCKCHAIN/umbrella /tmp/donkeyspace-umbrella-test -- --depth=1
DONKEYSPACE_TEST_REPO=/tmp/donkeyspace-umbrella-test \
  cargo test -p donkeyspace-worker \
  container_execution_cleanup_and_cancellation_in_umbrella_checkout \
  -- --ignored --nocapture
```

Run it with Docker available and the worker's workspace, Codex, technology, and
tool mount environment variables unset. It runs no paid agents or hardware tools.
It checks successful and failed commands, abandoned execution, explicit
cancellation, and overlapping invocations in one scope. All test containers and
the test's internal checkout are removed, including on assertion failure. Remove
the supplied temporary source checkout afterward. No GitHub issues, comments,
branches, PRs, or historical workflows are modified by the smoke test. This is
container execution coverage, not a webhook-to-workflow cancellation test.
