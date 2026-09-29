# Maintainability refactor: upgrade and rollback

This release keeps the built-in software lifecycle and full lifecycle plugins.
Developer-only serial plugins and no-op configuration fields are retired. The
checkpoint change requires a coordinated API/worker upgrade.

1. Stop admitting new work. Finish queued/running serial plugin jobs on the old
   release. Disconnect those plugins or convert their manifests to full lifecycle
   graphs. The new release rejects an old serial selection with guidance.
2. Drain active model executions. Keep paused lifecycle jobs, checkout directories,
   task workspaces, filesystem checkpoints, publications and approval history.
   Back up PostgreSQL and retained workspaces together.
3. Review policy migration warnings. Save version 2 policies without the removed
   fields. Keep working native/OpenAI-compatible provider settings and credentials.
4. Reconnect dedicated automation authentication using the updated CLI (instance
   schema 9), following [the authentication migration](installation.md#codex).
   Personal Codex homes are no longer mounted; no credentials are imported.
   Confirm the Docker daemon supports volume subdirectory mounts (26 or newer).
5. Upgrade API, worker, plugin manifest and plugin image together. Migrations
   `0007_lifecycle_checkpoints.sql`, `0008_repository_label_sync.sql`, and
   `0009_repository_retirement.sql` apply automatically. They add durable
   checkpoints, bounded repository maintenance, and generation-fenced retirement.
   Review the selected repository list before starting: applying removal retires
   historical work, while temporarily inaccessible tracked repositories back off.
   Do not run old and new workers together or overlap independent stacks.
6. Audit paused lifecycles before resuming. New checkpoints contain immutable
   repository provenance; older database and filesystem checkpoints without it
   are blocked before execution. Preserve their files/history and reconcile the
   original accepted revision, or start a fresh run with renewed approval. There
   is no automatic inference of acceptance from a retained directory. Verify a
   fresh disposable lifecycle's pause/resume, targeted decisions and completed
   siblings before opening normal admission. The shared infrastructure graph is
   for new runs; do not retrofit historical checkpoints or resume terminated runs.

Expired executions remain fenced and require operator inspection; this change
never automatically replays model work. Resume requires the saved Git objects,
either locally or fetchable from the repository. It restores their exact revision
when the aggregate checkout is lost; advancing the remote branch does not change
what was accepted. Back up publications and objects together with database state.

Before the new worker writes checkpoints, code rollback can ignore the additive
checkpoint table. After new checkpoint/approval writes, restore the matching
pre-upgrade database **and** workspace backup before running the old worker. An
old filesystem checkpoint may otherwise replay already completed work.

## Verification

- `cargo fmt --all -- --check`
- `cargo test --workspace`
- `scripts/test-database` starts and removes its own PostgreSQL container. Tests
  cover atomic issue/PR/push intake, rollback/redelivery, authorization failure,
  stale state, checkpoint compare-and-swap, lease fencing, legacy import and a
  deterministic lifecycle in `busybox:latest` containers (no model or GitHub).
- `cd web && npm run build && npm test`
- `scripts/test-execution-image IMAGE` checks the packaged coordinator client
  against disposable volume-subdirectory mounts, not just the host Docker client.
- For each selected plugin, run its documented contract and runtime checks
  against the updated manifest and image before enabling new work.

Live GitHub delivery, a full model run and synthesis with production technology
mounts remain deployment smoke tests; the isolated checks do not substitute for
those environment-specific checks.
