# Manage tracked repositories

Use the saved GitHub connection to change the repositories tracked by an existing
instance. This does not register an App or request credentials again.

There are three separate controls:

1. **GitHub installation access:** the existing App installation (or PAT) must
   have access to a repository. Grant access in GitHub if it is missing.
2. **Tracked repositories:** select which accessible repositories this instance
   should ingest. All must belong to the existing owner and App installation.
3. **Trusted identities:** configure who may start work and who may approve it.
   Newly tracked repositories deny both scopes until you configure them.

## Headless CLI

```sh
donkeyspace configure repositories list
donkeyspace configure repositories list --accessible
donkeyspace configure repositories add OWNER/NEW_REPO
donkeyspace configure github-access --repository OWNER/NEW_REPO add --user STARTER
donkeyspace configure github-access --repository OWNER/NEW_REPO --scope approvers add --user APPROVER
```

Use `--config-dir PATH` when managing an instance outside the default configuration
directory. Run the CLI as the instance owner, with access to its credential files
and Docker daemon.

Adding is additive and case-insensitively idempotent. It preserves other tracked
repositories and their starter/approver scopes, credential paths, ingress URL and
mode, polling interval, plugin selection, facade, ports and other instance settings.
Unavailable repositories and unsupported owners fail before changing `instance.json`.
Saving uses a lock and atomic rename; a repository edit based on stale configuration
must be reloaded and retried.

The updated CLI automatically migrates local instance configuration to version 9.
The repository pending-apply field was introduced in version 8; version 9 also
requires [dedicated automation authentication](installation.md#codex).
Use the updated CLI for subsequent operations: older versions reject this format,
which prevents them from silently discarding pending repository changes.

## Apply saved changes

Repository edits **save only**. `list` reports that they await a controlled restart;
the running stack continues using its previous selection. While repository edits
are pending, access-management edits also save only, avoiding an API-only restart.

Pause new submissions and let active jobs and pending outbound actions finish.
Then explicitly acknowledge that the stack has drained:

```sh
donkeyspace configure repositories apply --confirm-drained
```

This stops API and worker before recreating either with the same saved settings.
It waits up to 60 seconds for Compose readiness. PostgreSQL must already be running;
the database, dashboard and volumes are retained. A failed recreation leaves the
saved change pending and attempts to stop both consumers; inspect the error and
retry after fixing it. An unsuccessful cleanup is reported, not treated as success.

The acknowledgement is an operator assertion, **not an automatic job drain**.
Restarting a worker with active jobs can interrupt them. An ordinary `up` refuses
to apply pending repository edits while either consumer is running. If both are
already stopped, `up` can start the stack with the saved configuration.

## Remove a repository

```sh
donkeyspace configure repositories remove OWNER/OLD_REPO --confirm
```

Removal is explicit and offline, so a repository whose installation access was
revoked can still be removed. After applying, new webhook deliveries for it are
rejected and it is no longer polled. The command does not delete workflow/history
data, revoke GitHub installation access, or close issues. Saving removal does not
change the running stack. After apply, API/worker startup reconciliation cancels outstanding jobs,
approvals, publications, issue projections, and outbound actions for removed
repositories. Running jobs use the existing cancellation and container cleanup
path. Results, checkpoint files, published commits, and history remain available.
A `repository_retired` lifecycle event records the reason; `repositories.retired_at`
is local tracking state, not a claim of deletion on GitHub.

Removal advances each historical workflow's generation. Re-adding a repository
starts with empty starter/approver scopes and permits new authorized work after
configuration and apply. It does **not** resume cancelled jobs/checkpoints or
replay old actions, publications, or managed-PR repair work. Start a fresh authorized
request after checking the retained history. Retrying a run from the old generation
is rejected. Repeated polling and restarts do not duplicate retirement events.

Migration `0009_repository_retirement.sql` supplies the tracking flag, admission
fences, and durable closure-cleanup retry timing. Authenticated standalone workers
must provide a nonempty `DONKEYSPACE_GITHUB_REPOSITORIES`; an absent selection is
an error, never permission to process arbitrary historical repositories.

Keep at least one repository in a connected instance. Removing the last repository
is rejected because an authenticated runtime requires a nonempty tracked selection;
this command does not disconnect GitHub or invent a new installation boundary.

## Repository maintenance failures

The worker synchronizes policy labels only for `DONKEYSPACE_GITHUB_REPOSITORIES`,
using the same case-insensitive identities as ingress. Historical database records
and App installation membership do not add repositories to this selection. A label
failure is logged with the repository and operation; it does not stop maintenance
for other repositories. A 404 indicates unavailable access, not confirmed deletion.

Migration `0008_repository_label_sync.sql` records each attempt before contacting
GitHub. Failed attempts back off from one minute to one hour, including across
worker restarts. Each call times out after 30 seconds. Successful synchronization
is rechecked hourly; changing the managed label set triggers an immediate attempt.
The `repository_label_sync` table exposes `last_error`, `last_success_at`, and
`next_attempt_at` for diagnosis. These rules apply to both App and PAT credentials.

Temporary GitHub failures do not change tracking selection or retire a repository.
A failed job retains its result and requires an explicit retry. Publication failures
retain their bounded retry behavior; restoring access does not automatically retry
a publication already marked failed. Use the dashboard's publication retry after
restoring access and reviewing the current workflow.

Failed outbound actions are retained with `last_error`; comment creation is not
automatically replayed because a failed response can hide a completed GitHub write.
Review GitHub before creating a replacement comment/status update. The existing
idempotent issue-closure label cleanup stops after eight failed attempts, with
persisted 30-second exponential delays capped at 15 minutes. Access
errors (including 401/403/404) stop automatic cleanup retries. After restoring
access, reconcile the labels on GitHub explicitly. These terminal failures remain
visible; they are not silently discarded or interpreted as repository deletion.

## TUI

Run `donkeyspace` and choose **Manage repositories**. The picker uses the saved
connection, preselects tracked repositories, and retains tracked entries even if
GitHub no longer lists them. Space toggles selections; Enter saves additions.
Deselection requires a separate `y` confirmation before saving; Esc discards it.
After saving, the existing trusted-identity screen is offered. Apply pending changes
through the headless command above after draining work.

This manages one instance. It does not coordinate multiple stacks tracking the same
repository; see #14.
