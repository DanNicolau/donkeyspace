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
data, revoke GitHub installation access, close issues, or cancel jobs. Existing
queued work and outbound actions are retained: drain them before applying removal.
Re-adding a removed repository starts with empty starter and approver scopes.

Keep at least one repository in a connected instance. Removing the last repository
is rejected because an authenticated runtime requires a nonempty tracked selection;
this command does not disconnect GitHub or invent a new installation boundary.

## TUI

Run `donkeyspace` and choose **Manage repositories**. The picker uses the saved
connection, preselects tracked repositories, and retains tracked entries even if
GitHub no longer lists them. Space toggles selections; Enter saves additions.
Deselection requires a separate `y` confirmation before saving; Esc discards it.
After saving, the existing trusted-identity screen is offered. Apply pending changes
through the headless command above after draining work.

This manages one instance. It does not coordinate multiple stacks tracking the same
repository; see #14.
