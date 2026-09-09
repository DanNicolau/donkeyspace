# Closed workflows

Closing a tracked GitHub issue with either `completed` or `not_planned` finishes
its workflow and records the provider's close reason. One database transaction
cancels queued, leased, waiting and paused jobs, requests cancellation of running
jobs, cancels pending approvals/publications/outbound actions, and queues removal
of the configured active state labels. History and checkpoint artifacts remain.
Duplicate close observations do not duplicate transitions or label cleanup.
Failed label cleanup is eligible for retry after 30 seconds.

A healthy worker checks cancellation and renews its owned lease every second.
Cancellation drops the workflow execution future, preventing its later steps,
and waits for all nested process supervisors to terminate processes and remove
containers before acknowledging `cancelled`. A cleanup failure leaves cancellation
unacknowledged for reconciliation. This uses the [process supervisor](process-cancellation.md);
restarting a stack alone does not establish cleanup after a hard crash.

Jobs and outbound actions carry a workflow generation. Reopening starts a new
generation and does not resume cancelled jobs or paused checkpoints. Older provider
timestamps are ignored; lifecycle edges are checked against GitHub when credentials
are configured, including same-second delayed close/reopen events. Already recorded
PRs retain their generation and cannot drive a reopened workflow. PR generation
validation, state/history updates and queued label changes share one transaction
and workflow lock. Reviewer jobs retain that admitted generation even when they
are created after a concurrent reopen. Worker state
writes, publication retries, and outbound dispatch reject cancelled or superseded
jobs. Git pushes, PR creation, and GitHub projections take a workflow row lock to
serialize admission with closure; closure can wait for an admitted operation.
Individual outbound/push/PR creation calls and each complete GitHub projection
batch are bounded to 60 seconds. A stalled projection releases the workflow lock
when its deadline expires so that closure can persist cancellation. A remote
request already accepted by GitHub cannot be retracted if its local caller times
out or loses its connection.

Migration `0003_workflow_cancellation.sql` adds provider timestamps, close reasons,
and generation columns, and installs job/publication/outbound fencing triggers.
It stamps existing job inputs with their generation and is safe to apply again.
Upgrade the API and worker together after draining existing execution; an older
worker does not implement cancellation acknowledgement. No policy changes are
required. The worker also converges previously stored closed workflows that still
have an active state, without resuming historical work.

New managed final/checkpoint and attempt branches include the full originating
job UUID. UUIDv7 IDs created close together share their leading timestamp bytes;
truncating them to eight characters previously allowed different runs to reuse a
branch. Full IDs preserve distinct branches, including after reopening.

Migration `0004_pull_request_generation.sql` resolves a managed PR's original
generation from that job ID even if the first PR delivery arrives after reopen.
For old shortened names, persisted publication records provide attribution when
they identify exactly one generation. Otherwise, after reopening, the PR is
retained with generation `0` (unknown) and cannot drive state, labels or jobs.
Repeated deliveries keep a known attribution. PRs first observed without a linked
workflow can be attributed when the workflow becomes known. Existing attributed
PRs and saved publication branch names remain unchanged. Non-managed human PRs
keep their existing issue-linking behavior.

This is a partial implementation of [#28](https://github.com/DanNicolau/donkeyspace/issues/28).
Remaining work includes recovery of unregistered native processes and legacy
containers, and the policy-gated manual cancel API/UI. Unknown ownership leaves
`cancel_requested` pending for operator reconciliation. Ambiguous legacy managed branches remain
inactive until their original attribution can be established; this migration does
not guess or repair already misattributed historical rows. Cancellation acknowledgement
is validated on Unix workers; other platforms lack process-group guarantees.

## Container launch ownership

Migration `0005_container_executions.sql` adds a durable launch registry. Before
issuing any Docker launch command, the worker commits a `container_executions` row with
the invocation UUID, unique container name, coordinator job UUID, workflow ID and
generation, lease owner, and hashed execution scope. Registration requires a
running coordinator with an unexpired lease owned by this worker; it takes the
workflow lock to serialize with closure/reopen. A new invocation gets a new name,
including validators and repeated invocations in the same task directory.

Docker labels `donkeyspace.execution-id`, `donkeyspace.coordinator-job-id`,
`donkeyspace.workflow-id` (when linked), and `donkeyspace.workflow-generation`
match that persisted row. The existing `donkeyspace.managed` and
`donkeyspace.execution-scope` labels remain. No credentials, repository contents,
or command arguments are stored in this registry or the new labels.

Launches run inside the coordinator's execution scope. An unscoped launch fails
before contacting Docker; future code that spawns a separate Tokio task must
explicitly propagate execution ownership and process supervision to that task.

Rows are **launch intents**, not evidence that a container exists or has stopped.
They remain after normal completion, cancellation, failed configuration/creation,
or worker death so the exact intended resource can be inspected later. This
release periodically reconciles these retained intents, including late Docker
materialization. Older launches are never backfilled by guessing ownership.

## Expired execution recovery

Migration `0006_execution_recovery.sql` records the Docker daemon ID read before
launch registration, a recovery-request timestamp on jobs, and separate cleanup
observations. Existing intent rows keep unknown (`NULL`) daemon ownership.
The migration runner reapplies the cancellation trigger from migration 0003,
which now also fences cancelled standalone jobs. Drain existing execution and
apply migrations before upgrading workers; no policy additions are required.

The worker runs a recovery task independently of its serial job loop. Every five
seconds after the preceding pass, it fences up to 100 expired root leases and
checks up to 100 retired intents for its Docker daemon. Each pass has a 30-second
deadline; each Docker command has a 10-second deadline. Busy workflow/job locks
are skipped. Child tasks inherit the coordinator's lifetime, not a separate lease.
Heartbeats cannot revive expired leases; expired leased UUIDs are never reassigned.
Unstarted leased jobs become cancelled; running jobs and their active children
request cancellation. Current open workflows move to `needs_human`, with history
and configured labels queued. Closed/reopened generations are not rewritten.
There is no automatic execution replay or checkpoint resume.

Cleanup requires an exact container name and all persisted identity labels. It
removes by immutable Docker container ID and verifies absence with a successful
Docker listing. Another daemon's absence, Docker errors, and ownership mismatches
are not cleanup proof. Errors are recorded and retried. Crashed coordinators with
registered containers are acknowledged only when every owning daemon has reported
successful cleanup since cancellation. A recovery worker must run against each
owning daemon. A daemon reset with a new identity requires operator reconciliation.

Successful cleanup observations do not retire launch intents: subsequent passes
continue checking them, so a Docker request accepted before worker death that
materializes later is eventually removed. This is eventual convergence, not an
atomic transaction with Docker: a late container can briefly exist after an
absence check or cancellation acknowledgement. Retained history grows and is
rotated in batches; cleanup latency grows with backlog or daemon failures.
No unbounded Docker creation delay can be proven finished by an absence check.
Unregistered native process groups and legacy containers require operator review;
recovery never guesses from PIDs, worker names, or path prefixes. GitHub requests
already accepted remotely retain the ambiguity described above.

For diagnostics or a bounded reconciliation without leasing new work, use
`donkeyspace-worker --recover-once`. It requires the same database and Docker
endpoint as the owning worker. An unsuccessful pass exits nonzero; individual
container failures remain in `container_cleanup_observations` for retry. A zero
exit status alone does not assert every orphan was removed.

## Regression and live validation

Run the ordinary workspace tests and dashboard build. The opt-in database test
requires a disposable PostgreSQL database named `donkeyspace_cancellation_test`:

```sh
DONKEYSPACE_CANCELLATION_TEST_DATABASE_URL=postgres://postgres:test-only@127.0.0.1:55438/donkeyspace_cancellation_test \
  cargo test -p donkeyspace-db closure_cancels_jobs_fences_publication_and_reopen_starts_fresh \
  -- --ignored --nocapture
```

It covers both close reasons, all active job states, late completion/failure,
publication retry fencing, retained checkpoints, generation fencing, stale
observations, duplicate close events, heartbeat ownership, side-effect admission
ordering, projection foreign-key lock compatibility, and legacy convergence.

Two further regressions use the same disposable database:

```sh
DONKEYSPACE_CANCELLATION_TEST_DATABASE_URL=postgres://postgres:test-only@127.0.0.1:55438/donkeyspace_cancellation_test \
  cargo test -p donkeyspace-db pr_effects_and_followup_jobs_cannot_cross_reopen \
  -- --ignored --nocapture
DONKEYSPACE_CANCELLATION_TEST_DATABASE_URL=postgres://postgres:test-only@127.0.0.1:55438/donkeyspace_cancellation_test \
  cargo test -p donkeyspace-worker stalled_projection_releases_lock_so_closure_can_commit \
  -- --ignored --nocapture
```

The PR regression interleaves close/reopen after a successful earlier generation
check and verifies that stale state, history, labels and reviewer jobs are fenced,
while a new PR remains accepted. The projection regression uses a local HTTP
endpoint that accepts the actual Octocrab PATCH request and never replies, with a
short injected deadline; concurrent closure must commit and later work is denied.

`late_managed_prs_keep_origin_generation_and_unknown_branches_stay_fenced` in
`donkeyspace-db` also runs with the same database and `--ignored`. It covers first
delivery after reopen, full job IDs, legacy publication provenance, ambiguous and
unknown branches, duplicate updates, initially unlinked PRs, and another reopen.
Worker branch tests use UUIDv7 IDs with identical timestamp prefixes to detect
branch collisions; API tests accept both legacy and full-UUID names.

`container_intents_require_live_ownership_and_retain_origin_after_reopen` runs
with the same isolated database and `--ignored`. It covers running/lease-owner
admission, expired leases, distinct invocations, committed intent visibility,
registration waiting behind closure, old-generation rejection after reopening,
retained provenance, and unlinked jobs. Ordinary worker tests verify identity
labels and reject unscoped launches before Docker is invoked.

The live harness is restricted to the user-authorized `EPIC-BLOCKCHAIN/umbrella`
test repository. It requires authenticated `gh`, Docker with `busybox:latest`,
and an **empty disposable** database named `donkeyspace_cancellation_live_test`
inside a PostgreSQL container accessible with `docker exec ... psql -U postgres`.
Build the changed API and worker first:

```sh
cargo build -p donkeyspace-api -p donkeyspace-worker
DONKEYSPACE_CLOSURE_LIVE_TEST=1 \
DONKEYSPACE_CLOSURE_TEST_DATABASE_URL=postgres://postgres:test-only@127.0.0.1:55438/donkeyspace_cancellation_live_test \
DONKEYSPACE_CLOSURE_TEST_DATABASE_CONTAINER=donkeyspace-cancellation-test-db \
  python3 tests/fixtures/closed-workflow/run.py
```

The harness starts an isolated API/worker, creates fresh scoped labels and one
fresh issue, and executes a deterministic TERM-ignoring container against an
umbrella checkout. It closes, reopens, and closes that issue with the other reason.
It verifies heartbeat renewal beyond a three-second lease, distinct jobs on
reopening, cancellation acknowledgement only after container removal, active
label removal, duplicate delivery handling and a delayed old close event.
It also compares each live container's labels with its committed launch record
and verifies distinct names/generations and unchanged records after cleanup.
Signed webhook payloads contain actual GitHub issue snapshots but are delivered
locally by the harness; no installed webhook configuration is changed. This tests
GitHub reads/writes and the changed ingress/worker/container path, not GitHub's
webhook transport, paid agents, or hardware tools.

Add `DONKEYSPACE_REOPEN_PR_LIVE_TEST=1` to also create two temporary draft PRs for
the fresh test issue. The changed worker publishes each initial checkpoint to its
full-job-ID branch. The harness reads that persisted branch, adds a tiny test
commit so GitHub can open a PR, and withholds the first PR delivery until after
reopening. It verifies old PR/duplicate rejection and new PR acceptance through
the changed API, and checks that the first branch's commit remains unchanged.
It closes both PRs and deletes the worker-published branches in cleanup. This
exercises the production branch formatter, checkpoint push and PR attribution;
the extra commits required for nonempty PRs are created by the harness.

Evidence and logs are written under `/tmp/donkeyspace-closure-live-<run>/`, including
the source revision, dirty-tree flag, umbrella revision, issue URL and job IDs.
The harness stops its API/workers, removes its containers and labels, and leaves
the new issue closed. Retain evidence and logs, then remove the disposable database
container and temporary workspaces after inspection. Never reuse a terminated
historical run or point the harness at an existing stack database.

Add `DONKEYSPACE_RECOVERY_LIVE_TEST=1` (separately from the PR scenario) to SIGKILL
the fresh worker in both generations. The first open workflow verifies expiry,
no replay, ownership-mismatch refusal, cleanup retry, needs-human labeling, and
late container materialization after a prior successful absence check. The second
verifies closed-workflow recovery after Docker unavailability, with duplicate
close delivery and preserved launch history. The deterministic late-materialization
fixture recreates the registered resource; it does not delay an actual daemon
create request. The ordinary closure mode still validates healthy supervision.

The database regression
`expired_coordinators_are_fenced_once_and_cleanup_requires_all_owning_daemons`
covers concurrent recoverers, busy locks, healthy roots/children, lease expiry,
late writes, daemon ownership, failed observations, retained tombstones, reopen,
and unknown legacy ownership. Run all DB regressions with `--ignored --test-threads=1`
in the isolated test database; concurrent migration setup can otherwise contend
with active fixtures.
