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
Remaining work includes hard-crash/expired-orphan reconciliation, durable
workflow/job container identities and Docker creation-race recovery, and the
policy-gated manual cancel API/UI. A crashed worker can leave `cancel_requested`
jobs until that reconciliation exists. Ambiguous legacy managed branches remain
inactive until their original attribution can be established; this migration does
not guess or repair already misattributed historical rows. Cancellation acknowledgement
is validated on Unix workers; other platforms lack process-group guarantees.

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
