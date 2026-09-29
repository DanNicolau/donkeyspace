# donkeyspace Architecture and Scope

This document describes the system that exists today and its current
boundaries. Operational details live in the focused GitHub workflow, agent
contract, and policy documents.

## Purpose

donkeyspace is a self-hosted orchestration and policy harness for agentic
repository work. GitHub issues, labels, comments, and pull requests remain the
human-visible collaboration surface. donkeyspace coordinates agent jobs,
applies repository policy, records state and side effects, and routes uncertain
work to people.

The default deployment targets a small team operating one GitHub repository at
a time. Humans remain responsible for merging pull requests.

## Current Workflow

1. A signed GitHub issue webhook records the delivery and may queue triage.
2. A worker leases the job and prepares a fresh repository checkout.
3. The triage agent returns `ready`, `needs_info`, `needs_human`, `blocked`, or
   `failed` through `.donkeyspace/run-result.json`.
4. A ready issue is reconciled into a developer job.
5. The developer edits the checkout. donkeyspace runs policy-required commands,
   commits the changes, pushes a `donkeyspace/issue-*` branch, and opens a pull
   request.
6. Pull request webhooks queue reviewer jobs for managed, non-draft PRs.
7. Default-branch pushes and periodic reconciliation queue repair checks. The
   repair agent runs only when the base branch cannot be merged cleanly.
8. GitHub labels and comments communicate the result; PostgreSQL retains the
   job, transition, command, PR, and outbound-action records.
9. Semantic lifecycle events record the trigger source, agent waves, task
   outcomes, approvals, handoffs, publications, and final PR as an ordered
   issue-level history.

In the default lifecycle, review findings do not automatically requeue
development. Lifecycle plugins may define bounded task feedback edges.
Donkeyspace does not merge pull requests.

## Components

- `donkeyspace-api`: Axum API, webhook signature validation and intake, job
  inspection, leasing, and manual retry.
- `donkeyspace-worker`: polling, job execution, repository preparation, policy
  checks, reconciliation, and GitHub action delivery.
- `donkeyspace-core`: workflow state, policy routing, and run-result types.
- `donkeyspace-db`: PostgreSQL records and lease operations.
- `donkeyspace-github`: GitHub API helpers and webhook signature verification.
- `donkeyspace-cli`: reusable installation services, scriptable setup commands,
  and the Ratatui installation and operations console.
- `donkeyspace-runner`: external command execution and structured result
  validation.
- `web`: React and TanStack Query dashboard served by Vite in the current
  Compose development stack.

Docker Compose runs PostgreSQL, the API, the worker, and the dashboard. Built-in agent
commands run in supervised worker processes. Plugin tasks run in separate
containers with filtered workspaces and durable execution ownership.

## State and Audit Model

GitHub labels are the visible workflow state. donkeyspace keeps one configured
workflow label active when it performs a transition and treats conflicting
workflow labels as a human-review condition.

PostgreSQL stores:

- repositories and workflow items;
- idempotent webhook deliveries;
- jobs, retry lineage, owners, and lease expiry;
- state transitions and structured run results;
- managed pull request metadata;
- command results; and
- pending, completed, or failed outbound GitHub actions.
- accepted-checkpoint and forensic-attempt branch publications, including
  retry state and commit links.
- bounded user-facing lifecycle events; prompts and raw logs are deliberately
  excluded, while diagnostic branches remain available as links.

The action outbox records label and comment writes before the worker sends them
to GitHub. Rapid parent-issue status updates coalesce into one pending upsert,
so GitHub contains one live lifecycle comment rather than a comment per event.

## Agent Runtime

External agent commands and optional lifecycle plugins are selected in policy.
The reference commands wrap Codex CLI for triage, development, review, and
merge repair. Lifecycle plugins supply their own roles, prompts, images, and
task graphs. donkeyspace owns repository checkout, result validation, required
checks, Git commits, pushes, PR creation, labels, and comments.

An optional OpenAI-compatible triage path receives bounded repository excerpts.
It is a single model request rather than a donkeyspace-owned repository-tool
loop. Provider or quota failure blocks triage; there is no deterministic
fallback.

## Policy and Safety Boundaries

Policy can enable roles, require allow labels, define block labels, run local
commands, and route high-risk, unknown-risk, or sensitive-path changes to human
review. Issue closure cancels and fences work using workflow generations. Required
GitHub checks, automatic merge, automatic retries and configurable maximum
concurrency are not implemented; version 2 rejects their former no-op settings.

GitHub App credentials are the default. The API and worker authenticate as one
configured installation; Octocrab refreshes installation tokens for REST calls
and the worker obtains a current token for authenticated git operations. App
webhooks from any other installation are rejected. Fine-grained PATs remain a
deprecated compatibility mode. Codex authentication is delegated to Codex CLI.

## Known Limitations

- No GitHub check-run/check-suite enforcement or CI-status evaluation.
- No automatic merge or reviewer-to-developer feedback loop in the default
  lifecycle; plugins may define bounded task-level feedback.
- No cancellation API, per-command timeout, or provider pause/resume control.
- Built-in agent commands have no per-job container or VM boundary; plugin tasks
  run in separate containers. Network isolation is not configurable.
- V1 supports one GitHub repository owner per Donkeyspace instance; setup
  discovers that owner's manifest-created App installation automatically.
- No token accounting or configurable retention policy.
- The dashboard does not yet show GitHub links, transitions, policy snapshots,
  or engagement decisions, although decision records are available from the API.
- Registry images are modeled by setup but intentionally unavailable until a
  release-image backend exists.
- Automated tests cover PostgreSQL transactions, deterministic plugin lifecycles,
  and dashboard flows. Live GitHub and production model/tool runs still require
  deployment smoke tests.
- Donkeyspace does not coordinate changes across repositories.

## Transaction and lifecycle boundaries

GitHub intake prepares network-dependent authorization before entering a database
transaction. The delivery receipt, engagement audit, queued/resumed jobs, lifecycle
history and outbound actions commit together. Issue closure observations converge
independently; intake checks the observed generation/state again under a row lock.
Transient authorization failures and stale admission snapshots remain retryable.

The lifecycle coordinator owns typed in-memory progress and task graph state.
Separate modules execute tasks, apply human decisions, capture/persist checkpoints,
track jobs, publish artifacts and reconcile projected work items. One checkpoint
constructor serves every pause and wave, and one projection path handles initial,
proposed and accepted plans. Legacy checkpoint interpretation lives at import.
PostgreSQL is the progress authority; retained workspaces hold repository content.
