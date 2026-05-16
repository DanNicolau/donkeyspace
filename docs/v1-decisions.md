# V1 Decisions

This document records defaults chosen to reduce ambiguity before implementation begins.

## Project Name

The project name is `donkeyspace`.

Use lowercase `donkeyspace` in package names, commands, config paths, container names, and documentation unless grammar requires sentence capitalization.

## Policy Format

V1 policy config uses YAML.

Default path:

```text
.donkeyspace/policy.yml
```

YAML is the default because it is familiar for GitHub-native teams, works well for lists of commands and path rules, and is easy to edit in repositories. TOML can be reconsidered later if the config becomes more application-like than workflow-like.

## Human Control Surface

V1 uses GitHub labels and normal issue/PR comments.

Slash commands are deferred. Comments should be treated as human context or clarification unless later policy explicitly enables command parsing.

## GitHub Identity

V1 should use a GitHub App as the preferred integration identity.

The GitHub App should request only the permissions needed to:

- Read issues, pull requests, repository contents, and checks.
- Write issue comments.
- Apply and remove labels.
- Create branches.
- Push commits.
- Open pull requests.

A personal access token can be supported for local development, but it should not be the recommended team deployment path.

## Database

V1 self-hosted deployment should use PostgreSQL.

PostgreSQL stores:

- Installation and repository records.
- Issue and PR workflow state snapshots.
- Job records and leases.
- State transitions.
- Policy snapshots and decisions.
- Agent summaries.
- Command results.
- Log metadata.

SQLite can be considered later for local development, but Docker Compose should default to PostgreSQL so the production and development shapes do not diverge too early.

## Implementation Stack

donkeyspace should use Rust for backend services and TypeScript React for UI work.

Backend defaults:

- Rust workspace.
- `axum` for HTTP and webhook endpoints.
- `tokio` for async execution.
- `sqlx` for PostgreSQL access and migrations.
- `serde`, `serde_json`, and `serde_yaml` for structured data.
- `tracing` for logs.

Frontend defaults:

- TypeScript React.
- Vite.
- React.
- TanStack Query for dashboard API state.

The first backend implementation should prioritize a working vertical slice over perfect crate boundaries. If needed, start with one Rust crate and keep modules aligned with the eventual service boundaries.

The v1 dashboard should be buildable as static assets and served by the Rust API in production. Vite can run separately during development.

## Agent Result Contract

Agent CLIs should return a structured result file in addition to logs.

The first reference adapter should target Codex CLI through the generic command-runner contract. donkeyspace should still treat agent execution as configurable commands so other CLIs can be added without changing the orchestration model.

Default path inside the run workspace:

```text
.donkeyspace/run-result.json
```

The result should include:

- `outcome`: `ready`, `needs_info`, `implemented`, `needs_changes`, `needs_human`, `blocked`, or `failed`.
- `summary`: short human-readable explanation.
- `confidence`: `low`, `medium`, or `high`.
- `risk`: `low`, `medium`, `high`, or `unknown`.
- `questions`: clarifying questions when the outcome is `needs_info`.
- `tests`: commands run and their results.
- `changed_files`: files touched by the run when available.
- `human_review_reason`: required when the outcome is `needs_human`.
- `blocked_reason`: required when the outcome is `blocked` or `failed`.

The orchestrator should treat missing, invalid, or contradictory result files as `needs_human` or `failed`, depending on whether the agent produced useful partial work.

## Agent Repository Access And Triage Runtimes

V1 should prefer external agent CLIs with workspace-native file and search tools over a custom donkeyspace LLM tool loop.

The worker may still build bounded repository context for cheap deterministic or OpenAI-compatible triage, but that prompt context is a fallback and fast-path aid rather than the long-term agent interface. For actual agentic triage, implementation, and review, donkeyspace should create the repository workspace, write `.donkeyspace/run-input.json`, run the configured agent command in that workspace, and read `.donkeyspace/run-result.json`.

Triage agents should receive read-only repository access. Developer agents may write to the workspace and create branches or PRs according to policy. Reviewer agents should inspect diffs, test output, and policy context, and should avoid mutating repository state except for structured review output and approved GitHub comments.

donkeyspace should eventually support two agentic triage workflows:

- External CLI triage: run a configured agent command such as Codex CLI in the prepared workspace. This remains the first reference path because mature CLIs already provide file search, file read, and tool-use loops.
- Built-in OpenAI-compatible triage: run a donkeyspace-owned tool loop against OpenRouter or another OpenAI-compatible endpoint. OpenRouter acts as the model router, while donkeyspace exposes and executes tools such as `repo.list_tree`, `repo.search`, `repo.read_file`, `issue.context`, `policy.read`, and `result.write`.

The built-in OpenAI-compatible loop should be implemented after the external command path is working. That keeps the near-term project focused on orchestration while preserving a longer-term default that does not require every installation to install and authenticate a separate agent CLI.

## Duplicate Work Prevention

V1 should use both visible GitHub state and an internal lease.

The workflow manager should:

- Check the current GitHub labels before starting work.
- Acquire a database lease for the issue or PR before scheduling an agent run.
- Record the GitHub webhook delivery ID or equivalent idempotency key.
- Re-check current labels before applying a state transition.
- Release or expire leases on completion, failure, timeout, or cancellation.

GitHub labels make state visible to humans. Database leases prevent duplicate worker execution.

## Dashboard Scope

The v1 dashboard is read-only except for operational controls such as retry or cancel if those are implemented.

Minimum views:

- Active jobs.
- Run history.
- Run detail with logs and command results.
- Issue and PR links.
- State transitions.
- Policy snapshot used for a run.
- Escalation and failure reasons.

Policy editing through the UI is deferred.

The first useful dashboard slice is the run list plus run detail page. It should answer: what is running, what recently ran, why did it transition, what commands executed, and where is the linked GitHub issue or PR?

## Local Development Credentials

Local development may use a personal access token when setting up a GitHub App would slow down early iteration.

Recommended local environment variables:

- `DONKEYSPACE_GITHUB_TOKEN`
- `DONKEYSPACE_WEBHOOK_SECRET`
- `DONKEYSPACE_DATABASE_URL`
- `DONKEYSPACE_AGENT_IMAGE`

Team deployments should use GitHub App installation credentials instead of a personal token.

## Automatic Merge

Automatic merge is not part of default v1 behavior.

Future automatic merge should require all of the following:

- Repository policy explicitly enables it.
- The PR is classified as low risk.
- Required local commands passed.
- Required GitHub checks passed.
- Reviewer agent returned a high-confidence approval.
- No protected paths or labels require human review.
- GitHub branch protection allows the merge.

If any condition is unclear, route to human review.

## Multi-Repo Work

V1 operates on one repository at a time.

Future multi-repo support should be modeled around workspace groups: named sets of repositories with declared relationships, shared policies, and cross-repo impact analysis. V1 should avoid architecture choices that make workspace groups difficult later, but it should not implement cross-repo pull requests.
