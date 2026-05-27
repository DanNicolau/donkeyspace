# donkeyspace

donkeyspace is a self-hosted harness for agentic repository work.

It coordinates issue triage, clarification, agent implementation, automated checks, reviewer-agent feedback, and human approval around the tools teams already use. The first target is a GitHub-first workflow where labels and comments remain the visible collaboration surface, while donkeyspace manages policy, orchestration, sandboxed agent runs, and audit history.

## V1 Direction

- GitHub-first: Issues, labels, comments, pull requests, and CI.
- Self-hosted: Docker Compose deployment for small engineering teams.
- Agent-runtime agnostic: orchestrates external agent CLIs instead of building a new coding agent.
- Human-gated by default: agents can prepare, implement, and review, but humans merge v1 PRs.
- Policy driven: repository config defines allowed automation, required checks, risk gates, and escalation behavior.

## Current Documents

- [Design document](docs/agentic-repo-harness-design.md)
- [V1 readiness checklist](docs/v1-readiness-checklist.md)
- [V1 decisions](docs/v1-decisions.md)
- [V1 agent contract](docs/v1-agent-contract.md)
- [V1 GitHub workflow](docs/v1-github-workflow.md)
- [V1 architecture stack](docs/v1-architecture-stack.md)
- [Example policy](docs/policy.example.yml)
- [Run result JSON Schema](schemas/run-result.schema.json)

## Working Assumptions

- Policy config lives at `.donkeyspace/policy.yml`.
- GitHub labels drive workflow state in v1.
- GitHub comments carry human clarification and agent summaries.
- Agent work runs in ephemeral containers.
- Tool-using agent work runs through external CLIs in a prepared workspace; bounded prompt context is only used for OpenAI-compatible triage.
- PostgreSQL stores run history, decisions, locks, and audit records.
- Rust powers the backend and worker; TypeScript React powers the dashboard.

## Development

Run the Rust checks:

```sh
cargo check --workspace
cargo test --workspace
```

Run the dashboard build:

```sh
cd web
npm install
npm run build
```

Start the local API, worker, and PostgreSQL services:

```sh
docker compose up --build
```

To let the worker clone private repos, push branches, open PRs, and apply pending GitHub labels/comments, set `DONKEYSPACE_GITHUB_TOKEN` before starting Compose. Compose automatically reads `.env` from the donkeyspace project directory. If your credentials live elsewhere, pass them explicitly:

```sh
docker compose --env-file /path/to/secrets.env up -d --force-recreate worker
```

Without the token, GitHub writes remain pending and private-repo checkout fails.

Triage defaults to `DONKEYSPACE_TRIAGE_PROVIDER=agent` in Docker Compose. Set `DONKEYSPACE_TRIAGE_PROVIDER=auto` to use an OpenAI-compatible chat endpoint when `DONKEYSPACE_LLM_API_KEY` or `OPENROUTER_API_KEY` is present. The default test configuration for that path is:

```sh
DONKEYSPACE_LLM_BASE_URL=https://openrouter.ai/api/v1
DONKEYSPACE_LLM_MODEL=openrouter/free
OPENROUTER_API_KEY=...
```

If the OpenAI-compatible triage path has no usable key, hits provider quota, or fails before producing a valid result, donkeyspace does not use deterministic fallback triage. It marks the triage job blocked and comments that LLM triage token usage was exceeded.

Set `DONKEYSPACE_TRIAGE_PROVIDER=agent` to run the configured `agents.triage.command` from `.donkeyspace/policy.yml` inside the prepared workspace. In this mode the worker writes `.donkeyspace/run-input.json`, expects `.donkeyspace/run-result.json`, and records the command exit code plus captured stdout/stderr in command results.

The default agent triage command is `donkeyspace-codex-triage`, a small wrapper around Codex CLI. The wrapper uses `schemas/run-result.codex.schema.json` for Codex structured output, then donkeyspace validates the result against the stricter Rust orchestration rules. It disables Codex's inner bubblewrap sandbox because the worker already runs inside Docker and common Docker hosts do not allow the user namespaces bubblewrap needs. For local testing, authenticate Codex once into the Compose-managed `codex-home` volume:

```sh
docker compose build worker
docker compose run --rm --no-deps worker codex login --device-auth
docker compose run --rm --no-deps worker codex login status
DONKEYSPACE_TRIAGE_PROVIDER=agent docker compose up -d --build worker
```

When triage returns `ready`, the worker queues a developer job if `agents.developer.enabled` is true. The default developer command is `donkeyspace-codex-developer`, which runs Codex CLI against the cloned checkout. If it returns `implemented`, donkeyspace commits the changed files, pushes a branch named `donkeyspace/issue-{number}-{job-id}`, opens a GitHub PR with a Conventional Commit title, and moves the issue to `ai:pr-open`.

Before pushing a developer branch, the worker runs every command in `checks.required_commands` from the policy file inside the repository checkout. Each command result is recorded in `command_results` and exposed through `/api/runs/{id}`. If any required command fails or cannot start, donkeyspace marks the developer job failed, moves the issue to `ai:blocked`, and writes the failed command summary to the issue through the GitHub action outbox.

The default local policy uses `git diff --check` because the current worker image is intentionally small. Repo-specific commands such as `cargo test`, `npm test`, or `make test` must be available inside the worker image or wrapped by a custom worker image.

When GitHub sends a `pull_request` webhook for a donkeyspace-managed PR, the API links it back to the source issue and queues a reviewer job. The default reviewer command is `donkeyspace-codex-reviewer`. Reviewer jobs fetch the PR head into the ephemeral checkout, receive PR metadata plus changed-file and diff context, and post their result as a PR conversation comment. V1 reviewer findings do not automatically start another developer job.

When GitHub sends a `push` webhook for a repository's default branch, donkeyspace queues repair checks for open donkeyspace-managed PRs targeting that branch. The repair worker locally attempts to merge the updated base branch into the PR branch. If the merge is clean, it records that no repair was needed. If Git reports conflicts, the default repair command `donkeyspace-codex-repair` resolves the merge conflict in the checkout, then donkeyspace commits and pushes the repair to the existing PR branch.

The worker also reconciles open donkeyspace-managed PRs on every poll. If a PR has no queued, leased, running, or completed repair check for its current recorded head/base pair, the worker queues one. `DONKEYSPACE_REPAIR_RECONCILE_LIMIT` controls the per-poll batch size and defaults to `1`.

The worker also reconciles ready issues on every poll. If a workflow item is already `ready` and has no queued, leased, or running developer job, the worker queues one from the most recent completed triage input. This covers worker restarts and old ready issues without requiring a new GitHub comment. `DONKEYSPACE_READY_RECONCILE_LIMIT` controls the per-poll batch size and defaults to `1`.

Closed GitHub issues are not eligible for agent work. The API records GitHub's issue state from webhooks and will not queue triage for closed issues. The worker also skips any already-queued closed-issue job before running an agent, and developer jobs verify the current GitHub issue state when `DONKEYSPACE_GITHUB_TOKEN` is available.

The worker clones the target repository into an ephemeral workspace before each agent run. The OpenAI-compatible triage path receives bounded excerpts from that checkout. The external-agent path runs the configured CLI in the prepared workspace so the agent can use its own file search, file read, and edit tools, then report through `.donkeyspace/run-result.json`. Tune prompt context with:

```sh
DONKEYSPACE_WORKSPACE_ROOT=/tmp/donkeyspace/workspaces
DONKEYSPACE_REPO_CONTEXT_MAX_BYTES=20000
DONKEYSPACE_REPO_CONTEXT_MAX_FILE_BYTES=4000
DONKEYSPACE_REPO_CONTEXT_MAX_FILES=12
```

Start the Vite dashboard dev server:

```sh
docker compose --profile dev up web
```

Useful local endpoints:

- `GET /healthz`
- `GET /api/runs`
- `GET /api/outbound-actions`
- `GET /api/runs/{id}`
- `GET /api/runs/{id}/transitions`
- `POST /api/runs/{id}/lease`
- `POST /webhooks/github`

The current workflow supports OpenAI-compatible and external-command triage plus Codex-backed developer, reviewer, and repair paths. A signed `issues.opened` webhook creates a triage run, the worker leases it, records the result, updates workflow state, and applies GitHub label/comment actions. Ready issues can now proceed to an agent-authored PR and Donkeyspace-managed PRs can be reviewed or repaired when needed.
