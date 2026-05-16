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
- Tool-using agent work runs through external CLIs in a prepared workspace; bounded prompt context is only the fast path or fallback.
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

To let the worker apply pending GitHub labels and comments, set `DONKEYSPACE_GITHUB_TOKEN` in your environment before starting Compose. Without it, outbound actions remain pending and visible in the dashboard.

Triage defaults to `DONKEYSPACE_TRIAGE_PROVIDER=auto`. If `DONKEYSPACE_LLM_API_KEY` or `OPENROUTER_API_KEY` is present, the worker calls an OpenAI-compatible chat endpoint. The default test configuration is:

```sh
DONKEYSPACE_LLM_BASE_URL=https://openrouter.ai/api/v1
DONKEYSPACE_LLM_MODEL=openrouter/free
OPENROUTER_API_KEY=...
```

Set `DONKEYSPACE_TRIAGE_PROVIDER=deterministic` to force local no-token triage.

The worker clones the target repository into an ephemeral read-only triage workspace before deciding whether an issue is ready. The current OpenAI-compatible triage path receives bounded excerpts from that checkout. The intended v1 agentic path is to run a configured external agent CLI in the prepared workspace so the agent can use its own file search and read tools, then report through `.donkeyspace/run-result.json`. Tune prompt context with:

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

The current workflow supports deterministic and OpenAI-compatible triage. A signed `issues.opened` webhook creates a triage run, the worker leases it, marks it running, writes a result, completes the run, records a workflow transition, and creates pending GitHub label/comment actions in the outbound action outbox.
