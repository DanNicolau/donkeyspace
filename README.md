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

Start the Vite dashboard dev server:

```sh
docker compose --profile dev up web
```

Useful local endpoints:

- `GET /healthz`
- `GET /api/runs`
- `GET /api/runs/{id}`
- `POST /api/runs/{id}/lease`
- `POST /webhooks/github`
