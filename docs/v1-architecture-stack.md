# V1 Architecture Stack

donkeyspace uses Rust for backend reliability and TypeScript React for UI work.

## Backend

The backend should be a Rust workspace.

Recommended crates:

- `axum` for the HTTP API and GitHub webhook endpoints.
- `tokio` for async runtime and worker execution.
- `sqlx` for PostgreSQL access and migrations.
- `serde` and `serde_json` for structured payloads and agent contracts.
- `serde_yaml` for `.donkeyspace/policy.yml`.
- `octocrab` for GitHub API calls where it fits cleanly.
- `tracing` and `tracing-subscriber` for structured logs.
- `thiserror` for domain errors.
- `clap` for internal CLI commands such as migrations, worker startup, or local test utilities.

The backend workspace should separate concerns without over-splitting early:

- `donkeyspace-api`: HTTP server, webhook intake, dashboard API.
- `donkeyspace-worker`: background job runner.
- `donkeyspace-core`: policies, state machine, agent contract types, risk routing.
- `donkeyspace-db`: database models, migrations, repositories.
- `donkeyspace-github`: GitHub webhook parsing and API operations.
- `donkeyspace-runner`: sandbox and external agent command execution.

If this feels too much for the first commit, start with one Rust crate and keep module boundaries named after these components.

## Frontend

The UI should be TypeScript React.

Recommended stack:

- Vite for development/build tooling.
- React for the dashboard UI.
- TanStack Query for server state.
- A small generated or hand-written API client until the API stabilizes.

The v1 UI is intentionally narrow:

- Active runs list.
- Recent runs list.
- Run detail page.
- Links to GitHub issue/PR.
- State transitions.
- Command results.
- Policy snapshot.
- Escalation and failure reasons.

Policy editing is deferred.

## Service Shape

Docker Compose should run:

- Rust API service.
- Rust worker service.
- PostgreSQL.
- TypeScript React dashboard, either served by the Rust API after build or by a separate development service.

For v1, serving static dashboard assets from the Rust API is preferred for deployment simplicity. During development, Vite can run separately.

## First Vertical Slice

The first implementation slice should prove the backend loop before UI polish:

1. Parse `.donkeyspace/policy.yml`.
2. Receive a GitHub issue webhook.
3. Validate the webhook signature.
4. Store a job and state transition in PostgreSQL.
5. Run a fake triage agent command.
6. Validate `.donkeyspace/run-result.json`.
7. Compute the next issue state.
8. Record the decision and expose it through an API endpoint.

After that works, add GitHub label/comment writes and the minimal dashboard.
