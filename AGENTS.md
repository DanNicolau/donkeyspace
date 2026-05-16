# Repository Guidelines

## Project Structure & Module Organization

donkeyspace is a Rust workspace with a TypeScript React dashboard.

- `crates/donkeyspace-core`: shared policy, state, run-result, and GitHub workflow logic.
- `crates/donkeyspace-api`: Axum HTTP API and GitHub webhook handling.
- `crates/donkeyspace-worker`: job leasing, agent execution, and outbound GitHub action processing.
- `crates/donkeyspace-db`: PostgreSQL access and migrations.
- `crates/donkeyspace-github`: GitHub signature verification and Octocrab client helpers.
- `web/`: Vite React dashboard.
- `docs/`, `schemas/`, `migrations/`, `.donkeyspace/`: design docs, JSON schema, SQL migrations, and example policy.

Rust tests live beside code in `#[cfg(test)]` modules. Frontend source lives in `web/src`.

## Build, Test, and Development Commands

```sh
cargo check --workspace
```
Type-check the Rust workspace.

```sh
cargo test
```
Run Rust unit and doc tests.

```sh
cargo fmt --all -- --check
```
Verify Rust formatting.

```sh
cd web && npm install && npm run build
```
Install dashboard dependencies and build the React app.

```sh
docker compose up -d --build
```
Run PostgreSQL, API, and worker locally. Set `DONKEYSPACE_WEBHOOK_SECRET` and `DONKEYSPACE_GITHUB_TOKEN` for live GitHub webhook tests.

## Coding Style & Naming Conventions

Use `rustfmt` defaults for Rust. Keep modules focused by crate responsibility and prefer explicit names such as `record_state_transition` or `list_pending_outbound_actions`. Rust functions and variables use `snake_case`; types use `PascalCase`.

Frontend code uses TypeScript, React function components, and CSS in `web/src/styles.css`. Keep UI copy concise and operational.

## Testing Guidelines

Add Rust unit tests near the behavior they cover. Name tests after expected behavior, for example `donkeyspace_comment_does_not_queue_triage`. For webhook or worker changes, cover both the accepted path and loop/duplicate prevention. Run `cargo test` and `npm run build` before opening a PR.

## Commit & Pull Request Guidelines

The history is informal but concise. Prefer imperative commits with a useful prefix when applicable, for example `fix: prevent issue comment loop` or `feat: execute outbound github actions`.

PRs should include:

- Summary of behavior changed.
- Tests run.
- Linked issue or rationale.
- Screenshots for dashboard changes.
- Notes about GitHub tokens, webhook setup, or migration impact when relevant.

## Security & Configuration Tips

Never commit real `.env` values or GitHub tokens. Use `.env.example` for placeholders. Webhook secrets must match GitHub’s configured secret. Treat live GitHub tests as side-effectful: use a test repository before enabling new automation on important repos.
