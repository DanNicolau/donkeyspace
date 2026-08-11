# Installation and authentication

The `donkeyspace` CLI is the reusable setup control plane for the future TUI.
Instance configuration uses `schema_version = 1` and lives under
`$XDG_CONFIG_HOME/donkeyspace` or `~/.config/donkeyspace` by default. Pass
`--config-dir` to select a different instance.

## First run

```sh
cargo build --bin donkeyspace
donkeyspace init --source-tree /path/to/donkeyspace
donkeyspace connect github --repositories OWNER/REPOSITORY
donkeyspace connect codex --method chatgpt
donkeyspace doctor
donkeyspace up
```

`init` is resumable and preserves connection settings. `doctor` is read-only:
it checks Docker and Compose, required source files, the API port, GitHub
repository access, secret permissions, and `codex login status`.

`down` preserves PostgreSQL and Codex/workspace volumes. Deletion requires the
explicit pair `reset --delete-data --confirm`.

## GitHub App

With no credential flags, `connect github` starts a CSRF-protected callback on
localhost, opens GitHub's App manifest registration, exchanges the one-time
manifest code, and writes the returned private key and webhook secret as
separate mode `0600` files. The manifest requests only:

- metadata: read;
- contents: write;
- issues: write; and
- pull requests: write.

The App is created under the personal account by default. Organization owners
can pass `--organization ORGANIZATION` to use the organization manifest route.
After installation, setup discovers the installation ID from the repository
owner and validates access to every selected repository. All selected
repositories must belong to that one owner.

When a public HTTPS URL is provided, the App subscribes to `issues`,
`issue_comment`, `pull_request`, and `push` with signed webhooks. Without one,
the selected repositories use delayed polling and the CLI prints a persistent
degraded-ingress warning. Polling is not real-time. GitHub's manifest schema
still requires a public webhook URL, so polling registrations use the official
documentation's `https://example.com/github/events` example with delivery
explicitly disabled; Donkeyspace does not send repository events there.

An existing private App can be imported with `--app-id`, `--installation-id`,
and `--private-key-file`. Add `--webhook-secret-file` when using `--public-url`.

The Compose environment interface is:

```text
DONKEYSPACE_GITHUB_AUTH_MODE=app|pat
DONKEYSPACE_GITHUB_APP_ID
DONKEYSPACE_GITHUB_INSTALLATION_ID
DONKEYSPACE_GITHUB_PRIVATE_KEY_FILE
DONKEYSPACE_WEBHOOK_SECRET_FILE
DONKEYSPACE_GITHUB_TOKEN  # deprecated PAT mode only
```

App and PAT values are mutually exclusive. The default Compose file binds the
API and dashboard to localhost, does not publish PostgreSQL, and does not mount
the Docker socket. Use `docker-compose.dev.yml` for Vite hot reload and
`docker-compose.plugins.yml` only when plugin execution needs Docker access.
The checked-in `.donkeyspace/compose-placeholder` is not a key or webhook
secret; it only lets an unauthenticated Compose configuration parse. Runtime
credentials are stored outside the source tree by default, and
`.donkeyspace/secrets/` is ignored as a defense against accidental commits.

## Codex

`connect codex --method chatgpt` runs `codex login` and leaves the browser flow
entirely with Codex. `--method api-key` reads a hidden prompt and pipes the key
to `codex login --with-api-key`; Donkeyspace does not persist the key. Both
branches finish with `codex login status`. Setup records only the Codex home
directory path so Compose can mount the CLI-owned credentials into the worker;
Donkeyspace never reads or copies the OAuth/API credentials. These are the two local sign-in
methods described by the
[official Codex authentication documentation](https://learn.chatgpt.com/docs/auth).

Direct OpenAI-compatible triage settings remain an advanced runtime option and
are not required during setup.
