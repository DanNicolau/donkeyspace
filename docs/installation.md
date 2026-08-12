# Installation and authentication

The `donkeyspace` CLI and TUI share one reusable setup control plane.
Instance configuration uses `schema_version = 1` and lives under
`$XDG_CONFIG_HOME/donkeyspace` or `~/.config/donkeyspace` by default. Pass
`--config-dir` to select a different instance.

## First run

The recommended interactive path is:

```sh
cargo build --bin donkeyspace
donkeyspace
```

Running `donkeyspace` without a subcommand opens the terminal interface. On an
uninitialized instance it starts the setup wizard; configured instances open
the operations home screen. Use arrow keys or Tab to move, Enter to continue,
Space to select repositories or toggle webhook ingress, Esc to go back, and
`q` to leave the home screen.

The wizard creates or resumes the local-build instance, connects GitHub and
Codex, runs `doctor`, and offers to start the stack when required checks pass.
The home screen refreshes Compose service state every two seconds and provides
Doctor, Start, Stop, and authentication reconfiguration actions. Stop preserves
all volumes. Live logs and destructive reset remain explicit CLI operations.

The equivalent non-interactive commands are:

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

Bare-command TUI startup requires an interactive stdin and stdout. Automation
must use an explicit subcommand; this prevents terminal control sequences from
being written to redirected output.

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

The TUI presents the same manifest flow as its primary option. After App
installation it fetches every repository accessible to the installation and
shows a checkbox list; one or more repositories from the configured owner must
be selected. Existing-App import and deprecated PAT authentication are under
the Advanced choices.

If the process stops after manifest conversion but before repository selection,
the TUI resumes from a private `pending-github.json` record on its next launch.
The pending record contains only identifiers, file paths, and setup choices;
the private key and webhook secret remain separate mode `0600` files. The user
can resume installation or explicitly discard the pending registration.

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

In the TUI, ChatGPT login temporarily restores the normal terminal while Codex
owns the browser interaction, then returns to the full-screen interface and
checks login status. API-key input is masked and piped directly to Codex; it is
cleared from UI state and never written to instance configuration.
