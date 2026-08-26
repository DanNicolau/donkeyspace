# Installation and authentication

The `donkeyspace` CLI and TUI share one reusable setup control plane.
Instance configuration uses `schema_version = 4` and lives under
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

The wizard creates or resumes the local-build instance, connects GitHub,
configures per-repository trusted identities, connects Codex, runs `doctor`,
and offers to start the stack when required checks pass.
The home screen refreshes Compose service state every two seconds and provides
Doctor, Start, Stop, and authentication reconfiguration actions. Stop preserves
all volumes. Live logs and destructive reset remain explicit CLI operations.

For a headless remote host, establish the manifest callback tunnel from your
workstation before starting GitHub App registration:

```sh
ssh -N -L 8787:127.0.0.1:8787 USER@HOST
```

Keep the tunnel open and visit `http://127.0.0.1:8787/` in your workstation's
browser when the TUI displays the registration prompt. Both the registration
URL and the later GitHub App installation URL remain visible for copying.
Donkeyspace skips automatic browser launch when the host has no graphical
display.

The equivalent non-interactive commands are:

```sh
cargo build --bin donkeyspace
donkeyspace init --source-tree /path/to/donkeyspace
donkeyspace connect github --repositories OWNER/REPOSITORY
donkeyspace configure github-access --repository OWNER/REPOSITORY add --user GITHUB_LOGIN
donkeyspace connect codex --method chatgpt
donkeyspace doctor
donkeyspace up
```

First-run automation can select stable host ports explicitly:

```sh
donkeyspace init --source-tree /path/to/donkeyspace \
  --api-port 8081 --web-port 5174
```

Interactive setup suggests nearby available ports when the defaults are busy.
For an existing instance, change either or both ports with:

```sh
donkeyspace configure ports --api-port 8081 --web-port 5174
```

Port changes are persisted but do not silently restart a running stack. Run
`donkeyspace down` followed by `donkeyspace up` to apply them.

`init` is resumable and preserves connection settings. `doctor` is read-only:
it checks Docker and Compose, required source files, the API and dashboard ports, GitHub
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
- issues: write;
- pull requests: write; and
- members: read.

The App is created under the personal account by default. Organization owners
can pass `--organization ORGANIZATION` to use the organization manifest route.
After installation, setup discovers the installation ID from the repository
owner and validates access to every selected repository. All selected
repositories must belong to that one owner.

The TUI presents the same manifest flow as its primary option. After App
installation it fetches every repository accessible to the installation and
shows a checkbox list; one or more repositories from the configured owner must
be selected. It then provides per-repository user, owner-organization, and
owner-team access management. Existing-App import and deprecated PAT
authentication are under the Advanced choices.

`Members: read` is used only to verify organization and team membership. An
existing App without it can still use explicit user rules. To enable organization
or team rules, the App owner adds the permission under
`https://github.com/settings/apps/APP-SLUG/permissions`, then the organization
owner approves the update under
`https://github.com/organizations/ORGANIZATION/settings/installations`.

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
DONKEYSPACE_GITHUB_REPOSITORIES
DONKEYSPACE_GITHUB_TOKEN  # deprecated PAT mode only
```

App and PAT values are mutually exclusive. The default Compose file binds the
API and dashboard to localhost, does not publish PostgreSQL, and does not mount
the Docker socket. The installer writes the configured host bindings through
`DONKEYSPACE_API_PORT` and `DONKEYSPACE_WEB_PORT`; container-internal ports stay
fixed. Use `docker-compose.dev.yml` for Vite hot reload. The setup control plane
generates a private effective policy and plugin overlay, and mounts the Docker
socket only on the worker when a plugin flow is active.
The checked-in `.donkeyspace/compose-placeholder` is not a key or webhook
secret; it only lets an unauthenticated Compose configuration parse. Runtime
credentials are stored outside the source tree by default, and
`.donkeyspace/secrets/` is ignored as a defense against accidental commits.

### SELinux hosts

The Compose files label host bind mounts for container access on
SELinux-enforcing Fedora, RHEL, CentOS Stream, Rocky Linux, and AlmaLinux
hosts. Policy and plugin paths use the shared `z` label because both the API
and worker mount them. A host-backed Codex home uses the private `Z` label and
the setup CLI configures it automatically. Named volumes require no relabeling.
Do not add `z` or `Z` to the Docker socket mount.

If startup still reports `Permission denied (os error 13)`, confirm whether
SELinux denied the access with `getenforce` and
`sudo ausearch -m AVC -ts recent`. Avoid disabling SELinux or broadly changing
file modes; correct the specific bind mount label instead.

## GitHub engagement access

Each selected repository has separate job-starter and human-approver lists.
Both start empty for new installations and fail closed. Manage them
interactively from **Manage GitHub access** or headlessly:

```sh
donkeyspace configure github-access --repository OWNER/REPOSITORY list
donkeyspace configure github-access --repository OWNER/REPOSITORY add --user USER
donkeyspace configure github-access --repository OWNER/REPOSITORY add --organization OWNER
donkeyspace configure github-access --repository OWNER/REPOSITORY add --team OWNER/TEAM-SLUG
donkeyspace configure github-access --repository OWNER/REPOSITORY remove --user USER
donkeyspace configure github-access --repository OWNER/REPOSITORY --scope approvers list
donkeyspace configure github-access --repository OWNER/REPOSITORY --scope approvers add --team OWNER/TEAM-SLUG
```

Organization and team entries must belong to the repository owner. Changes are
saved outside the source tree and recreate a running API service automatically;
already queued or running jobs continue. Removing the final entry returns that
scope to deny-all. `doctor` fails until every selected repository has at least
one job starter and one human approver. Schema-v4 instances initially copy
their existing trusted identities into both independent lists.

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

## Plugins

Connect a local plugin repository with the same control plane used by the TUI:

```sh
donkeyspace connect plugin --path ../donkey-kong --flow rtl_blocks
donkeyspace plugin list
```

The manifest's optional `installation` section tells Donkeyspace how to build
the image and which environment inputs it accepts. Images are built only when
missing; use `donkeyspace plugin rebuild ID` for an explicit rebuild. Supply
non-interactive values through private files, never command-line values:

```sh
donkeyspace connect plugin --path ../example-plugin \
  --environment-file PROVIDER_TOKEN=/secure/provider-token
```

Several plugins may be installed, but exactly one flow can be active. Activate
or switch with `donkeyspace plugin activate ID --flow FLOW`. Disable plugin
execution with `donkeyspace plugin disable`; this restores the default policy
without deleting checkouts, images, configuration, or secret files. The TUI's
“Manage plugins” action provides connect, activate, rebuild, and disable
operations and clearly marks lifecycle-replacement flows as exclusive.

An active plugin may provide facade defaults for the dashboard and GitHub
workflow. Override any field for one installation with:

```sh
donkeyspace configure facade \
  --display-name "ePIC Agent Platform" \
  --tagline "Agentic hardware design workflow" \
  --command epic-agent
```

Restart a running stack after changing the facade. Use
`donkeyspace configure facade --reset` to return to plugin and policy defaults.
