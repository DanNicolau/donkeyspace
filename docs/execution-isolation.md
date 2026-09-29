# Execution isolation

Built-in triage, developer, reviewer, and repair commands and configured required
checks run in disposable containers through the same registered execution path
as plugin agents and validators. The worker coordinates execution and performs
publication; it does not fall back to running an agent or check as a subprocess
when Docker or the execution image is unavailable.

Compose builds the worker image as `donkeyspace-execution:local` and supplies
`DONKEYSPACE_AGENT_IMAGE` with that name. Standalone workers must set this variable
to an image containing their configured commands. A custom image must already be
present on the worker's Docker daemon: launches use `--pull=never`. The base worker
now requires Docker socket access even without a plugin; execution containers do
not receive that socket. Plugin overlays no longer duplicate the worker mount.

Each execution mounts its own directory at `/workspace`. Built-in agents receive
the checkout at `repo`, the input/result protocol under `.donkeyspace`, and a
policy snapshot at `.donkeyspace/policy.json`. Required checks mount just the
checkout and run there. Plugin task directories remain filtered by their manifest
and policy access rules. Worker paths are not passed as executable checkout paths.

For a named workspace volume, the launcher uses `volume-subpath`; it rejects a
request to mount the volume root or a path outside that volume. The Docker daemon
and client must support this mount option ([Docker 26 or newer](https://docs.docker.com/engine/release-notes/26.0/);
integration-tested with Engine/CLI 29.5.3). The runtime image copies a digest-pinned Docker CLI
instead of installing bookworm's older `docker.io` package. An
unsupported or missing mount fails execution rather than exposing the full volume.
Standalone bind mounts use the same directory boundary.

Checkout and protocol directories are pinned as separate mounts so an execution
cannot rename them and substitute a new directory at their old paths. Existing
Git metadata is mounted read-only. Protocol reads and writes reject symlinks and
special files left by an agent. Launches drop capabilities except `DAC_OVERRIDE`,
which allows a root image to edit a host-user-owned bind checkout, and enable
`no-new-privileges`. That capability does not bypass read-only mounts. There are
no privileged-mode, host-namespace, or arbitrary mount options in the task protocol.
Configured tool and technology inputs remain read-only. Worker environment
variables are not automatically passed into the execution container.

The coordinator selects agent or check execution explicitly. Plugin validators
and required checks receive neither Codex credentials nor role environment variables
(which can include model/MCP credentials). They retain their checkout and read-only
tool mounts. A repository or agent result cannot request agent credential access
for a validator. Agent-issued shell commands still share their agent's access.

## Coordinator Git operations

Clone, checkout, merge, status, commit, and publication use one supervised Git
wrapper. It clears the inherited environment and global/system configuration,
disables hooks, credential helpers, signing, fsmonitor, and automatic maintenance,
and permits only GitHub HTTPS for remote operations. Authentication is a
GitHub-scoped HTTP header supplied to that subprocess, never stored in the
checkout or an agent-visible askpass script. Fetch/push URLs come from the job or
publication's repository identity, not a mutable `origin` setting. Redirects are
disabled. The explicit local forensic-clone operation permits file transport and
copies independent objects; it does not share objects with the aggregate checkout.

Before using an existing checkout, the worker rejects metadata links, alternate
object stores, worktree indirection, configuration includes, and settings outside
its small allowlist of ordinary clone/branch/author metadata. This applies to
legacy paused checkouts too. A resumed lifecycle can replace a lost or rejected
checkout only from its recorded immutable checkpoint, preserving an unexpected
old directory for inspection. Missing provenance or unavailable exact objects
produce a restoration/reapproval request before execution. See
[checkpoint authority](plugin-interface.md#checkpoint-authority).

Artifact reads and writes use a shared checked copy path. Declarations must name
content below the repository; `.` and `.git` are not artifact roots. Nested Git
metadata, symlinks, special files, and redirected destination directories are
rejected before replacement. Plugin build contexts may still use `.`.

## Automation authentication and remaining boundaries

Setup owns a dedicated automation home per instance. It does not read or copy
ambient personal authentication. Agent containers mount only its `auth.json`;
configuration/history/session files are private to each container. API-key jobs
use read-only mounts and shared file locks. ChatGPT jobs use writable mounts and
exclusive locks for the full command, serializing automatic refresh writes.
The coordinator's shell holds the lock while the command runs; container removal
releases it. The bootstrap's private-file mask is restored before agent execution
so output artifacts remain readable by the coordinator.

Account/status inspection takes a shared lock and skips a busy credential rather
than reading during a rewrite. Login takes exclusive ownership. Drain the worker
before reconnecting or replacing the credential file. See
[installation and migration](installation.md#codex) for configuration, pinned CLI
compatibility and the subscription concurrency tradeoff. Authenticated role
configuration cannot override `CODEX_*` variables (except CA configuration) or
`OPENAI_API_KEY`; the coordinator owns that selection.

Agents can read their assigned automation credentials and explicitly forwarded
role secrets. Subscription agents can modify their credential file. Containers
retain outbound network access. These changes do not restrict network
destinations or protect deliberately forwarded credentials from the code
receiving them. A credential broker and network restrictions are separate work.

`scripts/test-database` includes real-container tests of all built-in roles and
required checks using synthetic secrets, temporary workspaces, bind mounts, and a
disposable named volume. They cover editing/checking, sibling exclusion, denied
runtime-socket access, read-only metadata, directory substitution, result/log
symlinks, and missing-image failure without host execution. A synthetic credential
file and role token verify that agents receive intended credentials while plugin
validators and required checks receive neither. Tests also verify that surrounding
home files and per-job history are not shared, API-key jobs overlap without write
access, subscription jobs serialize and preserve credential updates, and cancelling
a lock holder releases a waiting job. These are deterministic local credential
rotation tests, not live provider OAuth refresh tests. The existing container
cancellation test separately verifies cleanup after failure, abort, and cancellation.

Git regressions exercise ordinary commits/merges and forensic-clone survival after
source deletion, malicious hooks/configuration, inherited environment overrides,
metadata links/indirection, and ephemeral host-scoped authentication. Artifact
regressions check that a rejected output leaves earlier accepted files intact.
These tests use synthetic credentials and local repositories, with no GitHub writes.

The image-level check `scripts/test-execution-image IMAGE` runs the packaged
Docker client against a disposable volume. It verifies subdirectory mounts,
read-only Git metadata, output persistence, and absence of the coordinator socket
and other jobs in the child container. This catches packaging incompatibilities
that host-client integration tests cannot detect. It requires a local Docker
daemon, the built image and cached `busybox:latest`; it uses synthetic data only.
