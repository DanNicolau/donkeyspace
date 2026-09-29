# Donkeyspace Policy

Donkeyspace reads the base policy from `.donkeyspace/policy.yml`. CLI-managed
deployments generate a private effective policy that combines the base policy,
local GitHub access, and active plugin selection.

Missing or invalid policy makes the API or worker fail at startup. The dashboard
does not currently display or edit the active policy.

## Product Facade

`facade` controls the product name, dashboard tagline, GitHub issue command, and
branch namespace
shown to users. Values are optional and resolve field-by-field from built-in
defaults, the active plugin, policy, and private instance overrides.

```yaml
facade:
  display_name: "Example Agent Platform"
  tagline: "Agentic repository workflow"
  command: "example-agent"
  branch_prefix: "example-agent"
```

The command omits its leading slash. Configure private deployment overrides
with `donkeyspace configure facade`; use `--reset` to return to plugin and
policy defaults. Facades do not rename stable runtime identifiers such as
`.donkeyspace`, environment variables, or hidden GitHub markers.

Only the resolved issue command is accepted. Changing it intentionally makes
comments using the previous command token ineligible to resume paused jobs.

## Workflow Labels

`workflow.state_labels` maps Donkeyspace workflow states to GitHub labels. Donkeyspace keeps one active workflow label on an issue when it applies state changes.

`workflow.allow_labels` is an opt-in gate for automation. When this list is non-empty, issue triage only queues if the issue has at least one configured allow label.

```yaml
workflow:
  allow_labels:
    - "ai"
```

`workflow.block_labels` is an override gate. If any configured block label is present, Donkeyspace records the webhook and issue state but does not queue new agent work.

```yaml
workflow:
  block_labels:
    - "ai:disabled"
    - "wontfix"
```

Block labels win over allow labels. For example, an issue with both `ai` and `ai:disabled` will not queue triage.

## Engagement Authorization

`workflow.engagement` controls which GitHub actors may start or resume AI work. Separate
rules cover initial issue events, clarification after `ai:needs-info`, blocked
work, and `ai:needs-human` resumption, including plugin checkpoints.

Engagement is repository scoped. A missing repository or an empty `allow` list
denies all engagement. The TUI and `configure github-access` commands maintain
separate local lists: job starters populate `default.allow`, while human
approvers populate `needs_human_resume.allow`. They do not modify the checked-in
policy or GitHub membership.

```yaml
workflow:
  engagement:
    default:
      allow: []
    repositories:
      "example/hardware":
        default:
          required_labels: ["ai"]
          allow:
            - type: user
              login: maintainer
            - type: team_member
              organization: example
              team_slug: hardware
        needs_human_resume:
          required_labels: ["ai"]
          allow:
            - type: collaborator_permission
              minimum: maintain
```

An omitted repository gate inherits that repository's `default`. Required
labels are all required; identity selectors are alternatives. An explicit empty
`allow` list denies everyone.
Supported selectors are `token_owner`, `any_user`, `user`, `issue_author`,
`repository_owner`, `repository_organization_member`, `organization_member`,
`team_member`, `author_association`, `collaborator_permission`, `bot`, and
`github_app`. GitHub App selectors accept exactly one numeric `id` or `slug`.
`token_owner` is retained only for deprecated PAT deployments and never matches
an installation token.

API-backed selectors require adequate GitHub App or PAT permissions. Organization
and team selectors require `Members: read`; collaborator permission uses
`Metadata: read`. Missing actor metadata, insufficient scope, lookup errors, and
unknown permissions fail closed. Decisions are available from
`GET /api/engagement-decisions`.

For public repositories, prefer explicit maintainers/teams or collaborators
with at least `write`; `any_user` and `issue_author` allow
arbitrary public issue content to reach the agent. Private repositories may use
`read` collaborator access when repository access is the trust boundary.

## Agents

`agents` controls which roles can run and which command Donkeyspace invokes inside the prepared workspace.

```yaml
agents:
  triage:
    enabled: true
    command: ["donkeyspace-codex-triage", ".donkeyspace/run-input.json"]
  developer:
    enabled: true
    command: ["donkeyspace-codex-developer", ".donkeyspace/run-input.json"]
```

Disabled roles do not start new work for that role. Empty commands fail closed when a queued job reaches the worker.

## Checks

`checks.required_commands` runs after a developer agent modifies the checkout and before Donkeyspace pushes a branch. Commands run from the repository checkout. A failed command blocks the developer job and records the command result for the dashboard/API.

```yaml
checks:
  required_commands:
    - name: "diff whitespace"
      command: ["git", "diff", "--check"]
```

Commands must exist in the execution image selected by `DONKEYSPACE_AGENT_IMAGE`. Compose uses its locally built runtime image, which includes Git, Node, npm, and the Rust toolchain. Other project-specific tools need a custom execution image. Agents and required checks run in disposable containers; see [execution isolation](execution-isolation.md) for mount boundaries and the remaining audit work.

Policy version 2 rejects `checks.require_github_checks`; GitHub check enforcement is not implemented.

## Risk Routing

Agent results must report a risk value explicitly. Version 2 removes the unused
`risk.default` and `risk.agent_classification` declarations.

`risk.route_unknown_to_human` and `risk.route_high_to_human` route `ready` triage results to `needs_human` before developer work is queued.

```yaml
risk:
  route_unknown_to_human: true
  route_high_to_human: true
```

`risk.human_review_paths` routes implemented developer results to human review when changed files match a configured path. Donkeyspace still pushes the branch and opens the PR, then applies the human-review workflow state so the diff remains available.

```yaml
risk:
  human_review_paths:
    - ".github/**"
    - "migrations/**"
    - "**/security/**"
```

Supported path patterns are exact paths, prefix globs ending in `/**`, and nested directory globs such as `**/security/**`.

## Automation

Humans remain responsible for merging. Version 2 rejects the unused `automation`
settings for auto-merge, concurrency and automatic retries.

Failed jobs can be retried manually through `POST /api/runs/{id}/retry` or the
dashboard when `dashboard.allow_retry` is true. Only failed jobs are eligible;
results with `blocked` or `needs_human` outcomes must be resolved by a person
instead. For a paused lifecycle plugin, a newly created
`/<facade-command> approve` or `/<facade-command> revise` comment on the parent issue resumes the checkpoint only
after `needs_human_resume` authorizes the actor. A
retry creates a new job linked through `retry_of_job_id`.

## Dashboard

`dashboard` declares which policy-related actions the dashboard should expose.

```yaml
dashboard:
  # Optional externally reachable dashboard origin used in GitHub status links.
  public_url: https://agents.example.com
  allow_retry: true
```

`dashboard.allow_retry` gates the retry API. Version 2 removes `expose_policy` and
`allow_cancel`. The dashboard does not edit policy. When `dashboard.public_url` is omitted, lifecycle status comments
remain self-contained and do not publish a broken dashboard link.

## Version migration

Save new policies as `version: 2`. Version 1 remains readable through an explicit
migration boundary: retired no-op fields are removed with startup warnings and
serialization emits version 2. Version 2 rejects those fields and unknown fields
in the affected sections. Selecting a serial role plugin is an error in either
version. Set `lifecycle.plugin` for a full graph or keep the built-in role commands.
The worker rejects the retired `deterministic`, `fake` and `local` triage providers;
use the native agent or the configured OpenAI-compatible provider.
