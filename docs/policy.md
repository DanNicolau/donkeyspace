# Donkeyspace Policy

Donkeyspace reads repository policy from `.donkeyspace/policy.yml`. The API and worker load this file at startup, so restart the services after changing policy.

Policy is managed as a repository file. Missing or invalid policy makes the API
or worker fail at startup. The dashboard does not currently display or edit the
active policy.

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

## AI Engagement Authorization

`workflow.engagement` controls which GitHub actors may start or resume agent work. The four independently configurable paths are `initial`, `clarification`, `blocked_resume`, and `human_authorization`. Missing actor metadata always fails closed, and denied trigger attempts are retained as webhook deliveries and state-transition audit records.

The secure default for every path is the owner of `DONKEYSPACE_GITHUB_TOKEN`. At startup Donkeyspace resolves the token through GitHub's `/user` endpoint; startup fails if the token is absent or does not represent a user. This keeps a personal-token deployment limited to the logged-in GitHub user without copying their login into policy.

```yaml
workflow:
  engagement:
    initial: { token_owner: true }
    clarification: { token_owner: true }
    blocked_resume: { token_owner: true }
    human_authorization: { token_owner: true }
```

Each rule is an OR of `token_owner`, `issue_author`, explicit human `users`, GitHub `author_associations`, `trusted_bots`, and `any`. An empty rule permits nobody and is an explicit kill switch. Usernames and associations are matched case-insensitively.

```yaml
workflow:
  engagement:
    initial:
      issue_author: true
      author_associations: [OWNER, MEMBER, COLLABORATOR]
    clarification:
      issue_author: true
      users: [octocat]
    blocked_resume: { users: [octocat] }
    human_authorization: { author_associations: [OWNER] }
```

For public repositories, keep `token_owner` or a small explicit allowlist for resume and human-authorization paths. For private repositories, association-based initial engagement may be appropriate. GitHub App installation tokens have no authenticated human user, so use explicit `users`, `trusted_bots`, or associations instead of `token_owner`.

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

Commands must exist in the worker image. The default image includes Git, Node, npm, and the Rust toolchain so Donkeyspace can dogfood its own Rust workspace and dashboard build. Other project-specific tools need a custom worker image or wrapper command.

`checks.require_github_checks` is reserved for required GitHub status/check enforcement. The field is parsed but GitHub check enforcement is not implemented yet, so keep it `false` for dogfooding.

## Risk Routing

`risk.default` and `risk.agent_classification` are parsed for forward
compatibility but do not currently alter routing. Agent results must report a
risk value explicitly.

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

`automation.auto_merge` is false by default and humans remain responsible for merging. `automation.max_concurrent_jobs` and `automation.retry_failed_jobs` are reserved declarations; neither is enforced yet.

Failed jobs can be retried manually through `POST /api/runs/{id}/retry` or the
dashboard when `dashboard.allow_retry` is true. Only failed jobs are eligible;
results with `blocked` or `needs_human` outcomes must be resolved by a person
instead. For a paused lifecycle plugin, a human comment on the parent issue
resumes the saved coordinator checkpoint. A retry creates a new job linked
through `retry_of_job_id`.

## Dashboard

`dashboard` declares which policy-related actions the dashboard should expose.

```yaml
dashboard:
  expose_policy: true
  allow_retry: true
  allow_cancel: true
```

`dashboard.allow_retry` gates the retry API. `dashboard.expose_policy` and
`dashboard.allow_cancel` are parsed but not implemented. The dashboard does not
edit policy.
