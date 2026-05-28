# Donkeyspace Policy

Donkeyspace reads repository policy from `.donkeyspace/policy.yml`. The API and worker load this file at startup, so restart the services after changing policy.

Policy is currently managed as a repository file. A future dashboard workflow should expose these settings for review and editing, then write or propose the corresponding policy change.

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

`automation.auto_merge` is false by default and v1 keeps humans responsible for merging. `automation.max_concurrent_jobs` and `automation.retry_failed_jobs` are policy declarations for scheduler/retry behavior; full enforcement is future work.

## Dashboard

`dashboard` declares which policy-related actions the dashboard should expose.

```yaml
dashboard:
  expose_policy: true
  allow_retry: true
  allow_cancel: true
```

The current dashboard does not edit policy. Future dashboard support should show the active policy, explain which rules affected each run, and provide a controlled way to propose policy changes.
