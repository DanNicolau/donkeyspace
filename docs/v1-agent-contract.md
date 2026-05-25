# V1 Agent Contract

donkeyspace orchestrates external agent CLIs. Agents are expected to read a structured input file, do their work, write a structured result file, and leave normal logs behind for debugging.

## Runtime Invocation

The orchestrator runs one agent role per job inside an ephemeral workspace.

Default files:

- Input: `.donkeyspace/run-input.json`
- Result: `.donkeyspace/run-result.json`

The exact command is configured by policy. Example:

```yaml
agents:
  triage:
    command: ["donkeyspace-codex-triage", ".donkeyspace/run-input.json"]
  developer:
    command: ["donkeyspace-codex-developer", ".donkeyspace/run-input.json"]
  reviewer:
    command: ["donkeyspace-agent-review", "--input", ".donkeyspace/run-input.json"]
  repair:
    command: ["donkeyspace-codex-repair", ".donkeyspace/run-input.json"]
```

## Run Input

The orchestrator writes `.donkeyspace/run-input.json` before invoking an agent.

Required fields:

```json
{
  "run_id": "run_123",
  "role": "triage",
  "repository": {
    "provider": "github",
    "owner": "example",
    "name": "service",
    "default_branch": "main"
  },
  "issue": {
    "number": 42,
    "title": "Fix invoice export",
    "body": "The export fails for large accounts.",
    "labels": ["bug", "ai:ready"],
    "comments": []
  },
  "pull_request": null,
  "policy": {
    "path": ".donkeyspace/policy.yml",
    "snapshot_id": "policy_123"
  },
  "workspace": {
    "repo_path": "/workspace/repo",
    "result_path": ".donkeyspace/run-result.json"
  }
}
```

Role-specific notes:

- Triage jobs receive issue title, body, labels, recent comments, and bounded read-only repository context.
- Developer jobs receive issue context, policy checks, and a writable repository checkout path.
- Reviewer jobs receive issue context, PR metadata, diff summary, changed files, and check status.
- Repair jobs receive issue context, PR metadata, and merge-conflict details after the worker attempts to merge the base branch into the PR branch.

## Run Result

Every agent must write `.donkeyspace/run-result.json`.

The validation schema lives at `schemas/run-result.schema.json`.

Codex CLI triage uses `schemas/run-result.codex.schema.json` and Codex CLI developer runs use `schemas/run-result.codex-developer.schema.json` for model-facing structured output because OpenAI response-format schemas do not support every JSON Schema feature used by the orchestration schema. The reference wrappers disable Codex's inner bubblewrap sandbox and rely on the worker container boundary for local testing. The orchestrator still applies Rust validation after reading the result file.

Codex CLI reviewer runs use `schemas/run-result.codex-reviewer.schema.json`. Reviewer agents may inspect the PR checkout and diff context, but must not edit files, commit, push, apply labels, or open pull requests.

Required shape:

```json
{
  "outcome": "needs_human",
  "summary": "The issue references a failing export but does not specify the expected file format.",
  "confidence": "medium",
  "risk": "unknown",
  "questions": [
    "Which export format is failing: CSV, XLSX, or both?"
  ],
  "tests": [],
  "changed_files": [],
  "human_review_reason": "Missing acceptance criteria for the expected export output.",
  "blocked_reason": null
}
```

Allowed `outcome` values:

- `ready` - Triage believes the issue is ready for implementation.
- `needs_info` - Triage needs human clarification.
- `implemented` - Developer completed implementation and checks.
- `reviewed` - Reviewer completed review and found no actionable changes.
- `needs_changes` - Reviewer found fixable problems that should return to the developer agent.
- `needs_human` - The agent cannot safely continue without human judgment.
- `blocked` - The run cannot proceed because of a hard external blocker.
- `failed` - The run failed unexpectedly.

Allowed `confidence` values:

- `low`
- `medium`
- `high`

Allowed `risk` values:

- `low`
- `medium`
- `high`
- `unknown`

## Required Fields By Outcome

- `needs_info`: must include at least one `questions` entry.
- `needs_human`: must include `human_review_reason`.
- `blocked`: must include `blocked_reason`.
- `failed`: must include `blocked_reason`.
- `implemented`: must include `tests`; `changed_files` should be present when available.
- `needs_changes`: must include a summary specific enough for a developer agent or human to act on.

## Developer PR Behavior

Developer agents modify files in the checkout only. They must not commit, push, apply labels, or open pull requests directly. When a developer result is `implemented`, the worker inspects `git status`, commits actual checkout changes, pushes `donkeyspace/issue-{issue_number}-{job_id_short}`, opens a PR, and transitions the issue to `pr_open`.

Commit and PR titles use Conventional Commit formatting. The current heuristic chooses `docs:` for README or documentation-only changes, `fix:` for bug/failure language, `feat:` for add/create/feature language, and `chore:` otherwise.

## Reviewer PR Behavior

Reviewer agents review donkeyspace-managed PRs after `pull_request` webhooks. A reviewer result of `needs_changes` posts a PR comment and keeps the issue in `pr_open`; it does not automatically requeue implementation in v1. A future role may handle approved PR-comment fixes or reviewer-feedback repair under explicit human approval or policy rules.

A reviewer result of `reviewed` also posts a PR comment and leaves the issue in `pr_open`; it is an audit signal, not automatic approval or merge authority in v1.

## Repair PR Behavior

Repair agents resolve merge conflicts on donkeyspace-managed PRs after default-branch `push` webhooks. The worker checks out the PR branch, attempts to merge the current base branch, and invokes the repair agent only when Git reports conflicted files. Repair agents may edit the checkout but must not commit, push, open pull requests, or apply labels directly; donkeyspace commits and pushes successful repairs to the existing PR branch.

## Failure Handling

The orchestrator should fail closed.

- Missing result file: mark the job `failed`.
- Invalid JSON: mark the job `failed`.
- Unknown outcome: mark the job `failed`.
- Contradictory result, such as `outcome: ready` with `confidence: low`: route to `needs_human`.
- `risk: high` or `risk: unknown`: route to human review unless policy explicitly allows continued automation.

## Human Help

Agents request human help by returning either `needs_info`, `needs_human`, or `blocked`.

The orchestrator turns these outcomes into GitHub comments and labels. Agents should not directly apply workflow labels unless a future runtime mode explicitly allows it.
