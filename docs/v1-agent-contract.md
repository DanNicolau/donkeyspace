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
    command: ["donkeyspace-agent-triage", "--input", ".donkeyspace/run-input.json"]
  developer:
    command: ["codex", "exec", "--json", "--input", ".donkeyspace/run-input.json"]
  reviewer:
    command: ["donkeyspace-agent-review", "--input", ".donkeyspace/run-input.json"]
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

- Triage jobs receive issue title, body, labels, and recent comments.
- Developer jobs receive issue context, policy checks, branch naming hints, and repository checkout path.
- Reviewer jobs receive issue context, PR metadata, diff summary, changed files, and check status.

## Run Result

Every agent must write `.donkeyspace/run-result.json`.

The validation schema lives at `schemas/run-result.schema.json`.

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
