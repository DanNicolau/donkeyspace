# V1 GitHub Workflow

GitHub is the visible workflow surface for v1. donkeyspace watches GitHub events, applies labels, posts comments, opens PRs, and records audit state internally.

## Labels

Workflow labels:

- `ai:needs-info`
- `ai:ready`
- `ai:in-progress`
- `ai:pr-open`
- `ai:needs-human`
- `ai:blocked`

Only one workflow label should be active on an issue at a time.

## Event Triggers

V1 should subscribe to these GitHub events:

- `issues.opened`: schedule triage unless blocked by policy.
- `issues.edited`: re-run triage if the issue is not already in progress or linked to an open PR.
- `issues.reopened`: schedule triage unless blocked by policy.
- `issues.labeled`: react to `ai:*` workflow labels and policy allow/block labels.
- `issues.unlabeled`: re-evaluate workflow state when an `ai:*` label is removed.
- `issue_comment.created`: re-run triage when a human comments on an issue in `ai:needs-info`.
- `pull_request.opened`: link PR to issue when created by donkeyspace.
- `pull_request.synchronize`: schedule reviewer agent for donkeyspace-managed PRs.
- `pull_request.reopened`: schedule reviewer agent for donkeyspace-managed PRs.
- `pull_request.ready_for_review`: schedule reviewer agent for donkeyspace-managed PRs.
- `check_run.completed` or `check_suite.completed`: update required-check status for donkeyspace-managed PRs.

Webhook delivery IDs should be recorded for idempotency.

## State Transitions

Expected issue transitions:

- No AI label -> triage job queued.
- Triage returns `needs_info` -> apply `ai:needs-info` and post questions.
- Triage returns `ready` -> apply `ai:ready`.
- Developer job starts -> apply `ai:in-progress`.
- Developer returns `implemented` and opens PR -> apply `ai:pr-open`.
- Reviewer returns `reviewed` -> keep `ai:pr-open` and post the reviewer summary.
- Reviewer returns `needs_changes` -> keep `ai:pr-open` and request updates.
- Reviewer returns `needs_human` -> apply `ai:needs-human`.
- Any hard blocker -> apply `ai:blocked`.

When applying a new workflow label, donkeyspace should remove any existing `ai:*` workflow label from the same issue.

The current implementation records intended GitHub label and comment writes as pending outbound actions before executing them. When `DONKEYSPACE_GITHUB_TOKEN` is configured, the worker applies those actions through GitHub and marks them completed or failed.

## Human Control

V1 uses labels and normal comments rather than slash commands.

- Pause: apply `ai:needs-human`.
- Resume after clarification: comment with the requested information. If the issue has `ai:needs-info`, donkeyspace re-runs triage.
- Retry implementation: remove `ai:blocked` or `ai:needs-human`, then apply `ai:ready`.
- Cancel agent automation for an issue: apply a policy-defined block label such as `ai:disabled` if configured, or leave the issue in `ai:needs-human`.

Slash commands are deferred to a later version.

## Conflict Handling

If multiple workflow labels are present, donkeyspace should:

1. Stop scheduling new agent work for that issue.
2. Post a short comment explaining the conflicting labels.
3. Apply `ai:needs-human` if policy allows automatic cleanup.
4. Record an audit event.

If a webhook event arrives for an issue or PR with an active job lease, donkeyspace should update the state snapshot but avoid starting duplicate work.

## Comment Formats

### Triage Question Comment

```markdown
donkeyspace triage needs clarification before this issue can move to implementation.

Questions:
- ...

Current state: `ai:needs-info`
```

### Ready Comment

```markdown
donkeyspace marked this issue ready for agent implementation.

Reason:
...

Current state: `ai:ready`
```

### PR Description

```markdown
## Summary
...

## Issue
Closes #123

## Tests
- ...

## Risk
Risk: low|medium|high|unknown
Confidence: low|medium|high

## donkeyspace
Run: run_123
Policy: policy_123
```

### Reviewer Agent Comment

```markdown
donkeyspace reviewer result: needs human review

Summary:
...

Risk: unknown
Confidence: medium

Reason:
...
```
