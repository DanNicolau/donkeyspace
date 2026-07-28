# Default GitHub Workflow

GitHub is the visible workflow surface. donkeyspace watches GitHub events, applies labels, posts comments, opens PRs, and records audit state internally. This document describes the built-in lifecycle; lifecycle plugins may replace its role scheduling while retaining the same labels and human interaction surface.

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

The current implementation handles these GitHub events:

- `issues.opened`: schedule triage unless blocked by policy.
- `issues.edited`: re-run triage for an open, policy-eligible issue.
- `issues.reopened`: schedule triage unless blocked by policy.
- `issues.labeled`: schedule triage when an allow label is added to an
  unstarted, `needs_info`, `needs_human`, or `blocked` issue.
- `issue_comment.created` and `issue_comment.edited`: re-run triage when a
  human comments on an issue in `needs_info` or `blocked`; for a paused plugin
  lifecycle in `needs_human`, requeue the same lifecycle coordinator and resume
  its checkpoint.
- `pull_request.opened`: link a managed PR to its issue and schedule review.
- `pull_request.synchronize`: schedule reviewer agent for donkeyspace-managed PRs.
- `pull_request.reopened`: schedule reviewer agent for donkeyspace-managed PRs.
- `pull_request.ready_for_review`: schedule reviewer agent for donkeyspace-managed PRs.
- `push` to the default branch: schedule repair checks for open donkeyspace-managed PRs targeting that branch.

Draft and unmanaged PRs do not queue review. Duplicate review and repair jobs
for the same recorded PR head/base pair are suppressed. Other webhook event
types are recorded for idempotency and otherwise ignored; check-run and
check-suite handling is not implemented.

## State Transitions

Expected issue transitions:

- Policy-eligible open issue -> triage job queued.
- Triage returns `needs_info` -> apply `ai:needs-info` and post questions.
- Triage returns `ready` -> apply `ai:ready`.
- Developer job starts -> apply `ai:in-progress`.
- Developer returns `implemented` and opens PR -> apply `ai:pr-open`.
- Default branch push creates a merge conflict for a managed PR -> run repair, then push to the existing PR branch.
- Reviewer returns `reviewed` -> keep `ai:pr-open` and post the reviewer summary.
- Reviewer returns `needs_changes` -> keep `ai:pr-open` and request updates.
- Any role returns `needs_human` -> apply `ai:needs-human` and post the review
  reason, failed verification evidence, and instructions to comment with the
  decision or correction needed to requeue the workflow.
- Any hard blocker -> apply `ai:blocked`.

When applying a new workflow label, donkeyspace removes the other configured workflow labels from the same issue.

The worker ensures configured workflow, allow, and block labels exist for known
repositories when a GitHub token is available. It records intended label and
comment writes as pending outbound actions before executing them, then marks
each action completed or failed.

## Human Control

The default workflow uses labels and normal comments rather than slash commands.

- Pause new automation: apply a configured block label such as `ai:disabled`.
- Resume after clarification: comment with the requested information. If the issue has `ai:needs-info`, donkeyspace re-runs triage.
- Resume a plugin human handoff: comment with the requested decision or
  correction while the issue has `ai:needs-human`. Donkeyspace resumes the same
  coordinator and preserves completed graph work.
- Retry a failed job: use the dashboard or `POST /api/runs/{id}/retry`. Results
  ending in `blocked` or `needs_human` are not eligible for direct retry.
- Resume a blocked issue through GitHub: remove the block condition and add the
  configured allow label or provide a human comment, as appropriate.

There is no cancellation API. A block label prevents new jobs but does not stop
an already running command. Slash commands are not supported.

## Conflict Handling

If multiple configured workflow labels are present, donkeyspace records the
issue internally as `needs_human`, records a transition, and does not queue new
work from that webhook. It does not currently clean up the labels or post a
conflict comment.

Database job leases prevent two workers from claiming the same queued job.
Webhook delivery IDs and PR head/base checks suppress duplicate scheduling.

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
Closes #123

## Summary
...

## Changed Files
- `path/to/file`

## Tests
- `command`: passed

Generated by donkeyspace developer job `job-id`.
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

Run: `job-id`
```
