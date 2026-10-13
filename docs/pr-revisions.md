# Revising an open pull request

An authorized maintainer can ask Donkeyspace to revise an existing managed PR.
Generated PR descriptions include a Requesting changes section with the configured
command prefix, available task targets, and a link to the parent issue. Existing
PRs receive this section when a revision starts or resumes; unrelated description
text is preserved.

Post a new top-level conversation comment **on the PR**:

```text
/donkeyspace revise
Handle the empty input case without changing the public interface.
```

This runs the built-in developer and configured required checks, then updates
the same PR branch. The worker converts the PR to draft before editing and marks
it ready for review only after successful validation and publication, using
[GitHub’s draft/ready mutations](https://docs.github.com/en/graphql/reference/pulls#convertpullrequesttodraft). Paused or
failed revisions remain draft; failure to enter draft prevents execution. A
readiness update failure is reported separately from a successful code push, so
the published commit remains visible. It does not approve or merge the PR or resolve review
threads automatically. Use your configured facade command instead of
`/donkeyspace` when applicable.

For a lifecycle plugin, explicitly select a manifest task. Workflow tasks use
`TASK`; work-item tasks use `TASK/WORK-ITEM`:

```text
/donkeyspace revise implement/queue
Apply the review's reset correction and update the affected tests.
```

A behavior change that also changes the approved specification should start with
the planning task. An implementation-only target may pause if its write scope or
role contract prevents it from updating that specification. Continue such a pause
with the permitted upstream revision command on the parent issue, rather than
approving unchanged work or creating another PR revision.

A planning/specification revision can target a workflow task, for example
`/donkeyspace revise plan`. Tasks with required approval request a new approval
on the **parent issue** before their dependents execute. Follow the approval
instructions there. The existing subissues are reused, and affected completed
subissues are reopened for the revision.

## Selecting feedback

By default the task receives PR reviews, inline comments with their file, line,
commit and reply context, the PR conversation, and the original issue. Narrow
the review evidence with numeric GitHub IDs:

```text
/donkeyspace revise implement/queue review=123456 thread=789012
Address this review thread; preserve the existing interface.
```

`review` identifies a submitted review. `thread` identifies a review comment
(the root comment or a reply); the task receives the entire selected thread,
including subsequent replies. Both selectors must refer to this PR. Feedback
on lines after the command is mandatory, including when selectors are given.
The selected evidence and requesting actor are recorded with the revision.

Only explicit, newly created revision commands authorize work. An ordinary
question, edited command, inline comment, or GitHub “Request changes” review
alone does not start execution. Generated comments and bot commands are
excluded. The repository's `needs_human_resume` engagement rule authorizes the
request, using the parent issue's labels; block labels still apply. PRs must be
managed, open, and belong to the current workflow generation, with a branch in
the same repository.

## Continuation and publication

Each revision has a new run ID, linked to the previous published run, original
parent issue, PR, command comment, and workflow generation. Plugin scheduling
state is retained in PostgreSQL after successful execution; revisions do not
require the original temporary checkout. The selected task and its dependent
results are invalidated while independent completed work is retained. Edits
already present on the PR also invalidate tasks whose declared read/write
roots or repository resources cover those files. This can expand the revision to planning and require
fresh approval. Superseded task summaries remain in the historical evidence.

The worker checks out the current remote PR head, preserving human commits.
Agents receive `pr_revision` and `pull_request` in their input. Role plugins
should read the requested feedback in addition to the original issue and
preserve unrelated work. Task access roots, validators, and required checks
continue to apply. Intermediate plugin checkpoints use the revision's own
publication branch; the existing PR is updated only after successful validation.

Publication verifies that the PR remains open and its head still matches the
revision input, checks ancestry, and uses a normal push. A concurrent divergent
push is rejected by Git. Donkeyspace never force-pushes a revision. If the head
moves, inspect the attempt and submit a **new PR revision command** to apply
feedback to the latest head. Approving a terminal publication conflict on the
parent issue does not start a new independent lifecycle.

The PR receives a result comment with the run, summary, blockers, checks, and
published commit. The dashboard's existing run history and lifecycle timeline
show revision progress and approval decisions. No separate dashboard control
is required.

## Failures and upgrades

Only one revision/lifecycle execution may be active or paused for a workflow.
Complete a pending approval on the parent issue before requesting another PR
revision. Failed checks leave the PR unchanged and publish the available
attempt evidence. After addressing the cause, use a new PR revision command;
generic dashboard retry is disabled for revision jobs because it would bypass
PR-head refresh and revision admission.

Worker lease loss retains the existing execution fencing and cleanup behavior:
there is no automatic agent replay. After cleanup, a new revision command can
continue from the remote head. Publication intent is recorded before pushing;
if a push succeeded just before a worker stopped, the next authorized command
reconciles that commit against GitHub and uses its published continuation.
Duplicate webhook/poll deliveries of the same command never enqueue it twice.
Closing or merging the PR cancels active revision work when observed, and
publication independently checks the live PR state.

Migration `0010_pr_revisions.sql` adds lifecycle snapshots and revision records;
normal API/worker startup applies it. Rebuild and recreate the API and worker
when deploying this change. Newly completed plugin workflows retain the
snapshot needed for later revisions. Completed database checkpoints from the
schema-9 baseline are also supported. A plugin PR completed by an older version
may have no snapshot; its command is rejected with an explanation rather than
inventing accepted task state. Built-in PR revisions do not need a plugin
snapshot.
