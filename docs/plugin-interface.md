# Donkeyspace Plugin Interface

Donkeyspace plugins are manifests plus executable container images. Core owns
policy, scheduling, durable job state, filtered workspaces, Git/GitHub effects,
and result validation. Plugins own roles, prompts, images, and task graphs.

## Integration modes

Use the built-in software lifecycle, or select a full lifecycle plugin. Developer-only
serial plugins are retired. Finish queued/running serial jobs with the previous
release, then disconnect them or migrate to a lifecycle graph. New policy rejects
`agents.developer.plugin` with migration guidance.

### Lifecycle replacement

A repository can select an opt-in lifecycle flow:

```yaml
lifecycle:
  plugin:
    manifest_path: /plugins/rtl/donkeyspace-plugin.yml
    flow: rtl_blocks
    max_handoffs_per_edge: 2
    environment:
      PROVIDER_TOKEN: WORKER_PROVIDER_TOKEN
```

The selected manifest flow must declare `replaces_default_lifecycle: true`.
Its start task's role is queued directly from an eligible issue; built-in
triage, developer, reviewer, and repair webhook scheduling is bypassed. Repositories without a plugin use the built-in software lifecycle.

The environment map is `container variable: worker variable`. A value is
injected only when the selected role allowlists that variable. Secret values
are not written into run input.

## Issue conversation input

Lifecycle tasks receive the issue conversation in
`.donkeyspace/run-input.json` at `issue.comments`. This is an array of comment
objects with `id`, `body`, `user.login`, `user.type`, `author_association`,
`created_at`, `updated_at`, and `html_url` (unavailable metadata is null).
The numeric GitHub comment count is retained separately as `issue.comment_count`.

For connected GitHub runs, the worker fetches every page of comments once per
plugin invocation, including resumed workflows, and merges the triggering
comment by ID without overwriting a newer edit. Every task in that
invocation receives the same snapshot. A fetch failure stops execution instead
of silently omitting clarification. Offline runs retain supplied comment arrays
and the triggering comment. Reading conversation context does not authorize
new work or change generated-comment and duplicate-trigger suppression.

## Roles and tasks

Roles are agent identities and runtime definitions. Tasks are graph nodes
assigned to roles. This allows one role to perform several phases without
misrepresenting those phases as new roles.

```yaml
api_version: 1
id: example.rtl
facade:
  display_name: Example Agent Platform
  tagline: Agentic hardware design workflow
  command: example-agent
  branch_prefix: example-agent
runtime:
  default_image: example-rtl:dev

installation:
  build:
    context: .
    dockerfile: Dockerfile
  environment:
    EXAMPLE_MODE:
      description: Select the plugin execution mode.
      default: fake
    PROVIDER_TOKEN:
      description: Token used by the external provider.
      required: true
      secret: true

roles:
  architect:
    display_name: Design Architect
    command: [/plugin/bin/run-agent, architect]
  rtl:
    command: [/plugin/bin/run-agent, rtl]
  dv:
    command: [/plugin/bin/run-agent, dv]
  syn:
    command: [/plugin/bin/run-agent, syn]

flows:
  rtl_blocks:
    start: architect
    pull_request_title: "[RTL][DV][SYN] Implement and verify {issue_title} (#{issue_number})"
    replaces_default_lifecycle: true
    work_items_path: docs/design/blocks/index.json
    project_github_issues: true
    max_handoffs_per_edge: 2
    max_parallel_tasks: 4
    tasks:
      architect:
        role: architect
        display_name: Block specification
        publication_tag: SPEC
        approval_subject: proposed block specifications
        write: [docs/design]
        approval: required
      rtl:
        role: rtl
        publication_tag: RTL
        scope: work_item
        depends_on_work_items: true
        read: [docs/design, rtl]
        write: ["rtl/{work_item}.sv"]
      dv_prepare:
        role: dv
        scope: work_item
        read: [docs/design]
        write: ["dv/{work_item}"]
      dv_verify:
        role: dv
        display_name: Design verification
        publication_tag: DV
        scope: work_item
        dependencies: [rtl, dv_prepare]
        read: [docs/design, rtl, "dv/{work_item}"]
        write: ["dv/{work_item}"]
        allowed_handoffs: [rtl]
        handoff_descriptions:
          rtl: Verification found an RTL-correctable defect.
      synthesis:
        role: syn
        publication_tag: SYN
        scope: work_item
        dependencies: [rtl]
        read: [docs/design, rtl]
        write: ["synth/{work_item}"]
        allowed_handoffs: [rtl]
```

The optional `facade` supplies user-facing defaults. Core validates and applies
these values across the dashboard, GitHub prose and commands, and generated Git
identity. Policy and private instance configuration may override individual
fields without changing the plugin.

Optional role and task `display_name` values, task `approval_subject` values,
handoff descriptions keyed by an allowed target, and artifact or diagnostic
display names customize lifecycle timelines without embedding plugin-specific
terminology in core. Donkeyspace snapshots resolved wording into each event so
later manifest edits do not rewrite history.

Task `publication_tag` values customize checkpoint and forensic-attempt commit
subjects, rendered in brackets such as `[RTL]` or `[SYN]`. A flow may also set
`pull_request_title`; Donkeyspace expands `{issue_title}` and `{issue_number}`
and uses the result for both the final aggregate commit (when one is needed)
and pull-request title. Plugins that omit these fields retain Donkeyspace's
generic Conventional Commit titles.

`max_parallel_tasks` bounds the number of simultaneously running ready tasks;
it defaults to four. `scope: workflow` is the default. A lifecycle start task must have workflow
scope. After it completes, donkeyspace reads the planner-created registry and
expands every `scope: work_item` task. Work-item write roots must contain the
`{work_item}` placeholder so parallel attempts cannot replace one another's
files.

## Resources

Plugins can supply ordinary files or recursively snapshotted directories to a
role without embedding their meaning in Donkeyspace:

```yaml
resources:
  project-standards:
    source: plugin
    path: resources/project-standards.md
  reference-library:
    source: repository
    path: .project/references

roles:
  developer:
    command: [/plugin/run, developer]
    resources:
      - id: project-standards
        required: true

flows:
  implementation:
    start: develop
    tasks:
      develop:
        role: developer
        resources:
          - id: reference-library
            required: false
```

`source: plugin` paths are relative to the manifest directory;
`source: repository` paths are relative to the repository checkout. A path may
name one regular file or one directory. Directories include every regular file
beneath them recursively, so a newly added file is visible on the next task
attempt without a manifest change. Empty required directories are valid.

Role and task assignments are unioned. If either assignment marks the same ID
required, it is required. `required` controls missing-source failure only; an
available optional resource is still supplied. Missing optional resources are
recorded as unavailable.

Each attempt materializes an independent snapshot at
`.donkeyspace/resources/<id>/`. A file snapshot contains the source basename;
a directory snapshot preserves its relative tree. Run input records the source,
declared source path, materialized root, availability, sorted relative
inventory, and a SHA-256 tree digest. The digest covers both sorted paths and
contents and is verified after every publishable execution. Resource mutation
therefore prevents publication.

Resource IDs and paths must be relative and traversal-safe. Symlinks and
special files are rejected, as are snapshots over 1,024 files or 32 MiB. These
rules apply to both plugin- and repository-sourced material.

## Typed parameters

A manifest can expose deployment-selected values while retaining defaults:

```yaml
parameters:
  source_root:
    type: path
    default: src
  source_extension:
    type: enum
    values: [rs, txt]
    default: rs
  project_name:
    type: string
    default: example
  retry_count:
    type: integer
    default: 2
  strict:
    type: boolean
    default: true

flows:
  implementation:
    start: develop
    tasks:
      develop:
        role: developer
        read: ["{source_root}"]
        write: ["{source_root}/{work_item}.{source_extension}"]
```

Policy selects values for the flow:

```yaml
lifecycle:
  plugin:
    manifest_path: /plugins/example/donkeyspace-plugin.yml
    flow: implementation
    parameters:
      source_root: lib
      source_extension: txt
```

All resolved values are included under `parameters` in task input. Only `path`
and filesystem-safe `enum` parameters may appear in resource paths, work-item
registry paths, task read/write roots, and artifact paths. Donkeyspace rejects
missing or unknown parameters, wrong types, invalid enum values, unknown
placeholders, absolute paths, and traversal. Parameters are never expanded in
commands, image names, or environment-variable names.

## Artifact contracts and validators

Tasks can declare exact output paths and commands that mechanically validate a
publishable result:

```yaml
tasks:
  develop:
    role: developer
    write: ["{source_root}"]
    artifacts:
      - path: "{source_root}/{work_item}.{source_extension}"
        type: file
        required: true
    validators:
      - name: source validation
        command: [/plugin/checks/validate-source]
```

Artifact types are behavioral and limited to `file` and `directory`; paths are
exact and must remain within the task's write roots. For an `implemented`
result, Donkeyspace validates reported changed paths, verifies the resource
snapshot, validates artifacts, and runs validators before copying any changes
back. Validators run in the task's image with the same workspace and resources,
but without its Codex home or role environment variables. Their exit codes and
summaries are appended to the standard test results. A missing or wrong-type
artifact or modified resource prevents retention. A failed validator prevents
copying the task's write roots. Validators use their supplied files and read-only
tools; do not depend on model/MCP credentials or agent environment defaults in a
validator command.
Artifact and validator checks are skipped for non-publishable outcomes such as
`needs_changes`; result, changed-path, and resource checks still apply.

Tasks can opt exact files or directories into `preserve_on_success`, using the
same declaration shape as `artifacts`. After exit zero and a valid result, these
paths are copied and checkpointed even for `needs_changes`, `needs_human`,
`needs_info`, or a semantic `failed` result. This retains evidence without
completing the task, satisfying its validators, or granting approval. For example:

```yaml
preserve_on_success:
  - path: "build/{work_item}/reports"
    type: directory
```

Only declared paths inside the effective write roots are retained on those
outcomes. Undeclared output remains in the disposable attempt. An absent optional
path leaves earlier evidence intact; `required: true` instead fails if absent.
Symlinks and special files are rejected before replacing existing output.
Nonzero exit or malformed/invalid results retain nothing through this contract.
The coordinator checkpoints retained output before processing its semantic
pause or handoff; failed publication remains an explicit publication failure.
Explicitly retained paths are versioned even if a repository ignore rule would
otherwise exclude them. Other ignored output remains excluded from the commit.
Use this opt-in for reviewable reports or proposals, not raw build trees.
This contract does not by itself restore a deleted aggregate checkout or certify
that a proposal was accepted; those require checkpoint/publication provenance.

An agent's `needs_info` result pauses the lifecycle coordinator and commits its
questions and task context to the database. An authorized clarification reply
resumes that coordinator and its retained checkout; it does not create a fresh
planning run or accept a completed proposal. A required approval is still requested
when the clarified task subsequently completes. Declare draft paths under
`preserve_on_success` if the next attempt needs their exact bytes.

Parallel clarification requests are collected before pausing, and completed
independent siblings remain valid. If a wave also needs explicit approval, it
stays in `needs_human`: each clarification target is listed alongside the approval
targets, and the normal approve/revise commands apply. An ordinary reply cannot
bypass that stronger gate. Revision feedback supplies the requested answers.

Tasks may also declare optional forensic `diagnostics` using the same exact
file-or-directory shape. Diagnostic paths must be inside a declared read or
write root. Non-empty diagnostics from successful tasks are published as a
bounded, text-only `diagnostic` snapshot; unsuccessful tasks use an `attempt`
snapshot. Neither enters the aggregate checkout or final pull request branch.

## Work-item registry

The planner writes the JSON file configured by `work_items_path`:

```json
{
  "work_items": [
    {
      "id": "fifo",
      "spec": "docs/design/blocks/fifo.md",
      "depends_on": ["storage"],
      "metadata": {"module": "fifo"}
    }
  ]
}
```

IDs must be unique, filesystem-safe, and acyclic. Every dependency must name
another work item. `depends_on_work_items: true` makes a task wait for the same
task on each listed dependency.

The registry is the persistent repository catalog, not the current execution
set. A lifecycle-replacing planner must also return `work_items` in its task
result with only the catalog IDs selected for the current parent issue.
Unselected catalog dependencies are treated as already available; they are not
scheduled or projected again. Planner revisions reuse projected issues for IDs
that remain selected.

Donkeyspace creates a persisted child job for every expanded task. Jobs remain
`waiting` until their dependencies complete. All ready jobs in a task wave run
concurrently. The lifecycle's initial role job acts as the coordinator and the
aggregate checkout is published only after the graph completes.

## Feedback and human routing

Every task accepts `approval: none|required` and defaults to `none`. With
`required`, an `implemented` result is preserved but does not satisfy graph
dependencies until an authorized human approves it. The same setting works on
the lifecycle start task and work-item tasks. Agents may still return
`needs_human` dynamically regardless of this setting.

An `implemented` result completes a task. DV or synthesis can return
`needs_changes` with an allowed handoff:

```json
{
  "outcome": "needs_changes",
  "summary": "Read-valid timing differs from the block contract.",
  "confidence": "high",
  "risk": "low",
  "questions": [],
  "tests": [],
  "changed_files": ["dv/fifo/results.txt"],
  "human_review_reason": null,
  "blocked_reason": null,
  "handoff": {
    "target": "rtl",
    "reason": "Correct the externally observable read-valid timing."
  }
}
```

The target task's scope determines the restart key: workflow tasks discard the
source work-item ID, while work-item tasks retain it. The normalized target and
every downstream dependent are invalidated and rerun. Unknown targets or
checkpoint keys fail the lifecycle without mutating the graph.
Handoffs are bounded per work item/source/target edge. Exceeding the limit
produces `needs_human`. A role may return `needs_human` directly for ambiguous,
high-risk, or tool-limited decisions. Repository risk policy is applied again
before publication.

`needs_human` pauses the lifecycle coordinator instead of completing it. Before
pausing, donkeyspace writes a versioned handoff checkpoint to PostgreSQL and
retains the coordinator's durable workspace. It records completed graph nodes, child jobs, projected
GitHub issues, handoff counters, test evidence, and the exact task to resume.
The GitHub comment explains what decision is needed, the exact approval command,
and what work will be preserved. An explicit `/donkeyspace approve` or
`/donkeyspace revise` reply requeues the same coordinator UUID, reuses the
checkout and projected block issues, and restarts only the target task and its
downstream dependents. Successful parallel siblings remain complete.

The reply must first pass the policy's `needs_human_resume` engagement rule;
denied replies leave the coordinator paused. Projected issue IDs are registered
as Donkeyspace-managed resources so their webhook or polling events cannot
start an independent lifecycle.

## Filesystem isolation

Every attempt receives a separate physical workspace containing only declared
read and write roots. Only declared write roots are copied back into the
aggregate checkout. Absolute paths, parent traversal, and reported changes
outside write roots fail closed. Repository policy can narrow task access with
`task_access_overrides` (`stage_access_overrides` is a compatibility alias); it cannot widen manifest
access.

For GitHub-backed runs, Donkeyspace publishes accepted aggregate changes to a
single `donkeyspace/issue-<number>-<run>` checkpoint branch after each task
wave. A non-successful task receives a separate immutable attempt branch
containing its declared write-root changes, structured result, bounded logs,
and declared diagnostics. Successful tasks with non-empty diagnostics receive
the same isolated snapshot recorded as kind `diagnostic`. Publication errors
are recorded independently of the agent outcome and retain the workspace for a
dashboard-triggered retry. GitHub credentials are resolved immediately before
every push so long-running jobs do not reuse expired App installation tokens.

Attempt publication metadata includes `supporting_files`: regular files in the
task's write scope at that exact commit. It includes unchanged retained drafts,
which may already have been checkpointed before the attempt snapshot, and omits
deleted paths, symlinks, submodules and `.donkeyspace` diagnostics. This inventory
describes available supporting files, not which files the attempt created.
It is persisted before pushing, so publication status must be checked separately
before presenting remote file links. An absent inventory on an older record does
not establish that no draft exists.

## Run input and result

Lifecycle task input includes the actual role, graph task, and work item:

```json
{
  "role": "dv",
  "plugin": {
    "id": "example.rtl",
    "flow": "rtl_blocks",
    "task": "dv_verify",
    "attempt": 302
  },
  "work_item": {
    "id": "fifo",
    "spec": "docs/design/blocks/fifo.md",
    "depends_on": []
  },
  "workspace": {
    "repo_path": "repo",
    "result_path": ".donkeyspace/run-result.json",
    "read": ["docs/design", "rtl", "dv/fifo"],
    "write": ["dv/fifo"]
  },
  "parameters": {
    "source_root": "src",
    "source_extension": "rs"
  },
  "resources": [
    {
      "id": "project-standards",
      "source": "plugin",
      "source_path": "resources/project-standards.md",
      "root": ".donkeyspace/resources/project-standards",
      "available": true,
      "inventory": ["project-standards.md"],
      "digest": "sha256:..."
    }
  ],
  "previous_tasks": []
}
```

Each task writes `.donkeyspace/run-result.json`, using the standard `RunResult`
plus an optional handoff and optional `resources_used` array. Every reported
resource ID must have been available to that attempt. Roles must not commit,
push, apply labels, open pull requests, or edit outside the filtered workspace.

`plugin.allowed_handoffs` lists the current task's manifest-authorized targets.
Use it to choose a repair route; previous execution history is bounded and does
not establish current permissions. `previous_tasks` includes structured handoffs
with their full reason alongside abbreviated summaries, questions and blocker
reasons. A repair target can therefore inspect the reported failure without
depending on the source agent repeating every detail in its summary.

## GitHub relationship projection

Work-item issue identity is scoped to the repository, parent lifecycle issue,
and work-item ID. Retries and revisions reuse issues from the same parent;
another lifecycle using the same block name creates its own issue, even if an
earlier lifecycle's work-item issue is still open. Closed historical issues
retain their original specification and parent. Existing issues without the
parent identity marker remain reusable only when their recorded parent matches.

When `project_github_issues: true`, donkeyspace creates one GitHub sub-issue per
work item and projects registry dependencies as native blocked-by
relationships. Generated issues are marked so their webhooks cannot recursively
start another lifecycle. A successful start-task run first publishes an
immutable checkpoint, then renders each issue as **Proposed — awaiting
approval**, with exact commit and diff links. Approval promotes that desired
state to **Accepted** before dependent tasks are released. A repair updates the
same issue identities; accepted removals remain open until the replacement
proposal is approved, while never-accepted superseded issues may close
immediately. Donkeyspace's durable projection records remain authoritative for
scheduling, retries, managed dependency edges, and reconciliation. An approval
command is not exposed while publication or GitHub projection is incomplete.

## MCP boundary and limitations

Named stdio and HTTP MCP definitions are validated and included in task input
for roles that opt into them. Donkeyspace does not yet start those servers or
configure the agent CLI automatically.

Lifecycle execution is currently coordinated by one worker process. Human
pauses are resumable from their durable checkpoint, but an unplanned
coordinator crash between checkpoints does not yet resume the graph from the
last completed task. Parallelism is wave-based, and publication occurs after
the complete graph succeeds.

## Deployment

The installer consumes the manifest's optional `installation` metadata. Build
paths are relative to the plugin directory and default to `.` and `Dockerfile`.
Environment names must also be allowlisted by at least one role. Secret inputs
cannot declare defaults.

Connect and optionally activate a plugin with:

```sh
donkeyspace connect plugin --path ../example-plugin --flow implementation \
  --environment-file PROVIDER_TOKEN=/secure/provider-token
```

Donkeyspace keeps a registry of installed plugins but generates policy and a
Compose overlay for only the active flow. Lifecycle-replacement flows are
exclusive; ordinary flows replace the developer role. The active plugin is
mounted read-only, environment values are stored in mode-`0600` files and
mounted as Compose secrets, and only the active worker receives the Docker
socket. `donkeyspace plugin disable` returns to the default lifecycle while
preserving installed plugin state.

## Checkpoint authority

### Answering current blockers

The parent GitHub status comment and dashboard show current questions, blocking
reasons, task/work-item identity, and the action needed to continue. Workflow
cards preview the first question or reason and link to the complete blocker list.
These views read persisted task results and checkpoint targets; waiting task
reservations do not replace the result that asked the question. Resolved and
superseded questions leave the current view. New task-completion timeline events
retain complete questions and reasons for later inspection.

For a `needs_info` pause, reply on the parent issue with the requested answers.
If the same wave also requires approval, the workflow remains `needs_human`:
use the displayed task-specific revision command with your answers. Other
pending approvals remain required. Existing approval controls are unchanged.
Recovery errors appear before outstanding task questions so a missing checkpoint
is not mistaken for an ordinary clarification pause.

Supporting-file links point to the exact published attempt commit. The view
distinguishes an empty supporting-file inventory from pending or failed
publication; publication problems never hide the questions. Failed or pending
publications do not advertise remote file links. Older publications without an
inventory, or attempts without publication evidence, display unknown draft
availability rather than claiming that no draft was produced. A supporting file
may be an unchanged retained draft, not necessarily new work from that attempt.

### Revising completed upstream work

While a lifecycle is paused in `needs_human`, an authorized approver can reopen
a completed task that is an ancestor of a pending task. GitHub status and the
dashboard list eligible targets and the exact tasks each revision supersedes.
Workflow tasks use `TASK`; work-item tasks use `TASK/WORK-ITEM`. With the default
command prefix, a planning revision looks like:

```text
/donkeyspace revise plan
Change the accepted contract to address the validation finding.
```

The configured facade prefix applies. Feedback is mandatory. Approval remains
restricted to pending targets: `/donkeyspace approve plan` cannot reopen a
completed plan. Unknown, incorrectly scoped, unrelated, or unauthorized targets
do not change accepted progress.

The same coordinator records the human revision and invalidates the target and
its transitive dependents. A work-item revision preserves unrelated completed
siblings; a workflow revision can affect several work items. Affected pending
and accepted approval records and child attempts become superseded, retaining
their original evidence. Repository files remain available as repair inputs,
but their prior results cannot release dependents. Unrelated pending approvals
remain required. Revised tasks still obey their original write permissions.

A task configured with required approval pauses on its new proposal before
dependents run. A revised plan reconciles its work-item registry through the
existing projected-issue flow. Revision feedback, invalidation and audit records
commit with the durable checkpoint. Repeated delivery of the same comment cannot
apply the decision again; a newly authored command is a new human decision.

Revision choices require a saved graph from this version and a matching active
flow. Older checkpoints without that graph do not offer completed-task revision;
do not infer historical dependencies from a replacement plugin. Preserve their
history and use a fresh approved run when the original authority is unavailable.

### Persistence and repository recovery

PostgreSQL `lifecycle_checkpoints` is authoritative for plugin progress. Each commit
checks coordinator lease ownership, expiry, workflow generation and checkpoint
revision. Pauses commit checkpoint state, pending approval identities, completed
child results, coordinator/workflow state, audit and outbound actions together.
Approval commands are rendered from task/work-item identities, not extracted from
human-facing explanation text.

Each new production checkpoint also records the repository identity, exact commit
and tree, original base revision, and matching checkpoint publication. Resume
verifies these before starting an agent. A lost or modified aggregate checkout
is restored from retained local Git objects or by fetching those exact revisions
from GitHub. A later branch head cannot substitute for the saved content. Recovery
preserves an unexpected local checkout in `repo-unverified-*` for inspection.
Disposable task directories can be recreated from the verified aggregate.

The database records identity and progress, not a second copy of repository files.
If the exact objects are unavailable, or checkpoint provenance is missing or
inconsistent, the same coordinator pauses with a restoration/reapproval request.
Legacy filesystem checkpoints and older database checkpoints without immutable
repository provenance cannot resume automatically in production. Restore and
reconcile the original authority, or start a fresh run with renewed approval;
neither current `main` nor a reconstructed summary proves prior acceptance.
Completed checkpoints remain as tombstones, and expired model executions are
never automatically replayed. See
[upgrade and rollback](refactor-rollout.md) before upgrading an existing deployment.
