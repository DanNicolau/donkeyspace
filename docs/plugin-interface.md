# Donkeyspace Plugin Interface

Donkeyspace plugins are manifests plus executable container images. Core owns
policy, scheduling, durable job state, filtered workspaces, Git/GitHub effects,
and result validation. Plugins own roles, prompts, images, and task graphs.

## Integration modes

Plugins can integrate in two ways.

### Default-lifecycle role plugin

A serial flow may replace only the built-in developer command:

```yaml
agents:
  developer:
    enabled: true
    plugin:
      manifest_path: /plugins/example/donkeyspace-plugin.yml
      flow: implementation
```

Triage, review, and repair retain their built-in semantics. Existing serial
manifests using `agents`, `stages`, and `agent` remain accepted as compatibility
aliases for `roles`, `tasks`, and `role`.

### Lifecycle replacement

A repository can instead select an opt-in lifecycle flow:

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
triage, developer, reviewer, and repair webhook scheduling is bypassed. Plugins
that do not opt in keep the default lifecycle.

The environment map is `container variable: worker variable`. A value is
injected only when the selected role allowlists that variable. Secret values
are not written into run input.

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
    replaces_default_lifecycle: true
    work_items_path: docs/design/blocks/index.json
    project_github_issues: true
    max_handoffs_per_edge: 2
    max_parallel_tasks: 4
    tasks:
      architect:
        role: architect
        display_name: Block specification
        approval_subject: proposed block specifications
        write: [docs/design]
        approval: required
      rtl:
        role: rtl
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
        scope: work_item
        dependencies: [rtl, dv_prepare]
        read: [docs/design, rtl, "dv/{work_item}"]
        write: ["dv/{work_item}"]
        allowed_handoffs: [rtl]
        handoff_descriptions:
          rtl: Verification found an RTL-correctable defect.
      synthesis:
        role: syn
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
back. Validators run in the task's image with the same workspace, allowed
environment, and resources. Their exit codes and summaries are appended to the
standard test results. A missing or wrong-type artifact, modified resource, or
failed validator publishes nothing. Artifact and validator checks are skipped
for non-publishable outcomes such as `needs_changes`.

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
pausing, donkeyspace writes a versioned handoff checkpoint in the coordinator's
durable workspace. It records completed graph nodes, child jobs, projected
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

## GitHub relationship projection

When `project_github_issues: true`, donkeyspace creates one GitHub sub-issue per
work item and projects registry dependencies as native blocked-by
relationships. Generated issues are marked so their webhooks cannot recursively
start another lifecycle, and each is closed when its block graph completes.
This projection is for human visibility; donkeyspace's database remains
authoritative for scheduling and retries. Projection failures are reported but
do not block local task scheduling.

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
