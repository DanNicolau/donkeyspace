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
runtime:
  default_image: example-rtl:dev

roles:
  architect:
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
        write: [docs/design]
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
        scope: work_item
        dependencies: [rtl, dv_prepare]
        read: [docs/design, rtl, "dv/{work_item}"]
        write: ["dv/{work_item}"]
        allowed_handoffs: [rtl]
      synthesis:
        role: syn
        scope: work_item
        dependencies: [rtl]
        read: [docs/design, rtl]
        write: ["synth/{work_item}"]
        allowed_handoffs: [rtl]
```

`max_parallel_tasks` bounds the number of simultaneously running ready tasks;
it defaults to four. `scope: workflow` is the default. A lifecycle start task must have workflow
scope. After it completes, donkeyspace reads the planner-created registry and
expands every `scope: work_item` task. Work-item write roots must contain the
`{work_item}` placeholder so parallel attempts cannot replace one another's
files.

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

Donkeyspace creates a persisted child job for every expanded task. Jobs remain
`waiting` until their dependencies complete. All ready jobs in a task wave run
concurrently. The lifecycle's initial role job acts as the coordinator and the
aggregate checkout is published only after the graph completes.

## Feedback and human routing

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

The target task and every downstream dependent are invalidated and rerun.
Handoffs are bounded per work item/source/target edge. Exceeding the limit
produces `needs_human`. A role may return `needs_human` directly for ambiguous,
high-risk, or tool-limited decisions. Repository risk policy is applied again
before publication.

`needs_human` pauses the lifecycle coordinator instead of completing it. Before
pausing, donkeyspace writes a versioned handoff checkpoint in the coordinator's
durable workspace. It records completed graph nodes, child jobs, projected
GitHub issues, handoff counters, test evidence, and the exact task to resume.
The GitHub comment explains what decision is needed and what work will be
preserved. A human reply requeues the same coordinator UUID, reuses the
checkout and projected block issues, and restarts only the target task and its
downstream dependents. Successful parallel siblings remain complete.

## Filesystem isolation

Every attempt receives a separate physical workspace containing only declared
read and write roots. Only declared write roots are copied back into the
aggregate checkout. Absolute paths, parent traversal, and reported changes
outside write roots fail closed. Repository policy can narrow task access with
`task_access_overrides` (`stage_access_overrides` is a compatibility alias); it cannot widen manifest
access.

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
  "previous_tasks": []
}
```

Each task writes `.donkeyspace/run-result.json`, using the standard `RunResult`
plus an optional handoff. Roles must not commit, push, apply labels, open pull
requests, or edit outside the filtered workspace.

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

Build the plugin image according to the plugin's instructions. The base Compose
stack exposes a generic `/plugins` mount; each plugin owns its source mount,
runtime variables, Compose overlay, and ready-to-use policy. A typical local
deployment looks like this:

```sh
cd ../example-plugin
docker build -t example-plugin:dev .
cp donkeyspace.env.example .env

cd ../donkeyspace
docker compose \
  --env-file .env \
  --env-file ../example-plugin/.env \
  -f docker-compose.yml \
  -f ../example-plugin/docker-compose.donkeyspace.yml \
  up -d --build
```

Plugin-specific mount paths and runtime variables belong in the plugin-owned
overlay and environment file. Donkeyspace itself only requires the generic
`DONKEYSPACE_PLUGINS_DIR` installation root. Restart the API and worker after
changing the selected policy or plugin manifest.
