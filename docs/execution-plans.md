# Approved execution plans and change requests

Describe execution preferences alongside the issue requirements:

> Build and check the component, skip packaging, and let me review the build first.

The planning agent proposes the specification and a separate execution plan. The
initial approval accepts both. With no preferences, every stage is included and
existing approval gates remain; the default plan still needs approval.

At a pause, describe changes in a new issue or PR comment:

```text
/donkeyspace revise
Skip packaging. Keep the build and check review gates.
```

Use the installation's command prefix. A task name is optional when change intake
is enabled. Addressing the planning task does not itself restart specification
work. The planner first reads the retained checkpoint and returns a proposal. It
cannot publish file edits during intake.

The comment explains the plan changes, which tasks must run again, and what is
retained. Accept the displayed proposal with its exact versioned command, such as
`/donkeyspace approve change/2`, or send another `revise` request to adjust it. If
the planner asks a question, answer on the issue. Nothing in the current plan or
accepted artifacts changes until approval. A newer proposal supersedes the old
one; stale approvals and duplicate decisions cannot apply it again.

- Plan-only changes preserve specification approval and unaffected completed work.
- Implementation feedback reruns the selected tasks and included dependents.
- Mixed requests can change scheduling and request implementation work together.
- Specification changes use the normal planning and specification approval path,
  conservatively rerunning its dependents.
- Approving a change does not approve retained artifacts. Existing review gates
  remain pending, with their own commands. New gates also apply to completed work.

For example, “simplify the implementation without changing behavior, and skip
packaging” should propose an implementation revision and a plan change together.
“Keep the current plan and do not change anything” can withdraw a proposed change
through a replacement, no-op proposal.

Requests are accepted at paused or recoverable failed checkpoints, and when
starting a revision of a completed managed PR. Wait for running work to pause;
this feature does not interrupt active agents. Human source edits already pushed
to a PR independently invalidate checks that read or write those paths. The
proposal includes that required revalidation.

## Plugin contract

Both features are explicit opt-ins and domain independent:

```yaml
flows:
  change:
    start: plan
    replaces_default_lifecycle: true
    change_intake: true
    execution_plan:
      optional_tasks: [package]
    # roles, tasks, and dependencies use the normal plugin interface
```

Only declared optional tasks may be excluded. The start task cannot be excluded;
included tasks cannot depend on excluded tasks. Plans may add approvals but cannot
remove ones required by the manifest. Excluded tasks are never marked passed and
automatic handoffs cannot restart them. Final summaries show the approved scope.

During initial planning the start task may return a top-level `execution_plan`:

```json
{
  "excluded_tasks": ["package"],
  "approval_tasks": ["build", "check"],
  "reason": "Review implementation and verification without packaging."
}
```

During change intake the same role receives `change_request` in run input, including
the human request, target hint, current plan, pending reviews, work-item identities,
and available task targets. The plugin must provide an intake prompt and support
this nullable result field:

```json
{
  "change_proposal": {
    "summary": "Omit packaging and simplify the component implementation.",
    "execution_plan": {
      "excluded_tasks": ["package"],
      "approval_tasks": ["build", "check"],
      "reason": "Requested execution scope."
    },
    "revisions": [
      {"target": "build/component", "instruction": "Simplify without changing behavior."}
    ]
  }
}
```

The nested plan is the complete desired settings, or null to preserve the current
plan. An empty revisions list means no new artifact revision. Targets use manifest
task identifiers and optional work-item identifiers. Intake returns `implemented`
for a valid proposal or `needs_info` with questions. All normal result metadata is
still required. Intake must return no changed files, work items, handoff, or
top-level execution plan. Normal task results must not return a change proposal.

The worker removes all write roots, including deployment overrides, and never
copies intake output back into the retained repository. It validates targets,
dependencies and approval rules, computes rerun scope itself, then binds approval
to the proposal version, checkpoint and configured flow. The virtual task name
`change` is reserved in opted-in flows.

Plugins without `change_intake` retain their existing targeted revision commands.
For execution-plan-enabled legacy flows, `revise execution-plan` still accepts
structured YAML/JSON followed by `approve execution-plan`. This is compatibility
behavior, not the user interface for an intake-enabled flow.

## Deployment and verification

Upgrade API, worker, plugin manifest and plugin runtime together. A prompt-only
refresh cannot add the scheduling or command handling. Checkpoint version 7 stores
pending proposals and monotonic proposal versions; versions 5 and 6 remain readable
and are upgraded on save. Do not downgrade against version 7 checkpoints. No SQL
migration is needed.

Authorization, required validators, publication checks and artifact approvals stay
in force. Core and worker tests cover proposals, selective invalidation, stale
approvals, PR preparation, and concise comments. `scripts/test-database` exercises
real containers and a disposable database, including read-only intake, duplicate
requests, clarification, plan-only changes and targeted reruns.
