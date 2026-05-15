# V1 Readiness Checklist

This checklist tracks the work needed before the first useful version of donkeyspace is ready.

## Product Definition

- [x] Finalize the project name.
- [x] Decide whether policy config uses YAML or TOML.
- [x] Define the exact MVP user journey from issue creation to human-reviewed PR.
- [x] Define v1 non-goals clearly enough to avoid scope creep.
- [x] Decide which GitHub events trigger triage, implementation, and review jobs.
- [x] Define how humans pause, resume, retry, or cancel agent work.
- [x] Choose the first reference agent CLI adapter.
- [x] Define the local-development credential default.
- [x] Choose backend and frontend implementation stack.
- [x] Scaffold Rust workspace and TypeScript React dashboard.
- [x] Add Docker Compose skeleton.
- [x] Add repository policy file at `.donkeyspace/policy.yml`.

## GitHub Workflow

- [x] Define the complete AI label set.
- [x] Implement one-active-state-label behavior.
- [x] Define allowed label transitions.
- [x] Define how the system handles invalid or conflicting labels.
- [x] Define issue comment format for triage questions.
- [x] Define PR description format for agent-authored PRs.
- [x] Define reviewer-agent comment format.
- [x] Decide whether v1 supports slash commands or labels only.

## Policy System

- [x] Design the first policy file schema.
- [x] Add examples for a minimal policy and a stricter team policy.
- [ ] Support enabling and disabling agent roles.
- [ ] Support required local commands.
- [ ] Support required GitHub CI checks.
- [ ] Support path-based human-review requirements.
- [ ] Support labels that block agent work.
- [ ] Support labels that allow agent work.
- [ ] Support risk classification defaults.
- [ ] Define policy behavior when config is missing or invalid.

## Agent Orchestration

- [x] Define the external agent CLI interface.
- [x] Define required inputs passed to each agent role.
- [x] Define required outputs from each agent role.
- [x] Define how agents report uncertainty.
- [x] Define how agents request human help.
- [x] Define job claiming and locking behavior.
- [ ] Define retry behavior.
- [x] Define timeout behavior.
- [ ] Define cancellation behavior.
- [x] Implement DB-backed job lease acquisition.
- [x] Implement fake triage agent execution.
- [x] Persist completed and failed job results.
- [x] Record result-driven workflow transitions.

## Sandboxing And Execution

- [ ] Define the container image strategy for agent runs.
- [ ] Create ephemeral workspace behavior.
- [ ] Define repository checkout behavior.
- [ ] Define branch naming behavior.
- [ ] Define allowed network behavior.
- [ ] Define how model-provider credentials are injected.
- [ ] Define how GitHub credentials are injected.
- [ ] Capture command logs and exit codes.
- [ ] Clean up workspaces after completion or failure.

## GitHub Integration

- [ ] Create GitHub App or bot identity design.
- [x] Handle issue events.
- [x] Handle issue comment events.
- [ ] Handle label events.
- [ ] Handle pull request events.
- [ ] Handle check-suite or check-run events if needed.
- [ ] Create issue comments.
- [ ] Apply and remove labels.
- [ ] Create branches.
- [ ] Push commits.
- [ ] Open pull requests.
- [ ] Read PR diffs and CI status.

## Audit And Storage

- [x] Choose the database.
- [x] Define run records.
- [x] Define state transition records.
- [x] Define policy decision records.
- [x] Define command result records.
- [x] Define agent decision summary records.
- [ ] Link audit records to GitHub issues, PRs, comments, commits, and branches.
- [x] Persist pending outbound GitHub actions for audit before live writes.
- [ ] Decide log retention defaults.
- [ ] Decide what data must be redacted.

## Dashboard

- [x] Define the minimum dashboard routes.
- [x] Show active jobs.
- [x] Show run history.
- [x] Show pending outbound GitHub actions.
- [ ] Show issue and PR links.
- [x] Show state transitions.
- [ ] Show policy decisions.
- [ ] Show command results.
- [x] Show failure and escalation reasons.
- [ ] Show current policy config in read-only form.
- [x] Define the first useful dashboard slice.

## Testing

- [ ] Unit test policy parsing.
- [ ] Unit test state transitions.
- [ ] Unit test label conflict handling.
- [ ] Unit test risk routing behavior.
- [ ] Unit test agent-output parsing.
- [ ] Integration test GitHub webhook handling.
- [ ] Integration test issue triage flow.
- [ ] Integration test ready issue to PR flow.
- [ ] Integration test reviewer-agent escalation flow.
- [ ] End-to-end test the full one-repo loop in a test repository.

## Documentation

- [x] Write quickstart instructions.
- [x] Write Docker Compose setup instructions.
- [ ] Document required GitHub App permissions.
- [ ] Document policy file configuration.
- [ ] Document label meanings.
- [ ] Document the agent role contract.
- [ ] Document security and credential handling assumptions.
- [ ] Document known limitations.

## V1 Release Criteria

- [ ] A self-hosted Docker Compose deployment can run the system.
- [ ] A GitHub repo can be connected.
- [ ] The system can triage an issue and ask clarifying questions.
- [ ] The system can mark a clear issue as agent-ready.
- [ ] The system can run a developer agent in an isolated container.
- [ ] The system can open a pull request for a ready issue.
- [ ] The system can run configured checks and record results.
- [ ] The system can run a reviewer agent on the PR.
- [ ] The system can route uncertain or high-risk work to humans.
- [ ] Humans remain responsible for merge by default.
- [ ] The dashboard shows enough run history and logs to debug failures.
- [ ] The project documentation is sufficient for another developer to install and test v1.
