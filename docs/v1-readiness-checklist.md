# V1 Readiness Checklist

This checklist tracks the work needed before the first useful version of the agentic repository harness is ready.

## Product Definition

- [ ] Finalize the project name.
- [ ] Decide whether policy config uses YAML or TOML.
- [ ] Define the exact MVP user journey from issue creation to human-reviewed PR.
- [ ] Define v1 non-goals clearly enough to avoid scope creep.
- [ ] Decide which GitHub events trigger triage, implementation, and review jobs.
- [ ] Define how humans pause, resume, retry, or cancel agent work.

## GitHub Workflow

- [ ] Define the complete AI label set.
- [ ] Implement one-active-state-label behavior.
- [ ] Define allowed label transitions.
- [ ] Define how the system handles invalid or conflicting labels.
- [ ] Define issue comment format for triage questions.
- [ ] Define PR description format for agent-authored PRs.
- [ ] Define reviewer-agent comment format.
- [ ] Decide whether v1 supports slash commands or labels only.

## Policy System

- [ ] Design the first policy file schema.
- [ ] Support enabling and disabling agent roles.
- [ ] Support required local commands.
- [ ] Support required GitHub CI checks.
- [ ] Support path-based human-review requirements.
- [ ] Support labels that block agent work.
- [ ] Support labels that allow agent work.
- [ ] Support risk classification defaults.
- [ ] Define policy behavior when config is missing or invalid.
- [ ] Add examples for a minimal policy and a stricter team policy.

## Agent Orchestration

- [ ] Define the external agent CLI interface.
- [ ] Define required inputs passed to each agent role.
- [ ] Define required outputs from each agent role.
- [ ] Define how agents report uncertainty.
- [ ] Define how agents request human help.
- [ ] Define job claiming and locking behavior.
- [ ] Define retry behavior.
- [ ] Define timeout behavior.
- [ ] Define cancellation behavior.

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
- [ ] Handle issue events.
- [ ] Handle issue comment events.
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

- [ ] Choose the database.
- [ ] Define run records.
- [ ] Define state transition records.
- [ ] Define policy decision records.
- [ ] Define command result records.
- [ ] Define agent decision summary records.
- [ ] Link audit records to GitHub issues, PRs, comments, commits, and branches.
- [ ] Decide log retention defaults.
- [ ] Decide what data must be redacted.

## Dashboard

- [ ] Define the minimum dashboard routes.
- [ ] Show active jobs.
- [ ] Show run history.
- [ ] Show issue and PR links.
- [ ] Show state transitions.
- [ ] Show policy decisions.
- [ ] Show command results.
- [ ] Show failure and escalation reasons.
- [ ] Show current policy config in read-only form.

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

- [ ] Write quickstart instructions.
- [ ] Write Docker Compose setup instructions.
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
