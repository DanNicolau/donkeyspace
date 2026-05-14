# Agentic Repository Harness Design

## Problem Statement

Modern software teams are beginning to use coding agents, reviewer agents, and issue triage agents, but the surrounding workflow is still fragmented. Agents can write code or review changes, but teams still need a reliable way to decide when an issue is clear enough for agent work, what level of autonomy is allowed, which tests are required, when humans must intervene, and how agent decisions should be audited.

This project is a self-hosted orchestration and policy harness for agentic repository work. It connects project-management signals, repository state, agent execution, tests, pull requests, reviews, and human approval into one controlled workflow.

The intended end state is that developers spend most of their time communicating real observations, resolving ambiguity, and reviewing high-stakes work. Routine clarification, implementation, testing, review preparation, and low-risk workflow coordination should be handled by agents under explicit policy.

## Goals

- Provide a self-hosted, containerized system that coordinates agentic work in repositories.
- Start with a GitHub-first workflow using Issues, labels, comments, pull requests, and CI.
- Let agents ask clarifying questions before implementation begins.
- Use explicit workflow states so humans and agents can understand what is ready, blocked, in progress, or waiting for review.
- Support configurable organizational policies for automation, required tests, risk handling, and human approval.
- Run agent work in isolated, ephemeral environments.
- Maintain useful audit logs for state transitions, agent decisions, commands, test results, and PR outcomes.
- Keep humans in control by default for v1, especially around merging.

## Non-Goals

- Build a new coding model or coding-agent runtime from scratch.
- Replace GitHub Issues, pull requests, labels, comments, or CI as the primary collaboration surface.
- Support every project-management and source-control provider in v1.
- Fully automate merging in the default v1 workflow.
- Coordinate multi-repo changes in v1.
- Provide a full enterprise governance platform in the first version.

## Target Users

The first target users are small engineering teams that want useful self-hosted automation without needing a dedicated platform engineering team.

These teams likely already use GitHub Issues, pull requests, and CI. They want coding agents to reduce routine implementation and review load, but they still need confidence, traceability, and clear human escape hatches.

Solo developers and larger platform teams may also benefit later, but the initial design should optimize for a small team adopting the tool on one repository.

## MVP Workflow

The v1 workflow centers on one GitHub repository.

1. A human creates or updates a GitHub issue.
2. A triage agent reviews the issue for ambiguity, missing context, risk signals, and success criteria.
3. If the issue is unclear, the triage agent comments with clarifying questions and applies `ai:needs-info`.
4. When the issue has enough context, the triage agent marks it `ai:ready`.
5. A developer agent claims the issue, applies `ai:in-progress`, creates an isolated workspace, and implements the change.
6. The developer agent runs required checks from policy and records the results.
7. If implementation succeeds, the developer agent opens a pull request and applies `ai:pr-open`.
8. A reviewer agent reviews the PR, evaluates risk, checks policy compliance, and requests changes or routes to human review.
9. Humans review and merge by default.

If an agent is uncertain, encounters missing context, fails tests, or believes the change is high risk, the workflow should move to `ai:needs-human` or `ai:blocked` instead of continuing autonomously.

## Agent Roles

### Triage Agent

The triage agent prepares issues for implementation.

Responsibilities:

- Detect ambiguity in requirements.
- Ask clarifying questions as GitHub issue comments.
- Identify missing reproduction steps, acceptance criteria, constraints, or affected areas.
- Decide when an issue is clear enough for implementation.
- Mark issues as ready for agent work.
- Record why an issue is ready or blocked.

The triage agent should not implement code.

### Developer Agent

The developer agent turns ready issues into pull requests.

Responsibilities:

- Claim an `ai:ready` issue.
- Create an isolated workspace.
- Inspect the repository and issue context.
- Implement the requested change.
- Run configured checks.
- Commit changes on a branch.
- Open a pull request.
- Summarize implementation choices, tests run, and known risks.

The developer agent should pause and request human help if requirements become unclear during implementation.

### Reviewer Agent

The reviewer agent reviews agent-authored pull requests before human review.

Responsibilities:

- Review the pull request for correctness, regressions, missing tests, and policy violations.
- Classify risk using both policy rules and agent judgment.
- Confirm required checks were run or are pending in CI.
- Request changes from the developer agent when appropriate.
- Route uncertain or high-risk changes to humans.

The reviewer agent should not be the final authority for merging in v1.

## GitHub Label State Machine

GitHub labels are the visible source of workflow state.

Initial labels:

- `ai:needs-info` - The issue needs human clarification before implementation.
- `ai:ready` - The issue has enough context for agent implementation.
- `ai:in-progress` - An agent is actively working on the issue.
- `ai:pr-open` - An agent-created PR exists for the issue.
- `ai:needs-human` - The system needs human judgment before continuing.
- `ai:blocked` - The agent cannot continue because of a hard blocker.

Expected transitions:

- New issue -> triage review.
- Triage unclear -> `ai:needs-info`.
- Human answers questions -> triage review.
- Triage clear -> `ai:ready`.
- Developer agent claims work -> `ai:in-progress`.
- PR opened -> `ai:pr-open`.
- Reviewer agent finds fixable issues -> back to developer agent work.
- Reviewer agent finds uncertainty or high risk -> `ai:needs-human`.
- Any hard failure or missing dependency -> `ai:blocked`.

Only one active AI workflow-state label should apply to an issue at a time. The system should remove stale AI state labels when applying a new state.

## Policy Model

Policies should be stored in repository or organization configuration. YAML is the default initial format, though TOML remains acceptable if the project later chooses it.

Policies should define:

- Enabled labels and allowed state transitions.
- Which agents are enabled.
- Which issue labels allow or prevent agent work.
- Required local commands.
- Required GitHub CI checks.
- Paths or file types that always require human review.
- Risk thresholds and escalation behavior.
- Whether agent judgment may classify risk.
- Whether future automatic merge is allowed.
- Maximum concurrency and retry behavior.

Risk classification should combine policy rules and agent judgment. Policy rules are authoritative. Agent judgment can identify likely risk, explain uncertainty, and recommend escalation.

Default v1 safety behavior:

- Human review is required before merge.
- Unclear risk routes to human review.
- Policy violations block progress.
- Failed required checks block progress.
- Agents must explain important decisions.

## Runtime And Sandboxing

The system should orchestrate external agent CLIs rather than implement a custom agent runtime in v1.

Each agent run should execute in an ephemeral container with:

- A fresh repository workspace or controlled checkout.
- Scoped GitHub/project credentials where possible.
- Access only to required configuration and secrets.
- Captured command logs.
- Captured exit status and test results.
- A controlled lifecycle managed by the orchestrator.

Model-provider credentials should remain abstract. A solo developer may use a personal account or local subscription, while an organization may use shared provider accounts or per-agent API credentials.

The system should distinguish between:

- Model-provider credentials used by the agent runtime.
- Repository credentials used to comment, push branches, open PRs, or inspect private code.
- Project-management credentials used for external integrations.

Repository and project-management credentials should be scoped per run or per installation wherever practical.

## Architecture

Core components:

- GitHub integration and webhook handler.
- Policy engine.
- Workflow state manager.
- Job scheduler.
- Agent-run worker.
- Sandbox manager.
- Audit and log store.
- Minimal dashboard.

GitHub remains the primary collaboration surface. The local database stores coordination metadata and audit history, but visible workflow state should remain understandable from GitHub labels and comments.

The dashboard should initially provide:

- Current agent jobs.
- Run history.
- Issue and PR links.
- State transitions.
- Policy decisions.
- Logs and command results.
- Basic policy visibility.

Policy editing may be added later. The first version can rely on repository configuration files.

## Auditability

The system should store enough information to understand what happened without forcing users to reconstruct events from scattered logs.

V1 audit records should include:

- Issue and PR identifiers.
- Agent role and run ID.
- State transitions.
- Policy version or policy snapshot used for the run.
- Agent decision summaries.
- Clarifying questions asked.
- Commands run.
- Test and CI results.
- Branch and commit identifiers.
- Links to PRs and comments created by agents.
- Failure reasons and escalation reasons.

The initial design does not need compliance-grade immutable audit logs, but the data model should avoid making that impossible later.

## Deployment Shape

The initial deployment target is Docker Compose.

Expected services:

- Web/API service.
- Background worker service.
- Database.
- Agent sandbox runner or worker runtime.
- Optional reverse proxy.

This should support one self-hosted installation managing one or more repositories eventually, while the MVP implementation proves the full loop on one repository.

Kubernetes support can come later if larger teams need stronger scheduling, scaling, and isolation.

## Future Extensions

Future versions may add:

- Multi-repo workspace groups.
- Cross-repo dependency awareness.
- GitLab support.
- Project-management integrations beyond GitHub Issues.
- Policy editing through the dashboard.
- Automatic merge for explicitly low-risk changes.
- Specialized agents for tests, security, documentation, releases, and dependency updates.
- Stronger isolation through VM-based execution.
- Compliance-grade audit exports.
- Organization-wide policy inheritance.
- Cost tracking and model-provider routing.

## Open Questions

- Should policy files standardize on YAML or TOML?
- How much policy editing should eventually happen through the dashboard?
- Which credential providers should be supported first?
- How should agents express uncertainty in a machine-readable way?
- What exact criteria should enable automatic merge in a future version?
- How should cross-repo dependency awareness work after the one-repo MVP?
- Should issue comments support explicit commands in addition to labels?
- How should the system prevent duplicate agent claims on the same issue?
- What is the minimum useful dashboard for v1?
