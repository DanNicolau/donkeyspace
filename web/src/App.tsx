import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useEffect, useMemo, useState } from "react";

type Facade = {
  display_name: string;
  tagline: string;
  issue_command: string;
  branch_prefix: string;
};

type Run = {
  id: string;
  workflow_item_id: number | null;
  retry_of_job_id: string | null;
  role: string;
  status: string;
  lease_owner: string | null;
  input?: {
    issue?: {
      number?: number;
      title?: string | null;
    };
    repository?: {
      full_name?: string;
      name?: string;
      owner?: {
        login?: string;
      };
    };
    plugin_execution?: {
      coordinator_run_id?: string;
      task?: string;
      work_item?: {
        id?: string;
      };
    };
  };
  result: RunResult | null;
  created_at: string;
  updated_at: string;
};

type RunResult = {
  outcome: string;
  summary: string;
  confidence: string;
  risk: string;
  questions: string[];
  blocked_reason: string | null;
};

type OutboundAction = {
  id: number;
  workflow_item_id: number;
  job_id: string | null;
  provider: string;
  action_type: string;
  status: string;
  last_error: string | null;
  payload: {
    owner?: string;
    repo?: string;
    issue_number?: number;
    label?: string;
    labels?: string[];
    body?: string;
    state?: string;
  };
  created_at: string;
};

type CommandResult = {
  id: number;
  job_id: string;
  name: string;
  command: string[];
  status: string;
  exit_code: number | null;
  summary: string | null;
  created_at: string;
};

type RunDetail = {
  job: Run;
  command_results: CommandResult[];
  publications: AgentPublication[];
};

type AgentPublication = {
  id: number;
  kind: string;
  branch_name: string;
  commit_sha: string;
  html_url: string;
  task: string | null;
  work_item: string | null;
  attempt: number | null;
  outcome: string | null;
  status: string;
  last_error: string | null;
};

type GitHubPollRepositoryStatus = {
  full_name: string;
  server_interval_seconds: number | null;
  next_eligible_at: string;
  last_polled_at: string | null;
  last_success_at: string | null;
  last_error: string | null;
  consecutive_failures: number;
};

type GitHubPollStatus = {
  enabled: boolean;
  running: boolean;
  pending_manual: boolean;
  configured_interval_seconds: number;
  last_started_at: string | null;
  last_completed_at: string | null;
  last_success_at: string | null;
  last_error: string | null;
  consecutive_failures: number;
  next_poll_at: string | null;
  repositories: GitHubPollRepositoryStatus[];
};

const repositoryStorageKey = "donkeyspace.dashboard.repository";

function repositoryQuery(repository: string): string {
  return repository ? `?repository=${encodeURIComponent(repository)}` : "";
}

async function fetchRuns(repository: string): Promise<Run[]> {
  const response = await fetch(`/api/runs${repositoryQuery(repository)}`);

  if (!response.ok) {
    throw new Error(`Failed to load runs: ${response.status}`);
  }

  return response.json();
}

async function fetchFacade(): Promise<Facade> {
  const response = await fetch("/api/facade");
  if (!response.ok) {
    throw new Error(`Failed to load facade: ${response.status}`);
  }
  return response.json();
}

async function fetchRepositories(): Promise<string[]> {
  const response = await fetch("/api/repositories");
  if (!response.ok) {
    throw new Error(`Failed to load repositories: ${response.status}`);
  }
  return response.json();
}

async function fetchOutboundActions(repository: string): Promise<OutboundAction[]> {
  const response = await fetch(`/api/outbound-actions${repositoryQuery(repository)}`);

  if (!response.ok) {
    throw new Error(`Failed to load outbound actions: ${response.status}`);
  }

  return response.json();
}

async function fetchRunDetail(id: string): Promise<RunDetail> {
  const response = await fetch(`/api/runs/${id}`);

  if (!response.ok) {
    throw new Error(`Failed to load run detail: ${response.status}`);
  }

  return response.json();
}

async function retryRun(id: string): Promise<Run> {
  const response = await fetch(`/api/runs/${id}/retry`, {
    method: "POST"
  });

  if (!response.ok) {
    const error = await response.json().catch(() => null);
    const message = error?.error ?? `Failed to retry run: ${response.status}`;
    throw new Error(message);
  }

  return response.json();
}

async function retryPublication(id: number): Promise<void> {
  const response = await fetch(`/api/publications/${id}/retry`, { method: "POST" });
  if (!response.ok) {
    const error = await response.json().catch(() => null);
    throw new Error(error?.error ?? `Failed to retry publication: ${response.status}`);
  }
}

async function fetchGitHubPollStatus(): Promise<GitHubPollStatus> {
  const response = await fetch("/api/github-poll/status");
  if (!response.ok) {
    throw new Error(`Failed to load GitHub polling status: ${response.status}`);
  }
  return response.json();
}

async function triggerGitHubPoll(): Promise<void> {
  const response = await fetch("/api/github-poll/trigger", { method: "POST" });
  if (!response.ok) {
    const error = await response.json().catch(() => null);
    throw new Error(error?.error ?? `Failed to request GitHub poll: ${response.status}`);
  }
}

export function App() {
  const [selectedRepository, setSelectedRepository] = useState(() => {
    try {
      return window.localStorage.getItem(repositoryStorageKey) ?? "";
    } catch {
      return "";
    }
  });
  const facadeQuery = useQuery({
    queryKey: ["facade"],
    queryFn: fetchFacade,
    staleTime: Infinity,
    retry: 1
  });
  const facade = facadeQuery.data ?? {
    display_name: "Agent Platform",
    tagline: "Agentic repository workflow",
    issue_command: "",
    branch_prefix: "agent"
  };
  useEffect(() => {
    document.title = facade.display_name;
  }, [facade.display_name]);
  const runsQuery = useQuery({
    queryKey: ["runs", selectedRepository],
    queryFn: () => fetchRuns(selectedRepository),
    refetchInterval: 10_000,
    retry: 1
  });
  const actionsQuery = useQuery({
    queryKey: ["outbound-actions", selectedRepository],
    queryFn: () => fetchOutboundActions(selectedRepository),
    refetchInterval: 10_000,
    retry: 1
  });
  const pollQuery = useQuery({
    queryKey: ["github-poll-status"],
    queryFn: fetchGitHubPollStatus,
    refetchInterval: 5_000,
    retry: 1
  });
  const repositoriesQuery = useQuery({
    queryKey: ["repositories"],
    queryFn: fetchRepositories,
    staleTime: 60_000,
    retry: 1
  });
  const repositories = useMemo(
    () =>
      Array.from(
        new Set([
          ...(repositoriesQuery.data ?? []),
          ...(pollQuery.data?.repositories.map((repository) => repository.full_name) ?? [])
        ])
      ).sort((left, right) => left.localeCompare(right)),
    [pollQuery.data?.repositories, repositoriesQuery.data]
  );
  useEffect(() => {
    if (
      selectedRepository &&
      (repositoriesQuery.isSuccess || pollQuery.isSuccess) &&
      !repositories.includes(selectedRepository)
    ) {
      setSelectedRepository("");
    }
  }, [pollQuery.isSuccess, repositories, repositoriesQuery.isSuccess, selectedRepository]);
  useEffect(() => {
    try {
      if (selectedRepository) {
        window.localStorage.setItem(repositoryStorageKey, selectedRepository);
      } else {
        window.localStorage.removeItem(repositoryStorageKey);
      }
    } catch {
      // Storage can be unavailable in locked-down browser contexts.
    }
  }, [selectedRepository]);
  const runs = runsQuery.data ?? [];
  const outboundActions = actionsQuery.data ?? [];
  const activeRuns = runs.filter((run) =>
    ["leased", "running"].includes(run.status)
  ).length;
  const queuedRuns = runs.filter((run) => run.status === "queued").length;
  const pendingActions = outboundActions.filter(
    (action) => action.status === "pending"
  ).length;
  const coordinatorRunIds = new Set(
    runs
      .map((run) => run.input?.plugin_execution?.coordinator_run_id)
      .filter((id): id is string => Boolean(id))
  );

  return (
    <main className="app-shell">
      <header className="topbar">
        <div>
          <h1>{facade.display_name}</h1>
          <p>{facade.tagline}</p>
        </div>
        <div className="topbar-actions">
          <label className="repository-picker">
            <span>Repository</span>
            <select
              aria-label="Dashboard repository"
              onChange={(event) => setSelectedRepository(event.target.value)}
              value={selectedRepository}
            >
              <option value="">All repositories</option>
              {repositories.map((repository) => (
                <option key={repository} value={repository}>
                  {repository}
                </option>
              ))}
            </select>
          </label>
          <a href="/healthz">API health</a>
        </div>
      </header>

      <section className="summary-grid" aria-label="Run summary">
        <div>
          <span>Active runs</span>
          <strong>{activeRuns}</strong>
        </div>
        <div>
          <span>Queued jobs</span>
          <strong>{queuedRuns}</strong>
        </div>
        <div>
          <span>Total runs</span>
          <strong>{runs.length}</strong>
        </div>
        <div>
          <span>Pending GitHub actions</span>
          <strong>{pendingActions}</strong>
        </div>
      </section>

      <GitHubPollingPanel
        repository={selectedRepository}
        status={pollQuery.data}
        unavailable={pollQuery.isError}
      />

      <section className="run-panel">
        <div className="panel-header">
          <div>
            <h2>Runs</h2>
            <span>{selectedRepository || "All repositories"}</span>
          </div>
          <span>{runsQuery.isError ? "API unavailable" : "Live API"}</span>
        </div>
        <div className="run-list">
          {runs.length ? (
            runs.map((run) => (
              <RunRow
                isCoordinator={coordinatorRunIds.has(run.id)}
                key={run.id}
                run={run}
              />
            ))
          ) : (
            <div className="empty-state">
              {runsQuery.isPending ? "Loading runs…" : "No runs for this repository yet."}
            </div>
          )}
        </div>
      </section>

      <section className="run-panel">
        <div className="panel-header">
          <div>
            <h2>GitHub action outbox</h2>
            <span>{selectedRepository || "All repositories"}</span>
          </div>
          <span>{actionsQuery.isError ? "API unavailable" : "Pending writes"}</span>
        </div>
        <div className="action-list">
          {outboundActions.length ? (
            outboundActions.map((action) => (
              <article className="action-row" key={action.id}>
                <div>
                  <h3>{action.action_type}</h3>
                  <p>{describeAction(action)}</p>
                </div>
                <dl>
                  <div>
                    <dt>Status</dt>
                    <dd>{action.status}</dd>
                  </div>
                  <div>
                    <dt>Repository</dt>
                    <dd>{actionRepository(action) ?? "unknown"}</dd>
                  </div>
                  <div>
                    <dt>Issue</dt>
                    <dd>
                      {action.payload.issue_number
                        ? `#${action.payload.issue_number}`
                        : "unknown"}
                    </dd>
                  </div>
                </dl>
              </article>
            ))
          ) : (
            <div className="empty-state">No outbound GitHub actions yet.</div>
          )}
        </div>
      </section>
    </main>
  );
}

function GitHubPollingPanel({
  repository,
  status,
  unavailable
}: {
  repository: string;
  status: GitHubPollStatus | undefined;
  unavailable: boolean;
}) {
  const queryClient = useQueryClient();
  const [now, setNow] = useState(() => Date.now());
  useEffect(() => {
    const timer = window.setInterval(() => setNow(Date.now()), 1_000);
    return () => window.clearInterval(timer);
  }, []);
  const triggerMutation = useMutation({
    mutationFn: triggerGitHubPoll,
    onSuccess: async () => {
      await queryClient.invalidateQueries({ queryKey: ["github-poll-status"] });
    }
  });
  const state = unavailable
    ? "Status unavailable"
    : status?.running
      ? "Polling GitHub"
      : status?.pending_manual
        ? "Poll requested"
        : status?.enabled
          ? "Polling enabled"
          : "Polling disabled";
  const visibleRepositories = repository
    ? status?.repositories.filter((item) => item.full_name === repository) ?? []
    : status?.repositories ?? [];
  const selectedStatus = repository ? visibleRepositories[0] : undefined;
  const lastPoll = repository
    ? selectedStatus?.last_polled_at ?? null
    : status?.last_completed_at ?? null;
  const nextPoll = repository
    ? selectedStatus?.next_eligible_at ?? null
    : status?.next_poll_at ?? null;
  const failures = repository
    ? selectedStatus?.consecutive_failures ?? 0
    : status?.consecutive_failures ?? 0;

  return (
    <section className="run-panel polling-panel">
      <div className="panel-header">
        <div>
          <h2>GitHub polling</h2>
          <span>{repository ? `${state} · ${repository}` : state}</span>
        </div>
        <button
          className="retry-button"
          disabled={!status?.enabled || triggerMutation.isPending}
          onClick={() => triggerMutation.mutate()}
          type="button"
        >
          {triggerMutation.isPending
            ? "Requesting…"
            : repository
              ? "Poll all now"
              : "Poll now"}
        </button>
      </div>
      {status ? (
        <div className="polling-status">
          <dl>
            <div>
              <dt>Configured</dt>
              <dd>{status.configured_interval_seconds}s</dd>
            </div>
            <div>
              <dt>Since last poll</dt>
              <dd>{formatElapsed(lastPoll, now)}</dd>
            </div>
            <div>
              <dt>Until next poll</dt>
              <dd>{formatCountdown(nextPoll, now)}</dd>
            </div>
            <div>
              <dt>Failures</dt>
              <dd>{failures}</dd>
            </div>
          </dl>
          {visibleRepositories.map((item) => (
            <div className="polling-repository" key={item.full_name}>
              <strong>{item.full_name}</strong>
              <span>
                GitHub minimum: {item.server_interval_seconds ?? "unknown"}s · next in{" "}
                {formatCountdown(item.next_eligible_at, now)}
              </span>
              {item.last_error ? <p>{item.last_error}</p> : null}
            </div>
          ))}
          {(repository ? selectedStatus?.last_error : status.last_error) ? (
            <p className="polling-error">
              {repository ? selectedStatus?.last_error : status.last_error}
            </p>
          ) : null}
        </div>
      ) : (
        <div className="empty-state">Polling status is unavailable.</div>
      )}
      {triggerMutation.isError ? (
        <p className="retry-status" role="status">
          {triggerMutation.error instanceof Error
            ? triggerMutation.error.message
            : "Failed to request GitHub poll"}
        </p>
      ) : null}
    </section>
  );
}

function RunCommandResults({ runId }: { runId: string }) {
  const detailQuery = useQuery({
    queryKey: ["run-detail", runId],
    queryFn: () => fetchRunDetail(runId),
    refetchInterval: 10_000,
    retry: 1
  });
  const commandResults = detailQuery.data?.command_results ?? [];
  const publications = detailQuery.data?.publications ?? [];

  if (!commandResults.length && !publications.length) {
    return null;
  }

  return (
    <div className="command-results">
      {publications.map((publication) => (
        <RunPublication key={publication.id} publication={publication} runId={runId} />
      ))}
      {commandResults.map((result) => (
        <div className="command-result" key={result.id}>
          <div>
            <strong>{result.name}</strong>
            <code>{formatCommand(result.command)}</code>
          </div>
          <span data-status={result.status}>
            {result.status}
            {result.exit_code === null ? "" : ` ${result.exit_code}`}
          </span>
          {result.summary ? <pre>{result.summary}</pre> : null}
        </div>
      ))}
    </div>
  );
}

function RunPublication({
  publication,
  runId
}: {
  publication: AgentPublication;
  runId: string;
}) {
  const queryClient = useQueryClient();
  const retryMutation = useMutation({
    mutationFn: () => retryPublication(publication.id),
    onSuccess: async () => {
      await queryClient.invalidateQueries({ queryKey: ["run-detail", runId] });
    }
  });
  const label = publication.task
    ? `${publication.task}${publication.work_item ? `/${publication.work_item}` : ""}`
    : "Accepted checkpoint";

  return (
    <div className="command-result publication-result">
      <div>
        <strong>{label}</strong>
        <a href={publication.html_url} rel="noreferrer" target="_blank">
          {publication.branch_name}
        </a>
        <code>{publication.commit_sha.slice(0, 12)}</code>
      </div>
      <span data-status={publication.status}>
        {publication.kind} · {publication.status}
      </span>
      {publication.outcome ? <p>Outcome: {publication.outcome}</p> : null}
      {publication.last_error ? <pre>{publication.last_error}</pre> : null}
      {publication.status === "failed" ? (
        <button
          className="retry-button"
          disabled={retryMutation.isPending}
          onClick={() => retryMutation.mutate()}
          type="button"
        >
          {retryMutation.isPending ? "Retrying…" : "Retry branch push"}
        </button>
      ) : null}
      {retryMutation.isError ? (
        <p className="retry-status" role="status">
          {retryMutation.error instanceof Error
            ? retryMutation.error.message
            : "Failed to retry publication"}
        </p>
      ) : null}
    </div>
  );
}

function RunRow({ run, isCoordinator }: { run: Run; isCoordinator: boolean }) {
  const queryClient = useQueryClient();
  const retryMutation = useMutation({
    mutationFn: retryRun,
    onSuccess: async () => {
      await Promise.all([
        queryClient.invalidateQueries({ queryKey: ["runs"] }),
        queryClient.invalidateQueries({ queryKey: ["run-detail", run.id] })
      ]);
    }
  });
  const canRetry =
    run.status === "failed" &&
    run.result?.outcome !== "blocked" &&
    run.result?.outcome !== "needs_human";
  const pluginExecution = run.input?.plugin_execution;
  const runType = isCoordinator
    ? "Lifecycle coordinator"
    : pluginExecution
      ? "Agent task"
      : "Standalone job";

  return (
    <article className="run-row">
      <div>
        <h3>{runIssueTitle(run)}</h3>
        <div className="run-meta">
          <span>{run.id}</span>
          {runRepository(run) ? <span>{runRepository(run)}</span> : null}
          <span>{runType}</span>
          {run.retry_of_job_id ? (
            <span>Retry of {shortJobId(run.retry_of_job_id)}</span>
          ) : null}
          {run.input?.issue?.number ? <span>Issue #{run.input.issue.number}</span> : null}
        </div>
        <p>
          {run.result?.summary ??
            (run.workflow_item_id
              ? `Workflow item ${run.workflow_item_id}`
              : "No workflow item linked yet")}
        </p>
        {isCoordinator ? (
          <p>
            This outcome is aggregate lifecycle state. Individual agent outcomes are shown on
            their task rows.
          </p>
        ) : null}
        {run.result?.questions.length ? (
          <ul className="question-list">
            {run.result.questions.map((question) => (
              <li key={question}>{question}</li>
            ))}
          </ul>
        ) : null}
        <RunCommandResults runId={run.id} />
      </div>
      <div className="run-sidecar">
        <dl>
          <div>
            <dt>Run type</dt>
            <dd>{runType}</dd>
          </div>
          <div>
            <dt>{isCoordinator ? "Initial role" : "Agent role"}</dt>
            <dd>{run.role}</dd>
          </div>
          {pluginExecution?.task ? (
            <div>
              <dt>Task</dt>
              <dd>{pluginExecution.task}</dd>
            </div>
          ) : null}
          {pluginExecution?.work_item?.id ? (
            <div>
              <dt>Work item</dt>
              <dd>{pluginExecution.work_item.id}</dd>
            </div>
          ) : null}
          <div>
            <dt>Status</dt>
            <dd>{run.status}</dd>
          </div>
          <div>
            <dt>Lease</dt>
            <dd>{run.lease_owner ?? "none"}</dd>
          </div>
          <div>
            <dt>{isCoordinator ? "Lifecycle outcome" : "Task outcome"}</dt>
            <dd>{run.result?.outcome ?? "pending"}</dd>
          </div>
        </dl>
        {canRetry ? (
          <button
            className="retry-button"
            disabled={retryMutation.isPending}
            onClick={() => {
              retryMutation.mutate(run.id);
            }}
            type="button"
          >
            {retryMutation.isPending ? "Retrying..." : "Retry failed job"}
          </button>
        ) : null}
        {retryMutation.isError ? (
          <p className="retry-status" role="status">
            {retryMutation.error instanceof Error
              ? retryMutation.error.message
              : "Failed to retry run"}
          </p>
        ) : null}
      </div>
    </article>
  );
}

function describeAction(action: OutboundAction): string {
  if (action.last_error) {
    return action.last_error;
  }

  if (action.action_type === "issue.add_label" && action.payload.label) {
    return `Add ${action.payload.label}`;
  }

  if (action.action_type === "issue.remove_labels" && action.payload.labels) {
    return `Remove ${action.payload.labels.join(", ")}`;
  }

  if (action.action_type === "issue.create_comment" && action.payload.body) {
    return action.payload.body.split("\n")[0];
  }

  return action.provider;
}

function actionRepository(action: OutboundAction): string | null {
  return action.payload.owner && action.payload.repo
    ? `${action.payload.owner}/${action.payload.repo}`
    : null;
}

function runRepository(run: Run): string | null {
  const repository = run.input?.repository;
  if (repository?.full_name) {
    return repository.full_name;
  }
  return repository?.owner?.login && repository.name
    ? `${repository.owner.login}/${repository.name}`
    : null;
}

function runIssueTitle(run: Run): string {
  return run.input?.issue?.title?.trim() || "Untitled issue";
}

function formatCommand(command: string[]): string {
  return command.length ? command.join(" ") : "<empty>";
}

function shortJobId(id: string): string {
  return id.length > 8 ? `${id.slice(0, 8)}…` : id;
}

function formatElapsed(value: string | null, now: number): string {
  if (!value) {
    return "not yet";
  }
  const timestamp = Date.parse(value);
  return Number.isFinite(timestamp)
    ? formatDuration(Math.max(0, now - timestamp))
    : "unknown";
}

function formatCountdown(value: string | null, now: number): string {
  if (!value) {
    return "unknown";
  }
  const timestamp = Date.parse(value);
  if (!Number.isFinite(timestamp)) {
    return "unknown";
  }
  const remaining = timestamp - now;
  return remaining <= 0 ? "due now" : formatDuration(remaining, true);
}

function formatDuration(milliseconds: number, roundUp = false): string {
  const rawSeconds = milliseconds / 1_000;
  const totalSeconds = Math.max(
    0,
    roundUp ? Math.ceil(rawSeconds) : Math.floor(rawSeconds)
  );
  const days = Math.floor(totalSeconds / 86_400);
  const hours = Math.floor((totalSeconds % 86_400) / 3_600);
  const minutes = Math.floor((totalSeconds % 3_600) / 60);
  const seconds = totalSeconds % 60;

  if (days > 0) {
    return `${days}d ${hours}h`;
  }
  if (hours > 0) {
    return `${hours}h ${minutes}m`;
  }
  if (minutes > 0) {
    return `${minutes}m ${seconds}s`;
  }
  return `${seconds}s`;
}
