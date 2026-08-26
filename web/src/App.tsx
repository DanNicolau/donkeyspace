import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useEffect } from "react";

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

const placeholderRuns: Run[] = [
  {
    id: "run_queued",
    workflow_item_id: null,
    retry_of_job_id: null,
    role: "triage",
    status: "queued",
    lease_owner: null,
    result: null,
    created_at: new Date().toISOString(),
    updated_at: new Date().toISOString()
  }
];

async function fetchRuns(): Promise<Run[]> {
  const response = await fetch("/api/runs");

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

async function fetchOutboundActions(): Promise<OutboundAction[]> {
  const response = await fetch("/api/outbound-actions");

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

export function App() {
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
    queryKey: ["runs"],
    queryFn: fetchRuns,
    refetchInterval: 10_000,
    retry: 1
  });
  const actionsQuery = useQuery({
    queryKey: ["outbound-actions"],
    queryFn: fetchOutboundActions,
    refetchInterval: 10_000,
    retry: 1
  });
  const runs = runsQuery.data ?? placeholderRuns;
  const outboundActions = actionsQuery.data ?? [];
  const activeRuns = runs.filter((run) =>
    ["leased", "running"].includes(run.status)
  ).length;
  const queuedRuns = runs.filter((run) => run.status === "queued").length;
  const pendingActions = outboundActions.filter(
    (action) => action.status === "pending"
  ).length;

  return (
    <main className="app-shell">
      <header className="topbar">
        <div>
          <h1>{facade.display_name}</h1>
          <p>{facade.tagline}</p>
        </div>
        <a href="/healthz">API health</a>
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

      <section className="run-panel">
        <div className="panel-header">
          <h2>Runs</h2>
          <span>{runsQuery.isError ? "API unavailable" : "Live API"}</span>
        </div>
        <div className="run-list">
          {runs.map((run) => (
            <RunRow key={run.id} run={run} />
          ))}
        </div>
      </section>

      <section className="run-panel">
        <div className="panel-header">
          <h2>GitHub action outbox</h2>
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

function RunRow({ run }: { run: Run }) {
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

  return (
    <article className="run-row">
      <div>
        <h3>{runIssueTitle(run)}</h3>
        <div className="run-meta">
          <span>{run.id}</span>
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
            <dt>Role</dt>
            <dd>{run.role}</dd>
          </div>
          <div>
            <dt>Status</dt>
            <dd>{run.status}</dd>
          </div>
          <div>
            <dt>Lease</dt>
            <dd>{run.lease_owner ?? "none"}</dd>
          </div>
          <div>
            <dt>Outcome</dt>
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

function runIssueTitle(run: Run): string {
  return run.input?.issue?.title?.trim() || "Untitled issue";
}

function formatCommand(command: string[]): string {
  return command.length ? command.join(" ") : "<empty>";
}

function shortJobId(id: string): string {
  return id.length > 8 ? `${id.slice(0, 8)}…` : id;
}
