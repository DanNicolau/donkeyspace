import { useQuery } from "@tanstack/react-query";

type Run = {
  id: string;
  workflow_item_id: number | null;
  role: string;
  status: string;
  lease_owner: string | null;
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

const placeholderRuns: Run[] = [
  {
    id: "run_queued",
    workflow_item_id: null,
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

async function fetchOutboundActions(): Promise<OutboundAction[]> {
  const response = await fetch("/api/outbound-actions");

  if (!response.ok) {
    throw new Error(`Failed to load outbound actions: ${response.status}`);
  }

  return response.json();
}

export function App() {
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
          <h1>donkeyspace</h1>
          <p>Agentic repository workflow harness</p>
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
            <article className="run-row" key={run.id}>
              <div>
                <h3>{run.id}</h3>
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
              </div>
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
            </article>
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
