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

export function App() {
  const runsQuery = useQuery({
    queryKey: ["runs"],
    queryFn: fetchRuns,
    refetchInterval: 10_000,
    retry: 1
  });
  const runs = runsQuery.data ?? placeholderRuns;
  const activeRuns = runs.filter((run) =>
    ["leased", "running"].includes(run.status)
  ).length;
  const queuedRuns = runs.filter((run) => run.status === "queued").length;

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
    </main>
  );
}
