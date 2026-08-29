import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useEffect, useState } from "react";

type Facade = { display_name: string; tagline: string; issue_command: string; branch_prefix: string };
type WorkflowTask = { job_id: string; role: string; role_display_name: string; task: string; task_display_name: string; work_item: string | null; status: string; outcome: string | null; summary: string | null; updated_at: string };
type Workflow = { id: number; owner: string; repository: string; issue_number: number; issue_title: string; issue_url: string; provider_state: string; current_state: string | null; coordinator_job_id: string | null; coordinator_status: string | null; outcome: string | null; summary: string | null; pending_approval: string | null; tasks: WorkflowTask[]; pull_request_number: number | null; pull_request_url: string | null; pull_request_state: string | null; no_pr_reason: string | null; updated_at: string };
type TimelineLink = { kind: string; label: string; url: string };
type TimelineEvent = { id: number; event_type: string; level: string; source: string; actor: string | null; wave: number | null; attempt: number | null; role: string | null; role_display_name: string | null; task: string | null; task_display_name: string | null; work_item: string | null; status: string | null; outcome: string | null; summary: string; reason: string | null; handoff_target: string | null; links: TimelineLink[]; created_at: string };
type EventPage = { events: TimelineEvent[]; next_before_id: number | null };
type Run = { id: string; role: string; status: string; result: { outcome?: string; summary?: string } | null; input?: { issue?: { number?: number; title?: string }; repository?: { full_name?: string } }; updated_at: string };
type OutboundAction = { id: number; action_type: string; status: string; last_error: string | null; payload: { owner?: string; repo?: string; issue_number?: number }; created_at: string };
type PollStatus = { enabled: boolean; running: boolean; pending_manual: boolean; configured_interval_seconds: number; last_completed_at: string | null; next_poll_at: string | null };

const storageKey = "donkeyspace.dashboard.repository";
const json = async <T,>(url: string, init?: RequestInit): Promise<T> => {
  const response = await fetch(url, init);
  if (!response.ok) {
    const error = await response.json().catch(() => null);
    throw new Error(error?.error ?? `Request failed: ${response.status}`);
  }
  return response.json();
};
const repositoryQuery = (repository: string) => repository ? `?repository=${encodeURIComponent(repository)}` : "";

export function App() {
  const facadeQuery = useQuery({ queryKey: ["facade"], queryFn: () => json<Facade>("/api/facade"), staleTime: Infinity });
  const facade = facadeQuery.data ?? { display_name: "Agent Platform", tagline: "Agentic repository workflow", issue_command: "", branch_prefix: "agent" };
  useEffect(() => { document.title = facade.display_name; }, [facade.display_name]);
  const path = window.location.pathname.replace(/\/$/, "") || "/";
  const detail = path.match(/^\/repositories\/([^/]+)\/([^/]+)\/issues\/(\d+)$/);
  const page = path === "/operations" ? "operations" : detail ? "detail" : "issues";
  return <main className="app-shell">
    <header className="topbar"><div><h1>{facade.display_name}</h1><p>{facade.tagline}</p></div>
      <nav className="primary-nav" aria-label="Primary navigation"><a aria-current={page !== "operations" ? "page" : undefined} href="/">Issues</a><a aria-current={page === "operations" ? "page" : undefined} href="/operations">Operations</a><a href="/healthz">API health</a></nav>
    </header>
    {page === "operations" ? <OperationsPage /> : detail ? <WorkflowDetail owner={decodeURIComponent(detail[1])} repo={decodeURIComponent(detail[2])} number={Number(detail[3])} /> : <IssuesPage />}
  </main>;
}

function useRepositorySelection() {
  const [repository, setRepository] = useState(() => { try { return window.localStorage.getItem(storageKey) ?? ""; } catch { return ""; } });
  useEffect(() => { try { repository ? window.localStorage.setItem(storageKey, repository) : window.localStorage.removeItem(storageKey); } catch { /* unavailable */ } }, [repository]);
  return [repository, setRepository] as const;
}
function RepositoryPicker({ value, onChange }: { value: string; onChange: (value: string) => void }) {
  const repositories = useQuery({ queryKey: ["repositories"], queryFn: () => json<string[]>("/api/repositories"), staleTime: 60_000 }).data ?? [];
  return <label className="repository-picker"><span>Repository</span><select aria-label="Dashboard repository" value={value} onChange={(event) => onChange(event.target.value)}><option value="">All repositories</option>{repositories.map((repository) => <option key={repository}>{repository}</option>)}</select></label>;
}

function IssuesPage() {
  const [repository, setRepository] = useRepositorySelection();
  const workflowsQuery = useQuery({ queryKey: ["workflows", repository], queryFn: () => json<Workflow[]>(`/api/workflows${repositoryQuery(repository)}`), refetchInterval: 5_000 });
  const workflows = workflowsQuery.data ?? [];
  const counts = { active: workflows.filter((item) => ["in_progress", "ready"].includes(item.current_state ?? "")).length, attention: workflows.filter((item) => item.current_state === "needs_human").length, blocked: workflows.filter((item) => item.current_state === "blocked").length, prs: workflows.filter((item) => item.current_state === "pr_open").length };
  return <><PageHeading title="Issue workflows" subtitle="Current work, decisions, and end results." picker={<RepositoryPicker value={repository} onChange={setRepository} />} />
    <section className="summary-grid"><Metric label="Active issues" value={counts.active} /><Metric label="Needs attention" value={counts.attention} /><Metric label="Blocked or failed" value={counts.blocked} /><Metric label="Pull requests open" value={counts.prs} /></section>
    <section className="workflow-grid">{workflows.map((workflow) => <WorkflowCard key={workflow.id} workflow={workflow} />)}{!workflows.length ? <div className="empty-state panel">{workflowsQuery.isPending ? "Loading issue workflows…" : "No issue workflows for this repository."}</div> : null}</section>
  </>;
}
function Metric({ label, value }: { label: string; value: number }) { return <div><span>{label}</span><strong>{value}</strong></div>; }
function WorkflowCard({ workflow }: { workflow: Workflow }) {
  const active = workflow.tasks.filter((task) => ["running", "leased"].includes(task.status));
  const href = `/repositories/${encodeURIComponent(workflow.owner)}/${encodeURIComponent(workflow.repository)}/issues/${workflow.issue_number}`;
  return <article className="workflow-card"><div className="workflow-card-heading"><div><span className="eyebrow">{workflow.owner}/{workflow.repository} · Issue #{workflow.issue_number}</span><h3><a href={href}>{workflow.issue_title}</a></h3></div><StatusPill status={workflow.current_state ?? workflow.coordinator_status ?? "unknown"} /></div>
    <p className="workflow-summary">{workflow.summary ?? "Agents have not reported an outcome yet."}</p>
    {active.length ? <div className="agent-strip"><strong>Running now</strong>{active.map((task) => <span key={task.job_id}>{task.task_display_name}{task.work_item ? ` / ${task.work_item}` : ""}</span>)}</div> : null}
    {workflow.pending_approval ? <div className="attention-box"><strong>Decision required</strong><p>{firstParagraph(workflow.pending_approval)}</p></div> : null}
    <div className="workflow-result">{workflow.pull_request_url ? <a href={workflow.pull_request_url} target="_blank" rel="noreferrer">Pull request{workflow.pull_request_number ? ` #${workflow.pull_request_number}` : ""} · {workflow.pull_request_state ?? "open"}</a> : <span><strong>Why no PR:</strong> {workflow.no_pr_reason}</span>}<time title={new Date(workflow.updated_at).toLocaleString()}>{relativeTime(workflow.updated_at)}</time></div>
  </article>;
}

function WorkflowDetail({ owner, repo, number }: { owner: string; repo: string; number: number }) {
  const [showDetails, setShowDetails] = useState(false);
  const workflowQuery = useQuery({ queryKey: ["workflow", owner, repo, number], queryFn: () => json<Workflow>(`/api/workflows/${encodeURIComponent(owner)}/${encodeURIComponent(repo)}/issues/${number}`), refetchInterval: 5_000 });
  const eventsQuery = useQuery({ queryKey: ["workflow-events", owner, repo, number, showDetails], queryFn: () => json<EventPage>(`/api/workflows/${encodeURIComponent(owner)}/${encodeURIComponent(repo)}/issues/${number}/events?level=${showDetails ? "all" : "milestone"}&limit=200`), refetchInterval: 5_000 });
  const workflow = workflowQuery.data;
  if (!workflow) return <div className="empty-state panel">{workflowQuery.isError ? "Workflow could not be loaded." : "Loading workflow…"}</div>;
  const events = [...(eventsQuery.data?.events ?? [])].reverse();
  return <><a className="back-link" href="/">← All issues</a><section className="detail-hero panel"><div><span className="eyebrow">{owner}/{repo} · Issue #{number}</span><h2>{workflow.issue_title}</h2><p>{workflow.summary}</p></div><div className="detail-state"><StatusPill status={workflow.current_state ?? "unknown"} /><a href={workflow.issue_url} target="_blank" rel="noreferrer">Open on GitHub</a></div></section>
    <section className="detail-grid"><section className="panel"><PanelHeading title="Current work" subtitle="Task state is independent per agent." /><div className="task-list">{workflow.tasks.map((task) => <TaskRow key={task.job_id} task={task} />)}{!workflow.tasks.length ? <div className="empty-state">Planning is the only active phase.</div> : null}</div></section>
      <section className="panel"><PanelHeading title="Outcome" subtitle="Latest aggregate lifecycle result." /><div className="outcome-content"><dl><div><dt>Outcome</dt><dd>{workflow.outcome ?? "pending"}</dd></div><div><dt>Coordinator</dt><dd>{workflow.coordinator_status ?? "unknown"}</dd></div></dl>{workflow.pending_approval ? <div className="attention-box"><strong>Decision required</strong><pre>{workflow.pending_approval}</pre></div> : null}{workflow.pull_request_url ? <a className="primary-link" href={workflow.pull_request_url} target="_blank" rel="noreferrer">Open final pull request</a> : <p><strong>Why no PR:</strong> {workflow.no_pr_reason}</p>}</div></section>
    </section>
    <section className="panel timeline-panel"><div className="panel-header"><div><h2>Lifecycle timeline</h2><span>Triggers, agent waves, handoffs, decisions, artifacts, and end results.</span></div><label className="detail-toggle"><input type="checkbox" checked={showDetails} onChange={(event) => setShowDetails(event.target.checked)} /> Show scheduling details</label></div><div className="timeline">{events.map((event) => <TimelineRow event={event} key={event.id} />)}{!events.length ? <div className="empty-state">Timeline tracking began after this workflow was created, or no milestone has been recorded yet.</div> : null}</div></section>
  </>;
}
function TaskRow({ task }: { task: WorkflowTask }) { return <article className="task-row"><div><strong>{task.task_display_name}{task.work_item ? ` / ${task.work_item}` : ""}</strong><span>{task.role_display_name}</span><p>{task.summary ?? "Waiting for an agent result."}</p></div><div><StatusPill status={task.status} /><small>{task.outcome ?? "pending"}</small></div></article>; }
function TimelineRow({ event }: { event: TimelineEvent }) {
  const label = event.task_display_name ?? event.role_display_name ?? humanize(event.event_type);
  return <article className="timeline-row" data-level={event.level}><div className="timeline-marker" /><time title={new Date(event.created_at).toLocaleString()}>{relativeTime(event.created_at)}</time><div><div className="timeline-heading"><strong>{label}{event.work_item ? ` / ${event.work_item}` : ""}</strong>{event.wave ? <span>Wave {event.wave}</span> : null}{["poll", "webhook"].includes(event.source) ? <span>{event.source}</span> : null}</div><p>{event.summary}</p>{event.reason && event.reason !== event.summary ? <details><summary>Reason</summary><pre>{event.reason}</pre></details> : null}{event.handoff_target ? <p className="handoff">Handoff to {event.handoff_target}</p> : null}{event.links?.length ? <div className="event-links">{event.links.map((link) => <a key={link.url} href={link.url} target="_blank" rel="noreferrer">{link.label}</a>)}</div> : null}</div></article>;
}

function OperationsPage() {
  const [repository, setRepository] = useRepositorySelection();
  const runs = useQuery({ queryKey: ["runs", repository], queryFn: () => json<Run[]>(`/api/runs${repositoryQuery(repository)}`), refetchInterval: 10_000 });
  const actions = useQuery({ queryKey: ["outbound-actions", repository], queryFn: () => json<OutboundAction[]>(`/api/outbound-actions${repositoryQuery(repository)}`), refetchInterval: 10_000 });
  const poll = useQuery({ queryKey: ["poll-status"], queryFn: () => json<PollStatus>("/api/github-poll/status"), refetchInterval: 5_000 });
  return <><PageHeading title="Operations" subtitle="Polling controls, raw jobs, and GitHub delivery diagnostics." picker={<RepositoryPicker value={repository} onChange={setRepository} />} /><PollingPanel status={poll.data} />
    <section className="panel"><PanelHeading title="Raw jobs" subtitle={repository || "All repositories"} /><div className="operations-list">{(runs.data ?? []).map((run) => <article key={run.id}><div><strong>{run.input?.issue?.title ?? "Untitled issue"}</strong><code>{run.id}</code><p>{run.result?.summary ?? `Issue #${run.input?.issue?.number ?? "?"}`}</p></div><div><StatusPill status={run.status} /><small>{run.role} · {run.result?.outcome ?? "pending"}</small></div></article>)}</div></section>
    <section className="panel"><PanelHeading title="GitHub action outbox" subtitle="Pending and completed writes" /><div className="operations-list">{(actions.data ?? []).map((action) => <article key={action.id}><div><strong>{action.action_type}</strong><p>{action.last_error ?? `${action.payload.owner ?? "?"}/${action.payload.repo ?? "?"} #${action.payload.issue_number ?? "?"}`}</p></div><StatusPill status={action.status} /></article>)}</div></section>
  </>;
}
function PollingPanel({ status }: { status?: PollStatus }) {
  const queryClient = useQueryClient(); const trigger = useMutation({ mutationFn: () => json<unknown>("/api/github-poll/trigger", { method: "POST" }), onSuccess: () => queryClient.invalidateQueries({ queryKey: ["poll-status"] }) }); const [now, setNow] = useState(Date.now());
  useEffect(() => { const timer = window.setInterval(() => setNow(Date.now()), 1_000); return () => window.clearInterval(timer); }, []);
  return <section className="panel polling-panel"><PanelHeading title="GitHub polling" subtitle={status?.running ? "Polling now" : status?.enabled ? "Enabled" : "Disabled"} /><div className="polling-content"><dl><div><dt>Since last poll</dt><dd>{elapsed(status?.last_completed_at, now)}</dd></div><div><dt>Until next poll</dt><dd>{countdown(status?.next_poll_at, now)}</dd></div><div><dt>Interval</dt><dd>{status?.configured_interval_seconds ?? "—"}s</dd></div></dl><button disabled={trigger.isPending || status?.running} onClick={() => trigger.mutate()}>{trigger.isPending ? "Requesting…" : status?.running ? "Polling…" : "Poll now"}</button></div></section>;
}

function PageHeading({ title, subtitle, picker }: { title: string; subtitle: string; picker: React.ReactNode }) { return <section className="page-heading"><div><h2>{title}</h2><p>{subtitle}</p></div>{picker}</section>; }
function PanelHeading({ title, subtitle }: { title: string; subtitle: string }) { return <div className="panel-header"><div><h2>{title}</h2><span>{subtitle}</span></div></div>; }
function StatusPill({ status }: { status: string }) { return <span className="status-pill" data-status={status}>{humanize(status)}</span>; }
function humanize(value: string) { return value.replaceAll("_", " ").replace(/\b\w/g, (letter) => letter.toUpperCase()); }
function firstParagraph(value: string) { return value.split("\n\n")[0]; }
function relativeTime(value: string) { const seconds = Math.max(0, Math.floor((Date.now() - Date.parse(value)) / 1000)); if (seconds < 60) return `${seconds}s ago`; if (seconds < 3600) return `${Math.floor(seconds / 60)}m ago`; if (seconds < 86400) return `${Math.floor(seconds / 3600)}h ago`; return `${Math.floor(seconds / 86400)}d ago`; }
function elapsed(value: string | null | undefined, now: number) { return value ? duration(Math.max(0, now - Date.parse(value))) : "not yet"; }
function countdown(value: string | null | undefined, now: number) { if (!value) return "unknown"; const remaining = Date.parse(value) - now; return remaining <= 0 ? "due now" : duration(remaining); }
function duration(ms: number) { const total = Math.floor(ms / 1000); const minutes = Math.floor(total / 60); return minutes ? `${minutes}m ${total % 60}s` : `${total}s`; }
