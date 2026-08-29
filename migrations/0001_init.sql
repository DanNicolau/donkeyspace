CREATE TABLE IF NOT EXISTS installations (
    id BIGSERIAL PRIMARY KEY,
    provider TEXT NOT NULL,
    external_id TEXT NOT NULL,
    account_login TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (provider, external_id)
);

CREATE TABLE IF NOT EXISTS repositories (
    id BIGSERIAL PRIMARY KEY,
    installation_id BIGINT REFERENCES installations(id),
    provider TEXT NOT NULL,
    owner TEXT NOT NULL,
    name TEXT NOT NULL,
    default_branch TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (provider, owner, name)
);

CREATE TABLE IF NOT EXISTS workflow_items (
    id BIGSERIAL PRIMARY KEY,
    repository_id BIGINT NOT NULL REFERENCES repositories(id),
    provider_issue_id TEXT NOT NULL,
    issue_number BIGINT NOT NULL,
    provider_state TEXT NOT NULL DEFAULT 'open',
    current_state TEXT,
    current_labels JSONB NOT NULL DEFAULT '[]'::jsonb,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (repository_id, provider_issue_id)
);

ALTER TABLE workflow_items ADD COLUMN IF NOT EXISTS provider_state TEXT NOT NULL DEFAULT 'open';

CREATE TABLE IF NOT EXISTS webhook_deliveries (
    id BIGSERIAL PRIMARY KEY,
    repository_id BIGINT REFERENCES repositories(id),
    delivery_id TEXT NOT NULL UNIQUE,
    event_name TEXT NOT NULL,
    payload JSONB NOT NULL,
    received_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS engagement_decisions (
    id BIGSERIAL PRIMARY KEY,
    webhook_delivery_id BIGINT NOT NULL UNIQUE REFERENCES webhook_deliveries(id),
    workflow_item_id BIGINT REFERENCES workflow_items(id),
    gate TEXT NOT NULL,
    disposition TEXT NOT NULL,
    actor JSONB,
    matched_selector JSONB,
    reason TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS engagement_decisions_workflow_item_id_idx
    ON engagement_decisions(workflow_item_id);

CREATE TABLE IF NOT EXISTS github_managed_resources (
    id BIGSERIAL PRIMARY KEY,
    repository_id BIGINT NOT NULL REFERENCES repositories(id),
    workflow_item_id BIGINT REFERENCES workflow_items(id),
    outbound_action_id BIGINT,
    resource_kind TEXT NOT NULL,
    provider_id TEXT NOT NULL,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (repository_id, resource_kind, provider_id)
);

CREATE TABLE IF NOT EXISTS jobs (
    id UUID PRIMARY KEY,
    workflow_item_id BIGINT REFERENCES workflow_items(id),
    retry_of_job_id UUID REFERENCES jobs(id),
    role TEXT NOT NULL,
    status TEXT NOT NULL,
    lease_owner TEXT,
    lease_expires_at TIMESTAMPTZ,
    input JSONB NOT NULL DEFAULT '{}'::jsonb,
    result JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

ALTER TABLE jobs ADD COLUMN IF NOT EXISTS retry_of_job_id UUID REFERENCES jobs(id);
CREATE INDEX IF NOT EXISTS jobs_status_idx ON jobs(status);
CREATE INDEX IF NOT EXISTS jobs_lease_expires_at_idx ON jobs(lease_expires_at);
CREATE INDEX IF NOT EXISTS jobs_retry_of_job_id_idx ON jobs(retry_of_job_id);

CREATE TABLE IF NOT EXISTS state_transitions (
    id BIGSERIAL PRIMARY KEY,
    workflow_item_id BIGINT NOT NULL REFERENCES workflow_items(id),
    job_id UUID REFERENCES jobs(id),
    from_state TEXT,
    to_state TEXT NOT NULL,
    reason TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS pull_requests (
    id BIGSERIAL PRIMARY KEY,
    repository_id BIGINT NOT NULL REFERENCES repositories(id),
    workflow_item_id BIGINT REFERENCES workflow_items(id),
    provider_pr_id TEXT NOT NULL,
    pr_number BIGINT NOT NULL,
    title TEXT NOT NULL,
    html_url TEXT NOT NULL,
    state TEXT NOT NULL,
    head_ref TEXT NOT NULL,
    head_sha TEXT,
    base_ref TEXT NOT NULL,
    base_sha TEXT,
    managed_by_donkeyspace BOOLEAN NOT NULL DEFAULT false,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (repository_id, provider_pr_id)
);

CREATE INDEX IF NOT EXISTS pull_requests_workflow_item_id_idx ON pull_requests(workflow_item_id);

ALTER TABLE pull_requests ADD COLUMN IF NOT EXISTS base_sha TEXT;

CREATE TABLE IF NOT EXISTS policy_snapshots (
    id UUID PRIMARY KEY,
    repository_id BIGINT NOT NULL REFERENCES repositories(id),
    source_path TEXT NOT NULL,
    content JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS command_results (
    id BIGSERIAL PRIMARY KEY,
    job_id UUID NOT NULL REFERENCES jobs(id),
    name TEXT NOT NULL,
    command JSONB NOT NULL,
    status TEXT NOT NULL,
    exit_code INTEGER,
    summary TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS outbound_actions (
    id BIGSERIAL PRIMARY KEY,
    workflow_item_id BIGINT NOT NULL REFERENCES workflow_items(id),
    job_id UUID REFERENCES jobs(id),
    provider TEXT NOT NULL,
    action_type TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    payload JSONB NOT NULL,
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

ALTER TABLE outbound_actions ADD COLUMN IF NOT EXISTS last_error TEXT;
ALTER TABLE outbound_actions ADD COLUMN IF NOT EXISTS provider_resource_id TEXT;
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'github_managed_resources_outbound_action_id_fkey'
    ) THEN
        ALTER TABLE github_managed_resources
            ADD CONSTRAINT github_managed_resources_outbound_action_id_fkey
            FOREIGN KEY (outbound_action_id) REFERENCES outbound_actions(id);
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS outbound_actions_status_idx ON outbound_actions(status);
CREATE INDEX IF NOT EXISTS outbound_actions_job_id_idx ON outbound_actions(job_id);

CREATE TABLE IF NOT EXISTS agent_publications (
    id BIGSERIAL PRIMARY KEY,
    coordinator_job_id UUID NOT NULL REFERENCES jobs(id),
    job_id UUID REFERENCES jobs(id),
    workflow_item_id BIGINT REFERENCES workflow_items(id),
    kind TEXT NOT NULL,
    branch_name TEXT NOT NULL,
    commit_sha TEXT NOT NULL,
    html_url TEXT NOT NULL,
    local_repo_path TEXT NOT NULL,
    task TEXT,
    work_item TEXT,
    attempt INTEGER,
    outcome TEXT,
    status TEXT NOT NULL DEFAULT 'pending',
    last_error TEXT,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (coordinator_job_id, branch_name)
);

CREATE INDEX IF NOT EXISTS agent_publications_coordinator_idx
    ON agent_publications(coordinator_job_id, created_at);
CREATE INDEX IF NOT EXISTS agent_publications_job_idx
    ON agent_publications(job_id, created_at);
CREATE INDEX IF NOT EXISTS agent_publications_status_idx
    ON agent_publications(status, updated_at);

-- Immutable, user-facing history for a workflow item.  Mutable job rows remain
-- the scheduler authority; this table records the ordered semantic sequence
-- which produced their current state.
CREATE TABLE IF NOT EXISTS lifecycle_events (
    id BIGSERIAL PRIMARY KEY,
    workflow_item_id BIGINT NOT NULL REFERENCES workflow_items(id),
    coordinator_job_id UUID REFERENCES jobs(id),
    job_id UUID REFERENCES jobs(id),
    dedupe_key TEXT,
    event_type TEXT NOT NULL,
    level TEXT NOT NULL DEFAULT 'milestone',
    source TEXT NOT NULL DEFAULT 'worker',
    actor TEXT,
    wave INTEGER,
    attempt INTEGER,
    role TEXT,
    role_display_name TEXT,
    task TEXT,
    task_display_name TEXT,
    work_item TEXT,
    status TEXT,
    outcome TEXT,
    summary TEXT NOT NULL,
    reason TEXT,
    handoff_target TEXT,
    links JSONB NOT NULL DEFAULT '[]'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (workflow_item_id, dedupe_key)
);

CREATE INDEX IF NOT EXISTS lifecycle_events_workflow_order_idx
    ON lifecycle_events(workflow_item_id, id DESC);
CREATE INDEX IF NOT EXISTS lifecycle_events_coordinator_idx
    ON lifecycle_events(coordinator_job_id, id DESC);
CREATE INDEX IF NOT EXISTS lifecycle_events_task_idx
    ON lifecycle_events(workflow_item_id, task, work_item, id DESC);

ALTER TABLE outbound_actions ADD COLUMN IF NOT EXISTS dedupe_key TEXT;
CREATE UNIQUE INDEX IF NOT EXISTS outbound_actions_pending_dedupe_idx
    ON outbound_actions(dedupe_key)
    WHERE status = 'pending' AND dedupe_key IS NOT NULL;
