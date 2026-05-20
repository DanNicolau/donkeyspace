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

CREATE TABLE IF NOT EXISTS jobs (
    id UUID PRIMARY KEY,
    workflow_item_id BIGINT REFERENCES workflow_items(id),
    role TEXT NOT NULL,
    status TEXT NOT NULL,
    lease_owner TEXT,
    lease_expires_at TIMESTAMPTZ,
    input JSONB NOT NULL DEFAULT '{}'::jsonb,
    result JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS jobs_status_idx ON jobs(status);
CREATE INDEX IF NOT EXISTS jobs_lease_expires_at_idx ON jobs(lease_expires_at);

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
    managed_by_donkeyspace BOOLEAN NOT NULL DEFAULT false,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (repository_id, provider_pr_id)
);

CREATE INDEX IF NOT EXISTS pull_requests_workflow_item_id_idx ON pull_requests(workflow_item_id);

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

CREATE INDEX IF NOT EXISTS outbound_actions_status_idx ON outbound_actions(status);
CREATE INDEX IF NOT EXISTS outbound_actions_job_id_idx ON outbound_actions(job_id);
