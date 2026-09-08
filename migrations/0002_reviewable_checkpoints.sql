ALTER TABLE agent_publications ADD COLUMN IF NOT EXISTS base_sha TEXT;
ALTER TABLE agent_publications ADD COLUMN IF NOT EXISTS commit_url TEXT;
ALTER TABLE agent_publications ADD COLUMN IF NOT EXISTS compare_url TEXT;
ALTER TABLE agent_publications ADD COLUMN IF NOT EXISTS changed_files JSONB NOT NULL DEFAULT '[]'::jsonb;
ALTER TABLE agent_publications ADD COLUMN IF NOT EXISTS task_scopes JSONB NOT NULL DEFAULT '[]'::jsonb;
ALTER TABLE agent_publications ADD COLUMN IF NOT EXISTS zero_diff BOOLEAN NOT NULL DEFAULT false;
ALTER TABLE agent_publications ADD COLUMN IF NOT EXISTS retry_count INTEGER NOT NULL DEFAULT 0;
ALTER TABLE agent_publications ADD COLUMN IF NOT EXISTS next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now();

ALTER TABLE agent_publications
    DROP CONSTRAINT IF EXISTS agent_publications_coordinator_job_id_branch_name_key;
CREATE UNIQUE INDEX IF NOT EXISTS agent_publications_checkpoint_idx
    ON agent_publications(coordinator_job_id, branch_name, commit_sha);

UPDATE agent_publications
SET commit_url = format(
    'https://github.com/%s/%s/commit/%s',
    metadata->>'owner', metadata->>'repo', commit_sha
)
WHERE metadata->>'owner' IS NOT NULL
  AND metadata->>'repo' IS NOT NULL;

CREATE TABLE IF NOT EXISTS approval_requests (
    id BIGSERIAL PRIMARY KEY,
    workflow_item_id BIGINT NOT NULL REFERENCES workflow_items(id),
    coordinator_job_id UUID NOT NULL REFERENCES jobs(id),
    target_task TEXT NOT NULL,
    target_work_item TEXT,
    purpose TEXT NOT NULL DEFAULT 'accept_result',
    trigger TEXT NOT NULL,
    approval_subject TEXT NOT NULL,
    result_summary TEXT NOT NULL,
    changed_files JSONB NOT NULL DEFAULT '[]'::jsonb,
    proposed_publication_id BIGINT REFERENCES agent_publications(id),
    accepted_publication_id BIGINT REFERENCES agent_publications(id),
    projected_issues JSONB NOT NULL DEFAULT '[]'::jsonb,
    downstream_tasks JSONB NOT NULL DEFAULT '[]'::jsonb,
    state TEXT NOT NULL DEFAULT 'pending',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX IF NOT EXISTS approval_requests_pending_target_idx
    ON approval_requests(coordinator_job_id, target_task, COALESCE(target_work_item, ''))
    WHERE state = 'pending';

CREATE TABLE IF NOT EXISTS projected_work_items (
    id BIGSERIAL PRIMARY KEY,
    workflow_item_id BIGINT NOT NULL REFERENCES workflow_items(id),
    coordinator_job_id UUID NOT NULL REFERENCES jobs(id),
    work_item TEXT NOT NULL,
    issue_id TEXT,
    issue_number BIGINT,
    spec_path TEXT NOT NULL,
    body_digest TEXT NOT NULL,
    managed_dependencies JSONB NOT NULL DEFAULT '[]'::jsonb,
    accepted_publication_id BIGINT REFERENCES agent_publications(id),
    proposed_publication_id BIGINT REFERENCES agent_publications(id),
    sync_status TEXT NOT NULL DEFAULT 'pending',
    desired_revision BIGINT NOT NULL DEFAULT 1,
    applied_revision BIGINT NOT NULL DEFAULT 0,
    retry_count INTEGER NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_error TEXT,
    accepted BOOLEAN NOT NULL DEFAULT false,
    proposed_removal BOOLEAN NOT NULL DEFAULT false,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (coordinator_job_id, work_item)
);
CREATE INDEX IF NOT EXISTS projected_work_items_sync_idx
    ON projected_work_items(sync_status, next_attempt_at);
