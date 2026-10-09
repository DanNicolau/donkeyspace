-- Continuation state lives outside agent-controlled repository files. Each
-- revision gets a new execution identity while retaining its workflow lineage.
CREATE TABLE IF NOT EXISTS lifecycle_snapshots (
    job_id UUID PRIMARY KEY REFERENCES jobs(id),
    checkpoint JSONB NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS pr_revisions (
    id BIGSERIAL PRIMARY KEY,
    repository_id BIGINT NOT NULL REFERENCES repositories(id),
    workflow_item_id BIGINT NOT NULL REFERENCES workflow_items(id),
    pr_number BIGINT NOT NULL,
    comment_id BIGINT NOT NULL,
    source_job_id UUID NOT NULL REFERENCES jobs(id),
    job_id UUID NOT NULL UNIQUE REFERENCES jobs(id),
    request JSONB NOT NULL,
    head_sha TEXT NOT NULL,
    publication_sha TEXT,
    status TEXT NOT NULL DEFAULT 'queued',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (repository_id, comment_id)
);

-- Keep execution outcomes visible without losing the durable publication
-- intent needed to reconcile a crash between git push and acknowledgment.
CREATE OR REPLACE FUNCTION reflect_pr_revision_status() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    UPDATE pr_revisions SET status=NEW.status,updated_at=now()
    WHERE job_id=NEW.id AND status NOT IN ('published','publishing');
    RETURN NEW;
END $$;
DROP TRIGGER IF EXISTS reflect_pr_revision_status ON jobs;
CREATE TRIGGER reflect_pr_revision_status AFTER UPDATE OF status ON jobs
FOR EACH ROW EXECUTE FUNCTION reflect_pr_revision_status();
