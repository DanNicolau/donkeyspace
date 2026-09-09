-- Launch intents, committed before contacting Docker. A row does not prove that
-- creation succeeded or that the container is still running. Retain identities
-- after cleanup so a future reconciler can inspect uncertain/late creations.
CREATE TABLE IF NOT EXISTS container_executions (
    id UUID PRIMARY KEY,
    coordinator_job_id UUID NOT NULL REFERENCES jobs(id),
    workflow_item_id BIGINT REFERENCES workflow_items(id),
    generation BIGINT NOT NULL,
    lease_owner TEXT NOT NULL,
    container_name TEXT NOT NULL UNIQUE,
    execution_scope TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS container_executions_coordinator_idx
    ON container_executions(coordinator_job_id);
