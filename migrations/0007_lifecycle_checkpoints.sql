-- Additive and intentionally retained after completion. A completed row prevents
-- accidentally importing an older filesystem checkpoint on a subsequent resume.
CREATE TABLE IF NOT EXISTS lifecycle_checkpoints (
    coordinator_job_id UUID PRIMARY KEY REFERENCES jobs(id),
    generation BIGINT NOT NULL,
    revision BIGINT NOT NULL CHECK (revision > 0),
    version INTEGER NOT NULL,
    state JSONB NOT NULL,
    completed BOOLEAN NOT NULL DEFAULT false,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
