-- Retry state survives worker restarts; no workflow history is deleted.
CREATE TABLE IF NOT EXISTS repository_label_sync (
    repository TEXT PRIMARY KEY,
    labels JSONB NOT NULL,
    attempt_id UUID NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 1 CHECK (attempts BETWEEN 0 AND 7),
    next_attempt_at TIMESTAMPTZ NOT NULL,
    last_success_at TIMESTAMPTZ,
    last_error TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
