-- Legacy intents have unknown daemon ownership: never infer absence remotely.
ALTER TABLE container_executions ADD COLUMN IF NOT EXISTS docker_daemon_id TEXT;
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS recovery_requested_at TIMESTAMPTZ;
CREATE TABLE IF NOT EXISTS container_cleanup_observations (
    execution_id UUID PRIMARY KEY REFERENCES container_executions(id),
    checked_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    error TEXT
);
