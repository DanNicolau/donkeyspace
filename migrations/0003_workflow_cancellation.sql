ALTER TABLE workflow_items ADD COLUMN IF NOT EXISTS provider_close_reason TEXT;
ALTER TABLE workflow_items ADD COLUMN IF NOT EXISTS provider_updated_at TIMESTAMPTZ;
ALTER TABLE workflow_items ADD COLUMN IF NOT EXISTS generation BIGINT NOT NULL DEFAULT 1;
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS generation BIGINT NOT NULL DEFAULT 1;
ALTER TABLE outbound_actions ADD COLUMN IF NOT EXISTS generation BIGINT NOT NULL DEFAULT 1;

-- Generation stamping fences old coordinators after a reopen, including inserts
-- racing with closure. Dispatch also revalidates against the locked workflow.
CREATE OR REPLACE FUNCTION fence_workflow_job() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE w workflow_items; parent_generation BIGINT; parent_status TEXT;
BEGIN
    IF TG_OP = 'UPDATE' AND OLD.status IN ('cancel_requested', 'cancelled') THEN
        NEW.status := CASE WHEN NEW.status = 'cancelled' THEN 'cancelled' ELSE OLD.status END;
        NEW.result := OLD.result;
        NEW.input := NEW.input || jsonb_build_object('donkeyspace_workflow_generation', NEW.generation);
        RETURN NEW;
    END IF;
    -- A crashed coordinator may have an in-flight child insert/update. An open
    -- issue alone cannot authorize work for an already fenced parent.
    IF TG_OP='INSERT' THEN
        -- Serialize child admission with parent fencing. The fence reads its
        -- children after acquiring the parent lock, so it sees this commit.
        SELECT status INTO parent_status FROM jobs
            WHERE id = (NEW.input #>> '{plugin_execution,coordinator_run_id}')::uuid
            FOR SHARE;
    ELSE
        -- UPDATE already holds the child row; do not invert parent/child locks.
        SELECT status INTO parent_status FROM jobs
            WHERE id = (NEW.input #>> '{plugin_execution,coordinator_run_id}')::uuid;
    END IF;
    IF parent_status IN ('cancel_requested','cancelled') THEN
        NEW.status := CASE WHEN TG_OP='UPDATE' AND OLD.status='running'
            THEN 'cancel_requested' ELSE 'cancelled' END;
        NEW.lease_owner := NULL;
        NEW.lease_expires_at := NULL;
    END IF;
    IF NEW.workflow_item_id IS NULL THEN RETURN NEW; END IF;
    SELECT * INTO w FROM workflow_items WHERE id = NEW.workflow_item_id;
    IF TG_OP = 'INSERT' THEN
        parent_generation := (NEW.input->>'donkeyspace_workflow_generation')::bigint;
        NEW.generation := COALESCE(parent_generation, w.generation);
    END IF;
    IF (w.provider_state = 'closed' OR NEW.generation <> w.generation)
        AND NEW.status IN ('waiting', 'queued', 'leased', 'running', 'paused') THEN
        NEW.status := CASE WHEN TG_OP='UPDATE' AND OLD.status='running' THEN 'cancel_requested' ELSE 'cancelled' END;
        NEW.lease_owner := NULL;
        NEW.lease_expires_at := NULL;
    END IF;
    NEW.input := NEW.input || jsonb_build_object('donkeyspace_workflow_generation', NEW.generation);
    RETURN NEW;
END $$;
DROP TRIGGER IF EXISTS fence_workflow_job ON jobs;
CREATE TRIGGER fence_workflow_job BEFORE INSERT OR UPDATE ON jobs
FOR EACH ROW EXECUTE FUNCTION fence_workflow_job();

CREATE OR REPLACE FUNCTION fence_workflow_outbound() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE w workflow_items; j jobs;
BEGIN
    SELECT * INTO w FROM workflow_items WHERE id = NEW.workflow_item_id;
    IF NEW.job_id IS NOT NULL THEN SELECT * INTO j FROM jobs WHERE id = NEW.job_id; END IF;
    IF TG_OP = 'INSERT' THEN NEW.generation := COALESCE(j.generation, w.generation); END IF;
    IF NEW.status = 'pending' AND (
        NEW.generation <> w.generation OR j.status IN ('cancel_requested', 'cancelled')
        OR (w.provider_state = 'closed' AND NOT (
            NEW.job_id IS NULL AND NEW.action_type = 'issue.remove_labels'
            AND COALESCE(NEW.payload->>'closure_cleanup' = 'true', false)
        ))
    ) THEN NEW.status := 'cancelled'; END IF;
    RETURN NEW;
END $$;
DROP TRIGGER IF EXISTS fence_workflow_outbound ON outbound_actions;
CREATE TRIGGER fence_workflow_outbound BEFORE INSERT OR UPDATE ON outbound_actions
FOR EACH ROW EXECUTE FUNCTION fence_workflow_outbound();

ALTER TABLE pull_requests ADD COLUMN IF NOT EXISTS generation BIGINT NOT NULL DEFAULT 1;
CREATE OR REPLACE FUNCTION stamp_pull_request_generation() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.workflow_item_id IS NOT NULL THEN
        SELECT generation INTO NEW.generation FROM workflow_items WHERE id=NEW.workflow_item_id;
    END IF;
    RETURN NEW;
END $$;
DROP TRIGGER IF EXISTS stamp_pull_request_generation ON pull_requests;
CREATE TRIGGER stamp_pull_request_generation BEFORE INSERT ON pull_requests
FOR EACH ROW EXECUTE FUNCTION stamp_pull_request_generation();

CREATE OR REPLACE FUNCTION fence_agent_publication() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE w workflow_items; j jobs;
BEGIN
    IF NEW.workflow_item_id IS NULL THEN RETURN NEW; END IF;
    SELECT * INTO w FROM workflow_items WHERE id=NEW.workflow_item_id;
    SELECT * INTO j FROM jobs WHERE id=NEW.coordinator_job_id;
    IF NEW.status IN ('pending','failed') AND (w.provider_state='closed'
        OR w.generation <> j.generation OR j.status IN ('cancel_requested','cancelled')) THEN
        NEW.status := 'cancelled';
    END IF;
    RETURN NEW;
END $$;
DROP TRIGGER IF EXISTS fence_agent_publication ON agent_publications;
CREATE TRIGGER fence_agent_publication BEFORE INSERT OR UPDATE ON agent_publications
FOR EACH ROW EXECUTE FUNCTION fence_agent_publication();

CREATE OR REPLACE FUNCTION fence_workflow_approval() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE w workflow_items; j jobs;
BEGIN
    SELECT * INTO w FROM workflow_items WHERE id=NEW.workflow_item_id;
    SELECT * INTO j FROM jobs WHERE id=NEW.coordinator_job_id;
    IF NEW.state='pending' AND (w.provider_state='closed'
        OR w.generation <> j.generation OR j.status IN ('cancel_requested','cancelled')) THEN
        NEW.state := 'cancelled';
    END IF;
    RETURN NEW;
END $$;
DROP TRIGGER IF EXISTS fence_workflow_approval ON approval_requests;
CREATE TRIGGER fence_workflow_approval BEFORE INSERT OR UPDATE ON approval_requests
FOR EACH ROW EXECUTE FUNCTION fence_workflow_approval();

UPDATE jobs SET input=input || jsonb_build_object('donkeyspace_workflow_generation',generation)
WHERE NOT input ? 'donkeyspace_workflow_generation';
