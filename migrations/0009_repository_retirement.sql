ALTER TABLE repositories ADD COLUMN IF NOT EXISTS retired_at TIMESTAMPTZ;

ALTER TABLE outbound_actions ADD COLUMN IF NOT EXISTS retry_count INTEGER NOT NULL DEFAULT 0;
ALTER TABLE outbound_actions ADD COLUMN IF NOT EXISTS next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now();

CREATE OR REPLACE FUNCTION workflow_repository_tracked(workflow BIGINT) RETURNS BOOLEAN
LANGUAGE sql STABLE AS $$
    SELECT NOT EXISTS (SELECT 1 FROM workflow_items w JOIN repositories r ON r.id=w.repository_id
        WHERE w.id=workflow AND r.retired_at IS NOT NULL)
$$;

-- This is local tracking state, never a claim that GitHub deleted a repository.
-- Late inserts are fenced even if ingress prepared them before selection changed.
CREATE OR REPLACE FUNCTION fence_retired_repository() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE retired BOOLEAN; reason TEXT := 'Repository removed from tracking; history retained, automatic replay disabled';
BEGIN
    retired := NOT workflow_repository_tracked(NEW.workflow_item_id);
    IF TG_TABLE_NAME='jobs' AND NEW.workflow_item_id IS NULL THEN
        SELECT EXISTS (SELECT 1 FROM repositories r WHERE r.provider='github' AND r.retired_at IS NOT NULL
            AND lower(r.owner)=lower(NEW.input #>> '{repository,owner,login}')
            AND lower(r.name)=lower(NEW.input #>> '{repository,name}')) INTO retired;
    END IF;
    IF NOT retired THEN RETURN NEW; END IF;
    IF TG_TABLE_NAME='jobs' THEN
        IF NEW.status IN ('waiting','queued','leased','running','paused') THEN
            NEW.status := CASE WHEN TG_OP='UPDATE' AND OLD.status='running' THEN 'cancel_requested' ELSE 'cancelled' END;
            NEW.input := NEW.input || jsonb_build_object('donkeyspace_repository_retirement',reason);
        END IF;
    ELSIF TG_TABLE_NAME='approval_requests' THEN
        IF NEW.state='pending' THEN NEW.state := 'cancelled'; END IF;
    ELSIF TG_TABLE_NAME='projected_work_items' THEN
        IF NEW.sync_status IN ('pending','failed') THEN
            NEW.sync_status := 'cancelled'; NEW.last_error := reason;
        END IF;
    ELSE
        IF NEW.status IN ('pending','failed') THEN
            NEW.status := 'cancelled'; NEW.last_error := reason;
        END IF;
    END IF;
    RETURN NEW;
END $$;

DROP TRIGGER IF EXISTS fence_retired_repository ON jobs;
CREATE TRIGGER fence_retired_repository BEFORE INSERT OR UPDATE ON jobs
FOR EACH ROW EXECUTE FUNCTION fence_retired_repository();
DROP TRIGGER IF EXISTS fence_retired_repository ON outbound_actions;
CREATE TRIGGER fence_retired_repository BEFORE INSERT OR UPDATE ON outbound_actions
FOR EACH ROW EXECUTE FUNCTION fence_retired_repository();
DROP TRIGGER IF EXISTS fence_retired_repository ON agent_publications;
CREATE TRIGGER fence_retired_repository BEFORE INSERT OR UPDATE ON agent_publications
FOR EACH ROW EXECUTE FUNCTION fence_retired_repository();
DROP TRIGGER IF EXISTS fence_retired_repository ON approval_requests;
CREATE TRIGGER fence_retired_repository BEFORE INSERT OR UPDATE ON approval_requests
FOR EACH ROW EXECUTE FUNCTION fence_retired_repository();
DROP TRIGGER IF EXISTS fence_retired_repository ON projected_work_items;
CREATE TRIGGER fence_retired_repository BEFORE INSERT OR UPDATE ON projected_work_items
FOR EACH ROW EXECUTE FUNCTION fence_retired_repository();
