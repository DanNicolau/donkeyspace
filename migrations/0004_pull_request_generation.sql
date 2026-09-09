-- Managed branch names carry the full originating job UUID. Never infer that a
-- newly observed historical PR belongs to the current reopened generation.
CREATE OR REPLACE FUNCTION stamp_pull_request_generation() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE w workflow_items; origin_job TEXT; origin_generation BIGINT; newest_generation BIGINT;
BEGIN
    -- Once attributed, a PR keeps its original generation on every delivery.
    IF TG_OP = 'UPDATE' AND OLD.workflow_item_id IS NOT DISTINCT FROM NEW.workflow_item_id
        AND OLD.generation > 0 THEN
        NEW.generation := OLD.generation;
        RETURN NEW;
    END IF;
    NEW.generation := 0; -- unknown attribution; dispatch must fail closed
    IF NEW.workflow_item_id IS NULL THEN RETURN NEW; END IF;
    SELECT * INTO w FROM workflow_items WHERE id=NEW.workflow_item_id;
    IF NOT NEW.managed_by_donkeyspace THEN
        NEW.generation := w.generation;
        RETURN NEW;
    END IF;
    origin_job := substring(NEW.head_ref FROM '/(?:issue|attempt)-[0-9]+-([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})(?:-|$)');
    IF origin_job IS NOT NULL THEN
        SELECT generation INTO origin_generation FROM jobs
        WHERE id=origin_job::uuid AND workflow_item_id=NEW.workflow_item_id;
        NEW.generation := COALESCE(origin_generation,0);
        RETURN NEW;
    END IF;
    -- Legacy shortened branch names can be recovered from persisted checkpoint
    -- provenance. Ambiguous names reused by multiple generations stay unknown.
    SELECT min(j.generation),max(j.generation) INTO origin_generation,newest_generation
    FROM agent_publications p JOIN jobs j ON j.id=p.coordinator_job_id
    WHERE p.workflow_item_id=NEW.workflow_item_id AND p.branch_name=NEW.head_ref;
    IF origin_generation = newest_generation THEN
        NEW.generation := origin_generation;
    ELSIF origin_generation IS NULL AND w.generation=1 THEN
        -- Before the first reopen there is no earlier workflow generation.
        NEW.generation := 1;
    END IF;
    RETURN NEW;
END $$;
DROP TRIGGER IF EXISTS stamp_pull_request_generation ON pull_requests;
CREATE TRIGGER stamp_pull_request_generation BEFORE INSERT OR UPDATE ON pull_requests
FOR EACH ROW EXECUTE FUNCTION stamp_pull_request_generation();
