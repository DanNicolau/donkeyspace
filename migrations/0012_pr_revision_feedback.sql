ALTER TABLE pr_revisions ADD COLUMN IF NOT EXISTS feedback_digest TEXT;
-- Initialize only once: do not post historical terminal runs on upgrade.
DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='pr_revisions' AND column_name='feedback_next_at' AND table_schema=current_schema()) THEN
        ALTER TABLE pr_revisions ADD COLUMN feedback_next_at TIMESTAMPTZ NOT NULL DEFAULT now();
        UPDATE pr_revisions r SET feedback_next_at='infinity'
        FROM jobs j WHERE j.id=r.job_id AND j.status NOT IN ('queued','leased','running','paused');
    END IF;
END $$;
ALTER TABLE pr_revisions ADD COLUMN IF NOT EXISTS feedback_failures INTEGER NOT NULL DEFAULT 0;

CREATE INDEX IF NOT EXISTS pr_revision_feedback_due ON pr_revisions(feedback_next_at,id);

-- A new decision wakes the independent feedback publisher, including a run
-- whose last terminal status was already projected.
CREATE OR REPLACE FUNCTION wake_pr_revision_feedback() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF OLD.status IS DISTINCT FROM NEW.status OR OLD.input IS DISTINCT FROM NEW.input THEN
        UPDATE pr_revisions SET feedback_next_at=now() WHERE job_id=NEW.id;
    END IF;
    RETURN NEW;
END $$;
DROP TRIGGER IF EXISTS wake_pr_revision_feedback ON jobs;
CREATE TRIGGER wake_pr_revision_feedback AFTER UPDATE OF status,input ON jobs
FOR EACH ROW EXECUTE FUNCTION wake_pr_revision_feedback();
