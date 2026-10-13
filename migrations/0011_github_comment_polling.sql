-- Transport-independent identity for created comments. Preserve the original
-- delivery ID for ingress diagnostics, including historical duplicates.
ALTER TABLE webhook_deliveries ADD COLUMN IF NOT EXISTS github_comment_id BIGINT;
UPDATE webhook_deliveries d SET github_comment_id = existing.comment_id
FROM (
    SELECT repository_id, (payload #>> '{comment,id}')::BIGINT AS comment_id, min(id) AS first_id
    FROM webhook_deliveries
    WHERE event_name='issue_comment' AND payload->>'action'='created'
      AND repository_id IS NOT NULL AND jsonb_typeof(payload #> '{comment,id}')='number'
    GROUP BY repository_id, payload #>> '{comment,id}'
) existing
WHERE d.id=existing.first_id AND d.github_comment_id IS NULL
  AND NOT EXISTS (SELECT 1 FROM webhook_deliveries seen
                  WHERE seen.repository_id=existing.repository_id
                    AND seen.github_comment_id=existing.comment_id);
CREATE UNIQUE INDEX IF NOT EXISTS webhook_deliveries_github_comment_idx
    ON webhook_deliveries(repository_id, github_comment_id)
    WHERE github_comment_id IS NOT NULL;

-- A bounded scan can continue on the next cycle or after a process restart.
CREATE TABLE IF NOT EXISTS github_comment_poll_cursors (
    repository_id BIGINT PRIMARY KEY REFERENCES repositories(id),
    since TIMESTAMPTZ NOT NULL DEFAULT now() - interval '24 hours',
    next_page INTEGER NOT NULL DEFAULT 1 CHECK (next_page > 0),
    scan_started_at TIMESTAMPTZ,
    scan_latest_at TIMESTAMPTZ,
    etag TEXT,
    revision BIGINT NOT NULL DEFAULT 0
);
