//! Revision feedback is projected independently of long-running agents.
use crate::{DbError, PgConnection, PgPool};
use serde_json::Value;
use sqlx::FromRow;
use uuid::Uuid;

#[derive(Debug, FromRow)]
pub struct Snapshot {
    pub job_id: Uuid,
    pub number: i64,
    pub pr_number: i64,
    pub head_sha: String,
    pub publication_sha: Option<String>,
    pub publication_status: String,
    pub status: String,
    pub input: Value,
    pub result: Option<Value>,
    pub checkpoint: Option<Value>,
    pub children: Value,
    pub feedback_digest: Option<String>,
}

pub async fn timestamp(connection: &mut PgConnection) -> Result<String, DbError> {
    Ok(sqlx::query_scalar(
        "SELECT to_char(now() AT TIME ZONE 'UTC','YYYY-MM-DD HH24:MI:SS') || ' UTC'",
    )
    .fetch_one(connection)
    .await?)
}

pub async fn due(pool: &PgPool) -> Result<Vec<Uuid>, DbError> {
    Ok(sqlx::query_scalar(
        r#"SELECT r.job_id FROM pr_revisions r
           JOIN jobs j ON j.id=r.job_id
           JOIN workflow_items w ON w.id=r.workflow_item_id
           WHERE r.feedback_next_at<=now() AND workflow_repository_tracked(w.id)
             AND w.provider_state='open' AND j.generation=w.generation
             AND j.status NOT IN ('cancelled','cancel_requested')
           ORDER BY r.feedback_next_at,r.id LIMIT 20"#,
    )
    .fetch_all(pool)
    .await?)
}

pub async fn snapshot(connection: &mut PgConnection, job: Uuid) -> Result<Snapshot, DbError> {
    Ok(sqlx::query_as(r#"SELECT r.job_id,
           (SELECT count(*) FROM pr_revisions older
            WHERE older.repository_id=r.repository_id AND older.pr_number=r.pr_number
              AND older.id<=r.id) AS number,
           r.pr_number,r.head_sha,r.publication_sha,r.status AS publication_status,
           j.status,j.input,j.result,c.state AS checkpoint,r.feedback_digest,
           COALESCE((SELECT jsonb_agg(jsonb_build_object(
               'input',child.input,'status',child.status,'result',child.result)
               ORDER BY child.created_at)
               FROM jobs child
               WHERE child.input #>> '{plugin_execution,coordinator_run_id}'=j.id::text),
               '[]'::jsonb) AS children
           FROM pr_revisions r JOIN jobs j ON j.id=r.job_id
           LEFT JOIN lifecycle_checkpoints c ON c.coordinator_job_id=j.id AND c.generation=j.generation
           WHERE r.job_id=$1"#)
        .bind(job).fetch_one(connection).await?)
}

pub async fn projected(
    connection: &mut PgConnection,
    job: Uuid,
    digest: &str,
    terminal: bool,
) -> Result<(), DbError> {
    sqlx::query("UPDATE pr_revisions SET feedback_digest=$2,feedback_failures=0,feedback_next_at=CASE WHEN $3 THEN 'infinity'::timestamptz ELSE now()+interval '10 seconds' END WHERE job_id=$1")
        .bind(job).bind(digest).bind(terminal).execute(connection).await?;
    Ok(())
}

pub async fn failed(pool: &PgPool, job: Uuid) -> Result<(), DbError> {
    sqlx::query("UPDATE pr_revisions SET feedback_next_at=now()+make_interval(secs=>LEAST(900,30*power(2,LEAST(feedback_failures,5)))::int),feedback_failures=feedback_failures+1 WHERE job_id=$1")
        .bind(job).execute(pool).await?;
    Ok(())
}

pub async fn wake(pool: &PgPool, job: Uuid) -> Result<(), DbError> {
    sqlx::query("UPDATE pr_revisions SET feedback_next_at=now() WHERE job_id=$1")
        .bind(job)
        .execute(pool)
        .await?;
    Ok(())
}
