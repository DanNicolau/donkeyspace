use crate::{DbError, PgPool};
use uuid::Uuid;

/// Reserve an attempt before contacting GitHub. A crashed worker consumes its
/// backoff too, and concurrent workers cannot claim the same due attempt.
pub async fn claim(
    pool: &PgPool,
    repository: &str,
    labels: &[String],
) -> Result<Option<Uuid>, DbError> {
    let attempt = Uuid::now_v7();
    Ok(sqlx::query_scalar(
        "INSERT INTO repository_label_sync (repository,labels,attempt_id,next_attempt_at)
         VALUES (lower($1),$2,$3,now()+interval '60 seconds')
         ON CONFLICT (repository) DO UPDATE SET
           labels=EXCLUDED.labels, attempt_id=EXCLUDED.attempt_id,
           attempts=CASE WHEN repository_label_sync.labels<>EXCLUDED.labels THEN 1
                         ELSE LEAST(repository_label_sync.attempts+1,7) END,
           next_attempt_at=now()+make_interval(secs => CASE
             WHEN repository_label_sync.labels<>EXCLUDED.labels THEN 60
             ELSE LEAST(60*power(2,repository_label_sync.attempts),3600)::integer END),
           updated_at=now()
         WHERE repository_label_sync.next_attempt_at<=now()
            OR repository_label_sync.labels<>EXCLUDED.labels
         RETURNING attempt_id",
    )
    .bind(repository)
    .bind(serde_json::json!(labels))
    .bind(attempt)
    .fetch_optional(pool)
    .await?)
}

pub async fn finish(
    pool: &PgPool,
    repository: &str,
    attempt: Uuid,
    error: Option<&str>,
) -> Result<(), DbError> {
    // Do not let a late completion overwrite a newer claim or policy revision.
    sqlx::query(
        "UPDATE repository_label_sync SET
           last_error=$3, updated_at=now(),
           attempts=CASE WHEN $3::text IS NULL THEN 0 ELSE attempts END,
           last_success_at=CASE WHEN $3::text IS NULL THEN now() ELSE last_success_at END,
           next_attempt_at=CASE WHEN $3::text IS NULL THEN now()+interval '1 hour' ELSE next_attempt_at END
         WHERE repository=lower($1) AND attempt_id=$2",
    ).bind(repository).bind(attempt).bind(error).execute(pool).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{DbConfig, apply_migrations, connect};

    #[tokio::test]
    #[ignore = "requires isolated DONKEYSPACE_CANCELLATION_TEST_DATABASE_URL"]
    async fn label_retry_claims_are_fenced_and_backoff_is_bounded() {
        let url = std::env::var("DONKEYSPACE_CANCELLATION_TEST_DATABASE_URL").unwrap();
        assert!(url.ends_with("/donkeyspace_cancellation_test"));
        let pool = connect(&DbConfig::from_database_url(url)).await.unwrap();
        apply_migrations(&pool).await.unwrap();
        apply_migrations(&pool).await.unwrap();
        let repo = format!("retry-{}/example", Uuid::now_v7());
        let labels = vec!["ready".into()];
        let uppercase_repo = repo.to_uppercase();
        let (first, second) = tokio::join!(
            claim(&pool, &repo, &labels),
            claim(&pool, &uppercase_repo, &labels)
        );
        let first = first.unwrap();
        let second = second.unwrap();
        assert_ne!(first.is_some(), second.is_some());
        let old = first.or(second).unwrap();
        let labels = vec!["ready".into(), "blocked".into()];
        let current = claim(&pool, &repo, &labels).await.unwrap().unwrap();
        finish(&pool, &repo, current, Some("unavailable"))
            .await
            .unwrap();
        finish(&pool, &repo, old, None).await.unwrap();
        let error: Option<String> =
            sqlx::query_scalar("SELECT last_error FROM repository_label_sync WHERE repository=$1")
                .bind(&repo)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(error.as_deref(), Some("unavailable"));
        for expected in [120.0, 240.0, 480.0, 960.0, 1920.0, 3600.0, 3600.0, 3600.0] {
            sqlx::query(
                "UPDATE repository_label_sync SET next_attempt_at=now() WHERE repository=$1",
            )
            .bind(&repo)
            .execute(&pool)
            .await
            .unwrap();
            let current = claim(&pool, &repo, &labels).await.unwrap().unwrap();
            let delay: f64 = sqlx::query_scalar("SELECT extract(epoch FROM next_attempt_at-updated_at)::double precision FROM repository_label_sync WHERE repository=$1")
                .bind(&repo).fetch_one(&pool).await.unwrap();
            assert_eq!(delay, expected);
            finish(&pool, &repo, current, Some("unavailable"))
                .await
                .unwrap();
            assert!(claim(&pool, &repo, &labels).await.unwrap().is_none());
        }
    }
}
