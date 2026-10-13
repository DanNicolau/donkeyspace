//! Durable progress for direct GitHub comment polling.
use crate::{DbError, PgPool};
use chrono::{DateTime, Utc};
use sqlx::FromRow;

#[derive(Debug, Clone, FromRow)]
pub struct Cursor {
    pub repository_id: i64,
    pub since: DateTime<Utc>,
    pub next_page: i32,
    pub scan_started_at: Option<DateTime<Utc>>,
    pub scan_latest_at: Option<DateTime<Utc>>,
    pub etag: Option<String>,
    pub revision: i64,
}

pub async fn load(pool: &PgPool, repository_id: i64) -> Result<Cursor, DbError> {
    sqlx::query("INSERT INTO github_comment_poll_cursors (repository_id) VALUES ($1) ON CONFLICT DO NOTHING")
        .bind(repository_id).execute(pool).await?;
    Ok(
        sqlx::query_as("SELECT * FROM github_comment_poll_cursors WHERE repository_id=$1")
            .bind(repository_id)
            .fetch_one(pool)
            .await?,
    )
}

/// Advance only after all comments on the page have durable admission receipts.
/// Another poller cannot overwrite newer progress with an older scan.
pub async fn save(pool: &PgPool, cursor: &mut Cursor) -> Result<(), DbError> {
    let changed = sqlx::query("UPDATE github_comment_poll_cursors SET since=$2,next_page=$3,scan_started_at=$4,scan_latest_at=$5,etag=$6,revision=revision+1 WHERE repository_id=$1 AND revision=$7")
        .bind(cursor.repository_id).bind(cursor.since).bind(cursor.next_page)
        .bind(cursor.scan_started_at).bind(cursor.scan_latest_at).bind(&cursor.etag)
        .bind(cursor.revision).execute(pool).await?.rows_affected();
    if changed != 1 {
        return Err(DbError::StaleAdmission);
    }
    cursor.revision += 1;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::*;
    use serde_json::json;

    async fn fixture() -> (PgPool, i64) {
        let url = std::env::var("DONKEYSPACE_CANCELLATION_TEST_DATABASE_URL").unwrap();
        assert!(url.ends_with("/donkeyspace_cancellation_test"));
        let pool = connect(&DbConfig::from_database_url(url)).await.unwrap();
        apply_migrations(&pool).await.unwrap();
        let repository = upsert_repository(
            &pool,
            &RepositoryInput {
                installation_external_id: None,
                installation_account_login: None,
                provider: "github".into(),
                owner: format!("comments-{}", Uuid::now_v7()),
                name: "test".into(),
                default_branch: "main".into(),
            },
        )
        .await
        .unwrap();
        (pool, repository)
    }

    #[tokio::test]
    #[ignore = "requires isolated DONKEYSPACE_CANCELLATION_TEST_DATABASE_URL"]
    async fn scan_progress_survives_restart_and_rejects_stale_writers() {
        let (pool, repository) = fixture().await;
        let mut cursor = load(&pool, repository).await.unwrap();
        assert!(cursor.since < Utc::now() - chrono::Duration::hours(23));
        let mut stale = cursor.clone();
        cursor.next_page = 2;
        cursor.scan_started_at = Some(Utc::now());
        cursor.scan_latest_at = Some(Utc::now());
        save(&pool, &mut cursor).await.unwrap();
        assert!(matches!(
            save(&pool, &mut stale).await,
            Err(DbError::StaleAdmission)
        ));
        let restarted = load(&pool, repository).await.unwrap();
        assert_eq!(restarted.next_page, 2);
        assert_eq!(restarted.since, cursor.since);
        assert!(restarted.scan_started_at.is_some());
        cursor.next_page = 1;
        cursor.etag = Some("stable-page".into());
        cursor.scan_started_at = None;
        cursor.scan_latest_at = None;
        save(&pool, &mut cursor).await.unwrap();
        assert_eq!(load(&pool, repository).await.unwrap().etag, cursor.etag);
    }

    #[tokio::test]
    #[ignore = "requires isolated DONKEYSPACE_CANCELLATION_TEST_DATABASE_URL"]
    async fn created_comment_receipts_deduplicate_transports_and_roll_back_with_admission() {
        let (pool, repository) = fixture().await;
        let payload = json!({"action":"created","comment":{"id":7}});
        let mut transaction = pool.begin().await.unwrap();
        assert!(
            record_webhook_delivery(
                &mut *transaction,
                Some(repository),
                "rolled-back-comment",
                "issue_comment",
                &payload
            )
            .await
            .unwrap()
            .is_some()
        );
        transaction.rollback().await.unwrap();
        assert!(
            !github_comment_delivery_exists(&pool, repository, 7)
                .await
                .unwrap()
        );
        let a = Uuid::now_v7().to_string();
        let b = Uuid::now_v7().to_string();
        let (one, two) = tokio::join!(
            record_webhook_delivery(&pool, Some(repository), &a, "issue_comment", &payload),
            record_webhook_delivery(&pool, Some(repository), &b, "issue_comment", &payload)
        );
        assert_eq!(
            usize::from(one.unwrap().is_some()) + usize::from(two.unwrap().is_some()),
            1
        );
        assert!(
            github_comment_delivery_exists(&pool, repository, 7)
                .await
                .unwrap()
        );
        assert!(
            record_webhook_delivery(
                &pool,
                Some(repository),
                &Uuid::now_v7().to_string(),
                "issue_comment",
                &payload
            )
            .await
            .unwrap()
            .is_none()
        );
        let edited = json!({"action":"edited","comment":{"id":7}});
        assert!(
            record_webhook_delivery(
                &pool,
                Some(repository),
                &Uuid::now_v7().to_string(),
                "issue_comment",
                &edited
            )
            .await
            .unwrap()
            .is_some()
        );
    }

    #[tokio::test]
    #[ignore = "requires isolated DONKEYSPACE_CANCELLATION_TEST_DATABASE_URL"]
    async fn migration_deduplicates_legacy_receipts_without_replaying_old_comments() {
        let (pool, repository) = fixture().await;
        for _ in 0..2 {
            sqlx::query("INSERT INTO webhook_deliveries(repository_id,delivery_id,event_name,payload) VALUES ($1,$2,'issue_comment',$3)")
                .bind(repository).bind(Uuid::now_v7().to_string())
                .bind(json!({"action":"created","comment":{"id":99}}))
                .execute(&pool).await.unwrap();
        }
        for _ in 0..2 {
            apply_migrations(&pool).await.unwrap();
        }
        assert!(
            github_comment_delivery_exists(&pool, repository, 99)
                .await
                .unwrap()
        );
        let rows: i64 =
            sqlx::query_scalar("SELECT count(*) FROM webhook_deliveries WHERE repository_id=$1")
                .bind(repository)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(rows, 2);
        assert!(
            record_webhook_delivery(
                &pool,
                Some(repository),
                &Uuid::now_v7().to_string(),
                "issue_comment",
                &json!({"action":"created","comment":{"id":99}})
            )
            .await
            .unwrap()
            .is_none()
        );
    }
}
