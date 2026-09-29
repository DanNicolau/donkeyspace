use donkeyspace_core::repository::RepositoryName;
use donkeyspace_db::{DbError, PgPool, repository_label_sync};
use std::time::Duration;

/// Only current configuration authorizes proactive maintenance. Historical
/// repository/installation records are deliberately not consulted.
pub async fn synchronize_labels(
    pool: &PgPool,
    repositories: &[RepositoryName],
    labels: &[String],
    ensure: impl AsyncFn(&RepositoryName, &[String]) -> Result<(), String>,
) -> Result<(), DbError> {
    if labels.is_empty() {
        return Ok(());
    }
    for repository in repositories {
        let name = repository.full_name();
        let Some(attempt) = repository_label_sync::claim(pool, &name, labels).await? else {
            continue;
        };
        let result = tokio::time::timeout(Duration::from_secs(30), ensure(repository, labels))
            .await
            .unwrap_or_else(|_| Err("GitHub label synchronization timed out".into()));
        // A 404 can mean missing access; it must never erase repository history.
        let error = result
            .err()
            .map(|error| error.chars().take(2000).collect::<String>());
        repository_label_sync::finish(pool, &name, attempt, error.as_deref()).await?;
        if let Some(error) = error {
            tracing::warn!(repository = name, operation = "label_sync", %error,
                "repository maintenance failed; retry deferred, other repositories continue");
        } else {
            tracing::info!(repository = name, "ensured donkeyspace github labels");
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use donkeyspace_db::{DbConfig, RepositoryInput, apply_migrations, connect, upsert_repository};
    use std::sync::Mutex;
    use uuid::Uuid;

    #[tokio::test]
    #[ignore = "requires isolated DONKEYSPACE_CANCELLATION_TEST_DATABASE_URL"]
    async fn repository_maintenance_contains_failure_and_survives_restart() {
        let url = std::env::var("DONKEYSPACE_CANCELLATION_TEST_DATABASE_URL").unwrap();
        assert!(url.ends_with("/donkeyspace_cancellation_test"));
        let config = DbConfig::from_database_url(url);
        let pool = connect(&config).await.unwrap();
        apply_migrations(&pool).await.unwrap();
        let owner = format!("maintenance-{}", Uuid::now_v7());
        // Historical installation membership must not authorize maintenance,
        // regardless of whether the current credentials use an App or PAT.
        for installation in [None, Some(12345)] {
            upsert_repository(
                &pool,
                &RepositoryInput {
                    installation_external_id: installation.map(|id| id.to_string()),
                    installation_account_login: installation.map(|_| owner.clone()),
                    provider: "github".into(),
                    owner: owner.clone(),
                    name: format!("historical-{}", installation.unwrap_or(0)),
                    default_branch: "main".into(),
                },
            )
            .await
            .unwrap();
        }
        let repositories =
            RepositoryName::parse_list(&format!("{owner}/inaccessible,{owner}/healthy")).unwrap();
        let labels = vec!["ready".to_string()];
        let calls = Mutex::new(Vec::new());
        synchronize_labels(&pool, &repositories, &labels, async |repo, _| {
            calls.lock().unwrap().push(repo.name.clone());
            if repo.name == "inaccessible" {
                Err("404: access unavailable".into())
            } else {
                Ok(())
            }
        })
        .await
        .unwrap();
        assert_eq!(*calls.lock().unwrap(), ["inaccessible", "healthy"]);
        let failed_name = repositories[0].full_name();
        let error: Option<String> =
            sqlx::query_scalar("SELECT last_error FROM repository_label_sync WHERE repository=$1")
                .bind(&failed_name)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(error.as_deref(), Some("404: access unavailable"));
        pool.close().await;
        let pool = connect(&config).await.unwrap();
        synchronize_labels(&pool, &repositories, &labels, async |_, _| {
            panic!("restarting must not bypass persisted backoff");
        })
        .await
        .unwrap();
        // Restored access is retried only when due, with healthy repositories
        // retaining their successful synchronization interval.
        sqlx::query("UPDATE repository_label_sync SET next_attempt_at=now() WHERE repository=$1")
            .bind(&failed_name)
            .execute(&pool)
            .await
            .unwrap();
        calls.lock().unwrap().clear();
        synchronize_labels(&pool, &repositories, &labels, async |repo, _| {
            calls.lock().unwrap().push(repo.name.clone());
            Ok(())
        })
        .await
        .unwrap();
        assert_eq!(*calls.lock().unwrap(), ["inaccessible"]);
        let error: Option<String> =
            sqlx::query_scalar("SELECT last_error FROM repository_label_sync WHERE repository=$1")
                .bind(&failed_name)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(error, None);
        assert!(
            repository_label_sync::claim(&pool, &failed_name, &labels)
                .await
                .unwrap()
                .is_none()
        );
    }
}
