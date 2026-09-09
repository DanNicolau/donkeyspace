//! Container launch provenance. These records are intents, not liveness claims.
use crate::{DbError, PgPool};
use sqlx::FromRow;
use uuid::Uuid;

#[derive(Debug, FromRow)]
pub struct ContainerExecution {
    pub id: Uuid,
    pub coordinator_job_id: Uuid,
    pub workflow_item_id: Option<i64>,
    pub generation: i64,
    pub lease_owner: String,
    pub container_name: String,
    pub execution_scope: String,
}

/// Register only while the coordinator owns a live running lease. Serialize
/// with closure/reopen and lease updates; never derive generation from caller
/// input. The caller must await this commit before issuing any Docker command.
pub async fn register_container_execution(
    pool: &PgPool,
    coordinator: Uuid,
    lease_owner: &str,
    execution_scope: &str,
) -> Result<ContainerExecution, DbError> {
    let mut tx = pool.begin().await?;
    // Use the same workflow-before-job lock order as issue closure. Jobs without
    // a linked workflow still require an owned running lease below.
    sqlx::query("SELECT w.id FROM workflow_items w JOIN jobs j ON j.workflow_item_id=w.id WHERE j.id=$1 FOR NO KEY UPDATE OF w")
        .bind(coordinator).execute(&mut *tx).await?;
    let job: Option<(Option<i64>, i64)> = sqlx::query_as(
        "SELECT j.workflow_item_id, j.generation FROM jobs j LEFT JOIN workflow_items w ON w.id=j.workflow_item_id
         WHERE j.id=$1 AND j.lease_owner=$2 AND j.status='running' AND j.lease_expires_at>clock_timestamp()
         AND (w.id IS NULL OR (w.provider_state<>'closed' AND w.generation=j.generation)) FOR UPDATE OF j",
    )
    .bind(coordinator).bind(lease_owner).fetch_optional(&mut *tx).await?;
    let Some((workflow, generation)) = job else {
        return Err(DbError::ExecutionCancelled);
    };
    let id = Uuid::now_v7();
    let record = sqlx::query_as(
        "INSERT INTO container_executions (id,coordinator_job_id,workflow_item_id,generation,lease_owner,container_name,execution_scope)
         VALUES ($1,$2,$3,$4,$5,$6,$7) RETURNING *",
    )
    .bind(id).bind(coordinator).bind(workflow).bind(generation).bind(lease_owner)
    .bind(format!("donkeyspace-execution-{id}")).bind(execution_scope)
    .fetch_one(&mut *tx).await?;
    tx.commit().await?;
    Ok(record)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        DbConfig, RepositoryInput, WorkflowItemInput, acquire_job_lease, apply_migrations,
        cancellation::{IssueObservation, observe_issue},
        connect, create_job, mark_job_running, upsert_repository,
    };
    use serde_json::json;
    use std::time::Duration;

    #[tokio::test]
    #[ignore = "requires isolated DONKEYSPACE_CANCELLATION_TEST_DATABASE_URL"]
    async fn container_intents_require_live_ownership_and_retain_origin_after_reopen() {
        let url = std::env::var("DONKEYSPACE_CANCELLATION_TEST_DATABASE_URL").unwrap();
        assert!(url.ends_with("/donkeyspace_cancellation_test"));
        let pool = connect(&DbConfig::from_database_url(url)).await.unwrap();
        apply_migrations(&pool).await.unwrap();
        apply_migrations(&pool).await.unwrap();
        let repository = upsert_repository(
            &pool,
            &RepositoryInput {
                installation_external_id: None,
                installation_account_login: None,
                provider: "github".into(),
                owner: format!("container-test-{}", Uuid::now_v7()),
                name: "example".into(),
                default_branch: "main".into(),
            },
        )
        .await
        .unwrap();
        let issue = WorkflowItemInput {
            repository_id: repository,
            provider_issue_id: "1".into(),
            issue_number: 1,
            provider_state: "open".into(),
            current_state: Some("in_progress".into()),
            current_labels: vec![],
        };
        let workflow = observe_issue(
            &pool,
            &IssueObservation {
                issue: &issue,
                updated_at: None,
                close_reason: None,
                owner: "test",
                repo: "example",
                state_labels: vec![],
            },
        )
        .await
        .unwrap()
        .unwrap();
        let job = create_job(&pool, Some(workflow), "developer", &json!({}))
            .await
            .unwrap();
        assert!(
            register_container_execution(&pool, job.id, "owner", "scope")
                .await
                .is_err()
        );
        acquire_job_lease(&pool, job.id, "owner", 60)
            .await
            .unwrap()
            .unwrap();
        assert!(
            register_container_execution(&pool, job.id, "owner", "scope")
                .await
                .is_err()
        );
        mark_job_running(&pool, job.id).await.unwrap().unwrap();
        assert!(
            register_container_execution(&pool, job.id, "other", "scope")
                .await
                .is_err()
        );

        let first = register_container_execution(&pool, job.id, "owner", "scope")
            .await
            .unwrap();
        let second = register_container_execution(&pool, job.id, "owner", "scope")
            .await
            .unwrap();
        assert_ne!(first.id, second.id);
        assert_ne!(first.container_name, second.container_name);
        assert_eq!(first.coordinator_job_id, job.id);
        assert_eq!(first.workflow_item_id, Some(workflow));
        assert_eq!(first.generation, 1);
        assert_eq!(first.lease_owner, "owner");
        assert_eq!(first.execution_scope, "scope");
        // Observe committed intent from another connection before Docker would run.
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT count(*) FROM container_executions WHERE coordinator_job_id=$1"
            )
            .bind(job.id)
            .fetch_one(&pool)
            .await
            .unwrap(),
            2
        );

        sqlx::query("UPDATE jobs SET lease_expires_at=now()-interval '1 second' WHERE id=$1")
            .bind(job.id)
            .execute(&pool)
            .await
            .unwrap();
        assert!(
            register_container_execution(&pool, job.id, "owner", "scope")
                .await
                .is_err()
        );
        sqlx::query("UPDATE jobs SET lease_expires_at=now()+interval '60 seconds' WHERE id=$1")
            .bind(job.id)
            .execute(&pool)
            .await
            .unwrap();

        // Force registration to wait behind a lifecycle transaction. Its prior
        // open/running snapshot cannot authorize a launch after that commit.
        let mut lifecycle = pool.begin().await.unwrap();
        sqlx::query("SELECT id FROM workflow_items WHERE id=$1 FOR NO KEY UPDATE")
            .bind(workflow)
            .execute(&mut *lifecycle)
            .await
            .unwrap();
        let registration = register_container_execution(&pool, job.id, "owner", "scope");
        tokio::pin!(registration);
        let waiting = async {
            loop {
                let blocked: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE query LIKE 'SELECT w.id FROM workflow_items w JOIN jobs j%' AND cardinality(pg_blocking_pids(pid))>0)")
                    .fetch_one(&pool).await.unwrap();
                if blocked {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        };
        tokio::select! {
            result = &mut registration => panic!("registration bypassed lifecycle lock: {result:?}"),
            result = tokio::time::timeout(Duration::from_secs(5), waiting) => result.unwrap(),
        }
        sqlx::query("UPDATE workflow_items SET provider_state='closed' WHERE id=$1")
            .bind(workflow)
            .execute(&mut *lifecycle)
            .await
            .unwrap();
        lifecycle.commit().await.unwrap();
        assert!(matches!(
            registration.await,
            Err(DbError::ExecutionCancelled)
        ));

        // Reopen increments the workflow generation. Retain both old launch
        // intents unchanged and reject the old coordinator even if still running.
        observe_issue(
            &pool,
            &IssueObservation {
                issue: &issue,
                updated_at: None,
                close_reason: None,
                owner: "test",
                repo: "example",
                state_labels: vec![],
            },
        )
        .await
        .unwrap();
        assert!(
            register_container_execution(&pool, job.id, "owner", "scope")
                .await
                .is_err()
        );
        let new_job = create_job(&pool, Some(workflow), "developer", &json!({}))
            .await
            .unwrap();
        acquire_job_lease(&pool, new_job.id, "owner", 60)
            .await
            .unwrap()
            .unwrap();
        mark_job_running(&pool, new_job.id).await.unwrap().unwrap();
        let new = register_container_execution(&pool, new_job.id, "owner", "scope")
            .await
            .unwrap();
        assert_eq!(new.generation, 2);
        assert_ne!(new.container_name, first.container_name);
        let saved: ContainerExecution =
            sqlx::query_as("SELECT * FROM container_executions WHERE id=$1")
                .bind(first.id)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(saved.generation, 1);
        assert_eq!(saved.container_name, first.container_name);
        assert_eq!(saved.coordinator_job_id, job.id);

        // Non-workflow jobs also require ownership, and never borrow a workflow.
        let standalone = create_job(&pool, None, "test", &json!({})).await.unwrap();
        acquire_job_lease(&pool, standalone.id, "owner", 60)
            .await
            .unwrap()
            .unwrap();
        mark_job_running(&pool, standalone.id)
            .await
            .unwrap()
            .unwrap();
        let standalone = register_container_execution(&pool, standalone.id, "owner", "scope")
            .await
            .unwrap();
        assert_eq!(standalone.workflow_item_id, None);

        // Retain fixture history in the disposable database for inspection.
    }
}
