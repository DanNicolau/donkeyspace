//! Fence expired coordinators before attempting destructive execution cleanup.
use crate::{DbError, PgPool, container_executions::ContainerExecution};
use serde_json::json;
use std::collections::BTreeMap;
use uuid::Uuid;

/// Never recycle a leased/running UUID: a stalled former owner may still return.
/// Child tasks have no independent lease; their coordinator owns their lifetime.
pub async fn fence_expired_jobs(
    pool: &PgPool,
    limit: i64,
    labels: &BTreeMap<String, String>,
) -> Result<(), DbError> {
    let candidates: Vec<(Uuid,Option<i64>)> = sqlx::query_as("SELECT id,workflow_item_id FROM jobs WHERE status IN ('leased','running') AND (lease_expires_at IS NULL OR lease_expires_at<=clock_timestamp()) AND input #>> '{plugin_execution,coordinator_run_id}' IS NULL ORDER BY created_at LIMIT $1")
        .bind(limit).fetch_all(pool).await?;
    for (id, workflow) in candidates {
        let mut tx = pool.begin().await?;
        // Same order as closure/side effects/launch admission. Recheck expiry
        // after waiting, so a heartbeat that wins admission protects live work.
        if let Some(workflow) = workflow {
            let locked: Option<i64> = sqlx::query_scalar(
                "SELECT id FROM workflow_items WHERE id=$1 FOR NO KEY UPDATE SKIP LOCKED",
            )
            .bind(workflow)
            .fetch_optional(&mut *tx)
            .await?;
            if locked.is_none() {
                continue;
            }
        }
        let locked: Option<Uuid> =
            sqlx::query_scalar("SELECT id FROM jobs WHERE id=$1 FOR UPDATE SKIP LOCKED")
                .bind(id)
                .fetch_optional(&mut *tx)
                .await?;
        if locked.is_none() {
            continue;
        }
        let job: Option<(Option<i64>, i64)> = sqlx::query_as("UPDATE jobs SET status=CASE WHEN status='leased' THEN 'cancelled' ELSE 'cancel_requested' END, recovery_requested_at=clock_timestamp(), updated_at=now() WHERE id=$1 AND status IN ('leased','running') AND (lease_expires_at IS NULL OR lease_expires_at<=clock_timestamp()) RETURNING workflow_item_id,generation")
            .bind(id).fetch_optional(&mut *tx).await?;
        if let Some((workflow, generation)) = job {
            sqlx::query("UPDATE jobs SET status='cancel_requested', recovery_requested_at=clock_timestamp(),updated_at=now() WHERE input #>> '{plugin_execution,coordinator_run_id}'=$1::text AND status IN ('waiting','queued','leased','running','paused')")
                .bind(id).execute(&mut *tx).await?;
            sqlx::query("UPDATE outbound_actions SET status='cancelled',updated_at=now() WHERE job_id=$1 AND status='pending'")
                .bind(id).execute(&mut *tx).await?;
            sqlx::query("UPDATE agent_publications SET status='cancelled',updated_at=now() WHERE coordinator_job_id=$1 AND status IN ('pending','failed')")
                .bind(id).execute(&mut *tx).await?;
            sqlx::query("UPDATE approval_requests SET state='cancelled',updated_at=now() WHERE coordinator_job_id=$1 AND state='pending'")
                .bind(id).execute(&mut *tx).await?;
            if let Some(workflow) = workflow {
                let previous: Option<(Option<String>,)> = sqlx::query_as("SELECT current_state FROM workflow_items WHERE id=$1 AND generation=$2 AND provider_state<>'closed'")
                    .bind(workflow).bind(generation).fetch_optional(&mut *tx).await?;
                if let Some((previous,)) = previous {
                    sqlx::query("UPDATE workflow_items SET current_state='needs_human',updated_at=now() WHERE id=$1")
                        .bind(workflow).execute(&mut *tx).await?;
                    sqlx::query("INSERT INTO state_transitions(workflow_item_id,job_id,from_state,to_state,reason) VALUES($1,$2,$3,'needs_human','Worker lease expired; execution fenced for cleanup, no automatic replay')")
                        .bind(workflow).bind(id).bind(previous).execute(&mut *tx).await?;
                    let (owner,repo,number): (String,String,i64) = sqlx::query_as("SELECT r.owner,r.name,w.issue_number FROM workflow_items w JOIN repositories r ON r.id=w.repository_id WHERE w.id=$1")
                        .bind(workflow).fetch_one(&mut *tx).await?;
                    let remove: Vec<&String> = labels
                        .iter()
                        .filter(|(state, _)| state.as_str() != "needs_human")
                        .map(|(_, label)| label)
                        .collect();
                    for (action, payload) in [
                        (
                            "issue.remove_labels",
                            json!({"owner":owner,"repo":repo,"issue_number":number,"labels":remove}),
                        ),
                        (
                            "issue.add_label",
                            json!({"owner":owner,"repo":repo,"issue_number":number,"label":labels.get("needs_human")}),
                        ),
                    ] {
                        if action == "issue.add_label" && !labels.contains_key("needs_human") {
                            continue;
                        }
                        sqlx::query("INSERT INTO outbound_actions(workflow_item_id,provider,action_type,payload) VALUES($1,'github',$2,$3)")
                            .bind(workflow).bind(action).bind(payload).execute(&mut *tx).await?;
                    }
                }
                sqlx::query("INSERT INTO lifecycle_events(workflow_item_id,job_id,event_type,level,source,status,summary,reason) VALUES($1,$2,'execution_recovery','milestone','worker','cancel_requested','Worker lease expired; execution fenced for cleanup',$3)")
                    .bind(workflow).bind(id).bind("Automatic replay is disabled; inspect retained execution history").execute(&mut *tx).await?;
            }
        }
        tx.commit().await?;
    }
    Ok(())
}

/// Rotate retained tombstones forever, including previously absent containers:
/// an accepted Docker create/run request can materialize after local cleanup.
pub async fn cleanup_candidates(
    pool: &PgPool,
    daemon: &str,
    limit: i64,
) -> Result<Vec<ContainerExecution>, DbError> {
    Ok(sqlx::query_as("SELECT e.* FROM container_executions e JOIN jobs j ON j.id=e.coordinator_job_id LEFT JOIN workflow_items w ON w.id=e.workflow_item_id LEFT JOIN container_cleanup_observations o ON o.execution_id=e.id WHERE e.docker_daemon_id=$1 AND (j.status IN ('cancel_requested','cancelled','completed','failed','superseded') OR (w.id IS NOT NULL AND (w.provider_state='closed' OR w.generation<>e.generation))) ORDER BY o.checked_at NULLS FIRST,e.id LIMIT $2")
        .bind(daemon).bind(limit).fetch_all(pool).await?)
}

pub async fn record_cleanup(
    pool: &PgPool,
    execution: Uuid,
    error: Option<&str>,
) -> Result<(), DbError> {
    sqlx::query("INSERT INTO container_cleanup_observations(execution_id,error) VALUES($1,$2) ON CONFLICT(execution_id) DO UPDATE SET checked_at=clock_timestamp(),error=EXCLUDED.error")
        .bind(execution).bind(error).execute(pool).await?;
    Ok(())
}

/// Only acknowledge crashed coordinators whose registered executions have all
/// been checked on their owning daemons after cancellation. Unknown legacy
/// ownership or unregistered native processes need operator reconciliation.
pub async fn finish_recovered_jobs(pool: &PgPool) -> Result<(), DbError> {
    let ids: Vec<Uuid> = sqlx::query_scalar("SELECT j.id FROM jobs j WHERE j.status='cancel_requested' AND (j.lease_expires_at IS NULL OR j.lease_expires_at<=clock_timestamp()) AND j.input #>> '{plugin_execution,coordinator_run_id}' IS NULL AND EXISTS(SELECT 1 FROM container_executions e WHERE e.coordinator_job_id=j.id) AND NOT EXISTS(SELECT 1 FROM container_executions e LEFT JOIN container_cleanup_observations o ON o.execution_id=e.id WHERE e.coordinator_job_id=j.id AND (e.docker_daemon_id IS NULL OR o.execution_id IS NULL OR o.error IS NOT NULL OR o.checked_at<j.updated_at)) LIMIT 100")
        .fetch_all(pool).await?;
    for id in ids {
        crate::cancellation::finish_job_cancellation(pool, id).await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        DbConfig, RepositoryInput, WorkflowItemInput, acquire_job_lease, apply_migrations,
        cancellation::{
            IssueObservation, heartbeat_job, job_execution_allowed, lock_job_side_effect,
            observe_issue,
        },
        complete_job, connect,
        container_executions::register_container_execution,
        create_job, create_waiting_job, fail_job, get_job, mark_job_running,
    };
    use serde_json::json;

    #[tokio::test]
    #[ignore = "requires isolated DONKEYSPACE_CANCELLATION_TEST_DATABASE_URL"]
    async fn expired_coordinators_are_fenced_once_and_cleanup_requires_all_owning_daemons() {
        let url = std::env::var("DONKEYSPACE_CANCELLATION_TEST_DATABASE_URL").unwrap();
        assert!(url.ends_with("/donkeyspace_cancellation_test"));
        let pool = connect(&DbConfig::from_database_url(url)).await.unwrap();
        apply_migrations(&pool).await.unwrap();
        apply_migrations(&pool).await.unwrap();
        let repository = crate::upsert_repository(
            &pool,
            &RepositoryInput {
                installation_external_id: None,
                installation_account_login: None,
                provider: "github".into(),
                owner: format!("recovery-{}", Uuid::now_v7()),
                name: "example".into(),
                default_branch: "main".into(),
            },
        )
        .await
        .unwrap();
        let mut issue = WorkflowItemInput {
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
        let root = create_job(&pool, Some(workflow), "developer", &json!({}))
            .await
            .unwrap();
        acquire_job_lease(&pool, root.id, "original", 60)
            .await
            .unwrap()
            .unwrap();
        mark_job_running(&pool, root.id).await.unwrap().unwrap();
        let first = register_container_execution(&pool, root.id, "original", "scope", "daemon-a")
            .await
            .unwrap();
        let second = register_container_execution(&pool, root.id, "original", "scope", "daemon-b")
            .await
            .unwrap();
        let child = create_waiting_job(
            &pool,
            Some(workflow),
            "task",
            &json!({"plugin_execution":{"coordinator_run_id":root.id}}),
        )
        .await
        .unwrap();
        // Child tasks have no lease. Healthy coordinator protects them all.
        fence_expired_jobs(&pool, 100, &BTreeMap::new())
            .await
            .unwrap();
        assert_eq!(
            get_job(&pool, child.id).await.unwrap().unwrap().status,
            "waiting"
        );
        assert!(
            cleanup_candidates(&pool, "daemon-a", 100)
                .await
                .unwrap()
                .iter()
                .all(|e| e.id != first.id)
        );
        assert!(heartbeat_job(&pool, root.id, "original", 60).await.unwrap());
        sqlx::query("UPDATE jobs SET lease_expires_at=now()-interval '1 second' WHERE id=$1")
            .bind(root.id)
            .execute(&pool)
            .await
            .unwrap();
        let mut held = pool.begin().await.unwrap();
        sqlx::query("SELECT id FROM workflow_items WHERE id=$1 FOR NO KEY UPDATE")
            .bind(workflow)
            .execute(&mut *held)
            .await
            .unwrap();
        tokio::time::timeout(
            std::time::Duration::from_secs(2),
            fence_expired_jobs(&pool, 100, &BTreeMap::new()),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(
            get_job(&pool, root.id).await.unwrap().unwrap().status,
            "running",
            "busy workflow must be skipped without fencing from an unlocked snapshot"
        );
        held.commit().await.unwrap();
        let mut admission = pool.begin().await.unwrap();
        sqlx::query("INSERT INTO jobs(id,workflow_item_id,role,status,input) VALUES($1,$2,'test','waiting',$3)")
            .bind(Uuid::now_v7()).bind(workflow).bind(json!({"plugin_execution":{"coordinator_run_id":root.id}}))
            .execute(&mut *admission).await.unwrap();
        fence_expired_jobs(&pool, 100, &BTreeMap::new())
            .await
            .unwrap();
        assert_eq!(
            get_job(&pool, root.id).await.unwrap().unwrap().status,
            "running",
            "parent must not be fenced before its child admission commits"
        );
        admission.commit().await.unwrap();

        assert!(
            !heartbeat_job(&pool, root.id, "original", 60).await.unwrap(),
            "expired owner resurrected lease"
        );
        let labels = BTreeMap::new();
        let (a, b) = tokio::join!(
            fence_expired_jobs(&pool, 100, &labels),
            fence_expired_jobs(&pool, 100, &labels)
        );
        a.unwrap();
        b.unwrap();
        assert!(!job_execution_allowed(&pool, root.id).await.unwrap());
        assert!(lock_job_side_effect(&pool, root.id).await.is_err());
        assert!(
            acquire_job_lease(&pool, root.id, "replacement", 60)
                .await
                .unwrap()
                .is_none()
        );
        complete_job(&pool, root.id, &json!({"late":true}))
            .await
            .unwrap();
        fail_job(&pool, root.id, &json!({"late":true}))
            .await
            .unwrap();
        assert_eq!(
            get_job(&pool, root.id).await.unwrap().unwrap().status,
            "cancel_requested"
        );
        assert_eq!(
            get_job(&pool, child.id).await.unwrap().unwrap().status,
            "cancelled"
        );
        assert_eq!(sqlx::query_scalar::<_,i64>("SELECT count(*) FROM lifecycle_events WHERE job_id=$1 AND event_type='execution_recovery'").bind(root.id).fetch_one(&pool).await.unwrap(),1);
        assert_eq!(
            sqlx::query_scalar::<_, String>("SELECT current_state FROM workflow_items WHERE id=$1")
                .bind(workflow)
                .fetch_one(&pool)
                .await
                .unwrap(),
            "needs_human"
        );
        assert!(
            cleanup_candidates(&pool, "daemon-a", 100)
                .await
                .unwrap()
                .iter()
                .any(|e| e.id == first.id)
        );
        assert!(
            cleanup_candidates(&pool, "daemon-a", 100)
                .await
                .unwrap()
                .iter()
                .all(|e| e.id != second.id)
        );
        record_cleanup(&pool, first.id, None).await.unwrap();
        record_cleanup(&pool, second.id, Some("Docker unavailable"))
            .await
            .unwrap();
        finish_recovered_jobs(&pool).await.unwrap();
        assert_eq!(
            get_job(&pool, root.id).await.unwrap().unwrap().status,
            "cancel_requested"
        );
        record_cleanup(&pool, second.id, None).await.unwrap();
        finish_recovered_jobs(&pool).await.unwrap();
        assert_eq!(
            get_job(&pool, root.id).await.unwrap().unwrap().status,
            "cancelled"
        );
        assert_eq!(
            get_job(&pool, child.id).await.unwrap().unwrap().status,
            "cancelled"
        );
        let late_child = create_waiting_job(
            &pool,
            Some(workflow),
            "task",
            &json!({"plugin_execution":{"coordinator_run_id":root.id}}),
        )
        .await
        .unwrap();
        assert_eq!(
            late_child.status, "cancelled",
            "late child insert revived a fenced coordinator"
        );
        // Successful absence checks are not tombstone deletion: late Docker
        // creation after acknowledgement remains eligible on later passes.
        assert!(
            cleanup_candidates(&pool, "daemon-a", 100)
                .await
                .unwrap()
                .iter()
                .any(|e| e.id == first.id)
        );
        issue.provider_state = "closed".into();
        observe_issue(
            &pool,
            &IssueObservation {
                issue: &issue,
                updated_at: None,
                close_reason: Some("completed"),
                owner: "test",
                repo: "example",
                state_labels: vec![],
            },
        )
        .await
        .unwrap();
        issue.provider_state = "open".into();
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
        let new = create_job(&pool, Some(workflow), "developer", &json!({}))
            .await
            .unwrap();
        acquire_job_lease(&pool, new.id, "original", 60)
            .await
            .unwrap()
            .unwrap();
        mark_job_running(&pool, new.id).await.unwrap().unwrap();
        let current = register_container_execution(&pool, new.id, "original", "scope", "daemon-a")
            .await
            .unwrap();
        assert_eq!(current.generation, 2);
        assert!(
            cleanup_candidates(&pool, "daemon-a", 100)
                .await
                .unwrap()
                .iter()
                .all(|e| e.id != current.id)
        );
        // Unknown legacy daemon cannot be treated as absent on this daemon.
        sqlx::query("UPDATE container_executions SET docker_daemon_id=NULL WHERE id=$1")
            .bind(current.id)
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("UPDATE jobs SET lease_expires_at=now()-interval '1 second' WHERE id=$1")
            .bind(new.id)
            .execute(&pool)
            .await
            .unwrap();
        fence_expired_jobs(&pool, 100, &BTreeMap::new())
            .await
            .unwrap();
        record_cleanup(&pool, current.id, None).await.unwrap();
        finish_recovered_jobs(&pool).await.unwrap();
        assert_eq!(
            get_job(&pool, new.id).await.unwrap().unwrap().status,
            "cancel_requested"
        );
        // An expired lease is never silently transferred to another owner.
        let leased = create_job(&pool, None, "test", &json!({})).await.unwrap();
        acquire_job_lease(&pool, leased.id, "original", 60)
            .await
            .unwrap()
            .unwrap();
        sqlx::query("UPDATE jobs SET lease_expires_at=now()-interval '1 second' WHERE id=$1")
            .bind(leased.id)
            .execute(&pool)
            .await
            .unwrap();
        assert!(
            acquire_job_lease(&pool, leased.id, "replacement", 60)
                .await
                .unwrap()
                .is_none()
        );
        fence_expired_jobs(&pool, 100, &BTreeMap::new())
            .await
            .unwrap();
        fail_job(&pool, leased.id, &json!({})).await.unwrap();
        assert_eq!(
            get_job(&pool, leased.id).await.unwrap().unwrap().status,
            "cancelled"
        );
    }
}
