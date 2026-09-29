//! Reconcile local tracking selection without deleting history or replaying work.
use crate::{DbError, PgPool};
use donkeyspace_core::repository::RepositoryName;

const REASON: &str =
    "Repository removed from tracking; history retained, automatic replay disabled";

/// Called before scheduling or publishing. Re-adding enables new authorized work,
/// but old generations and their cancelled records remain fenced permanently.
/// Selection changes require a controlled API/worker restart on a single stack.
pub async fn reconcile(pool: &PgPool, selected: &[RepositoryName]) -> Result<(), DbError> {
    if selected.is_empty() {
        return Err(DbError::EmptyRepositorySelection);
    }
    let names = selected
        .iter()
        .map(|repo| repo.full_name().to_lowercase())
        .collect::<Vec<_>>();
    let mut tx = pool.begin().await?;
    let repositories: Vec<(i64, String, String, bool)> = sqlx::query_as(
        "SELECT id,owner,name,lower(owner||'/'||name)=ANY($1) AS selected
         FROM repositories WHERE provider='github'
         AND ((retired_at IS NULL AND NOT lower(owner||'/'||name)=ANY($1))
           OR (retired_at IS NOT NULL AND lower(owner||'/'||name)=ANY($1)))
         ORDER BY id FOR NO KEY UPDATE",
    )
    .bind(&names)
    .fetch_all(&mut *tx)
    .await?;
    for (repository, owner, name, tracked) in repositories {
        if tracked {
            sqlx::query("UPDATE repositories SET retired_at=NULL WHERE id=$1")
                .bind(repository)
                .execute(&mut *tx)
                .await?;
            continue;
        }
        // Same workflow lock used by side-effect and ingress admission. An
        // already admitted side effect completes before retirement commits.
        let workflows: Vec<(i64, Option<String>)> = sqlx::query_as(
            "SELECT id,current_state FROM workflow_items WHERE repository_id=$1 ORDER BY id FOR NO KEY UPDATE")
            .bind(repository).fetch_all(&mut *tx).await?;
        sqlx::query("UPDATE repositories SET retired_at=now() WHERE id=$1")
            .bind(repository)
            .execute(&mut *tx)
            .await?;
        for (workflow, previous) in workflows {
            let state: String = sqlx::query_scalar("UPDATE workflow_items SET generation=generation+1,
                current_state=CASE WHEN provider_state='closed' OR current_state='finished' THEN 'finished' ELSE 'needs_human' END,
                updated_at=now() WHERE id=$1 RETURNING current_state")
                .bind(workflow).fetch_one(&mut *tx).await?;
            sqlx::query("UPDATE jobs SET status=CASE WHEN status='running' THEN 'cancel_requested' ELSE 'cancelled' END,
                input=input||jsonb_build_object('donkeyspace_repository_retirement',$2::text), updated_at=now()
                WHERE workflow_item_id=$1 AND status IN ('waiting','queued','leased','running','paused')")
                .bind(workflow).bind(REASON).execute(&mut *tx).await?;
            sqlx::query(
                "UPDATE outbound_actions SET status='cancelled',last_error=$2,updated_at=now()
                WHERE workflow_item_id=$1 AND status IN ('pending','failed')",
            )
            .bind(workflow)
            .bind(REASON)
            .execute(&mut *tx)
            .await?;
            sqlx::query(
                "UPDATE agent_publications SET status='cancelled',last_error=$2,updated_at=now()
                WHERE workflow_item_id=$1 AND status IN ('pending','failed')",
            )
            .bind(workflow)
            .bind(REASON)
            .execute(&mut *tx)
            .await?;
            sqlx::query("UPDATE projected_work_items SET sync_status='cancelled',last_error=$2,updated_at=now()
                WHERE workflow_item_id=$1 AND sync_status IN ('pending','failed')")
                .bind(workflow).bind(REASON).execute(&mut *tx).await?;
            sqlx::query(
                "UPDATE approval_requests SET state='cancelled',updated_at=now()
                WHERE workflow_item_id=$1 AND state='pending'",
            )
            .bind(workflow)
            .execute(&mut *tx)
            .await?;
            sqlx::query(
                "INSERT INTO state_transitions(workflow_item_id,from_state,to_state,reason)
                VALUES($1,$2,$3,$4)",
            )
            .bind(workflow)
            .bind(previous)
            .bind(state)
            .bind(REASON)
            .execute(&mut *tx)
            .await?;
            sqlx::query("INSERT INTO lifecycle_events(workflow_item_id,event_type,level,source,status,summary,reason)
                VALUES($1,'repository_retired','milestone','worker','cancelled',$2,$2)")
                .bind(workflow).bind(REASON).execute(&mut *tx).await?;
        }
        // Legacy jobs may have repository input but no workflow foreign key.
        sqlx::query("UPDATE jobs SET status=CASE WHEN status='running' THEN 'cancel_requested' ELSE 'cancelled' END,
            input=input||jsonb_build_object('donkeyspace_repository_retirement',$3::text),updated_at=now()
            WHERE workflow_item_id IS NULL AND lower(input #>> '{repository,owner,login}')=lower($1)
            AND lower(input #>> '{repository,name}')=lower($2)
            AND status IN ('waiting','queued','leased','running','paused')")
            .bind(&owner).bind(&name).bind(REASON).execute(&mut *tx).await?;
    }
    tx.commit().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        DbConfig, RepositoryInput, WorkflowItemInput, apply_migrations,
        cancellation::{job_execution_allowed, lock_job_side_effect, lock_outbound_side_effect},
        connect, create_job, create_retry_job, get_job, upsert_repository, upsert_workflow_item,
    };
    use serde_json::json;
    use uuid::Uuid;

    async fn database() -> (DbConfig, PgPool) {
        let url = std::env::var("DONKEYSPACE_CANCELLATION_TEST_DATABASE_URL").unwrap();
        assert!(url.ends_with("/donkeyspace_cancellation_test"));
        let config = DbConfig::from_database_url(url);
        let pool = connect(&config).await.unwrap();
        apply_migrations(&pool).await.unwrap();
        apply_migrations(&pool).await.unwrap();
        (config, pool)
    }

    async fn repository(pool: &PgPool, owner: &str, name: &str, app: bool) -> (i64, i64) {
        let repository = upsert_repository(
            pool,
            &RepositoryInput {
                installation_external_id: app.then(|| Uuid::now_v7().to_string()),
                installation_account_login: app.then(|| owner.to_string()),
                provider: "github".into(),
                owner: owner.into(),
                name: name.into(),
                default_branch: "main".into(),
            },
        )
        .await
        .unwrap();
        let workflow = upsert_workflow_item(
            pool,
            &WorkflowItemInput {
                repository_id: repository,
                provider_issue_id: "1".into(),
                issue_number: 1,
                provider_state: "open".into(),
                current_state: Some("ready".into()),
                current_labels: vec![],
            },
        )
        .await
        .unwrap();
        (repository, workflow)
    }

    #[tokio::test]
    #[ignore = "requires isolated DONKEYSPACE_CANCELLATION_TEST_DATABASE_URL"]
    async fn retirement_preserves_history_and_never_replays_it_after_readding() {
        let (config, pool) = database().await;
        let owner = format!("retirement-{}", Uuid::now_v7());
        let (_, healthy) = repository(&pool, &owner, "healthy", true).await;
        let healthy_job = create_job(&pool, Some(healthy), "triage", &json!({}))
            .await
            .unwrap();
        let selected = RepositoryName::parse_list(&format!("{owner}/healthy")).unwrap();
        let mut retired = Vec::new();
        for (name, app) in [("removed-app", true), ("removed-pat", false)] {
            let (repo, workflow) = repository(&pool, &owner, name, app).await;
            let input = json!({"repository":{"owner":{"login":owner},"name":name}});
            let mut jobs = Vec::new();
            for status in [
                "waiting",
                "queued",
                "leased",
                "running",
                "paused",
                "completed",
                "failed",
            ] {
                let job = create_job(&pool, Some(workflow), "triage", &input)
                    .await
                    .unwrap();
                sqlx::query("UPDATE jobs SET status=$2,result=$3 WHERE id=$1")
                    .bind(job.id)
                    .bind(status)
                    .bind(json!({"summary":"retained result"}))
                    .execute(&pool)
                    .await
                    .unwrap();
                jobs.push((job.id, status));
            }
            let coordinator = jobs[3].0;
            let action: i64 = sqlx::query_scalar(
                "INSERT INTO outbound_actions(workflow_item_id,job_id,provider,action_type,payload)
                VALUES($1,$2,'github','issue.add_label','{}') RETURNING id",
            )
            .bind(workflow)
            .bind(coordinator)
            .fetch_one(&pool)
            .await
            .unwrap();
            for state in ["pending", "failed", "published"] {
                sqlx::query("INSERT INTO agent_publications(coordinator_job_id,workflow_item_id,kind,branch_name,commit_sha,html_url,local_repo_path,status)
                    VALUES($1,$2,'checkpoint',$3,'immutable-sha','retained-url','retained-path',$3)")
                    .bind(coordinator).bind(workflow).bind(state).execute(&pool).await.unwrap();
            }
            sqlx::query("INSERT INTO approval_requests(workflow_item_id,coordinator_job_id,target_task,trigger,approval_subject,result_summary)
                VALUES($1,$2,'architect','required','contract','retained proposal')")
                .bind(workflow).bind(coordinator).execute(&pool).await.unwrap();
            sqlx::query("INSERT INTO projected_work_items(workflow_item_id,coordinator_job_id,work_item,spec_path,body_digest)
                VALUES($1,$2,'block','src/docs/block.md','immutable-digest')")
                .bind(workflow).bind(coordinator).execute(&pool).await.unwrap();
            let orphan = create_job(&pool, None, "triage", &input).await.unwrap();
            // A historical managed PR must not generate a repair after removal
            // or after the repository is selected again.
            sqlx::query("INSERT INTO pull_requests(repository_id,workflow_item_id,provider_pr_id,pr_number,title,html_url,state,head_ref,base_ref,managed_by_donkeyspace)
                VALUES($1,$2,'old-pr',1,'old PR','retained-url','open',$3,'main',true)")
                .bind(repo).bind(workflow).bind(format!("donkeyspace/issue-1-{coordinator}")).execute(&pool).await.unwrap();
            retired.push((repo, workflow, input, jobs, action, orphan.id));
        }
        // Retirement respects the existing side-effect admission lock.
        let guard = lock_job_side_effect(&pool, retired[0].3[3].0)
            .await
            .unwrap();
        let other_pool = pool.clone();
        let selection = selected.clone();
        let mut removal = tokio::spawn(async move { reconcile(&other_pool, &selection).await });
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(40), &mut removal)
                .await
                .is_err()
        );
        let other_pool = pool.clone();
        let selection = selected.clone();
        let concurrent_removal =
            tokio::spawn(async move { reconcile(&other_pool, &selection).await });
        drop(guard);
        tokio::time::timeout(std::time::Duration::from_secs(5), removal)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(5), concurrent_removal)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(reconcile(&pool, &[]).await.is_err());
        for (_, workflow, input, jobs, action, orphan) in &retired {
            for (id, status) in jobs {
                let job = get_job(&pool, *id).await.unwrap().unwrap();
                let expected = match *status {
                    "running" => "cancel_requested",
                    "completed" => "completed",
                    "failed" => "failed",
                    _ => "cancelled",
                };
                assert_eq!(job.status, expected);
                assert_eq!(job.result.unwrap()["summary"], "retained result");
                assert!(!job_execution_allowed(&pool, *id).await.unwrap());
            }
            // Late legacy retries cannot reset retired effects to pending.
            for statement in [
                "UPDATE outbound_actions SET status='pending' WHERE workflow_item_id=$1",
                "UPDATE agent_publications SET status='pending' WHERE workflow_item_id=$1 AND status='cancelled'",
                "UPDATE approval_requests SET state='pending' WHERE workflow_item_id=$1",
                "UPDATE projected_work_items SET sync_status='pending' WHERE workflow_item_id=$1",
            ] {
                sqlx::query(statement)
                    .bind(workflow)
                    .execute(&pool)
                    .await
                    .unwrap();
            }
            assert!(
                lock_outbound_side_effect(&pool, *action)
                    .await
                    .unwrap()
                    .is_none()
            );
            assert_eq!(
                get_job(&pool, *orphan).await.unwrap().unwrap().status,
                "cancelled"
            );
            let publications: Vec<(String,String,String)> = sqlx::query_as("SELECT branch_name,status,commit_sha FROM agent_publications WHERE workflow_item_id=$1")
                .bind(workflow).fetch_all(&pool).await.unwrap();
            assert_eq!(publications.len(), 3);
            for (previous, status, sha) in publications {
                assert_eq!(
                    status,
                    if previous == "published" {
                        "published"
                    } else {
                        "cancelled"
                    }
                );
                assert_eq!(sha, "immutable-sha");
            }
            let approval: String =
                sqlx::query_scalar("SELECT state FROM approval_requests WHERE workflow_item_id=$1")
                    .bind(workflow)
                    .fetch_one(&pool)
                    .await
                    .unwrap();
            assert_eq!(approval, "cancelled");
            let projection: (String,String) = sqlx::query_as("SELECT sync_status,body_digest FROM projected_work_items WHERE workflow_item_id=$1")
                .bind(workflow).fetch_one(&pool).await.unwrap();
            assert_eq!(projection, ("cancelled".into(), "immutable-digest".into()));
            assert_eq!(
                crate::retry_projected_work_items(&pool, jobs[3].0)
                    .await
                    .unwrap(),
                0
            );
            let late = create_job(&pool, Some(*workflow), "triage", input)
                .await
                .unwrap();
            assert_eq!(late.status, "cancelled");
            let late_orphan = create_job(&pool, None, "triage", input).await.unwrap();
            assert_eq!(late_orphan.status, "cancelled");
        }
        assert!(job_execution_allowed(&pool, healthy_job.id).await.unwrap());
        assert!(
            crate::acquire_job_lease(&pool, healthy_job.id, "healthy-worker", 60)
                .await
                .unwrap()
                .is_some()
        );
        pool.close().await;
        let pool = connect(&config).await.unwrap();
        reconcile(&pool, &selected).await.unwrap();
        let restored = RepositoryName::parse_list(&format!(
            "{owner}/healthy,{owner}/REMOVED-APP,{owner}/removed-pat"
        ))
        .unwrap();
        reconcile(&pool, &restored).await.unwrap();
        reconcile(&pool, &restored).await.unwrap();
        for (repo, workflow, input, jobs, action, _) in &retired {
            let counts: (i64,i64,bool) = sqlx::query_as("SELECT generation,(SELECT count(*) FROM lifecycle_events WHERE workflow_item_id=$1 AND event_type='repository_retired'),(SELECT retired_at IS NULL FROM repositories WHERE id=$2) FROM workflow_items WHERE id=$1")
                .bind(workflow).bind(repo).fetch_one(&pool).await.unwrap();
            assert_eq!(counts, (2, 1, true));
            assert!(!job_execution_allowed(&pool, jobs[3].0).await.unwrap());
            assert!(lock_job_side_effect(&pool, jobs[3].0).await.is_err());
            assert!(
                lock_outbound_side_effect(&pool, *action)
                    .await
                    .unwrap()
                    .is_none()
            );
            assert!(
                crate::resume_latest_paused_job(&pool, *workflow, input)
                    .await
                    .unwrap()
                    .is_none()
            );
            let old = get_job(&pool, jobs[6].0).await.unwrap().unwrap();
            let retry = create_retry_job(&pool, Some(*workflow), old.id, &old.role, &old.input)
                .await
                .unwrap();
            assert_eq!(retry.status, "cancelled");
            assert_eq!(
                crate::retry_projected_work_items(&pool, jobs[3].0)
                    .await
                    .unwrap(),
                0
            );
            let fresh = create_job(&pool, Some(*workflow), "triage", input)
                .await
                .unwrap();
            assert_eq!(fresh.status, "queued");
            assert_eq!(fresh.input["donkeyspace_workflow_generation"], 2);
        }
        let repairs = crate::list_repair_candidates(&pool, 50).await.unwrap();
        assert!(
            !repairs
                .iter()
                .any(|candidate| retired.iter().any(|r| r.1 == candidate.workflow_item_id))
        );
    }

    #[tokio::test]
    #[ignore = "requires isolated DONKEYSPACE_CANCELLATION_TEST_DATABASE_URL"]
    async fn closure_cleanup_backoff_is_bounded_and_access_errors_do_not_replay() {
        let (config, pool) = database().await;
        let owner = format!("cleanup-retry-{}", Uuid::now_v7());
        let (_, workflow) = repository(&pool, &owner, "tracked", false).await;
        sqlx::query("UPDATE workflow_items SET provider_state='closed',current_state='finished' WHERE id=$1")
            .bind(workflow).execute(&pool).await.unwrap();
        let action: i64 = sqlx::query_scalar(
            "INSERT INTO outbound_actions(workflow_item_id,provider,action_type,payload)
            VALUES($1,'github','issue.remove_labels','{\"closure_cleanup\":true}') RETURNING id",
        )
        .bind(workflow)
        .fetch_one(&pool)
        .await
        .unwrap();
        for attempt in 1..=8 {
            crate::mark_outbound_action_failed(&pool, action, "temporary network failure")
                .await
                .unwrap();
            crate::cancellation::reconcile_closed_workflows(&pool, &[])
                .await
                .unwrap();
            let record: (String,i32,f64) = sqlx::query_as("SELECT status,retry_count,extract(epoch FROM next_attempt_at-updated_at)::double precision FROM outbound_actions WHERE id=$1")
                .bind(action).fetch_one(&pool).await.unwrap();
            assert_eq!(record.0, "failed");
            assert_eq!(record.1, attempt);
            assert_eq!(
                record.2,
                f64::from((30 * 2_i32.pow((attempt - 1) as u32)).min(900))
            );
            sqlx::query("UPDATE outbound_actions SET updated_at=now()-interval '31 seconds',next_attempt_at=now() WHERE id=$1")
                .bind(action).execute(&pool).await.unwrap();
            crate::cancellation::reconcile_closed_workflows(&pool, &[])
                .await
                .unwrap();
            let state: String =
                sqlx::query_scalar("SELECT status FROM outbound_actions WHERE id=$1")
                    .bind(action)
                    .fetch_one(&pool)
                    .await
                    .unwrap();
            assert_eq!(state, if attempt < 8 { "pending" } else { "failed" });
        }
        sqlx::query("UPDATE outbound_actions SET retry_count=0,status='pending' WHERE id=$1")
            .bind(action)
            .execute(&pool)
            .await
            .unwrap();
        crate::mark_outbound_action_failed(&pool, action, "GitHub 404: access unavailable")
            .await
            .unwrap();
        sqlx::query("UPDATE outbound_actions SET updated_at=now()-interval '1 hour',next_attempt_at=now() WHERE id=$1")
            .bind(action).execute(&pool).await.unwrap();
        pool.close().await;
        let pool = connect(&config).await.unwrap();
        crate::cancellation::reconcile_closed_workflows(&pool, &[])
            .await
            .unwrap();
        let state: (String, i32) =
            sqlx::query_as("SELECT status,retry_count FROM outbound_actions WHERE id=$1")
                .bind(action)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(state, ("failed".into(), 1));
        let retired: bool =
            sqlx::query_scalar("SELECT retired_at IS NOT NULL FROM repositories WHERE owner=$1")
                .bind(owner)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert!(
            !retired,
            "access failure must not retire a tracked repository"
        );
    }
}
