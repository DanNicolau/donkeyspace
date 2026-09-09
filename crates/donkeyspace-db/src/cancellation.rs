//! Durable issue closure and execution fencing. Historical rows are retained.
use crate::{DbError, PgPool, WorkflowItemInput};
use chrono::{DateTime, Utc};
use serde_json::json;
use sqlx::{FromRow, Postgres, Transaction};
use uuid::Uuid;

#[derive(FromRow)]
struct Workflow {
    id: i64,
    provider_state: String,
    provider_updated_at: Option<DateTime<Utc>>,
    current_state: Option<String>,
    generation: i64,
}

pub struct IssueObservation<'a> {
    pub issue: &'a WorkflowItemInput,
    pub updated_at: Option<DateTime<Utc>>,
    pub close_reason: Option<&'a str>,
    pub owner: &'a str,
    pub repo: &'a str,
    pub state_labels: Vec<String>,
}

/// Returns None for an older provider snapshot. Repeat observations converge
/// without duplicating closure history or cleanup actions.
pub async fn observe_issue(
    pool: &PgPool,
    observation: &IssueObservation<'_>,
) -> Result<Option<i64>, DbError> {
    let input = observation.issue;
    let mut tx = pool.begin().await?;
    sqlx::query("INSERT INTO workflow_items (repository_id, provider_issue_id, issue_number) VALUES ($1,$2,$3) ON CONFLICT DO NOTHING")
        .bind(input.repository_id).bind(&input.provider_issue_id).bind(input.issue_number)
        .execute(&mut *tx).await?;
    let old: Workflow = sqlx::query_as(
        "SELECT * FROM workflow_items WHERE repository_id=$1 AND provider_issue_id=$2 FOR NO KEY UPDATE",
    )
    .bind(input.repository_id)
    .bind(&input.provider_issue_id)
    .fetch_one(&mut *tx)
    .await?;
    if observation.updated_at.is_none() && old.provider_updated_at.is_some() {
        return Ok(None);
    }
    if observation
        .updated_at
        .zip(old.provider_updated_at)
        .is_some_and(|(new, old)| new < old)
    {
        return Ok(None);
    }
    let closed = input.provider_state == "closed";
    let reopened = !closed && old.provider_state == "closed";
    let needs_closure = closed
        && (old.provider_state != "closed" || old.current_state.as_deref() != Some("finished"));
    let generation = old.generation + i64::from(reopened);
    let state = if closed {
        Some("finished")
    } else if reopened {
        None
    } else {
        input
            .current_state
            .as_deref()
            .or(old.current_state.as_deref())
    };
    sqlx::query("UPDATE workflow_items SET provider_state=$2, provider_close_reason=$3, provider_updated_at=COALESCE($4,provider_updated_at), current_state=$5, current_labels=$6, generation=$7, updated_at=now() WHERE id=$1")
        .bind(old.id).bind(&input.provider_state).bind(observation.close_reason).bind(observation.updated_at)
        .bind(state).bind(json!(input.current_labels)).bind(generation).execute(&mut *tx).await?;
    if closed {
        sqlx::query("UPDATE jobs SET status=CASE WHEN status='running' THEN 'cancel_requested' ELSE 'cancelled' END, updated_at=now() WHERE workflow_item_id=$1 AND status IN ('waiting','queued','leased','running','paused')")
            .bind(old.id).execute(&mut *tx).await?;
        sqlx::query("UPDATE outbound_actions SET status='cancelled', updated_at=now() WHERE workflow_item_id=$1 AND status='pending' AND NOT (job_id IS NULL AND action_type='issue.remove_labels' AND COALESCE(payload->>'closure_cleanup'='true',false))")
            .bind(old.id).execute(&mut *tx).await?;
        sqlx::query("UPDATE agent_publications SET status='cancelled', updated_at=now() WHERE workflow_item_id=$1 AND status IN ('pending','failed')")
            .bind(old.id).execute(&mut *tx).await?;
        sqlx::query("UPDATE approval_requests SET state='cancelled', updated_at=now() WHERE workflow_item_id=$1 AND state='pending'")
            .bind(old.id).execute(&mut *tx).await?;
        if needs_closure {
            sqlx::query("INSERT INTO outbound_actions (workflow_item_id, provider, action_type, payload) VALUES ($1,'github','issue.remove_labels',$2)")
                .bind(old.id).bind(json!({"owner": observation.owner, "repo": observation.repo, "issue_number": input.issue_number, "labels": observation.state_labels, "closure_cleanup": true}))
                .execute(&mut *tx).await?;
        }
    }
    if needs_closure || reopened {
        let status = if closed { "finished" } else { "reopened" };
        let reason = if closed {
            "GitHub issue closed; workflow execution cancelled"
        } else {
            "GitHub issue reopened; starting a fresh workflow generation"
        };
        sqlx::query("INSERT INTO state_transitions (workflow_item_id, from_state, to_state, reason) VALUES ($1,$2,$3,$4)")
            .bind(old.id).bind(old.current_state).bind(status).bind(reason).execute(&mut *tx).await?;
        sqlx::query("INSERT INTO lifecycle_events (workflow_item_id,event_type,level,source,status,summary,reason) VALUES ($1,'workflow_transition','milestone','github',$2,$3,$3)")
            .bind(old.id).bind(status).bind(reason).execute(&mut *tx).await?;
    }
    tx.commit().await?;
    Ok(Some(old.id))
}

/// Hold this transaction across a remote side effect. Closure uses the same row
/// lock: a side effect admitted before closure finishes before closure commits.
pub async fn lock_job_side_effect(
    pool: &PgPool,
    job_id: Uuid,
) -> Result<Transaction<'static, Postgres>, DbError> {
    let mut tx = pool.begin().await?;
    let allowed: Option<bool> = sqlx::query_scalar("SELECT w.provider_state <> 'closed' AND w.generation=j.generation AND j.status NOT IN ('cancel_requested','cancelled') FROM workflow_items w JOIN jobs j ON j.workflow_item_id=w.id WHERE j.id=$1 FOR NO KEY UPDATE OF w")
        .bind(job_id).fetch_optional(&mut *tx).await?;
    if allowed != Some(true) {
        return Err(DbError::ExecutionCancelled);
    }
    Ok(tx)
}

pub async fn job_execution_allowed(pool: &PgPool, job_id: Uuid) -> Result<bool, DbError> {
    Ok(sqlx::query_scalar::<_,bool>("SELECT j.status NOT IN ('cancel_requested','cancelled') AND (w.id IS NULL OR (w.provider_state <> 'closed' AND w.generation=j.generation)) FROM jobs j LEFT JOIN workflow_items w ON w.id=j.workflow_item_id WHERE j.id=$1")
        .bind(job_id).fetch_optional(pool).await?.unwrap_or(false))
}

pub async fn finish_job_cancellation(pool: &PgPool, job_id: Uuid) -> Result<(), DbError> {
    sqlx::query("UPDATE jobs SET status='cancelled', lease_owner=NULL, lease_expires_at=NULL, updated_at=now() WHERE (id=$1 OR input #>> '{plugin_execution,coordinator_run_id}'=$1::text) AND status='cancel_requested'")
        .bind(job_id).execute(pool).await?;
    Ok(())
}

pub async fn heartbeat_job(
    pool: &PgPool,
    job_id: Uuid,
    owner: &str,
    lease_seconds: i32,
) -> Result<bool, DbError> {
    Ok(sqlx::query("UPDATE jobs SET lease_expires_at=now()+make_interval(secs => $3), updated_at=now() WHERE id=$1 AND lease_owner=$2 AND status IN ('leased','running')")
        .bind(job_id).bind(owner).bind(lease_seconds as f64).execute(pool).await?.rows_affected()==1)
}

pub async fn update_workflow_state_for_job(
    pool: &PgPool,
    workflow: i64,
    job: Uuid,
    state: &str,
) -> Result<(), DbError> {
    sqlx::query("UPDATE workflow_items w SET current_state=$3, updated_at=now() WHERE w.id=$1 AND w.provider_state <> 'closed' AND EXISTS (SELECT 1 FROM jobs j WHERE j.id=$2 AND j.workflow_item_id=w.id AND j.generation=w.generation AND j.status NOT IN ('cancel_requested','cancelled'))")
        .bind(workflow).bind(job).bind(state).execute(pool).await?;
    Ok(())
}

pub async fn lock_outbound_side_effect(
    pool: &PgPool,
    action_id: i64,
) -> Result<Option<Transaction<'static, Postgres>>, DbError> {
    let mut tx = pool.begin().await?;
    let allowed: Option<bool> = sqlx::query_scalar("SELECT a.status='pending' AND a.generation=w.generation AND (j.id IS NULL OR j.status NOT IN ('cancel_requested','cancelled')) AND (w.provider_state <> 'closed' OR (a.job_id IS NULL AND a.action_type='issue.remove_labels' AND COALESCE(a.payload->>'closure_cleanup'='true',false))) FROM outbound_actions a JOIN workflow_items w ON w.id=a.workflow_item_id LEFT JOIN jobs j ON j.id=a.job_id WHERE a.id=$1 FOR NO KEY UPDATE OF w")
        .bind(action_id).fetch_optional(&mut *tx).await?;
    if allowed != Some(true) {
        sqlx::query("UPDATE outbound_actions SET status='cancelled',updated_at=now() WHERE id=$1 AND status='pending'")
            .bind(action_id).execute(&mut *tx).await?;
        tx.commit().await?;
        return Ok(None);
    }
    Ok(Some(tx))
}

/// Apply PR-driven state and label changes under the same lock as closure and
/// reopen. Return the admitted generation for any subsequently created job.
/// A caller's earlier PR/workflow snapshot is never sufficient authorization.
pub async fn apply_pull_request_effects(
    pool: &PgPool,
    workflow: i64,
    provider_pr_id: &str,
    state: Option<&str>,
    actions: &[(String, serde_json::Value)],
) -> Result<Option<i64>, DbError> {
    let mut tx = pool.begin().await?;
    let current: Workflow =
        sqlx::query_as("SELECT * FROM workflow_items WHERE id=$1 FOR NO KEY UPDATE")
            .bind(workflow)
            .fetch_one(&mut *tx)
            .await?;
    // Read after acquiring the workflow lock, including when admission waited
    // for an in-flight close/reopen transaction.
    let pr_generation: Option<i64> = sqlx::query_scalar(
        "SELECT generation FROM pull_requests WHERE workflow_item_id=$1 AND provider_pr_id=$2",
    )
    .bind(workflow)
    .bind(provider_pr_id)
    .fetch_optional(&mut *tx)
    .await?;
    if current.provider_state == "closed" || pr_generation != Some(current.generation) {
        return Ok(None);
    }
    if let Some(state) = state
        && current.current_state.as_deref() != Some(state)
    {
        let reason = format!("managed pull request transitioned workflow to {state}");
        sqlx::query("UPDATE workflow_items SET current_state=$2,updated_at=now() WHERE id=$1")
            .bind(workflow)
            .bind(state)
            .execute(&mut *tx)
            .await?;
        sqlx::query("INSERT INTO state_transitions (workflow_item_id,from_state,to_state,reason) VALUES ($1,$2,$3,$4)")
            .bind(workflow).bind(current.current_state).bind(state).bind(&reason)
            .execute(&mut *tx).await?;
        sqlx::query("INSERT INTO lifecycle_events (workflow_item_id,event_type,level,source,status,summary,reason) VALUES ($1,'workflow_transition','milestone','system',$2,$3,$3)")
            .bind(workflow).bind(state).bind(&reason).execute(&mut *tx).await?;
    }
    for (action_type, payload) in actions {
        sqlx::query("INSERT INTO outbound_actions (workflow_item_id,provider,action_type,payload) VALUES ($1,'github',$2,$3)")
            .bind(workflow).bind(action_type).bind(payload).execute(&mut *tx).await?;
    }
    tx.commit().await?;
    Ok(Some(current.generation))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        DbConfig, OutboundActionInput, RepositoryInput, acquire_job_lease, apply_migrations,
        complete_job, connect, create_job, create_outbound_action, create_waiting_job, fail_job,
        get_job, mark_job_running, resume_latest_paused_job, upsert_repository,
    };
    use chrono::Duration;

    #[tokio::test]
    #[ignore = "requires isolated DONKEYSPACE_CANCELLATION_TEST_DATABASE_URL"]
    async fn late_managed_prs_keep_origin_generation_and_unknown_branches_stay_fenced() {
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
                owner: format!("late-pr-{}", Uuid::now_v7()),
                name: "umbrella".into(),
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
            current_state: None,
            current_labels: vec![],
        };
        let observe = |issue: WorkflowItemInput| {
            let pool = pool.clone();
            async move {
                observe_issue(
                    &pool,
                    &IssueObservation {
                        issue: &issue,
                        updated_at: None,
                        close_reason: None,
                        owner: "test",
                        repo: "umbrella",
                        state_labels: vec![],
                    },
                )
                .await
                .unwrap()
                .unwrap()
            }
        };
        let workflow = observe(issue.clone()).await;
        let old = create_job(&pool, Some(workflow), "developer", &json!({}))
            .await
            .unwrap();
        sqlx::query("INSERT INTO agent_publications (coordinator_job_id,workflow_item_id,kind,branch_name,commit_sha,html_url,local_repo_path) VALUES ($1,$2,'checkpoint','test/issue-1-legacy','abc','https://example.invalid','/tmp/test')")
            .bind(old.id).bind(workflow).execute(&pool).await.unwrap();
        issue.provider_state = "closed".into();
        observe(issue.clone()).await;
        issue.provider_state = "open".into();
        observe(issue.clone()).await;
        let current = create_job(&pool, Some(workflow), "developer", &json!({}))
            .await
            .unwrap();
        let record = |branch: String, workflow_id: Option<i64>| {
            let pool = pool.clone();
            async move {
                let pr = crate::PullRequestInput {
                    repository_id: repository,
                    workflow_item_id: workflow_id,
                    provider_pr_id: Uuid::now_v7().to_string(),
                    pr_number: 10,
                    title: "test".into(),
                    html_url: "https://example.invalid".into(),
                    state: "open".into(),
                    head_ref: branch,
                    head_sha: Some("abc".into()),
                    base_ref: "main".into(),
                    base_sha: None,
                    managed_by_donkeyspace: true,
                };
                crate::upsert_pull_request(&pool, &pr).await.unwrap();
                pr
            }
        };
        let generation = |id: String| {
            let pool = pool.clone();
            async move {
                sqlx::query_scalar::<_,i64>("SELECT generation FROM pull_requests WHERE repository_id=$1 AND provider_pr_id=$2")
                .bind(repository).bind(id).fetch_one(&pool).await.unwrap()
            }
        };
        for branch in [
            format!("test/issue-1-{}", old.id),
            format!("test/attempt-1-{}-rtl-block-a1", old.id),
            "test/issue-1-legacy".into(),
        ] {
            let pr = record(branch, Some(workflow)).await;
            assert_eq!(generation(pr.provider_pr_id.clone()).await, 1);
            assert!(
                apply_pull_request_effects(
                    &pool,
                    workflow,
                    &pr.provider_pr_id,
                    Some("pr_open"),
                    &[("issue.add_label".into(), json!({"label":"ai:pr-open"}))]
                )
                .await
                .unwrap()
                .is_none()
            );
            crate::upsert_pull_request(&pool, &pr).await.unwrap();
            assert_eq!(generation(pr.provider_pr_id.clone()).await, 1);
        }
        // Neither a missing UUID nor an unrecognized legacy branch may be
        // silently adopted by the current generation after reopening.
        for branch in [
            format!("test/issue-1-{}", Uuid::now_v7()),
            "test/issue-1-unknown".into(),
        ] {
            let pr = record(branch, Some(workflow)).await;
            assert_eq!(generation(pr.provider_pr_id.clone()).await, 0);
            crate::upsert_pull_request(&pool, &pr).await.unwrap();
            assert_eq!(generation(pr.provider_pr_id.clone()).await, 0);
            assert!(
                apply_pull_request_effects(
                    &pool,
                    workflow,
                    &pr.provider_pr_id,
                    Some("pr_open"),
                    &[]
                )
                .await
                .unwrap()
                .is_none()
            );
        }
        let pr = record(format!("test/issue-1-{}", current.id), Some(workflow)).await;
        assert_eq!(generation(pr.provider_pr_id.clone()).await, 2);
        assert_eq!(
            apply_pull_request_effects(&pool, workflow, &pr.provider_pr_id, Some("pr_open"), &[])
                .await
                .unwrap(),
            Some(2)
        );
        // A PR first observed without its linked issue can be attributed later.
        let mut unlinked = record(format!("test/issue-1-{}", old.id), None).await;
        assert_eq!(generation(unlinked.provider_pr_id.clone()).await, 0);
        unlinked.workflow_item_id = Some(workflow);
        crate::upsert_pull_request(&pool, &unlinked).await.unwrap();
        assert_eq!(generation(unlinked.provider_pr_id.clone()).await, 1);
        // Ambiguous legacy branch reuse must not pick whichever generation is newer.
        sqlx::query("INSERT INTO agent_publications (coordinator_job_id,workflow_item_id,kind,branch_name,commit_sha,html_url,local_repo_path) VALUES ($1,$2,'checkpoint','test/issue-1-legacy','def','https://example.invalid','/tmp/test')")
            .bind(current.id).bind(workflow).execute(&pool).await.unwrap();
        let ambiguous = record("test/issue-1-legacy".into(), Some(workflow)).await;
        assert_eq!(generation(ambiguous.provider_pr_id).await, 0);
        // Closing/reopening again cannot rewrite an already attributed PR.
        issue.provider_state = "closed".into();
        observe(issue.clone()).await;
        issue.provider_state = "open".into();
        observe(issue.clone()).await;
        crate::upsert_pull_request(&pool, &pr).await.unwrap();
        assert_eq!(generation(pr.provider_pr_id.clone()).await, 2);
        assert!(
            !pull_request_is_current(&pool, workflow, &pr.provider_pr_id)
                .await
                .unwrap()
        );
        assert_eq!(
            get_job(&pool, old.id).await.unwrap().unwrap().status,
            "cancelled"
        );
    }

    #[tokio::test]
    #[ignore = "requires isolated DONKEYSPACE_CANCELLATION_TEST_DATABASE_URL"]
    async fn pr_effects_and_followup_jobs_cannot_cross_reopen() {
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
                owner: format!("pr-race-{}", Uuid::now_v7()),
                name: "umbrella".into(),
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
            current_state: None,
            current_labels: vec![],
        };
        let observe = |issue: WorkflowItemInput| {
            let pool = pool.clone();
            async move {
                observe_issue(
                    &pool,
                    &IssueObservation {
                        issue: &issue,
                        updated_at: None,
                        close_reason: None,
                        owner: "test",
                        repo: "umbrella",
                        state_labels: vec!["ai:pr-open".into()],
                    },
                )
                .await
                .unwrap()
                .unwrap()
            }
        };
        let workflow = observe(issue.clone()).await;
        let mut pr = crate::PullRequestInput {
            repository_id: repository,
            workflow_item_id: Some(workflow),
            provider_pr_id: "old".into(),
            pr_number: 2,
            title: "test".into(),
            html_url: "https://example.invalid".into(),
            state: "open".into(),
            head_ref: "test".into(),
            head_sha: None,
            base_ref: "main".into(),
            base_sha: None,
            managed_by_donkeyspace: true,
        };
        crate::upsert_pull_request(&pool, &pr).await.unwrap();
        let actions = vec![("issue.add_label".into(), json!({"label":"ai:pr-open"}))];
        let admitted =
            apply_pull_request_effects(&pool, workflow, "old", Some("pr_open"), &actions)
                .await
                .unwrap()
                .unwrap();
        assert_eq!(admitted, 1);
        // This is the old API's successful precheck. Pause that request here,
        // then close/reopen before it can write state, history, labels or a job.
        assert!(
            pull_request_is_current(&pool, workflow, "old")
                .await
                .unwrap()
        );
        issue.provider_state = "closed".into();
        observe(issue.clone()).await;
        assert!(
            apply_pull_request_effects(&pool, workflow, "old", Some("pr_open"), &actions)
                .await
                .unwrap()
                .is_none()
        );
        issue.provider_state = "open".into();
        observe(issue.clone()).await;
        let counts_before: (i64, i64) = sqlx::query_as("SELECT (SELECT count(*) FROM state_transitions WHERE workflow_item_id=$1),(SELECT count(*) FROM outbound_actions WHERE workflow_item_id=$1)")
            .bind(workflow).fetch_one(&pool).await.unwrap();
        assert!(
            apply_pull_request_effects(&pool, workflow, "old", Some("pr_open"), &actions)
                .await
                .unwrap()
                .is_none()
        );
        let counts_after: (i64, i64) = sqlx::query_as("SELECT (SELECT count(*) FROM state_transitions WHERE workflow_item_id=$1),(SELECT count(*) FROM outbound_actions WHERE workflow_item_id=$1)")
            .bind(workflow).fetch_one(&pool).await.unwrap();
        assert_eq!(
            counts_after, counts_before,
            "old event emitted history or labels"
        );
        let state: Option<String> =
            sqlx::query_scalar("SELECT current_state FROM workflow_items WHERE id=$1")
                .bind(workflow)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert!(state.is_none(), "old PR restored pr_open after reopen");
        let late_reviewer = create_job(
            &pool,
            Some(workflow),
            "reviewer",
            &json!({"donkeyspace_workflow_generation":admitted}),
        )
        .await
        .unwrap();
        assert_eq!(late_reviewer.status, "cancelled");
        // A newly admitted PR still works and stamps its actions with generation 2.
        pr.provider_pr_id = "new".into();
        pr.pr_number = 3;
        let current_job = create_job(&pool, Some(workflow), "developer", &json!({}))
            .await
            .unwrap();
        pr.head_ref = format!("test/issue-1-{}", current_job.id);
        crate::upsert_pull_request(&pool, &pr).await.unwrap();
        assert_eq!(
            apply_pull_request_effects(&pool, workflow, "new", Some("pr_open"), &actions)
                .await
                .unwrap(),
            Some(2)
        );
        let action: (String, i64) = sqlx::query_as("SELECT status,generation FROM outbound_actions WHERE workflow_item_id=$1 ORDER BY id DESC LIMIT 1")
            .bind(workflow).fetch_one(&pool).await.unwrap();
        assert_eq!(action, ("pending".into(), 2));
        let state: String =
            sqlx::query_scalar("SELECT current_state FROM workflow_items WHERE id=$1")
                .bind(workflow)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(state, "pr_open");
    }

    #[tokio::test]
    #[ignore = "requires isolated DONKEYSPACE_CANCELLATION_TEST_DATABASE_URL"]
    async fn closure_cancels_jobs_fences_publication_and_reopen_starts_fresh() {
        let url = std::env::var("DONKEYSPACE_CANCELLATION_TEST_DATABASE_URL").unwrap();
        assert!(
            url.ends_with("/donkeyspace_cancellation_test"),
            "use the disposable cancellation test database"
        );
        let pool = connect(&DbConfig::from_database_url(url)).await.unwrap();
        apply_migrations(&pool).await.unwrap();
        apply_migrations(&pool).await.unwrap();
        for close_reason in ["completed", "not_planned"] {
            let repository = upsert_repository(
                &pool,
                &RepositoryInput {
                    installation_external_id: None,
                    installation_account_login: None,
                    provider: "github".into(),
                    owner: format!("test-{}", Uuid::now_v7()),
                    name: "umbrella".into(),
                    default_branch: "main".into(),
                },
            )
            .await
            .unwrap();
            let mut issue = WorkflowItemInput {
                repository_id: repository,
                provider_issue_id: "100".into(),
                issue_number: 1,
                provider_state: "open".into(),
                current_state: Some("in_progress".into()),
                current_labels: vec!["ai:in-progress".into()],
            };
            let now = Utc::now();
            let observe = |issue: WorkflowItemInput, date, reason| {
                let pool = pool.clone();
                async move {
                    observe_issue(
                        &pool,
                        &IssueObservation {
                            issue: &issue,
                            updated_at: Some(date),
                            close_reason: reason,
                            owner: "test",
                            repo: "umbrella",
                            state_labels: vec!["ai:in-progress".into()],
                        },
                    )
                    .await
                    .unwrap()
                }
            };
            let workflow = observe(issue.clone(), now, None).await.unwrap();
            let input = json!({"issue":{"state":"open"}});
            let root = create_job(&pool, Some(workflow), "developer", &input)
                .await
                .unwrap();
            acquire_job_lease(&pool, root.id, "owner", 30)
                .await
                .unwrap()
                .unwrap();
            mark_job_running(&pool, root.id).await.unwrap().unwrap();
            assert!(heartbeat_job(&pool, root.id, "owner", 60).await.unwrap());
            assert!(!heartbeat_job(&pool, root.id, "other", 60).await.unwrap());
            let child = create_waiting_job(
                &pool,
                Some(workflow),
                "rtl",
                &json!({"plugin_execution":{"coordinator_run_id":root.id}}),
            )
            .await
            .unwrap();
            crate::start_waiting_job(&pool, child.id)
                .await
                .unwrap()
                .unwrap();
            let queued = create_job(&pool, Some(workflow), "triage", &input)
                .await
                .unwrap();
            let leased = create_job(&pool, Some(workflow), "reviewer", &input)
                .await
                .unwrap();
            acquire_job_lease(&pool, leased.id, "other", 30)
                .await
                .unwrap()
                .unwrap();
            let paused = create_waiting_job(&pool, Some(workflow), "architect", &input)
                .await
                .unwrap();
            sqlx::query("UPDATE jobs SET status='paused',result=$2 WHERE id=$1")
                .bind(paused.id)
                .bind(json!({"summary":"retained checkpoint"}))
                .execute(&pool)
                .await
                .unwrap();
            let action = create_outbound_action(
                &pool,
                &OutboundActionInput {
                    workflow_item_id: workflow,
                    job_id: Some(root.id),
                    provider: "github".into(),
                    action_type: "issue.add_label".into(),
                    payload: json!({"label":"ai:in-progress"}),
                },
            )
            .await
            .unwrap();
            let publication: i64 = sqlx::query_scalar("INSERT INTO agent_publications (coordinator_job_id, workflow_item_id, kind, branch_name, commit_sha, html_url, local_repo_path) VALUES ($1,$2,'checkpoint','test','abc','https://example.invalid','/tmp/test') RETURNING id")
                .bind(root.id).bind(workflow).fetch_one(&pool).await.unwrap();
            crate::upsert_pull_request(
                &pool,
                &crate::PullRequestInput {
                    repository_id: repository,
                    workflow_item_id: Some(workflow),
                    provider_pr_id: "200".into(),
                    pr_number: 2,
                    title: "old generation".into(),
                    html_url: "https://example.invalid".into(),
                    state: "open".into(),
                    head_ref: "test".into(),
                    head_sha: None,
                    base_ref: "main".into(),
                    base_sha: None,
                    managed_by_donkeyspace: true,
                },
            )
            .await
            .unwrap();
            assert!(
                pull_request_is_current(&pool, workflow, "200")
                    .await
                    .unwrap()
            );
            issue.provider_state = "closed".into();
            observe(
                issue.clone(),
                now + Duration::seconds(1),
                Some(close_reason),
            )
            .await
            .unwrap();
            observe(
                issue.clone(),
                now + Duration::seconds(1),
                Some(close_reason),
            )
            .await
            .unwrap();
            let state: (String, String) = sqlx::query_as(
                "SELECT current_state,provider_close_reason FROM workflow_items WHERE id=$1",
            )
            .bind(workflow)
            .fetch_one(&pool)
            .await
            .unwrap();
            assert_eq!(state, ("finished".into(), close_reason.into()));
            for id in [root.id, child.id] {
                assert_eq!(
                    get_job(&pool, id).await.unwrap().unwrap().status,
                    "cancel_requested"
                );
            }
            for id in [queued.id, leased.id, paused.id] {
                assert_eq!(
                    get_job(&pool, id).await.unwrap().unwrap().status,
                    "cancelled"
                );
            }
            assert!(
                complete_job(&pool, root.id, &json!({"late":true}))
                    .await
                    .unwrap()
                    .is_none()
            );
            assert_eq!(
                fail_job(&pool, root.id, &json!({"late":true}))
                    .await
                    .unwrap()
                    .unwrap()
                    .status,
                "cancel_requested"
            );
            assert!(!job_execution_allowed(&pool, root.id).await.unwrap());
            let publication_status: String = sqlx::query_scalar(
                "UPDATE agent_publications SET status='pending' WHERE id=$1 RETURNING status",
            )
            .bind(publication)
            .fetch_one(&pool)
            .await
            .unwrap();
            assert_eq!(publication_status, "cancelled");
            let late_approval: String = sqlx::query_scalar("INSERT INTO approval_requests (workflow_item_id,coordinator_job_id,target_task,trigger,approval_subject,result_summary) VALUES ($1,$2,'rtl','required','test','late result') RETURNING state")
                .bind(workflow).bind(root.id).fetch_one(&pool).await.unwrap();
            assert_eq!(late_approval, "cancelled");
            assert!(
                lock_outbound_side_effect(&pool, action.id)
                    .await
                    .unwrap()
                    .is_none()
            );
            let cleanup: Vec<i64> = sqlx::query_scalar(
                "SELECT id FROM outbound_actions WHERE workflow_item_id=$1 AND status='pending'",
            )
            .bind(workflow)
            .fetch_all(&pool)
            .await
            .unwrap();
            assert_eq!(cleanup.len(), 1);
            assert!(
                lock_outbound_side_effect(&pool, cleanup[0])
                    .await
                    .unwrap()
                    .is_some()
            );
            let count:i64=sqlx::query_scalar("SELECT count(*) FROM state_transitions WHERE workflow_item_id=$1 AND to_state='finished'").bind(workflow).fetch_one(&pool).await.unwrap();
            assert_eq!(count, 1);
            let late = create_job(&pool, Some(workflow), "late", &root.input)
                .await
                .unwrap();
            assert_eq!(late.status, "cancelled");
            finish_job_cancellation(&pool, root.id).await.unwrap();
            assert_eq!(
                get_job(&pool, child.id).await.unwrap().unwrap().status,
                "cancelled"
            );
            assert_eq!(
                get_job(&pool, paused.id)
                    .await
                    .unwrap()
                    .unwrap()
                    .result
                    .unwrap()["summary"],
                "retained checkpoint"
            );
            issue.provider_state = "open".into();
            assert!(observe(issue.clone(), now, None).await.is_none());
            observe(issue.clone(), now + Duration::seconds(2), None)
                .await
                .unwrap();
            assert!(
                !pull_request_is_current(&pool, workflow, "200")
                    .await
                    .unwrap()
            );
            assert!(
                resume_latest_paused_job(&pool, workflow, &input)
                    .await
                    .unwrap()
                    .is_none()
            );
            assert!(
                lock_outbound_side_effect(&pool, cleanup[0])
                    .await
                    .unwrap()
                    .is_none()
            );
            let fresh = create_job(&pool, Some(workflow), "triage", &input)
                .await
                .unwrap();
            assert_eq!(fresh.status, "queued");
            assert_eq!(fresh.input["donkeyspace_workflow_generation"], 2);
            let stale = create_job(&pool, Some(workflow), "stale", &root.input)
                .await
                .unwrap();
            assert_eq!(stale.status, "cancelled");
            update_workflow_state_for_job(&pool, workflow, root.id, "in_progress")
                .await
                .unwrap();
            let state: Option<String> =
                sqlx::query_scalar("SELECT current_state FROM workflow_items WHERE id=$1")
                    .bind(workflow)
                    .fetch_one(&pool)
                    .await
                    .unwrap();
            assert!(state.is_none());
            // Close serialization: an admitted side effect finishes before the
            // close transaction can commit; subsequent side effects are denied.
            let guard = lock_job_side_effect(&pool, fresh.id).await.unwrap();
            issue.provider_state = "closed".into();
            let closing = tokio::spawn(observe(
                issue,
                now + Duration::seconds(3),
                Some(close_reason),
            ));
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            assert!(!closing.is_finished());
            // Projection metadata uses another pool connection and takes a
            // foreign-key KEY SHARE lock. It must not deadlock with its own
            // admission guard, even while closure is waiting for that guard.
            tokio::time::timeout(std::time::Duration::from_secs(1),
                sqlx::query("INSERT INTO lifecycle_events (workflow_item_id,event_type,level,source,summary) VALUES ($1,'projection','detail','test','admitted projection')")
                    .bind(workflow).execute(&pool)
            ).await.expect("projection metadata blocked on its own guard").unwrap();
            drop(guard);
            closing.await.unwrap().unwrap();
            assert!(lock_job_side_effect(&pool, fresh.id).await.is_err());
            let legacy = crate::upsert_workflow_item(
                &pool,
                &WorkflowItemInput {
                    repository_id: repository,
                    provider_issue_id: "legacy".into(),
                    issue_number: 3,
                    provider_state: "closed".into(),
                    current_state: Some("in_progress".into()),
                    current_labels: vec!["ai:in-progress".into()],
                },
            )
            .await
            .unwrap();
            reconcile_closed_workflows(&pool, &["ai:in-progress".into()])
                .await
                .unwrap();
            let legacy_state: String =
                sqlx::query_scalar("SELECT current_state FROM workflow_items WHERE id=$1")
                    .bind(legacy)
                    .fetch_one(&pool)
                    .await
                    .unwrap();
            assert_eq!(legacy_state, "finished");
            sqlx::query("UPDATE outbound_actions SET status='failed',updated_at=now()-interval '31 seconds' WHERE workflow_item_id=$1")
                .bind(legacy).execute(&pool).await.unwrap();
            for _ in 0..2 {
                reconcile_closed_workflows(&pool, &["ai:in-progress".into()])
                    .await
                    .unwrap();
            }
            let retries: i64 = sqlx::query_scalar("SELECT count(*) FROM outbound_actions WHERE workflow_item_id=$1 AND status='pending'")
                .bind(legacy).fetch_one(&pool).await.unwrap();
            assert_eq!(retries, 1);
        }
    }
}

pub async fn pull_request_is_current(
    pool: &PgPool,
    workflow: i64,
    provider_pr_id: &str,
) -> Result<bool, DbError> {
    Ok(sqlx::query_scalar::<_,bool>("SELECT p.generation=w.generation AND w.provider_state <> 'closed' FROM pull_requests p JOIN workflow_items w ON w.id=p.workflow_item_id WHERE w.id=$1 AND p.provider_pr_id=$2")
        .bind(workflow).bind(provider_pr_id).fetch_optional(pool).await?.unwrap_or(false))
}

/// Converge closed issues ingested by older versions, without resuming any work.
pub async fn reconcile_closed_workflows(
    pool: &PgPool,
    state_labels: &[String],
) -> Result<(), DbError> {
    #[derive(FromRow)]
    struct Closed {
        repository_id: i64,
        provider_issue_id: String,
        issue_number: i64,
        provider_updated_at: Option<DateTime<Utc>>,
        provider_close_reason: Option<String>,
        current_state: Option<String>,
        current_labels: serde_json::Value,
        owner: String,
        name: String,
    }
    let rows:Vec<Closed>=sqlx::query_as("SELECT w.*, r.owner, r.name FROM workflow_items w JOIN repositories r ON r.id=w.repository_id WHERE w.provider_state='closed' AND w.current_state IS DISTINCT FROM 'finished' ORDER BY w.id LIMIT 100")
        .fetch_all(pool).await?;
    for row in rows {
        observe_issue(
            pool,
            &IssueObservation {
                issue: &WorkflowItemInput {
                    repository_id: row.repository_id,
                    provider_issue_id: row.provider_issue_id,
                    issue_number: row.issue_number,
                    provider_state: "closed".into(),
                    current_state: row.current_state,
                    current_labels: serde_json::from_value(row.current_labels).unwrap_or_default(),
                },
                updated_at: row.provider_updated_at,
                close_reason: row.provider_close_reason.as_deref(),
                owner: &row.owner,
                repo: &row.name,
                state_labels: state_labels.to_vec(),
            },
        )
        .await?;
    }
    sqlx::query("UPDATE outbound_actions a SET status='pending',updated_at=now() FROM workflow_items w WHERE w.id=a.workflow_item_id AND w.provider_state='closed' AND w.generation=a.generation AND a.action_type='issue.remove_labels' AND a.payload->>'closure_cleanup'='true' AND a.status='failed' AND a.updated_at < now()-interval '30 seconds'")
        .execute(pool).await?;
    Ok(())
}
