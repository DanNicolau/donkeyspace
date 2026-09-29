//! The commit boundary for lifecycle decisions. No network or model work belongs here.
use crate::{ApprovalRequestInput, DbError, JobRecord, OutboundActionInput, PgPool};
use donkeyspace_core::TaskKey;
use serde_json::Value;
use sqlx::FromRow;
use uuid::Uuid;

#[derive(Debug, FromRow)]
pub struct CheckpointRecord {
    pub generation: i64,
    pub revision: i64,
    pub version: i32,
    pub state: Value,
    pub completed: bool,
}

pub async fn load(pool: &PgPool, coordinator: Uuid) -> Result<Option<CheckpointRecord>, DbError> {
    let record: Option<CheckpointRecord> =
        sqlx::query_as("SELECT * FROM lifecycle_checkpoints WHERE coordinator_job_id=$1")
            .bind(coordinator)
            .fetch_optional(pool)
            .await?;
    if let Some(record) = &record {
        let generation: i64 = sqlx::query_scalar("SELECT generation FROM jobs WHERE id=$1")
            .bind(coordinator)
            .fetch_one(pool)
            .await?;
        if record.generation != generation {
            return Err(DbError::CheckpointConflict);
        }
    }
    Ok(record)
}

/// Retry the existing coordinator so its accepted checkpoints and approvals
/// retain their identity. A new job UUID cannot resume this state.
pub async fn retry_failed(pool: &PgPool, coordinator: Uuid) -> Result<Option<JobRecord>, DbError> {
    let Some(job) = crate::get_job(pool, coordinator).await? else {
        return Ok(None);
    };
    let mut tx = pool.begin().await?;
    if let Some(workflow) = job.workflow_item_id {
        crate::ingress::lock_workflow(&mut tx, workflow).await?;
        if crate::active_job_exists_for_workflow_item(&mut *tx, workflow).await? {
            return Ok(None);
        }
    }
    if !crate::cancellation::job_execution_allowed(&mut *tx, coordinator).await? {
        return Ok(None);
    }
    let resumed = sqlx::query_as::<_, JobRecord>(
        "UPDATE jobs j SET status='queued', result=NULL, lease_owner=NULL,
            lease_expires_at=NULL, updated_at=now(),
            input=jsonb_set(input,'{donkeyspace_resume}','true'::jsonb)
         WHERE j.id=$1 AND j.status='failed'
           AND j.input->>'donkeyspace_lifecycle_coordinator'='true'
           AND COALESCE(j.result->>'outcome','failed') NOT IN ('blocked','needs_human')
           AND EXISTS (SELECT 1 FROM lifecycle_checkpoints c WHERE c.coordinator_job_id=j.id
               AND c.generation=j.generation AND NOT c.completed
               AND c.state->'repository_snapshot'->>'commit_sha' IS NOT NULL)
         RETURNING j.*",
    )
    .bind(coordinator)
    .fetch_optional(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(resumed)
}

#[derive(Default)]
pub struct Effects {
    pub waiting_children: Vec<WaitingChild>,
    pub starting_children: Vec<Uuid>,
    pub events: Vec<crate::LifecycleEventInput>,
    pub approvals: Vec<ApprovalRequestInput>,
    pub decisions: Vec<(TaskKey, String)>,
    pub invalidated_tasks: Vec<TaskKey>,
    pub child_results: Vec<(Uuid, Value)>,
    pub failed_children: Vec<(Uuid, Value)>,
    pub pause_result: Option<Value>,
    pub outbound: Vec<OutboundActionInput>,
}

pub struct WaitingChild {
    pub id: Uuid,
    pub role: String,
    pub input: Value,
}

pub struct Commit<'a> {
    pub coordinator: &'a JobRecord,
    pub expected_revision: i64,
    pub version: i32,
    pub state: &'a Value,
    pub completed: bool,
    pub effects: &'a Effects,
}

pub async fn save(pool: &PgPool, commit: Commit<'_>) -> Result<i64, DbError> {
    let job = commit.coordinator;
    if job.lease_owner.is_none() {
        return Err(DbError::ExecutionCancelled);
    }
    let mut tx = pool.begin().await?;
    // Same lock order as closure: workflow, then jobs. Check owner and lease
    // under the lock so a stale executor cannot commit after reassignment.
    if let Some(workflow) = job.workflow_item_id {
        crate::ingress::lock_workflow(&mut tx, workflow).await?;
    }
    let (allowed, generation): (bool, i64) = sqlx::query_as(
        "SELECT status='running' AND lease_owner IS NOT DISTINCT FROM $2 AND lease_expires_at > clock_timestamp(), generation FROM jobs WHERE id=$1 FOR UPDATE",
    ).bind(job.id).bind(&job.lease_owner).fetch_one(&mut *tx).await?;
    if !allowed || !crate::cancellation::job_execution_allowed(&mut *tx, job.id).await? {
        return Err(DbError::ExecutionCancelled);
    }
    let revision: Option<i64> = sqlx::query_scalar(
        "INSERT INTO lifecycle_checkpoints (coordinator_job_id,generation,revision,version,state,completed)
         SELECT $1,$2,1,$4,$5,$6 WHERE $3::bigint=0
         ON CONFLICT (coordinator_job_id) DO NOTHING RETURNING revision",
    ).bind(job.id).bind(generation).bind(commit.expected_revision).bind(commit.version)
        .bind(commit.state).bind(commit.completed).fetch_optional(&mut *tx).await?;
    let revision = if let Some(revision) = revision {
        revision
    } else {
        sqlx::query_scalar("UPDATE lifecycle_checkpoints SET revision=revision+1,version=$4,state=$5,completed=$6,updated_at=now()
            WHERE coordinator_job_id=$1 AND generation=$2 AND revision=$3 AND NOT completed RETURNING revision")
            .bind(job.id).bind(generation).bind(commit.expected_revision).bind(commit.version)
            .bind(commit.state).bind(commit.completed).fetch_optional(&mut *tx).await?
            .ok_or(DbError::CheckpointConflict)?
    };
    for (target, state) in &commit.effects.decisions {
        crate::transition_approval_request(
            &mut *tx,
            job.id,
            &target.task,
            target.work_item.as_deref(),
            state,
        )
        .await?;
    }
    for approval in &commit.effects.approvals {
        crate::upsert_approval_request(&mut *tx, approval).await?;
    }
    for target in &commit.effects.invalidated_tasks {
        sqlx::query(
            "UPDATE approval_requests SET state='superseded', updated_at=now()
            WHERE coordinator_job_id=$1 AND target_task=$2
              AND target_work_item IS NOT DISTINCT FROM $3 AND state IN ('pending','approved')",
        )
        .bind(job.id)
        .bind(&target.task)
        .bind(&target.work_item)
        .execute(&mut *tx)
        .await?;
        // Preserve the original result and publication as historical evidence.
        // Only its authority to release dependents is superseded.
        sqlx::query(
            "UPDATE jobs SET status='superseded',updated_at=now()
            WHERE input #>> '{plugin_execution,coordinator_run_id}'=$1
              AND input #>> '{plugin_execution,task}'=$2
              AND input #>> '{plugin_execution,work_item,id}' IS NOT DISTINCT FROM $3
              AND status IN ('waiting','completed','failed')",
        )
        .bind(job.id.to_string())
        .bind(&target.task)
        .bind(&target.work_item)
        .execute(&mut *tx)
        .await?;
    }
    let finished = commit
        .effects
        .child_results
        .iter()
        .map(|(id, result)| (id, result, "completed"))
        .chain(
            commit
                .effects
                .failed_children
                .iter()
                .map(|(id, result)| (id, result, "failed")),
        );
    for (child, result, status) in finished {
        sqlx::query("UPDATE jobs SET status=$4,result=$3,lease_owner=NULL,lease_expires_at=NULL,updated_at=now()
            WHERE id=$1 AND input #>> '{plugin_execution,coordinator_run_id}'=$2 AND status='running'")
            .bind(child).bind(job.id.to_string()).bind(result).bind(status).execute(&mut *tx).await?;
    }
    for child in &commit.effects.waiting_children {
        if child
            .input
            .pointer("/plugin_execution/coordinator_run_id")
            .and_then(Value::as_str)
            != Some(job.id.to_string().as_str())
        {
            return Err(DbError::CheckpointConflict);
        }
        sqlx::query("INSERT INTO jobs (id,workflow_item_id,role,status,input) VALUES ($1,$2,$3,'waiting',$4)")
            .bind(child.id).bind(job.workflow_item_id).bind(&child.role).bind(&child.input).execute(&mut *tx).await?;
    }
    for child in &commit.effects.starting_children {
        let started = sqlx::query("UPDATE jobs SET status='running',updated_at=now() WHERE id=$1 AND status='waiting' AND input #>> '{plugin_execution,coordinator_run_id}'=$2")
            .bind(child).bind(job.id.to_string()).execute(&mut *tx).await?.rows_affected();
        if started != 1 {
            return Err(DbError::CheckpointConflict);
        }
    }
    for event in &commit.effects.events {
        crate::record_lifecycle_event_on(&mut tx, event).await?;
    }
    if let Some(result) = &commit.effects.pause_result {
        let state = match result.get("outcome").and_then(Value::as_str) {
            Some("needs_info") => "needs_info",
            Some("needs_human") => "needs_human",
            _ => return Err(DbError::CheckpointConflict),
        };
        sqlx::query("UPDATE jobs SET status='paused',result=$2,lease_owner=NULL,lease_expires_at=NULL,updated_at=now() WHERE id=$1")
            .bind(job.id).bind(result).execute(&mut *tx).await?;
        if let Some(workflow) = job.workflow_item_id {
            sqlx::query("UPDATE workflow_items SET current_state=$2,updated_at=now() WHERE id=$1")
                .bind(workflow)
                .bind(state)
                .execute(&mut *tx)
                .await?;
            crate::record_state_transition_on(
                &mut tx,
                workflow,
                Some(job.id),
                Some("in_progress"),
                state,
                "Lifecycle checkpoint requires human input.",
            )
            .await?;
        }
    }
    for action in &commit.effects.outbound {
        crate::create_outbound_action(&mut *tx, action).await?;
    }
    if let Some(workflow) = job.workflow_item_id {
        sqlx::query("INSERT INTO lifecycle_events (workflow_item_id,coordinator_job_id,job_id,dedupe_key,event_type,level,source,summary)
            VALUES ($1,$2,$2,$3,'checkpoint_committed','detail','worker',$4) ON CONFLICT DO NOTHING")
            .bind(workflow).bind(job.id).bind(format!("checkpoint:{}:{revision}",job.id))
            .bind(format!("Lifecycle checkpoint revision {revision} committed.")).execute(&mut *tx).await?;
    }
    tx.commit().await?;
    Ok(revision)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::*;
    use serde_json::json;

    #[tokio::test]
    #[ignore = "requires isolated DONKEYSPACE_CANCELLATION_TEST_DATABASE_URL"]
    async fn checkpoint_commit_is_atomic_revision_checked_and_execution_fenced() {
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
                owner: format!("checkpoint-{}", Uuid::now_v7()),
                name: "test".into(),
                default_branch: "main".into(),
            },
        )
        .await
        .unwrap();
        let workflow = upsert_workflow_item(
            &pool,
            &WorkflowItemInput {
                repository_id: repository,
                provider_issue_id: "1".into(),
                issue_number: 1,
                provider_state: "open".into(),
                current_state: Some("in_progress".into()),
                current_labels: vec![],
            },
        )
        .await
        .unwrap();
        let job = create_job(&pool, Some(workflow), "developer", &json!({}))
            .await
            .unwrap();
        acquire_job_lease(&pool, job.id, "checkpoint-test", 120)
            .await
            .unwrap()
            .unwrap();
        let job = mark_job_running(&pool, job.id).await.unwrap().unwrap();
        let state = json!({"completed_keys":[{"task":"rtl","work_item":"sibling"}]});
        let mut effects = Effects::default();
        effects.approvals.push(ApprovalRequestInput {
            workflow_item_id: workflow,
            coordinator_job_id: job.id,
            target_task: "dv".into(),
            target_work_item: Some("target".into()),
            purpose: "accept_result".into(),
            trigger: "required".into(),
            approval_subject: "verification".into(),
            result_summary: "ready".into(),
            changed_files: json!([]),
            proposed_publication_id: None,
            projected_issues: json!([]),
            downstream_tasks: json!([]),
        });
        let child_id = Uuid::now_v7();
        effects.waiting_children.push(WaitingChild {
            id: child_id,
            role: "rtl".into(),
            input: json!({"plugin_execution":{"coordinator_run_id":job.id}}),
        });
        effects.starting_children.push(child_id);
        effects.outbound.push(OutboundActionInput {
            workflow_item_id: -1,
            job_id: Some(job.id),
            provider: "github".into(),
            action_type: "issue.comment".into(),
            payload: json!({}),
        });
        macro_rules! commit {
            ($revision:expr, $effects:expr) => {
                Commit {
                    coordinator: &job,
                    expected_revision: $revision,
                    version: 5,
                    state: &state,
                    completed: false,
                    effects: $effects,
                }
            };
        }
        assert!(save(&pool, commit!(0, &effects)).await.is_err());
        assert!(load(&pool, job.id).await.unwrap().is_none());
        assert!(
            list_approval_requests_for_run(&pool, job.id)
                .await
                .unwrap()
                .is_empty()
        );
        assert!(get_job(&pool, child_id).await.unwrap().is_none());
        effects.outbound[0].workflow_item_id = workflow;
        assert_eq!(save(&pool, commit!(0, &effects)).await.unwrap(), 1);
        assert_eq!(load(&pool, job.id).await.unwrap().unwrap().state, state);
        assert_eq!(
            get_job(&pool, child_id).await.unwrap().unwrap().status,
            "running"
        );
        // A later failure must roll back revision invalidation as well as the
        // checkpoint. A stale approval must never be half-superseded.
        let mut rejected_revision = Effects::default();
        rejected_revision.invalidated_tasks.push(TaskKey {
            task: "dv".into(),
            work_item: Some("target".into()),
        });
        rejected_revision.outbound.push(OutboundActionInput {
            workflow_item_id: -1,
            job_id: Some(job.id),
            provider: "github".into(),
            action_type: "issue.comment".into(),
            payload: json!({}),
        });
        assert!(save(&pool, commit!(1, &rejected_revision)).await.is_err());
        assert_eq!(load(&pool, job.id).await.unwrap().unwrap().revision, 1);
        assert!(
            list_approval_requests_for_run(&pool, job.id)
                .await
                .unwrap()
                .iter()
                .all(|approval| approval.state == "pending")
        );
        let empty = Effects::default();
        assert!(matches!(
            save(&pool, commit!(0, &empty)).await,
            Err(DbError::CheckpointConflict)
        ));
        let mut stale_owner = job.clone();
        stale_owner.lease_owner = Some("lost-owner".into());
        assert!(matches!(
            save(
                &pool,
                Commit {
                    coordinator: &stale_owner,
                    ..commit!(1, &empty)
                }
            )
            .await,
            Err(DbError::ExecutionCancelled)
        ));
        sqlx::query("UPDATE jobs SET lease_expires_at=now()-interval '1 second' WHERE id=$1")
            .bind(job.id)
            .execute(&pool)
            .await
            .unwrap();
        assert!(matches!(
            save(&pool, commit!(1, &empty)).await,
            Err(DbError::ExecutionCancelled)
        ));
        sqlx::query("UPDATE jobs SET lease_expires_at=now()+interval '1 minute' WHERE id=$1")
            .bind(job.id)
            .execute(&pool)
            .await
            .unwrap();
        let mut pause = Effects::default();
        pause.decisions.push((
            TaskKey {
                task: "dv".into(),
                work_item: Some("target".into()),
            },
            "approved".into(),
        ));
        pause.pause_result = Some(json!({"outcome":"needs_human"}));
        assert_eq!(save(&pool, commit!(1, &pause)).await.unwrap(), 2);
        assert_eq!(
            get_job(&pool, job.id).await.unwrap().unwrap().status,
            "paused"
        );
        assert_eq!(
            list_approval_requests_for_run(&pool, job.id).await.unwrap()[0].state,
            "approved"
        );
        assert!(matches!(
            save(&pool, commit!(2, &empty)).await,
            Err(DbError::ExecutionCancelled)
        ));
    }
}
