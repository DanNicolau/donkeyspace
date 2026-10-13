//! Durable admission, provenance and continuation for revisions of managed PRs.
use crate::{DbError, JobRecord, PgPool};
use serde_json::{Value, json};
use sqlx::FromRow;
use uuid::Uuid;

#[derive(Debug, FromRow)]
pub struct RevisionSource {
    pub workflow_item_id: i64,
    pub issue_number: i64,
    pub job_id: Uuid,
    pub input: Value,
    pub checkpoint: Option<Value>,
}

pub async fn source(
    pool: &PgPool,
    repository: i64,
    pr: i64,
) -> Result<Option<RevisionSource>, DbError> {
    Ok(sqlx::query_as(
        r#"SELECT w.id AS workflow_item_id, w.issue_number, j.id AS job_id, j.input,
                  COALESCE(s.checkpoint, c.state) AS checkpoint
           FROM pull_requests p JOIN workflow_items w ON w.id=p.workflow_item_id
           JOIN LATERAL (
             SELECT j.* FROM jobs j WHERE j.workflow_item_id=w.id AND j.generation=w.generation
               AND j.input #>> '{plugin_execution,coordinator_run_id}' IS NULL
               AND (j.role='developer' OR j.input->>'donkeyspace_lifecycle_coordinator'='true')
               AND (EXISTS (SELECT 1 FROM agent_publications a WHERE a.coordinator_job_id=j.id
                            AND a.metadata->>'pull_request_url'=p.html_url AND a.status='published')
                    OR EXISTS (SELECT 1 FROM pr_revisions r WHERE r.job_id=j.id AND r.status='published' AND r.pr_number=p.pr_number AND r.repository_id=p.repository_id))
             ORDER BY j.created_at DESC LIMIT 1
           ) j ON true LEFT JOIN lifecycle_snapshots s ON s.job_id=j.id
           LEFT JOIN lifecycle_checkpoints c ON c.coordinator_job_id=j.id AND c.completed AND c.generation=j.generation
           WHERE p.repository_id=$1 AND p.pr_number=$2 AND p.managed_by_donkeyspace
             AND p.state='open' AND p.generation=w.generation AND w.provider_state='open'
             AND w.current_state IS DISTINCT FROM 'finished'"#,
    ).bind(repository).bind(pr).fetch_optional(pool).await?)
}

pub async fn save_snapshot(pool: &PgPool, job: Uuid, checkpoint: &Value) -> Result<(), DbError> {
    sqlx::query("INSERT INTO lifecycle_snapshots(job_id,checkpoint) VALUES($1,$2) ON CONFLICT(job_id) DO UPDATE SET checkpoint=$2,updated_at=now()")
        .bind(job).bind(checkpoint).execute(pool).await?;
    Ok(())
}

pub async fn snapshot(pool: &PgPool, job: Uuid) -> Result<Option<Value>, DbError> {
    let retained = sqlx::query_scalar("SELECT checkpoint FROM lifecycle_snapshots WHERE job_id=$1")
        .bind(job)
        .fetch_optional(pool)
        .await?;
    if retained.is_some() {
        return Ok(retained);
    }
    Ok(crate::lifecycle_checkpoints::load(pool, job)
        .await?
        .filter(|record| record.completed)
        .map(|record| record.state))
}

pub enum Admission {
    Queued(Box<JobRecord>),
    Duplicate,
    Unavailable,
}

pub async fn active(pool: &PgPool, repository: i64, pr: i64) -> Result<Option<JobRecord>, DbError> {
    Ok(sqlx::query_as("SELECT j.* FROM pr_revisions r JOIN jobs j ON j.id=r.job_id JOIN workflow_items w ON w.id=r.workflow_item_id WHERE r.repository_id=$1 AND r.pr_number=$2 AND j.generation=w.generation AND j.status IN ('queued','leased','running','paused','cancel_requested') ORDER BY r.id DESC LIMIT 1")
        .bind(repository).bind(pr).fetch_optional(pool).await?)
}

pub enum ResumeAdmission {
    Resumed(Box<JobRecord>),
    Duplicate,
    Rejected(String),
}

pub struct ResumeRequest<'a> {
    pub repository: i64,
    pub pr: i64,
    pub job: &'a JobRecord,
    pub head: &'a str,
    pub delivery: &'a str,
    pub input: &'a Value,
    pub receipt: &'a Value,
}

/// Resume exactly the PR's paused coordinator, committing its decision receipt
/// and audit together. Concurrent polling/webhook delivery cannot replay it.
pub async fn resume(pool: &PgPool, request: ResumeRequest<'_>) -> Result<ResumeAdmission, DbError> {
    let workflow = request
        .job
        .workflow_item_id
        .ok_or(DbError::StaleAdmission)?;
    let mut tx = pool.begin().await?;
    crate::ingress::lock_workflow(&mut tx, workflow).await?;
    let duplicate: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM webhook_deliveries WHERE delivery_id=$1 OR (repository_id=$2 AND github_comment_id=$3))")
        .bind(request.delivery).bind(request.repository).bind(request.receipt["comment"]["id"].as_i64()).fetch_one(&mut *tx).await?;
    if duplicate {
        return Ok(ResumeAdmission::Duplicate);
    }
    let current: Option<JobRecord> = sqlx::query_as("SELECT j.* FROM jobs j JOIN pr_revisions r ON r.job_id=j.id JOIN workflow_items w ON w.id=j.workflow_item_id JOIN pull_requests p ON p.repository_id=r.repository_id AND p.pr_number=r.pr_number AND p.workflow_item_id=w.id WHERE j.id=$1 AND r.repository_id=$2 AND r.pr_number=$3 AND j.status='paused' AND j.generation=w.generation AND p.generation=w.generation AND p.state='open' AND p.managed_by_donkeyspace AND w.provider_state='open' AND workflow_repository_tracked(w.id) FOR UPDATE OF j")
        .bind(request.job.id).bind(request.repository).bind(request.pr).fetch_optional(&mut *tx).await?;
    let Some(current) = current else {
        return Ok(ResumeAdmission::Rejected("This revision is no longer paused or the PR is no longer eligible. Check its latest progress before posting a new command.".into()));
    };
    if current.input["pr_revision"]["base_sha"].as_str() != Some(request.head) {
        return Ok(ResumeAdmission::Rejected("The PR head changed while this run was paused. The preserved run cannot safely resume against a different head; review the branch change before continuing.".into()));
    }
    if crate::active_job_exists_for_workflow_item(&mut *tx, workflow).await? {
        return Ok(ResumeAdmission::Rejected("Other work is running on this workflow. Wait for it to finish before continuing the paused revision.".into()));
    }
    let decision = &request.input["donkeyspace_human_decision"];
    if current.input["donkeyspace_lifecycle_coordinator"] == true {
        let checkpoint: Option<Value> = sqlx::query_scalar(
            "SELECT state FROM lifecycle_checkpoints WHERE coordinator_job_id=$1 AND NOT completed AND generation=(SELECT generation FROM jobs WHERE id=$1)",
        )
        .bind(current.id)
        .fetch_optional(&mut *tx)
        .await?;
        let Some(checkpoint) = checkpoint else {
            return Ok(ResumeAdmission::Rejected("The paused run's checkpoint is unavailable. Restore its checkpoint before retrying the command.".into()));
        };
        if let Err(reason) = donkeyspace_core::pr_revision::validate_decision(&checkpoint, decision)
        {
            return Ok(ResumeAdmission::Rejected(reason));
        }
    } else if decision["target"]
        .as_str()
        .is_some_and(|target| target != "developer")
    {
        return Ok(ResumeAdmission::Rejected(
            "Use approve developer or revise developer for this paused run.".into(),
        ));
    }
    let Some(receipt) = crate::record_webhook_delivery(
        &mut *tx,
        Some(request.repository),
        request.delivery,
        "issue_comment",
        request.receipt,
    )
    .await?
    else {
        return Ok(ResumeAdmission::Duplicate);
    };
    let engagement = &request.input["donkeyspace_engagement"];
    crate::record_engagement_decision(
        &mut *tx,
        &crate::EngagementDecisionInput {
            webhook_delivery_id: receipt,
            workflow_item_id: Some(workflow),
            gate: "needs_human_resume".into(),
            disposition: "allowed".into(),
            actor: request.receipt.get("sender").cloned(),
            matched_selector: engagement
                .get("matched_selector")
                .filter(|v| !v.is_null())
                .cloned(),
            reason: engagement["reason"]
                .as_str()
                .unwrap_or("Authorized PR continuation")
                .into(),
        },
    )
    .await?;
    let mut input = current.input.clone();
    for key in [
        "issue",
        "comment",
        "sender",
        "donkeyspace_engagement",
        "donkeyspace_human_decision",
    ] {
        input[key] = request.input[key].clone();
    }
    input["donkeyspace_resume"] = json!(true);
    let resumed: JobRecord = sqlx::query_as("UPDATE jobs SET status='queued',input=$2,result=NULL,lease_owner=NULL,lease_expires_at=NULL,updated_at=now() WHERE id=$1 RETURNING *")
        .bind(current.id).bind(input).fetch_one(&mut *tx).await?;
    sqlx::query(
        "UPDATE workflow_items SET current_state='in_progress',updated_at=now() WHERE id=$1",
    )
    .bind(workflow)
    .execute(&mut *tx)
    .await?;
    sqlx::query("INSERT INTO lifecycle_events(workflow_item_id,coordinator_job_id,job_id,event_type,level,source,summary,actor) VALUES($1,$2,$2,'pr_revision_resumed','milestone','github',$3,$4)")
        .bind(workflow).bind(current.id).bind(format!("PR decision accepted: {} {}",decision["action"].as_str().unwrap_or_default(),decision["target"].as_str().unwrap_or("pending target")))
        .bind(request.receipt["sender"]["login"].as_str()).execute(&mut *tx).await?;
    tx.commit().await?;
    Ok(ResumeAdmission::Resumed(Box::new(resumed)))
}

/// Lock the workflow across eligibility, deduplication and enqueueing. A second
/// command cannot start competing work; webhook delivery IDs alone are not
/// sufficient because polling observes the same comment under a different ID.
#[allow(clippy::too_many_arguments)]
pub async fn enqueue(
    pool: &PgPool,
    repository: i64,
    pr: i64,
    comment: i64,
    source: &RevisionSource,
    role: &str,
    input: &Value,
) -> Result<Admission, DbError> {
    let mut tx = pool.begin().await?;
    let eligible: Option<bool> = sqlx::query_scalar(
        "SELECT w.provider_state='open' AND w.current_state IS DISTINCT FROM 'finished' AND w.generation=j.generation FROM workflow_items w JOIN jobs j ON j.id=$2 AND j.workflow_item_id=w.id WHERE w.id=$1 FOR NO KEY UPDATE OF w",
    ).bind(source.workflow_item_id).bind(source.job_id).fetch_optional(&mut *tx).await?;
    let duplicate: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM pr_revisions WHERE repository_id=$1 AND comment_id=$2)",
    )
    .bind(repository)
    .bind(comment)
    .fetch_one(&mut *tx)
    .await?;
    if duplicate {
        return Ok(Admission::Duplicate);
    }
    let active: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM jobs WHERE workflow_item_id=$1 AND status IN ('waiting','queued','leased','running','paused','cancel_requested'))")
        .bind(source.workflow_item_id).fetch_one(&mut *tx).await?;
    let open: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pull_requests p JOIN workflow_items w ON w.id=p.workflow_item_id WHERE p.repository_id=$1 AND p.pr_number=$2 AND p.workflow_item_id=$3 AND p.generation=w.generation AND p.state='open' AND p.managed_by_donkeyspace)")
        .bind(repository).bind(pr).bind(source.workflow_item_id).fetch_one(&mut *tx).await?;
    let newer_publication: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pr_revisions r JOIN jobs j ON j.id=r.job_id JOIN jobs original ON original.id=$3 WHERE r.repository_id=$1 AND r.pr_number=$2 AND r.status='published' AND j.created_at>original.created_at)")
        .bind(repository).bind(pr).bind(source.job_id).fetch_one(&mut *tx).await?;
    if eligible != Some(true) || active || !open || newer_publication {
        return Ok(Admission::Unavailable);
    }
    let job: JobRecord = sqlx::query_as("INSERT INTO jobs(id,workflow_item_id,role,status,input) VALUES($1,$2,$3,'queued',$4) RETURNING *")
        .bind(Uuid::now_v7()).bind(source.workflow_item_id).bind(role).bind(input).fetch_one(&mut *tx).await?;
    sqlx::query("INSERT INTO pr_revisions(repository_id,workflow_item_id,pr_number,comment_id,source_job_id,job_id,request,head_sha) VALUES($1,$2,$3,$4,$5,$6,$7,$8)")
        .bind(repository).bind(source.workflow_item_id).bind(pr).bind(comment).bind(source.job_id).bind(job.id)
        .bind(&input["pr_revision"]).bind(input["pull_request"]["head"]["sha"].as_str().unwrap_or_default()).execute(&mut *tx).await?;
    // Reuse native issue identities and accepted specification provenance.
    sqlx::query(r#"INSERT INTO projected_work_items(workflow_item_id,coordinator_job_id,work_item,issue_id,issue_number,spec_path,body_digest,managed_dependencies,accepted_publication_id,proposed_publication_id,sync_status,desired_revision,applied_revision,accepted,proposed_removal)
        SELECT workflow_item_id,$2,work_item,issue_id,issue_number,spec_path,body_digest,managed_dependencies,accepted_publication_id,proposed_publication_id,sync_status,desired_revision,applied_revision,accepted,proposed_removal FROM projected_work_items WHERE coordinator_job_id=$1"#)
        .bind(source.job_id).bind(job.id).execute(&mut *tx).await?;
    sqlx::query(
        "UPDATE workflow_items SET current_state='in_progress',updated_at=now() WHERE id=$1",
    )
    .bind(source.workflow_item_id)
    .execute(&mut *tx)
    .await?;
    sqlx::query("INSERT INTO lifecycle_events(workflow_item_id,job_id,event_type,level,source,status,summary,links) VALUES($1,$2,'pr_revision_requested','milestone','github','queued','Pull request revision queued',$3)")
        .bind(source.workflow_item_id).bind(job.id).bind(json!([{ "kind":"pull_request", "url":input["pull_request"]["html_url"], "label":format!("PR #{pr} revision") }])).execute(&mut *tx).await?;
    tx.commit().await?;
    Ok(Admission::Queued(Box::new(job)))
}

pub async fn mark_publication(
    pool: &PgPool,
    job: Uuid,
    sha: &str,
    published: bool,
) -> Result<(), DbError> {
    sqlx::query(
        "UPDATE pr_revisions SET publication_sha=$2,status=$3,updated_at=now() WHERE job_id=$1",
    )
    .bind(job)
    .bind(sha)
    .bind(if published { "published" } else { "publishing" })
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn prepared(pool: &PgPool, job: Uuid, input: &Value, head: &str) -> Result<(), DbError> {
    let mut tx = pool.begin().await?;
    sqlx::query("UPDATE jobs SET input=$2 WHERE id=$1 AND status='running'")
        .bind(job)
        .bind(input)
        .execute(&mut *tx)
        .await?;
    sqlx::query(
        "UPDATE pr_revisions SET head_sha=$2,status='running',updated_at=now() WHERE job_id=$1",
    )
    .bind(job)
    .bind(head)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(())
}

pub async fn has_revision<'e, E: sqlx::Executor<'e, Database = sqlx::Postgres>>(
    pool: E,
    workflow: i64,
) -> Result<bool, DbError> {
    Ok(sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pr_revisions r JOIN jobs j ON j.id=r.job_id JOIN workflow_items w ON w.id=r.workflow_item_id WHERE w.id=$1 AND j.generation=w.generation)")
        .bind(workflow).fetch_one(pool).await?)
}

#[derive(FromRow)]
pub struct PendingPublication {
    pub job_id: Uuid,
    pub publication_sha: String,
}

pub async fn pending_publication(
    pool: &PgPool,
    repository: i64,
    pr: i64,
) -> Result<Option<PendingPublication>, DbError> {
    Ok(sqlx::query_as("SELECT r.job_id,r.publication_sha FROM pr_revisions r JOIN jobs j ON j.id=r.job_id JOIN workflow_items w ON w.id=r.workflow_item_id WHERE r.repository_id=$1 AND r.pr_number=$2 AND r.status='publishing' AND r.publication_sha IS NOT NULL AND j.generation=w.generation AND j.status IN ('completed','failed','cancelled') ORDER BY r.id DESC LIMIT 1")
        .bind(repository).bind(pr).fetch_optional(pool).await?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{DbConfig, PullRequestInput, RepositoryInput, WorkflowItemInput};

    async fn exercise_pr_decisions_and_feedback(
        pool: &PgPool,
        repository: i64,
        workflow: i64,
        job: &JobRecord,
    ) {
        use crate::revision_feedback as feedback;
        sqlx::query("UPDATE jobs SET status='paused',input=jsonb_set(input,'{pr_revision,base_sha}','\"base\"') WHERE id=$1")
            .bind(job.id).execute(pool).await.unwrap();
        let checkpoint = json!({"pending_approvals":[{"key":{"task":"build","work_item":"queue"}}],
            "revision_options":[{"target":{"task":"plan"},"affected":[{"task":"build","work_item":"queue"}]}]});
        sqlx::query("INSERT INTO lifecycle_checkpoints(coordinator_job_id,generation,revision,version,state) SELECT id,generation,1,5,$2 FROM jobs WHERE id=$1")
            .bind(job.id).bind(&checkpoint).execute(pool).await.unwrap();
        let mut tx = crate::cancellation::lock_job_side_effect(pool, job.id)
            .await
            .unwrap();
        let saved = feedback::snapshot(&mut tx, job.id).await.unwrap();
        assert_eq!(saved.checkpoint, Some(checkpoint));
        assert_eq!(saved.status, "paused");
        assert_eq!(saved.pr_number, 2);
        feedback::projected(&mut tx, job.id, "previous-feedback", true)
            .await
            .unwrap();
        tx.commit().await.unwrap();
        assert!(!feedback::due(pool).await.unwrap().contains(&job.id));

        let receipt = json!({"action":"created","comment":{"id":9001},"sender":{"login":"maintainer","type":"User"}});
        let input = json!({"issue":{"number":1},"comment":{"id":9001,"body":"/studio revise plan\nFix specification"},
            "sender":receipt["sender"],"donkeyspace_human_decision":{"action":"revise","target":"plan","feedback":"Fix specification"},
            "donkeyspace_engagement":{"reason":"authorized maintainer"}});
        let delivery = format!("test-pr-{}", job.id);
        let polled = format!("poll-pr-{}", job.id);
        let make_request = |head| ResumeRequest {
            repository,
            pr: 2,
            job,
            head,
            delivery: &delivery,
            input: &input,
            receipt: &receipt,
        };
        assert!(matches!(
            resume(pool, make_request("moved")).await.unwrap(),
            ResumeAdmission::Rejected(_)
        ));
        let mut invalid = input.clone();
        invalid["donkeyspace_human_decision"]["target"] = json!("unrelated");
        assert!(matches!(
            resume(
                pool,
                ResumeRequest {
                    input: &invalid,
                    ..make_request("base")
                }
            )
            .await
            .unwrap(),
            ResumeAdmission::Rejected(_)
        ));
        sqlx::query("UPDATE pull_requests SET state='closed' WHERE repository_id=$1")
            .bind(repository)
            .execute(pool)
            .await
            .unwrap();
        assert!(matches!(
            resume(pool, make_request("base")).await.unwrap(),
            ResumeAdmission::Rejected(_)
        ));
        sqlx::query("UPDATE pull_requests SET state='open' WHERE repository_id=$1")
            .bind(repository)
            .execute(pool)
            .await
            .unwrap();
        assert!(
            !crate::webhook_delivery_exists(pool, &delivery)
                .await
                .unwrap()
        );
        assert_eq!(
            crate::get_job(pool, job.id).await.unwrap().unwrap().status,
            "paused"
        );

        let (webhook, poll) = tokio::join!(
            resume(pool, make_request("base")),
            resume(
                pool,
                ResumeRequest {
                    delivery: &polled,
                    ..make_request("base")
                }
            )
        );
        let resumed = match (webhook.unwrap(), poll.unwrap()) {
            (ResumeAdmission::Resumed(job), ResumeAdmission::Duplicate)
            | (ResumeAdmission::Duplicate, ResumeAdmission::Resumed(job)) => job,
            _ => panic!("both transports must resume exactly once"),
        };
        assert_eq!(resumed.id, job.id);
        assert_eq!(resumed.status, "queued");
        assert_eq!(resumed.input["pull_request"], job.input["pull_request"]);
        assert_eq!(
            resumed.input["pr_revision"]["comment_id"],
            job.input["pr_revision"]["comment_id"]
        );
        assert_eq!(resumed.input["pr_revision"]["base_sha"], "base");
        assert_eq!(
            resumed.input["donkeyspace_human_decision"],
            input["donkeyspace_human_decision"]
        );
        let audits: i64 = sqlx::query_scalar("SELECT count(*) FROM engagement_decisions e JOIN webhook_deliveries d ON d.id=e.webhook_delivery_id WHERE d.repository_id=$1 AND d.github_comment_id=9001").bind(repository).fetch_one(pool).await.unwrap();
        assert_eq!(audits, 1);
        assert!(
            feedback::due(pool).await.unwrap().contains(&job.id),
            "accepted decision wakes publication"
        );
        feedback::failed(pool, job.id).await.unwrap();
        assert!(
            !feedback::due(pool).await.unwrap().contains(&job.id),
            "errors back off"
        );
        feedback::wake(pool, job.id).await.unwrap();
        assert!(feedback::due(pool).await.unwrap().contains(&job.id));

        // Background and foreground dispatchers must finish under the same lock.
        let action = crate::OutboundActionInput {
            workflow_item_id: workflow,
            job_id: None,
            provider: "github".into(),
            action_type: "issue.upsert_comment".into(),
            payload: json!({"body":"queued"}),
        };
        let key = format!("feedback-test-{}", job.id);
        let first = crate::upsert_pending_outbound_action(pool, &action, &key)
            .await
            .unwrap();
        let mut guard = crate::cancellation::lock_outbound_side_effect(pool, first.id)
            .await
            .unwrap()
            .unwrap();
        assert!(
            tokio::time::timeout(
                std::time::Duration::from_millis(100),
                crate::cancellation::lock_outbound_side_effect(pool, first.id)
            )
            .await
            .is_err()
        );
        let latest = crate::OutboundActionInput {
            payload: json!({"body":"completed"}),
            ..action
        };
        let replacement = crate::upsert_pending_outbound_action(pool, &latest, &key);
        tokio::pin!(replacement);
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(100), &mut replacement)
                .await
                .is_err()
        );
        crate::mark_outbound_action_completed_on(&mut guard, first.id, None)
            .await
            .unwrap();
        guard.commit().await.unwrap();
        let replacement = replacement.await.unwrap();
        assert_ne!(
            replacement.id, first.id,
            "newer feedback remains pending after in-flight completion"
        );
        assert_eq!(replacement.payload["body"], "completed");
        assert!(
            crate::cancellation::lock_outbound_side_effect(pool, first.id)
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    #[ignore = "requires isolated DONKEYSPACE_REVISION_TEST_DATABASE_URL"]
    async fn revision_admission_is_atomic_deduplicated_and_generation_scoped() {
        let url = std::env::var("DONKEYSPACE_REVISION_TEST_DATABASE_URL")
            .or_else(|_| std::env::var("DONKEYSPACE_CANCELLATION_TEST_DATABASE_URL"))
            .unwrap();
        assert!(
            url.ends_with("/donkeyspace_revision_test")
                || url.ends_with("/donkeyspace_cancellation_test")
        );
        let pool = crate::connect(&DbConfig::from_database_url(url))
            .await
            .unwrap();
        crate::apply_migrations(&pool).await.unwrap();
        crate::apply_migrations(&pool).await.unwrap();
        let repository = crate::upsert_repository(
            &pool,
            &RepositoryInput {
                installation_external_id: None,
                installation_account_login: None,
                provider: "github".into(),
                owner: format!("revision-{}", Uuid::now_v7()),
                name: "example".into(),
                default_branch: "main".into(),
            },
        )
        .await
        .unwrap();
        let workflow = crate::cancellation::observe_issue(
            &pool,
            &crate::cancellation::IssueObservation {
                issue: &WorkflowItemInput {
                    repository_id: repository,
                    provider_issue_id: "1".into(),
                    issue_number: 1,
                    provider_state: "open".into(),
                    current_state: Some("pr_open".into()),
                    current_labels: vec![],
                },
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
        let original = crate::create_job(
            &pool,
            Some(workflow),
            "plan",
            &json!({"donkeyspace_lifecycle_coordinator":true}),
        )
        .await
        .unwrap();
        sqlx::query("UPDATE jobs SET status='completed' WHERE id=$1")
            .bind(original.id)
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("INSERT INTO agent_publications(coordinator_job_id,workflow_item_id,kind,branch_name,commit_sha,html_url,local_repo_path,status,metadata) VALUES($1,$2,'checkpoint','work','base','https://example.invalid','/tmp/test','published',$3)")
            .bind(original.id).bind(workflow).bind(json!({"pull_request_url":"https://example.invalid/pull/2"})).execute(&pool).await.unwrap();
        crate::upsert_pull_request(
            &pool,
            &PullRequestInput {
                repository_id: repository,
                workflow_item_id: Some(workflow),
                provider_pr_id: "2".into(),
                pr_number: 2,
                title: "Implement".into(),
                html_url: "https://example.invalid/pull/2".into(),
                state: "open".into(),
                head_ref: format!("studio/issue-1-{}", original.id),
                head_sha: Some("base".into()),
                base_ref: "main".into(),
                base_sha: Some("main".into()),
                managed_by_donkeyspace: true,
            },
        )
        .await
        .unwrap();
        // PRs completed on the schema-9 baseline retain their authoritative
        // checkpoint even though they predate the revision snapshot table.
        let prior_state = json!({"version":5,"completed_keys":[{"task":"plan","work_item":null}],
            "repository_snapshot":{"commit_sha":"prior-head"}});
        sqlx::query("INSERT INTO lifecycle_checkpoints(coordinator_job_id,generation,revision,version,state,completed) SELECT id,generation,1,5,$2,true FROM jobs WHERE id=$1")
            .bind(original.id).bind(&prior_state).execute(&pool).await.unwrap();
        assert_eq!(
            source(&pool, repository, 2)
                .await
                .unwrap()
                .unwrap()
                .checkpoint,
            Some(prior_state.clone())
        );
        assert_eq!(
            snapshot(&pool, original.id).await.unwrap(),
            Some(prior_state)
        );
        save_snapshot(
            &pool,
            original.id,
            &json!({"completed_keys":[{"task":"plan","work_item":null}]}),
        )
        .await
        .unwrap();
        sqlx::query("INSERT INTO projected_work_items(workflow_item_id,coordinator_job_id,work_item,issue_number,spec_path,body_digest,sync_status,accepted) VALUES($1,$2,'queue',3,'docs/queue.md','digest','applied',true)")
            .bind(workflow).bind(original.id).execute(&pool).await.unwrap();
        let baseline = source(&pool, repository, 2).await.unwrap().unwrap();
        assert_eq!(baseline.job_id, original.id);
        assert!(baseline.checkpoint.is_some());
        let input = json!({"donkeyspace_lifecycle_coordinator":true,"pull_request":{"head":{"sha":"base"}},"pr_revision":{"comment_id":5,"command":{"feedback":"Fix"}}});
        let (first, second) = tokio::join!(
            enqueue(&pool, repository, 2, 5, &baseline, "plan", &input),
            enqueue(&pool, repository, 2, 5, &baseline, "plan", &input)
        );
        let job = match (first.unwrap(), second.unwrap()) {
            (Admission::Queued(job), Admission::Duplicate)
            | (Admission::Duplicate, Admission::Queued(job)) => job,
            _ => panic!("duplicate delivery must create exactly one job"),
        };
        assert_ne!(job.id, original.id);
        assert!(matches!(
            enqueue(&pool, repository, 2, 6, &baseline, "plan", &input)
                .await
                .unwrap(),
            Admission::Unavailable
        ));
        let projected: Option<i64> = sqlx::query_scalar(
            "SELECT issue_number FROM projected_work_items WHERE coordinator_job_id=$1",
        )
        .bind(job.id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(projected, Some(3));
        sqlx::query("UPDATE jobs SET status='paused' WHERE id=$1")
            .bind(job.id)
            .execute(&pool)
            .await
            .unwrap();
        let resumed = crate::resume_latest_paused_job(
            &pool,
            workflow,
            &json!({"comment":{"body":"/studio approve plan"}}),
        )
        .await
        .unwrap()
        .unwrap();
        assert!(resumed.input.get("pr_revision").is_some());
        assert_eq!(resumed.input["donkeyspace_resume"], true);
        exercise_pr_decisions_and_feedback(&pool, repository, workflow, &resumed).await;
        sqlx::query("UPDATE jobs SET status='completed' WHERE id=$1")
            .bind(job.id)
            .execute(&pool)
            .await
            .unwrap();
        mark_publication(&pool, job.id, "new", true).await.unwrap();
        save_snapshot(&pool, job.id, &json!({"completed_keys":[]}))
            .await
            .unwrap();
        assert_eq!(
            source(&pool, repository, 2).await.unwrap().unwrap().job_id,
            job.id
        );
        crate::upsert_pull_request(
            &pool,
            &PullRequestInput {
                repository_id: repository,
                workflow_item_id: Some(workflow),
                provider_pr_id: "99".into(),
                pr_number: 99,
                title: "Other PR".into(),
                html_url: "https://example.invalid/pull/99".into(),
                state: "open".into(),
                head_ref: format!("studio/issue-1-{}", original.id),
                head_sha: Some("base".into()),
                base_ref: "main".into(),
                base_sha: None,
                managed_by_donkeyspace: true,
            },
        )
        .await
        .unwrap();
        assert!(
            source(&pool, repository, 99).await.unwrap().is_none(),
            "a publication for another PR is not a continuation source"
        );
        assert!(matches!(
            enqueue(&pool, repository, 2, 10, &baseline, "plan", &input)
                .await
                .unwrap(),
            Admission::Unavailable
        ));
        let latest = source(&pool, repository, 2).await.unwrap().unwrap();
        let next = match enqueue(&pool, repository, 2, 11, &latest, "plan", &input)
            .await
            .unwrap()
        {
            Admission::Queued(job) => job,
            _ => panic!("new revision should queue"),
        };
        sqlx::query("UPDATE jobs SET status='running' WHERE id=$1")
            .bind(next.id)
            .execute(&pool)
            .await
            .unwrap();
        let child = crate::create_waiting_job(
            &pool,
            Some(workflow),
            "check",
            &json!({"pr_revision":{},"plugin_execution":{"coordinator_run_id":next.id}}),
        )
        .await
        .unwrap();
        crate::cancellation::apply_pull_request_effects(&pool, workflow, "2", Some("pr_open"), &[])
            .await
            .unwrap();
        let state: Option<String> =
            sqlx::query_scalar("SELECT current_state FROM workflow_items WHERE id=$1")
                .bind(workflow)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(state.as_deref(), Some("in_progress"));
        mark_publication(&pool, next.id, "candidate", false)
            .await
            .unwrap();
        sqlx::query("UPDATE pull_requests SET state='closed' WHERE repository_id=$1")
            .bind(repository)
            .execute(&pool)
            .await
            .unwrap();
        crate::cancellation::apply_pull_request_effects(
            &pool,
            workflow,
            "2",
            Some("pr_closed"),
            &[],
        )
        .await
        .unwrap();
        assert_eq!(
            crate::get_job(&pool, next.id)
                .await
                .unwrap()
                .unwrap()
                .status,
            "cancel_requested"
        );
        assert_eq!(
            crate::get_job(&pool, child.id)
                .await
                .unwrap()
                .unwrap()
                .status,
            "cancelled"
        );
        sqlx::query("UPDATE jobs SET status='cancelled' WHERE id=$1")
            .bind(next.id)
            .execute(&pool)
            .await
            .unwrap();
        assert_eq!(
            pending_publication(&pool, repository, 2)
                .await
                .unwrap()
                .unwrap()
                .publication_sha,
            "candidate"
        );
        assert!(source(&pool, repository, 2).await.unwrap().is_none());
        assert!(matches!(
            enqueue(&pool, repository, 2, 7, &baseline, "plan", &input)
                .await
                .unwrap(),
            Admission::Unavailable
        ));
        sqlx::query("UPDATE pull_requests SET state='open' WHERE repository_id=$1")
            .bind(repository)
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("UPDATE workflow_items SET generation=generation+1 WHERE id=$1")
            .bind(workflow)
            .execute(&pool)
            .await
            .unwrap();
        assert!(source(&pool, repository, 2).await.unwrap().is_none());
        assert!(matches!(
            enqueue(&pool, repository, 2, 8, &baseline, "plan", &input)
                .await
                .unwrap(),
            Admission::Unavailable
        ));
        pool.close().await;
    }
}
