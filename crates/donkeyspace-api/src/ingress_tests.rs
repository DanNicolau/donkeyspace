use super::*;

async fn database() -> PgPool {
    let url = env::var("DONKEYSPACE_CANCELLATION_TEST_DATABASE_URL").unwrap();
    assert!(url.ends_with("/donkeyspace_cancellation_test"));
    let pool = connect(&DbConfig::from_database_url(url)).await.unwrap();
    apply_migrations(&pool).await.unwrap();
    pool
}

fn issue(owner: &str) -> Vec<u8> {
    serde_json::to_vec(&json!({
        "action":"opened", "sender":{"login":"alice","type":"User"},
        "repository":{"name":"test","default_branch":"main","owner":{"login":owner}},
        "issue":{"id":1,"number":1,"title":"test","body":"test","state":"open","labels":[{"name":"ai"}]}
    })).unwrap()
}

#[tokio::test]
#[ignore = "requires isolated DONKEYSPACE_CANCELLATION_TEST_DATABASE_URL"]
async fn authorized_clarification_and_upstream_revision_resume_same_coordinator_once() {
    let pool = database().await;
    let mut state = tests::engagement_state(vec![EngagementSelector::User {
        login: "alice".into(),
    }]);
    state.policy.facade.command = Some("example".into());
    for (pause, response) in [
        ("needs_info", "Preserve the proposed behavior."),
        (
            "needs_human",
            "/example revise plan\nChange the accepted contract.",
        ),
    ] {
        let owner = format!("clarification-{}", Uuid::now_v7());
        let WebhookPersistOutcome::Queued(job) = persist_issue_webhook(
            &pool,
            &state,
            "issues",
            &Uuid::now_v7().to_string(),
            &issue(&owner),
        )
        .await
        .unwrap() else {
            panic!("expected initial job")
        };
        let workflow = job.workflow_item_id.unwrap();
        sqlx::query("UPDATE jobs SET status='paused',result=$2 WHERE id=$1")
        .bind(job.id)
        .bind(json!({"outcome":pause,"questions":["Which behavior?"],"human_review_reason":"Review required"}))
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query("UPDATE workflow_items SET current_state=$2 WHERE id=$1")
            .bind(workflow)
            .bind(pause)
            .execute(&pool)
            .await
            .unwrap();
        let mut payload: Value = serde_json::from_slice(&issue(&owner)).unwrap();
        payload["action"] = json!("created");
        payload["comment"] = json!({"id":42,"body":response});
        payload["sender"]["login"] = json!("mallory");
        assert!(matches!(
            persist_issue_webhook(
                &pool,
                &state,
                "issue_comment",
                &Uuid::now_v7().to_string(),
                &serde_json::to_vec(&payload).unwrap()
            )
            .await
            .unwrap(),
            WebhookPersistOutcome::Ignored
        ));
        assert_eq!(
            get_job(&pool, job.id).await.unwrap().unwrap().status,
            "paused"
        );
        payload["sender"]["login"] = json!("alice");
        let body = serde_json::to_vec(&payload).unwrap();
        let delivery = Uuid::now_v7().to_string();
        let WebhookPersistOutcome::Queued(resumed) =
            persist_issue_webhook(&pool, &state, "issue_comment", &delivery, &body)
                .await
                .unwrap()
        else {
            panic!("expected resume")
        };
        assert_eq!(resumed.id, job.id);
        assert_eq!(resumed.input["donkeyspace_resume"], true);
        assert_eq!(resumed.input["comment"]["body"], response);
        if pause == "needs_human" {
            assert_eq!(
                resumed.input["donkeyspace_human_decision"],
                json!({"action":"revise","target":"plan","feedback":"Change the accepted contract."})
            );
        }
        assert!(matches!(
            persist_issue_webhook(&pool, &state, "issue_comment", &delivery, &body)
                .await
                .unwrap(),
            WebhookPersistOutcome::Duplicate
        ));
        assert_eq!(
            list_jobs_for_workflow_item(&pool, workflow)
                .await
                .unwrap()
                .len(),
            1
        );
    }
}

#[tokio::test]
#[ignore = "requires isolated DONKEYSPACE_CANCELLATION_TEST_DATABASE_URL"]
async fn issue_failure_after_job_creation_rolls_back_receipt_audit_and_job() {
    let pool = database().await;
    let state = tests::engagement_state(vec![EngagementSelector::AnyUser]);
    let owner = format!("ingress-{}", Uuid::now_v7());
    let delivery = format!("test-{}", Uuid::now_v7());
    sqlx::raw_sql("CREATE FUNCTION reject_test_ingress() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.dedupe_key LIKE 'ingress:test-%:issue_received' THEN RAISE EXCEPTION 'test fault after job creation'; END IF; RETURN NEW; END $$; CREATE TRIGGER reject_test_ingress BEFORE INSERT ON lifecycle_events FOR EACH ROW EXECUTE FUNCTION reject_test_ingress();")
        .execute(&pool).await.unwrap();
    let result = persist_issue_webhook(&pool, &state, "issues", &delivery, &issue(&owner)).await;
    sqlx::raw_sql("DROP TRIGGER reject_test_ingress ON lifecycle_events; DROP FUNCTION reject_test_ingress();")
        .execute(&pool).await.unwrap();
    assert!(result.is_err());
    assert!(!webhook_delivery_exists(&pool, &delivery).await.unwrap());
    let jobs: i64 = sqlx::query_scalar("SELECT count(*) FROM jobs j JOIN workflow_items w ON w.id=j.workflow_item_id JOIN repositories r ON r.id=w.repository_id WHERE r.owner=$1")
        .bind(&owner).fetch_one(&pool).await.unwrap();
    assert_eq!(jobs, 0);
    assert!(matches!(
        persist_issue_webhook(&pool, &state, "issues", &delivery, &issue(&owner))
            .await
            .unwrap(),
        WebhookPersistOutcome::Queued(_)
    ));
    assert!(matches!(
        persist_issue_webhook(&pool, &state, "issues", &delivery, &issue(&owner))
            .await
            .unwrap(),
        WebhookPersistOutcome::Duplicate
    ));
}

#[tokio::test]
#[ignore = "requires isolated DONKEYSPACE_CANCELLATION_TEST_DATABASE_URL"]
async fn concurrent_deliveries_schedule_one_job_and_unavailable_authorization_is_retryable() {
    let pool = database().await;
    let mut state = tests::engagement_state(vec![EngagementSelector::OrganizationMember {
        organization: "unavailable".into(),
    }]);
    let owner = format!("concurrent-{}", Uuid::now_v7());
    let payload = issue(&owner);
    let delivery = Uuid::now_v7().to_string();
    assert!(
        persist_issue_webhook(&pool, &state, "issues", &delivery, &payload)
            .await
            .is_err()
    );
    assert!(!webhook_delivery_exists(&pool, &delivery).await.unwrap());
    state.policy.workflow.engagement.default.allow = vec![EngagementSelector::AnyUser];
    let (a, b) = tokio::join!(
        persist_issue_webhook(&pool, &state, "issues", &delivery, &payload),
        persist_issue_webhook(&pool, &state, "issues", &delivery, &payload)
    );
    assert!(a.is_ok() && b.is_ok());
    let jobs: i64 = sqlx::query_scalar("SELECT count(*) FROM jobs j JOIN workflow_items w ON w.id=j.workflow_item_id JOIN repositories r ON r.id=w.repository_id WHERE r.owner=$1")
        .bind(&owner).fetch_one(&pool).await.unwrap();
    assert_eq!(jobs, 1);
}

#[tokio::test]
#[ignore = "requires isolated DONKEYSPACE_CANCELLATION_TEST_DATABASE_URL"]
async fn stale_admission_after_reopen_cannot_commit_a_receipt() {
    let pool = database().await;
    let owner = format!("stale-{}", Uuid::now_v7());
    let state = tests::engagement_state(vec![]);
    persist_issue_webhook(
        &pool,
        &state,
        "issues",
        &Uuid::now_v7().to_string(),
        &issue(&owner),
    )
    .await
    .unwrap();
    let workflow: i64 = sqlx::query_scalar("SELECT w.id FROM workflow_items w JOIN repositories r ON r.id=w.repository_id WHERE r.owner=$1")
        .bind(&owner).fetch_one(&pool).await.unwrap();
    let snapshot = donkeyspace_db::ingress::snapshot(&pool, workflow)
        .await
        .unwrap();
    sqlx::query("UPDATE workflow_items SET generation=generation+1 WHERE id=$1")
        .bind(workflow)
        .execute(&pool)
        .await
        .unwrap();
    let mut tx = pool.begin().await.unwrap();
    assert!(matches!(
        donkeyspace_db::ingress::lock_snapshot(&mut tx, workflow, &snapshot).await,
        Err(donkeyspace_db::DbError::StaleAdmission)
    ));
}

#[tokio::test]
#[ignore = "requires isolated DONKEYSPACE_CANCELLATION_TEST_DATABASE_URL"]
async fn pull_request_and_multi_job_push_roll_back_all_followups() {
    let pool = database().await;
    let state = tests::engagement_state(vec![EngagementSelector::AnyUser]);
    let owner = format!("followups-{}", Uuid::now_v7());
    let mut workflows = Vec::new();
    for number in 1..=2 {
        let mut payload: Value = serde_json::from_slice(&issue(&owner)).unwrap();
        payload["issue"]["id"] = json!(number);
        payload["issue"]["number"] = json!(number);
        let WebhookPersistOutcome::Queued(job) = persist_issue_webhook(
            &pool,
            &state,
            "issues",
            &Uuid::now_v7().to_string(),
            &serde_json::to_vec(&payload).unwrap(),
        )
        .await
        .unwrap() else {
            panic!("expected issue job");
        };
        workflows.push(job.workflow_item_id.unwrap());
        let pr = json!({"action":"opened","repository":payload["repository"], "pull_request":{
            "id":number,"number":number+10,"title":"review","body":format!("Closes #{number}"),
            "html_url":"https://github.com/test/test/pull/11", "state":"open",
            "head":{"ref":format!("donkeyspace/issue-{number}-{}",job.id),"sha":"head"},
            "base":{"ref":"main","sha":"base"}}});
        let delivery = Uuid::now_v7().to_string();
        if number == 1 {
            sqlx::raw_sql("CREATE FUNCTION reject_test_pr() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.to_state='reviewer_queued' THEN RAISE EXCEPTION 'test PR failure'; END IF; RETURN NEW; END $$; CREATE TRIGGER reject_test_pr BEFORE INSERT ON state_transitions FOR EACH ROW EXECUTE FUNCTION reject_test_pr();")
                .execute(&pool).await.unwrap();
            let result = persist_pull_request_webhook(
                &pool,
                &state.policy,
                "pull_request",
                &delivery,
                &serde_json::to_vec(&pr).unwrap(),
            )
            .await;
            sqlx::raw_sql(
                "DROP TRIGGER reject_test_pr ON state_transitions; DROP FUNCTION reject_test_pr();",
            )
            .execute(&pool)
            .await
            .unwrap();
            assert!(result.is_err());
            assert!(!webhook_delivery_exists(&pool, &delivery).await.unwrap());
            let count: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM outbound_actions WHERE workflow_item_id=$1",
            )
            .bind(workflows[0])
            .fetch_one(&pool)
            .await
            .unwrap();
            assert_eq!(count, 0);
        }
        assert!(matches!(
            persist_pull_request_webhook(
                &pool,
                &state.policy,
                "pull_request",
                &delivery,
                &serde_json::to_vec(&pr).unwrap()
            )
            .await
            .unwrap(),
            WebhookPersistOutcome::Queued(_)
        ));
    }
    let push = serde_json::to_vec(&json!({"ref":"refs/heads/main","after":"new-base","repository":{"name":"test","owner":{"login":owner},"default_branch":"main"}})).unwrap();
    let delivery = Uuid::now_v7().to_string();
    sqlx::raw_sql(&format!("CREATE FUNCTION reject_test_push() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.role='repair' AND NEW.workflow_item_id={} THEN RAISE EXCEPTION 'test second followup failure'; END IF; RETURN NEW; END $$; CREATE TRIGGER reject_test_push BEFORE INSERT ON jobs FOR EACH ROW EXECUTE FUNCTION reject_test_push();", workflows[1])).execute(&pool).await.unwrap();
    let result = persist_push_webhook(&pool, &state.policy, "push", &delivery, &push).await;
    sqlx::raw_sql("DROP TRIGGER reject_test_push ON jobs; DROP FUNCTION reject_test_push();")
        .execute(&pool)
        .await
        .unwrap();
    assert!(result.is_err());
    assert!(!webhook_delivery_exists(&pool, &delivery).await.unwrap());
    let count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM jobs WHERE workflow_item_id=ANY($1) AND role='repair'",
    )
    .bind(&workflows)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(count, 0);
    assert!(matches!(
        persist_push_webhook(&pool, &state.policy, "push", &delivery, &push)
            .await
            .unwrap(),
        WebhookPersistOutcome::Queued(_)
    ));
    let count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM jobs WHERE workflow_item_id=ANY($1) AND role='repair'",
    )
    .bind(&workflows)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(count, 2);
}

#[tokio::test]
#[ignore = "requires isolated DONKEYSPACE_CANCELLATION_TEST_DATABASE_URL"]
async fn approval_commands_use_each_durable_target_independently_of_reason_text() {
    let pool = database().await;
    let state = tests::engagement_state(vec![EngagementSelector::AnyUser]);
    let owner = format!("approval-{}", Uuid::now_v7());
    let WebhookPersistOutcome::Queued(job) = persist_issue_webhook(
        &pool,
        &state,
        "issues",
        &Uuid::now_v7().to_string(),
        &issue(&owner),
    )
    .await
    .unwrap() else {
        panic!("expected job");
    };
    let workflow = job.workflow_item_id.unwrap();
    sqlx::query("UPDATE workflow_items SET current_state='needs_human' WHERE id=$1")
        .bind(workflow)
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("UPDATE jobs SET status='paused',input=input || '{\"donkeyspace_lifecycle_coordinator\":true}'::jsonb,result=$2 WHERE id=$1")
        .bind(job.id).bind(json!({"outcome":"needs_human","summary":"review","human_review_reason":"Wording without commands."})).execute(&pool).await.unwrap();
    let publication: i64 = sqlx::query_scalar("INSERT INTO agent_publications (coordinator_job_id,job_id,workflow_item_id,kind,branch_name,commit_sha,html_url,local_repo_path,status) VALUES ($1,$1,$2,'checkpoint','test','sha','https://github.com/test/test/commit/sha','/tmp','published') RETURNING id")
        .bind(job.id).bind(workflow).fetch_one(&pool).await.unwrap();
    for target in ["one", "two"] {
        donkeyspace_db::upsert_approval_request(
            &pool,
            &donkeyspace_db::ApprovalRequestInput {
                workflow_item_id: workflow,
                coordinator_job_id: job.id,
                target_task: "rtl".into(),
                target_work_item: Some(target.into()),
                purpose: "accept_result".into(),
                trigger: "required".into(),
                approval_subject: target.into(),
                result_summary: "new wording".into(),
                changed_files: json!([]),
                proposed_publication_id: Some(publication),
                projected_issues: json!([]),
                downstream_tasks: json!([]),
            },
        )
        .await
        .unwrap();
    }
    let mut facade = state.policy.facade.resolve();
    facade.command = "example".into();
    let revision_state = json!({"revision_options":[{"target":{"task":"plan","work_item":null},"affected":[{"task":"plan","work_item":null},{"task":"rtl","work_item":"one"},{"task":"rtl","work_item":"two"}]}]});
    sqlx::query("INSERT INTO lifecycle_checkpoints (coordinator_job_id,generation,revision,version,state) SELECT id,generation,1,5,$2 FROM jobs WHERE id=$1")
        .bind(job.id).bind(revision_state).execute(&pool).await.unwrap();
    let overview = get_workflow_by_issue(&pool, &owner, "test", 1)
        .await
        .unwrap()
        .unwrap();
    let summary = workflow_summary(&pool, overview, &facade).await.unwrap();
    assert_eq!(summary.approvals.len(), 2);
    assert_eq!(summary.revision_targets.len(), 1);
    assert_eq!(
        summary.revision_targets[0].revise_command,
        "/example revise plan"
    );
    assert_eq!(
        summary.revision_targets[0].affected,
        ["plan", "rtl/one", "rtl/two"]
    );
    for approval in summary.approvals {
        assert_eq!(
            approval.approve_command,
            Some(format!(
                "/example approve rtl/{}",
                approval.target_work_item.unwrap()
            ))
        );
    }
    sqlx::query("UPDATE approval_requests SET state='approved' WHERE coordinator_job_id=$1")
        .bind(job.id)
        .execute(&pool)
        .await
        .unwrap();
    let overview = get_workflow_by_issue(&pool, &owner, "test", 1)
        .await
        .unwrap()
        .unwrap();
    let legacy = workflow_summary(&pool, overview, &facade).await.unwrap();
    assert_eq!(legacy.approvals.len(), 1);
    assert!(legacy.approvals[0].approve_command.is_none());
    assert!(legacy.approvals[0].revise_command.is_none());
}

#[tokio::test]
#[ignore = "requires isolated DONKEYSPACE_CANCELLATION_TEST_DATABASE_URL"]
async fn workflow_blockers_expose_questions_and_exact_publication_state_until_resolved() {
    let pool = database().await;
    let state = tests::engagement_state(vec![EngagementSelector::AnyUser]);
    let owner = format!("blockers-{}", Uuid::now_v7());
    let WebhookPersistOutcome::Queued(job) = persist_issue_webhook(
        &pool,
        &state,
        "issues",
        &Uuid::now_v7().to_string(),
        &issue(&owner),
    )
    .await
    .unwrap() else {
        panic!("expected job");
    };
    let workflow = job.workflow_item_id.unwrap();
    sqlx::query("UPDATE workflow_items SET current_state='needs_info' WHERE id=$1")
        .bind(workflow)
        .execute(&pool)
        .await
        .unwrap();
    let result = json!({"outcome":"needs_info","summary":"Questions before implementation","questions":["Which reset polarity?","Should output saturate?"],"changed_files":[]});
    sqlx::query("UPDATE jobs SET status='paused',input=input || '{\"donkeyspace_lifecycle_coordinator\":true}'::jsonb,result=$2 WHERE id=$1").bind(job.id).bind(&result).execute(&pool).await.unwrap();
    let saved = json!({"start_approved":false,"attempt":1,"previous":[{"task":"plan","outcome":"needs_info","questions":result["questions"]}],"last_result":result});
    sqlx::query("INSERT INTO lifecycle_checkpoints (coordinator_job_id,generation,revision,version,state) SELECT id,generation,1,5,$2 FROM jobs WHERE id=$1").bind(job.id).bind(saved).execute(&pool).await.unwrap();
    let publication: i64 = sqlx::query_scalar("INSERT INTO agent_publications (coordinator_job_id,job_id,workflow_item_id,kind,branch_name,commit_sha,html_url,local_repo_path,task,attempt,status,metadata) VALUES ($1,$1,$2,'attempt','blocker-fixture','accepted-sha','unused','/unused','plan',1,'pending',$3) RETURNING id")
        .bind(job.id).bind(workflow).bind(json!({"supporting_files":[]})).fetch_one(&pool).await.unwrap();
    for (status, files, expected) in [
        ("published", json!([]), "none"),
        ("pending", json!(["docs/design.md"]), "pending"),
        ("failed", json!(["docs/design.md"]), "failed"),
        ("published", json!(["docs/design.md"]), "published"),
    ] {
        sqlx::query("UPDATE agent_publications SET status=$2,metadata=$3 WHERE id=$1")
            .bind(publication)
            .bind(status)
            .bind(json!({"supporting_files":files}))
            .execute(&pool)
            .await
            .unwrap();
        let overview = get_workflow_by_issue(&pool, &owner, "test", 1)
            .await
            .unwrap()
            .unwrap();
        let summary = workflow_summary(&pool, overview, &state.policy.facade.resolve())
            .await
            .unwrap();
        assert_eq!(summary.blockers.len(), 1);
        let blocker = &summary.blockers[0];
        assert_eq!(blocker.task.as_deref(), Some("plan"));
        assert_eq!(
            blocker.questions,
            ["Which reset polarity?", "Should output saturate?"]
        );
        assert_eq!(blocker.evidence.state, expected);
        assert!(
            summary
                .no_pr_reason
                .as_deref()
                .unwrap()
                .contains("Which reset polarity?")
        );
        if expected == "published" {
            assert_eq!(
                blocker.evidence.files[0].url,
                donkeyspace_github::file_url(&owner, "test", "accepted-sha", "docs/design.md")
            );
        } else {
            assert!(blocker.evidence.files.is_empty());
        }
    }
    sqlx::query("UPDATE workflow_items SET current_state='in_progress' WHERE id=$1")
        .bind(workflow)
        .execute(&pool)
        .await
        .unwrap();
    let overview = get_workflow_by_issue(&pool, &owner, "test", 1)
        .await
        .unwrap()
        .unwrap();
    assert!(
        workflow_summary(&pool, overview, &state.policy.facade.resolve())
            .await
            .unwrap()
            .blockers
            .is_empty()
    );
}
