use super::*;
use donkeyspace_db::{
    DbConfig, RepositoryInput, WorkflowItemInput, acquire_job_lease, create_job,
    list_jobs_for_workflow_item, mark_job_running, resume_latest_paused_job, upsert_repository,
    upsert_workflow_item,
};

#[tokio::test]
#[ignore = "requires isolated PostgreSQL and Docker busybox:latest; no model or GitHub access"]
async fn clarification_retains_contract_and_completed_sibling_across_restart() {
    for required_approval in [false, true] {
        clarification_scenario(required_approval).await;
    }
}

async fn clarification_scenario(required_approval: bool) {
    let url = env::var("DONKEYSPACE_CANCELLATION_TEST_DATABASE_URL").unwrap();
    assert!(url.ends_with("/donkeyspace_cancellation_test"));
    let config = DbConfig::from_database_url(url);
    let mut pool = donkeyspace_db::connect(&config).await.unwrap();
    donkeyspace_db::apply_migrations(&pool).await.unwrap();
    let owner = format!("clarification-{}", Uuid::now_v7());
    let repository = upsert_repository(
        &pool,
        &RepositoryInput {
            installation_external_id: None,
            installation_account_login: None,
            provider: "github".into(),
            owner: owner.clone(),
            name: "fixture".into(),
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
    let input = json!({"donkeyspace_lifecycle_coordinator":true,"repository":{"owner":{"login":owner},"name":"fixture"},"issue":{"number":1,"title":"contract clarification"}});
    let coordinator = create_job(&pool, Some(workflow), "plan", &input)
        .await
        .unwrap();
    let workspace = env::temp_dir().join(format!("clarification-{}", coordinator.id));
    let repo = workspace.join("repo");
    fs::create_dir_all(&repo).unwrap();
    let result = |outcome: &str| {
        json!({"outcome":outcome,"summary":"fixture contract", "confidence":"high","risk":"low",
        "questions": if outcome == "needs_info" { vec!["Confirm the existing FR-017 contract?".to_string(), format!("Confirm this extended requirement: {} end of FR-018.", "detail ".repeat(100))] } else { vec![] },
                "tests":[{"name":"preserved contract","command":["test"],"status":"passed","exit_code":0}],"changed_files":[],"work_items":["one","two"]})
    };
    let emit = |outcome: &str| {
        format!(
            "printf '%s' '{}' > .donkeyspace/run-result.json",
            result(outcome)
        )
    };
    let registry = json!({"work_items":[{"id":"one","spec":"docs/one.md","depends_on":[]},{"id":"two","spec":"docs/two.md","depends_on":[]}]});
    let planner = format!(
        "umask 000; if test -f repo/docs/items.json; then test \"$(cat repo/docs/one.md)\" = FR-017 || exit 8; grep -q confirmed-plan .donkeyspace/run-input.json || exit 9; {}; else mkdir -p repo/docs; printf FR-017 > repo/docs/one.md; printf FR-018 > repo/docs/two.md; printf '%s' '{}' > repo/docs/items.json; {}; fi",
        emit("implemented"),
        registry,
        emit("needs_info")
    );
    let implement = format!(
        "umask 000; test \"$(cat repo/docs/one.md)\" = FR-017 || exit 8; if grep -q '\"id\": \"one\"' .donkeyspace/run-input.json; then if test -f repo/output/one/proposal; then test \"$(cat repo/output/one/proposal)\" = FR-017 || exit 9; grep -q confirmed-output .donkeyspace/run-input.json || exit 10; {}; else mkdir -p repo/output/one; printf FR-017 > repo/output/one/proposal; {}; fi; else {}; fi",
        emit("implemented"),
        emit("needs_info"),
        emit("implemented")
    );
    let manifest = json!({"api_version":1,"id":"test.clarification","runtime":{"default_image":"busybox:latest"},
        "roles":{"plan":{"command":["sh","-c",planner]},"implement":{"command":["sh","-c",implement]}},
        "flows":{"test":{"start":"plan","replaces_default_lifecycle":true,"work_items_path":"docs/items.json","max_parallel_tasks":2,
            "tasks":{"plan":{"role":"plan","write":["docs"],"approval":"required","preserve_on_success":[{"path":"docs","type":"directory"}]},
                "implement":{"role":"implement","approval":if required_approval { "required" } else { "none" },"scope":"work_item","dependencies":["plan"],"read":["docs"],"write":["output/{work_item}"],"preserve_on_success":[{"path":"output/{work_item}","type":"directory"}]}}}}});
    let path = workspace.join("plugin.json");
    fs::write(&path, manifest.to_string()).unwrap();
    let selection: PluginFlowSelection =
        serde_json::from_value(json!({"manifest_path":path,"flow":"test"})).unwrap();
    let policy =
        donkeyspace_core::Policy::from_yaml(include_str!("../../../../.donkeyspace/policy.yml"))
            .unwrap();
    let outcomes = if required_approval {
        vec![
            Outcome::NeedsInfo,
            Outcome::NeedsHuman,
            Outcome::NeedsHuman,
            Outcome::NeedsHuman,
            Outcome::NeedsHuman,
            Outcome::Implemented,
        ]
    } else {
        vec![
            Outcome::NeedsInfo,
            Outcome::NeedsHuman,
            Outcome::NeedsInfo,
            Outcome::Implemented,
        ]
    };
    for (step, expected) in outcomes.into_iter().enumerate() {
        if step > 0 {
            let mut next = input.clone();
            next["comment"] =
                json!({"body": if step == 1 { "confirmed-plan" } else { "confirmed-output" }});
            if step == 2 {
                next["donkeyspace_human_decision"] = json!({"action":"approve","target":"plan"});
            } else if required_approval && step > 2 {
                next["donkeyspace_human_decision"] = match step {
                    3 => json!({"action":"approve","target":"implement/two"}),
                    4 => {
                        json!({"action":"revise","target":"implement/one","feedback":"confirmed-output"})
                    }
                    _ => json!({"action":"approve","target":"implement/one"}),
                };
            }
            assert_eq!(
                resume_latest_paused_job(&pool, workflow, &next)
                    .await
                    .unwrap()
                    .unwrap()
                    .id,
                coordinator.id
            );
        }
        acquire_job_lease(&pool, coordinator.id, "clarification-test", 120)
            .await
            .unwrap()
            .unwrap();
        let job = mark_job_running(&pool, coordinator.id)
            .await
            .unwrap()
            .unwrap();
        let result = crate::plugin_container::with_execution_owner(
            &pool,
            job.id,
            "clarification-test",
            run(
                &selection,
                &repo,
                &workspace,
                &job.input,
                Some(LifecycleTracking {
                    pool: &pool,
                    policy: &policy,
                    coordinator: &job,
                    github: None,
                    publication: None,
                }),
            ),
        )
        .await
        .unwrap();
        assert_eq!(
            result.outcome, expected,
            "step {step}, approval {required_approval}"
        );
        assert_eq!(
            fs::read_to_string(repo.join("docs/one.md")).unwrap(),
            "FR-017"
        );
        let checkpoint = donkeyspace_db::lifecycle_checkpoints::load(&pool, job.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(checkpoint.completed, expected == Outcome::Implemented);
        if step == 0 {
            let publication: i64 = sqlx::query_scalar("INSERT INTO agent_publications (coordinator_job_id,job_id,workflow_item_id,kind,branch_name,commit_sha,html_url,local_repo_path,task,attempt,status,metadata) VALUES ($1,$1,$2,'attempt','blocker-fixture','accepted-sha','unused','/unused','plan',1,'pending',$3) RETURNING id")
                .bind(job.id).bind(workflow).bind(json!({"supporting_files":["docs/one.md"]})).fetch_one(&pool).await.unwrap();
            for (status, files, message) in [
                (
                    "pending",
                    json!(["docs/one.md"]),
                    "Draft publication pending",
                ),
                ("failed", json!(["docs/one.md"]), "Draft publication failed"),
                ("published", json!([]), "No draft produced"),
                (
                    "published",
                    json!(["docs/one.md"]),
                    "Supporting files at the exact attempt revision",
                ),
            ] {
                sqlx::query("UPDATE agent_publications SET status=$2,metadata=$3 WHERE id=$1")
                    .bind(publication)
                    .bind(status)
                    .bind(json!({"supporting_files":files}))
                    .execute(&pool)
                    .await
                    .unwrap();
                let action = crate::publication::queue_lifecycle_status_for_job(&pool, &job)
                    .await
                    .unwrap()
                    .unwrap();
                let body = action.payload["body"].as_str().unwrap();
                assert!(body.contains("#### Current blockers"));
                assert!(body.contains("**plan**"));
                assert!(body.contains("Confirm the existing FR-017 contract?"));
                assert!(body.contains(message), "{body}");
                assert_eq!(body.contains("[commit `"), status == "published");
                let expected_link =
                    donkeyspace_github::file_url(&owner, "fixture", "accepted-sha", "docs/one.md");
                assert_eq!(
                    body.contains(&expected_link),
                    status == "published" && !files.as_array().unwrap().is_empty()
                );
            }
        }
        if step == 1 || step == 2 || expected == Outcome::Implemented {
            let action = crate::publication::queue_lifecycle_status_for_job(&pool, &job)
                .await
                .unwrap()
                .unwrap();
            let body = action.payload["body"].as_str().unwrap();
            let current = body.split("#### Current agents").next().unwrap();
            if step == 2 {
                assert!(current.contains("**implement/one**"), "{body}");
                assert!(current.contains("Confirm the existing FR-017 contract?"));
                assert!(!current.contains("**implement/two**"));
                if required_approval {
                    assert!(current.contains(&format!(
                        "{} revise implement/one",
                        active_facade().issue_command()
                    )));
                }
            } else {
                assert!(
                    !current.contains("Confirm the existing FR-017 contract?"),
                    "resolved question remains current: {body}"
                );
            }
        }
        if matches!(step, 0 | 2) {
            assert!(!result.questions.is_empty());
            assert_eq!(
                get_job(&pool, job.id).await.unwrap().unwrap().status,
                "paused"
            );
            assert_eq!(
                donkeyspace_db::get_workflow_item_state(&pool, repository, "1")
                    .await
                    .unwrap()
                    .as_deref(),
                Some(if expected == Outcome::NeedsInfo {
                    "needs_info"
                } else {
                    "needs_human"
                })
            );
        }
        if step >= 2 {
            let jobs = list_jobs_for_workflow_item(&pool, workflow).await.unwrap();
            assert_eq!(
                jobs.iter()
                    .filter(|job| job.role == "implement"
                        && job
                            .input
                            .pointer("/plugin_execution/work_item/id")
                            .and_then(Value::as_str)
                            == Some("two"))
                    .count(),
                1
            );
        }
        if workspace.join("plugin-tasks").exists() {
            fs::remove_dir_all(workspace.join("plugin-tasks")).unwrap();
        }
        pool.close().await;
        pool = donkeyspace_db::connect(&config).await.unwrap();
    }
    let events = donkeyspace_db::list_lifecycle_events(&pool, workflow, None, true, 200)
        .await
        .unwrap();
    assert!(events.iter().any(|event| {
        event.event_type == "task_completed"
            && event.reason.as_deref().is_some_and(|reason| {
                reason.contains("Confirm the existing FR-017 contract?")
                    && reason.contains("end of FR-018.")
            })
    }));
    fs::remove_dir_all(workspace).unwrap();
}
