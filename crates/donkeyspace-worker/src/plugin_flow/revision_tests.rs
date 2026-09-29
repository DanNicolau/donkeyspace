use super::*;
use donkeyspace_db::{DbConfig, RepositoryInput, WorkflowItemInput};

#[tokio::test]
#[ignore = "requires disposable PostgreSQL and Docker busybox:latest; no model or GitHub calls"]
async fn upstream_revision_reuses_coordinator_and_renews_approval_after_restart() {
    let url = env::var("DONKEYSPACE_CANCELLATION_TEST_DATABASE_URL").unwrap();
    assert!(url.ends_with("/donkeyspace_cancellation_test"));
    let config = DbConfig::from_database_url(url);
    let mut pool = donkeyspace_db::connect(&config).await.unwrap();
    donkeyspace_db::apply_migrations(&pool).await.unwrap();
    let owner = format!("revision-{}", Uuid::now_v7());
    let repository = donkeyspace_db::upsert_repository(
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
    let workflow = donkeyspace_db::upsert_workflow_item(
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
    let input = json!({"donkeyspace_lifecycle_coordinator":true,"repository":{"owner":{"login":owner},"name":"fixture"},"issue":{"number":1,"title":"revision recovery"}});
    let coordinator = donkeyspace_db::create_job(&pool, Some(workflow), "plan", &input)
        .await
        .unwrap();
    let workspace = env::temp_dir().join(format!("ds-revision-{}", coordinator.id));
    struct Cleanup(PathBuf);
    impl Drop for Cleanup {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }
    let _cleanup = Cleanup(workspace.clone());
    let repo = workspace.join("repo");
    fs::create_dir_all(&repo).unwrap();
    let success = json!({"outcome":"implemented","summary":"fixture task passed","confidence":"high","risk":"low","questions":[],"tests":[{"name":"fixture checks","command":["test"],"status":"passed","exit_code":0}],"changed_files":[]});
    let mut initial = success.clone();
    initial["work_items"] = json!(["left", "right"]);
    let mut revised = success.clone();
    revised["work_items"] = json!(["left", "right", "added"]);
    let registry = |ids: &[&str]| json!({"work_items":ids.iter().map(|id| json!({"id":id,"spec":format!("docs/{id}.md")})).collect::<Vec<_>>()});
    let planner = format!(
        "umask 000; mkdir -p repo/docs; if grep -q change-contract .donkeyspace/run-input.json; then printf v2 > repo/docs/left.md; printf v2 > repo/docs/right.md; printf v2 > repo/docs/added.md; printf '%s' '{}' > repo/docs/items.json; printf '%s' '{}' > .donkeyspace/run-result.json; else printf v1 > repo/docs/left.md; printf v1 > repo/docs/right.md; printf '%s' '{}' > repo/docs/items.json; printf '%s' '{}' > .donkeyspace/run-result.json; fi",
        registry(&["left", "right", "added"]),
        revised,
        registry(&["left", "right"]),
        initial
    );
    let build = format!(
        "umask 000; for id in left right added; do if test -d repo/build/$id; then cat repo/docs/$id.md > repo/build/$id/version; fi; done; printf '%s' '{}' > .donkeyspace/run-result.json",
        success
    );
    let mut blocked = success.clone();
    blocked["outcome"] = json!("needs_human");
    blocked["human_review_reason"] = json!("The accepted contract needs revision.");
    let validate = format!(
        "set -eu; if test -f repo/build/left/version && test \"$(cat repo/build/left/version)\" = v1; then printf '%s' '{}' > .donkeyspace/run-result.json; else for id in left right added; do if test -f repo/build/$id/version; then test \"$(cat repo/build/$id/version)\" = \"$(cat repo/docs/$id.md)\"; fi; done; printf '%s' '{}' > .donkeyspace/run-result.json; fi",
        blocked, success
    );
    let manifest = json!({"api_version":1,"id":"test.revision","runtime":{"default_image":"busybox:latest"},
        "roles":{"plan":{"command":["sh","-c",planner]},"build":{"command":["sh","-c",build]},"check":{"command":["sh","-c",validate]}},
        "flows":{"test":{"start":"plan","replaces_default_lifecycle":true,"work_items_path":"docs/items.json","max_parallel_tasks":3,"tasks":{
            "plan":{"role":"plan","write":["docs"],"approval":"required"},
            "build":{"role":"build","scope":"work_item","dependencies":["plan"],"read":["docs/{work_item}.md"],"write":["build/{work_item}"]},
            "check":{"role":"check","scope":"work_item","dependencies":["build"],"read":["docs/{work_item}.md","build/{work_item}"]}}}}});
    let manifest_path = workspace.join("plugin.json");
    fs::write(&manifest_path, manifest.to_string()).unwrap();
    // Empty output roots exist before task filtering; the model fixture only
    // edits its permitted block, just as the production roles must.
    for id in ["left", "right", "added"] {
        fs::create_dir_all(repo.join("build").join(id)).unwrap();
    }
    let selection: PluginFlowSelection =
        serde_json::from_value(json!({"manifest_path":manifest_path,"flow":"test"})).unwrap();
    let policy =
        donkeyspace_core::Policy::from_yaml(include_str!("../../../../.donkeyspace/policy.yml"))
            .unwrap();
    let decisions = [
        None,
        Some((1, json!({"action":"approve","target":"plan"}))),
        Some((2, json!({"action":"approve","target":"plan"}))), // completed, not pending
        Some((
            3,
            json!({"action":"revise","target":"build/right","feedback":"unrelated"}),
        )),
        Some((
            4,
            json!({"action":"revise","target":"build/left","feedback":"repair implementation"}),
        )),
        Some((
            5,
            json!({"action":"revise","target":"plan","feedback":"change-contract"}),
        )),
        Some((
            5,
            json!({"action":"revise","target":"plan","feedback":"change-contract"}),
        )), // redelivery
        Some((6, json!({"action":"approve","target":"plan"}))),
    ];
    for (step, decision) in decisions.into_iter().enumerate() {
        let before = donkeyspace_db::lifecycle_checkpoints::load(&pool, coordinator.id)
            .await
            .unwrap();
        if let Some((comment, decision)) = decision {
            let mut next = input.clone();
            next["comment"] = json!({"id":comment,"body":decision.to_string()});
            next["donkeyspace_human_decision"] = decision;
            next["donkeyspace_engagement"] = json!({"actor":{"login":"maintainer"}});
            donkeyspace_db::resume_latest_paused_job(&pool, workflow, &next)
                .await
                .unwrap()
                .unwrap();
        }
        donkeyspace_db::acquire_job_lease(&pool, coordinator.id, "revision-test", 300)
            .await
            .unwrap()
            .unwrap();
        let job = donkeyspace_db::mark_job_running(&pool, coordinator.id)
            .await
            .unwrap()
            .unwrap();
        let result = crate::plugin_container::with_execution_owner(
            &pool,
            job.id,
            "revision-test",
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
            result.outcome,
            if step == 7 {
                Outcome::Implemented
            } else {
                Outcome::NeedsHuman
            },
            "step {step}: {result:?}"
        );
        let record = donkeyspace_db::lifecycle_checkpoints::load(&pool, job.id)
            .await
            .unwrap()
            .unwrap();
        let saved: LifecycleCheckpoint = serde_json::from_value(record.state.clone()).unwrap();
        if step == 1 {
            let status = crate::publication::queue_lifecycle_status_for_job(&pool, &job)
                .await
                .unwrap()
                .unwrap();
            let body = status.payload["body"].as_str().unwrap();
            assert!(body.contains("Revise completed upstream work"));
            assert!(body.contains(&format!("{} revise plan", active_facade().issue_command())));
            assert!(body.contains("`build/right`"));
            assert!(body.contains("Required approval will be requested again"));
        }
        let jobs = donkeyspace_db::list_jobs_for_workflow_item(&pool, workflow)
            .await
            .unwrap();
        let count = |task: &str, item: &str| {
            jobs.iter()
                .filter(|job| {
                    job.result.is_some()
                        && job
                            .input
                            .pointer("/plugin_execution/task")
                            .and_then(Value::as_str)
                            == Some(task)
                        && job
                            .input
                            .pointer("/plugin_execution/work_item/id")
                            .and_then(Value::as_str)
                            == Some(item)
                })
                .count()
        };
        if matches!(step, 2 | 3 | 6) {
            let before = before.unwrap();
            assert_eq!(
                record.revision, before.revision,
                "rejected/duplicate command changed revision"
            );
            assert_eq!(
                record.state, before.state,
                "rejected/duplicate command changed authority"
            );
        }
        if step == 4 {
            assert_eq!(count("build", "left"), 2);
            assert_eq!(count("check", "left"), 2);
            assert_eq!(count("build", "right"), 1);
            assert_eq!(count("check", "right"), 1);
            assert!(
                saved
                    .completed_keys
                    .iter()
                    .any(|key| key.target() == "check/right")
            );
            assert_eq!(
                jobs.iter()
                    .filter(|job| job.status == "superseded" && job.result.is_some())
                    .count(),
                2
            );
        }
        if step == 5 || step == 6 {
            assert_eq!(saved.pending_approvals[0].key.target(), "plan");
            assert!(!saved.start_approved);
            assert_eq!(saved.active_work_items, ["left", "right", "added"]);
            assert_eq!(
                count("build", "added"),
                0,
                "new plan ran before renewed approval"
            );
            assert_eq!(
                fs::read_to_string(repo.join("build/left/version")).unwrap(),
                "v1"
            );
            let approvals = donkeyspace_db::list_approval_requests_for_run(&pool, job.id)
                .await
                .unwrap();
            assert!(
                approvals.iter().any(
                    |approval| approval.target_task == "plan" && approval.state == "superseded"
                )
            );
            assert_eq!(
                approvals
                    .iter()
                    .filter(|approval| approval.state == "pending")
                    .count(),
                1
            );
        }
        if step == 5 {
            // A worker restart must retain the revision feedback and new gate.
            pool.close().await;
            pool = donkeyspace_db::connect(&config).await.unwrap();
        }
        if step == 7 {
            assert!(record.completed);
            assert_eq!(count("build", "left"), 3);
            assert_eq!(count("build", "right"), 2);
            for id in ["left", "right", "added"] {
                assert_eq!(
                    fs::read_to_string(repo.join(format!("build/{id}/version"))).unwrap(),
                    "v2"
                );
            }
            let events = donkeyspace_db::list_lifecycle_events(&pool, workflow, None, false, 200)
                .await
                .unwrap();
            let revisions = events
                .iter()
                .filter(|event| event.event_type == "upstream_revision_applied")
                .collect::<Vec<_>>();
            assert_eq!(revisions.len(), 2);
            assert!(
                revisions
                    .iter()
                    .all(|event| event.actor.as_deref() == Some("maintainer"))
            );
        }
    }
}
