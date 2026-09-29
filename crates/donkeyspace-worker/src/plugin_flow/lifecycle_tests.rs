use super::*;
use donkeyspace_db::{
    DbConfig, RepositoryInput, WorkflowItemInput, acquire_job_lease, create_job,
    list_jobs_for_workflow_item, mark_job_running, resume_latest_paused_job, upsert_repository,
    upsert_workflow_item,
};

#[tokio::test]
#[ignore = "requires isolated PostgreSQL and Docker busybox:latest; no model or GitHub access"]
async fn fake_lifecycle_preserves_completed_siblings_through_partial_approval_and_revision() {
    let url = env::var("DONKEYSPACE_CANCELLATION_TEST_DATABASE_URL").unwrap();
    assert!(url.ends_with("/donkeyspace_cancellation_test"));
    let pool = donkeyspace_db::connect(&DbConfig::from_database_url(url))
        .await
        .unwrap();
    donkeyspace_db::apply_migrations(&pool).await.unwrap();
    let owner = format!("fake-lifecycle-{}", Uuid::now_v7());
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
    let input = json!({"donkeyspace_lifecycle_coordinator":true,"repository":{"owner":{"login":owner},"name":"fixture"},"issue":{"number":1,"title":"fake lifecycle"}});
    let coordinator = create_job(&pool, Some(workflow), "architect", &input)
        .await
        .unwrap();
    let workspace = env::temp_dir().join(format!("donkeyspace-fake-lifecycle-{}", coordinator.id));
    let repo = workspace.join("repo");
    fs::create_dir_all(&repo).unwrap();
    let result = json!({"outcome":"implemented","summary":"fake task completed","confidence":"high","risk":"low","questions":[],"tests":[{"name":"fixture command","command":["true"],"status":"passed","exit_code":0}],"changed_files":[],"resources_used":[],"work_items":["one","two"]});
    let result_command = format!(
        "true; printf '%s' '{}' > .donkeyspace/run-result.json",
        result
    );
    let registry = json!({"work_items":[{"id":"one","spec":"docs/one.md","depends_on":[]},{"id":"two","spec":"docs/two.md","depends_on":[]}]});
    let planner_command = format!(
        "umask 000; mkdir -p repo/docs; printf '%s' '{}' > repo/docs/items.json; touch repo/docs/one.md repo/docs/two.md; {}",
        registry, result_command
    );
    let manifest = json!({"api_version":1,"id":"test.lifecycle","runtime":{"default_image":"busybox:latest"},
        "roles":{"architect":{"command":["sh","-c",planner_command]},"rtl":{"command":["sh","-c",result_command]}},
        "flows":{"test":{"start":"architect","replaces_default_lifecycle":true,"work_items_path":"docs/items.json","max_parallel_tasks":2,
            "tasks":{"architect":{"role":"architect","write":["docs"]},"rtl":{"role":"rtl","scope":"work_item","dependencies":["architect"],"approval":"required"}}}}});
    let manifest_path = workspace.join("plugin.json");
    fs::write(&manifest_path, manifest.to_string()).unwrap();
    let selection = PluginFlowSelection {
        manifest_path: manifest_path.to_string_lossy().into(),
        flow: "test".into(),
        max_handoffs_per_edge: None,
        environment: BTreeMap::new(),
        parameters: BTreeMap::new(),
        task_access_overrides: BTreeMap::new(),
    };
    let policy =
        donkeyspace_core::Policy::from_yaml(include_str!("../../../../.donkeyspace/policy.yml"))
            .unwrap();
    let decisions = [
        None,
        Some(json!({"action":"approve","target":"rtl/one"})),
        Some(json!({"action":"revise","target":"rtl/two","feedback":"try again"})),
        Some(json!({"action":"approve","target":"rtl/two"})),
    ];
    for (step, decision) in decisions.into_iter().enumerate() {
        if let Some(decision) = decision {
            let mut next = input.clone();
            next["donkeyspace_human_decision"] = decision;
            resume_latest_paused_job(&pool, workflow, &next)
                .await
                .unwrap()
                .unwrap();
        }
        acquire_job_lease(&pool, coordinator.id, "fake-test", 120)
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
            "fake-test",
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
            if step == 3 {
                Outcome::Implemented
            } else {
                Outcome::NeedsHuman
            }
        );
        let jobs = list_jobs_for_workflow_item(&pool, workflow).await.unwrap();
        let children = jobs
            .iter()
            .filter(|job| job.role == "rtl")
            .collect::<Vec<_>>();
        assert_eq!(children.len(), if step < 2 { 2 } else { 3 });
        assert!(children.iter().all(|job| job.status == "completed"));
        assert_eq!(
            children
                .iter()
                .filter(|job| job
                    .input
                    .pointer("/plugin_execution/work_item/id")
                    .and_then(Value::as_str)
                    == Some("one"))
                .count(),
            1
        );
    }
    assert!(
        donkeyspace_db::lifecycle_checkpoints::load(&pool, coordinator.id)
            .await
            .unwrap()
            .unwrap()
            .completed
    );
    // A failed parallel task must not overwrite its successful sibling's
    // execution record when the coordinator later reports failure.
    donkeyspace_db::complete_job(&pool, coordinator.id, &result)
        .await
        .unwrap();
    let failed_run = create_job(&pool, Some(workflow), "architect", &input)
        .await
        .unwrap();
    let failure_workspace =
        env::temp_dir().join(format!("donkeyspace-fake-lifecycle-{}", failed_run.id));
    let failure_repo = failure_workspace.join("repo");
    fs::create_dir_all(&failure_repo).unwrap();
    let mut failure_manifest = manifest.clone();
    failure_manifest["roles"]["rtl"]["command"] = json!([
        "sh",
        "-c",
        format!(
            "if grep -q '\"id\": \"two\"' .donkeyspace/run-input.json; then exit 1; fi; {result_command}"
        )
    ]);
    let failure_manifest_path = failure_workspace.join("plugin.json");
    fs::write(&failure_manifest_path, failure_manifest.to_string()).unwrap();
    let failure_selection = PluginFlowSelection {
        manifest_path: failure_manifest_path.to_string_lossy().into(),
        ..selection.clone()
    };
    acquire_job_lease(&pool, failed_run.id, "fake-test", 120)
        .await
        .unwrap();
    let failed_run = mark_job_running(&pool, failed_run.id)
        .await
        .unwrap()
        .unwrap();
    assert!(
        crate::plugin_container::with_execution_owner(
            &pool,
            failed_run.id,
            "fake-test",
            run(
                &failure_selection,
                &failure_repo,
                &failure_workspace,
                &failed_run.input,
                Some(LifecycleTracking {
                    pool: &pool,
                    policy: &policy,
                    coordinator: &failed_run,
                    github: None,
                    publication: None
                }),
            )
        )
        .await
        .is_err()
    );
    let coordinator_id = failed_run.id.to_string();
    let children = list_jobs_for_workflow_item(&pool, workflow)
        .await
        .unwrap()
        .into_iter()
        .filter(|job| {
            job.input
                .pointer("/plugin_execution/coordinator_run_id")
                .and_then(Value::as_str)
                == Some(coordinator_id.as_str())
        })
        .collect::<Vec<_>>();
    assert_eq!(children.len(), 2);
    for child in children {
        let item = child
            .input
            .pointer("/plugin_execution/work_item/id")
            .and_then(Value::as_str)
            .unwrap();
        assert_eq!(
            child.status,
            if item == "one" { "completed" } else { "failed" }
        );
    }
    // Two agent-requested pauses in the same wave must both survive. A
    // decision for one target must not silently release the other target.
    let blocked_run = create_job(&pool, Some(workflow), "architect", &input)
        .await
        .unwrap();
    let blocked_workspace =
        env::temp_dir().join(format!("donkeyspace-fake-lifecycle-{}", blocked_run.id));
    let blocked_repo = blocked_workspace.join("repo");
    fs::create_dir_all(&blocked_repo).unwrap();
    let mut blocked_manifest = manifest.clone();
    blocked_manifest["flows"]["test"]["tasks"]["rtl"]["approval"] = json!("none");
    let mut blocked_result = result.clone();
    blocked_result["outcome"] = json!("needs_human");
    blocked_result["human_review_reason"] = json!("Restore shared infrastructure.");
    blocked_manifest["roles"]["rtl"]["command"] = json!([
        "sh",
        "-c",
        format!(
            "printf '%s' '{}' > .donkeyspace/run-result.json",
            blocked_result
        )
    ]);
    let blocked_manifest_path = blocked_workspace.join("plugin.json");
    fs::write(&blocked_manifest_path, blocked_manifest.to_string()).unwrap();
    let blocked_selection = PluginFlowSelection {
        manifest_path: blocked_manifest_path.to_string_lossy().into(),
        ..selection.clone()
    };
    for step in 0..3 {
        if step > 0 {
            // The prerequisite is fixed, but only explicit decisions may
            // release the two paused targets.
            blocked_manifest["roles"]["rtl"]["command"] = json!(["sh", "-c", result_command]);
            fs::write(&blocked_manifest_path, blocked_manifest.to_string()).unwrap();
            let mut next = input.clone();
            next["donkeyspace_human_decision"] = json!({"action":"approve", "target": if step == 1 { "rtl/one" } else { "rtl/two" }});
            resume_latest_paused_job(&pool, workflow, &next)
                .await
                .unwrap()
                .unwrap();
        }
        acquire_job_lease(&pool, blocked_run.id, "fake-test", 120)
            .await
            .unwrap()
            .unwrap();
        let job = mark_job_running(&pool, blocked_run.id)
            .await
            .unwrap()
            .unwrap();
        let outcome = crate::plugin_container::with_execution_owner(
            &pool,
            job.id,
            "fake-test",
            run(
                &blocked_selection,
                &blocked_repo,
                &blocked_workspace,
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
            outcome.outcome,
            if step == 2 {
                Outcome::Implemented
            } else {
                Outcome::NeedsHuman
            }
        );
        let checkpoint = donkeyspace_db::lifecycle_checkpoints::load(&pool, job.id)
            .await
            .unwrap()
            .unwrap();
        let saved: LifecycleCheckpoint = serde_json::from_value(checkpoint.state).unwrap();
        assert_eq!(
            saved.pending_approvals.len(),
            if step == 0 {
                2
            } else if step == 1 {
                1
            } else {
                0
            }
        );
        if step == 1 {
            assert_eq!(saved.pending_approvals[0].key.target(), "rtl/two");
        }
        let id = job.id.to_string();
        let completed_children = list_jobs_for_workflow_item(&pool, workflow)
            .await
            .unwrap()
            .into_iter()
            .filter(|child| {
                child.status == "completed"
                    && child.role == "rtl"
                    && child
                        .input
                        .pointer("/plugin_execution/coordinator_run_id")
                        .and_then(Value::as_str)
                        == Some(id.as_str())
            })
            .count();
        assert_eq!(
            completed_children,
            if step == 2 { 4 } else { 2 },
            "partial approval must not rerun either task"
        );
    }
    fs::remove_dir_all(blocked_workspace).unwrap();
    fs::remove_dir_all(failure_workspace).unwrap();
    fs::remove_dir_all(workspace).unwrap();
}
