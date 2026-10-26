use super::*;
use donkeyspace_db::{
    DbConfig, RepositoryInput, WorkflowItemInput, acquire_job_lease, create_job, mark_job_running,
    resume_latest_paused_job, upsert_repository, upsert_workflow_item,
};

#[tokio::test]
#[ignore = "requires disposable PostgreSQL and Docker busybox:latest; no model or GitHub access"]
async fn pending_infrastructure_retry_loads_current_scope_and_preserves_block_boundaries() {
    let url = env::var("DONKEYSPACE_CANCELLATION_TEST_DATABASE_URL").unwrap();
    assert!(url.ends_with("/donkeyspace_cancellation_test"));
    let pool = donkeyspace_db::connect(&DbConfig::from_database_url(url))
        .await
        .unwrap();
    donkeyspace_db::apply_migrations(&pool).await.unwrap();
    let owner = format!("platform-resume-{}", Uuid::now_v7());
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
    let input = json!({"donkeyspace_lifecycle_coordinator":true,
        "repository":{"owner":{"login":owner},"name":"fixture"},"issue":{"number":1,"title":"platform resume"}});
    let coordinator = create_job(&pool, Some(workflow), "architect", &input)
        .await
        .unwrap();
    let workspace = env::temp_dir().join(format!("platform-resume-{}", coordinator.id));
    let repo = workspace.join("repo");
    fs::create_dir_all(repo.join("src/rtl/other")).unwrap();
    fs::write(repo.join("src/rtl/other/block.v"), "unchanged block").unwrap();
    let result = json!({"outcome":"implemented","summary":"fixture completed","confidence":"high","risk":"low",
        "questions":[],"tests":[{"name":"fixture","command":["true"],"status":"passed","exit_code":0}],
        "changed_files":[],"resources_used":[],"work_items":null});
    let mut proposal = result.clone();
    proposal["work_items"] = json!(["one"]);
    proposal["changed_files"] = json!(["docs/items.json", "docs/spec.md"]);
    let registry = json!({"work_items":[{"id":"one","spec":"docs/spec.md","depends_on":[]}]});
    let planner = format!(
        "set -eu; umask 000; mkdir -p repo/docs; printf '%s' '{registry}' > repo/docs/items.json; echo accepted > repo/docs/spec.md; printf '%s' '{proposal}' > .donkeyspace/run-result.json"
    );
    let mut pause = result.clone();
    pause["outcome"] = json!("needs_human");
    pause["human_review_reason"] = json!("Platform mapping outside scope");
    let old = format!(
        "set -eu; ! grep -q '\"src/syn/platforms\"' .donkeyspace/run-input.json; printf '%s' '{pause}' > .donkeyspace/run-result.json"
    );
    let mut manifest = json!({"api_version":1,"id":"test.platform-resume","runtime":{"default_image":"busybox:latest"},
    "roles":{"architect":{"command":["sh","-c",planner]},"infrastructure":{"command":["sh","-c",old]}},
    "flows":{"test":{"start":"architect","replaces_default_lifecycle":true,"work_items_path":"docs/items.json","tasks":{
        "architect":{"role":"architect","write":["docs"]},
        "infrastructure":{"role":"infrastructure","dependencies":["architect"],"read":["docs","src/rtl"],"write":["src/syn/scripts"]}
    }}}});
    let manifest_path = workspace.join("plugin.json");
    fs::write(&manifest_path, manifest.to_string()).unwrap();
    let selection: PluginFlowSelection =
        serde_json::from_value(json!({"manifest_path":manifest_path,"flow":"test"})).unwrap();
    let policy =
        donkeyspace_core::Policy::from_yaml(include_str!("../../../../.donkeyspace/policy.yml"))
            .unwrap();
    for step in 0..2 {
        if step == 1 {
            let mut repaired = result.clone();
            repaired["changed_files"] = json!([
                "src/syn/platforms/test/config.mk",
                "src/syn/platforms/test/latch_map.v"
            ]);
            let new = format!(
                "set -eu; umask 000; grep -q '\"src/syn/platforms\"' .donkeyspace/run-input.json; test -f repo/src/rtl/other/block.v; mkdir -p repo/src/syn/platforms/test; echo mapping > repo/src/syn/platforms/test/latch_map.v; echo configured > repo/src/syn/platforms/test/config.mk; printf '%s' '{repaired}' > .donkeyspace/run-result.json"
            );
            manifest["flows"]["test"]["tasks"]["infrastructure"]["write"] =
                json!(["src/syn/scripts", "src/syn/platforms"]);
            manifest["roles"]["infrastructure"]["command"] = json!(["sh", "-c", new]);
            // If the accepted architect were replayed, fail immediately.
            manifest["roles"]["architect"]["command"] = json!(["false"]);
            fs::write(&manifest_path, manifest.to_string()).unwrap();
            let mut next = input.clone();
            next["donkeyspace_human_decision"] = json!({"action":"revise","target":"infrastructure","feedback":"repair platform with updated scope"});
            resume_latest_paused_job(&pool, workflow, &next)
                .await
                .unwrap()
                .unwrap();
        }
        acquire_job_lease(&pool, coordinator.id, "platform-resume-test", 600)
            .await
            .unwrap()
            .unwrap();
        let job = mark_job_running(&pool, coordinator.id)
            .await
            .unwrap()
            .unwrap();
        let outcome = crate::plugin_container::with_execution_owner(
            &pool,
            job.id,
            "platform-resume-test",
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
            outcome.outcome,
            if step == 0 {
                Outcome::NeedsHuman
            } else {
                Outcome::Implemented
            }
        );
    }
    assert_eq!(
        fs::read_to_string(repo.join("src/syn/platforms/test/latch_map.v")).unwrap(),
        "mapping\n"
    );
    assert_eq!(
        fs::read_to_string(repo.join("docs/spec.md")).unwrap(),
        "accepted\n"
    );
    assert_eq!(
        fs::read_to_string(repo.join("src/rtl/other/block.v")).unwrap(),
        "unchanged block"
    );
    let loaded = PluginManifest::from_path(&manifest_path).unwrap();
    let task = &loaded.flows["test"].tasks["infrastructure"];
    assert!(
        validate_changed_files(&["src/syn/platforms/test/latch_map.v".into()], &task.write).is_ok()
    );
    assert!(validate_changed_files(&["src/rtl/other/block.v".into()], &task.write).is_err());
    assert!(
        validate_changed_files(
            &["src/syn/designs/other/constraint.sdc".into()],
            &task.write
        )
        .is_err()
    );
    let mut narrowed = selection.clone();
    narrowed.task_access_overrides.insert(
        "infrastructure".into(),
        serde_json::from_value(json!({"write":["src/syn/scripts"]})).unwrap(),
    );
    let (_, write) = resolve_access(
        &narrowed,
        "infrastructure",
        &task.read,
        &task.write,
        &BTreeMap::new(),
        &loaded.parameters,
    )
    .unwrap();
    assert!(
        validate_changed_files(&["src/syn/platforms/test/latch_map.v".into()], &write).is_err()
    );
    fs::remove_dir_all(workspace).unwrap();
}
