use super::*;
use donkeyspace_db::{
    DbConfig, RepositoryInput, WorkflowItemInput, acquire_job_lease, create_job,
    list_jobs_for_workflow_item, mark_job_running, upsert_repository, upsert_workflow_item,
};

#[tokio::test]
#[ignore = "requires isolated PostgreSQL and Docker busybox:latest; no model or GitHub access"]
async fn shared_repair_converges_and_revalidates_siblings_with_bounded_feedback() {
    let url = env::var("DONKEYSPACE_CANCELLATION_TEST_DATABASE_URL").unwrap();
    assert!(url.ends_with("/donkeyspace_cancellation_test"));
    let pool = donkeyspace_db::connect(&DbConfig::from_database_url(url))
        .await
        .unwrap();
    donkeyspace_db::apply_migrations(&pool).await.unwrap();
    let policy =
        donkeyspace_core::Policy::from_yaml(include_str!("../../../../.donkeyspace/policy.yml"))
            .unwrap();

    // Single feedback invalidates an already successful validator sibling;
    // simultaneous feedback converges on the same workflow-scoped repair;
    // repeated feedback reaches the existing per-edge human gate.
    for mode in ["single", "parallel", "bounded"] {
        let owner = format!("shared-repair-{}", Uuid::now_v7());
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
            "repository":{"owner":{"login":owner},"name":"fixture"},
            "issue":{"number":1,"title":"shared repair fixture"}});
        let coordinator = create_job(&pool, Some(workflow), "plan", &input)
            .await
            .unwrap();
        let workspace = env::temp_dir().join(format!("shared-repair-{}", coordinator.id));
        let repo = workspace.join("repo");
        fs::create_dir_all(&repo).unwrap();
        let success = json!({"outcome":"implemented", "summary":"fixture completed",
            "confidence":"high", "risk":"low", "questions":[], "tests":[
                {"name":"fixture validation", "command":["true"], "status":"passed", "exit_code":0}],
            "changed_files":[], "resources_used":[], "work_items":null});
        let emit =
            |result: &Value| format!("printf '%s' '{}' > .donkeyspace/run-result.json", result);
        let mut proposal = success.clone();
        proposal["work_items"] = json!(["one", "two"]);
        proposal["changed_files"] = json!(["docs/items.json", "docs/one.md", "docs/two.md"]);
        let registry = json!({"work_items":[
            {"id":"one", "spec":"docs/one.md", "depends_on":[]},
            {"id":"two", "spec":"docs/two.md", "depends_on":[]}]});
        let planner = format!(
            "set -eu; umask 000; mkdir -p repo/docs; printf '%s' '{registry}' > repo/docs/items.json; touch repo/docs/one.md repo/docs/two.md; {}",
            emit(&proposal)
        );
        let mut setup_result = success.clone();
        setup_result["changed_files"] = json!(["shared/version"]);
        let setup = format!(
            "set -eu; umask 000; mkdir -p repo/shared; version=0; if test -f repo/shared/version; then version=$(cat repo/shared/version); grep -q COMPLETE_DIAGNOSTIC_TAIL .donkeyspace/run-input.json; {}; fi; echo $((version + 1)) > repo/shared/version; {}",
            if mode == "single" {
                "true"
            } else {
                "test $(grep -c '\"target\": \"setup\"' .donkeyspace/run-input.json) -ge 2"
            },
            emit(&setup_result)
        );
        let select_item = "item=two; if grep -q '\"spec\": \"docs/one.md\"' .donkeyspace/run-input.json; then item=one; fi";
        let mut builds = Vec::new();
        for item in ["one", "two"] {
            let mut result = success.clone();
            result["changed_files"] = json!([format!("blocks/{item}/built")]);
            builds.push(format!(
                "if test \"$item\" = {item}; then {}; fi",
                emit(&result)
            ));
        }
        let build = format!(
            "set -eu; umask 000; {select_item}; mkdir -p repo/blocks/$item; cp repo/shared/version repo/blocks/$item/built; {}",
            builds.join("; ")
        );
        let mut repair = success.clone();
        repair["outcome"] = json!("needs_changes");
        // Keep the reason deliberately absent from summary and beyond its
        // truncation limit. The target must receive the actual structured handoff.
        repair["summary"] = json!("shared operation failed");
        repair["handoff"] = json!({"target":"setup", "reason":format!(
            "{} COMPLETE_DIAGNOSTIC_TAIL", "Detailed evidence. ".repeat(200))});
        let validate = format!(
            "set -eu; {select_item}; grep -A2 '\"allowed_handoffs\": \\[' .donkeyspace/run-input.json | grep -q '\"setup\"'; cmp repo/shared/version repo/blocks/$item/built; if {}; then {}; else {}; fi",
            match mode {
                "single" => "test \"$item\" = two && test $(cat repo/shared/version) -eq 1",
                "parallel" => "test $(cat repo/shared/version) -eq 1",
                _ => "true",
            },
            emit(&repair),
            emit(&success)
        );
        let manifest = json!({"api_version":1, "id":"test.shared-repair",
            "runtime":{"default_image":"busybox:latest"},
            "roles":{
                "plan":{"command":["sh","-c",planner]},
                "setup":{"command":["sh","-c",setup]},
                "build":{"command":["sh","-c",build]},
                "validate":{"command":["sh","-c",validate]}},
            "flows":{"test":{"start":"plan", "replaces_default_lifecycle":true,
                "work_items_path":"docs/items.json", "max_parallel_tasks":2, "max_handoffs_per_edge":2,
                "tasks":{
                    "plan":{"role":"plan", "write":["docs"]},
                    "setup":{"role":"setup", "dependencies":["plan"], "read":["shared"], "write":["shared"]},
                    "build":{"role":"build", "scope":"work_item", "dependencies":["setup"],
                        "read":["docs", "shared"], "write":["blocks/{work_item}"]},
                    "validate":{"role":"validate", "scope":"work_item", "dependencies":["build", "setup"],
                        "read":["shared", "blocks/{work_item}"], "allowed_handoffs":["setup"]}}}}});
        let manifest_path = workspace.join("plugin.json");
        fs::write(&manifest_path, manifest.to_string()).unwrap();
        let selection: PluginFlowSelection = serde_json::from_value(json!({
            "manifest_path":manifest_path, "flow":"test"}))
        .unwrap();
        acquire_job_lease(&pool, coordinator.id, "shared-repair-test", 600)
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
            "shared-repair-test",
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
        let bounded = mode == "bounded";
        assert_eq!(
            result.outcome,
            if bounded {
                Outcome::NeedsHuman
            } else {
                Outcome::Implemented
            },
            "{mode}: {result:?}"
        );
        let executions = if bounded { 3 } else { 2 };
        assert_eq!(
            fs::read_to_string(repo.join("shared/version"))
                .unwrap()
                .trim(),
            executions.to_string()
        );
        let jobs = list_jobs_for_workflow_item(&pool, workflow).await.unwrap();
        for role in ["setup", "build", "validate"] {
            let expected = executions * if role == "setup" { 1 } else { 2 };
            assert_eq!(
                jobs.iter()
                    .filter(|job| job.role == role && job.status == "completed")
                    .count(),
                expected,
                "{mode}: {role}"
            );
        }
        for item in ["one", "two"] {
            assert_eq!(
                fs::read_to_string(repo.join(format!("blocks/{item}/built")))
                    .unwrap()
                    .trim(),
                executions.to_string()
            );
        }
        let checkpoint = donkeyspace_db::lifecycle_checkpoints::load(&pool, coordinator.id)
            .await
            .unwrap()
            .unwrap();
        let saved: LifecycleCheckpoint = serde_json::from_value(checkpoint.state).unwrap();
        assert_eq!(saved.pending_approvals.len(), usize::from(bounded));
        if bounded {
            assert_eq!(saved.pending_approvals[0].key.target(), "setup");
            assert!(
                result
                    .human_review_reason
                    .unwrap()
                    .contains("exceeded policy limit 2")
            );
        }
        fs::remove_dir_all(workspace).unwrap();
    }
}
