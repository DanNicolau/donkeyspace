use super::*;

#[tokio::test]
#[ignore = "requires isolated PostgreSQL and Docker busybox:latest; no model or GitHub"]
async fn execution_plan_gates_revisions_and_excludes_tasks_across_restarts() {
    let url = env::var("DONKEYSPACE_CANCELLATION_TEST_DATABASE_URL").unwrap();
    assert!(url.ends_with("/donkeyspace_cancellation_test"));
    let pool = donkeyspace_db::connect(&donkeyspace_db::DbConfig::from_database_url(url))
        .await
        .unwrap();
    donkeyspace_db::apply_migrations(&pool).await.unwrap();
    let job = donkeyspace_db::create_job(&pool, None, "fixture", &json!({}))
        .await
        .unwrap();
    donkeyspace_db::acquire_job_lease(&pool, job.id, "plan-test", 300)
        .await
        .unwrap()
        .unwrap();
    donkeyspace_db::mark_job_running(&pool, job.id)
        .await
        .unwrap()
        .unwrap();
    crate::plugin_container::with_execution_owner(&pool, job.id, "plan-test", async {
    let workspace = env::temp_dir().join(format!("ds-plan-{}", Uuid::now_v7()));
    let repo = workspace.join("repo");
    fs::create_dir_all(repo.join("build/one")).unwrap();
    fs::create_dir_all(repo.join("check/one")).unwrap();
    fs::create_dir_all(repo.join("package/one")).unwrap();
    let success = json!({"outcome":"implemented","summary":"fixture passed","confidence":"high","risk":"low","questions":[],"tests":[{"name":"fixture","command":["true"],"status":"passed","exit_code":0}],"changed_files":[]});
    let mut planner = success.clone();
    planner["work_items"] = json!(["one"]);
    planner["execution_plan"] = json!({"excluded_tasks":["package"],"approval_tasks":["build"],"reason":"User requests implementation review and no packaging"});
    let registry = json!({"work_items":[{"id":"one","spec":"docs/one.md"}]});
    let plan_command = format!(
        "umask 000; mkdir -p repo/docs; printf contract > repo/docs/one.md; printf '%s' '{}' > repo/docs/items.json; printf '%s' '{}' > .donkeyspace/run-result.json",
        registry, planner
    );
    let build_command = format!(
        "umask 000; printf attempt >> repo/build/one/history; printf '%s' '{}' > .donkeyspace/run-result.json",
        success
    );
    let check_command = format!(
        "umask 000; cp repo/build/one/history repo/check/one/verified; printf '%s' '{}' > .donkeyspace/run-result.json",
        success
    );
    let manifest = json!({"api_version":1,"id":"test.execution-plan","runtime":{"default_image":"busybox:latest"},"roles":{
        "planner":{"command":["sh","-c",plan_command]},"writer":{"command":["sh","-c",build_command]},"checker":{"command":["sh","-c",check_command]},"publisher":{"command":["sh","-c","exit 99"]}},
        "flows":{"test":{"start":"plan","replaces_default_lifecycle":true,"work_items_path":"docs/items.json","execution_plan":{"optional_tasks":["package"]},"tasks":{
            "plan":{"role":"planner","write":["docs"]},
            "build":{"role":"writer","scope":"work_item","dependencies":["plan"],"read":["docs"],"write":["build/{work_item}"]},
            "check":{"role":"checker","scope":"work_item","dependencies":["build"],"read":["build/{work_item}"],"write":["check/{work_item}"]},
            "package":{"role":"publisher","scope":"work_item","dependencies":["build"],"write":["package/{work_item}"]}}}}});
    let manifest_path = workspace.join("plugin.json");
    fs::write(&manifest_path, manifest.to_string()).unwrap();
    let selection: PluginFlowSelection =
        serde_json::from_value(json!({"manifest_path":manifest_path,"flow":"test"})).unwrap();
    let input = json!({"issue":{"title":"one"}});
    let first = run(&selection, &repo, &workspace, &input, None)
        .await
        .unwrap();
    assert_eq!(first.outcome, Outcome::NeedsHuman);
    assert!(first.summary.contains("awaiting approval"));
    assert!(!repo.join("build/one/history").exists());
    let resume = |id, action: &str, target: &str, feedback: &str| json!({"donkeyspace_resume":true,"issue":{"title":"one"},"comment":{"id":id},"donkeyspace_human_decision":{"action":action,"target":target,"feedback":feedback}});
    let second = run(
        &selection,
        &repo,
        &workspace,
        &resume(1, "approve", "plan", ""),
        None,
    )
    .await
    .unwrap();
    assert_eq!(second.outcome, Outcome::NeedsHuman);
    assert!(!repo.join("check/one/verified").exists());
    assert_eq!(
        fs::read_to_string(repo.join("build/one/history")).unwrap(),
        "attempt"
    );
    let third = run(
        &selection,
        &repo,
        &workspace,
        &resume(2, "revise", "build/one", "simplify"),
        None,
    )
    .await
    .unwrap();
    assert_eq!(third.outcome, Outcome::NeedsHuman);
    assert_eq!(
        fs::read_to_string(repo.join("build/one/history")).unwrap(),
        "attemptattempt"
    );
    // Redelivering the same decision must not cause another edit.
    run(
        &selection,
        &repo,
        &workspace,
        &resume(2, "revise", "build/one", "simplify"),
        None,
    )
    .await
    .unwrap();
    assert_eq!(
        fs::read_to_string(repo.join("build/one/history")).unwrap(),
        "attemptattempt"
    );
    let final_result = run(
        &selection,
        &repo,
        &workspace,
        &resume(3, "approve", "build/one", ""),
        None,
    )
    .await
    .unwrap();
    assert_eq!(final_result.outcome, Outcome::Implemented);
    assert!(final_result.summary.contains("across 4 task execution(s)"));
    assert_eq!(
        fs::read_to_string(repo.join("check/one/verified")).unwrap(),
        "attemptattempt"
    );
    assert!(final_result.summary.contains("No — excluded"));
    assert!(final_result.summary.contains("v1 — approved"));
    fs::remove_dir_all(workspace).unwrap();
    }).await;
}

#[test]
fn execution_plan_change_preserves_spec_approval_and_requires_its_own_approval() {
    let base: PluginFlow = serde_json::from_value(json!({"start":"plan","execution_plan":{"optional_tasks":["package"]},"tasks":{"plan":{"role":"planner","approval":"required"},"build":{"role":"writer","scope":"work_item","dependencies":["plan"]},"package":{"role":"publisher","scope":"work_item","dependencies":["build"]}}})).unwrap();
    let items: Vec<PluginWorkItem> =
        serde_json::from_value(json!([{"id":"one","spec":"docs/one.md"}])).unwrap();
    let mut graph = TaskGraph::for_work_items(&base, &items);
    let build = TaskKey {
        task: "build".into(),
        work_item: Some("one".into()),
    };
    graph.mark_completed(&build).unwrap();
    let result: RunResult = serde_json::from_value(json!({"outcome":"failed","summary":"failed check","confidence":"high","risk":"low","blocked_reason":"failed"})).unwrap();
    let mut saved = LifecycleCheckpoint::capture(
        1,
        &[],
        &[],
        Risk::Low,
        Confidence::High,
        &result,
        &graph,
        &BTreeMap::new(),
        &BTreeMap::new(),
        &BTreeMap::new(),
        &BTreeSet::new(),
        vec![],
        true,
        &items,
    );
    let decision = json!({"action":"revise","target":"execution-plan","feedback":"excluded_tasks: [package]\napproval_tasks: [build]\nreason: Human-directed scope","decision_id":"one"});
    apply_human_decision(&mut saved, &base, &decision).unwrap();
    assert!(saved.start_approved);
    assert!(!saved.execution_plan.as_ref().unwrap().approved);
    assert!(saved.pending_approvals.iter().any(|p| p.key == build));
    assert!(
        saved
            .pending_approvals
            .iter()
            .any(|p| p.key.task == "execution-plan")
    );
    apply_human_decision(&mut saved, &base, &decision).unwrap();
    assert_eq!(saved.execution_plan.as_ref().unwrap().revision, 1);
    assert!(
        apply_human_decision(
            &mut saved,
            &base,
            &json!({"action":"approve","target":"all"})
        )
        .is_err()
    );
    let mut plan_only = saved.clone();
    plan_only
        .pending_approvals
        .retain(|a| a.key.task == "execution-plan");
    assert!(apply_human_decision(&mut plan_only, &base, &json!({"action":"approve"})).is_err());
    assert!(!plan_only.execution_plan.as_ref().unwrap().approved);
    apply_human_decision(
        &mut saved,
        &base,
        &json!({"action":"approve","target":"execution-plan"}),
    )
    .unwrap();
    assert!(saved.start_approved);
    assert!(saved.execution_plan.as_ref().unwrap().approved);
    assert_eq!(saved.pending_approvals.len(), 1);
}
