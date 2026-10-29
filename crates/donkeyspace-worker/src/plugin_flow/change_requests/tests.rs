use super::*;

fn fixture() -> (PluginFlow, LifecycleCheckpoint, Vec<PluginWorkItem>) {
    let flow: PluginFlow = serde_json::from_value(json!({"start":"plan","change_intake":true,
        "replaces_default_lifecycle":true,"work_items_path":"docs/items.json","execution_plan":{"optional_tasks":["package"]},
        "tasks":{"plan":{"role":"planner","approval":"required"},"infrastructure":{"role":"tools","dependencies":["plan"]},
        "build":{"role":"writer","scope":"work_item","dependencies":["plan","infrastructure"]},
        "check":{"role":"tester","scope":"work_item","dependencies":["build"]},
        "package":{"role":"publisher","scope":"work_item","dependencies":["build"]}}})).unwrap();
    let items: Vec<PluginWorkItem> = serde_json::from_value(json!([
        {"id":"one","spec":"docs/one.md"},{"id":"two","spec":"docs/two.md"}]))
    .unwrap();
    let mut graph = proposal_graph(&flow, &items);
    for target in ["infrastructure", "build/two", "check/two"] {
        let key = graph.keys().find(|k| k.target() == target).unwrap().clone();
        graph.mark_completed(&key).unwrap();
    }
    let pending = PendingApproval {
        key: TaskKey {
            task: "build".into(),
            work_item: Some("one".into()),
        },
        trigger: ApprovalTrigger::Required,
    };
    let result = pending_approval_result(
        &[pending.clone()],
        &BTreeMap::new(),
        &flow,
        "Review implementation.",
    );
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
        vec![pending],
        true,
        &items,
    );
    let mut plan = ExecutionPlan::resolve(
        &flow,
        Some(donkeyspace_core::ExecutionPlanProposal {
            excluded_tasks: vec![],
            approval_tasks: vec!["build".into(), "check".into()],
            reason: "Review implementation and checks".into(),
        }),
        vec!["one".into(), "two".into()],
        1,
    )
    .unwrap();
    plan.approved = true;
    saved.execution_plan = Some(plan);
    (flow, saved, items)
}

fn propose(
    saved: &mut LifecycleCheckpoint,
    flow: &PluginFlow,
    items: &[PluginWorkItem],
    value: Value,
) {
    let proposal: ChangeProposal = serde_json::from_value(value).unwrap();
    let (plan, affected) = validate_proposal(saved, flow, items, &proposal).unwrap();
    saved.change_sequence += 1;
    let pending = PendingChange {
        revision: saved.change_sequence,
        request: "human request".into(),
        target_hint: Some(flow.start.clone()),
        base_digest: base_digest(saved).unwrap(),
        flow_digest: flow_fingerprint(flow).unwrap(),
        proposal: Some(proposal),
        plan,
        affected,
    };
    saved.pending_approvals.push(PendingApproval {
        key: pending.key(),
        trigger: ApprovalTrigger::Required,
    });
    saved.pending_change = Some(pending);
}

#[test]
fn scheduling_change_preserves_completed_work_and_separate_artifact_approval() {
    let (flow, mut saved, items) = fixture();
    let completed = saved.completed_keys.clone();
    propose(
        &mut saved,
        &flow,
        &items,
        json!({"summary":"Skip packaging","execution_plan":{"excluded_tasks":["package"],"approval_tasks":["build","check"],"reason":"User request"},"revisions":[]}),
    );
    assert!(saved.execution_plan.as_ref().unwrap().tasks["package"].included);
    assert_eq!(saved.completed_keys, completed);
    assert!(
        change_review(&saved)
            .human_review_reason
            .unwrap()
            .contains("No completed task needs to rerun")
    );
    let applied = apply_proposal(&mut saved, &flow, &items, Some("approval-1")).unwrap();
    assert!(applied.invalidated.is_empty());
    assert_eq!(saved.completed_keys, completed);
    assert!(saved.start_approved);
    assert!(!saved.execution_plan.as_ref().unwrap().tasks["package"].included);
    assert!(saved.execution_plan.as_ref().unwrap().approved);
    assert_eq!(saved.pending_approvals.len(), 1);
    assert_eq!(saved.pending_approvals[0].key.target(), "build/one");
}

#[test]
fn mixed_change_revises_only_target_and_included_dependents() {
    let (flow, mut saved, items) = fixture();
    propose(
        &mut saved,
        &flow,
        &items,
        json!({"summary":"Simplify one and omit packaging","execution_plan":{"excluded_tasks":["package"],"approval_tasks":["build","check"]},"revisions":[{"target":"build/one","instruction":"Simplify without changing behavior"}]}),
    );
    let applied = apply_proposal(&mut saved, &flow, &items, Some("approval-1")).unwrap();
    assert_eq!(
        applied
            .invalidated
            .iter()
            .map(TaskKey::target)
            .collect::<Vec<_>>(),
        vec!["build/one", "check/one"]
    );
    assert!(
        saved
            .completed_keys
            .iter()
            .any(|k| k.target() == "infrastructure")
    );
    assert!(
        saved
            .completed_keys
            .iter()
            .any(|k| k.target() == "check/two")
    );
    assert_eq!(
        saved
            .revision_targets
            .iter()
            .map(TaskKey::target)
            .collect::<Vec<_>>(),
        vec!["build/one"]
    );
    assert!(saved.start_approved);
}

#[test]
fn human_pr_edits_still_require_revalidation_after_a_plan_only_request() {
    let (flow, mut saved, items) = fixture();
    let key = TaskKey {
        task: "build".into(),
        work_item: Some("two".into()),
    };
    saved.revision_targets.push(key);
    propose(
        &mut saved,
        &flow,
        &items,
        json!({"summary":"Skip packaging", "execution_plan":{"excluded_tasks":["package"],"approval_tasks":["build","check"]}, "revisions":[]}),
    );
    assert_eq!(
        saved
            .pending_change
            .as_ref()
            .unwrap()
            .affected
            .iter()
            .map(TaskKey::target)
            .collect::<Vec<_>>(),
        vec!["build/two", "check/two"]
    );
    let review = change_review(&saved).human_review_reason.unwrap();
    assert!(review.contains("build/two"));
    assert!(!review.contains("No completed task needs to rerun"));
    apply_proposal(&mut saved, &flow, &items, None).unwrap();
    assert!(
        saved
            .completed_keys
            .iter()
            .all(|key| key.work_item.as_deref() != Some("two"))
    );
    assert!(saved.start_approved);
}

#[test]
fn changed_checkpoint_or_configuration_rejects_stale_proposal() {
    let (flow, mut saved, items) = fixture();
    propose(
        &mut saved,
        &flow,
        &items,
        json!({"summary":"Keep current workflow","execution_plan":null,"revisions":[]}),
    );
    let mut altered = saved.clone();
    altered.start_approved = false;
    assert!(
        apply_proposal(&mut altered, &flow, &items, None)
            .unwrap_err()
            .to_string()
            .contains("checkpoint")
    );
    let mut new_flow = flow.clone();
    new_flow.max_parallel_tasks += 1;
    assert!(apply_proposal(&mut saved, &new_flow, &items, None).is_err());
    assert!(saved.pending_change.is_some());
}

#[test]
fn invalid_proposals_do_not_change_accepted_state() {
    let (flow, saved, items) = fixture();
    let before = serde_json::to_value(&saved).unwrap();
    for value in [
        json!({"summary":"Unknown target","execution_plan":null,"revisions":[{"target":"missing/one","instruction":"Change"}]}),
        json!({"summary":"Omit required task","execution_plan":{"excluded_tasks":["infrastructure"]},"revisions":[]}),
        json!({"summary":"Contradictory request","execution_plan":{"excluded_tasks":["package"]},"revisions":[{"target":"package/one","instruction":"Run"}]}),
    ] {
        let proposal = serde_json::from_value(value).unwrap();
        assert!(validate_proposal(&saved, &flow, &items, &proposal).is_err());
        assert_eq!(serde_json::to_value(&saved).unwrap(), before);
    }
}

#[tokio::test]
#[ignore = "requires isolated PostgreSQL and Docker busybox:latest; no model or GitHub"]
async fn change_intake_preserves_work_through_proposal_clarification_and_revision() {
    let url = env::var("DONKEYSPACE_CANCELLATION_TEST_DATABASE_URL").unwrap();
    assert!(url.ends_with("/donkeyspace_cancellation_test"));
    let pool = donkeyspace_db::connect(&donkeyspace_db::DbConfig::from_database_url(url))
        .await
        .unwrap();
    donkeyspace_db::apply_migrations(&pool).await.unwrap();
    let job = donkeyspace_db::create_job(&pool, None, "fixture", &json!({}))
        .await
        .unwrap();
    donkeyspace_db::acquire_job_lease(&pool, job.id, "change-test", 300)
        .await
        .unwrap()
        .unwrap();
    donkeyspace_db::mark_job_running(&pool, job.id)
        .await
        .unwrap()
        .unwrap();
    crate::plugin_container::with_execution_owner(&pool,job.id,"change-test",async {
        let workspace=env::temp_dir().join(format!("ds-change-{}",Uuid::now_v7()));
        let repo=workspace.join("repo");fs::create_dir_all(repo.join("fixtures")).unwrap();
        let success=json!({"outcome":"implemented","summary":"fixture passed","confidence":"high","risk":"low","questions":[],"tests":[{"name":"fixture","command":["true"],"status":"passed","exit_code":0}],"changed_files":[]});
        let mut planner=success.clone();planner["work_items"]=json!(["one"]);
        planner["execution_plan"]=json!({"excluded_tasks":[],"approval_tasks":["build","check"]});
        let registry=json!({"work_items":[{"id":"one","spec":"docs/one.md"}]});
        let planning=format!("umask 000; if grep -q '\"change_request\": {{' .donkeyspace/run-input.json; then cat repo/fixtures/proposal.json > .donkeyspace/run-result.json; printf unauthorized > repo/docs/one.md; else mkdir -p repo/docs; printf contract > repo/docs/one.md; printf '%s' '{}' > repo/docs/items.json; printf '%s' '{}' > .donkeyspace/run-result.json; fi",registry,planner);
        let writing=|path:&str| format!("umask 000; mkdir -p repo/{path}; printf attempt >> repo/{path}/history; printf '%s' '{}' > .donkeyspace/run-result.json",success);
        let manifest=json!({"api_version":1,"id":"test.change-intake","runtime":{"default_image":"busybox:latest"},"roles":{
            "planner":{"command":["sh","-c",planning]},"tools":{"command":["sh","-c",writing("infra")]},
            "writer":{"command":["sh","-c",writing("build/one")]},"checker":{"command":["sh","-c",writing("check/one")]},"publisher":{"command":["sh","-c","exit 99"]}},
            "flows":{"test":{"start":"plan","change_intake":true,"replaces_default_lifecycle":true,"work_items_path":"docs/items.json","execution_plan":{"optional_tasks":["package"]},"tasks":{
                "plan":{"role":"planner","read":["docs","fixtures"],"write":["docs"]},
                "infrastructure":{"role":"tools","dependencies":["plan"],"write":["infra"]},
                "build":{"role":"writer","scope":"work_item","dependencies":["plan","infrastructure"],"write":["build/{work_item}"]},
                "check":{"role":"checker","scope":"work_item","dependencies":["build"],"write":["check/{work_item}"]},
                "package":{"role":"publisher","scope":"work_item","dependencies":["build"],"write":["package/{work_item}"]}}}}});
        let path=workspace.join("plugin.json");fs::write(&path,manifest.to_string()).unwrap();
        let selection:PluginFlowSelection=serde_json::from_value(json!({"manifest_path":path,"flow":"test"})).unwrap();
        let resume=|id,action:&str,target:Option<&str>,feedback:&str| json!({"donkeyspace_resume":true,"issue":{"title":"one"},"comment":{"id":id,"body":feedback},"donkeyspace_human_decision":{"action":action,"target":target,"feedback":feedback}});
        let snapshot=|| serde_json::from_str::<LifecycleCheckpoint>(&fs::read_to_string(workspace.join(".donkeyspace/lifecycle-checkpoint.json")).unwrap()).unwrap();
        assert_eq!(run(&selection,&repo,&workspace,&json!({"issue":{"title":"one"}}),None).await.unwrap().outcome,Outcome::NeedsHuman);
        run(&selection,&repo,&workspace,&resume(1,"approve",Some("plan"),""),None).await.unwrap();
        let mut proposed=success.clone();proposed["change_proposal"]=json!({"summary":"Skip packaging","execution_plan":{"excluded_tasks":["package"],"approval_tasks":["build","check"]},"revisions":[]});
        fs::write(repo.join("fixtures/proposal.json"),proposed.to_string()).unwrap();
        let request=resume(2,"revise",Some("plan"),"Skip packaging and keep both reviews");
        run(&selection,&repo,&workspace,&request,None).await.unwrap();
        assert_eq!(fs::read_to_string(repo.join("docs/one.md")).unwrap(),"contract");
        assert_eq!(fs::read_to_string(repo.join("build/one/history")).unwrap(),"attempt");
        assert!(snapshot().execution_plan.unwrap().tasks["package"].included);
        run(&selection,&repo,&workspace,&request,None).await.unwrap();
        assert_eq!(snapshot().change_sequence,1);
        for (id,target) in [(3,"change/0"),(4,"build/one")] {
            let rejected=run(&selection,&repo,&workspace,&resume(id,"approve",Some(target),""),None).await.unwrap();
            assert!(rejected.human_review_reason.unwrap().contains("not applied"));
            assert!(snapshot().pending_change.is_some());
        }
        run(&selection,&repo,&workspace,&resume(5,"approve",Some("change/1"),""),None).await.unwrap();
        assert!(snapshot().start_approved);
        assert!(!snapshot().execution_plan.unwrap().tasks["package"].included);
        assert_eq!(snapshot().pending_approvals[0].key.target(),"build/one");
        let stale = run(&selection,&repo,&workspace,&resume(51,"approve",Some("change/1"),""),None).await.unwrap();
        assert!(stale.human_review_reason.unwrap().contains("approve build/one"));
        let question=json!({"outcome":"needs_info","summary":"Which simplification?","confidence":"high","risk":"low","questions":["Which simplification do you want?"],"tests":[],"changed_files":[]});
        fs::write(repo.join("fixtures/proposal.json"),question.to_string()).unwrap();
        let asking=run(&selection,&repo,&workspace,&resume(6,"revise",None,"Simplify the implementation"),None).await.unwrap();
        assert_eq!(asking.outcome,Outcome::NeedsInfo);
        proposed["change_proposal"]=json!({"summary":"Simplify the implementation","execution_plan":null,"revisions":[{"target":"build/one","instruction":"Inline the helper and explain its behavior"}]});
        fs::write(repo.join("fixtures/proposal.json"),proposed.to_string()).unwrap();
        run(&selection,&repo,&workspace,&json!({"donkeyspace_resume":true,"issue":{"title":"one"},"comment":{"id":7,"body":"Inline the helper"}}),None).await.unwrap();
        assert_eq!(snapshot().change_sequence,3);
        assert_eq!(fs::read_to_string(repo.join("build/one/history")).unwrap(),"attempt");
        run(&selection,&repo,&workspace,&resume(8,"approve",Some("change/3"),""),None).await.unwrap();
        assert_eq!(fs::read_to_string(repo.join("build/one/history")).unwrap(),"attemptattempt");
        assert_eq!(fs::read_to_string(repo.join("infra/history")).unwrap(),"attempt");
        assert_eq!(fs::read_to_string(repo.join("docs/one.md")).unwrap(),"contract");
        assert!(!repo.join("check/one/history").exists());
        assert_eq!(snapshot().change_sequence,3);
        run(&selection,&repo,&workspace,&resume(9,"approve",Some("build/one"),""),None).await.unwrap();
        assert_eq!(snapshot().pending_approvals[0].key.target(),"check/one");
        let completed=run(&selection,&repo,&workspace,&resume(10,"approve",Some("check/one"),""),None).await.unwrap();
        assert_eq!(completed.outcome,Outcome::Implemented);
        assert!(completed.summary.contains("across 5 task execution(s)"));
        fs::remove_dir_all(workspace).unwrap();
    }).await;
}
