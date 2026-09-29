use super::*;
use crate::{checkout_recovery, trusted_git};
use donkeyspace_db::{
    DbConfig, RepositoryInput, WorkflowItemInput, acquire_job_lease, create_job, mark_job_running,
    resume_latest_paused_job, upsert_repository, upsert_workflow_item,
};
use std::sync::atomic::{AtomicUsize, Ordering};

#[tokio::test]
#[ignore = "requires disposable PostgreSQL and Docker busybox:latest; local Git fixture, no model or GitHub calls"]
async fn lost_checkout_recovers_exact_contract_and_missing_authority_blocks_execution() {
    let url = env::var("DONKEYSPACE_CANCELLATION_TEST_DATABASE_URL").unwrap();
    assert!(url.ends_with("/donkeyspace_cancellation_test"));
    let db = DbConfig::from_database_url(url);
    let pool = donkeyspace_db::connect(&db).await.unwrap();
    donkeyspace_db::apply_migrations(&pool).await.unwrap();
    let owner = format!("recovery-{}", Uuid::now_v7());
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
    let input = json!({"repository":{"owner":{"login":owner},"name":"fixture","default_branch":"main"},"issue":{"number":1,"title":"accepted contract"},"donkeyspace_lifecycle_coordinator":true});
    let job = create_job(&pool, Some(workflow), "architect", &input)
        .await
        .unwrap();
    acquire_job_lease(&pool, job.id, "recovery-test", 1200)
        .await
        .unwrap()
        .unwrap();
    let job = mark_job_running(&pool, job.id).await.unwrap().unwrap();
    let root = env::temp_dir().join(format!("checkout-recovery-{}", job.id));
    struct Cleanup(PathBuf);
    impl Drop for Cleanup {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }
    let _cleanup = Cleanup(root.clone());
    let workspace = root.join(job.id.to_string());
    let repo = workspace.join("repo");
    fs::create_dir_all(&repo).unwrap();
    trusted_git::run(&repo, &["init", "-b", "main"], None)
        .await
        .unwrap();
    crate::configure_git_author(&repo).await.unwrap();
    fs::write(repo.join(".gitignore"), "reports/\n").unwrap();
    trusted_git::run(&repo, &["add", "-A"], None).await.unwrap();
    trusted_git::run(&repo, &["commit", "-m", "base"], None)
        .await
        .unwrap();
    trusted_git::run(
        &repo,
        &["update-ref", "refs/remotes/origin/main", "HEAD"],
        None,
    )
    .await
    .unwrap();
    fs::create_dir_all(repo.join("src/docs")).unwrap();
    fs::create_dir_all(repo.join("reports")).unwrap();
    fs::create_dir_all(repo.join("src/rtl/sibling")).unwrap();
    fs::write(
        repo.join("src/docs/contract.md"),
        "FR-017: preserve the accepted requirement\n",
    )
    .unwrap();
    fs::write(
        repo.join("src/docs/work-items.json"),
        "{\"work_items\":[{\"id\":\"one\",\"spec\":\"src/docs/contract.md\"}]}\n",
    )
    .unwrap();
    fs::write(
        repo.join("src/rtl/sibling/done.v"),
        "// completed sibling\n",
    )
    .unwrap();
    let report = b"Tool report\n  slack = 0.031\n\tunits: ns\n";
    fs::write(repo.join("reports/reviewed.txt"), report).unwrap();
    fs::write(repo.join("reports/raw.bin"), [0, 1, 2]).unwrap();
    let context = PublicationContext {
        pool: &pool,
        coordinator_job_id: job.id,
        workflow_item_id: Some(workflow),
        issue_number: 1,
        owner: &owner,
        repo: "fixture",
        workspace_path: &workspace,
        token: None,
    };
    let retained: PluginArtifact =
        serde_json::from_value(json!({"path":"reports/reviewed.txt","type":"file"})).unwrap();
    // Capture the actual commit/publication without sending GitHub writes. The
    // copied object store below models a successful remote publication.
    assert!(
        publish_checkpoint(&context, &repo, "test: accepted contract", &[retained])
            .await
            .is_err()
    );
    let publications = list_agent_publications_for_run(&pool, job.id, None)
        .await
        .unwrap();
    assert_eq!(publications.len(), 1);
    let accepted_sha = publications[0].commit_sha.clone();
    let remote = root.join("published-objects");
    trusted_git::clone_local(&repo, &remote).await.unwrap();
    donkeyspace_db::mark_agent_publication_published(&pool, publications[0].id)
        .await
        .unwrap();
    let mut policy =
        donkeyspace_core::Policy::from_yaml(include_str!("../../../../.donkeyspace/policy.yml"))
            .unwrap();
    policy.lifecycle.plugin = Some(
        serde_json::from_value(json!({"manifest_path":"must-not-execute","flow":"test"})).unwrap(),
    );
    let tracking = LifecycleTracking {
        pool: &pool,
        policy: &policy,
        coordinator: &job,
        github: None,
        publication: Some(context),
    };
    let mut store = CheckpointStore::new(&workspace, Some(&tracking));
    let mut saved: LifecycleCheckpoint =
        serde_json::from_value(super::tests::legacy(CHECKPOINT_VERSION)).unwrap();
    saved.start_approved = true;
    let flow = super::tests::flow();
    store.save(&saved, &flow, true, false).await.unwrap();
    let record = donkeyspace_db::lifecycle_checkpoints::load(&pool, job.id)
        .await
        .unwrap()
        .unwrap();
    let snapshot: checkout_recovery::Snapshot =
        serde_json::from_value(record.state["repository_snapshot"].clone()).unwrap();
    assert_eq!(
        record.state["repository_snapshot"]["commit_sha"],
        accepted_sha
    );
    let expected_state = record.state.clone();
    drop(store);
    // The moving branch now contains a different requirement. Recovery must
    // still check out the older accepted object, not this new branch head.
    crate::configure_git_author(&remote).await.unwrap();
    fs::write(
        remote.join("src/docs/contract.md"),
        "FR-999: unapproved replacement\n",
    )
    .unwrap();
    trusted_git::run(&remote, &["add", "-A"], None)
        .await
        .unwrap();
    trusted_git::run(&remote, &["commit", "-m", "later unapproved change"], None)
        .await
        .unwrap();
    let mut resumed = input.clone();
    resumed["donkeyspace_resume"] = json!(true);
    resume_latest_paused_job(&pool, workflow, &resumed)
        .await
        .unwrap()
        .unwrap();
    acquire_job_lease(&pool, job.id, "recovery-test", 1200)
        .await
        .unwrap()
        .unwrap();
    let running = mark_job_running(&pool, job.id).await.unwrap().unwrap();
    fs::remove_dir_all(&workspace).unwrap();
    pool.close().await;
    let pool = donkeyspace_db::connect(&db).await.unwrap();
    let calls = AtomicUsize::new(0);
    checkout_recovery::ensure_with_remote(
        &pool,
        job.id,
        &repo,
        &owner,
        "fixture",
        "main",
        async |requested, destination| {
            assert_eq!(requested, &snapshot);
            calls.fetch_add(1, Ordering::SeqCst);
            trusted_git::clone_local(&remote, destination).await
        },
    )
    .await
    .unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        fs::read_to_string(repo.join("src/docs/contract.md")).unwrap(),
        "FR-017: preserve the accepted requirement\n"
    );
    assert_eq!(fs::read(repo.join("reports/reviewed.txt")).unwrap(), report);
    assert!(!repo.join("reports/raw.bin").exists());
    assert!(repo.join("src/rtl/sibling/done.v").is_file());
    let fresh = root.join("fresh-task");
    crate::repository_files::copy_root(&repo, &fresh, "src/docs").unwrap();
    assert_eq!(
        fs::read(fresh.join("src/docs/contract.md")).unwrap(),
        fs::read(repo.join("src/docs/contract.md")).unwrap()
    );
    assert_eq!(
        donkeyspace_db::lifecycle_checkpoints::load(&pool, job.id)
            .await
            .unwrap()
            .unwrap()
            .state,
        expected_state
    );
    // An intact checkout must not bypass provenance checks. Wrong-generation
    // state and a cancelled publication cannot authorize a continuation.
    sqlx::query(
        "UPDATE lifecycle_checkpoints SET generation=generation+1 WHERE coordinator_job_id=$1",
    )
    .bind(job.id)
    .execute(&pool)
    .await
    .unwrap();
    let error = checkout_recovery::ensure_with_remote(
        &pool,
        job.id,
        &repo,
        &owner,
        "fixture",
        "main",
        async |_, _| panic!("invalid provenance must not fetch"),
    )
    .await
    .unwrap_err();
    assert!(matches!(
        error.downcast_ref::<donkeyspace_db::DbError>(),
        Some(donkeyspace_db::DbError::CheckpointConflict)
    ));
    sqlx::query("UPDATE lifecycle_checkpoints SET generation=$2 WHERE coordinator_job_id=$1")
        .bind(job.id)
        .bind(record.generation)
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("UPDATE agent_publications SET status='cancelled' WHERE id=$1")
        .bind(publications[0].id)
        .execute(&pool)
        .await
        .unwrap();
    let error = checkout_recovery::ensure_with_remote(
        &pool,
        job.id,
        &repo,
        &owner,
        "fixture",
        "main",
        async |_, _| panic!("cancelled provenance must not fetch"),
    )
    .await
    .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("publication record is missing or inconsistent")
    );
    sqlx::query("UPDATE agent_publications SET status='published' WHERE id=$1")
        .bind(publications[0].id)
        .execute(&pool)
        .await
        .unwrap();
    // Run the actual filtered-task/container path against the recovered files.
    let result = json!({"outcome":"implemented","summary":"recovered contract verified","confidence":"high","risk":"low","questions":[],"tests":[{"name":"exact contract and retained evidence","command":["sh","-c","check recovered inputs"],"status":"passed","exit_code":0,"summary":"Verified FR-017, reviewed report and completed sibling in the filtered checkout."}],"changed_files":["verified"],"human_review_reason":null,"blocked_reason":null});
    let command = format!(
        "umask 000; test \"$(cat repo/src/docs/contract.md)\" = 'FR-017: preserve the accepted requirement' || exit 8; test -f repo/src/rtl/sibling/done.v || exit 9; grep -q 'slack = 0.031' repo/reports/reviewed.txt || exit 10; printf checked > repo/verified; printf '%s' '{}' > .donkeyspace/run-result.json",
        result
    );
    let manifest: PluginManifest = serde_json::from_value(json!({"api_version":1,"id":"test.recovery","runtime":{"default_image":"busybox:latest"},
        "roles":{"consumer":{"command":["sh","-c",command]}},
        "flows":{"test":{"start":"consumer","replaces_default_lifecycle":true,"work_items_path":"src/docs/work-items.json",
            "tasks":{"consumer":{"role":"consumer","read":["src/docs","src/rtl/sibling","reports/reviewed.txt"],"write":["verified"]}}}}})).unwrap();
    manifest.validate().unwrap();
    let selection: PluginFlowSelection =
        serde_json::from_value(json!({"manifest_path":"unused","flow":"test"})).unwrap();
    let execution = crate::plugin_container::with_execution_owner(
        &pool,
        job.id,
        "recovery-test",
        execute_task(
            &selection,
            &manifest,
            "consumer",
            &manifest.flows["test"].tasks["consumer"],
            None,
            1,
            &repo,
            &workspace,
            &input,
            &[],
            &BTreeMap::new(),
            &root,
        ),
    )
    .await
    .unwrap();
    assert_eq!(execution.result.outcome, Outcome::Implemented);
    assert_eq!(
        fs::read_to_string(repo.join("verified")).unwrap(),
        "checked"
    );
    // Missing files in an otherwise intact checkout are recovered from local
    // objects. Preserve the unverified directory instead of erasing evidence.
    fs::remove_file(repo.join("src/docs/contract.md")).unwrap();
    fs::write(repo.join("uncommitted.txt"), "keep for inspection").unwrap();
    checkout_recovery::ensure_with_remote(
        &pool,
        job.id,
        &repo,
        &owner,
        "fixture",
        "main",
        async |_, _| panic!("local recovery must not contact GitHub"),
    )
    .await
    .unwrap();
    let unverified = fs::read_dir(&workspace)
        .unwrap()
        .map(|e| e.unwrap().path())
        .find(|p| {
            p.file_name()
                .unwrap()
                .to_string_lossy()
                .starts_with("repo-unverified-")
        })
        .unwrap();
    assert_eq!(
        fs::read_to_string(unverified.join("uncommitted.txt")).unwrap(),
        "keep for inspection"
    );
    assert!(repo.join("src/docs/contract.md").is_file());
    // Missing remote objects are an actionable blocker, never a new contract.
    fs::remove_dir_all(&workspace).unwrap();
    let error = checkout_recovery::ensure_with_remote(
        &pool,
        job.id,
        &repo,
        &owner,
        "fixture",
        "main",
        async |_, _| Err("exact object unavailable".into()),
    )
    .await
    .unwrap_err();
    assert!(
        error
            .downcast_ref::<checkout_recovery::RecoveryBlocked>()
            .is_some()
    );
    assert!(error.to_string().contains(&accepted_sha));
    assert!(!repo.exists());
    assert_eq!(
        donkeyspace_db::lifecycle_checkpoints::load(&pool, job.id)
            .await
            .unwrap()
            .unwrap()
            .state,
        expected_state
    );
    // Exercise the production coordinator on a legacy checkpoint without
    // provenance. It pauses before reading a plugin manifest or launching an
    // agent, retains the original state, and exposes a recovery question.
    sqlx::query("UPDATE lifecycle_checkpoints SET state=state-'repository_snapshot' WHERE coordinator_job_id=$1").bind(job.id).execute(&pool).await.unwrap();
    let config = crate::repo_context::RepoContextConfig::new(&root, 10000, 1000, 10);
    crate::execute_developer_job(&pool, &policy, &config, None, running)
        .await
        .unwrap();
    let paused = donkeyspace_db::get_job(&pool, job.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(paused.status, "paused");
    assert_eq!(paused.result.as_ref().unwrap()["outcome"], "needs_human");
    assert!(
        paused.result.as_ref().unwrap()["human_review_reason"]
            .as_str()
            .unwrap()
            .contains("no immutable repository provenance")
    );
    assert!(
        !paused.result.as_ref().unwrap()["questions"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    assert!(!repo.exists());
    let after = donkeyspace_db::lifecycle_checkpoints::load(&pool, job.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(after.revision, record.revision);
    assert_eq!(
        after.state["completed_keys"],
        expected_state["completed_keys"]
    );
    assert_eq!(after.state["start_approved"], true);
}

#[tokio::test]
#[ignore = "requires disposable PostgreSQL and Docker busybox:latest; no model or GitHub calls"]
async fn approved_wave_commits_output_before_capturing_provenance_and_retains_failed_evidence() {
    let url = env::var("DONKEYSPACE_CANCELLATION_TEST_DATABASE_URL").unwrap();
    assert!(url.ends_with("/donkeyspace_cancellation_test"));
    let pool = donkeyspace_db::connect(&DbConfig::from_database_url(url))
        .await
        .unwrap();
    donkeyspace_db::apply_migrations(&pool).await.unwrap();
    let root = env::temp_dir().join(format!("published-wave-{}", Uuid::now_v7()));
    let config = crate::RepoContextConfig::new(&root, 20000, 4000, 12);
    let repository = upsert_repository(
        &pool,
        &RepositoryInput {
            installation_external_id: None,
            installation_account_login: None,
            provider: "github".into(),
            owner: format!("wave-{}", Uuid::now_v7()),
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
    let input = json!({"donkeyspace_lifecycle_coordinator":true,"donkeyspace_resume":true,
        "repository":{"owner":{"login":"fixture"},"name":"fixture","default_branch":"main"},
        "issue":{"number":1,"title":"approved wave"}});
    let job = create_job(&pool, Some(workflow), "architect", &input)
        .await
        .unwrap();
    acquire_job_lease(&pool, job.id, "published-wave-test", 600)
        .await
        .unwrap()
        .unwrap();
    let job = mark_job_running(&pool, job.id).await.unwrap().unwrap();
    let workspace = root.join(job.id.to_string());
    let repo = workspace.join("repo");
    fs::create_dir_all(repo.join("docs")).unwrap();
    fs::write(repo.join("docs/spec.md"), "Approved FIFO contract\n").unwrap();
    fs::write(
        repo.join("docs/items.json"),
        r#"{"work_items":[{"id":"fifo","spec":"docs/spec.md","depends_on":[]}]}"#,
    )
    .unwrap();
    trusted_git::run(&repo, &["init", "-b", "main"], None)
        .await
        .unwrap();
    crate::configure_git_author(&repo).await.unwrap();
    trusted_git::run(&repo, &["add", "-A"], None).await.unwrap();
    trusted_git::run(&repo, &["commit", "-m", "accepted specification"], None)
        .await
        .unwrap();
    trusted_git::run(
        &repo,
        &["update-ref", "refs/remotes/origin/main", "HEAD"],
        None,
    )
    .await
    .unwrap();
    let base = trusted_git::run(&repo, &["rev-parse", "HEAD"], None)
        .await
        .unwrap();
    let result = json!({"outcome":"implemented","summary":"shared infrastructure fixed",
        "confidence":"high","risk":"low","questions":[],"tests":[{"name":"fixture","command":["true"],"status":"passed","exit_code":0}],
        "changed_files":["shared/fixed"],"resources_used":[]});
    let command = format!(
        "set -eu; umask 000; mkdir -p repo/shared; echo repaired > repo/shared/fixed; printf '%s' '{result}' > .donkeyspace/run-result.json"
    );
    let manifest: PluginManifest = serde_json::from_value(json!({"api_version":1,"id":"test.published-wave",
        "runtime":{"default_image":"busybox:latest"},
        "roles":{"architect":{"command":["false"]},"infrastructure":{"command":["sh","-c",command]}},
        "flows":{"test":{"start":"architect","replaces_default_lifecycle":true,"work_items_path":"docs/items.json",
            "tasks":{"architect":{"role":"architect","approval":"required","write":["docs"]},
                "infrastructure":{"role":"infrastructure","dependencies":["architect"],"read":["docs"],"write":["shared"]}}}}})).unwrap();
    manifest.validate().unwrap();
    let selection: PluginFlowSelection =
        serde_json::from_value(json!({"manifest_path":"unused","flow":"test"})).unwrap();
    let flow = &manifest.flows["test"];
    let policy =
        donkeyspace_core::Policy::from_yaml(include_str!("../../../../.donkeyspace/policy.yml"))
            .unwrap();
    let publication = PublicationContext {
        pool: &pool,
        coordinator_job_id: job.id,
        workflow_item_id: None,
        issue_number: 1,
        owner: "fixture",
        repo: "fixture",
        workspace_path: &workspace,
        token: None,
    };
    let tracking = LifecycleTracking {
        pool: &pool,
        policy: &policy,
        coordinator: &job,
        github: None,
        publication: Some(publication),
    };
    let mut saved: LifecycleCheckpoint =
        serde_json::from_value(super::tests::legacy(CHECKPOINT_VERSION)).unwrap();
    saved.start_approved = true;
    saved.completed_keys = vec![TaskKey {
        task: "architect".into(),
        work_item: None,
    }];
    saved.projected_issues.clear();
    saved.resume_target = saved.completed_keys[0].clone();
    saved.active_work_items = vec!["fifo".into()];
    let mut store = CheckpointStore::new(&workspace, Some(&tracking));
    store.save(&saved, flow, false, false).await.unwrap();
    drop(store);
    // Exercise the real resumed wave, including task output copying, Git and
    // durable publication records. Lack of credentials deliberately interrupts
    // only the network push, after the output has been committed.
    let error = crate::plugin_container::with_execution_owner(
        &pool,
        job.id,
        "published-wave-test",
        run_work_item_lifecycle(
            &selection,
            &manifest,
            flow,
            &repo,
            &workspace,
            &input,
            Some(tracking),
            &BTreeMap::new(),
            &workspace,
        ),
    )
    .await
    .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("configured GitHub authentication is required"),
        "{error}"
    );
    let publications = list_agent_publications_for_run(&pool, job.id, None)
        .await
        .unwrap();
    assert_eq!(publications.len(), 1);
    let record = &publications[0];
    assert_eq!(record.changed_files, json!(["shared/fixed"]));
    assert_ne!(record.commit_sha, base.trim());
    assert_eq!(
        trusted_git::run(&repo, &["status", "--porcelain"], None)
            .await
            .unwrap(),
        ""
    );
    assert_eq!(
        trusted_git::run(
            &repo,
            &["show", &format!("{}:shared/fixed", record.commit_sha)],
            None
        )
        .await
        .unwrap(),
        "repaired\n"
    );
    // A failed push must not mark the task completed in the checkpoint.
    let checkpoint = donkeyspace_db::lifecycle_checkpoints::load(&pool, job.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        checkpoint.state["repository_snapshot"]["commit_sha"],
        base.trim()
    );
    assert_eq!(
        checkpoint.state["completed_keys"],
        json!([{"task":"architect","work_item":null}])
    );
    // Even after all registered publications drain, unpublished attempt evidence
    // from a coordinator failure must survive the generic cleanup path.
    donkeyspace_db::mark_agent_publication_published(&pool, record.id)
        .await
        .unwrap();
    donkeyspace_db::fail_job(&pool, job.id, &json!({"outcome":"failed"}))
        .await
        .unwrap();
    donkeyspace_db::fail_active_plugin_child_jobs(&pool, job.id, &json!({"outcome":"failed"}))
        .await
        .unwrap();
    let diagnostic = workspace.join("unpublished-diagnostic.txt");
    fs::write(&diagnostic, "failure before attempt registration").unwrap();
    crate::cleanup_published_workspace(&pool, job.id, &config).await;
    assert!(diagnostic.exists());
    // Closed issues and stale generations cannot be revived by retry.
    sqlx::query("UPDATE workflow_items SET provider_state='closed' WHERE id=$1")
        .bind(workflow)
        .execute(&pool)
        .await
        .unwrap();
    assert!(
        donkeyspace_db::lifecycle_checkpoints::retry_failed(&pool, job.id)
            .await
            .unwrap()
            .is_none()
    );
    sqlx::query(
        "UPDATE workflow_items SET provider_state='open',generation=generation+1 WHERE id=$1",
    )
    .bind(workflow)
    .execute(&pool)
    .await
    .unwrap();
    assert!(
        donkeyspace_db::lifecycle_checkpoints::retry_failed(&pool, job.id)
            .await
            .unwrap()
            .is_none()
    );
    sqlx::query("UPDATE workflow_items SET generation=generation-1 WHERE id=$1")
        .bind(workflow)
        .execute(&pool)
        .await
        .unwrap();
    let (first, duplicate) = tokio::join!(
        donkeyspace_db::lifecycle_checkpoints::retry_failed(&pool, job.id),
        donkeyspace_db::lifecycle_checkpoints::retry_failed(&pool, job.id)
    );
    let retries: Vec<_> = [first.unwrap(), duplicate.unwrap()]
        .into_iter()
        .flatten()
        .collect();
    assert_eq!(retries.len(), 1);
    assert_eq!(retries[0].id, job.id);
    assert_eq!(retries[0].input["donkeyspace_resume"], true);
    assert_eq!(
        donkeyspace_db::lifecycle_checkpoints::load(&pool, job.id)
            .await
            .unwrap()
            .unwrap()
            .state,
        checkpoint.state
    );
    // Successful runs remain eligible for ordinary cleanup.
    let completed = create_job(&pool, None, "architect", &input).await.unwrap();
    acquire_job_lease(&pool, completed.id, "published-wave-test", 60)
        .await
        .unwrap()
        .unwrap();
    mark_job_running(&pool, completed.id)
        .await
        .unwrap()
        .unwrap();
    donkeyspace_db::complete_job(&pool, completed.id, &json!({"outcome":"implemented"}))
        .await
        .unwrap();
    let completed_workspace = root.join(completed.id.to_string());
    fs::create_dir_all(&completed_workspace).unwrap();
    crate::cleanup_published_workspace(&pool, completed.id, &config).await;
    assert!(!completed_workspace.exists());
    fs::remove_dir_all(&root).unwrap();
}
