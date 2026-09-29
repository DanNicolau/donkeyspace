use super::*;

#[tokio::test]
#[ignore = "requires isolated PostgreSQL and Docker busybox:latest; no model or GitHub access"]
async fn successful_process_retains_only_declared_evidence_for_continuation() {
    let url = env::var("DONKEYSPACE_CANCELLATION_TEST_DATABASE_URL").unwrap();
    assert!(url.ends_with("/donkeyspace_cancellation_test"));
    let pool = donkeyspace_db::connect(&donkeyspace_db::DbConfig::from_database_url(url))
        .await
        .unwrap();
    donkeyspace_db::apply_migrations(&pool).await.unwrap();
    let job = donkeyspace_db::create_job(&pool, None, "fixture", &json!({}))
        .await
        .unwrap();
    donkeyspace_db::acquire_job_lease(&pool, job.id, "retention-test", 120)
        .await
        .unwrap()
        .unwrap();
    donkeyspace_db::mark_job_running(&pool, job.id)
        .await
        .unwrap()
        .unwrap();
    let workspace = env::temp_dir().join(format!("retention-{}", job.id));
    let repo = workspace.join("repo");
    fs::create_dir_all(repo.join("outputs/reports")).unwrap();
    let git = |args: &[&str]| {
        let output = std::process::Command::new("git")
            .current_dir(&repo)
            .args([
                "-c",
                "core.hooksPath=/dev/null",
                "-c",
                "user.name=Fixture",
                "-c",
                "user.email=fixture@example.invalid",
                "-c",
                "commit.gpgsign=false",
            ])
            .args(args)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8(output.stdout).unwrap()
    };
    git(&["init", "--quiet"]);
    git(&["commit", "--allow-empty", "-m", "fixture"]);
    let selection: PluginFlowSelection =
        serde_json::from_value(json!({"manifest_path":"unused", "flow":"test"})).unwrap();
    for (index, outcome) in [
        "needs_human",
        "needs_changes",
        "needs_info",
        "failed",
        "process_error",
        "invalid_result",
    ]
    .iter()
    .enumerate()
    {
        let result = json!({"outcome":if *outcome == "process_error" { "needs_human" } else { outcome },
            "summary":"inspect retained evidence", "confidence":"high", "risk":"low",
            "questions":["Accept this proposal?"], "human_review_reason":"review required", "blocked_reason":"fixture",
            "tests":[], "changed_files":["outputs/reports/result.txt","outputs/raw.bin"]});
        let command = format!(
            "umask 000; mkdir -p repo/outputs/reports; printf retained > repo/outputs/reports/result.txt; printf raw > repo/outputs/raw.bin; printf '%s' '{}' > .donkeyspace/run-result.json; exit {}",
            result,
            if *outcome == "process_error" { 7 } else { 0 }
        );
        let manifest: PluginManifest = serde_json::from_value(json!({"api_version":1,"id":"test.retention","runtime":{"default_image":"busybox:latest"},
            "roles":{"generate":{"command":["sh","-c",command]}},
            "flows":{"test":{"start":"generate","replaces_default_lifecycle":true,"work_items_path":"items.json",
                "tasks":{"generate":{"role":"generate","write":["outputs"],"preserve_on_success":[{"path":"outputs/reports","type":"directory"}]}}}}})).unwrap();
        manifest.validate().unwrap();
        fs::write(repo.join("outputs/reports/result.txt"), "original").unwrap();
        let attempt = index as u32 + 1;
        let execution = crate::plugin_container::with_execution_owner(
            &pool,
            job.id,
            "retention-test",
            execute_task(
                &selection,
                &manifest,
                "generate",
                &manifest.flows["test"].tasks["generate"],
                None,
                attempt,
                &repo,
                &workspace,
                &json!({}),
                &[],
                &BTreeMap::new(),
                &workspace,
            ),
        )
        .await;
        let succeeded = index < 4;
        assert_eq!(execution.is_ok(), succeeded, "{outcome}: {execution:?}");
        assert_eq!(
            fs::read_to_string(repo.join("outputs/reports/result.txt")).unwrap(),
            if succeeded { "retained" } else { "original" }
        );
        assert!(!repo.join("outputs/raw.bin").exists());
        if index == 0 {
            // No credential provider is initialized in this isolated test.
            // Checkpoint creation is durable even though publication must fail.
            let context = PublicationContext {
                pool: &pool,
                coordinator_job_id: job.id,
                workflow_item_id: None,
                issue_number: 1,
                owner: "fixture",
                repo: "fixture",
                workspace_path: &workspace,
                token: None,
            };
            assert!(
                publish_checkpoint(&context, &repo, "test: retain proposal", &[])
                    .await
                    .is_err()
            );
            let records = list_agent_publications_for_run(&pool, job.id, Some(job.id))
                .await
                .unwrap();
            assert_eq!(records.len(), 1);
            assert_eq!(records[0].kind, "checkpoint");
            assert_ne!(records[0].status, "published");
            assert_eq!(
                git(&[
                    "show",
                    &format!("{}:outputs/reports/result.txt", records[0].commit_sha)
                ]),
                "retained"
            );
            assert!(
                !git(&["ls-tree", "-r", "--name-only", &records[0].commit_sha]).contains("raw.bin")
            );
            // The retained report is already in the checkpoint. The attempt
            // diff adds raw output and diagnostics, but must still inventory the
            // unchanged report so a paused workflow can link directly to it.
            let task_root = task_attempt_root(&workspace, "generate", None, attempt);
            assert!(
                publish_attempt(
                    &context,
                    &repo,
                    &AttemptPublication {
                        job_id: Some(job.id),
                        task: "generate",
                        publication_tag: None,
                        work_item: None,
                        attempt,
                        outcome: Some(Outcome::NeedsHuman),
                        task_root: &task_root,
                        write_roots: &["outputs".into()],
                        diagnostics: &[],
                        reason: "review retained evidence",
                        related_issue_number: None,
                        redactions: &[],
                    },
                )
                .await
                .is_err()
            );
            let records = list_agent_publications_for_run(&pool, job.id, Some(job.id))
                .await
                .unwrap();
            let attempt_record = records.iter().find(|record| record.task.is_some()).unwrap();
            assert_ne!(attempt_record.status, "published");
            assert_eq!(
                attempt_record.metadata["supporting_files"],
                json!(["outputs/raw.bin", "outputs/reports/result.txt"])
            );
            assert!(
                !attempt_record
                    .changed_files
                    .as_array()
                    .unwrap()
                    .contains(&json!("outputs/reports/result.txt"))
            );
            assert_eq!(
                crate::trusted_git::run(
                    Path::new(&attempt_record.local_repo_path),
                    &[
                        "show",
                        &format!("{}:outputs/reports/result.txt", attempt_record.commit_sha)
                    ],
                    None,
                )
                .await
                .unwrap(),
                "retained"
            );
        }
        // Destroy disposable attempts before preparing a new filtered checkout.
        fs::remove_dir_all(workspace.join("plugin-tasks")).unwrap();
        let fresh = workspace.join(format!("fresh-{attempt}"));
        copy_root(&repo, &fresh, "outputs").unwrap();
        assert_eq!(
            fs::read(fresh.join("outputs/reports/result.txt")).unwrap(),
            fs::read(repo.join("outputs/reports/result.txt")).unwrap()
        );
        assert!(!fresh.join("outputs/raw.bin").exists());
    }
    fs::remove_dir_all(workspace).unwrap();
}

#[test]
fn retention_respects_narrowed_access_and_does_not_erase_optional_artifacts() {
    let root = env::temp_dir().join(format!("retention-scope-{}", Uuid::now_v7()));
    let source = root.join("source");
    let target = root.join("target");
    fs::create_dir_all(&source).unwrap();
    fs::create_dir_all(target.join("outputs/reports")).unwrap();
    fs::write(target.join("outputs/reports/result.txt"), "previous").unwrap();
    let artifact: PluginArtifact =
        serde_json::from_value(json!({"path":"outputs/reports","type":"directory"})).unwrap();
    retain_task_output(
        &source,
        &target,
        &["outputs".into()],
        std::slice::from_ref(&artifact),
        Outcome::NeedsHuman,
    )
    .unwrap();
    assert_eq!(
        fs::read_to_string(target.join("outputs/reports/result.txt")).unwrap(),
        "previous"
    );
    assert!(
        retain_task_output(
            &source,
            &target,
            &["outputs/other".into()],
            &[artifact],
            Outcome::NeedsHuman
        )
        .is_err()
    );
    fs::remove_dir_all(root).unwrap();
}

#[cfg(unix)]
#[test]
fn retention_rejects_symlinks_before_replacing_prior_evidence() {
    use std::os::unix::fs::symlink;
    let root = env::temp_dir().join(format!("retention-links-{}", Uuid::now_v7()));
    let source = root.join("source");
    let target = root.join("target");
    fs::create_dir_all(source.join("outputs/reports")).unwrap();
    fs::create_dir_all(target.join("outputs/reports")).unwrap();
    fs::write(target.join("outputs/reports/result.txt"), "previous").unwrap();
    fs::write(root.join("outside"), "private fixture").unwrap();
    let artifact: PluginArtifact =
        serde_json::from_value(json!({"path":"outputs/reports","type":"directory"})).unwrap();
    symlink(root.join("outside"), source.join("outputs/reports/leak")).unwrap();
    assert!(
        retain_task_output(
            &source,
            &target,
            &["outputs".into()],
            std::slice::from_ref(&artifact),
            Outcome::NeedsHuman
        )
        .is_err()
    );
    assert_eq!(
        fs::read_to_string(target.join("outputs/reports/result.txt")).unwrap(),
        "previous"
    );
    fs::remove_file(source.join("outputs/reports/leak")).unwrap();
    fs::remove_dir_all(source.join("outputs")).unwrap();
    symlink(target.join("outputs"), source.join("outputs")).unwrap();
    assert!(
        retain_task_output(
            &source,
            &target,
            &["outputs".into()],
            &[artifact],
            Outcome::NeedsHuman
        )
        .is_err()
    );
    fs::remove_dir_all(root).unwrap();
}
