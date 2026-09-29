//! Exercise the production built-in and required-check paths, including the
//! named-volume layout used by Compose. Fixtures contain no real credentials.
use super::*;
use tokio::process::Command;
use uuid::Uuid;

#[tokio::test]
#[ignore = "requires disposable PostgreSQL and Docker busybox:latest"]
async fn isolated_execution_denies_other_jobs_and_worker_secrets() {
    let root = env::temp_dir().join(format!("ds-isolation-{}", Uuid::now_v7()));
    std::fs::create_dir_all(&root).unwrap();
    let codex_home = root.join("synthetic-codex-home");
    std::fs::create_dir(&codex_home).unwrap();
    std::fs::write(codex_home.join("auth.json"), "synthetic-model-credential").unwrap();
    std::fs::write(
        codex_home.join("private-history"),
        "another-job-conversation",
    )
    .unwrap();
    let volume = format!("ds-isolation-{}", Uuid::now_v7());
    struct Cleanup(PathBuf, String);
    impl Drop for Cleanup {
        fn drop(&mut self) {
            let _ = std::process::Command::new("docker")
                .args(["volume", "rm", &self.1])
                .output();
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    let _cleanup = Cleanup(root.clone(), volume.clone());
    let created = Command::new("docker")
        .args([
            "volume",
            "create",
            "--driver",
            "local",
            "--opt",
            "type=none",
            "--opt",
            "o=bind",
            "--opt",
        ])
        .arg(format!("device={}", root.display()))
        .arg(&volume)
        .output()
        .await
        .unwrap();
    assert!(
        created.status.success(),
        "{}",
        String::from_utf8_lossy(&created.stderr)
    );
    for (fixture, method, named_volume) in [
        ("isolated_execution_fixture", "api-key", false),
        ("isolated_execution_fixture", "api-key", true),
        ("credential_lock_fixture", "api-key", false),
        ("credential_lock_fixture", "chatgpt", false),
    ] {
        // Separate process: environment mutation must never race other tests.
        let mut child = Command::new(env::current_exe().unwrap());
        child
            .args([
                "--exact",
                &format!("execution_isolation_tests::{fixture}"),
                "--ignored",
                "--nocapture",
            ])
            .env("DONKEYSPACE_ISOLATION_TEST_ROOT", &root)
            .env("DONKEYSPACE_AGENT_IMAGE", "busybox:latest")
            .env("DONKEYSPACE_WORKSPACE_ROOT", &root)
            .env("DONKEYSPACE_GITHUB_TOKEN", "synthetic-worker-token")
            .env("DONKEYSPACE_DATABASE_URL", "synthetic-worker-database")
            .env("DONKEYSPACE_WEBHOOK_SECRET", "synthetic-webhook-secret");
        for name in [
            "DONKEYSPACE_WORKSPACE_VOLUME",
            "DONKEYSPACE_CODEX_HOME_SOURCE",
            "DONKEYSPACE_CODEX_AUTH_SOURCE",
            "DONKEYSPACE_CODEX_AUTH_METHOD",
            "DONKEYSPACE_CODEX_VOLUME",
            "DONKEYSPACE_OSS_TOOLS_PATH",
            "DONKEYSPACE_TECH_PATH",
        ] {
            child.env_remove(name);
        }
        child
            .env(
                "DONKEYSPACE_CODEX_AUTH_SOURCE",
                codex_home.join("auth.json"),
            )
            .env("DONKEYSPACE_CODEX_AUTH_METHOD", method)
            .env("DONKEYSPACE_CODEX_AUTH_MOUNT_SUFFIX", "")
            .env("SYNTHETIC_PLUGIN_TOKEN", "synthetic-plugin-credential");
        if named_volume {
            child.env("DONKEYSPACE_WORKSPACE_VOLUME", &volume);
        }
        let result = child.output().await.unwrap();
        assert!(
            result.status.success(),
            "{fixture}/{method}, named volume {named_volume}: {}\n{}",
            String::from_utf8_lossy(&result.stdout),
            String::from_utf8_lossy(&result.stderr)
        );
    }
}

#[tokio::test]
#[ignore = "private subprocess fixture; run isolated_execution_denies_other_jobs_and_worker_secrets"]
async fn isolated_execution_fixture() {
    let root = PathBuf::from(env::var("DONKEYSPACE_ISOLATION_TEST_ROOT").unwrap());
    assert!(root.starts_with(env::temp_dir()));
    assert!(
        root.file_name()
            .unwrap()
            .to_str()
            .unwrap()
            .starts_with("ds-isolation-")
    );
    let url = env::var("DONKEYSPACE_CANCELLATION_TEST_DATABASE_URL").unwrap();
    assert!(url.ends_with("/donkeyspace_cancellation_test"));
    let pool = connect(&DbConfig::from_database_url(url)).await.unwrap();
    apply_migrations(&pool).await.unwrap();
    let mut policy = Policy::from_yaml(include_str!("../../../.donkeyspace/policy.yml")).unwrap();
    let config = RepoContextConfig::new(&root, 20000, 4000, 12);
    std::fs::create_dir_all(root.join("other-job")).unwrap();
    std::fs::write(root.join("other-job/private"), "synthetic-other-job-secret").unwrap();
    let checks = format!(
        "set -eu; test -z \"${{DONKEYSPACE_GITHUB_TOKEN-}}\"; test -z \"${{DONKEYSPACE_DATABASE_URL-}}\"; test -z \"${{DONKEYSPACE_WEBHOOK_SECRET-}}\"; test ! -e /var/run/docker.sock; test ! -e /run/secrets; test ! -e /workspaces; test ! -e '{}'; test ! -e /workspace/../other-job/private;",
        root.join("other-job/private").display()
    );
    let result = json!({"outcome":"blocked","summary":"fixture finished","confidence":"high","risk":"low","questions":[],"tests":[],"changed_files":[],"blocked_reason":"fixture"});
    let script = format!(
        "{checks} test \"$(cat /root/.codex/auth.json)\" = synthetic-model-credential; test ! -e /root/.codex/private-history; if printf forbidden > /root/.codex/auth.json 2>/dev/null; then exit 76; fi; test -d repo/.git; if touch repo/.git/injected 2>/dev/null; then exit 77; fi; if mv repo moved-repo 2>/dev/null; then exit 78; fi; if mv .donkeyspace moved-protocol 2>/dev/null; then exit 79; fi; printf edited > repo/edited; printf '%s' '{}' > .donkeyspace/run-result.json",
        result
    );
    for role in [
        &mut policy.agents.triage,
        &mut policy.agents.developer,
        &mut policy.agents.reviewer,
        &mut policy.agents.repair,
    ] {
        role.enabled = true;
        role.command = vec!["sh".into(), "-c".into(), script.clone()];
    }
    for role in ["triage", "developer", "reviewer", "repair"] {
        let job = create_job(&pool, None, role, &json!({})).await.unwrap();
        donkeyspace_db::acquire_job_lease(&pool, job.id, "isolation-test", 120)
            .await
            .unwrap()
            .unwrap();
        let job = donkeyspace_db::mark_job_running(&pool, job.id)
            .await
            .unwrap()
            .unwrap();
        let workspace = workspace_path(job.id, &config);
        let repo = workspace.join("repo");
        std::fs::create_dir_all(repo.join(".git")).unwrap();
        std::fs::write(repo.join(".git/config"), "synthetic git metadata").unwrap();
        let output = plugin_container::with_execution_owner(
            &pool,
            job.id,
            "isolation-test",
            run_configured_agent(&pool, &policy, &job, &json!({}), &config),
        )
        .await
        .unwrap();
        assert_eq!(output.outcome, Outcome::Blocked);
        assert_eq!(
            std::fs::read_to_string(repo.join("edited")).unwrap(),
            "edited"
        );
        assert!(!repo.join(".git/injected").exists());
        if role == "triage" {
            let private = root.join("other-job/private-result");
            let private_bytes = result.to_string();
            std::fs::write(&private, &private_bytes).unwrap();
            for target in ["run-result.json", "agent.stdout.log"] {
                policy.agents.triage.command = vec![
                    "sh".into(),
                    "-c".into(),
                    format!(
                        "rm -f .donkeyspace/{target}; ln -s '{}' .donkeyspace/{target}",
                        private.display()
                    ),
                ];
                let blocked = plugin_container::with_execution_owner(
                    &pool,
                    job.id,
                    "isolation-test",
                    run_configured_agent(&pool, &policy, &job, &json!({}), &config),
                )
                .await;
                assert!(blocked.is_err(), "followed protocol symlink {target}");
                assert_eq!(std::fs::read_to_string(&private).unwrap(), private_bytes);
                std::fs::remove_file(workspace.join(".donkeyspace").join(target)).unwrap();
            }
            let host_marker = root.join("must-not-execute");
            let failed = plugin_container::with_execution_owner(
                &pool,
                job.id,
                "isolation-test",
                plugin_container::run_container(
                    "ds-isolation/nonexistent:fixture",
                    &[
                        "sh".into(),
                        "-c".into(),
                        format!("touch '{}'", host_marker.display()),
                    ],
                    &workspace,
                    &Default::default(),
                    &[],
                    plugin_container::ExecutionKind::Agent,
                ),
            )
            .await;
            assert!(failed.map_or(true, |output| !output.status.success()));
            assert!(
                !host_marker.exists(),
                "launch failure fell back to host execution"
            );
            for name in ["CODEX_HOME", "CODEX_ACCESS_TOKEN", "OPENAI_API_KEY"] {
                let rejected = plugin_container::with_execution_owner(
                    &pool,
                    job.id,
                    "isolation-test",
                    plugin_container::run_container(
                        "busybox:latest",
                        &["sh".into(), "-c".into(), "touch auth-override".into()],
                        &workspace,
                        &Default::default(),
                        &[name.into()],
                        plugin_container::ExecutionKind::Agent,
                    ),
                )
                .await;
                assert!(
                    rejected
                        .unwrap_err()
                        .to_string()
                        .contains("coordinator-owned")
                );
                assert!(!workspace.join("auth-override").exists());
            }
            // Exercise the full plugin task path: the agent receives its
            // declared token/home, then its validator receives neither.
            let plugin_result = json!({"outcome":"implemented","summary":"proposal","confidence":"high","risk":"low","questions":[],"tests":[{"name":"fixture","command":["test"],"status":"passed","exit_code":0}],"changed_files":["spec.md","items.json"],"work_items":["one"]});
            let command = format!(
                "set -eu; test \"$(cat /root/.codex/auth.json)\" = synthetic-model-credential; test \"$PLUGIN_TOKEN\" = synthetic-plugin-credential; printf proposal > repo/spec.md; printf '%s' '{{\"work_items\":[{{\"id\":\"one\",\"title\":\"One\",\"spec\":\"spec.md\"}}]}}' > repo/items.json; printf '%s' '{}' > .donkeyspace/run-result.json",
                plugin_result
            );
            let validator = format!(
                "{checks} test ! -e /root/.codex; test -z \"${{PLUGIN_TOKEN-}}\"; test -z \"${{SYNTHETIC_PLUGIN_TOKEN-}}\"; test -f repo/spec.md; printf 'validator has no agent credentials'"
            );
            let manifest = json!({"api_version":1,"id":"test.credentials","runtime":{"default_image":"busybox:latest"},
                "roles":{"plan":{"command":["sh","-c",command],"environment":["PLUGIN_TOKEN"]}},
                "flows":{"test":{"start":"plan","replaces_default_lifecycle":true,"work_items_path":"items.json",
                    "tasks":{"plan":{"role":"plan","write":["spec.md","items.json"],"approval":"required",
                        "validators":[{"name":"credential isolation","command":["sh","-c",validator]}]}}}}});
            let manifest_path = root.join("credential-plugin.json");
            std::fs::write(&manifest_path, manifest.to_string()).unwrap();
            let selection = serde_json::from_value(json!({"manifest_path":manifest_path,"flow":"test","environment":{"PLUGIN_TOKEN":"SYNTHETIC_PLUGIN_TOKEN"}})).unwrap();
            let result = plugin_container::with_execution_owner(
                &pool,
                job.id,
                "isolation-test",
                plugin_flow::run(
                    &selection,
                    &repo,
                    &workspace.join("plugin"),
                    &json!({}),
                    None,
                ),
            )
            .await
            .unwrap();
            assert_eq!(result.outcome, Outcome::NeedsHuman, "{result:?}");
            assert!(
                result
                    .tests
                    .iter()
                    .any(|test| test.name == "credential isolation"
                        && test.status == TestStatus::Passed
                        && test.summary.as_deref() == Some("validator has no agent credentials"))
            );
        }
        let required = RequiredCommand {
            name: "isolated check".into(),
            command: vec![
                "sh".into(),
                "-c".into(),
                format!(
                    "{checks} test ! -e /root/.codex; test -z \"${{SYNTHETIC_PLUGIN_TOKEN-}}\"; test -f edited; if touch .git/injected 2>/dev/null; then exit 77; fi; printf checked > checked"
                ),
            ],
        };
        let output = plugin_container::with_execution_owner(
            &pool,
            job.id,
            "isolation-test",
            run_required_command(&pool, job.id, &repo, &required),
        )
        .await
        .unwrap();
        assert_eq!(output.status, TestStatus::Passed, "{output:?}");
        assert_eq!(
            std::fs::read_to_string(repo.join("checked")).unwrap(),
            "checked"
        );
        assert!(!repo.join(".git/injected").exists());
    }
}

#[tokio::test]
#[ignore = "private subprocess fixture; run isolated_execution_denies_other_jobs_and_worker_secrets"]
async fn credential_lock_fixture() {
    use std::time::Duration;
    let root = PathBuf::from(env::var("DONKEYSPACE_ISOLATION_TEST_ROOT").unwrap());
    assert!(root.starts_with(env::temp_dir()));
    let subscription = env::var("DONKEYSPACE_CODEX_AUTH_METHOD").unwrap() == "chatgpt";
    let auth = PathBuf::from(env::var("DONKEYSPACE_CODEX_AUTH_SOURCE").unwrap());
    assert!(auth.starts_with(&root));
    std::fs::write(&auth, "initial-credential").unwrap();
    let url = env::var("DONKEYSPACE_CANCELLATION_TEST_DATABASE_URL").unwrap();
    assert!(url.ends_with("/donkeyspace_cancellation_test"));
    let pool = connect(&DbConfig::from_database_url(url)).await.unwrap();
    apply_migrations(&pool).await.unwrap();
    let job = create_job(&pool, None, "auth-fixture", &json!({}))
        .await
        .unwrap();
    donkeyspace_db::acquire_job_lease(&pool, job.id, "auth-test", 120)
        .await
        .unwrap()
        .unwrap();
    donkeyspace_db::mark_job_running(&pool, job.id)
        .await
        .unwrap()
        .unwrap();
    let first = root.join(format!("auth-{}-first", job.id));
    let second = root.join(format!("auth-{}-second", job.id));
    for directory in [&first, &second] {
        std::fs::create_dir_all(directory).unwrap();
    }
    let launch = |directory: PathBuf, script: String| {
        let pool = pool.clone();
        tokio::spawn(async move {
            plugin_container::with_execution_owner(
                &pool,
                job.id,
                "auth-test",
                plugin_container::run_builtin(
                    &["sh".into(), "-c".into(), script],
                    &directory,
                    plugin_container::ExecutionKind::Agent,
                ),
            )
            .await
            .map_err(|error| error.to_string())
        })
    };
    let first_run = launch(
        first.clone(),
        format!(
            "set -eu; test ! -e /root/.codex/private-history; printf private > /root/.codex/history.jsonl; {}; touch started; while test ! -e release; do sleep 0.05; done; {}; touch finished",
            if subscription {
                ": > /root/.codex/auth.json"
            } else {
                ":"
            },
            if subscription {
                "printf rotated-credential > /root/.codex/auth.json"
            } else {
                "if printf forbidden > /root/.codex/auth.json 2>/dev/null; then exit 91; fi"
            }
        ),
    );
    wait_for_auth_marker(&first.join("started")).await;
    let second_run = launch(second.clone(), "set -eu; test ! -e /root/.codex/history.jsonl; cat /root/.codex/auth.json > observed; touch started".into());
    if subscription {
        // Confirm both actual containers are running, not merely registered.
        wait_for_auth_containers(job.id, 2).await;
        assert!(!second.join("started").exists());
    } else {
        // Shared API-key locks permit overlap while the first is still active.
        wait_for_auth_marker(&second.join("started")).await;
        assert!(!first.join("finished").exists());
    }
    std::fs::write(first.join("release"), "release").unwrap();
    for run in [first_run, second_run] {
        let result = tokio::time::timeout(Duration::from_secs(15), run)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(result.exit_code, Some(0), "{result:?}");
    }
    assert_eq!(
        std::fs::read_to_string(second.join("observed")).unwrap(),
        if subscription {
            "rotated-credential"
        } else {
            "initial-credential"
        }
    );
    assert_eq!(
        std::fs::read_to_string(&auth).unwrap(),
        if subscription {
            "rotated-credential"
        } else {
            "initial-credential"
        }
    );
    assert!(!auth.parent().unwrap().join("history.jsonl").exists());
    wait_for_auth_containers(job.id, 0).await;

    if subscription {
        let held = root.join(format!("auth-{}-held", job.id));
        let waiting = root.join(format!("auth-{}-waiting", job.id));
        std::fs::create_dir(&held).unwrap();
        std::fs::create_dir(&waiting).unwrap();
        let holder = launch(held.clone(), "set -eu; printf refreshed-before-cancellation > /root/.codex/auth.json; touch started; sleep 60".into());
        wait_for_auth_marker(&held.join("started")).await;
        let waiter = launch(
            waiting.clone(),
            "set -eu; cat /root/.codex/auth.json > observed; touch started".into(),
        );
        wait_for_auth_containers(job.id, 2).await;
        assert!(!waiting.join("started").exists());
        // Dropping the real launcher triggers its existing owned-container
        // cleanup. Credential ownership must not outlive the removed container.
        holder.abort();
        assert!(holder.await.unwrap_err().is_cancelled());
        let result = tokio::time::timeout(Duration::from_secs(15), waiter)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(result.exit_code, Some(0), "{result:?}");
        assert_eq!(
            std::fs::read_to_string(waiting.join("observed")).unwrap(),
            "refreshed-before-cancellation"
        );
        wait_for_auth_containers(job.id, 0).await;
    }
}

async fn wait_for_auth_marker(path: &Path) {
    tokio::time::timeout(std::time::Duration::from_secs(15), async {
        while !path.exists() {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("agent did not reach {}", path.display()));
}

async fn wait_for_auth_containers(job: Uuid, count: usize) {
    tokio::time::timeout(std::time::Duration::from_secs(15), async {
        loop {
            let output = Command::new("docker")
                .args([
                    "ps",
                    "--filter",
                    &format!("label=donkeyspace.coordinator-job-id={job}"),
                    "--format",
                    "{{.ID}}",
                ])
                .output()
                .await
                .unwrap();
            assert!(output.status.success());
            if String::from_utf8_lossy(&output.stdout).lines().count() == count {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("expected {count} running credential fixture containers"));
}
