//! Continue a published workflow from the remote PR head and publish only after
//! the normal implementation/validation path succeeds.
use super::*;
use donkeyspace_db::pr_revisions;

pub async fn instructions(
    pool: &donkeyspace_db::PgPool,
    policy: &Policy,
    job: &JobRecord,
    source: uuid::Uuid,
) -> Result<String, Box<dyn std::error::Error>> {
    let planning = if let Some(selection) = &policy.lifecycle.plugin {
        let manifest = donkeyspace_core::PluginManifest::from_path(&selection.manifest_path)?;
        let flow = manifest
            .flows
            .get(&selection.flow)
            .ok_or("Missing lifecycle flow")?;
        if flow.change_intake {
            return Ok(donkeyspace_core::pr_revision::intake_instructions(
                &policy.facade.resolve().command,
                issue_number(&job.input).unwrap_or(0),
            ));
        }
        Some(flow.start.clone())
    } else {
        None
    };
    let targets = pr_revisions::snapshot(pool, source)
        .await?
        .and_then(|saved| saved.get("completed_keys").cloned())
        .map(serde_json::from_value::<Vec<donkeyspace_core::TaskKey>>)
        .transpose()?
        .unwrap_or_default();
    Ok(donkeyspace_core::pr_revision::instructions(
        &policy.facade.resolve().command,
        issue_number(&job.input).unwrap_or(0),
        &targets,
        planning.as_deref(),
    ))
}

pub async fn prepare(
    pool: &donkeyspace_db::PgPool,
    policy: &Policy,
    job: &mut JobRecord,
    token: Option<&str>,
    config: &RepoContextConfig,
) -> Result<Value, Box<dyn std::error::Error>> {
    if job.input.get("pr_revision").is_none() {
        return build_repository_context(pool, &job.input, job.id, token, config).await;
    }
    let client =
        configured_github_client(token.ok_or("PR revision requires GitHub authentication")?)?;
    let pr = client
        .pull_request(
            &repository_owner(&job.input)?,
            &repository_name(&job.input)?,
            number(&job.input)? as u64,
        )
        .await?;
    let resumed = job.input["donkeyspace_resume"] == true;
    let expected = resumed
        .then(|| job.input["pr_revision"]["base_sha"].as_str())
        .flatten();
    validate_live_pr(&job.input, &pr, expected)?;
    let source = serde_json::from_value(job.input["pr_revision"]["source_job_id"].clone())?;
    let guidance = instructions(pool, policy, job, source).await?;
    // Draft is a prerequisite for editing, including resumed work. A failure
    // here stops execution before any agent or branch modification starts.
    cancellation::side_effect(pool, job.id, async {
        let owner = repository_owner(&job.input)?;
        let repo = repository_name(&job.input)?;
        client
            .set_pull_request_draft(
                &owner,
                &repo,
                number(&job.input)? as u64,
                true,
                pr["head"]["sha"].as_str(),
            )
            .await?;
        client
            .ensure_revision_instructions(&owner, &repo, number(&job.input)? as u64, &guidance)
            .await?;
        Ok::<_, Box<dyn std::error::Error>>(())
    })
    .await?;
    if resumed {
        return build_repository_context(pool, &job.input, job.id, token, config).await;
    }
    if donkeyspace_db::lifecycle_checkpoints::load(pool, job.id)
        .await?
        .is_some()
    {
        return Err("Revision preparation was interrupted after checkpoint persistence; submit a fresh revision command to preserve the retained attempt.".into());
    }
    job.input["pull_request"] = pr;
    let context = build_repository_context(pool, &job.input, job.id, token, config).await?;
    let repo = repository_checkout_path(&context)?;
    checkout_pull_request_branch(&job.input, &repo, token).await?;
    let head = run_git(&repo, &["rev-parse", "HEAD"], None)
        .await?
        .trim()
        .to_owned();
    // Fetch may race with the API read. The checked-out commit is the input
    // actually used; publication verifies it again against GitHub.
    job.input["pull_request"]["head"]["sha"] = json!(head);
    job.input["pr_revision"]["base_sha"] = json!(head);
    if let Some(selection) = &policy.lifecycle.plugin {
        let source: uuid::Uuid =
            serde_json::from_value(job.input["pr_revision"]["source_job_id"].clone())?;
        let snapshot = pr_revisions::snapshot(pool, source)
            .await?
            .ok_or("Completed lifecycle snapshot is unavailable")?;
        let changed_paths = if let Some(previous) = snapshot["head_sha"].as_str().or_else(|| {
            snapshot
                .pointer("/repository_snapshot/commit_sha")
                .and_then(Value::as_str)
        }) {
            run_git(
                &repo,
                &["diff", "--name-only", "--no-renames", previous, "HEAD"],
                None,
            )
            .await?
            .lines()
            .map(str::to_owned)
            .collect::<Vec<_>>()
        } else {
            Vec::new()
        };
        let target = job.input["pr_revision"]["command"]["target"]
            .as_str()
            .unwrap_or_default();
        let (revision_target, reopened) = plugin_flow::prepare_pr_revision(
            selection,
            snapshot,
            target,
            &repo,
            &workspace_path(job.id, config),
            &changed_paths,
        )?;
        for number in reopened {
            cancellation::side_effect(
                pool,
                job.id,
                client.reopen_issue(
                    &repository_owner(&job.input)?,
                    &repository_name(&job.input)?,
                    number,
                ),
            )
            .await?;
        }
        job.input["donkeyspace_human_decision"] = json!({"action":"revise","target":revision_target,"feedback":job.input["pr_revision"]["command"]["feedback"]});
        let owner = repository_owner(&job.input)?;
        let name = repository_name(&job.input)?;
        let workspace = workspace_path(job.id, config);
        let publication = PublicationContext {
            pool,
            coordinator_job_id: job.id,
            workflow_item_id: job.workflow_item_id,
            issue_number: issue_number(&job.input).ok_or("Missing parent issue")?,
            owner: &owner,
            repo: &name,
            workspace_path: &workspace,
            token,
        };
        publish_checkpoint(&publication, &repo, "chore: retain PR revision input", &[]).await?;
        plugin_flow::persist_pr_revision_checkpoint(
            selection,
            &workspace,
            &job.input,
            &plugin_flow::LifecycleTracking {
                pool,
                policy,
                coordinator: job,
                github: Some(&client),
                publication: Some(publication),
            },
        )
        .await?;
    }
    job.input["donkeyspace_resume"] = json!(true);
    pr_revisions::prepared(pool, job.id, &job.input, &head).await?;
    Ok(serde_json::to_value(repo_context::summarize_checkout(
        &repository_owner(&job.input)?,
        &repository_name(&job.input)?,
        &repository_default_branch(&job.input),
        &repo,
        &job.input,
        config,
    )?)?)
}

pub async fn save_checkpoint(
    pool: &donkeyspace_db::PgPool,
    job: &JobRecord,
    config: &RepoContextConfig,
) -> Result<(), Box<dyn std::error::Error>> {
    let path = workspace_path(job.id, config).join(".donkeyspace/lifecycle-checkpoint.json");
    let checkpoint =
        if let Some(record) = donkeyspace_db::lifecycle_checkpoints::load(pool, job.id).await? {
            Some(record.state)
        } else if path.is_file() {
            Some(serde_json::from_slice::<Value>(&fs::read(path)?)?)
        } else {
            None
        };
    if let Some(mut checkpoint) = checkpoint {
        checkpoint["head_sha"] = json!(
            run_git(
                &workspace_path(job.id, config).join("repo"),
                &["rev-parse", "HEAD"],
                None
            )
            .await?
            .trim()
        );
        pr_revisions::save_snapshot(pool, job.id, &checkpoint).await?;
    }
    Ok(())
}

/// Built-in revisions have no plugin graph, but a human pause still needs the
/// same immutable checkout provenance used by repository recovery.
pub async fn save_builtin_pause(
    job: &JobRecord,
    publication: &PublicationContext<'_>,
    repo: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    publish_checkpoint(publication, repo, "chore: retain paused PR revision", &[]).await?;
    let snapshot = crate::checkout_recovery::capture(
        publication,
        repo,
        &repository_default_branch(&job.input),
    )
    .await?;
    let current = donkeyspace_db::lifecycle_checkpoints::load(publication.pool, job.id).await?;
    donkeyspace_db::lifecycle_checkpoints::save(
        publication.pool,
        donkeyspace_db::lifecycle_checkpoints::Commit {
            coordinator: job,
            expected_revision: current.map_or(0, |record| record.revision),
            version: 1,
            state: &json!({"repository_snapshot":snapshot}),
            completed: false,
            effects: &Default::default(),
        },
    )
    .await?;
    Ok(())
}

pub fn number(input: &Value) -> Result<i64, Box<dyn std::error::Error>> {
    input["pull_request"]["number"]
        .as_i64()
        .ok_or_else(|| "revision has no PR number".into())
}

fn validate_live_pr(
    input: &Value,
    live: &Value,
    expected_head: Option<&str>,
) -> Result<(), Box<dyn std::error::Error>> {
    if live["state"] != "open" || live["merged"] == true {
        return Err("The PR is closed or merged.".into());
    }
    if live["head"]["ref"] != input["pull_request"]["head"]["ref"]
        || live["base"]["ref"] != input["pull_request"]["base"]["ref"]
        || live["head"]["repo"]["full_name"] != live["base"]["repo"]["full_name"]
    {
        return Err("The PR branch or target changed; request a fresh revision against its current configuration.".into());
    }
    if expected_head.is_some_and(|head| live["head"]["sha"].as_str() != Some(head)) {
        return Err(
            donkeyspace_github::GitHubClientError::PullRequestHeadMismatch {
                expected: expected_head.unwrap().into(),
                actual: live["head"]["sha"].as_str().map(str::to_owned),
            }
            .into(),
        );
    }
    Ok(())
}

pub async fn publish(
    pool: &donkeyspace_db::PgPool,
    job: &JobRecord,
    config: &RepoContextConfig,
    token: Option<&str>,
    result: &RunResult,
    repo: &Path,
    uncommitted_files: &[String],
) -> Result<String, Box<dyn std::error::Error>> {
    let owner = repository_owner(&job.input)?;
    let name = repository_name(&job.input)?;
    let number = number(&job.input)?;
    let client =
        configured_github_client(token.ok_or("PR revision requires GitHub authentication")?)?;
    let expected = job.input["pr_revision"]["base_sha"]
        .as_str()
        .ok_or("Revision base is missing")?;
    let live = client.pull_request(&owner, &name, number as u64).await?;
    validate_live_pr(&job.input, &live, Some(expected))?;
    if client
        .issue_is_closed(
            &owner,
            &name,
            issue_number(&job.input).ok_or("Missing parent issue")?,
        )
        .await?
    {
        return Err("The parent issue is closed; revision publication stopped.".into());
    }
    configure_git_author(repo).await?;
    if !uncommitted_files.is_empty() {
        stage_changed_files(repo, uncommitted_files).await?;
        run_git(
            repo,
            &[
                "commit",
                "-m",
                &format!("fix: address review feedback for PR #{number}"),
                "-m",
                &result.summary,
            ],
            None,
        )
        .await?;
    }
    let sha = run_git(repo, &["rev-parse", "HEAD"], None)
        .await?
        .trim()
        .to_owned();
    // A normal push rejects concurrent divergent updates. Before pushing, also
    // assert our candidate descends from the exact input head.
    run_git(repo, &["merge-base", "--is-ancestor", expected, &sha], None).await?;
    save_checkpoint(pool, job, config).await?;
    pr_revisions::mark_publication(pool, job.id, &sha, false).await?;
    let branch = live["head"]["ref"].as_str().ok_or("Missing PR branch")?;
    let current = current_github_token(token).await?;
    cancellation::side_effect(pool, job.id, async {
        // Recheck immediately before the remote write, including PR closure.
        let live = client.pull_request(&owner, &name, number as u64).await?;
        validate_live_pr(&job.input, &live, Some(expected))?;
        // A maintainer may have changed the draft flag while work was paused.
        client
            .set_pull_request_draft(&owner, &name, number as u64, true, Some(expected))
            .await?;
        push_commit(
            repo,
            &format!("https://github.com/{owner}/{name}.git"),
            branch,
            current.as_deref(),
        )
        .await?;
        Ok::<_, Box<dyn std::error::Error>>(())
    })
    .await?;
    pr_revisions::mark_publication(pool, job.id, &sha, true).await?;
    Ok(sha)
}

/// Separate readiness from publication so a failed metadata update never
/// reports a successfully pushed commit as unpublished.
pub async fn mark_ready(
    pool: &donkeyspace_db::PgPool,
    policy: &Policy,
    job: &JobRecord,
    token: Option<&str>,
    sha: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let client =
        configured_github_client(token.ok_or("PR revision requires GitHub authentication")?)?;
    let owner = repository_owner(&job.input)?;
    let repo = repository_name(&job.input)?;
    let guidance = instructions(pool, policy, job, job.id).await?;
    let previous_head = job.input["pr_revision"]["base_sha"]
        .as_str()
        .ok_or("Revision base is missing")?;
    let delays = [1, 2, 4, 8, 16].map(Duration::from_secs);
    retry_readiness(previous_head, sha, &delays, || {
        cancellation::side_effect(pool, job.id, async {
            let live = client
                .pull_request(&owner, &repo, number(&job.input)? as u64)
                .await?;
            validate_live_pr(&job.input, &live, Some(sha))?;
            client
                .ensure_revision_instructions(&owner, &repo, number(&job.input)? as u64, &guidance)
                .await?;
            client
                .set_pull_request_draft(&owner, &repo, number(&job.input)? as u64, false, Some(sha))
                .await?;
            Ok::<_, Box<dyn std::error::Error>>(())
        })
    })
    .await
}

/// GitHub can briefly return the pre-push PR head after a successful push.
/// Retry only that known stale head; unrelated commits, closure, retargeting,
/// permissions errors, and failed draft restoration require attention.
async fn retry_readiness<F, Fut>(
    previous_head: &str,
    published_head: &str,
    delays: &[Duration],
    mut attempt: F,
) -> Result<(), Box<dyn std::error::Error>>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<(), Box<dyn std::error::Error>>>,
{
    let mut delays = delays.iter();
    loop {
        match attempt().await {
            Ok(()) => return Ok(()),
            Err(error) => {
                let stale = matches!(error.downcast_ref::<donkeyspace_github::GitHubClientError>(),
                    Some(donkeyspace_github::GitHubClientError::PullRequestHeadMismatch { expected, actual })
                    if expected == published_head && actual.as_deref() == Some(previous_head) && previous_head != published_head);
                if !stale {
                    return Err(error);
                }
                let Some(delay) = delays.next() else {
                    return Err(format!("Commit {published_head} was published, but GitHub still reports the previous head {previous_head}. Readiness confirmation timed out; the PR remains draft.").into());
                };
                tracing::info!(
                    published_head,
                    previous_head,
                    delay_seconds = delay.as_secs(),
                    "waiting for GitHub to expose the published PR head"
                );
                // Release the side-effect lock between attempts so cancellation
                // can proceed. Every retry revalidates execution and PR state.
                tokio::time::sleep(*delay).await;
            }
        }
    }
}

fn push_arguments(remote: &str, branch: &str) -> Vec<String> {
    vec![
        "push".into(),
        "--".into(),
        remote.into(),
        format!("HEAD:refs/heads/{branch}"),
    ]
}

async fn push_commit(
    repo: &Path,
    remote: &str,
    branch: &str,
    token: Option<&str>,
) -> Result<(), Box<dyn std::error::Error>> {
    let args = push_arguments(remote, branch);
    run_git(
        repo,
        &args.iter().map(String::as_str).collect::<Vec<_>>(),
        token,
    )
    .await?;
    Ok(())
}

pub async fn report(
    pool: &donkeyspace_db::PgPool,
    job: &donkeyspace_db::JobRecord,
) -> Result<(), Box<dyn std::error::Error>> {
    donkeyspace_db::revision_feedback::wake(pool, job.id).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    // Only the local fixture enables file transport; production uses the
    // same non-forcing push arguments through the HTTPS-only Git boundary.
    async fn push_local(
        repo: &Path,
        remote: &str,
        branch: &str,
        _: Option<&str>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let args = push_arguments(remote, branch);
        let mut local_args = vec!["-c", "protocol.file.allow=always"];
        local_args.extend(args.iter().map(String::as_str));
        run_git(repo, &local_args, None).await?;
        Ok(())
    }
    #[tokio::test]
    async fn normal_revision_push_preserves_human_commits_and_rejects_a_race() {
        let root = env::temp_dir().join(format!("donkeyspace-pr-push-{}", uuid::Uuid::now_v7()));
        let remote = root.join("remote.git");
        let agent = root.join("agent");
        let human = root.join("human");
        for path in [&remote, &agent, &human] {
            fs::create_dir_all(path).unwrap();
        }
        run_git(&remote, &["init", "-b", "unused"], None)
            .await
            .unwrap();
        run_git(&human, &["init"], None).await.unwrap();
        configure_git_author(&human).await.unwrap();
        fs::write(human.join("human.txt"), "Original human work").unwrap();
        run_git(&human, &["add", "."], None).await.unwrap();
        run_git(&human, &["commit", "-m", "human base"], None)
            .await
            .unwrap();
        push_local(&human, remote.to_str().unwrap(), "work", None)
            .await
            .unwrap();
        fs::remove_dir(&agent).unwrap();
        crate::trusted_git::clone_local(&human, &agent)
            .await
            .unwrap();
        configure_git_author(&agent).await.unwrap();
        fs::write(agent.join("fix.txt"), "Requested fix").unwrap();
        run_git(&agent, &["add", "."], None).await.unwrap();
        run_git(&agent, &["commit", "-m", "fix"], None)
            .await
            .unwrap();
        push_local(&agent, remote.to_str().unwrap(), "work", None)
            .await
            .unwrap();
        assert_eq!(
            run_git(&remote, &["show", "work:human.txt"], None)
                .await
                .unwrap(),
            "Original human work"
        );
        // A human extends the now-published PR while the next agent attempt
        // still has the previous head. Its normal push must be rejected.
        run_git(
            &human,
            &[
                "-c",
                "protocol.file.allow=always",
                "fetch",
                remote.to_str().unwrap(),
                "work",
            ],
            None,
        )
        .await
        .unwrap();
        run_git(&human, &["reset", "--hard", "FETCH_HEAD"], None)
            .await
            .unwrap();
        fs::write(human.join("human.txt"), "New human work").unwrap();
        run_git(&human, &["add", "."], None).await.unwrap();
        run_git(&human, &["commit", "-m", "human update"], None)
            .await
            .unwrap();
        push_local(&human, remote.to_str().unwrap(), "work", None)
            .await
            .unwrap();
        let human_head = run_git(&human, &["rev-parse", "HEAD"], None).await.unwrap();
        fs::write(agent.join("fix.txt"), "Another fix").unwrap();
        run_git(&agent, &["add", "."], None).await.unwrap();
        run_git(&agent, &["commit", "-m", "stale fix"], None)
            .await
            .unwrap();
        assert!(
            push_local(&agent, remote.to_str().unwrap(), "work", None)
                .await
                .is_err()
        );
        assert_eq!(
            run_git(&remote, &["rev-parse", "work"], None)
                .await
                .unwrap(),
            human_head
        );
        assert_eq!(
            run_git(&remote, &["show", "work:human.txt"], None)
                .await
                .unwrap(),
            "New human work"
        );
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn publication_rejects_moved_closed_or_retargeted_prs() {
        let pr = json!({"state":"open","head":{"ref":"work","sha":"a","repo":{"full_name":"o/r"}},"base":{"ref":"main","repo":{"full_name":"o/r"}}});
        let input = json!({"pull_request":pr});
        assert!(validate_live_pr(&input, &pr, Some("a")).is_ok());
        assert!(validate_live_pr(&input, &pr, Some("b")).is_err());
        for (path, value) in [
            ("/state", json!("closed")),
            ("/base/ref", json!("other")),
            ("/head/repo/full_name", json!("fork/r")),
        ] {
            let mut changed = pr.clone();
            *changed.pointer_mut(path).unwrap() = value;
            assert!(validate_live_pr(&input, &changed, Some("a")).is_err());
        }
    }

    #[tokio::test]
    async fn readiness_retries_stale_pre_push_reads_until_publication_is_visible() {
        let pr = json!({"state":"open","head":{"ref":"work","sha":"old","repo":{"full_name":"o/r"}},"base":{"ref":"main","repo":{"full_name":"o/r"}}});
        let input = json!({"pull_request":pr});
        let mut attempts = 0;
        retry_readiness("old", "published", &[Duration::ZERO; 3], || {
            attempts += 1;
            let mut live = pr.clone();
            if attempts == 3 {
                live["head"]["sha"] = json!("published");
            }
            std::future::ready(validate_live_pr(&input, &live, Some("published")))
        })
        .await
        .unwrap();
        assert_eq!(attempts, 3);
    }

    #[tokio::test]
    async fn readiness_timeout_retains_successful_publication_in_its_error() {
        let mut attempts = 0;
        let error = retry_readiness("old", "published", &[Duration::ZERO; 2], || {
            attempts += 1;
            std::future::ready(Err(
                donkeyspace_github::GitHubClientError::PullRequestHeadMismatch {
                    expected: "published".into(),
                    actual: Some("old".into()),
                }
                .into(),
            ))
        })
        .await
        .unwrap_err();
        assert_eq!(attempts, 3);
        assert!(error.to_string().contains("Commit published was published"));
        assert!(error.to_string().contains("PR remains draft"));
        assert!(!error.to_string().contains("No update was pushed"));
    }

    #[tokio::test]
    async fn readiness_never_retries_human_updates_closure_or_retargeting() {
        let pr = json!({"state":"open","head":{"ref":"work","sha":"published","repo":{"full_name":"o/r"}},"base":{"ref":"main","repo":{"full_name":"o/r"}}});
        let input = json!({"pull_request":pr});
        for (path, value) in [
            ("/head/sha", json!("human-update")),
            ("/head/sha", Value::Null),
            ("/state", json!("closed")),
            ("/base/ref", json!("other")),
            ("/head/ref", json!("other")),
            ("/head/repo/full_name", json!("fork/r")),
        ] {
            let mut live = pr.clone();
            *live.pointer_mut(path).unwrap() = value;
            let mut attempts = 0;
            assert!(
                retry_readiness("old", "published", &[Duration::ZERO; 3], || {
                    attempts += 1;
                    std::future::ready(validate_live_pr(&input, &live, Some("published")))
                })
                .await
                .is_err()
            );
            assert_eq!(attempts, 1, "{path}");
        }
    }
}
