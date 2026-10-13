//! Keep the PR conversation useful while agents execute on the main loop.
use super::*;
use donkeyspace_db::revision_feedback::{self as db, Snapshot};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

pub async fn run(pool: donkeyspace_db::PgPool, provider: GitHubCredentialProvider, policy: Policy) {
    let client = provider.client();
    loop {
        if let Err(error) = cycle(&pool, &client, &policy).await {
            tracing::warn!(%error, "PR revision feedback refresh failed");
        }
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
}

pub async fn cycle(
    pool: &donkeyspace_db::PgPool,
    client: &GitHubClient,
    policy: &Policy,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    for job in db::due(pool).await? {
        let result =
            tokio::time::timeout(Duration::from_secs(45), project(pool, client, policy, job)).await;
        let error = match result {
            Ok(Ok(())) => None,
            Ok(Err(error)) => Some(error.to_string()),
            Err(_) => Some("revision feedback projection timed out".into()),
        };
        if let Some(error) = error {
            db::failed(pool, job).await?;
            tracing::warn!(job_id=%job, %error, "PR revision feedback will retry");
        }
    }
    Ok(())
}

async fn project(
    pool: &donkeyspace_db::PgPool,
    client: &GitHubClient,
    policy: &Policy,
    job: uuid::Uuid,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Serialize projection with decisions and other publishers. Always load
    // current state under the lock; an older queued update cannot regress it.
    let mut tx = donkeyspace_db::cancellation::lock_job_side_effect(pool, job).await?;
    let saved = db::snapshot(&mut tx, job).await?;
    let body = render(&saved, &policy.facade.resolve());
    let digest = format!("{:x}", Sha256::digest(body.as_bytes()));
    let terminal = matches!(saved.status.as_str(), "completed" | "failed" | "superseded");
    if saved.feedback_digest.as_deref() != Some(&digest) {
        let owner = saved.input["repository"]["owner"]["login"]
            .as_str()
            .ok_or("revision owner missing")?;
        let repo = saved.input["repository"]["name"]
            .as_str()
            .ok_or("revision repository missing")?;
        let marker = format!("<!-- donkeyspace-pr-revision-status:{job} -->");
        let timestamp = db::timestamp(&mut tx).await?;
        client.upsert_issue_comment(owner, repo, saved.pr_number, &marker,
            &format!("{body}\n\nLast updated: {timestamp}\n\n{marker}\n<!-- donkeyspace-generated -->")).await?;
    }
    db::projected(&mut tx, job, &digest, terminal).await?;
    tx.commit().await?;
    Ok(())
}

fn key(value: &Value) -> Option<donkeyspace_core::TaskKey> {
    serde_json::from_value(value.clone()).ok()
}

fn render(saved: &Snapshot, facade: &donkeyspace_core::Facade) -> String {
    let input = &saved.input;
    let checkpoint = saved.checkpoint.as_ref().unwrap_or(&Value::Null);
    let result = saved.result.as_ref().unwrap_or(&Value::Null);
    let owner = input["repository"]["owner"]["login"]
        .as_str()
        .unwrap_or_default();
    let repo = input["repository"]["name"].as_str().unwrap_or_default();
    let base = format!("https://github.com/{owner}/{repo}");
    let parent = input["issue"]["number"].as_i64().unwrap_or_default();
    let request = input["pr_revision"]["comment_id"]
        .as_i64()
        .unwrap_or_default();
    let requested = input["pr_revision"]["command"]["feedback"]
        .as_str()
        .unwrap_or("Requested revision");
    let target = input["pr_revision"]["command"]["target"]
        .as_str()
        .unwrap_or("developer");
    let published = saved.publication_status == "published";
    let phase = match saved.status.as_str() {
        "queued" | "leased" => {
            if input["donkeyspace_resume"] == true {
                "Decision accepted — continuing the same run"
            } else {
                "Request accepted — queued"
            }
        }
        "paused" => "Awaiting your decision",
        "running" if published => "Changes published — confirming ready for review",
        "running" if saved.publication_status == "publishing" => "Publishing validated changes",
        "running" => "Revision in progress",
        "completed" if published && result["outcome"] == "implemented" => "Ready for review",
        _ if published => "Changes published — readiness needs attention",
        _ if saved.publication_status == "publishing" => "Publication needs verification",
        _ => "Revision needs attention",
    };
    let mut lines = vec![
        format!("### Revision {} — {phase}", saved.number),
        String::new(),
    ];
    if saved.status == "paused" && result["outcome"] == "needs_info" {
        lines.push(format!("**Next: answer the questions on [parent issue #{parent}]({base}/issues/{parent}) to continue this run.** This pause needs clarification, not an approval."));
    } else if saved.status == "paused" {
        lines.push("**Next: post a new decision comment on this PR.** Commands on the parent issue also continue this same run.".into());
        let mut pending = checkpoint["pending_approvals"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|a| key(&a["key"]))
            .collect::<Vec<_>>();
        if pending.is_empty() && input["donkeyspace_lifecycle_coordinator"] != true {
            pending.push(donkeyspace_core::TaskKey {
                task: "developer".into(),
                work_item: None,
            });
        }
        for task in &pending {
            let commands = task.approval_commands(facade);
            lines.push(format!("\n- Approve `{}`: `{}`\n- Request changes: `{}` followed by specific feedback on the next lines.", task.target(), commands.approve, commands.revise));
        }
        for option in donkeyspace_core::approval::revision_previews(checkpoint, facade) {
            lines.push(format!(
                "\n- Revise completed upstream work: `{}` followed by feedback. This reruns: {}.",
                option.revise_command,
                option.affected.join(", ")
            ));
        }
        if pending.is_empty() {
            lines.push("\nNo task approval is available. Review the blocker below before retrying; another PR revision cannot start while this run remains paused.".into());
        }
    } else if saved.status == "completed" && published && result["outcome"] == "implemented" {
        lines.push("**Next: review the updated changes on this PR.** No further execution approval is required.".into());
    } else if published && saved.status != "running" {
        lines.push("**Next: review the published commit and PR state, then mark it ready when appropriate.** The changes were pushed; do not rerun implementation solely to retry readiness.".into());
    } else if matches!(saved.status.as_str(), "queued" | "leased" | "running") {
        lines.push("**Next: no action needed while work runs.** This PR becomes draft before edits and returns to ready after successful publication and confirmation.".into());
    } else {
        lines.push(format!("**Next: resolve the blocker below.** For a new implementation attempt, post `{} revise {target}` with feedback. If publication is uncertain, first compare the PR head with the recorded commit.", facade.issue_command()));
    }
    lines.push(format!("\n**Requested:** {}\n\nTarget: `{target}` · [Your request]({base}/pull/{}#issuecomment-{request}) · [Parent issue #{parent}]({base}/issues/{parent})", quote(requested, 1500), saved.pr_number));
    if let Some(decision) = input.get("donkeyspace_human_decision") {
        lines.push(format!(
            "\nLatest accepted decision: `{}` for `{}`{}.",
            decision["action"].as_str().unwrap_or_default(),
            decision["target"].as_str().unwrap_or("pending target"),
            decision["feedback"]
                .as_str()
                .map(|feedback| format!(" — {}", quote(feedback, 500)))
                .unwrap_or_default()
        ));
    }
    let mut tasks = BTreeMap::new();
    for child in saved.children.as_array().into_iter().flatten() {
        let task = child
            .pointer("/input/plugin_execution/task")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let item = child
            .pointer("/input/plugin_execution/work_item/id")
            .and_then(Value::as_str);
        tasks.insert(
            item.map_or_else(|| task.to_owned(), |item| format!("{task}/{item}")),
            child,
        );
    }
    if !tasks.is_empty() {
        lines.push("\n| Task | Progress |\n| --- | --- |".into());
        for (task, child) in tasks {
            lines.push(format!(
                "| `{task}` | {} |",
                child["status"].as_str().unwrap_or("waiting")
            ));
        }
    }
    let reason = result["blocked_reason"]
        .as_str()
        .or(result["human_review_reason"].as_str());
    if let Some(reason) = reason {
        // The first paragraph is the actual blocker; old result text can carry
        // obsolete parent-only command instructions, so never repeat those.
        lines.push(format!(
            "\n**Blocker / decision:** {}",
            quote(reason.split("\n\n").next().unwrap_or(reason), 1800)
        ));
    }
    for question in result["questions"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
    {
        lines.push(format!("\n- {}", quote(question, 1800)));
    }
    if let Some(sha) = &saved.publication_sha {
        lines.push(format!(
            "\n{}: [`{sha}`]({base}/commit/{sha}) · [Revision diff]({base}/compare/{}...{sha})",
            if published {
                "Published commit"
            } else {
                "Candidate commit (publication not confirmed)"
            },
            saved.head_sha
        ));
    } else if let Some(sha) = checkpoint
        .pointer("/repository_snapshot/commit_sha")
        .and_then(Value::as_str)
    {
        lines.push(format!("\n[Review proposed changes]({base}/compare/{}...{sha}) · [Checkpoint]({base}/commit/{sha})\n\nThese changes are retained in the revision workspace; they have not been published to this PR branch.",saved.head_sha));
    }
    let summary = result["summary"].as_str().unwrap_or_default();
    let checks = result["tests"]
        .as_array()
        .into_iter()
        .flatten()
        .map(|test| {
            format!(
                "- {}: {}",
                test["name"].as_str().unwrap_or("Check"),
                test["status"].as_str().unwrap_or("unknown")
            )
        })
        .collect::<Vec<_>>()
        .join("\n");
    if !summary.is_empty() || !checks.is_empty() {
        lines.push(format!("\n<details>\n<summary>Task summaries and check evidence</summary>\n\n{}\n\n{}\n\n</details>",quote(summary,12000),checks));
    }
    lines.push(format!("\nRun: `{}`", saved.job_id));
    lines.join("\n")
}

fn quote(text: &str, limit: usize) -> String {
    // Feedback is untrusted Markdown; do not let embedded HTML hide controls.
    text.chars()
        .take(limit)
        .collect::<String>()
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn snapshot() -> Snapshot {
        Snapshot {
            job_id: uuid::Uuid::nil(),
            number: 2,
            pr_number: 46,
            head_sha: "before".into(),
            publication_sha: None,
            publication_status: "paused".into(),
            status: "paused".into(),
            feedback_digest: None,
            input: json!({"repository":{"owner":{"login":"team"},"name":"example"},
                "issue":{"number":44},"donkeyspace_lifecycle_coordinator":true,
                "pr_revision":{"comment_id":123,"command":{"target":"build/queue","feedback":"Fix reset <details>"}}}),
            result: Some(
                json!({"outcome":"needs_human","human_review_reason":"Contract needs a decision.\n\nOld instructions on parent issue.","summary":"Agent evidence","tests":[{"name":"lint","status":"passed"}]}),
            ),
            checkpoint: Some(
                json!({"pending_approvals":[{"key":{"task":"build","work_item":"queue"}}],
                "revision_options":[{"target":{"task":"plan"},"affected":[{"task":"plan"},{"task":"build","work_item":"queue"}]}],
                "repository_snapshot":{"commit_sha":"proposed"}}),
            ),
            children: json!([
                {"input":{"plugin_execution":{"task":"build","work_item":{"id":"queue"}}},"status":"failed"},
                {"input":{"plugin_execution":{"task":"build","work_item":{"id":"queue"}}},"status":"running"}
            ]),
        }
    }
    fn body(saved: &Snapshot) -> String {
        render(
            saved,
            &donkeyspace_core::FacadeConfig {
                command: Some("studio".into()),
                ..Default::default()
            }
            .resolve(),
        )
    }

    #[test]
    fn paused_progress_shows_exact_pr_commands_and_latest_task_attempt() {
        let saved = snapshot();
        let text = body(&saved);
        for expected in [
            "Awaiting your decision",
            "on this PR",
            "/studio approve build/queue",
            "/studio revise plan",
            "This reruns: plan, build/queue",
            "compare/before...proposed",
            "Fix reset &lt;details&gt;",
            "lint: passed",
            "#issuecomment-123",
            "| `build/queue` | running |",
        ] {
            assert!(text.contains(expected), "missing {expected}: {text}");
        }
        assert!(!text.contains("Old instructions"));
        assert!(!text.contains("| failed |"));
    }

    #[test]
    fn publication_readiness_and_unconfirmed_push_have_distinct_feedback() {
        let mut saved = snapshot();
        saved.status = "completed".into();
        saved.publication_status = "publishing".into();
        saved.publication_sha = Some("after".into());
        assert!(body(&saved).contains("Publication needs verification"));
        assert!(body(&saved).contains("publication not confirmed"));
        saved.publication_status = "published".into();
        assert!(body(&saved).contains("readiness needs attention"));
        assert!(body(&saved).contains("do not rerun implementation"));
        saved.result = Some(json!({"outcome":"implemented"}));
        assert!(body(&saved).contains("Ready for review"));
        assert!(body(&saved).contains("Published commit"));
        assert!(!body(&saved).contains("post a new decision"));
    }

    #[test]
    fn acceptance_and_resume_are_distinct_and_unchanged_state_is_stable() {
        let mut saved = snapshot();
        saved.status = "queued".into();
        assert!(body(&saved).contains("Request accepted — queued"));
        let unchanged = body(&saved);
        saved.feedback_digest = Some("already-published".into());
        assert_eq!(body(&saved), unchanged);
        saved.input["donkeyspace_resume"] = json!(true);
        saved.input["donkeyspace_human_decision"] = json!({"action":"approve","target":"plan"});
        assert!(body(&saved).contains("Decision accepted"));
        assert!(body(&saved).contains("Latest accepted decision: `approve` for `plan`"));
    }

    #[test]
    fn clarification_pause_keeps_questions_and_does_not_offer_an_approval() {
        let mut saved = snapshot();
        saved.result = Some(json!({"outcome":"needs_info","questions":["Which reset polarity?"]}));
        let text = body(&saved);
        assert!(text.contains("answer the questions on [parent issue #44]"));
        assert!(text.contains("Which reset polarity?"));
        assert!(!text.contains("/studio approve"));
    }
}
