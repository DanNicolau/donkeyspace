//! PR conversation commands enter here before ordinary issue ingestion: a PR
//! must never accidentally become an independent issue workflow.
use super::*;
use donkeyspace_core::pr_revision::parse_command;
use donkeyspace_db::pr_revisions::{self, Admission};
use donkeyspace_db::{OutboundActionInput, create_outbound_action};

pub(super) async fn persist(
    pool: &PgPool,
    state: &AppState,
    event: &str,
    delivery: &str,
    body: &[u8],
) -> Result<WebhookPersistOutcome, Box<dyn std::error::Error>> {
    let payload: GitHubIssueWebhook = serde_json::from_slice(body)?;
    if event != "issue_comment" || payload.action != "created" || !human_command(&payload) {
        return Ok(WebhookPersistOutcome::Ignored);
    }
    let comment = payload.comment.as_ref().expect("human command has comment");
    let command = match parse_command(&comment.body, &state.policy.facade.resolve().command) {
        Ok(None) => return Ok(WebhookPersistOutcome::Ignored),
        other => other,
    };
    let Some(auth) = &state.github_auth else {
        return Ok(WebhookPersistOutcome::Ignored);
    };
    let github = auth.client();
    let owner = &payload.repository.owner.login;
    let repo = &payload.repository.name;
    let pr_number = payload.issue.number;
    let repository = upsert_repository(
        pool,
        &RepositoryInput {
            installation_external_id: payload.installation.as_ref().map(|i| i.id.to_string()),
            installation_account_login: payload.installation.as_ref().map(|_| owner.clone()),
            provider: "github".into(),
            owner: owner.clone(),
            name: repo.clone(),
            default_branch: payload.repository.default_branch.clone(),
        },
    )
    .await?;
    if webhook_delivery_exists(pool, delivery).await? {
        return Ok(WebhookPersistOutcome::Duplicate);
    }
    let Some(mut source) = pr_revisions::source(pool, repository, pr_number).await? else {
        return Ok(WebhookPersistOutcome::Ignored);
    };
    let parent = github.issue(owner, repo, source.issue_number).await?;
    let labels = parent["labels"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|l| l["name"].as_str().map(str::to_owned))
        .collect::<Vec<_>>();
    let authorization =
        authorize_engagement(state, EngagementGate::NeedsHumanResume, &labels, &payload).await;
    if !authorization.allowed
        || parent["state"] != "open"
        || labels
            .iter()
            .any(|label| state.policy.workflow.block_labels.contains(label))
    {
        record_authorization(
            pool,
            repository,
            source.workflow_item_id,
            delivery,
            event,
            body,
            &payload,
            authorization,
        )
        .await?;
        return Ok(WebhookPersistOutcome::Ignored);
    }
    let pr = github.pull_request(owner, repo, pr_number as u64).await?;
    if !open_same_repository_pr(&pr, owner, repo) {
        return Ok(WebhookPersistOutcome::Ignored);
    }
    if let Some(pending) = pr_revisions::pending_publication(pool, repository, pr_number).await? {
        let head = pr["head"]["sha"].as_str().ok_or("PR head is missing")?;
        if head == pending.publication_sha
            || github
                .commit_is_ancestor(owner, repo, &pending.publication_sha, head)
                .await?
        {
            // A prior push succeeded but its worker stopped before recording
            // acknowledgment. Reuse that published continuation, never replay
            // its already-published agent output.
            pr_revisions::mark_publication(pool, pending.job_id, &pending.publication_sha, true)
                .await?;
            source = pr_revisions::source(pool, repository, pr_number)
                .await?
                .ok_or("Recovered PR continuation is unavailable")?;
        }
    }
    let prepared = prepare_request(
        state, &github, &source, &payload, body, &parent, &pr, command,
    )
    .await;
    let (outcome, message, job_id) = match prepared {
        Ok((mut input, role)) => {
            input["pr_revision"]["authorization"] = json!({
                "gate": EngagementGate::NeedsHumanResume.as_str(),
                "matched_selector": authorization.matched_selector,
                "reason": authorization.reason,
            });
            let admission = pr_revisions::enqueue(
                pool,
                repository,
                pr_number,
                comment.id.unwrap(),
                &source,
                &role,
                &input,
            )
            .await?;
            match admission {
                Admission::Queued(job) => {
                    let id = job.id;
                    let message = format!("Revision queued for this PR (run `{id}`). Feedback will be applied to the current PR head; affected checks must pass before publication.");
                    (WebhookPersistOutcome::Queued(job), message, Some(id))
                }
                Admission::Duplicate => return Ok(WebhookPersistOutcome::Duplicate),
                Admission::Unavailable => (
                    WebhookPersistOutcome::Ignored,
                    "Revision not queued: this workflow has active or paused work, or the PR is no longer eligible. Complete the pending decision on the parent issue before requesting another PR revision.".into(),
                    None,
                ),
            }
        }
        Err(error) => (
            WebhookPersistOutcome::Ignored,
            format!("Revision not queued: {error}"),
            None,
        ),
    };
    // Record before queuing the response, so rejected commands do not produce
    // repeated notices when polled again under the same delivery identity.
    if record_authorization(
        pool,
        repository,
        source.workflow_item_id,
        delivery,
        event,
        body,
        &payload,
        authorization,
    )
    .await?
    {
        let response = OutboundActionInput {
            workflow_item_id: source.workflow_item_id,
            job_id,
            provider: "github".into(),
            action_type: "issue.create_comment".into(),
            payload: json!({"owner":owner,"repo":repo,"issue_number":pr_number,"body":format!("{message}\n\n<!-- donkeyspace-generated -->")}),
        };
        create_outbound_action(pool, &response).await?;
    }
    Ok(outcome)
}

fn human_command(payload: &GitHubIssueWebhook) -> bool {
    payload.comment.as_ref().is_some_and(|comment| {
        comment.id.is_some()
            && comment.performed_via_github_app.is_none()
            && !comment.body.contains("<!-- donkeyspace-generated -->")
            && comment
                .user
                .as_ref()
                .is_some_and(|a| a.kind.as_deref() == Some("User"))
    }) && payload
        .sender
        .as_ref()
        .is_some_and(|a| a.kind.as_deref() == Some("User"))
}

pub(super) fn open_same_repository_pr(pr: &Value, owner: &str, repo: &str) -> bool {
    pr["state"] == "open"
        && pr["merged"] != true
        && pr["head"]["repo"]["full_name"]
            .as_str()
            .is_some_and(|name| name.eq_ignore_ascii_case(&format!("{owner}/{repo}")))
        && pr["head"]["sha"].as_str().is_some_and(|s| !s.is_empty())
}

#[allow(clippy::too_many_arguments)]
async fn record_authorization(
    pool: &PgPool,
    repository: i64,
    workflow: i64,
    delivery: &str,
    event: &str,
    body: &[u8],
    payload: &GitHubIssueWebhook,
    authorization: AuthorizationDecision,
) -> Result<bool, Box<dyn std::error::Error>> {
    let Some(id) = record_webhook_delivery(
        pool,
        Some(repository),
        delivery,
        event,
        &serde_json::from_slice::<Value>(body)?,
    )
    .await?
    else {
        return Ok(false);
    };
    record_engagement_decision(
        pool,
        &EngagementDecisionInput {
            webhook_delivery_id: id,
            workflow_item_id: Some(workflow),
            gate: EngagementGate::NeedsHumanResume.as_str().into(),
            disposition: if authorization.allowed {
                "allowed"
            } else {
                "denied"
            }
            .into(),
            actor: payload
                .sender
                .as_ref()
                .map(|a| serde_json::to_value(a).expect("actor serializes")),
            matched_selector: authorization.matched_selector,
            reason: authorization.reason,
        },
    )
    .await?;
    Ok(true)
}

#[allow(clippy::too_many_arguments)]
async fn prepare_request(
    state: &AppState,
    github: &GitHubClient,
    source: &pr_revisions::RevisionSource,
    payload: &GitHubIssueWebhook,
    body: &[u8],
    parent: &Value,
    pr: &Value,
    parsed: Result<Option<donkeyspace_core::pr_revision::RevisionCommand>, String>,
) -> Result<(Value, String), String> {
    let owner = &payload.repository.owner.login;
    let repo = &payload.repository.name;
    let pr_number = payload.issue.number;
    let command = parsed?.expect("non-command already returned");
    let lifecycle = state.policy.lifecycle.plugin.as_ref();
    if lifecycle.is_some() != (source.input["donkeyspace_lifecycle_coordinator"] == true) {
        return Err(
            "The configured lifecycle differs from the workflow that created this PR.".into(),
        );
    }
    if let Some(selection) = lifecycle {
        let manifest = donkeyspace_core::PluginManifest::from_path(&selection.manifest_path)
            .map_err(|e| e.to_string())?;
        let flow = manifest
            .flows
            .get(&selection.flow)
            .ok_or("Configured flow is missing.")?;
        let target = command
            .target
            .as_deref()
            .ok_or("Plugin revisions require TASK or TASK/WORK-ITEM after revise.")?;
        let (task, item) = target
            .split_once('/')
            .map_or((target, None), |(t, i)| (t, Some(i)));
        let definition = flow
            .tasks
            .get(task)
            .ok_or("Unknown revision task; use a task from the configured flow.")?;
        if (definition.scope == donkeyspace_core::PluginTaskScope::WorkItem) != item.is_some() {
            return Err(
                "Use TASK for workflow tasks and TASK/WORK-ITEM for work-item tasks.".into(),
            );
        }
        let checkpoint = source.checkpoint.as_ref().ok_or("This PR predates durable continuation support. Its completed lifecycle snapshot is unavailable; start a new managed workflow or update the PR manually.")?;
        if !checkpoint["completed_keys"]
            .as_array()
            .into_iter()
            .flatten()
            .any(|k| k["task"] == task && k["work_item"].as_str() == item)
        {
            return Err("The target is not a completed task in this PR's lifecycle.".into());
        }
    } else if command
        .target
        .as_deref()
        .is_some_and(|target| target != "developer")
    {
        return Err(
            "The built-in lifecycle accepts only the developer target (or omit the target).".into(),
        );
    }
    let feedback = github
        .revision_feedback(owner, repo, pr_number, &command)
        .await
        .map_err(|e| e.to_string())?;
    let mut input = source.input.clone();
    let map = input
        .as_object_mut()
        .ok_or("Source job input is invalid.")?;
    for key in [
        "donkeyspace_resume",
        "donkeyspace_human_decision",
        "pr_revision",
        "pull_request",
    ] {
        map.remove(key);
    }
    map.insert("issue".into(), parent.clone());
    map.insert(
        "comment".into(),
        serde_json::from_slice::<Value>(body).map_err(|e| e.to_string())?["comment"].clone(),
    );
    map.insert("pull_request".into(), pr.clone());
    map.insert(
        "pr_revision".into(),
        json!({
            "comment_id":payload.comment.as_ref().and_then(|c|c.id), "source_job_id":source.job_id,
            "command":command,"feedback":feedback,"actor":payload.sender,
            "requested_head":pr["head"]["sha"],
        }),
    );
    if lifecycle.is_some() {
        map.insert("donkeyspace_lifecycle_coordinator".into(), json!(true));
    }
    let role = super::lifecycle_start_role(&state.policy)
        .map_err(|e| e.to_string())?
        .unwrap_or("developer".into());
    Ok::<_, String>((input, role))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generated_and_bot_comments_cannot_authorize_revision() {
        let raw = json!({"action":"created","repository":{"name":"example","default_branch":"main","owner":{"login":"team"}},
            "issue":{"id":2,"number":2,"state":"open","labels":[],"pull_request":{}},
            "comment":{"id":5,"body":"/studio revise implement/queue\nFix reset","user":{"login":"maintainer","type":"User"}},
            "sender":{"login":"maintainer","type":"User"}});
        assert!(human_command(&serde_json::from_value(raw.clone()).unwrap()));
        for (path, value) in [
            ("/sender/type", json!("Bot")),
            ("/comment/user/type", json!("Bot")),
            ("/comment/id", Value::Null),
            (
                "/comment/body",
                json!("/studio revise\nFix\n<!-- donkeyspace-generated -->"),
            ),
        ] {
            let mut payload = raw.clone();
            *payload.pointer_mut(path).unwrap() = value;
            assert!(!human_command(&serde_json::from_value(payload).unwrap()));
        }
    }

    #[test]
    fn only_open_prs_with_a_known_same_repository_head_are_eligible() {
        let pr = json!({"state":"open","head":{"sha":"a","repo":{"full_name":"team/example"}}});
        assert!(open_same_repository_pr(&pr, "team", "example"));
        assert!(!open_same_repository_pr(&pr, "fork", "example"));
        let mut closed = pr.clone();
        closed["merged"] = json!(true);
        assert!(!open_same_repository_pr(&closed, "team", "example"));
        closed["merged"] = json!(false);
        closed["state"] = json!("closed");
        assert!(!open_same_repository_pr(&closed, "team", "example"));
    }
}
