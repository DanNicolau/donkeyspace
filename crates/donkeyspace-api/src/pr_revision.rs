//! PR conversation commands enter here before ordinary issue ingestion: a PR
//! must never accidentally become an independent issue workflow.
use super::*;
use donkeyspace_core::pr_revision::parse_command;
use donkeyspace_db::pr_revisions::{self, Admission};
use donkeyspace_db::{OutboundActionInput, create_outbound_action};

#[derive(Debug)]
enum Command {
    Revise(donkeyspace_core::pr_revision::RevisionCommand),
    Approve { target: Option<String> },
}

fn parse_pr_command(body: &str, prefix: &str) -> Result<Option<Command>, String> {
    if let Some(command) = parse_command(body, prefix)? {
        return Ok(Some(Command::Revise(command)));
    }
    let first = body
        .trim()
        .lines()
        .next()
        .unwrap_or_default()
        .split_whitespace()
        .collect::<Vec<_>>();
    if first.first().copied() == Some(format!("/{prefix}").as_str())
        && first.get(1).copied() == Some("approve")
    {
        return match parse_human_approval_command(body, prefix) {
            Some(HumanApprovalAction::Approve { target }) => Ok(Some(Command::Approve { target })),
            _ => Err(format!(
                "Use /{prefix} approve TASK (or approve all) as the first line."
            )),
        };
    }
    Ok(None)
}

impl Command {
    fn decision(&self) -> Value {
        match self {
            Self::Approve { target } => json!({"action":"approve","target":target}),
            Self::Revise(command) => {
                json!({"action":"revise","target":command.target,"feedback":command.feedback})
            }
        }
    }
}

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
    let command = match parse_pr_command(&comment.body, &state.policy.facade.resolve().command) {
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
    if webhook_delivery_exists(pool, delivery).await?
        || comment_already_delivered(pool, repository, event, &payload).await?
    {
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
    if authorization.verification_unavailable {
        return Err(
            std::io::Error::new(std::io::ErrorKind::WouldBlock, authorization.reason).into(),
        );
    }
    let mut outcome = WebhookPersistOutcome::Ignored;
    let rejection = if !authorization.allowed {
        Some("This account is not authorized to request or approve revisions. Ask an authorized maintainer to submit the command.".into())
    } else if parent["state"] != "open" {
        Some(format!(
            "Parent issue #{} is closed. Reopen it before requesting a revision.",
            source.issue_number
        ))
    } else if labels
        .iter()
        .any(|label| state.policy.workflow.block_labels.contains(label))
    {
        Some(format!(
            "Parent issue #{} has a blocking label. Resolve that block before requesting a revision.",
            source.issue_number
        ))
    } else {
        let pr = github.pull_request(owner, repo, pr_number as u64).await?;
        if !open_same_repository_pr(&pr, owner, repo) {
            Some("This PR is closed, merged, or uses an unsupported fork branch. Use an open managed PR in this repository.".into())
        } else if let Err(error) = command {
            Some(error)
        } else {
            let command = command?.expect("non-command already returned");
            if let Some(active) = pr_revisions::active(pool, repository, pr_number).await? {
                if active.status != "paused" {
                    Some(format!(
                        "Revision run `{}` is {}. Your command was not queued. Wait for its progress comment to request a decision or confirm completion.",
                        active.id, active.status
                    ))
                } else if matches!(&command, Command::Revise(c) if c.review.is_some() || c.thread.is_some())
                {
                    Some("To continue this paused run, use approve TASK or revise TASK with feedback. Review/thread selectors apply when starting a new revision.".into())
                } else if active.input["pull_request"]["head"]["ref"] != pr["head"]["ref"]
                    || active.input["pull_request"]["base"]["ref"] != pr["base"]["ref"]
                {
                    Some("The PR branch or target changed while this run was paused. Restore its original branch/target before continuing.".into())
                } else {
                    let receipt: Value = serde_json::from_slice(body)?;
                    let input = json!({"issue":parent,"comment":receipt["comment"],"sender":receipt["sender"],
                        "donkeyspace_human_decision":command.decision(),
                        "donkeyspace_engagement":{"actor":receipt["sender"],"matched_selector":authorization.matched_selector,"reason":authorization.reason}});
                    match pr_revisions::resume(
                        pool,
                        pr_revisions::ResumeRequest {
                            repository,
                            pr: pr_number,
                            job: &active,
                            head: pr["head"]["sha"].as_str().unwrap_or_default(),
                            delivery,
                            input: &input,
                            receipt: &receipt,
                        },
                    )
                    .await?
                    {
                        pr_revisions::ResumeAdmission::Resumed(job) => {
                            return Ok(WebhookPersistOutcome::Queued(job));
                        }
                        pr_revisions::ResumeAdmission::Duplicate => {
                            return Ok(WebhookPersistOutcome::Duplicate);
                        }
                        pr_revisions::ResumeAdmission::Rejected(reason) => Some(reason),
                    }
                }
            } else if let Command::Revise(command) = command {
                if let Some(pending) =
                    pr_revisions::pending_publication(pool, repository, pr_number).await?
                {
                    let head = pr["head"]["sha"].as_str().ok_or("PR head is missing")?;
                    if head == pending.publication_sha
                        || github
                            .commit_is_ancestor(owner, repo, &pending.publication_sha, head)
                            .await?
                    {
                        pr_revisions::mark_publication(
                            pool,
                            pending.job_id,
                            &pending.publication_sha,
                            true,
                        )
                        .await?;
                        source = pr_revisions::source(pool, repository, pr_number)
                            .await?
                            .ok_or("Recovered PR continuation is unavailable")?;
                    }
                }
                match prepare_request(
                    state, &github, &source, &payload, body, &parent, &pr, command,
                )
                .await
                {
                    Ok((mut input, role)) => {
                        input["pr_revision"]["authorization"] = json!({"gate":EngagementGate::NeedsHumanResume.as_str(),"matched_selector":authorization.matched_selector,"reason":authorization.reason});
                        match pr_revisions::enqueue(pool, repository, pr_number, comment.id.unwrap(), &source, &role, &input).await? {
                            Admission::Queued(job) => { outcome = WebhookPersistOutcome::Queued(job); None }
                            Admission::Duplicate => return Ok(WebhookPersistOutcome::Duplicate),
                            Admission::Unavailable => Some("The workflow changed while this request was checked, or other work is active on the parent issue. Check the latest progress there and submit a new command when eligible.".into()),
                        }
                    }
                    Err(error) => Some(error),
                }
            } else {
                Some("There is no paused revision to approve. To request new work, post revise TASK with feedback on the following lines.".into())
            }
        }
    };
    record_authorization(
        pool,
        repository,
        source.workflow_item_id,
        delivery,
        event,
        body,
        &payload,
        authorization,
        rejection.as_deref(),
    )
    .await?;
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
    rejection: Option<&str>,
) -> Result<bool, Box<dyn std::error::Error>> {
    let mut tx = pool.begin().await?;
    let Some(id) = record_webhook_delivery(
        &mut *tx,
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
        &mut *tx,
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
    if let Some(reason) = rejection {
        let marker = format!(
            "<!-- donkeyspace-pr-command:{} -->",
            payload
                .comment
                .as_ref()
                .and_then(|c| c.id)
                .unwrap_or_default()
        );
        create_outbound_action(&mut *tx, &OutboundActionInput {
            workflow_item_id:workflow,job_id:None,provider:"github".into(),action_type:"issue.upsert_comment".into(),
            payload:json!({"owner":payload.repository.owner.login,"repo":payload.repository.name,"issue_number":payload.issue.number,"marker":marker,
                "body":format!("**Command not accepted**\n\n{reason}\n\n{marker}\n<!-- donkeyspace-generated -->")}),
        }).await?;
    }
    tx.commit().await?;
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
    command: donkeyspace_core::pr_revision::RevisionCommand,
) -> Result<(Value, String), String> {
    let owner = &payload.repository.owner.login;
    let repo = &payload.repository.name;
    let pr_number = payload.issue.number;
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
        source.checkpoint.as_ref().ok_or("This PR has no retained lifecycle checkpoint; start a new managed workflow or update the PR manually.")?;
        if !flow.change_intake {
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
    fn pr_decisions_use_configured_prefix_and_reject_malformed_commands() {
        for (body, decision) in [
            (
                "/studio approve build/queue",
                json!({"action":"approve","target":"build/queue"}),
            ),
            (
                "/studio approve all",
                json!({"action":"approve","target":"all"}),
            ),
            (
                "/studio revise plan\nChange the contract.",
                json!({"action":"revise","target":"plan","feedback":"Change the contract."}),
            ),
        ] {
            assert_eq!(
                parse_pr_command(body, "studio")
                    .unwrap()
                    .unwrap()
                    .decision(),
                decision
            );
        }
        for body in [
            "/studio approve a b",
            "/studio revise plan",
            "/studio approve review=1",
        ] {
            assert!(parse_pr_command(body, "studio").is_err());
        }
        for body in [
            "/other approve plan",
            "Please approve",
            "A quote:\n/studio approve plan",
        ] {
            assert!(parse_pr_command(body, "studio").unwrap().is_none());
        }
    }

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
