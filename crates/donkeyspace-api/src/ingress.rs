//! GitHub ingress preparation, authorization and atomic admission.
use super::*;

pub(super) async fn persist_github_webhook(
    pool: &PgPool,
    state: &AppState,
    event: &str,
    delivery: &str,
    body: &[u8],
) -> Result<WebhookPersistOutcome, Box<dyn std::error::Error>> {
    match event {
        "issues" | "issue_comment" => {
            persist_issue_webhook(pool, state, event, delivery, body).await
        }
        "pull_request" => {
            persist_pull_request_webhook(pool, &state.policy, event, delivery, body).await
        }
        "push" => persist_push_webhook(pool, &state.policy, event, delivery, body).await,
        _ => {
            let payload: Value = serde_json::from_slice(body)?;
            let inserted = record_webhook_delivery(pool, None, delivery, event, &payload).await?;
            Ok(if inserted.is_some() {
                WebhookPersistOutcome::Ignored
            } else {
                WebhookPersistOutcome::Duplicate
            })
        }
    }
}

pub(super) async fn persist_issue_webhook(
    pool: &PgPool,
    app_state: &AppState,
    event: &str,
    delivery: &str,
    body: &[u8],
) -> Result<WebhookPersistOutcome, Box<dyn std::error::Error>> {
    let policy = &app_state.policy;
    let payload: GitHubIssueWebhook = serde_json::from_slice(body)?;
    let mut payload_value: Value = serde_json::from_slice(body)?;
    let repository_id = upsert_repository(
        pool,
        &RepositoryInput {
            installation_external_id: payload
                .installation
                .as_ref()
                .map(|value| value.id.to_string()),
            installation_account_login: payload
                .installation
                .as_ref()
                .map(|_| payload.repository.owner.login.clone()),
            provider: "github".to_string(),
            owner: payload.repository.owner.login.clone(),
            name: payload.repository.name.clone(),
            default_branch: payload.repository.default_branch.clone(),
        },
    )
    .await?;

    if webhook_delivery_exists(pool, delivery).await? {
        return Ok(WebhookPersistOutcome::Duplicate);
    }

    let labels = payload
        .issue
        .labels
        .iter()
        .map(|label| label.name.clone())
        .collect::<Vec<_>>();
    let label_state = normalize_workflow_labels(&labels, &policy.workflow.state_labels);
    let label_state_name = match &label_state {
        LabelState::None => None,
        LabelState::One(label) => Some(label.state.to_string()),
        LabelState::Conflict(_) => Some(WorkflowState::NeedsHuman.to_string()),
    };
    let previous_state =
        get_workflow_item_state(pool, repository_id, &payload.issue.id.to_string()).await?;
    let was_finished = previous_state.as_deref() == Some("finished");
    let current_state = label_state_name.or(previous_state);
    // GitHub issue timestamps have second precision. Verify lifecycle edges
    // against GitHub so a delayed event from the same second cannot close a
    // reopened workflow or reopen an issue that is currently closed.
    if (matches!(payload.action.as_str(), "opened" | "closed" | "reopened")
        || payload.issue.state == "closed"
        || was_finished)
        && let Some(auth) = &app_state.github_auth
    {
        let currently_closed = auth
            .client()
            .issue_is_closed(
                &payload.repository.owner.login,
                &payload.repository.name,
                payload.issue.number,
            )
            .await?;
        if currently_closed != (payload.issue.state == "closed") {
            return Ok(WebhookPersistOutcome::Ignored);
        }
    }

    let Some(workflow_item_id) = donkeyspace_db::cancellation::observe_issue(
        pool,
        &donkeyspace_db::cancellation::IssueObservation {
            issue: &WorkflowItemInput {
                repository_id,
                provider_issue_id: payload.issue.id.to_string(),
                issue_number: payload.issue.number,
                provider_state: payload.issue.state.clone(),
                current_state: current_state.clone(),
                current_labels: labels.clone(),
            },
            updated_at: payload.issue.updated_at,
            close_reason: payload.issue.state_reason.as_deref(),
            owner: &payload.repository.owner.login,
            repo: &payload.repository.name,
            state_labels: policy.workflow.state_labels.values().cloned().collect(),
        },
    )
    .await?
    else {
        return Ok(WebhookPersistOutcome::Ignored);
    };
    let admission = donkeyspace_db::ingress::snapshot(pool, workflow_item_id).await?;
    let prepared_authorization = if payload.issue.state != "closed"
        && !matches!(label_state, LabelState::Conflict(_))
        && should_queue_triage(
            event,
            &payload.action,
            &payload.issue.state,
            admission.current_state.as_deref(),
            payload.comment.as_ref(),
            payload.label.as_ref().map(|label| label.name.as_str()),
            (
                &policy.workflow.allow_labels,
                &policy.facade.resolve().command,
            ),
        ) {
        let gate = engagement_gate(event, admission.current_state.as_deref())
            .ok_or("queueable github event has no engagement gate")?;
        let authorization = authorize_engagement(app_state, gate, &labels, &payload).await;
        if authorization.verification_unavailable {
            return Err(
                std::io::Error::new(std::io::ErrorKind::WouldBlock, authorization.reason).into(),
            );
        }
        Some(authorization)
    } else {
        None
    };
    let mut transaction = pool.begin().await?;
    let outcome: Result<_, Box<dyn std::error::Error>> = async {
        let connection = &mut *transaction;
        donkeyspace_db::ingress::lock_snapshot(connection, workflow_item_id, &admission).await?;
    // Closure is convergent and committed before delivery deduplication, so a
    // failed persistence attempt can be retried without losing cancellation.
    let inserted =
        record_webhook_delivery(&mut *connection, Some(repository_id), delivery, event, &payload_value).await?;
    let Some(webhook_delivery_id) = inserted else {
        return Ok(WebhookPersistOutcome::Duplicate);
    };

    let current_state =
        get_workflow_item_state(&mut *connection, repository_id, &payload.issue.id.to_string()).await?;
    if payload.issue.state == "closed" {
        return Ok(WebhookPersistOutcome::Ignored);
    }

    if matches!(label_state, LabelState::Conflict(_)) {
        donkeyspace_db::record_state_transition_on(
            &mut *connection,
            workflow_item_id,
            None,
            None,
            WorkflowState::NeedsHuman.as_str(),
            "conflicting ai workflow labels detected",
        )
        .await?;
        return Ok(WebhookPersistOutcome::Ignored);
    }

    let facade_command = policy.facade.resolve().command;
    if !should_queue_triage(
        event,
        &payload.action,
        &payload.issue.state,
        current_state.as_deref(),
        payload.comment.as_ref(),
        payload.label.as_ref().map(|label| label.name.as_str()),
        (&policy.workflow.allow_labels, &facade_command),
    ) {
        tracing::info!(
            event,
            action = payload.action,
            issue_number = payload.issue.number,
            current_state = current_state.as_deref().unwrap_or("none"),
            "webhook did not queue triage"
        );
        return Ok(WebhookPersistOutcome::Ignored);
    }

    let human_approval = if current_state.as_deref() == Some("needs_human") {
        payload.comment.as_ref().and_then(|comment| {
            parse_human_approval_command(&comment.body, &policy.facade.resolve().command)
        })
    } else {
        None
    };

    let gate = engagement_gate(event, current_state.as_deref())
        .ok_or("queueable github event has no engagement gate")?;
    let managed_resource = if let Some(comment) = &payload.comment {
        let registered = match comment.id {
            Some(comment_id) => {
                github_managed_resource_exists(
                    &mut *connection,
                    repository_id,
                    "issue_comment",
                    &comment_id.to_string(),
                )
                .await?
            }
            None => false,
        };
        let pending = match payload_value
            .pointer("/comment/body")
            .and_then(Value::as_str)
        {
            Some(body) => pending_outbound_comment_exists(&mut *connection, workflow_item_id, body).await?,
            None => false,
        };
        registered || pending
    } else {
        let created_by_this_app = app_state
            .github_auth
            .as_ref()
            .and_then(GitHubCredentialProvider::app_id)
            .zip(payload.issue.performed_via_github_app.as_ref())
            .is_some_and(|(configured, actual)| configured == actual.id);
        (is_projected_work_item(&payload.issue.body) && created_by_this_app)
            || github_managed_resource_exists(
                &mut *connection,
                repository_id,
                "issue",
                &payload.issue.id.to_string(),
            )
            .await?
    };
    if managed_resource {
        record_engagement_decision(
            &mut *connection,
            &EngagementDecisionInput {
                webhook_delivery_id,
                workflow_item_id: Some(workflow_item_id),
                gate: gate.as_str().into(),
                disposition: "system_generated".into(),
                actor: payload
                    .sender
                    .as_ref()
                    .and_then(|actor| serde_json::to_value(actor).ok()),
                matched_selector: None,
                reason: "platform-managed GitHub resource cannot trigger agent work".into(),
            },
        )
        .await?;
        return Ok(WebhookPersistOutcome::Ignored);
    }

    let automation_decision = policy.automation_decision_for_labels(&labels);
    if !automation_decision.is_allowed() {
        record_engagement_decision(
            &mut *connection,
            &EngagementDecisionInput {
                webhook_delivery_id,
                workflow_item_id: Some(workflow_item_id),
                gate: gate.as_str().into(),
                disposition: "denied".into(),
                actor: payload
                    .sender
                    .as_ref()
                    .and_then(|actor| serde_json::to_value(actor).ok()),
                matched_selector: None,
                reason: automation_decision.reason(),
            },
        )
        .await?;
        return Ok(WebhookPersistOutcome::Ignored);
    }

    let authorization = prepared_authorization.ok_or("queueable event was not authorized during preparation")?;
    let audit = record_engagement_decision(
        &mut *connection,
        &EngagementDecisionInput {
            webhook_delivery_id,
            workflow_item_id: Some(workflow_item_id),
            gate: gate.as_str().into(),
            disposition: if authorization.allowed {
                "allowed"
            } else {
                "denied"
            }
            .into(),
            actor: payload
                .sender
                .as_ref()
                .and_then(|actor| serde_json::to_value(actor).ok()),
            matched_selector: authorization.matched_selector.clone(),
            reason: authorization.reason.clone(),
        },
    )
    .await?;
    if !authorization.allowed {
        tracing::info!(
            event,
            action = payload.action,
            issue_number = payload.issue.number,
            reason = authorization.reason,
            "engagement authorization denied agent work"
        );
        return Ok(WebhookPersistOutcome::Ignored);
    }
    if let Value::Object(map) = &mut payload_value {
        map.insert(
            "donkeyspace_ingress".into(),
            json!({
                "delivery_id": delivery,
                "source": ingress_source(delivery),
                "event": event,
            }),
        );
        map.insert(
            "donkeyspace_engagement".into(),
            json!({
                "decision_id": audit.id,
                "gate": gate.as_str(),
                "actor": payload.sender,
                "matched_selector": authorization.matched_selector,
                "reason": authorization.reason,
            }),
        );
        if let Some(action) = human_approval.clone() {
            map.insert(
                "donkeyspace_human_decision".into(),
                serde_json::to_value(action).expect("approval action serializes"),
            );
        }
    }

    if policy.lifecycle.plugin.is_some()
        && let Value::Object(map) = &mut payload_value
    {
        map.insert(
            "donkeyspace_lifecycle_coordinator".into(),
            Value::Bool(true),
        );
    }

    if active_job_exists_for_workflow_item(&mut *connection, workflow_item_id).await? {
        tracing::info!(
            event,
            action = payload.action,
            issue_number = payload.issue.number,
            "active workflow job already exists; duplicate trigger ignored"
        );
        return Ok(WebhookPersistOutcome::Ignored);
    }

    if matches!(gate, EngagementGate::NeedsHumanResume | EngagementGate::NeedsInfoResume)
        && let Some(job) = resume_latest_paused_job(&mut *connection, workflow_item_id, &payload_value).await?
    {
        let (event_type, summary) = match human_approval.as_ref() {
            Some(HumanApprovalAction::Approve { target }) => (
                "approval_received",
                format!(
                    "Approval accepted{}.",
                    target
                        .as_deref()
                        .map(|target| format!(" for {target}"))
                        .unwrap_or_default()
                ),
            ),
            Some(HumanApprovalAction::Revise { target, .. }) => (
                "revision_received",
                format!(
                    "Revision requested{}.",
                    target
                        .as_deref()
                        .map(|target| format!(" for {target}"))
                        .unwrap_or_default()
                ),
            ),
            None => (
                "workflow_resumed",
                "Authorized feedback resumed the workflow.".into(),
            ),
        };
        record_ingress_lifecycle_event(
            &mut *connection,
            workflow_item_id,
            Some(job.id),
            delivery,
            event_type,
            &summary,
            payload.sender.as_ref().map(|sender| sender.login.as_str()),
        )
        .await?;
        donkeyspace_db::record_state_transition_on(
            &mut *connection,
            workflow_item_id,
            Some(job.id),
            current_state.as_deref(),
            "lifecycle_resumed",
            &format!(
                "resumed paused plugin lifecycle from authorized github event; engagement decision {}",
                audit.id
            ),
        )
        .await?;
        return Ok(WebhookPersistOutcome::Queued(Box::new(job)));
    }

    let lifecycle_role = lifecycle_start_role(policy)?;
    let initial_role = lifecycle_role.unwrap_or_else(|| AgentRole::Triage.as_str().to_string());
    let job = create_job(&mut *connection, Some(workflow_item_id), &initial_role, &payload_value).await?;
    record_ingress_lifecycle_event(
        &mut *connection,
        workflow_item_id,
        Some(job.id),
        delivery,
        "issue_received",
        "Issue accepted for agent work.",
        payload.sender.as_ref().map(|sender| sender.login.as_str()),
    )
    .await?;
    donkeyspace_db::record_state_transition_on(
        &mut *connection,
        workflow_item_id,
        Some(job.id),
        current_state.as_deref(),
        &format!("{initial_role}_queued"),
        &format!(
            "queued {initial_role} job from github webhook; engagement decision {}",
            audit.id
        ),
    )
    .await?;

    Ok(WebhookPersistOutcome::Queued(Box::new(job)))
    }.await;
    let outcome = outcome?;
    transaction.commit().await?;
    Ok(outcome)
}

pub(super) fn ingress_source(delivery: &str) -> &'static str {
    if delivery.starts_with("github-poll:") {
        "poll"
    } else {
        "webhook"
    }
}

pub(super) async fn record_ingress_lifecycle_event(
    pool: &mut donkeyspace_db::PgConnection,
    workflow_item_id: i64,
    coordinator_job_id: Option<Uuid>,
    delivery: &str,
    event_type: &str,
    summary: &str,
    actor: Option<&str>,
) -> Result<(), donkeyspace_db::DbError> {
    donkeyspace_db::record_lifecycle_event_on(
        pool,
        &LifecycleEventInput {
            workflow_item_id,
            coordinator_job_id,
            job_id: coordinator_job_id,
            dedupe_key: Some(format!("ingress:{delivery}:{event_type}")),
            event_type: event_type.into(),
            level: "milestone".into(),
            source: ingress_source(delivery).into(),
            actor: actor.map(Into::into),
            wave: None,
            attempt: None,
            role: None,
            role_display_name: None,
            task: None,
            task_display_name: None,
            work_item: None,
            status: None,
            outcome: None,
            summary: summary.into(),
            reason: None,
            handoff_target: None,
            links: json!([]),
        },
    )
    .await?;
    Ok(())
}

pub(super) fn is_projected_work_item(body: &str) -> bool {
    body.contains("<!-- donkeyspace-work-item -->")
}

pub(super) async fn persist_pull_request_webhook(
    pool: &PgPool,
    policy: &Policy,
    event: &str,
    delivery: &str,
    body: &[u8],
) -> Result<WebhookPersistOutcome, Box<dyn std::error::Error>> {
    let payload: GitHubPullRequestWebhook = serde_json::from_slice(body)?;
    let payload_value: Value = serde_json::from_slice(body)?;
    let repository_id = upsert_repository(
        pool,
        &RepositoryInput {
            installation_external_id: payload
                .installation
                .as_ref()
                .map(|value| value.id.to_string()),
            installation_account_login: payload
                .installation
                .as_ref()
                .map(|_| payload.repository.owner.login.clone()),
            provider: "github".to_string(),
            owner: payload.repository.owner.login.clone(),
            name: payload.repository.name.clone(),
            default_branch: payload.repository.default_branch.clone(),
        },
    )
    .await?;

    let mut transaction = pool.begin().await?;
    let outcome: Result<_, Box<dyn std::error::Error>> = async {
        let connection = &mut *transaction;
        let inserted = record_webhook_delivery(
            &mut *connection,
            Some(repository_id),
            delivery,
            event,
            &payload_value,
        )
        .await?;
        if inserted.is_none() {
            return Ok(WebhookPersistOutcome::Duplicate);
        }

        let branch_prefix = &policy.facade.resolve().branch_prefix;
        let managed = pull_request_is_managed(&payload.pull_request, branch_prefix);
        let linked_issue_number = payload
            .pull_request
            .body
            .as_deref()
            .and_then(extract_linked_issue_number)
            .or_else(|| {
                issue_number_from_managed_branch(&payload.pull_request.head.ref_name, branch_prefix)
            });
        let workflow_item = match linked_issue_number {
            Some(issue_number) => {
                get_workflow_item_by_issue_number(&mut *connection, repository_id, issue_number)
                    .await?
            }
            None => None,
        };

        let pull_request_state =
            normalized_pull_request_state(&payload.pull_request.state, payload.pull_request.merged);
        upsert_pull_request(
            &mut *connection,
            &PullRequestInput {
                repository_id,
                workflow_item_id: workflow_item.as_ref().map(|item| item.id),
                provider_pr_id: payload.pull_request.id.to_string(),
                pr_number: payload.pull_request.number,
                title: payload.pull_request.title.clone(),
                html_url: payload.pull_request.html_url.clone(),
                state: pull_request_state.into(),
                head_ref: payload.pull_request.head.ref_name.clone(),
                head_sha: Some(payload.pull_request.head.sha.clone()),
                base_ref: payload.pull_request.base.ref_name.clone(),
                base_sha: Some(payload.pull_request.base.sha.clone()),
                managed_by_donkeyspace: managed,
            },
        )
        .await?;

        let Some(workflow_item) = workflow_item else {
            tracing::info!(
                action = payload.action,
                pr_number = payload.pull_request.number,
                "pull request webhook did not match a known workflow item"
            );
            return Ok(WebhookPersistOutcome::Ignored);
        };
        let linked_issue_number = linked_issue_number
            .expect("a matched pull request workflow item has a linked issue number");
        let workflow_state = if managed {
            match pull_request_state {
                "open" => Some(WorkflowState::PrOpen.as_str()),
                "merged" => Some("pr_merged"),
                "closed" => Some("pr_closed"),
                _ => None,
            }
        } else {
            None
        };
        let actions = if managed {
            pull_request_label_actions(
                policy,
                &payload.repository.owner.login,
                &payload.repository.name,
                linked_issue_number,
                pull_request_state,
            )
        } else {
            Vec::new()
        };
        let Some(generation) = donkeyspace_db::cancellation::apply_pull_request_effects_on(
            &mut *connection,
            workflow_item.id,
            &payload.pull_request.id.to_string(),
            workflow_state,
            &actions,
        )
        .await?
        else {
            return Ok(WebhookPersistOutcome::Ignored);
        };

        if policy.lifecycle.plugin.is_some()
            || !should_queue_reviewer(
                &payload.action,
                &payload.pull_request.state,
                payload.pull_request.draft,
                managed,
                policy.agents.reviewer.enabled,
            )
        {
            tracing::info!(
                action = payload.action,
                pr_number = payload.pull_request.number,
                managed,
                "pull request webhook did not queue reviewer"
            );
            return Ok(WebhookPersistOutcome::Ignored);
        }

        if reviewer_job_exists_for_pr_head(
            &mut *connection,
            workflow_item.id,
            payload.pull_request.number,
            Some(&payload.pull_request.head.sha),
        )
        .await?
        {
            tracing::info!(
                pr_number = payload.pull_request.number,
                head_sha = payload.pull_request.head.sha,
                "reviewer job already exists for pull request head"
            );
            return Ok(WebhookPersistOutcome::Ignored);
        }

        let Some(mut job_input) =
            latest_workflow_job_input(&mut *connection, workflow_item.id).await?
        else {
            tracing::info!(
                pr_number = payload.pull_request.number,
                "pull request webhook found workflow item without reusable job input"
            );
            return Ok(WebhookPersistOutcome::Ignored);
        };
        // Keep the PR origin generation explicit in the queued input.
        job_input["donkeyspace_workflow_generation"] = json!(generation);
        attach_pull_request_input(&mut job_input, payload_value["pull_request"].clone());

        let job = create_job(
            &mut *connection,
            Some(workflow_item.id),
            AgentRole::Reviewer.as_str(),
            &job_input,
        )
        .await?;
        donkeyspace_db::record_state_transition_on(
            &mut *connection,
            workflow_item.id,
            Some(job.id),
            workflow_item.current_state.as_deref(),
            "reviewer_queued",
            "queued reviewer job from pull request webhook",
        )
        .await?;

        Ok(WebhookPersistOutcome::Queued(Box::new(job)))
    }
    .await;
    let outcome = outcome?;
    transaction.commit().await?;
    Ok(outcome)
}

pub(super) async fn persist_push_webhook(
    pool: &PgPool,
    policy: &Policy,
    event: &str,
    delivery: &str,
    body: &[u8],
) -> Result<WebhookPersistOutcome, Box<dyn std::error::Error>> {
    let payload: GitHubPushWebhook = serde_json::from_slice(body)?;
    let payload_value: Value = serde_json::from_slice(body)?;
    let repository_id = upsert_repository(
        pool,
        &RepositoryInput {
            installation_external_id: payload
                .installation
                .as_ref()
                .map(|value| value.id.to_string()),
            installation_account_login: payload
                .installation
                .as_ref()
                .map(|_| payload.repository.owner.login.clone()),
            provider: "github".to_string(),
            owner: payload.repository.owner.login,
            name: payload.repository.name,
            default_branch: payload.repository.default_branch.clone(),
        },
    )
    .await?;

    let mut transaction = pool.begin().await?;
    let outcome: Result<_, Box<dyn std::error::Error>> = async {
        let connection = &mut *transaction;
        let inserted = record_webhook_delivery(
            &mut *connection,
            Some(repository_id),
            delivery,
            event,
            &payload_value,
        )
        .await?;
        if inserted.is_none() {
            return Ok(WebhookPersistOutcome::Duplicate);
        }
        if policy.lifecycle.plugin.is_some() {
            return Ok(WebhookPersistOutcome::Ignored);
        }

        let Some(branch) = payload.git_ref.strip_prefix("refs/heads/") else {
            return Ok(WebhookPersistOutcome::Ignored);
        };
        if branch != payload.repository.default_branch {
            tracing::info!(
                branch,
                default_branch = payload.repository.default_branch,
                "push webhook ignored for non-default branch"
            );
            return Ok(WebhookPersistOutcome::Ignored);
        }

        let mut candidates =
            list_open_managed_pull_requests_for_base(&mut *connection, repository_id, branch)
                .await?;
        candidates.sort_by_key(|pr| pr.workflow_item_id);
        let mut queued = None;

        for pull_request in candidates {
            donkeyspace_db::ingress::lock_generation(
                &mut *connection,
                pull_request.workflow_item_id,
                pull_request.generation,
            )
            .await?;
            if repair_job_exists_for_pr_base(
                &mut *connection,
                pull_request.workflow_item_id,
                pull_request.pr_number,
                pull_request.head_sha.as_deref(),
                Some(&payload.after),
            )
            .await?
            {
                continue;
            }

            let Some(mut job_input) =
                latest_workflow_job_input(&mut *connection, pull_request.workflow_item_id).await?
            else {
                continue;
            };
            job_input["donkeyspace_workflow_generation"] = json!(pull_request.generation);
            attach_pull_request_input(
                &mut job_input,
                json!({
                    "number": pull_request.pr_number,
                    "title": pull_request.title,
                    "body": null,
                    "html_url": pull_request.html_url,
                    "state": pull_request.state,
                    "draft": false,
                    "head": {
                        "ref": pull_request.head_ref,
                        "sha": pull_request.head_sha,
                    },
                    "base": {
                        "ref": pull_request.base_ref,
                        "sha": payload.after,
                    },
                }),
            );

            let job = create_job(
                &mut *connection,
                Some(pull_request.workflow_item_id),
                AgentRole::Repair.as_str(),
                &job_input,
            )
            .await?;
            donkeyspace_db::record_state_transition_on(
                &mut *connection,
                pull_request.workflow_item_id,
                Some(job.id),
                Some(WorkflowState::PrOpen.as_str()),
                "repair_queued",
                "queued repair check after base branch push",
            )
            .await?;
            queued.get_or_insert(job);
        }

        Ok(queued
            .map(|job| WebhookPersistOutcome::Queued(Box::new(job)))
            .unwrap_or(WebhookPersistOutcome::Ignored))
    }
    .await;
    let outcome = outcome?;
    transaction.commit().await?;
    Ok(outcome)
}

pub(super) fn should_queue_triage(
    event: &str,
    action: &str,
    issue_state: &str,
    current_state: Option<&str>,
    comment: Option<&GitHubComment>,
    changed_label: Option<&str>,
    workflow: (&[String], &str),
) -> bool {
    let (allow_labels, facade_command) = workflow;
    if issue_state == "closed" {
        return false;
    }

    if current_state == Some("needs_human") {
        return event == "issue_comment"
            && action == "created"
            && comment.is_some_and(|comment| {
                parse_human_approval_command(&comment.body, facade_command).is_some()
            });
    }

    match (event, action) {
        ("issues", "opened" | "edited" | "reopened") => true,
        ("issues", "labeled") => {
            changed_label
                .map(|label| allow_labels.iter().any(|allowed| allowed == label))
                .unwrap_or(false)
                && matches!(current_state, None | Some("needs_info" | "blocked"))
        }
        ("issue_comment", "created" | "edited") => {
            matches!(
                current_state,
                Some(state) if matches!(state, "needs_info" | "blocked")
            ) && comment.is_some()
        }
        _ => false,
    }
}

pub(super) fn parse_human_approval_command(
    body: &str,
    facade_command: &str,
) -> Option<HumanApprovalAction> {
    let mut lines = body.lines();
    let first = lines.find(|line| !line.trim().is_empty())?.trim();
    let mut parts = first.split_whitespace();
    if parts.next()? != format!("/{facade_command}") {
        return None;
    }
    let action = parts.next()?;
    let target = parts.next().map(str::to_string);
    if parts.next().is_some()
        || target
            .as_deref()
            .is_some_and(|value| !valid_approval_target(value))
    {
        return None;
    }
    match action {
        "approve" => Some(HumanApprovalAction::Approve { target }),
        "revise" => {
            let feedback = lines.collect::<Vec<_>>().join("\n").trim().to_string();
            (!feedback.is_empty()).then_some(HumanApprovalAction::Revise { target, feedback })
        }
        _ => None,
    }
}

pub(super) fn valid_approval_target(value: &str) -> bool {
    value == "all"
        || (!value.is_empty()
            && value.matches('/').count() <= 1
            && value.chars().all(|character| {
                character.is_ascii_alphanumeric() || matches!(character, '-' | '_' | '.' | '/')
            }))
}

pub(super) fn engagement_gate(event: &str, current_state: Option<&str>) -> Option<EngagementGate> {
    match current_state {
        Some("needs_info") => Some(EngagementGate::NeedsInfoResume),
        Some("blocked") => Some(EngagementGate::BlockedResume),
        Some("needs_human") => Some(EngagementGate::NeedsHumanResume),
        _ if event == "issues" => Some(EngagementGate::Initial),
        _ => None,
    }
}

#[derive(Debug)]
pub(super) struct AuthorizationDecision {
    pub(super) allowed: bool,
    pub(super) verification_unavailable: bool,
    pub(super) reason: String,
    pub(super) matched_selector: Option<Value>,
}

pub(super) async fn authorize_engagement(
    state: &AppState,
    gate: EngagementGate,
    labels: &[String],
    payload: &GitHubIssueWebhook,
) -> AuthorizationDecision {
    let Some(actor) = payload.sender.as_ref() else {
        return AuthorizationDecision {
            allowed: false,
            verification_unavailable: false,
            reason: "github event is missing sender identity".into(),
            matched_selector: None,
        };
    };
    if actor.login.trim().is_empty() {
        return AuthorizationDecision {
            allowed: false,
            verification_unavailable: false,
            reason: "github event sender identity has no login".into(),
            matched_selector: None,
        };
    }
    if payload
        .comment
        .as_ref()
        .is_some_and(|comment| comment.id.is_none())
    {
        return AuthorizationDecision {
            allowed: false,
            verification_unavailable: false,
            reason: "github comment event is missing comment identity".into(),
            matched_selector: None,
        };
    }
    let repository = format!(
        "{}/{}",
        payload.repository.owner.login, payload.repository.name
    );
    let rule = state
        .policy
        .workflow
        .engagement
        .rule(gate, Some(&repository));
    let missing_labels = rule
        .required_labels
        .iter()
        .filter(|required| !labels.iter().any(|label| label == *required))
        .cloned()
        .collect::<Vec<_>>();
    if !missing_labels.is_empty() {
        return AuthorizationDecision {
            allowed: false,
            verification_unavailable: false,
            reason: format!(
                "missing required engagement labels: {}",
                missing_labels.join(", ")
            ),
            matched_selector: None,
        };
    }

    let (content_actor, content_association) = match payload.comment.as_ref() {
        Some(comment) => (comment.user.as_ref(), comment.author_association.as_deref()),
        None => (
            payload.issue.user.as_ref(),
            payload.issue.author_association.as_deref(),
        ),
    };
    let author_association = if content_actor
        .is_some_and(|content_actor| actor.login.eq_ignore_ascii_case(&content_actor.login))
    {
        content_association
    } else {
        None
    };
    let performed_app = payload
        .comment
        .as_ref()
        .and_then(|comment| comment.performed_via_github_app.as_ref())
        .or(payload.issue.performed_via_github_app.as_ref());
    let mut failures = Vec::new();
    let mut verification_unavailable = false;

    for selector in &rule.allow {
        let result: Result<bool, String> = match selector {
            EngagementSelector::TokenOwner => Ok(state
                .github_token_owner
                .as_ref()
                .map(|login| login.eq_ignore_ascii_case(&actor.login))
                .unwrap_or(false)),
            EngagementSelector::AnyUser => Ok(actor.kind.as_deref() == Some("User")),
            EngagementSelector::User { login } => Ok(
                actor.kind.as_deref() == Some("User") && actor.login.eq_ignore_ascii_case(login)
            ),
            EngagementSelector::IssueAuthor => Ok(payload
                .issue
                .user
                .as_ref()
                .is_some_and(|author| actor.login.eq_ignore_ascii_case(&author.login))),
            EngagementSelector::RepositoryOwner => Ok(payload.repository.owner.kind.as_deref()
                != Some("Organization")
                && actor
                    .login
                    .eq_ignore_ascii_case(&payload.repository.owner.login)),
            EngagementSelector::RepositoryOrganizationMember => {
                if payload.repository.owner.kind.as_deref() != Some("Organization") {
                    Ok(false)
                } else {
                    verify_organization_member(state, &payload.repository.owner.login, &actor.login)
                        .await
                }
            }
            EngagementSelector::OrganizationMember { organization } => {
                verify_organization_member(state, organization, &actor.login).await
            }
            EngagementSelector::TeamMember {
                organization,
                team_slug,
            } => verify_team_member(state, organization, team_slug, &actor.login).await,
            EngagementSelector::AuthorAssociation { association } => {
                Ok(author_association == Some(association.as_str()))
            }
            EngagementSelector::CollaboratorPermission { minimum } => {
                verify_collaborator_permission(
                    state,
                    &payload.repository.owner.login,
                    &payload.repository.name,
                    &actor.login,
                )
                .await
                .map(|actual| permission_rank(&actual) >= permission_rank(minimum))
            }
            EngagementSelector::Bot { login } => {
                Ok(actor.kind.as_deref() == Some("Bot") && actor.login.eq_ignore_ascii_case(login))
            }
            EngagementSelector::GitHubApp { id, slug } => Ok(performed_app
                .map(|app| {
                    id.map(|expected| app.id == expected).unwrap_or(false)
                        || slug
                            .as_ref()
                            .map(|expected| app.slug.eq_ignore_ascii_case(expected))
                            .unwrap_or(false)
                })
                .unwrap_or(false)),
        };

        match result {
            Ok(true) => {
                return AuthorizationDecision {
                    allowed: true,
                    verification_unavailable: false,
                    reason: format!("actor matched engagement selector `{selector:?}`"),
                    matched_selector: serde_json::to_value(selector).ok(),
                };
            }
            Ok(false) => failures.push(format!("`{selector:?}` did not match")),
            Err(error) => {
                verification_unavailable = true;
                failures.push(format!("`{selector:?}` could not be verified: {error}"));
            }
        }
    }

    AuthorizationDecision {
        allowed: false,
        verification_unavailable,
        reason: if failures.is_empty() {
            "engagement rule has no allowed identities".into()
        } else {
            failures.join("; ")
        },
        matched_selector: None,
    }
}

pub(super) async fn verify_organization_member(
    state: &AppState,
    organization: &str,
    actor: &str,
) -> Result<bool, String> {
    let key = format!("org:{organization}:{actor}").to_ascii_lowercase();
    if let Some(value) = verification_cache_get(state, &key).await {
        return Ok(value == "true");
    }
    let result = match &state.github_auth {
        Some(provider) => provider
            .client()
            .organization_member(organization, actor)
            .await
            .map_err(|error| error.to_string()),
        None => Err("github credentials are unavailable".into()),
    }?;
    verification_cache_put(state, key, result.to_string()).await;
    Ok(result)
}

pub(super) async fn verify_team_member(
    state: &AppState,
    organization: &str,
    team_slug: &str,
    actor: &str,
) -> Result<bool, String> {
    let key = format!("team:{organization}:{team_slug}:{actor}").to_ascii_lowercase();
    if let Some(value) = verification_cache_get(state, &key).await {
        return Ok(value == "true");
    }
    let result = match &state.github_auth {
        Some(provider) => provider
            .client()
            .team_member(organization, team_slug, actor)
            .await
            .map_err(|error| error.to_string()),
        None => Err("github credentials are unavailable".into()),
    }?;
    verification_cache_put(state, key, result.to_string()).await;
    Ok(result)
}

pub(super) async fn verify_collaborator_permission(
    state: &AppState,
    owner: &str,
    repo: &str,
    actor: &str,
) -> Result<String, String> {
    let key = format!("permission:{owner}:{repo}:{actor}").to_ascii_lowercase();
    if let Some(value) = verification_cache_get(state, &key).await {
        return Ok(value);
    }
    let result = match &state.github_auth {
        Some(provider) => provider
            .client()
            .collaborator_permission(owner, repo, actor)
            .await
            .map_err(|error| error.to_string()),
        None => Err("github credentials are unavailable".into()),
    }?;
    verification_cache_put(state, key, result.clone()).await;
    Ok(result)
}

pub(super) async fn verification_cache_get(state: &AppState, key: &str) -> Option<String> {
    let cache = state.verification_cache.lock().await;
    cache.get(key).and_then(|(created_at, value)| {
        (created_at.elapsed() < Duration::from_secs(300)).then(|| value.clone())
    })
}

pub(super) async fn verification_cache_put(state: &AppState, key: String, value: String) {
    let mut cache = state.verification_cache.lock().await;
    if cache.len() >= 1_024 {
        cache.retain(|_, (created_at, _)| created_at.elapsed() < Duration::from_secs(300));
        if cache.len() >= 1_024
            && let Some(oldest) = cache
                .iter()
                .min_by_key(|(_, (created_at, _))| *created_at)
                .map(|(key, _)| key.clone())
        {
            cache.remove(&oldest);
        }
    }
    cache.insert(key, (Instant::now(), value));
}

pub(super) fn permission_rank(permission: &str) -> u8 {
    match permission {
        "admin" => 5,
        "maintain" => 4,
        "write" | "push" => 3,
        "triage" => 2,
        "read" | "pull" => 1,
        _ => 0,
    }
}

pub(super) fn should_queue_reviewer(
    action: &str,
    pr_state: &str,
    draft: bool,
    managed: bool,
    reviewer_enabled: bool,
) -> bool {
    reviewer_enabled
        && managed
        && pr_state == "open"
        && !draft
        && matches!(
            action,
            "opened" | "synchronize" | "reopened" | "ready_for_review"
        )
}
