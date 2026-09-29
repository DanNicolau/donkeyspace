//! Read model for current blockers, shared by the API and GitHub projection.
//! A waiting reservation is not a new result; checkpoint targets decide which
//! completed attempts still require a human response.
use crate::{AgentPublicationRecord, JobRecord};
use donkeyspace_core::{Facade, TaskKey};
use serde::Serialize;
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use uuid::Uuid;

#[derive(Debug, Serialize)]
pub struct Blocker {
    pub job_id: Uuid,
    pub task: Option<String>,
    pub work_item: Option<String>,
    pub outcome: String,
    pub reason: Option<String>,
    pub questions: Vec<String>,
    pub action: String,
    pub response_command: Option<String>,
    pub evidence: SupportingEvidence,
}

#[derive(Debug, Serialize)]
pub struct SupportingEvidence {
    pub state: &'static str,
    pub message: &'static str,
    pub files: Vec<SupportingFile>,
}

#[derive(Debug, Serialize)]
pub struct SupportingFile {
    pub path: String,
    pub url: String,
}

impl Blocker {
    pub fn label(&self) -> String {
        match (&self.task, &self.work_item) {
            (Some(task), Some(item)) => format!("{task}/{item}"),
            (Some(task), None) => task.clone(),
            _ => "Workflow".into(),
        }
    }

    pub fn preview(&self) -> String {
        format!(
            "{}: {}",
            self.label(),
            self.questions
                .first()
                .or(self.reason.as_ref())
                .map(String::as_str)
                .unwrap_or(&self.action)
        )
    }
}

#[allow(clippy::too_many_arguments)]
pub fn project(
    state: Option<&str>,
    coordinator: Option<&JobRecord>,
    checkpoint: Option<&Value>,
    jobs: &[JobRecord],
    publications: &[AgentPublicationRecord],
    facade: &Facade,
    file_url: impl Fn(&str, &str) -> String,
) -> Vec<Blocker> {
    if !matches!(state, Some("needs_info" | "needs_human" | "blocked")) {
        return vec![];
    }
    let Some(coordinator) =
        coordinator.filter(|job| matches!(job.status.as_str(), "paused" | "completed" | "failed"))
    else {
        return vec![];
    };
    let targets = checkpoint.and_then(|saved| match state {
        Some("needs_info") => saved
            .get("revision_targets")
            .and_then(Value::as_array)
            .map(|keys| keys.iter().filter_map(task_key).collect::<BTreeSet<_>>()),
        Some("needs_human") => saved
            .get("pending_approvals")
            .and_then(Value::as_array)
            .map(|approvals| {
                approvals
                    .iter()
                    .filter_map(|approval| task_key(&approval["key"]))
                    .collect()
            }),
        _ => None,
    });
    let mut latest = BTreeMap::new();
    let coordinator_id = coordinator.id.to_string();
    for job in jobs {
        if job
            .input
            .pointer("/plugin_execution/coordinator_run_id")
            .and_then(Value::as_str)
            != Some(coordinator_id.as_str())
            || job.result.is_none()
        {
            continue;
        }
        let Some(task) = job
            .input
            .pointer("/plugin_execution/task")
            .and_then(Value::as_str)
        else {
            continue;
        };
        let key = TaskKey {
            task: task.into(),
            work_item: job
                .input
                .pointer("/plugin_execution/work_item/id")
                .and_then(Value::as_str)
                .map(str::to_owned),
        };
        let previous: Option<&&JobRecord> = latest.get(&key);
        if previous
            .is_none_or(|previous| (job.created_at, job.id) > (previous.created_at, previous.id))
        {
            latest.insert(key, job);
        }
    }
    let mut blockers = Vec::new();
    for (key, job) in latest {
        if matches!(job.status.as_str(), "superseded" | "cancelled")
            || targets
                .as_ref()
                .is_some_and(|targets| !targets.contains(&key))
        {
            continue;
        }
        if let Some(blocker) = from_result(
            job,
            Some(&key),
            state,
            publications,
            None,
            facade,
            &file_url,
        ) {
            blockers.push(blocker);
        }
    }
    // Recovery or a rejected command can pause the coordinator with a new
    // reason without rewriting the accepted checkpoint. Do not hide that reason
    // behind an older task's outstanding questions.
    let coordinator_changed = checkpoint
        .filter(|saved| saved.get("last_result").is_some_and(Value::is_object))
        .is_some_and(|saved| {
            coordinator.result.as_ref().is_some_and(|result| {
                [
                    "outcome",
                    "human_review_reason",
                    "blocked_reason",
                    "questions",
                ]
                .iter()
                .any(|field| result.get(field) != saved["last_result"].get(field))
            })
        });
    let approval_only = !coordinator_changed
        && checkpoint.is_some_and(|saved| {
            saved
                .get("pending_approvals")
                .and_then(Value::as_array)
                .is_some_and(|pending| {
                    !pending.is_empty()
                        && pending.iter().all(|approval| {
                            approval.get("trigger").and_then(Value::as_str) == Some("required")
                        })
                })
        });
    // Successful results awaiting their configured review already have approval
    // cards. Reserve blockers for questions, agent requests and recovery errors.
    if blockers.is_empty() && approval_only {
        return blockers;
    }
    if blockers.is_empty() || coordinator_changed {
        // Planning executes on the coordinator itself. Recover its identity
        // from the saved task record, never from a hard-coded role name.
        let planning = checkpoint
            .filter(|_| !coordinator_changed)
            .filter(|saved| saved.get("start_approved").and_then(Value::as_bool) == Some(false));
        let proposal = planning.and_then(|saved| {
            saved
                .get("previous")?
                .as_array()?
                .iter()
                .rev()
                .find(|entry| {
                    if entry.get("superseded").and_then(Value::as_bool) == Some(true) {
                        return false;
                    }
                    task_key(entry).is_some_and(|key| key.work_item.is_none())
                })
        });
        let key = proposal.and_then(|entry| task_key(entry)).or_else(|| {
            (coordinator
                .input
                .get("donkeyspace_lifecycle_coordinator")
                .and_then(Value::as_bool)
                != Some(true))
            .then(|| TaskKey {
                task: coordinator.role.clone(),
                work_item: None,
            })
        });
        let attempt = planning.and_then(|saved| saved.get("attempt")?.as_i64());
        let mut source = coordinator.clone();
        if let (Some(saved), Some(proposal), Some(result)) =
            (planning, proposal, &coordinator.result)
            && ["human_review_reason", "blocked_reason", "questions"]
                .iter()
                .all(|field| result.get(field) == saved["last_result"].get(field))
            && matches!(
                proposal.get("outcome").and_then(Value::as_str),
                Some("needs_info" | "needs_human" | "blocked" | "failed")
            )
        {
            source.result = Some(proposal.clone());
        }
        if let Some(blocker) = from_result(
            &source,
            key.as_ref(),
            state,
            publications,
            attempt,
            facade,
            &file_url,
        ) {
            blockers.insert(0, blocker);
        }
    }
    blockers
}

fn task_key(value: &Value) -> Option<TaskKey> {
    Some(TaskKey {
        task: value.get("task")?.as_str()?.into(),
        work_item: value
            .get("work_item")
            .and_then(Value::as_str)
            .map(str::to_owned),
    })
}

fn from_result(
    job: &JobRecord,
    key: Option<&TaskKey>,
    state: Option<&str>,
    publications: &[AgentPublicationRecord],
    attempt: Option<i64>,
    facade: &Facade,
    file_url: &impl Fn(&str, &str) -> String,
) -> Option<Blocker> {
    let result = job.result.as_ref()?;
    let outcome = result.get("outcome")?.as_str()?;
    if !matches!(outcome, "needs_info" | "needs_human" | "blocked" | "failed") {
        return None;
    }
    let questions = result
        .get("questions")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .filter(|text| !text.trim().is_empty())
        .map(str::to_owned)
        .collect();
    let reason = result
        .get("blocked_reason")
        .and_then(Value::as_str)
        .or_else(|| result.get("human_review_reason").and_then(Value::as_str))
        .or_else(|| result.get("summary").and_then(Value::as_str))
        .map(str::to_owned);
    let action = match outcome {
        "needs_info" if state == Some("needs_human") => "Answer these questions through the relevant revision control on the parent issue. Put your answers after the revision command. Other pending approvals remain required.".into(),
        "needs_info" => "Reply on the parent issue with answers to these questions.".into(),
        "needs_human" if result.get("blocked_reason").and_then(Value::as_str).is_some() => "Resolve the reported problem before resuming the workflow.".into(),
        "needs_human" => "Review the reason and supporting files, then use the approval or revision controls on the parent issue.".into(),
        _ => "Resolve the reported problem before retrying the workflow.".into(),
    };
    let response_command = (outcome == "needs_info" && state == Some("needs_human"))
        .then(|| key.map(|key| key.approval_commands(facade).revise))
        .flatten();
    let coordinator_id = job
        .input
        .pointer("/plugin_execution/coordinator_run_id")
        .and_then(Value::as_str)
        .and_then(|id| Uuid::parse_str(id).ok())
        .unwrap_or(job.id);
    let publication = publications
        .iter()
        .filter(|publication| {
            (key.is_some()
                || job
                    .input
                    .get("donkeyspace_lifecycle_coordinator")
                    .and_then(Value::as_bool)
                    != Some(true))
                && publication.coordinator_job_id == coordinator_id
                && publication.job_id == Some(job.id)
                && publication.kind != "checkpoint"
                && key.is_none_or(|key| {
                    publication.task.as_deref() == Some(&key.task)
                        && publication.work_item == key.work_item
                })
                && attempt.is_none_or(|attempt| publication.attempt.map(i64::from) == Some(attempt))
        })
        .max_by_key(|publication| publication.id);
    Some(Blocker {
        job_id: job.id,
        task: key.map(|key| key.task.clone()),
        work_item: key.and_then(|key| key.work_item.clone()),
        outcome: outcome.into(),
        reason,
        questions,
        action,
        response_command,
        evidence: evidence(publication, file_url),
    })
}

fn evidence(
    publication: Option<&AgentPublicationRecord>,
    file_url: &impl Fn(&str, &str) -> String,
) -> SupportingEvidence {
    let Some(publication) = publication else {
        return SupportingEvidence {
            state: "unavailable",
            message: "Draft availability unknown: no attempt publication is recorded.",
            files: vec![],
        };
    };
    let files = publication
        .metadata
        .get("supporting_files")
        .and_then(Value::as_array);
    let (state, message) = match publication.status.as_str() {
        "pending" | "publishing" => (
            "pending",
            "Draft publication pending. Questions can be answered while publication is pending.",
        ),
        "failed" => (
            "failed",
            "Draft publication failed. Retry publication to make supporting files available.",
        ),
        "published" if files.is_some_and(Vec::is_empty) => (
            "none",
            "No draft produced: this attempt contains no supporting files in its write scope.",
        ),
        "published" if files.is_some() => (
            "published",
            "Supporting files at the exact attempt revision; these may include unchanged drafts.",
        ),
        _ => (
            "unavailable",
            "Draft availability unknown: supporting-file evidence is unavailable for this attempt.",
        ),
    };
    SupportingEvidence {
        state,
        message,
        files: if state == "published" {
            files
                .into_iter()
                .flatten()
                .filter_map(Value::as_str)
                .map(|path| SupportingFile {
                    path: path.into(),
                    url: file_url(&publication.commit_sha, path),
                })
                .collect()
        } else {
            vec![]
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn job(coordinator: Option<Uuid>, item: &str, outcome: &str) -> JobRecord {
        let now = chrono::Utc::now();
        JobRecord {
            id: Uuid::now_v7(), workflow_item_id: None, retry_of_job_id: None,
            role: "reviewer".into(), status: if coordinator.is_some() { "completed" } else { "paused" }.into(),
            lease_owner: None, lease_expires_at: None, created_at: now, updated_at: now,
            input: coordinator.map_or(json!({"donkeyspace_lifecycle_coordinator":true}), |id| json!({"plugin_execution":{"coordinator_run_id":id,"task":"check","work_item":{"id":item}}})),
            result: Some(json!({"outcome":outcome,"summary":"Current result","questions":if outcome == "needs_info" { vec![format!("First question for {item}?"),format!("Second question for {item}?")] } else { vec![] },"blocked_reason":if outcome == "blocked" { Some("Install the missing tool.") } else { None },"human_review_reason":null})),
        }
    }

    fn publication(coordinator: Uuid, job: &JobRecord) -> AgentPublicationRecord {
        serde_json::from_value(json!({"id":1,"coordinator_job_id":coordinator,"job_id":job.id,
            "kind":"attempt","branch_name":"attempt","commit_sha":"accepted-sha","html_url":"unused",
            "changed_files":[],"task_scopes":[],"zero_diff":false,"local_repo_path":"/unused",
            "task":"check","work_item":job.input.pointer("/plugin_execution/work_item/id"),"attempt":200,
            "status":"published","retry_count":0,"next_attempt_at":job.updated_at,"metadata":{"supporting_files":["docs/proposal.md"]},
            "created_at":job.created_at,"updated_at":job.updated_at})).unwrap()
    }

    fn view(
        state: &str,
        coordinator: &JobRecord,
        checkpoint: Option<&Value>,
        jobs: &[JobRecord],
        publications: &[AgentPublicationRecord],
    ) -> Vec<Blocker> {
        let mut facade = donkeyspace_core::FacadeConfig::default().resolve();
        facade.command = "example".into();
        project(
            Some(state),
            Some(coordinator),
            checkpoint,
            jobs,
            publications,
            &facade,
            |sha, path| format!("https://example.invalid/blob/{sha}/{path}"),
        )
    }

    #[test]
    fn active_targets_preserve_parallel_questions_but_remove_resolved_and_superseded_attempts() {
        let mut coordinator = job(None, "", "needs_human");
        let left = job(Some(coordinator.id), "left", "needs_info");
        let right = job(Some(coordinator.id), "right", "needs_info");
        let mut reservation = job(Some(coordinator.id), "left", "needs_info");
        reservation.status = "waiting".into();
        reservation.result = None;
        let unrelated = job(Some(Uuid::now_v7()), "other-generation", "needs_info");
        let mut checkpoint = json!({"pending_approvals":[{"key":{"task":"check","work_item":"left"}},{"key":{"task":"check","work_item":"right"}}]});
        let mut jobs = vec![left.clone(), right, reservation, unrelated];
        let result = view("needs_human", &coordinator, Some(&checkpoint), &jobs, &[]);
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].questions.len(), 2);
        assert_eq!(
            result[0].response_command.as_deref(),
            Some("/example revise check/left")
        );
        assert!(
            result[0]
                .action
                .contains("Other pending approvals remain required")
        );
        assert_eq!(result[0].evidence.state, "unavailable");
        checkpoint["pending_approvals"]
            .as_array_mut()
            .unwrap()
            .remove(1);
        assert_eq!(
            view("needs_human", &coordinator, Some(&checkpoint), &jobs, &[]).len(),
            1
        );
        jobs[0].status = "superseded".into();
        let remaining = view("needs_human", &coordinator, Some(&checkpoint), &jobs, &[]);
        assert!(remaining.iter().all(|blocker| {
            !blocker
                .questions
                .iter()
                .any(|question| question.contains("left"))
        }));
        for state in ["in_progress", "pr_open", "finished", "cancelled"] {
            assert!(view(state, &coordinator, Some(&checkpoint), &jobs, &[]).is_empty());
        }
        coordinator.status = "running".into();
        assert!(view("needs_human", &coordinator, Some(&checkpoint), &jobs, &[]).is_empty());
        coordinator.status = "paused".into();
        let review = json!({"last_result":coordinator.result,"pending_approvals":[{"key":{"task":"plan","work_item":null},"trigger":"required"}]});
        assert!(view("needs_human", &coordinator, Some(&review), &[], &[]).is_empty());
    }

    #[test]
    fn evidence_states_do_not_hide_questions_or_link_unpublished_or_unrelated_files() {
        let coordinator = job(None, "", "needs_info");
        let child = job(Some(coordinator.id), "left", "needs_info");
        let mut publication = publication(coordinator.id, &child);
        for (status, expected) in [
            ("pending", "pending"),
            ("failed", "failed"),
            ("published", "published"),
        ] {
            publication.status = status.into();
            let result = view(
                "needs_info",
                &coordinator,
                None,
                std::slice::from_ref(&child),
                std::slice::from_ref(&publication),
            );
            assert_eq!(result[0].questions.len(), 2);
            assert_eq!(result[0].evidence.state, expected);
            assert_eq!(
                result[0].evidence.files.len(),
                usize::from(status == "published")
            );
            if status == "published" {
                assert_eq!(
                    result[0].evidence.files[0].url,
                    "https://example.invalid/blob/accepted-sha/docs/proposal.md"
                );
            }
        }
        publication.metadata = json!({"supporting_files":[]});
        assert_eq!(
            view(
                "needs_info",
                &coordinator,
                None,
                std::slice::from_ref(&child),
                std::slice::from_ref(&publication)
            )[0]
            .evidence
            .state,
            "none"
        );
        publication.metadata = json!({});
        assert_eq!(
            view(
                "needs_info",
                &coordinator,
                None,
                std::slice::from_ref(&child),
                std::slice::from_ref(&publication)
            )[0]
            .evidence
            .state,
            "unavailable"
        );
        publication.metadata = json!({"supporting_files":["other.md"]});
        publication.job_id = Some(Uuid::now_v7());
        assert_eq!(
            view(
                "needs_info",
                &coordinator,
                None,
                std::slice::from_ref(&child),
                &[publication]
            )[0]
            .evidence
            .state,
            "unavailable"
        );
        let completed = job(Some(coordinator.id), "left", "implemented");
        // The current coordinator reflects completion too; old questions vanish.
        assert!(view("in_progress", &coordinator, None, &[child, completed], &[]).is_empty());
    }

    #[test]
    fn planning_questions_use_the_saved_proposal_and_exact_attempt_without_masking_recovery_errors()
    {
        let mut coordinator = job(None, "plan", "needs_human");
        let mut proposal = job(None, "plan", "needs_info").result.unwrap();
        proposal["task"] = json!("outline");
        proposal["work_item"] = Value::Null;
        let saved = json!({"start_approved":false,"attempt":3,"previous":[proposal],"last_result":coordinator.result});
        let mut publication = publication(coordinator.id, &coordinator);
        publication.task = Some("outline".into());
        publication.work_item = None;
        publication.attempt = Some(2);
        let result = view(
            "needs_human",
            &coordinator,
            Some(&saved),
            &[],
            std::slice::from_ref(&publication),
        );
        assert_eq!(result[0].label(), "outline");
        assert_eq!(result[0].questions.len(), 2);
        assert_eq!(result[0].evidence.state, "unavailable");
        publication.attempt = Some(3);
        assert_eq!(
            view(
                "needs_human",
                &coordinator,
                Some(&saved),
                &[],
                &[publication]
            )[0]
            .evidence
            .state,
            "published"
        );
        coordinator.result.as_mut().unwrap()["blocked_reason"] =
            json!("Restore the accepted checkpoint before continuing.");
        let result = view("blocked", &coordinator, Some(&saved), &[], &[]);
        assert_eq!(
            result[0].reason.as_deref(),
            Some("Restore the accepted checkpoint before continuing.")
        );
        assert!(result[0].questions.is_empty());
    }

    #[test]
    fn coordinator_recovery_failure_takes_precedence_over_pending_child_questions() {
        let mut coordinator = job(None, "", "needs_human");
        let child = job(Some(coordinator.id), "left", "needs_info");
        let checkpoint = json!({"last_result":coordinator.result,"pending_approvals":[{"key":{"task":"check","work_item":"left"}}]});
        coordinator.result.as_mut().unwrap()["blocked_reason"] =
            json!("Restore the immutable checkpoint before continuing.");
        let result = view(
            "needs_human",
            &coordinator,
            Some(&checkpoint),
            std::slice::from_ref(&child),
            &[],
        );
        assert_eq!(result[0].label(), "Workflow");
        assert_eq!(
            result[0].reason.as_deref(),
            Some("Restore the immutable checkpoint before continuing.")
        );
        assert!(result[0].action.contains("Resolve the reported problem"));
        assert_eq!(result[1].label(), "check/left");
        for outcome in ["blocked", "failed", "needs_human"] {
            let mut blocked = child.clone();
            blocked.result = Some(
                json!({"outcome":outcome,"questions":[],"blocked_reason":"Required validation is unavailable.","human_review_reason":null}),
            );
            let result = view(
                if outcome == "needs_human" {
                    "needs_human"
                } else {
                    "blocked"
                },
                &coordinator,
                None,
                &[blocked],
                &[],
            );
            assert_eq!(result.len(), 1);
            assert_eq!(result[0].label(), "check/left");
            assert_eq!(
                result[0].reason.as_deref(),
                Some("Required validation is unavailable.")
            );
        }
    }
}
