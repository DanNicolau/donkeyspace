//! Commands and feedback for revisions of managed pull requests.
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RevisionCommand {
    pub target: Option<String>,
    pub review: Option<u64>,
    pub thread: Option<u64>,
    pub feedback: String,
}

/// Only a standalone first-line command authorizes execution. Selection uses
/// GitHub numeric review/comment IDs, never arbitrary URLs supplied by a user.
pub fn parse_command(body: &str, prefix: &str) -> Result<Option<RevisionCommand>, String> {
    let mut lines = body.trim().lines();
    let mut words = lines.next().unwrap_or_default().split_whitespace();
    if words.next() != Some(&format!("/{prefix}")) || words.next() != Some("revise") {
        return Ok(None);
    }
    let mut command = RevisionCommand {
        target: None,
        review: None,
        thread: None,
        feedback: lines.collect::<Vec<_>>().join("\n").trim().into(),
    };
    for word in words {
        if let Some(id) = word.strip_prefix("review=") {
            if command.review.is_some() {
                return Err("Specify at most one review.".into());
            }
            command.review = Some(parse_id(id)?);
        } else if let Some(id) = word.strip_prefix("thread=") {
            if command.thread.is_some() {
                return Err("Specify at most one thread.".into());
            }
            command.thread = Some(parse_id(id)?);
        } else if !word.contains('=') && command.target.is_none() {
            command.target = Some(word.into());
        } else {
            return Err("Use revise [TASK[/WORK-ITEM]] [review=ID] [thread=COMMENT_ID].".into());
        }
    }
    if command.feedback.is_empty() {
        return Err("Put the requested changes on lines after the revise command.".into());
    }
    Ok(Some(command))
}

fn parse_id(value: &str) -> Result<u64, String> {
    value
        .parse::<u64>()
        .ok()
        .filter(|id| *id > 0)
        .ok_or_else(|| "Review and thread IDs must be positive integers.".into())
}

/// Review and merge-repair jobs may inherit issue context from the latest
/// developer run, but must never inherit that run's continuation controls.
pub fn clear_execution_context(input: &mut serde_json::Value) {
    if let Some(map) = input.as_object_mut() {
        for key in [
            "pr_revision",
            "donkeyspace_resume",
            "donkeyspace_human_decision",
            "donkeyspace_lifecycle_coordinator",
        ] {
            map.remove(key);
        }
    }
}

const INSTRUCTIONS_START: &str = "<!-- donkeyspace-pr-revisions:start -->";
const INSTRUCTIONS_END: &str = "<!-- donkeyspace-pr-revisions:end -->";

pub fn intake_instructions(prefix: &str, parent: i64) -> String {
    format!(
        "## Requesting changes\n\nPost a new comment on this PR:\n\n```text\n/{prefix} revise\nDescribe what you want to change, including any execution preferences.\n```\n\nThe planning agent proposes the changes and shows which tasks need to run again. Review that proposal before approving it. Plan-only changes retain completed work; artifact approvals remain separate.\n\nFollow the decision commands in the live progress comment. Commands on parent issue #{parent} also work. Wait for a pause before changing a running workflow. The PR stays draft while a revision is in progress.\n"
    )
}

pub fn instructions(
    prefix: &str,
    parent: i64,
    targets: &[crate::TaskKey],
    planning_task: Option<&str>,
) -> String {
    let target_list = targets
        .iter()
        .map(crate::TaskKey::target)
        .collect::<std::collections::BTreeSet<_>>();
    let target = planning_task.or_else(|| target_list.first().map(String::as_str));
    let command = target.map_or_else(
        || format!("/{prefix} revise"),
        |target| format!("/{prefix} revise {target}"),
    );
    let mut text = format!(
        "## Requesting changes\n\nPost a **new conversation comment on this PR**, with feedback on the lines after the command:\n\n```text\n{command}\nDescribe the requested change and its acceptance criteria.\n```\n"
    );
    if !target_list.is_empty() {
        text.push_str(&format!(
            "\nAvailable targets: {}.\n",
            target_list
                .iter()
                .map(|target| format!("`{target}`"))
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    if let Some(planning) = planning_task {
        text.push_str(&format!("\nFor specification or behavior changes, start with `{planning}` so the contract is updated before implementation.\n"));
    }
    text.push_str(&format!(
        "\nThe PR becomes draft before revision work starts and returns to ready for review after the changes are published and required checks pass. Paused or failed revisions remain draft.\n\nFollow the live revision progress comment below. When a run pauses, post its approval or revision command on **this PR** to continue the same run. Commands on parent issue #{parent} also work. For example: `/{prefix} approve TASK`. A running revision must finish or pause before another command can be accepted.\n\nOrdinary comments, edited commands, and GitHub “Request changes” reviews do not start a run. Optional `review=ID` and `thread=COMMENT_ID` selectors can follow the target when starting a new revision.\n"
    ));
    text
}

/// Validate selection against the authoritative pause before queueing a resume.
/// The worker still verifies its saved flow and performs the actual transition.
pub fn validate_decision(
    checkpoint: &serde_json::Value,
    decision: &serde_json::Value,
) -> Result<(), String> {
    if checkpoint["change_intake_enabled"] == true {
        if decision["action"] == "revise" {
            return decision["feedback"]
                .as_str()
                .filter(|text| !text.trim().is_empty())
                .map(|_| ())
                .ok_or_else(|| "Describe the requested change after the revise command.".into());
        }
        if let Some(change) = checkpoint.get("pending_change").filter(|v| !v.is_null()) {
            let target = change["revision"].as_u64().map(|n| format!("change/{n}"));
            return if decision["action"] == "approve"
                && decision["target"].as_str() == target.as_deref()
                && change.get("proposal").is_some_and(|p| !p.is_null())
            {
                Ok(())
            } else {
                Err("Use the approval command for the displayed change proposal, or revise your request. Artifact approvals remain separate.".into())
            };
        }
    }
    if checkpoint["last_result"]["outcome"] == "needs_info" {
        return Err("This pause needs clarification. Answer the questions on the parent issue to continue the same run.".into());
    }
    let pending = checkpoint["pending_approvals"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|entry| serde_json::from_value::<crate::TaskKey>(entry["key"].clone()).ok())
        .map(|key| key.target())
        .collect::<Vec<_>>();
    let action = decision["action"].as_str().unwrap_or_default();
    let target = decision["target"].as_str();
    if action == "revise"
        && decision["feedback"]
            .as_str()
            .is_none_or(|text| text.trim().is_empty())
    {
        return Err("Put the requested changes on lines after the revise command.".into());
    }
    if !matches!(action, "approve" | "revise") {
        return Err("Use approve or revise to continue this run.".into());
    }
    if target == Some("all") && action == "approve" && !pending.is_empty()
        || target.is_none() && pending.len() == 1
        || target.is_some_and(|target| pending.iter().any(|key| key == target))
    {
        return Ok(());
    }
    let upstream = checkpoint["revision_options"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|entry| serde_json::from_value::<crate::RevisionOption>(entry.clone()).ok())
        .filter(|option| {
            option
                .affected
                .iter()
                .any(|key| pending.contains(&key.target()))
        })
        .map(|option| option.target.target())
        .collect::<Vec<_>>();
    if action == "revise" && target.is_some_and(|target| upstream.iter().any(|key| key == target)) {
        return Ok(());
    }
    Err(format!(
        "That decision does not match the current pause. Pending targets: {}. Completed upstream targets available for revise: {}. Use a command from the live revision progress comment.",
        if pending.is_empty() {
            "none".into()
        } else {
            pending.join(", ")
        },
        if upstream.is_empty() {
            "none".into()
        } else {
            upstream.join(", ")
        }
    ))
}

/// Replace only our marked section, preserving the rest of the PR description.
pub fn with_instructions(body: &str, instructions: &str) -> String {
    let section = format!("{INSTRUCTIONS_START}\n{instructions}\n{INSTRUCTIONS_END}");
    if let Some(start) = body.find(INSTRUCTIONS_START)
        && let Some(end) = body[start..].find(INSTRUCTIONS_END)
    {
        let end = start + end + INSTRUCTIONS_END.len();
        return format!("{}{section}{}", &body[..start], &body[end..]);
    }
    format!("{}\n\n{section}", body.trim_end())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn change_intake_accepts_plain_requests_and_only_the_displayed_proposal_approval() {
        let mut checkpoint = serde_json::json!({"change_intake_enabled":true,
            "pending_change":{"revision":3,"proposal":{"summary":"Skip packaging"}},
            "pending_approvals":[{"key":{"task":"build","work_item":"queue"}}]});
        for target in [
            serde_json::Value::Null,
            serde_json::json!("plan"),
            serde_json::json!("build/queue"),
        ] {
            assert!(validate_decision(&checkpoint, &serde_json::json!({"action":"revise","target":target,"feedback":"Skip packaging"})).is_ok());
        }
        assert!(
            validate_decision(
                &checkpoint,
                &serde_json::json!({"action":"approve","target":"change/3"})
            )
            .is_ok()
        );
        for target in [
            serde_json::Value::Null,
            serde_json::json!("all"),
            serde_json::json!("build/queue"),
            serde_json::json!("change/2"),
        ] {
            assert!(
                validate_decision(
                    &checkpoint,
                    &serde_json::json!({"action":"approve","target":target})
                )
                .is_err()
            );
        }
        checkpoint["pending_change"]["proposal"] = serde_json::Value::Null;
        assert!(
            validate_decision(
                &checkpoint,
                &serde_json::json!({"action":"approve","target":"change/3"})
            )
            .is_err()
        );
        assert!(
            validate_decision(
                &checkpoint,
                &serde_json::json!({"action":"revise","feedback":" "})
            )
            .is_err()
        );
        checkpoint["pending_change"] = serde_json::Value::Null;
        assert!(
            validate_decision(
                &checkpoint,
                &serde_json::json!({"action":"approve","target":"build/queue"})
            )
            .is_ok()
        );
    }

    #[test]
    fn decisions_match_the_current_pause_and_relevant_upstream_work() {
        use serde_json::json;
        let mut checkpoint = json!({
            "pending_approvals":[{"key":{"task":"build","work_item":"queue"}}],
            "revision_options":[
                {"target":{"task":"plan"},"affected":[{"task":"build","work_item":"queue"}]},
                {"target":{"task":"unrelated"},"affected":[{"task":"build","work_item":"other"}]}
            ]
        });
        for target in [serde_json::Value::Null, json!("all"), json!("build/queue")] {
            assert!(
                validate_decision(&checkpoint, &json!({"action":"approve","target":target}))
                    .is_ok()
            );
        }
        for target in [serde_json::Value::Null, json!("plan"), json!("build/queue")] {
            assert!(
                validate_decision(
                    &checkpoint,
                    &json!({"action":"revise","target":target,"feedback":"Fix reset"})
                )
                .is_ok()
            );
        }
        for decision in [
            json!({"action":"approve","target":"plan"}),
            json!({"action":"revise","target":"unrelated","feedback":"Fix"}),
            json!({"action":"revise","target":"plan","feedback":" "}),
            json!({"action":"revise","target":"all","feedback":"Fix"}),
        ] {
            assert!(validate_decision(&checkpoint, &decision).is_err());
        }
        checkpoint["pending_approvals"]
            .as_array_mut()
            .unwrap()
            .push(json!({"key":{"task":"test","work_item":"queue"}}));
        assert!(validate_decision(&checkpoint, &json!({"action":"approve"})).is_err());
        assert!(
            validate_decision(&checkpoint, &json!({"action":"approve","target":"all"})).is_ok()
        );
        checkpoint["pending_approvals"] = json!([]);
        assert!(
            validate_decision(&checkpoint, &json!({"action":"approve","target":"all"})).is_err()
        );
    }

    #[test]
    fn instructions_use_configured_targets_and_preserve_human_text() {
        let targets = vec![crate::TaskKey {
            task: "implement".into(),
            work_item: Some("queue".into()),
        }];
        let instructions = instructions("studio", 12, &targets, Some("plan"));
        assert!(instructions.contains("/studio revise plan"));
        assert!(instructions.contains("`implement/queue`"));
        assert!(instructions.contains("parent issue #12"));
        let original = "Closes #12\n\nMaintainer notes.";
        let body = with_instructions(original, &instructions);
        assert_eq!(with_instructions(&body, &instructions), body);
        let body = format!("{body}\n\nAdditional human notes.");
        let changed = with_instructions(&body, "Updated instructions");
        assert!(changed.starts_with(original));
        assert!(changed.ends_with("Additional human notes."));
        assert_eq!(changed.matches(INSTRUCTIONS_START).count(), 1);
        assert!(!changed.contains("/studio revise plan"));
        assert!(super::instructions("studio", 12, &[], None).contains("/studio revise\n"));
    }

    #[test]
    fn auxiliary_jobs_keep_issue_identity_without_resuming_a_revision() {
        let mut input = serde_json::json!({"issue":{"number":1},"donkeyspace_workflow_generation":3,
            "pr_revision":{},"donkeyspace_resume":true,"donkeyspace_human_decision":{},"donkeyspace_lifecycle_coordinator":true});
        clear_execution_context(&mut input);
        assert_eq!(
            input,
            serde_json::json!({"issue":{"number":1},"donkeyspace_workflow_generation":3})
        );
    }

    #[test]
    fn explicit_custom_prefix_and_feedback_are_required() {
        let command = parse_command(
            "/studio revise implement/queue review=23 thread=45\nFix reset.\nKeep the interface.",
            "studio",
        )
        .unwrap()
        .unwrap();
        assert_eq!(command.target.as_deref(), Some("implement/queue"));
        assert_eq!(command.review, Some(23));
        assert_eq!(command.thread, Some(45));
        assert_eq!(command.feedback, "Fix reset.\nKeep the interface.");
        for body in [
            "Please fix reset",
            "/other revise\nFix reset",
            "A suggestion:\n/studio revise\nFix reset",
        ] {
            assert!(parse_command(body, "studio").unwrap().is_none());
        }
        for body in [
            "/studio revise",
            "/studio revise review=0\nFix",
            "/studio revise a b\nFix",
            "/studio revise review=1 review=2\nFix",
        ] {
            assert!(parse_command(body, "studio").is_err());
        }
    }
}
