//! Explicit review revisions are separate from approvals of paused tasks.
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
        "\nThe PR becomes draft before revision work starts and returns to ready for review after the changes are published and required checks pass. Paused or failed revisions remain draft.\n\nIf a run pauses, follow the decision instructions on parent issue #{parent}; submit approvals or upstream revisions there to continue that run. Posting another PR command does not resume paused work.\n\nOrdinary comments, edited commands, and GitHub “Request changes” reviews do not start a run. Optional `review=ID` and `thread=COMMENT_ID` selectors can follow the target to select review feedback.\n"
    ));
    text
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
