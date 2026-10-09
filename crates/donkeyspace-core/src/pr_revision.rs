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

#[cfg(test)]
mod tests {
    use super::*;

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
