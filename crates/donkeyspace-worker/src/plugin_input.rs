use donkeyspace_github::GitHubClient;
use serde_json::{Value, json};

/// Build one conversation snapshot for all tasks/stages in this invocation.
/// Fetch failures propagate so agents cannot silently run without the answers.
pub async fn with_issue_comments(
    input: &Value,
    github: Option<&GitHubClient>,
) -> Result<Value, Box<dyn std::error::Error>> {
    let issue = input.get("issue").unwrap_or(input);
    let comments = if let Some(github) = github {
        let owner = input
            .pointer("/repository/owner/login")
            .and_then(Value::as_str)
            .ok_or("plugin issue conversation is missing repository owner")?;
        let repo = input
            .pointer("/repository/name")
            .and_then(Value::as_str)
            .ok_or("plugin issue conversation is missing repository name")?;
        let number = issue
            .get("number")
            .and_then(Value::as_i64)
            .ok_or("plugin issue conversation is missing issue number")?;
        github.issue_comments(owner, repo, number).await?
    } else {
        issue
            .get("comments")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default()
    };
    Ok(merge_issue_comments(input, comments))
}

fn merge_issue_comments(input: &Value, mut comments: Vec<Value>) -> Value {
    // A just-created/edited comment may not yet appear in the fetched history.
    // Conversely, a delayed webhook must not overwrite a newer fetched edit.
    if let Some(trigger) = input.get("comment") {
        let existing = comments.iter_mut().find(|comment| {
            trigger.get("id").is_some_and(|id| !id.is_null())
                && comment.get("id") == trigger.get("id")
        });
        if let Some(existing) = existing {
            if timestamp(trigger) >= timestamp(existing) {
                *existing = trigger.clone();
            }
        } else {
            comments.push(trigger.clone());
        }
    }
    comments.sort_by_key(|comment| comment.get("id").and_then(Value::as_u64));
    let comments = comments
        .iter()
        .filter(|comment| comment.get("body").and_then(Value::as_str).is_some())
        .map(|comment| {
            json!({
                "id": comment.get("id"),
                "body": comment["body"],
                "user": {
                    "login": comment.pointer("/user/login"),
                    "type": comment.pointer("/user/type"),
                },
                "author_association": comment.get("author_association"),
                "created_at": comment.get("created_at"),
                "updated_at": comment.get("updated_at"),
                "html_url": comment.get("html_url"),
            })
        })
        .collect::<Vec<_>>();
    let mut enriched = input.clone();
    let mut issue = input.get("issue").unwrap_or(input).clone();
    if let Some(issue) = issue.as_object_mut() {
        if let Some(count) = issue
            .get("comments")
            .filter(|value| value.is_number())
            .cloned()
        {
            issue.insert("comment_count".into(), count);
        }
        issue.insert("comments".into(), Value::Array(comments));
    }
    enriched["issue"] = issue;
    enriched
}

// GitHub supplies these timestamps in UTC RFC3339 format.
fn timestamp(comment: &Value) -> &str {
    comment
        .get("updated_at")
        .or_else(|| comment.get("created_at"))
        .and_then(Value::as_str)
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn comment(id: u64, body: &str, updated_at: &str) -> Value {
        json!({
            "id": id, "body": body, "updated_at": updated_at,
            "user": {"login": "maintainer", "type": "User"},
            "html_url": format!("https://github.com/example/project/issues/1#issuecomment-{id}")
        })
    }

    #[test]
    fn created_and_edited_replies_reach_plugin_issue_with_earlier_answers() {
        for action in ["created", "edited"] {
            let earlier = comment(1, "Use a simulation model.", "2026-01-01T00:00:00Z");
            let stale = comment(2, "Read timing TBD.", "2026-01-01T00:01:00Z");
            let reply = comment(2, "Read latency is one cycle.", "2026-01-01T00:02:00Z");
            let input = json!({
                "action": action, "issue": {"number": 1, "body": "Build a model", "comments": 2},
                "comment": reply,
                "donkeyspace_resume": true,
            });
            let history = if action == "created" {
                vec![earlier]
            } else {
                vec![earlier, stale]
            };
            let enriched = merge_issue_comments(&input, history);
            // Both plugin input writers serialize this issue object.
            let issue = &enriched["issue"];
            assert_eq!(issue["comments"].as_array().unwrap().len(), 2);
            assert_eq!(issue["comments"][0]["body"], "Use a simulation model.");
            assert_eq!(issue["comments"][1]["body"], "Read latency is one cycle.");
            assert_eq!(issue["comments"][1]["user"]["login"], "maintainer");
            assert_eq!(issue["comment_count"], 2);
            assert_eq!(enriched["donkeyspace_resume"], true);
        }
    }

    #[test]
    fn delayed_webhook_does_not_restore_an_older_comment_edit() {
        let old = comment(1, "Old answer", "2026-01-01T00:00:00Z");
        let new = comment(1, "Corrected answer", "2026-01-01T00:01:00Z");
        let input = json!({"issue": {"comments": 1}, "comment": old});
        let enriched = merge_issue_comments(&input, vec![new]);
        assert_eq!(enriched["issue"]["comments"].as_array().unwrap().len(), 1);
        assert_eq!(enriched["issue"]["comments"][0]["body"], "Corrected answer");
    }

    #[tokio::test]
    async fn offline_plugin_retains_supplied_history_and_trigger() {
        let input = json!({
            "issue": {"comments": [comment(1, "First answer", "2026-01-01T00:00:00Z")]},
            "comment": comment(2, "Second answer", "2026-01-01T00:01:00Z")
        });
        let enriched = with_issue_comments(&input, None).await.unwrap();
        assert_eq!(enriched["issue"]["comments"].as_array().unwrap().len(), 2);
        assert_eq!(enriched["issue"]["comments"][1]["body"], "Second answer");
    }

    #[test]
    fn issue_events_include_history_without_a_triggering_comment() {
        let input = json!({"issue": {"body": "Updated requirements", "comments": 1}});
        let enriched = merge_issue_comments(
            &input,
            vec![comment(1, "Earlier answer", "2026-01-01T00:00:00Z")],
        );
        assert_eq!(enriched["issue"]["body"], "Updated requirements");
        assert_eq!(enriched["issue"]["comments"][0]["body"], "Earlier answer");
        assert!(enriched.get("comment").is_none());
    }
}
