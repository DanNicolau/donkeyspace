//! Snapshot review evidence without interpreting it as execution authority.
use crate::{GitHubClient, GitHubClientError};
use donkeyspace_core::pr_revision::RevisionCommand;
use serde_json::{Value, json};

impl GitHubClient {
    pub async fn commit_is_ancestor(
        &self,
        owner: &str,
        repo: &str,
        base: &str,
        head: &str,
    ) -> Result<bool, GitHubClientError> {
        if ![base, head]
            .iter()
            .all(|sha| sha.len() == 40 && sha.bytes().all(|b| b.is_ascii_hexdigit()))
        {
            return Err(GitHubClientError::InvalidResponse(
                "Invalid commit identity".into(),
            ));
        }
        let comparison: Value = self
            .client
            .get(
                format!("/repos/{owner}/{repo}/compare/{base}...{head}"),
                None::<&()>,
            )
            .await?;
        Ok(matches!(
            comparison["status"].as_str(),
            Some("identical" | "ahead")
        ))
    }

    pub async fn reopen_issue(
        &self,
        owner: &str,
        repo: &str,
        number: i64,
    ) -> Result<(), GitHubClientError> {
        self.client
            .issues(owner, repo)
            .update(number as u64)
            .state(octocrab::models::IssueState::Open)
            .send()
            .await?;
        Ok(())
    }

    pub async fn issue(
        &self,
        owner: &str,
        repo: &str,
        number: i64,
    ) -> Result<Value, GitHubClientError> {
        Ok(self
            .client
            .get(
                format!("/repos/{owner}/{repo}/issues/{number}"),
                None::<&()>,
            )
            .await?)
    }

    pub async fn revision_feedback(
        &self,
        owner: &str,
        repo: &str,
        number: i64,
        command: &RevisionCommand,
    ) -> Result<Value, GitHubClientError> {
        let reviews = self
            .review_pages(format!(
                "/repos/{owner}/{repo}/pulls/{number}/reviews?per_page=100"
            ))
            .await?;
        let comments = self
            .review_pages(format!(
                "/repos/{owner}/{repo}/pulls/{number}/comments?per_page=100"
            ))
            .await?;
        let conversation = self.issue_comments(owner, repo, number).await?;
        select_feedback(command, reviews, comments, conversation)
    }

    async fn review_pages(&self, route: String) -> Result<Vec<Value>, GitHubClientError> {
        let mut page: octocrab::Page<Value> = self.client.get(route, None::<&()>).await?;
        let mut items = page.take_items();
        while let Some(mut next) = self.client.get_page(&page.next).await? {
            items.append(&mut next.take_items());
            page = next;
        }
        Ok(items)
    }
}

fn select_feedback(
    command: &RevisionCommand,
    reviews: Vec<Value>,
    comments: Vec<Value>,
    conversation: Vec<Value>,
) -> Result<Value, GitHubClientError> {
    let invalid = |message: &str| GitHubClientError::InvalidResponse(message.into());
    if command
        .review
        .is_some_and(|id| !reviews.iter().any(|r| r["id"].as_u64() == Some(id)))
    {
        return Err(invalid("The selected review does not belong to this PR."));
    }
    let root = if let Some(id) = command.thread {
        let comment = comments
            .iter()
            .find(|c| c["id"].as_u64() == Some(id))
            .ok_or_else(|| invalid("The selected thread comment does not belong to this PR."))?;
        Some(comment["in_reply_to_id"].as_u64().unwrap_or(id))
    } else {
        None
    };
    // A review filter selects its roots as well as subsequent replies, even
    // when those replies have a different pull_request_review_id.
    let roots: Vec<u64> = comments
        .iter()
        .filter(|c| {
            root.is_none_or(|id| c["id"].as_u64() == Some(id))
                && command
                    .review
                    .is_none_or(|id| c["pull_request_review_id"].as_u64() == Some(id))
        })
        .filter_map(|c| c["in_reply_to_id"].as_u64().or(c["id"].as_u64()))
        .collect();
    let selected_comments: Vec<_> = comments
        .into_iter()
        .filter(|c| {
            c["id"].as_u64().is_some_and(|id| roots.contains(&id))
                || c["in_reply_to_id"]
                    .as_u64()
                    .is_some_and(|id| roots.contains(&id))
        })
        .collect();
    if root.is_some() && selected_comments.is_empty() {
        return Err(invalid(
            "The selected thread is not part of the selected review.",
        ));
    }
    let result = json!({
        "instructions": command.feedback,
        "reviews": reviews.into_iter().filter(|r| command.review.is_none_or(|id| r["id"].as_u64()==Some(id))).collect::<Vec<_>>(),
        "inline_comments": selected_comments,
        "conversation": conversation,
    });
    if result.to_string().len() > 1_000_000 {
        return Err(invalid(
            "Review context exceeds 1 MB; select a review/thread or shorten the conversation before retrying.",
        ));
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn selected_review_includes_replies_and_line_context_but_not_other_threads() {
        let command = RevisionCommand {
            target: None,
            review: Some(10),
            thread: None,
            feedback: "Fix".into(),
        };
        let result = select_feedback(&command, vec![json!({"id":10,"body":"Requested changes"})], vec![
            json!({"id":1,"pull_request_review_id":10,"path":"src/a.rs","line":4,"commit_id":"old"}),
            json!({"id":2,"pull_request_review_id":11,"in_reply_to_id":1,"body":"Clarification"}),
            json!({"id":3,"pull_request_review_id":12,"path":"src/b.rs"}),
        ], vec![json!({"body":"Follow-up"})]).unwrap();
        assert_eq!(result["inline_comments"].as_array().unwrap().len(), 2);
        assert_eq!(result["inline_comments"][0]["line"], 4);
        assert_eq!(result["conversation"][0]["body"], "Follow-up");
        assert!(select_feedback(&command, vec![], vec![], vec![]).is_err());
    }
}
