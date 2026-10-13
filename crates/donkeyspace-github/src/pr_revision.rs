//! Snapshot review evidence without interpreting it as execution authority.
use crate::{GitHubClient, GitHubClientError};
use donkeyspace_core::pr_revision::RevisionCommand;
use serde_json::{Value, json};

impl GitHubClient {
    /// Idempotent state transition. Reject closed PRs and stale publication
    /// heads before sending either GraphQL mutation.
    pub async fn set_pull_request_draft(
        &self,
        owner: &str,
        repo: &str,
        number: u64,
        draft: bool,
        expected_head: Option<&str>,
    ) -> Result<(), GitHubClientError> {
        let pr = self.pull_request(owner, repo, number).await?;
        if pr["state"] != "open" || pr["merged"] == true {
            return Err(GitHubClientError::InvalidResponse(
                "PR is closed or merged".into(),
            ));
        }
        if expected_head.is_some_and(|head| pr["head"]["sha"].as_str() != Some(head)) {
            return Err(GitHubClientError::InvalidResponse(
                "PR head changed before draft/ready transition".into(),
            ));
        }
        if pr["draft"].as_bool() == Some(draft) {
            return Ok(());
        }
        let node = pr["node_id"].as_str().ok_or_else(|| {
            GitHubClientError::InvalidResponse("PR node identity is missing".into())
        })?;
        let transition = self
            .mutate_pull_request_draft(node, draft, expected_head)
            .await;
        if let Err(error) = transition {
            if !draft {
                // A timeout or partial GraphQL response may follow a successful
                // mutation. Restore draft before reporting readiness failure.
                if let Err(restore) =
                    Box::pin(self.set_pull_request_draft(owner, repo, number, true, None)).await
                {
                    return Err(GitHubClientError::InvalidResponse(format!(
                        "{error}; restoring draft also failed: {restore}"
                    )));
                }
            }
            return Err(error);
        }
        Ok(())
    }

    async fn mutate_pull_request_draft(
        &self,
        node: &str,
        draft: bool,
        expected_head: Option<&str>,
    ) -> Result<(), GitHubClientError> {
        let mutation = if draft {
            "convertPullRequestToDraft"
        } else {
            "markPullRequestReadyForReview"
        };
        let response: Value = self.client.post("/graphql", Some(&json!({
            "query": format!("mutation($id: ID!) {{ {mutation}(input: {{pullRequestId: $id}}) {{ pullRequest {{ id isDraft headRefOid }} }} }}"),
            "variables": {"id": node},
        }))).await?;
        // GraphQL can return HTTP 200 with errors or partial data.
        if response
            .get("errors")
            .and_then(Value::as_array)
            .is_some_and(|errors| !errors.is_empty())
        {
            return Err(GitHubClientError::InvalidResponse(format!(
                "PR draft/ready transition failed: {}",
                response["errors"]
            )));
        }
        let updated = &response["data"][mutation]["pullRequest"];
        if updated["id"].as_str() != Some(node) || updated["isDraft"].as_bool() != Some(draft) {
            return Err(GitHubClientError::InvalidResponse(
                "GitHub did not confirm the requested PR draft state".into(),
            ));
        }
        if !draft && expected_head.is_some_and(|head| updated["headRefOid"].as_str() != Some(head))
        {
            // GraphQL has no expected-head argument. If a push races the
            // transition, restore draft rather than endorse an unchecked head.
            return Err(GitHubClientError::InvalidResponse(
                "PR head moved during readiness transition".into(),
            ));
        }
        Ok(())
    }

    pub async fn ensure_revision_instructions(
        &self,
        owner: &str,
        repo: &str,
        number: u64,
        instructions: &str,
    ) -> Result<(), GitHubClientError> {
        let pr = self.pull_request(owner, repo, number).await?;
        if pr["state"] != "open" || pr["merged"] == true {
            return Err(GitHubClientError::InvalidResponse(
                "PR is closed or merged".into(),
            ));
        }
        let body = pr["body"].as_str().unwrap_or_default();
        let updated = donkeyspace_core::pr_revision::with_instructions(body, instructions);
        if updated != body {
            let _: Value = self
                .client
                .patch(
                    format!("/repos/{owner}/{repo}/pulls/{number}"),
                    Some(&json!({"body":updated})),
                )
                .await?;
        }
        Ok(())
    }

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
    use http_body_util::{BodyExt, Full};
    use octocrab::{AuthState, OctocrabBuilder};
    use std::{
        collections::VecDeque,
        convert::Infallible,
        sync::{Arc, Mutex},
    };
    use tower::service_fn;

    struct Fixture {
        client: GitHubClient,
        pr: Arc<Mutex<Value>>,
        writes: Arc<Mutex<Vec<Value>>>,
    }

    fn fixture(draft: bool, responses: Vec<Value>) -> Fixture {
        let pr = Arc::new(Mutex::new(
            json!({"node_id":"PR_test", "state":"open", "merged":false,
            "draft":draft, "head":{"sha":"accepted"}, "body":"Maintainer notes."}),
        ));
        let writes = Arc::new(Mutex::new(Vec::new()));
        let replies = Arc::new(Mutex::new(VecDeque::from(responses)));
        let state = pr.clone();
        let recorded = writes.clone();
        let service = service_fn(move |request: http::Request<octocrab::OctoBody>| {
            let state = state.clone();
            let recorded = recorded.clone();
            let replies = replies.clone();
            async move {
                let method = request.method().clone();
                let path = request.uri().path().to_string();
                let bytes = request.into_body().collect().await.unwrap().to_bytes();
                let response = if method == http::Method::GET {
                    assert_eq!(path, "/repos/example/project/pulls/2");
                    state.lock().unwrap().clone()
                } else {
                    let payload: Value = serde_json::from_slice(&bytes).unwrap();
                    recorded.lock().unwrap().push(payload.clone());
                    if method == http::Method::PATCH {
                        assert_eq!(path, "/repos/example/project/pulls/2");
                        state.lock().unwrap()["body"] = payload["body"].clone();
                        json!({})
                    } else {
                        assert_eq!(path, "/graphql");
                        assert_eq!(payload["variables"]["id"], "PR_test");
                        let response = replies
                            .lock()
                            .unwrap()
                            .pop_front()
                            .expect("unexpected mutation");
                        if let Some(data) = response["data"].as_object() {
                            for value in data.values() {
                                let updated = &value["pullRequest"];
                                state.lock().unwrap()["draft"] = updated["isDraft"].clone();
                                state.lock().unwrap()["head"]["sha"] =
                                    updated["headRefOid"].clone();
                            }
                        }
                        response
                    }
                };
                Ok::<_, Infallible>(
                    http::Response::builder()
                        .status(200)
                        .body(Full::new(bytes::Bytes::from(response.to_string())))
                        .unwrap(),
                )
            }
        });
        let client = GitHubClient {
            client: OctocrabBuilder::new_empty()
                .with_service(service)
                .with_auth(AuthState::None)
                .build()
                .unwrap(),
        };
        Fixture { client, pr, writes }
    }

    fn response(draft: bool, head: &str) -> Value {
        let mutation = if draft {
            "convertPullRequestToDraft"
        } else {
            "markPullRequestReadyForReview"
        };
        json!({"data":{mutation:{"pullRequest":{"id":"PR_test","isDraft":draft,"headRefOid":head}}}})
    }

    #[tokio::test]
    async fn draft_and_ready_transitions_are_idempotent_and_check_publication_head() {
        for desired in [true, false] {
            let f = fixture(!desired, vec![response(desired, "accepted")]);
            for _ in 0..2 {
                f.client
                    .set_pull_request_draft("example", "project", 2, desired, Some("accepted"))
                    .await
                    .unwrap();
            }
            let writes = f.writes.lock().unwrap();
            assert_eq!(writes.len(), 1);
            assert!(writes[0]["query"].as_str().unwrap().contains(if desired {
                "convertPullRequestToDraft"
            } else {
                "markPullRequestReadyForReview"
            }));
        }
        for closed in [false, true] {
            let f = fixture(true, vec![]);
            if closed {
                f.pr.lock().unwrap()["state"] = json!("closed");
            }
            assert!(
                f.client
                    .set_pull_request_draft("example", "project", 2, false, Some("different-head"))
                    .await
                    .is_err()
            );
            assert!(f.writes.lock().unwrap().is_empty());
        }
    }

    #[tokio::test]
    async fn failed_or_partial_graphql_mutations_are_not_reported_as_success() {
        for mutation_response in [
            json!({"errors":[{"message":"Denied"}]}),
            json!({"data":null}),
            json!({"data":{"markPullRequestReadyForReview":{"pullRequest":{"id":"PR_test","isDraft":false}}},"errors":[{"message":"Partial failure"}]}),
        ] {
            let f = fixture(true, vec![mutation_response, response(true, "accepted")]);
            assert!(
                f.client
                    .set_pull_request_draft("example", "project", 2, false, Some("accepted"))
                    .await
                    .is_err()
            );
            assert_eq!(f.pr.lock().unwrap()["draft"], true);
        }
    }

    #[tokio::test]
    async fn a_push_racing_readiness_restores_draft() {
        let f = fixture(
            true,
            vec![
                response(false, "human-update"),
                response(true, "human-update"),
            ],
        );
        assert!(
            f.client
                .set_pull_request_draft("example", "project", 2, false, Some("accepted"))
                .await
                .is_err()
        );
        assert_eq!(f.pr.lock().unwrap()["draft"], true);
        assert_eq!(f.writes.lock().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn existing_pr_instructions_preserve_description_and_do_not_duplicate_writes() {
        let f = fixture(false, vec![]);
        for _ in 0..2 {
            f.client
                .ensure_revision_instructions("example", "project", 2, "Use /studio revise")
                .await
                .unwrap();
        }
        assert_eq!(f.writes.lock().unwrap().len(), 1);
        let body = f.pr.lock().unwrap()["body"].as_str().unwrap().to_owned();
        assert!(body.starts_with("Maintainer notes."));
        assert!(body.contains("Use /studio revise"));
    }

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
