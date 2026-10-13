use super::*;

pub struct GitHubCommentPage {
    pub comments: Vec<Value>,
    pub has_next: bool,
    pub etag: Option<String>,
    pub poll_interval_seconds: Option<u64>,
    pub not_modified: bool,
}

pub(super) struct ConditionalPage {
    pub page: Option<Page<Value>>,
    pub etag: Option<String>,
    pub poll_interval_seconds: Option<u64>,
}

impl GitHubClient {
    /// Includes issue comments and ordinary PR conversation comments. Review
    /// threads are fetched separately when explicitly selected by a command.
    pub async fn repository_comments_page(
        &self,
        owner: &str,
        repo: &str,
        since: &str,
        page: i32,
        etag: Option<&str>,
    ) -> Result<GitHubCommentPage, GitHubClientError> {
        let route = format!(
            "/repos/{owner}/{repo}/issues/comments?sort=created&direction=asc&per_page=100&since={since}&page={page}"
        );
        let response = self.conditional_page(&route, etag).await?;
        let (comments, has_next, not_modified) = match response.page {
            Some(mut page) => {
                let has_next = page.next.is_some();
                (page.take_items(), has_next, false)
            }
            None => (Vec::new(), false, true),
        };
        Ok(GitHubCommentPage {
            comments,
            has_next,
            not_modified,
            etag: response.etag,
            poll_interval_seconds: response.poll_interval_seconds,
        })
    }

    pub(super) async fn conditional_page(
        &self,
        route: &str,
        etag: Option<&str>,
    ) -> Result<ConditionalPage, GitHubClientError> {
        let mut headers = HeaderMap::new();
        if let Some(etag) = etag {
            headers.insert(
                IF_NONE_MATCH,
                HeaderValue::from_str(etag).map_err(|_| {
                    GitHubClientError::InvalidResponse("invalid cached polling ETag".into())
                })?,
            );
        }
        let response = self.client._get_with_headers(route, Some(headers)).await?;
        let status = response.status();
        let response_etag = response
            .headers()
            .get(ETAG)
            .and_then(|v| v.to_str().ok())
            .map(str::to_owned)
            .or_else(|| {
                (status == StatusCode::NOT_MODIFIED)
                    .then(|| etag.map(str::to_owned))
                    .flatten()
            });
        let poll_interval_seconds = response
            .headers()
            .get("x-poll-interval")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse().ok());
        if matches!(
            status,
            StatusCode::FORBIDDEN | StatusCode::TOO_MANY_REQUESTS
        ) {
            let retry_after_seconds = response
                .headers()
                .get("retry-after")
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.parse::<u64>().ok())
                .or_else(|| {
                    let remaining = response
                        .headers()
                        .get("x-ratelimit-remaining")?
                        .to_str()
                        .ok()?;
                    if remaining != "0" {
                        return None;
                    }
                    let reset = response
                        .headers()
                        .get("x-ratelimit-reset")?
                        .to_str()
                        .ok()?
                        .parse::<u64>()
                        .ok()?;
                    let now = SystemTime::now().duration_since(UNIX_EPOCH).ok()?.as_secs();
                    Some(reset.saturating_sub(now).max(1))
                })
                .unwrap_or(60);
            return Err(GitHubClientError::RateLimited {
                retry_after_seconds: retry_after_seconds.max(poll_interval_seconds.unwrap_or(0)),
            });
        }
        let page = if status == StatusCode::NOT_MODIFIED {
            None
        } else if status.is_success() {
            Some(Page::<Value>::from_response(response).await?)
        } else {
            return Err(GitHubClientError::InvalidResponse(format!(
                "github polling returned {status}"
            )));
        };
        Ok(ConditionalPage {
            page,
            etag: response_etag,
            poll_interval_seconds,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use http_body_util::Full;
    use octocrab::{AuthState, OctocrabBuilder};
    use std::{
        convert::Infallible,
        sync::atomic::{AtomicUsize, Ordering},
    };
    use tower::service_fn;

    #[tokio::test]
    async fn comments_support_pagination_conditional_reads_and_server_intervals() {
        let requests = Arc::new(AtomicUsize::new(0));
        let count = requests.clone();
        let service = service_fn(move |request: http::Request<octocrab::OctoBody>| {
            let n = count.fetch_add(1, Ordering::SeqCst);
            async move {
                assert_eq!(request.uri().path(), "/repos/team/project/issues/comments");
                let query = request.uri().query().unwrap();
                assert!(query.contains("since=2026-10-13T12:00:00Z"));
                assert!(query.contains("per_page=100"));
                assert!(query.contains("sort=created&direction=asc"));
                let mut response = http::Response::builder().header("x-poll-interval", "90");
                let body = match n {
                    0 => {
                        assert!(query.contains("page=1"));
                        assert!(request.headers().get(IF_NONE_MATCH).is_none());
                        response = response.header("etag", "\"first\"").header("link", "<https://api.github.com/repos/team/project/issues/comments?page=2>; rel=\"next\"");
                        r#"[{"id":1,"body":"/example revise plan\nUpdate contract"}]"#
                    }
                    1 => {
                        assert!(query.contains("page=2"));
                        assert!(request.headers().get(IF_NONE_MATCH).is_none());
                        response = response.header("etag", "\"second\"");
                        r#"[{"id":2}]"#
                    }
                    _ => {
                        assert!(query.contains("page=1"));
                        assert_eq!(request.headers()[IF_NONE_MATCH], "\"first\"");
                        response = response.status(StatusCode::NOT_MODIFIED);
                        ""
                    }
                };
                Ok::<_, Infallible>(
                    response
                        .header("content-type", "application/json")
                        .body(Full::new(Bytes::from(body)))
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
        let first = client
            .repository_comments_page("team", "project", "2026-10-13T12:00:00Z", 1, None)
            .await
            .unwrap();
        assert!(first.has_next);
        assert_eq!(first.comments[0]["id"], 1);
        assert_eq!(first.poll_interval_seconds, Some(90));
        let second = client
            .repository_comments_page("team", "project", "2026-10-13T12:00:00Z", 2, None)
            .await
            .unwrap();
        assert!(!second.has_next);
        assert_eq!(second.comments[0]["id"], 2);
        let unchanged = client
            .repository_comments_page(
                "team",
                "project",
                "2026-10-13T12:00:00Z",
                1,
                first.etag.as_deref(),
            )
            .await
            .unwrap();
        assert!(unchanged.not_modified);
        assert!(unchanged.comments.is_empty());
        assert_eq!(unchanged.etag, first.etag);
        assert_eq!(requests.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn failed_comment_pages_propagate_retry_headers_and_never_look_empty() {
        for (status, headers, wait) in [
            (429, vec![("retry-after", "120")], Some(120)),
            (
                429,
                vec![("retry-after", "30"), ("x-poll-interval", "90")],
                Some(90),
            ),
            (
                403,
                vec![
                    ("x-ratelimit-remaining", "0"),
                    ("x-ratelimit-reset", "4102444800"),
                ],
                Some(1),
            ),
            (403, vec![], Some(60)),
            (500, vec![], None),
        ] {
            let service = service_fn(move |_: http::Request<octocrab::OctoBody>| {
                let headers = headers.clone();
                async move {
                    let mut response = http::Response::builder().status(status);
                    for (name, value) in headers {
                        response = response.header(name, value);
                    }
                    Ok::<_, Infallible>(
                        response.body(Full::new(Bytes::from_static(b"{}"))).unwrap(),
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
            let error = client
                .repository_comments_page("team", "project", "2026-10-13T12:00:00Z", 2, None)
                .await
                .err()
                .expect("page should fail");
            match wait {
                Some(1) => assert!(error.retry_after_seconds().unwrap() > 60),
                _ => assert_eq!(error.retry_after_seconds(), wait),
            }
        }
    }
}
