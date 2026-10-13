//! Direct comment discovery avoids the repository events feed's delivery delay.
//! All commands still pass through the ordinary authorization/admission path.
use super::*;
use donkeyspace_db::comment_polling::{self, Cursor};
use donkeyspace_github::GitHubCommentPage;

pub(super) async fn poll(
    state: &AppState,
    client: &GitHubClient,
    repository: &PolledRepository,
    repository_id: i64,
    repository_payload: &Value,
    max_pages: usize,
) -> Result<Option<Duration>, Box<dyn std::error::Error + Send + Sync>> {
    let pool = state.pool.as_ref().ok_or("database is not configured")?;
    let mut cursor = comment_polling::load(pool, repository_id).await?;
    let mut interval = None;
    for _ in 0..max_pages.max(1) {
        let started = Utc::now();
        let response = client
            .repository_comments_page(
                &repository.owner,
                &repository.name,
                &cursor
                    .since
                    .to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
                cursor.next_page,
                cursor.etag.as_deref(),
            )
            .await?;
        interval = interval.max(response.poll_interval_seconds.map(Duration::from_secs));
        // Preserve the server's minimum even if later comment admission or
        // event polling fails and this cycle exits through the backoff path.
        state
            .github_poller
            .observe_server_interval(repository, response.poll_interval_seconds)
            .await;
        for comment in &response.comments {
            if !human_comment(comment) {
                continue;
            }
            let id = comment["id"].as_i64().ok_or("comment ID is missing")?;
            if donkeyspace_db::github_comment_delivery_exists(pool, repository_id, id).await? {
                continue;
            }
            let number = issue_number(comment).ok_or("comment issue URL is invalid")?;
            // Fetch current issue/PR labels and state instead of reusing stale
            // event data. The issue endpoint also identifies PR conversations.
            let issue = client
                .issue(&repository.owner, &repository.name, number)
                .await?;
            let mut payload = json!({
                "action":"created", "repository":repository_payload,
                "issue":issue, "comment":comment, "sender":comment["user"],
            });
            if let Some(id) = state
                .github_auth
                .as_ref()
                .and_then(GitHubCredentialProvider::installation_id)
            {
                payload["installation"] = json!({"id":id});
            }
            let delivery = format!(
                "github-poll:{}/{}:comment:{id}",
                repository.owner, repository.name
            );
            let outcome = persist_github_webhook(
                pool,
                state,
                "issue_comment",
                &delivery,
                &serde_json::to_vec(&payload)?,
            )
            .await
            .map_err(|e| std::io::Error::other(e.to_string()))?;
            if let WebhookPersistOutcome::Queued(job) = outcome {
                tracing::info!(job_id=%job.id, comment_id=id, "queued direct GitHub comment polling job");
            } else {
                // Some intentionally ignored PR comments return before normal
                // admission records a receipt. Remember them too, so an idle
                // poll does not repeatedly fetch their issue context.
                record_webhook_delivery(
                    pool,
                    Some(repository_id),
                    &delivery,
                    "issue_comment",
                    &payload,
                )
                .await?;
            }
        }
        advance(&mut cursor, &response, started)?;
        comment_polling::save(pool, &mut cursor).await?;
        if !response.has_next || response.poll_interval_seconds.is_some() {
            break;
        }
    }
    Ok(interval)
}

fn human_comment(comment: &Value) -> bool {
    comment["user"]["type"] == "User"
        && comment["performed_via_github_app"].is_null()
        && comment["body"]
            .as_str()
            .is_some_and(|body| !body.contains("<!-- donkeyspace-generated -->"))
}

fn issue_number(comment: &Value) -> Option<i64> {
    let url = comment["issue_url"].as_str()?;
    let (prefix, number) = url.rsplit_once('/')?;
    prefix.ends_with("/issues").then_some(())?;
    number.parse().ok().filter(|number| *number > 0)
}

fn advance(
    cursor: &mut Cursor,
    response: &GitHubCommentPage,
    started: DateTime<Utc>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    if response.not_modified {
        cursor.etag = response.etag.clone();
        return Ok(());
    }
    let scan_started = *cursor.scan_started_at.get_or_insert(started);
    for comment in &response.comments {
        let updated = comment["updated_at"]
            .as_str()
            .ok_or("comment updated_at is missing")?
            .parse::<DateTime<Utc>>()?;
        cursor.scan_latest_at = cursor.scan_latest_at.max(Some(updated));
    }
    if response.has_next {
        cursor.next_page = cursor
            .next_page
            .checked_add(1)
            .ok_or("comment page overflow")?;
        cursor.etag = None;
    } else {
        // Replay the boundary second and retain a minute of overlap around the
        // scan start to catch arrivals/edits during pagination. Receipts dedupe
        // these overlaps. Idle scans keep the same URL, allowing 304 responses.
        let since = cursor
            .scan_latest_at
            .map(|latest| {
                (latest - chrono::Duration::seconds(1))
                    .min(scan_started - chrono::Duration::minutes(1))
                    .max(cursor.since)
            })
            .unwrap_or(cursor.since);
        cursor.etag = if cursor.next_page == 1 && since == cursor.since {
            response.etag.clone()
        } else {
            None
        };
        cursor.since = since;
        cursor.next_page = 1;
        cursor.scan_started_at = None;
        cursor.scan_latest_at = None;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn time(value: &str) -> DateTime<Utc> {
        value.parse().unwrap()
    }

    fn cursor() -> Cursor {
        Cursor {
            repository_id: 1,
            since: time("2026-10-13T12:00:00Z"),
            next_page: 1,
            scan_started_at: None,
            scan_latest_at: None,
            etag: None,
            revision: 0,
        }
    }

    fn page(updated: &str, has_next: bool) -> GitHubCommentPage {
        GitHubCommentPage {
            comments: vec![json!({"updated_at":updated})],
            has_next,
            not_modified: false,
            etag: Some("page-tag".into()),
            poll_interval_seconds: None,
        }
    }

    #[test]
    fn bounded_scan_keeps_its_window_until_the_last_page_and_replays_the_boundary() {
        let mut saved = cursor();
        let started = time("2026-10-13T13:00:00Z");
        advance(&mut saved, &page("2026-10-13T12:30:00Z", true), started).unwrap();
        assert_eq!(saved.next_page, 2);
        assert_eq!(saved.since, cursor().since);
        assert_eq!(saved.scan_started_at, Some(started));
        assert!(saved.etag.is_none());
        // A later cycle finishes the scan, with comments sharing a timestamp.
        advance(
            &mut saved,
            &page("2026-10-13T12:30:00Z", false),
            started + chrono::Duration::minutes(1),
        )
        .unwrap();
        assert_eq!(saved.since, time("2026-10-13T12:29:59Z"));
        assert_eq!(saved.next_page, 1);
        assert!(saved.scan_started_at.is_none());
        assert!(saved.etag.is_none());
        // The boundary comment is seen again; a stable URL can now cache its ETag.
        advance(&mut saved, &page("2026-10-13T12:30:00Z", false), started).unwrap();
        assert_eq!(saved.etag.as_deref(), Some("page-tag"));
        let mut unchanged = page("2026-10-13T12:30:00Z", false);
        unchanged.comments.clear();
        unchanged.not_modified = true;
        advance(&mut saved, &unchanged, started).unwrap();
        assert_eq!(saved.since, time("2026-10-13T12:29:59Z"));
        assert_eq!(saved.etag.as_deref(), Some("page-tag"));
    }

    #[test]
    fn arrivals_during_scan_retain_overlap_and_empty_scans_keep_a_stable_url() {
        let mut saved = cursor();
        let started = time("2026-10-13T13:00:00Z");
        advance(&mut saved, &page("2026-10-13T13:05:00Z", false), started).unwrap();
        assert_eq!(saved.since, time("2026-10-13T12:59:00Z"));
        let mut empty = page("", false);
        empty.comments.clear();
        advance(&mut saved, &empty, started).unwrap();
        assert_eq!(saved.since, time("2026-10-13T12:59:00Z"));
        assert_eq!(saved.etag.as_deref(), Some("page-tag"));
    }

    #[test]
    fn direct_discovery_ignores_bots_and_generated_comments() {
        let comment = json!({"user":{"type":"User"},"body":"/example revise plan\nUpdate contract", "issue_url":"https://api.github.com/repos/team/project/issues/41"});
        assert!(human_comment(&comment));
        assert_eq!(issue_number(&comment), Some(41));
        for (path, value) in [
            ("/user/type", json!("Bot")),
            ("/body", json!("<!-- donkeyspace-generated -->")),
        ] {
            let mut ignored = comment.clone();
            *ignored.pointer_mut(path).unwrap() = value;
            assert!(!human_comment(&ignored));
        }
        let mut via_app = comment;
        via_app["performed_via_github_app"] = json!({"id":1});
        assert!(!human_comment(&via_app));
    }
}
