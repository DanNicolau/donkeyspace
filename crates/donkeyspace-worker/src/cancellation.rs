use donkeyspace_db::cancellation::{finish_job_cancellation, heartbeat_job, job_execution_allowed};
use donkeyspace_db::{JobRecord, PgPool, get_job};
use std::{future::Future, time::Duration};

pub async fn execute_until_cancelled<F>(
    pool: &PgPool,
    job: &JobRecord,
    lease_seconds: i32,
    work: F,
) -> Result<(), Box<dyn std::error::Error>>
where
    F: Future<Output = Result<(), Box<dyn std::error::Error>>>,
{
    let owner = job.lease_owner.as_deref().ok_or("job has no lease owner")?;
    let monitor = async {
        loop {
            match job_execution_allowed(pool, job.id).await {
                Ok(true) => {}
                Ok(false) => break,
                Err(error) => {
                    tracing::error!(job_id=%job.id, %error, "cancelling execution after database monitoring failed");
                    break;
                }
            }
            // The owner is checked on every heartbeat. Never renew another
            // worker's lease, even if this execution resumed after a stall.
            match heartbeat_job(pool, job.id, owner, lease_seconds).await {
                Ok(true) => {}
                Ok(false) => match get_job(pool, job.id).await {
                    Ok(Some(current))
                        if matches!(current.status.as_str(), "completed" | "failed" | "paused") => {
                    }
                    _ => break,
                },
                Err(_) => break,
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    };
    #[cfg(unix)]
    let result = donkeyspace_runner::process::run_cancellable_execution(work, monitor).await?;
    #[cfg(not(unix))]
    let result = {
        tokio::pin!(work, monitor);
        tokio::select! { result = &mut work => Some(result), _ = &mut monitor => None }
    };
    // Closure may race with a final publication/state write and make work
    // return before the monitor gets another poll. Acknowledge that path too,
    // after the execution scope has drained its supervisors.
    if !job_execution_allowed(pool, job.id).await? {
        finish_job_cancellation(pool, job.id).await?;
        tracing::info!(job_id=%job.id, "workflow execution stopped; process cleanup completed");
        return Ok(());
    }
    match result {
        Some(result) => result,
        None => Err("execution stopped after losing database monitoring or lease ownership".into()),
    }
}

pub async fn side_effect<T, E: Into<Box<dyn std::error::Error>>>(
    pool: &PgPool,
    job: uuid::Uuid,
    work: impl Future<Output = Result<T, E>>,
) -> Result<T, Box<dyn std::error::Error>> {
    side_effect_with_timeout(pool, job, work, Duration::from_secs(60)).await
}

async fn side_effect_with_timeout<T, E: Into<Box<dyn std::error::Error>>>(
    pool: &PgPool,
    job: uuid::Uuid,
    work: impl Future<Output = Result<T, E>>,
    timeout: Duration,
) -> Result<T, Box<dyn std::error::Error>> {
    let _guard = donkeyspace_db::cancellation::lock_job_side_effect(pool, job).await?;
    #[cfg(unix)]
    let result =
        donkeyspace_runner::process::run_cancellable_execution(work, tokio::time::sleep(timeout))
            .await?;
    #[cfg(not(unix))]
    let result = tokio::time::timeout(timeout, work).await.ok();
    result
        .ok_or("remote side effect timed out")?
        .map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use super::*;
    use donkeyspace_db::cancellation::{IssueObservation, lock_job_side_effect, observe_issue};
    use donkeyspace_db::{
        DbConfig, RepositoryInput, WorkflowItemInput, apply_migrations, connect, create_job,
        upsert_repository,
    };
    use serde_json::json;
    use tokio::{io::AsyncReadExt, net::TcpListener, sync::oneshot};

    #[tokio::test]
    #[ignore = "requires isolated DONKEYSPACE_CANCELLATION_TEST_DATABASE_URL"]
    async fn stalled_projection_releases_lock_so_closure_can_commit() {
        let url = std::env::var("DONKEYSPACE_CANCELLATION_TEST_DATABASE_URL").unwrap();
        assert!(url.ends_with("/donkeyspace_cancellation_test"));
        let pool = connect(&DbConfig::from_database_url(url)).await.unwrap();
        apply_migrations(&pool).await.unwrap();
        let repository = upsert_repository(
            &pool,
            &RepositoryInput {
                installation_external_id: None,
                installation_account_login: None,
                provider: "github".into(),
                owner: format!("projection-{}", uuid::Uuid::now_v7()),
                name: "umbrella".into(),
                default_branch: "main".into(),
            },
        )
        .await
        .unwrap();
        let mut issue = WorkflowItemInput {
            repository_id: repository,
            provider_issue_id: "1".into(),
            issue_number: 1,
            provider_state: "open".into(),
            current_state: Some("in_progress".into()),
            current_labels: vec![],
        };
        let workflow = observe_issue(
            &pool,
            &IssueObservation {
                issue: &issue,
                updated_at: None,
                close_reason: None,
                owner: "test",
                repo: "umbrella",
                state_labels: vec![],
            },
        )
        .await
        .unwrap()
        .unwrap();
        let job = create_job(&pool, Some(workflow), "test", &json!({}))
            .await
            .unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let client = octocrab::Octocrab::builder()
            .personal_token("test-only".to_string())
            .base_uri(format!("http://{}", listener.local_addr().unwrap()))
            .unwrap()
            .build()
            .unwrap();
        let (received, request_received) = oneshot::channel();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut bytes = [0; 4096];
            let read = socket.read(&mut bytes).await.unwrap();
            assert!(String::from_utf8_lossy(&bytes[..read]).starts_with("PATCH "));
            received.send(()).unwrap();
            // Accept the actual projection HTTP request, but never respond.
            let _socket = socket;
            std::future::pending::<()>().await;
        });
        let projection = side_effect_with_timeout(
            &pool,
            job.id,
            async {
                client
                    .issues("test", "umbrella")
                    .update(1)
                    .state(octocrab::models::IssueState::Closed)
                    .send()
                    .await?;
                Ok::<_, octocrab::Error>(())
            },
            Duration::from_millis(500),
        );
        issue.provider_state = "closed".into();
        let close = async {
            request_received.await.unwrap();
            observe_issue(
                &pool,
                &IssueObservation {
                    issue: &issue,
                    updated_at: None,
                    close_reason: Some("completed"),
                    owner: "test",
                    repo: "umbrella",
                    state_labels: vec![],
                },
            )
            .await
        };
        let result = tokio::time::timeout(Duration::from_secs(3), async {
            tokio::join!(projection, close)
        })
        .await;
        server.abort();
        let _ = server.await;
        let (projection, closed) = result.expect("stalled projection kept closure locked");
        assert!(projection.unwrap_err().to_string().contains("timed out"));
        assert_eq!(closed.unwrap(), Some(workflow));
        assert!(!job_execution_allowed(&pool, job.id).await.unwrap());
        assert!(lock_job_side_effect(&pool, job.id).await.is_err());
    }
}
