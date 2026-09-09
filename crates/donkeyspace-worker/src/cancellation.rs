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
    let _guard = donkeyspace_db::cancellation::lock_job_side_effect(pool, job).await?;
    #[cfg(unix)]
    let result = donkeyspace_runner::process::run_cancellable_execution(
        work,
        tokio::time::sleep(Duration::from_secs(60)),
    )
    .await?;
    #[cfg(not(unix))]
    let result = tokio::time::timeout(Duration::from_secs(60), work)
        .await
        .ok();
    result
        .ok_or("remote side effect timed out")?
        .map_err(Into::into)
}
