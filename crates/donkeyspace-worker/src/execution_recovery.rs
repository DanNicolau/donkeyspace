//! Bounded, daemon-local cleanup of durably fenced container executions.
use donkeyspace_db::{PgPool, container_executions::ContainerExecution, execution_recovery as db};
use std::{collections::BTreeMap, time::Duration};
use tokio::process::Command;

type Error = Box<dyn std::error::Error + Send + Sync>;

async fn docker(args: &[&str]) -> Result<String, Error> {
    let output = tokio::time::timeout(
        Duration::from_secs(10),
        Command::new("docker")
            .args(args)
            .kill_on_drop(true)
            .output(),
    )
    .await
    .map_err(|_| "Docker recovery command timed out")??;
    if !output.status.success() {
        // Do not persist Docker stderr: it may contain local configuration.
        return Err("Docker recovery command failed".into());
    }
    Ok(String::from_utf8(output.stdout)?.trim().to_owned())
}

pub(crate) async fn docker_daemon_id() -> Result<String, Error> {
    let id = docker(&["info", "--format", "{{.ID}}"]).await?;
    if id.is_empty() || id == "<no value>" {
        return Err("Docker daemon identity is unavailable".into());
    }
    Ok(id)
}

fn owned_container(execution: &ContainerExecution, value: &serde_json::Value) -> bool {
    let Some(labels) = value.pointer("/Config/Labels") else {
        return false;
    };
    value["Name"].as_str() == Some(format!("/{}", execution.container_name).as_str())
        && labels["donkeyspace.managed"].as_str() == Some("true")
        && labels["donkeyspace.execution-id"].as_str() == Some(execution.id.to_string().as_str())
        && labels["donkeyspace.coordinator-job-id"].as_str()
            == Some(execution.coordinator_job_id.to_string().as_str())
        && labels["donkeyspace.workflow-generation"].as_str()
            == Some(execution.generation.to_string().as_str())
        && labels["donkeyspace.execution-scope"].as_str()
            == Some(execution.execution_scope.as_str())
        && labels["donkeyspace.workflow-id"]
            .as_str()
            .map(str::to_owned)
            == execution.workflow_item_id.map(|id| id.to_string())
}

async fn remove_owned_container(execution: &ContainerExecution) -> Result<(), Error> {
    let filter = format!("name=^/{}$", execution.container_name);
    let ids = docker(&["ps", "--all", "--quiet", "--no-trunc", "--filter", &filter]).await?;
    if ids.is_empty() {
        return Ok(());
    }
    if ids.lines().count() != 1 {
        return Err("Ambiguous container identity; cleanup withheld".into());
    }
    let inspection = docker(&["inspect", &ids]).await;
    // A healthy supervisor or another recovery worker may remove it concurrently.
    if inspection.is_err()
        && docker(&["ps", "-aq", "--filter", &filter])
            .await?
            .is_empty()
    {
        return Ok(());
    }
    let values: Vec<serde_json::Value> = serde_json::from_str(&inspection?)?;
    if values.len() != 1 || !owned_container(execution, &values[0]) {
        return Err("Container ownership mismatch; cleanup withheld".into());
    }
    // Remove the immutable Docker ID, never a name that could now be reused.
    let removal = docker(&["rm", "--force", &ids]).await;
    let remaining = docker(&["ps", "--all", "--quiet", "--filter", &filter]).await?;
    if !remaining.is_empty() {
        removal?;
        return Err("Container still present after cleanup".into());
    }
    Ok(())
}

pub(crate) async fn reconcile(
    pool: &PgPool,
    labels: &BTreeMap<String, String>,
) -> Result<(), Error> {
    db::fence_expired_jobs(pool, 100, labels).await?;
    let daemon = docker_daemon_id().await?;
    for execution in db::cleanup_candidates(pool, &daemon, 100).await? {
        let result = remove_owned_container(&execution).await;
        let error = result.err().map(|error| error.to_string());
        db::record_cleanup(pool, execution.id, error.as_deref()).await?;
        if let Some(error) = error {
            tracing::warn!(execution_id=%execution.id, %error, "container recovery will retry");
        }
    }
    db::finish_recovered_jobs(pool).await?;
    Ok(())
}

/// Runs independently of the serial job loop, which may execute a long agent.
/// A whole-pass deadline also bounds DB lock waits and large cleanup batches.
pub(crate) async fn run(pool: PgPool, labels: BTreeMap<String, String>) {
    loop {
        match tokio::time::timeout(Duration::from_secs(30), reconcile(&pool, &labels)).await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => tracing::warn!(%error, "execution recovery failed; will retry"),
            Err(_) => tracing::warn!("execution recovery pass timed out; will retry"),
        }
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use uuid::Uuid;

    #[test]
    fn cleanup_requires_every_persisted_identity_field() {
        let e = ContainerExecution {
            id: Uuid::now_v7(),
            coordinator_job_id: Uuid::now_v7(),
            workflow_item_id: Some(7),
            generation: 2,
            lease_owner: "worker".into(),
            container_name: "donkeyspace-execution-test".into(),
            execution_scope: "hash".into(),
            docker_daemon_id: Some("test".into()),
        };
        let value = json!({"Name":format!("/{}",e.container_name),"Config":{"Labels":{
            "donkeyspace.managed":"true", "donkeyspace.execution-id":e.id.to_string(),
            "donkeyspace.coordinator-job-id":e.coordinator_job_id.to_string(),
            "donkeyspace.workflow-generation":"2", "donkeyspace.workflow-id":"7",
            "donkeyspace.execution-scope":"hash"
        }}});
        assert!(owned_container(&e, &value));
        for key in value["Config"]["Labels"].as_object().unwrap().keys() {
            let mut wrong = value.clone();
            wrong["Config"]["Labels"][key] = json!("wrong");
            assert!(!owned_container(&e, &wrong), "{key}");
        }
        let mut wrong = value.clone();
        wrong["Name"] = json!("/unrelated");
        assert!(!owned_container(&e, &wrong));
    }
}
