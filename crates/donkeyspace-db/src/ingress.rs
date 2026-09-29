//! Admission snapshots keep network authorization outside database transactions.
use crate::{DbError, PgConnection, PgPool};
use sqlx::FromRow;

#[derive(Debug, PartialEq, Eq, FromRow)]
pub struct AdmissionSnapshot {
    pub generation: i64,
    pub provider_state: String,
    pub current_state: Option<String>,
    pub current_labels: serde_json::Value,
}

pub async fn snapshot(pool: &PgPool, workflow: i64) -> Result<AdmissionSnapshot, DbError> {
    Ok(sqlx::query_as("SELECT generation, provider_state, current_state, current_labels FROM workflow_items WHERE id=$1")
        .bind(workflow).fetch_one(pool).await?)
}

pub async fn lock_workflow(connection: &mut PgConnection, workflow: i64) -> Result<(), DbError> {
    sqlx::query("SELECT id FROM workflow_items WHERE id=$1 FOR NO KEY UPDATE")
        .bind(workflow)
        .fetch_one(connection)
        .await?;
    Ok(())
}

/// Caller retains this row lock until the receipt and all effects commit.
pub async fn lock_snapshot(
    connection: &mut PgConnection,
    workflow: i64,
    expected: &AdmissionSnapshot,
) -> Result<(), DbError> {
    let current: AdmissionSnapshot = sqlx::query_as("SELECT generation, provider_state, current_state, current_labels FROM workflow_items WHERE id=$1 FOR NO KEY UPDATE")
        .bind(workflow).fetch_one(connection).await?;
    if &current != expected {
        return Err(DbError::StaleAdmission);
    }
    Ok(())
}

/// A push candidate was selected before acquiring its workflow lock. Recheck
/// its generation under that lock before committing any delivery effects.
pub async fn lock_generation(
    connection: &mut PgConnection,
    workflow: i64,
    generation: i64,
) -> Result<(), DbError> {
    let current: bool = sqlx::query_scalar("SELECT workflow_repository_tracked(id) AND generation=$2 AND provider_state <> 'closed' FROM workflow_items WHERE id=$1 FOR NO KEY UPDATE")
        .bind(workflow).bind(generation).fetch_one(connection).await?;
    if !current {
        return Err(DbError::StaleAdmission);
    }
    Ok(())
}
