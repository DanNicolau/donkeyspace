//! A lifecycle checkpoint names immutable repository content, never a moving branch.
use crate::{publication::PublicationContext, repository_files, trusted_git};
use donkeyspace_db::{PgPool, lifecycle_checkpoints, list_agent_publications_for_run};
use serde::{Deserialize, Serialize};
use std::{fs, path::Path};
use uuid::Uuid;

type Error = Box<dyn std::error::Error>;

#[derive(Debug)]
pub struct RecoveryBlocked(pub String);
impl std::fmt::Display for RecoveryBlocked {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}
impl std::error::Error for RecoveryBlocked {}
fn blocked(message: impl Into<String>) -> Error {
    Box::new(RecoveryBlocked(message.into()))
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Snapshot {
    owner: String,
    repo: String,
    default_branch: String,
    commit_sha: String,
    tree_sha: String,
    base_sha: String,
    publication_id: Option<i64>,
}

impl Snapshot {
    fn base_ref(&self) -> String {
        format!("refs/remotes/origin/{}", self.default_branch)
    }
    fn validate(&self, owner: &str, repo: &str, branch: &str) -> Result<(), Error> {
        if !self.owner.eq_ignore_ascii_case(owner)
            || !self.repo.eq_ignore_ascii_case(repo)
            || self.default_branch != branch
        {
            return Err(blocked(
                "Checkpoint repository or base branch differs from the resumed request; restore the original context or start a new approved run.",
            ));
        }
        if [&self.commit_sha, &self.tree_sha, &self.base_sha]
            .iter()
            .any(|sha| !matches!(sha.len(), 40 | 64) || !sha.bytes().all(|c| c.is_ascii_hexdigit()))
        {
            return Err(blocked(
                "Checkpoint contains an invalid immutable Git identity.",
            ));
        }
        Ok(())
    }
}

pub async fn capture(
    context: &PublicationContext<'_>,
    repo: &Path,
    branch: &str,
) -> Result<Snapshot, Error> {
    let commit_sha = git_value(repo, "HEAD").await?;
    let tree_sha = git_value(repo, "HEAD^{tree}").await?;
    let base_sha = git_value(repo, &format!("refs/remotes/origin/{branch}")).await?;
    require_clean(repo).await?;
    let publications =
        list_agent_publications_for_run(context.pool, context.coordinator_job_id, None).await?;
    let publication_id = publications
        .iter()
        .find(|p| p.kind == "checkpoint" && p.status != "cancelled" && p.commit_sha == commit_sha)
        .map(|p| p.id);
    if publication_id.is_none() && commit_sha != base_sha {
        return Err(blocked(
            "Lifecycle files have no matching checkpoint publication; refusing to record them as accepted work.",
        ));
    }
    Ok(Snapshot {
        owner: context.owner.into(),
        repo: context.repo.into(),
        default_branch: branch.into(),
        commit_sha,
        tree_sha,
        base_sha,
        publication_id,
    })
}

async fn git_value(repo: &Path, revision: &str) -> Result<String, Error> {
    Ok(
        trusted_git::run(repo, &["rev-parse", "--verify", revision], None)
            .await?
            .trim()
            .to_string(),
    )
}

async fn require_clean(repo: &Path) -> Result<(), Error> {
    if !trusted_git::run(
        repo,
        &["status", "--porcelain", "--untracked-files=all"],
        None,
    )
    .await?
    .trim()
    .is_empty()
    {
        return Err(blocked(
            "Lifecycle checkout differs from its committed checkpoint; uncommitted files cannot be treated as accepted work.",
        ));
    }
    Ok(())
}

async fn verify(repo: &Path, snapshot: &Snapshot) -> Result<(), Error> {
    if git_value(repo, "HEAD").await? != snapshot.commit_sha
        || git_value(repo, "HEAD^{tree}").await? != snapshot.tree_sha
        || git_value(repo, &snapshot.base_ref()).await? != snapshot.base_sha
    {
        return Err(blocked(
            "Lifecycle checkout does not match the recorded checkpoint revision.",
        ));
    }
    require_clean(repo).await
}

/// Restore only the saved revision. A valid local checkout is preferred; a lost
/// checkout can use an independent forensic clone or exact objects from GitHub.
#[allow(clippy::too_many_arguments)]
pub async fn ensure(
    pool: &PgPool,
    coordinator: Uuid,
    repo_path: &Path,
    owner: &str,
    repo: &str,
    branch: &str,
    token: Option<&str>,
) -> Result<(), Error> {
    ensure_with_remote(
        pool,
        coordinator,
        repo_path,
        owner,
        repo,
        branch,
        async |snapshot, destination| {
            trusted_git::restore_remote(
                snapshot.owner.as_str(),
                snapshot.repo.as_str(),
                &snapshot.commit_sha,
                &snapshot.base_sha,
                destination,
                token,
            )
            .await
        },
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn ensure_with_remote(
    pool: &PgPool,
    coordinator: Uuid,
    repo_path: &Path,
    owner: &str,
    repo: &str,
    branch: &str,
    remote: impl AsyncFn(&Snapshot, &Path) -> Result<(), Error>,
) -> Result<(), Error> {
    if !donkeyspace_db::cancellation::job_execution_allowed(pool, coordinator).await? {
        return Err(donkeyspace_db::DbError::ExecutionCancelled.into());
    }
    let checkpoint = lifecycle_checkpoints::load(pool, coordinator).await?
        .ok_or_else(|| blocked("Paused lifecycle state is missing. Restore its authoritative checkpoint or explicitly start a new run; accepted work will not be reconstructed from main."))?;
    if checkpoint.completed {
        return Err(blocked(
            "A completed lifecycle checkpoint cannot be resumed.",
        ));
    }
    let snapshot: Snapshot = serde_json::from_value(checkpoint.state.get("repository_snapshot").cloned()
        .ok_or_else(|| blocked("This checkpoint has no immutable repository provenance. Restore and reconcile the original accepted revision, or start a new run with renewed approval."))?)
        .map_err(|error| blocked(format!("Cannot read checkpoint repository provenance: {error}")))?;
    snapshot.validate(owner, repo, branch)?;
    let publications = list_agent_publications_for_run(pool, coordinator, None).await?;
    if let Some(id) = snapshot.publication_id {
        if !publications.iter().any(|p| {
            p.id == id
                && p.kind == "checkpoint"
                && p.status != "cancelled"
                && p.commit_sha == snapshot.commit_sha
                && p.metadata
                    .get("owner")
                    .and_then(serde_json::Value::as_str)
                    .is_some_and(|v| v.eq_ignore_ascii_case(owner))
                && p.metadata
                    .get("repo")
                    .and_then(serde_json::Value::as_str)
                    .is_some_and(|v| v.eq_ignore_ascii_case(repo))
        }) {
            return Err(blocked(
                "The checkpoint's publication record is missing or inconsistent; accepted provenance cannot be inferred from another branch.",
            ));
        }
    } else if snapshot.commit_sha != snapshot.base_sha {
        return Err(blocked(
            "The checkpoint has generated content without a publication identity.",
        ));
    }
    if repo_path.exists() && verify(repo_path, &snapshot).await.is_ok() {
        return Ok(());
    }
    let workspace = repo_path.parent().ok_or("checkout has no workspace")?;
    repository_files::validate_directory(workspace)?;
    repository_files::validate_directory(repo_path)?;
    fs::create_dir_all(workspace)?;
    let temporary = workspace.join(format!("restore-{}", Uuid::now_v7()));
    struct Cleanup(std::path::PathBuf);
    impl Drop for Cleanup {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }
    let _cleanup = Cleanup(temporary.clone());
    let sources = std::iter::once(repo_path.to_path_buf()).chain(
        publications
            .iter()
            .map(|p| p.local_repo_path.clone().into()),
    );
    let mut restored = false;
    for source in sources {
        if !source.join(".git").is_dir() {
            continue;
        }
        let attempt = async {
            trusted_git::clone_local(&source, &temporary).await?;
            checkout_exact(&temporary, &snapshot).await
        }
        .await;
        if attempt.is_ok() {
            restored = true;
            break;
        }
        if temporary.exists() {
            fs::remove_dir_all(&temporary)?;
        }
    }
    if !restored {
        let attempt = async {
            remote(&snapshot, &temporary).await?;
            checkout_exact(&temporary, &snapshot).await
        }
        .await;
        if let Err(error) = attempt {
            return Err(blocked(format!(
                "Cannot recover exact checkpoint {} (base {}): {error}. Restore that revision or explicitly restart with renewed approval; no replacement contract was accepted.",
                snapshot.commit_sha, snapshot.base_sha
            )));
        }
    }
    // Do not discard unexpected local work. Publish the verified replacement
    // only after all immutable identities and the clean worktree have matched.
    let _guard = donkeyspace_db::cancellation::lock_job_side_effect(pool, coordinator).await?;
    if repo_path.exists() {
        fs::rename(
            repo_path,
            workspace.join(format!("repo-unverified-{}", Uuid::now_v7())),
        )?;
    }
    fs::rename(&temporary, repo_path)?;
    Ok(())
}

async fn checkout_exact(repo: &Path, snapshot: &Snapshot) -> Result<(), Error> {
    trusted_git::run(repo, &["checkout", "--detach", &snapshot.commit_sha], None).await?;
    trusted_git::run(
        repo,
        &["update-ref", &snapshot.base_ref(), &snapshot.base_sha],
        None,
    )
    .await?;
    verify(repo, snapshot).await
}
