use donkeyspace_core::{Outcome, PluginArtifact, PluginArtifactType};
use donkeyspace_db::{
    AgentPublicationInput, AgentPublicationRecord, OutboundActionInput, PgPool,
    create_outbound_action, list_agent_publications_for_run, mark_agent_publication_failed,
    mark_agent_publication_published, upsert_agent_publication,
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{env, fs, io::Read, path::Path, process::Stdio};
use tokio::process::Command;
use uuid::Uuid;

use crate::repo_context::write_askpass_script;

const MAX_DIAGNOSTIC_FILES: usize = 256;
const MAX_DIAGNOSTIC_FILE_BYTES: u64 = 5 * 1024 * 1024;
const MAX_DIAGNOSTIC_TOTAL_BYTES: u64 = 20 * 1024 * 1024;

#[derive(Clone, Copy)]
pub struct PublicationContext<'a> {
    pub pool: &'a PgPool,
    pub coordinator_job_id: Uuid,
    pub workflow_item_id: Option<i64>,
    pub issue_number: i64,
    pub owner: &'a str,
    pub repo: &'a str,
    pub workspace_path: &'a Path,
    pub token: Option<&'a str>,
}

pub struct AttemptPublication<'a> {
    pub job_id: Option<Uuid>,
    pub task: &'a str,
    pub work_item: Option<&'a str>,
    pub attempt: u32,
    pub outcome: Option<Outcome>,
    pub task_root: &'a Path,
    pub write_roots: &'a [String],
    pub diagnostics: &'a [PluginArtifact],
    pub reason: &'a str,
    pub related_issue_number: Option<i64>,
    pub redactions: &'a [String],
}

pub fn issue_branch_name(issue_number: i64, coordinator_job_id: Uuid) -> String {
    format!(
        "donkeyspace/issue-{issue_number}-{}",
        short_uuid(coordinator_job_id)
    )
}

pub async fn publish_checkpoint(
    context: &PublicationContext<'_>,
    repo_path: &Path,
    commit_title: &str,
) -> Result<AgentPublicationRecord, Box<dyn std::error::Error>> {
    let branch = issue_branch_name(context.issue_number, context.coordinator_job_id);
    configure_git_author(repo_path).await?;
    let current = git(repo_path, &["branch", "--show-current"], None, None).await?;
    if current.trim() != branch {
        git(repo_path, &["checkout", "-b", &branch], None, None).await?;
    }
    if !git_status(repo_path).await?.trim().is_empty() {
        git(repo_path, &["add", "-A"], None, None).await?;
        git(repo_path, &["commit", "-m", commit_title], None, None).await?;
    }
    let sha = git(repo_path, &["rev-parse", "HEAD"], None, None)
        .await?
        .trim()
        .to_string();
    let record = upsert_agent_publication(
        context.pool,
        &AgentPublicationInput {
            coordinator_job_id: context.coordinator_job_id,
            job_id: Some(context.coordinator_job_id),
            workflow_item_id: context.workflow_item_id,
            kind: "checkpoint".into(),
            branch_name: branch.clone(),
            commit_sha: sha,
            html_url: branch_url(context.owner, context.repo, &branch),
            local_repo_path: repo_path.display().to_string(),
            task: None,
            work_item: None,
            attempt: None,
            outcome: None,
            metadata: json!({
                "summary": commit_title,
                "owner": context.owner,
                "repo": context.repo,
                "issue_number": context.issue_number,
            }),
        },
    )
    .await?;
    let push_result = push_publication(context, &record).await;
    queue_status_comment(context, None).await?;
    push_result?;
    Ok(record)
}

pub async fn publish_attempt(
    context: &PublicationContext<'_>,
    aggregate_repo: &Path,
    attempt: &AttemptPublication<'_>,
) -> Result<AgentPublicationRecord, Box<dyn std::error::Error>> {
    let branch = attempt_branch_name(context.issue_number, context.coordinator_job_id, attempt);
    let local_repo = context
        .workspace_path
        .join("publication-attempts")
        .join(safe_segment(&branch));
    if !local_repo.exists() {
        fs::create_dir_all(local_repo.parent().expect("attempt repository has parent"))?;
        let output = Command::new("git")
            .args(["clone", "--shared", "--no-hardlinks"])
            .arg(aggregate_repo)
            .arg(&local_repo)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .output()
            .await?;
        if !output.status.success() {
            return Err(format!(
                "git clone for forensic branch failed: {}",
                String::from_utf8_lossy(&output.stderr).trim()
            )
            .into());
        }
    }
    configure_git_author(&local_repo).await?;
    for root in attempt.write_roots {
        sync_root(
            &attempt.task_root.join("repo").join(root),
            &local_repo.join(root),
        )?;
    }
    let diagnostic_root = local_repo.join(".donkeyspace/diagnostics").join(format!(
        "{}-{}-a{}",
        safe_segment(attempt.task),
        safe_segment(attempt.work_item.unwrap_or("workflow")),
        attempt.attempt
    ));
    fs::create_dir_all(&diagnostic_root)?;
    let mut manifest = Vec::<Value>::new();
    let mut budget = DiagnosticBudget::default();
    for name in [
        "agent.stdout.log",
        "agent.stderr.log",
        "run-input.json",
        "run-result.json",
        "required-checks.json",
    ] {
        copy_diagnostic_file(
            &attempt.task_root.join(".donkeyspace").join(name),
            &diagnostic_root.join(name),
            &mut budget,
            &mut manifest,
        )?;
    }
    for diagnostic in attempt.diagnostics {
        let source = attempt.task_root.join("repo").join(&diagnostic.path);
        let target = diagnostic_root.join("artifacts").join(&diagnostic.path);
        collect_diagnostic(
            &source,
            &target,
            diagnostic.kind,
            &mut budget,
            &mut manifest,
        )?;
    }
    redact_diagnostic_tree(&diagnostic_root, context.token, attempt.redactions)?;
    fs::write(
        diagnostic_root.join("manifest.json"),
        serde_json::to_vec_pretty(&json!({
            "task": attempt.task,
            "work_item": attempt.work_item,
            "attempt": attempt.attempt,
            "outcome": attempt.outcome,
            "reason": attempt.reason,
            "limits": {
                "max_files": MAX_DIAGNOSTIC_FILES,
                "max_file_bytes": MAX_DIAGNOSTIC_FILE_BYTES,
                "max_total_bytes": MAX_DIAGNOSTIC_TOTAL_BYTES,
            },
            "files": manifest,
        }))?,
    )?;
    git(&local_repo, &["add", "-A"], None, None).await?;
    let diagnostic_relative = diagnostic_root.strip_prefix(&local_repo)?.to_string_lossy();
    git(
        &local_repo,
        &["add", "-f", "--", diagnostic_relative.as_ref()],
        None,
        None,
    )
    .await?;
    git(
        &local_repo,
        &[
            "commit",
            "-m",
            &format!(
                "chore(donkeyspace): preserve {} attempt for issue #{}",
                attempt.task, context.issue_number
            ),
        ],
        None,
        None,
    )
    .await?;
    let sha = git(&local_repo, &["rev-parse", "HEAD"], None, None)
        .await?
        .trim()
        .to_string();
    let record = upsert_agent_publication(
        context.pool,
        &AgentPublicationInput {
            coordinator_job_id: context.coordinator_job_id,
            job_id: attempt.job_id,
            workflow_item_id: context.workflow_item_id,
            kind: "attempt".into(),
            branch_name: branch.clone(),
            commit_sha: sha,
            html_url: branch_url(context.owner, context.repo, &branch),
            local_repo_path: local_repo.display().to_string(),
            task: Some(attempt.task.into()),
            work_item: attempt.work_item.map(Into::into),
            attempt: Some(attempt.attempt as i32),
            outcome: attempt
                .outcome
                .map(|outcome| format!("{outcome:?}").to_lowercase()),
            metadata: json!({
                "reason": attempt.reason,
                "diagnostic_files": budget.files,
                "owner": context.owner,
                "repo": context.repo,
                "issue_number": context.issue_number,
                "related_issue_number": attempt.related_issue_number,
            }),
        },
    )
    .await?;
    let push_result = push_publication(context, &record).await;
    queue_status_comment(context, None).await?;
    if let Some(issue_number) = attempt.related_issue_number {
        queue_status_comment(
            context,
            Some((issue_number, attempt.work_item.unwrap_or(attempt.task))),
        )
        .await?;
    }
    push_result?;
    Ok(record)
}

async fn queue_status_comment(
    context: &PublicationContext<'_>,
    related: Option<(i64, &str)>,
) -> Result<(), Box<dyn std::error::Error>> {
    let Some(workflow_item_id) = context.workflow_item_id else {
        return Ok(());
    };
    let publications =
        list_agent_publications_for_run(context.pool, context.coordinator_job_id, None).await?;
    let (issue_number, marker, heading, visible) = match related {
        Some((issue_number, work_item)) => {
            let visible = publications
                .iter()
                .filter(|publication| publication.work_item.as_deref() == Some(work_item))
                .collect::<Vec<_>>();
            (
                issue_number,
                format!(
                    "<!-- donkeyspace-publication-status:{}:{} -->",
                    context.coordinator_job_id, work_item
                ),
                format!("Donkeyspace agent artifacts for `{work_item}`"),
                visible,
            )
        }
        None => (
            context.issue_number,
            format!(
                "<!-- donkeyspace-publication-status:{} -->",
                context.coordinator_job_id
            ),
            "Donkeyspace agent workspace status".to_string(),
            publications.iter().collect::<Vec<_>>(),
        ),
    };
    let mut lines = vec![
        format!("### {heading}"),
        String::new(),
        format!("Run: `{}`", context.coordinator_job_id),
        String::new(),
        "| Kind | Agent task | Outcome | Publication | Branch |".into(),
        "| --- | --- | --- | --- | --- |".into(),
    ];
    for publication in visible {
        let task = publication
            .task
            .as_deref()
            .map(|task| match publication.work_item.as_deref() {
                Some(work_item) => format!("{task}/{work_item}"),
                None => task.to_string(),
            })
            .unwrap_or_else(|| "accepted checkpoint".into());
        let publication_status = publication.last_error.as_deref().map_or_else(
            || publication.status.clone(),
            |error| format!("{}: {}", publication.status, truncate(error, 160)),
        );
        lines.push(format!(
            "| {} | {} | {} | {} | [{}]({}) |",
            publication.kind,
            task,
            publication.outcome.as_deref().unwrap_or("—"),
            publication_status.replace('|', "\\|").replace('\n', "<br>"),
            publication.branch_name,
            publication.html_url,
        ));
    }
    if related.is_none()
        && let Some(pull_request_url) = publications.iter().find_map(|publication| {
            publication
                .metadata
                .get("pull_request_url")
                .and_then(Value::as_str)
        })
    {
        lines.extend([
            String::new(),
            format!("Final pull request: {pull_request_url}"),
        ]);
    }
    lines.extend([
        String::new(),
        "Attempt branches are forensic snapshots and are not merged automatically.".into(),
        String::new(),
        marker.clone(),
        "<!-- donkeyspace-generated -->".into(),
    ]);
    create_outbound_action(
        context.pool,
        &OutboundActionInput {
            workflow_item_id,
            job_id: Some(context.coordinator_job_id),
            provider: "github".into(),
            action_type: "issue.upsert_comment".into(),
            payload: json!({
                "owner": context.owner,
                "repo": context.repo,
                "issue_number": issue_number,
                "marker": marker,
                "body": lines.join("\n"),
            }),
        },
    )
    .await?;
    Ok(())
}

fn truncate(value: &str, max: usize) -> String {
    value.chars().take(max).collect()
}

pub async fn queue_publication_status(
    pool: &PgPool,
    publication: &AgentPublicationRecord,
) -> Result<(), Box<dyn std::error::Error>> {
    let owner = publication
        .metadata
        .get("owner")
        .and_then(Value::as_str)
        .ok_or("publication metadata is missing owner")?;
    let repo = publication
        .metadata
        .get("repo")
        .and_then(Value::as_str)
        .ok_or("publication metadata is missing repository")?;
    let issue_number = publication
        .metadata
        .get("issue_number")
        .and_then(Value::as_i64)
        .ok_or("publication metadata is missing issue number")?;
    let context = PublicationContext {
        pool,
        coordinator_job_id: publication.coordinator_job_id,
        workflow_item_id: publication.workflow_item_id,
        issue_number,
        owner,
        repo,
        workspace_path: Path::new("."),
        token: None,
    };
    queue_status_comment(&context, None).await?;
    if let Some(related_issue_number) = publication
        .metadata
        .get("related_issue_number")
        .and_then(Value::as_i64)
    {
        queue_status_comment(
            &context,
            Some((
                related_issue_number,
                publication.work_item.as_deref().unwrap_or("workflow"),
            )),
        )
        .await?;
    }
    Ok(())
}

pub async fn push_existing_publication(
    pool: &PgPool,
    token: Option<&str>,
    workspace_path: &Path,
    publication: &AgentPublicationRecord,
) -> Result<(), Box<dyn std::error::Error>> {
    let askpass = workspace_path.join("git-askpass-publication.sh");
    let result = async {
        let token =
            token.ok_or("configured GitHub authentication is required to publish branches")?;
        let remote = github_remote(&publication.metadata)?;
        write_askpass_script(&askpass)?;
        let refspec = format!(
            "{}:refs/heads/{}",
            publication.commit_sha, publication.branch_name
        );
        git(
            Path::new(&publication.local_repo_path),
            &["push", &remote, &refspec],
            Some(token),
            Some(&askpass),
        )
        .await
    }
    .await;
    match result {
        Ok(_) => {
            mark_agent_publication_published(pool, publication.id).await?;
            Ok(())
        }
        Err(error) => {
            mark_agent_publication_failed(pool, publication.id, &error.to_string()).await?;
            Err(error)
        }
    }
}

fn github_remote(metadata: &Value) -> Result<String, Box<dyn std::error::Error>> {
    let owner = metadata
        .get("owner")
        .and_then(Value::as_str)
        .ok_or("publication metadata is missing owner")?;
    let repo = metadata
        .get("repo")
        .and_then(Value::as_str)
        .ok_or("publication metadata is missing repository")?;
    Ok(format!("https://github.com/{owner}/{repo}.git"))
}

async fn push_publication(
    context: &PublicationContext<'_>,
    publication: &AgentPublicationRecord,
) -> Result<(), Box<dyn std::error::Error>> {
    push_existing_publication(
        context.pool,
        context.token,
        context.workspace_path,
        publication,
    )
    .await
}

fn attempt_branch_name(
    issue_number: i64,
    coordinator_job_id: Uuid,
    attempt: &AttemptPublication<'_>,
) -> String {
    format!(
        "donkeyspace/attempt-{issue_number}-{}-{}-{}-a{}",
        short_uuid(attempt.job_id.unwrap_or(coordinator_job_id)),
        safe_segment(attempt.task),
        safe_segment(attempt.work_item.unwrap_or("workflow")),
        attempt.attempt
    )
}

fn branch_url(owner: &str, repo: &str, branch: &str) -> String {
    format!("https://github.com/{owner}/{repo}/tree/{branch}")
}

fn short_uuid(id: Uuid) -> String {
    id.to_string().chars().take(8).collect()
}

fn safe_segment(value: &str) -> String {
    value
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || matches!(character, '-' | '_') {
                character
            } else {
                '-'
            }
        })
        .collect()
}

async fn configure_git_author(repo: &Path) -> Result<(), Box<dyn std::error::Error>> {
    git(
        repo,
        &["config", "user.name", "donkeyspace[bot]"],
        None,
        None,
    )
    .await?;
    git(
        repo,
        &[
            "config",
            "user.email",
            "donkeyspace[bot]@users.noreply.github.com",
        ],
        None,
        None,
    )
    .await?;
    Ok(())
}

async fn git_status(repo: &Path) -> Result<String, Box<dyn std::error::Error>> {
    git(repo, &["status", "--porcelain"], None, None).await
}

async fn git(
    repo: &Path,
    args: &[&str],
    token: Option<&str>,
    askpass: Option<&Path>,
) -> Result<String, Box<dyn std::error::Error>> {
    let mut command = Command::new("git");
    command
        .args(args)
        .current_dir(repo)
        .env("GIT_TERMINAL_PROMPT", "0")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    if let Some(token) = token {
        command.env("DONKEYSPACE_GIT_TOKEN", token);
    }
    if let Some(askpass) = askpass {
        command.env("GIT_ASKPASS", askpass);
    }
    let output = command.output().await?;
    if !output.status.success() {
        return Err(format!(
            "git {:?} failed: {}",
            args,
            String::from_utf8_lossy(&output.stderr).trim()
        )
        .into());
    }
    Ok(String::from_utf8_lossy(&output.stdout).into_owned())
}

fn sync_root(source: &Path, target: &Path) -> Result<(), Box<dyn std::error::Error>> {
    if fs::symlink_metadata(source).is_ok_and(|metadata| metadata.file_type().is_symlink()) {
        return Ok(());
    }
    if target.exists() {
        if target.is_dir() {
            fs::remove_dir_all(target)?;
        } else {
            fs::remove_file(target)?;
        }
    }
    if source.is_dir() {
        copy_tree(source, target)?;
    } else if source.is_file() {
        if let Some(parent) = target.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::copy(source, target)?;
    }
    Ok(())
}

fn copy_tree(source: &Path, target: &Path) -> Result<(), Box<dyn std::error::Error>> {
    fs::create_dir_all(target)?;
    let mut entries = fs::read_dir(source)?.collect::<Result<Vec<_>, _>>()?;
    entries.sort_by_key(|entry| entry.file_name());
    for entry in entries {
        let metadata = fs::symlink_metadata(entry.path())?;
        if metadata.file_type().is_symlink() || (!metadata.is_file() && !metadata.is_dir()) {
            continue;
        }
        let destination = target.join(entry.file_name());
        if metadata.is_dir() {
            copy_tree(&entry.path(), &destination)?;
        } else {
            fs::copy(entry.path(), destination)?;
        }
    }
    Ok(())
}

#[derive(Default)]
struct DiagnosticBudget {
    files: usize,
    bytes: u64,
}

fn collect_diagnostic(
    source: &Path,
    target: &Path,
    kind: PluginArtifactType,
    budget: &mut DiagnosticBudget,
    manifest: &mut Vec<Value>,
) -> Result<(), Box<dyn std::error::Error>> {
    if fs::symlink_metadata(source).is_ok_and(|metadata| metadata.file_type().is_symlink()) {
        manifest.push(json!({"path": source, "status": "skipped", "reason": "symlink"}));
        return Ok(());
    }
    match kind {
        PluginArtifactType::File => copy_diagnostic_file(source, target, budget, manifest),
        PluginArtifactType::Directory => {
            if !source.is_dir() {
                manifest.push(json!({"path": source, "status": "missing"}));
                return Ok(());
            }
            let mut entries = fs::read_dir(source)?.collect::<Result<Vec<_>, _>>()?;
            entries.sort_by_key(|entry| entry.file_name());
            for entry in entries {
                let destination = target.join(entry.file_name());
                if entry.path().is_dir() {
                    collect_diagnostic(
                        &entry.path(),
                        &destination,
                        PluginArtifactType::Directory,
                        budget,
                        manifest,
                    )?;
                } else {
                    copy_diagnostic_file(&entry.path(), &destination, budget, manifest)?;
                }
            }
            Ok(())
        }
    }
}

fn copy_diagnostic_file(
    source: &Path,
    target: &Path,
    budget: &mut DiagnosticBudget,
    manifest: &mut Vec<Value>,
) -> Result<(), Box<dyn std::error::Error>> {
    if fs::symlink_metadata(source).is_ok_and(|metadata| metadata.file_type().is_symlink()) {
        manifest.push(json!({"path": source, "status": "skipped", "reason": "symlink"}));
        return Ok(());
    }
    if !source.is_file() {
        return Ok(());
    }
    let metadata = fs::metadata(source)?;
    let size = metadata.len();
    let reason = if budget.files >= MAX_DIAGNOSTIC_FILES {
        Some("file_count_limit")
    } else if size > MAX_DIAGNOSTIC_FILE_BYTES {
        Some("file_size_limit")
    } else if budget.bytes.saturating_add(size) > MAX_DIAGNOSTIC_TOTAL_BYTES {
        Some("total_size_limit")
    } else {
        None
    };
    if let Some(reason) = reason {
        manifest.push(json!({
            "path": source,
            "status": "skipped",
            "reason": reason,
            "size": size,
            "sha256": sha256_file(source)?,
        }));
        return Ok(());
    }
    let contents = fs::read(source)?;
    if contents.contains(&0) || std::str::from_utf8(&contents).is_err() {
        manifest.push(json!({
            "path": source,
            "status": "skipped",
            "reason": "binary",
            "size": size,
            "sha256": format!("{:x}", Sha256::digest(&contents)),
        }));
        return Ok(());
    }
    if let Some(parent) = target.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(target, &contents)?;
    budget.files += 1;
    budget.bytes += size;
    manifest.push(json!({"path": source, "status": "included", "size": size}));
    Ok(())
}

fn sha256_file(path: &Path) -> Result<String, Box<dyn std::error::Error>> {
    let mut file = fs::File::open(path)?;
    let mut hasher = Sha256::new();
    let mut buffer = [0u8; 64 * 1024];
    loop {
        let count = file.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        hasher.update(&buffer[..count]);
    }
    Ok(format!("{:x}", hasher.finalize()))
}

fn redact_diagnostic_tree(
    root: &Path,
    github_token: Option<&str>,
    additional: &[String],
) -> Result<(), Box<dyn std::error::Error>> {
    let mut secrets = github_token
        .filter(|value| !value.is_empty())
        .map(str::to_string)
        .into_iter()
        .collect::<Vec<_>>();
    secrets.extend(additional.iter().filter(|value| !value.is_empty()).cloned());
    for name in [
        "DONKEYSPACE_LLM_API_KEY",
        "OPENROUTER_API_KEY",
        "DONKEYSPACE_GITHUB_TOKEN",
    ] {
        if let Ok(value) = env::var(name)
            && !value.is_empty()
        {
            secrets.push(value);
        }
    }
    if secrets.is_empty() {
        return Ok(());
    }
    fn visit(directory: &Path, secrets: &[String]) -> Result<(), Box<dyn std::error::Error>> {
        for entry in fs::read_dir(directory)? {
            let entry = entry?;
            if entry.path().is_dir() {
                visit(&entry.path(), secrets)?;
                continue;
            }
            let Ok(mut value) = fs::read_to_string(entry.path()) else {
                continue;
            };
            let original = value.clone();
            for secret in secrets {
                value = value.replace(secret, "[REDACTED]");
            }
            if value != original {
                fs::write(entry.path(), value)?;
            }
        }
        Ok(())
    }
    visit(root, &secrets)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn branch_names_are_safe_and_stable() {
        let id = Uuid::parse_str("01a03537-b408-7213-bdc7-ead9a6f1a48a").unwrap();
        assert_eq!(issue_branch_name(29, id), "donkeyspace/issue-29-01a03537");
        let attempt = AttemptPublication {
            job_id: Some(id),
            task: "synthesis/task",
            work_item: Some("counter detect"),
            attempt: 301,
            outcome: Some(Outcome::Failed),
            task_root: Path::new("/tmp/unused"),
            write_roots: &[],
            diagnostics: &[],
            reason: "failure",
            related_issue_number: None,
            redactions: &[],
        };
        assert_eq!(
            attempt_branch_name(29, id, &attempt),
            "donkeyspace/attempt-29-01a03537-synthesis-task-counter-detect-a301"
        );
    }

    #[test]
    fn publication_pushes_target_the_github_repository_not_the_clone_origin() {
        assert_eq!(
            github_remote(&json!({"owner": "EPIC-BLOCKCHAIN", "repo": "hw-ai-flow-test"})).unwrap(),
            "https://github.com/EPIC-BLOCKCHAIN/hw-ai-flow-test.git"
        );
    }

    #[test]
    fn diagnostics_include_text_redact_tokens_and_skip_binary_files() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::env::temp_dir().join(format!("donkeyspace-publication-{unique}"));
        let source = root.join("source");
        let target = root.join("target");
        fs::create_dir_all(&source).unwrap();
        fs::write(source.join("tool.log"), "token=secret-value\nfailed\n").unwrap();
        fs::write(source.join("wave.bin"), [0, 1, 2, 3]).unwrap();
        let mut budget = DiagnosticBudget::default();
        let mut manifest = Vec::new();
        collect_diagnostic(
            &source,
            &target,
            PluginArtifactType::Directory,
            &mut budget,
            &mut manifest,
        )
        .unwrap();
        redact_diagnostic_tree(&target, Some("secret-value"), &[]).unwrap();
        assert_eq!(
            fs::read_to_string(target.join("tool.log")).unwrap(),
            "token=[REDACTED]\nfailed\n"
        );
        assert!(!target.join("wave.bin").exists());
        assert!(
            manifest
                .iter()
                .any(|entry| { entry["reason"] == "binary" && entry["sha256"].as_str().is_some() })
        );
        fs::remove_dir_all(root).unwrap();
    }
}
