use serde::Serialize;
use serde_json::Value;
use std::{
    collections::BTreeSet,
    ffi::OsStr,
    fs,
    path::{Path, PathBuf},
    process::Stdio,
};
use tokio::process::Command;
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct RepoContextConfig {
    pub workspace_root: PathBuf,
    pub max_bytes: usize,
    pub max_file_bytes: usize,
    pub max_files: usize,
}

impl RepoContextConfig {
    pub fn new(
        workspace_root: impl Into<PathBuf>,
        max_bytes: usize,
        max_file_bytes: usize,
        max_files: usize,
    ) -> Self {
        Self {
            workspace_root: workspace_root.into(),
            max_bytes,
            max_file_bytes,
            max_files,
        }
    }
}

#[derive(Debug, Serialize)]
pub struct RepositoryContext {
    pub owner: String,
    pub repo: String,
    pub default_branch: String,
    pub checkout_path: String,
    pub file_count: usize,
    pub file_tree: Vec<String>,
    pub excerpts: Vec<FileExcerpt>,
    pub referenced_paths_missing: Vec<String>,
    pub truncated: bool,
}

#[derive(Debug, Serialize)]
pub struct FileExcerpt {
    pub path: String,
    pub content: String,
    pub truncated: bool,
}

pub async fn build_repository_context(
    input: &Value,
    job_id: Uuid,
    github_token: Option<&str>,
    config: &RepoContextConfig,
) -> Result<Value, Box<dyn std::error::Error>> {
    let owner = input
        .pointer("/repository/owner/login")
        .and_then(Value::as_str)
        .ok_or("webhook payload is missing repository owner")?;
    let repo = input
        .pointer("/repository/name")
        .and_then(Value::as_str)
        .ok_or("webhook payload is missing repository name")?;
    let default_branch = input
        .pointer("/repository/default_branch")
        .and_then(Value::as_str)
        .filter(|branch| !branch.trim().is_empty())
        .unwrap_or("main");

    let workspace_path = config.workspace_root.join(job_id.to_string());
    let repo_path = workspace_path.join("repo");
    if workspace_path.exists() {
        fs::remove_dir_all(&workspace_path)?;
    }
    fs::create_dir_all(&workspace_path)?;

    clone_repository(
        owner,
        repo,
        default_branch,
        &repo_path,
        &workspace_path,
        github_token,
    )
    .await?;

    let context = summarize_checkout(owner, repo, default_branch, &repo_path, input, config)?;
    Ok(serde_json::to_value(context)?)
}

pub fn enrich_input_with_repository_context(input: &Value, repository_context: Value) -> Value {
    let mut enriched = input.clone();
    if let Value::Object(map) = &mut enriched {
        map.insert("repository_context".to_string(), repository_context);
    }
    enriched
}

pub fn cleanup_repository_context(
    job_id: Uuid,
    config: &RepoContextConfig,
) -> Result<(), Box<dyn std::error::Error>> {
    let workspace_path = config.workspace_root.join(job_id.to_string());
    if workspace_path.exists() {
        fs::remove_dir_all(workspace_path)?;
    }
    Ok(())
}

async fn clone_repository(
    owner: &str,
    repo: &str,
    default_branch: &str,
    repo_path: &Path,
    workspace_path: &Path,
    github_token: Option<&str>,
) -> Result<(), Box<dyn std::error::Error>> {
    let askpass_path = workspace_path.join("git-askpass.sh");
    write_askpass_script(&askpass_path)?;

    let mut command = Command::new("git");
    command
        .arg("clone")
        .arg("--depth")
        .arg("1")
        .arg("--branch")
        .arg(default_branch)
        .arg("--single-branch")
        .arg(format!("https://github.com/{owner}/{repo}.git"))
        .arg(repo_path)
        .env("GIT_TERMINAL_PROMPT", "0")
        .env("GIT_ASKPASS", &askpass_path)
        .stdout(Stdio::null());

    if let Some(token) = github_token.filter(|token| !token.trim().is_empty()) {
        command.env("DONKEYSPACE_GIT_TOKEN", token);
    }

    let output = command.output().await?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(format!("git clone failed for {owner}/{repo}: {}", stderr.trim()).into());
    }

    Ok(())
}

fn write_askpass_script(path: &Path) -> Result<(), Box<dyn std::error::Error>> {
    fs::write(
        path,
        "#!/bin/sh\ncase \"$1\" in\n*Username*) printf '%s' 'x-access-token' ;;\n*Password*) printf '%s' \"$DONKEYSPACE_GIT_TOKEN\" ;;\n*) printf '%s' '' ;;\nesac\n",
    )?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o700))?;
    }

    Ok(())
}

fn summarize_checkout(
    owner: &str,
    repo: &str,
    default_branch: &str,
    repo_path: &Path,
    input: &Value,
    config: &RepoContextConfig,
) -> Result<RepositoryContext, Box<dyn std::error::Error>> {
    let files = list_files(repo_path)?;
    let selected = select_files(input, &files, config.max_files);
    let selected_set = selected.iter().cloned().collect::<BTreeSet<_>>();
    let referenced = referenced_file_tokens(input);
    let missing = referenced
        .into_iter()
        .filter(|reference| {
            !files
                .iter()
                .any(|path| path_matches_reference(path, reference))
        })
        .collect::<Vec<_>>();

    let mut total_bytes = 0usize;
    let mut excerpts = Vec::new();
    let mut truncated = false;

    for path in selected {
        if excerpts.len() >= config.max_files || total_bytes >= config.max_bytes {
            truncated = true;
            break;
        }

        let full_path = repo_path.join(&path);
        let Ok(raw) = fs::read(&full_path) else {
            continue;
        };
        if raw.contains(&0) {
            continue;
        }

        let remaining = config.max_bytes.saturating_sub(total_bytes);
        let file_limit = config.max_file_bytes.min(remaining);
        if file_limit == 0 {
            truncated = true;
            break;
        }

        let content = String::from_utf8_lossy(&raw);
        let excerpt = truncate_to_char_boundary(&content, file_limit);
        let file_truncated = excerpt.len() < content.len();
        total_bytes += excerpt.len();
        truncated |= file_truncated;

        excerpts.push(FileExcerpt {
            path,
            content: excerpt.to_string(),
            truncated: file_truncated,
        });
    }

    Ok(RepositoryContext {
        owner: owner.to_string(),
        repo: repo.to_string(),
        default_branch: default_branch.to_string(),
        checkout_path: repo_path.display().to_string(),
        file_count: files.len(),
        file_tree: files.into_iter().take(200).collect(),
        excerpts,
        referenced_paths_missing: missing,
        truncated: truncated || selected_set.len() > config.max_files,
    })
}

fn list_files(repo_path: &Path) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let mut files = Vec::new();
    visit_dir(repo_path, repo_path, &mut files)?;
    files.sort();
    Ok(files)
}

fn visit_dir(
    root: &Path,
    dir: &Path,
    files: &mut Vec<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        let file_name = entry.file_name();

        if should_skip_path(&file_name) {
            continue;
        }

        if path.is_dir() {
            visit_dir(root, &path, files)?;
        } else if path.is_file() {
            let relative = path.strip_prefix(root)?;
            files.push(relative.to_string_lossy().replace('\\', "/"));
        }
    }

    Ok(())
}

fn should_skip_path(file_name: &OsStr) -> bool {
    matches!(
        file_name.to_string_lossy().as_ref(),
        ".git" | "target" | "node_modules" | ".next" | "dist" | "build" | ".cache"
    )
}

fn select_files(input: &Value, files: &[String], max_files: usize) -> Vec<String> {
    let mut selected = BTreeSet::new();
    let references = referenced_file_tokens(input);

    for reference in &references {
        for file in files {
            if path_matches_reference(file, reference) {
                selected.insert(file.clone());
            }
        }
    }

    for preferred in [
        "README.md",
        "README",
        "AGENTS.md",
        ".donkeyspace/policy.yml",
        "package.json",
        "Cargo.toml",
        "pyproject.toml",
    ] {
        if files.iter().any(|file| file == preferred) {
            selected.insert(preferred.to_string());
        }
    }

    if selected.is_empty() {
        for file in files.iter().take(max_files) {
            selected.insert(file.clone());
        }
    }

    selected.into_iter().take(max_files).collect()
}

fn referenced_file_tokens(input: &Value) -> Vec<String> {
    let mut tokens = BTreeSet::new();
    for text in [
        input.pointer("/issue/title").and_then(Value::as_str),
        input.pointer("/issue/body").and_then(Value::as_str),
        input.pointer("/comment/body").and_then(Value::as_str),
    ]
    .into_iter()
    .flatten()
    {
        for token in text.split(|ch: char| {
            ch.is_whitespace() || matches!(ch, ',' | ';' | ':' | '(' | ')' | '[' | ']')
        }) {
            let token =
                token.trim_matches(|ch: char| matches!(ch, '"' | '\'' | '`' | '.' | '!' | '?'));
            if looks_like_path_reference(token) {
                tokens.insert(normalize_reference(token));
            }
        }
    }
    tokens.into_iter().collect()
}

fn looks_like_path_reference(token: &str) -> bool {
    let lower = token.to_ascii_lowercase();
    lower == "readme"
        || lower.starts_with("readme.")
        || lower == "agents.md"
        || lower.contains('/')
        || lower.ends_with(".md")
        || lower.ends_with(".rs")
        || lower.ends_with(".ts")
        || lower.ends_with(".tsx")
        || lower.ends_with(".js")
        || lower.ends_with(".json")
        || lower.ends_with(".toml")
        || lower.ends_with(".yml")
        || lower.ends_with(".yaml")
}

fn normalize_reference(token: &str) -> String {
    if token.eq_ignore_ascii_case("readme") {
        "README".to_string()
    } else {
        token.trim_start_matches("./").replace('\\', "/")
    }
}

fn path_matches_reference(path: &str, reference: &str) -> bool {
    path.eq_ignore_ascii_case(reference)
        || Path::new(path)
            .file_name()
            .and_then(OsStr::to_str)
            .map(|name| name.eq_ignore_ascii_case(reference))
            .unwrap_or(false)
        || (reference.eq_ignore_ascii_case("README")
            && Path::new(path)
                .file_name()
                .and_then(OsStr::to_str)
                .map(|name| name.to_ascii_lowercase().starts_with("readme"))
                .unwrap_or(false))
}

fn truncate_to_char_boundary(value: &str, max_bytes: usize) -> &str {
    if value.len() <= max_bytes {
        return value;
    }

    let mut end = max_bytes;
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    &value[..end]
}

#[cfg(test)]
mod tests {
    use super::{path_matches_reference, referenced_file_tokens, select_files};
    use serde_json::json;

    #[test]
    fn readme_reference_selects_readme_file() {
        let selected = select_files(
            &json!({"issue": {"title": "Capitize D and S in README", "body": null}}),
            &["README.md".to_string()],
            12,
        );

        assert_eq!(selected, vec!["README.md"]);
    }

    #[test]
    fn exact_path_reference_is_detected() {
        let tokens = referenced_file_tokens(&json!({
            "issue": {
                "title": "Update docs/setup.md",
                "body": "The docs/setup.md page has stale text."
            }
        }));

        assert!(tokens.contains(&"docs/setup.md".to_string()));
    }

    #[test]
    fn path_matching_supports_basenames() {
        assert!(path_matches_reference("docs/README.md", "README.md"));
        assert!(path_matches_reference("README.md", "README"));
    }
}
