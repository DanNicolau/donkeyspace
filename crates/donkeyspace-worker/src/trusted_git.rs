//! Git runs in the coordinator, so repository configuration is data, not code.
use base64::Engine;
use std::{
    fs,
    path::Path,
    process::{Output, Stdio},
};
use tokio::process::Command;

type Error = Box<dyn std::error::Error>;

fn command() -> Command {
    let mut command = Command::new("git");
    command
        .current_dir("/")
        .env_clear()
        .env("PATH", "/usr/local/bin:/usr/bin:/bin")
        .env("HOME", "/nonexistent-donkeyspace-git-home")
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_ATTR_NOSYSTEM", "1")
        .env("GIT_NO_REPLACE_OBJECTS", "1")
        .env("GIT_TERMINAL_PROMPT", "0")
        .args([
            "--no-pager",
            "-c",
            "core.hooksPath=/dev/null",
            "-c",
            "core.fsmonitor=false",
            "-c",
            "core.askPass=",
            "-c",
            "credential.helper=",
            "-c",
            "commit.gpgsign=false",
            "-c",
            "tag.gpgsign=false",
            "-c",
            "gc.auto=0",
            "-c",
            "maintenance.auto=false",
            "-c",
            "init.templateDir=",
            "-c",
            "protocol.allow=never",
            "-c",
            "protocol.https.allow=always",
            "-c",
            "http.followRedirects=false",
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    command
}

async fn execute(command: &mut Command) -> Result<Output, Error> {
    #[cfg(unix)]
    {
        Ok(
            donkeyspace_runner::process::run_command_until(command, None, std::future::pending())
                .await?
                .output,
        )
    }
    #[cfg(not(unix))]
    {
        Ok(command.output().await?)
    }
}

pub fn github_remote(owner: &str, repo: &str) -> Result<String, Error> {
    if [owner, repo].iter().any(|part| {
        part.is_empty()
            || !part
                .bytes()
                .all(|c| c.is_ascii_alphanumeric() || b"._-".contains(&c))
            || matches!(*part, "." | "..")
    }) {
        return Err("invalid GitHub repository identity".into());
    }
    Ok(format!("https://github.com/{owner}/{repo}.git"))
}

fn valid_remote(value: &str) -> bool {
    let Some(path) = value.strip_prefix("https://github.com/") else {
        return false;
    };
    let Some((owner, repo)) = path.split_once('/') else {
        return false;
    };
    github_remote(owner, repo.strip_suffix(".git").unwrap_or(repo)).is_ok()
}

fn allowed_config(key: &str, value: &str) -> bool {
    if value.chars().any(char::is_control) {
        return false;
    }
    match key {
        "core.repositoryformatversion" => matches!(value, "0" | "1"),
        "core.filemode"
        | "core.bare"
        | "core.logallrefupdates"
        | "core.ignorecase"
        | "core.precomposeunicode" => matches!(value, "true" | "false"),
        "extensions.objectformat" => matches!(value, "sha1" | "sha256"),
        "user.name" | "user.email" => true,
        "remote.origin.url" => valid_remote(value),
        "remote.origin.fetch" => {
            value.starts_with("+refs/") && value.contains(":refs/remotes/origin/")
        }
        _ if key.starts_with("branch.") && key.ends_with(".remote") => value == "origin",
        _ if key.starts_with("branch.") && key.ends_with(".merge") => {
            value.starts_with("refs/heads/")
        }
        _ => false,
    }
}

fn validate_metadata(repo: &Path) -> Result<(), Error> {
    fn walk(path: &Path) -> Result<(), Error> {
        let metadata = fs::symlink_metadata(path)?;
        if metadata.is_dir() {
            for entry in fs::read_dir(path)? {
                walk(&entry?.path())?;
            }
        } else if !metadata.is_file() {
            return Err(
                "Git metadata contains a link or special file; restore a trusted checkout".into(),
            );
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            if metadata.is_file() && metadata.nlink() != 1 {
                return Err(
                    "Git metadata contains shared file links; restore a trusted checkout".into(),
                );
            }
        }
        Ok(())
    }
    crate::repository_files::validate_directory(repo)?;
    let git = repo.join(".git");
    if !fs::symlink_metadata(&git)?.is_dir() {
        return Err("worker Git requires a standalone checkout with a real .git directory".into());
    }
    for forbidden in [
        "commondir",
        "config.worktree",
        "objects/info/alternates",
        "info/grafts",
    ] {
        if fs::symlink_metadata(git.join(forbidden)).is_ok() {
            return Err(format!(
                "unsupported Git metadata `{forbidden}`; restore a trusted checkout"
            )
            .into());
        }
    }
    walk(&git)
}

async fn validate_repository(repo: &Path) -> Result<(), Error> {
    validate_metadata(repo)?;
    if fs::metadata(repo.join(".git/config"))?.len() > 1024 * 1024 {
        return Err("Git configuration exceeds coordinator limit".into());
    }
    // Config inspection reads one file without following includes. It cannot
    // invoke hooks, filters, fsmonitor, credential helpers, or remote transports.
    let output = execute(
        command()
            .args(["config", "--file"])
            .arg(std::path::absolute(repo)?.join(".git/config"))
            .args(["--no-includes", "--null", "--list"]),
    )
    .await?;
    if !output.status.success() {
        return Err("cannot inspect repository Git configuration".into());
    }
    for entry in std::str::from_utf8(&output.stdout)?
        .split('\0')
        .filter(|entry| !entry.is_empty())
    {
        let (key, value) = entry.split_once('\n').unwrap_or((entry, "true"));
        if !allowed_config(key, value) {
            return Err(format!("repository Git configuration `{key}` is not permitted in the coordinator; restore a trusted checkout").into());
        }
    }
    Ok(())
}

fn authenticate(command: &mut Command, token: Option<&str>) {
    if let Some(token) = token {
        // Scoped to GitHub HTTPS and passed only through the child environment,
        // never a command argument, repository config, or agent-visible script.
        let authorization =
            base64::engine::general_purpose::STANDARD.encode(format!("x-access-token:{token}"));
        command
            .env("GIT_CONFIG_COUNT", "1")
            .env("GIT_CONFIG_KEY_0", "http.https://github.com/.extraheader")
            .env(
                "GIT_CONFIG_VALUE_0",
                format!("Authorization: Basic {authorization}"),
            );
    }
}

pub async fn output(repo: &Path, args: &[&str], token: Option<&str>) -> Result<Output, Error> {
    crate::repository_files::validate_directory(repo)?;
    if args.first() != Some(&"init") || fs::symlink_metadata(repo.join(".git")).is_ok() {
        validate_repository(repo).await?;
    }
    let mut command = command();
    command.current_dir(repo).args(args);
    authenticate(&mut command, token);
    execute(&mut command).await
}

pub async fn run(repo: &Path, args: &[&str], token: Option<&str>) -> Result<String, Error> {
    let output = output(repo, args, token).await?;
    if !output.status.success() {
        return Err(format!(
            "git {} failed: {}",
            args.first().unwrap_or(&"command"),
            String::from_utf8_lossy(&output.stderr).trim()
        )
        .into());
    }
    Ok(String::from_utf8_lossy(&output.stdout).into_owned())
}

pub async fn clone_remote(
    owner: &str,
    repo: &str,
    branch: &str,
    destination: &Path,
    token: Option<&str>,
) -> Result<(), Error> {
    crate::repository_files::validate_directory(destination)?;
    let remote = github_remote(owner, repo)?;
    let mut command = command();
    command
        .args([
            "clone",
            "--depth",
            "1",
            "--branch",
            branch,
            "--single-branch",
            "--",
            &remote,
        ])
        .arg(std::path::absolute(destination)?);
    authenticate(&mut command, token);
    let output = execute(&mut command).await?;
    if !output.status.success() {
        return Err(format!(
            "repository clone failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        )
        .into());
    }
    validate_repository(destination).await
}

/// Recover exact objects without consulting the current default branch.
pub async fn restore_remote(
    owner: &str,
    repo: &str,
    commit: &str,
    base: &str,
    destination: &Path,
    token: Option<&str>,
) -> Result<(), Error> {
    crate::repository_files::validate_directory(destination)?;
    fs::create_dir_all(destination)?;
    run(destination, &["init"], None).await?;
    let remote = github_remote(owner, repo)?;
    run(destination, &["config", "remote.origin.url", &remote], None).await?;
    run(
        destination,
        &["fetch", "--no-tags", "--", &remote, commit, base],
        token,
    )
    .await?;
    Ok(())
}

pub async fn clone_local(source: &Path, destination: &Path) -> Result<(), Error> {
    crate::repository_files::validate_directory(destination)?;
    validate_repository(source).await?;
    let output = execute(
        command()
            .args([
                "-c",
                "protocol.file.allow=always",
                "clone",
                "--no-hardlinks",
                "--",
            ])
            .arg(std::path::absolute(source)?)
            .arg(std::path::absolute(destination)?),
    )
    .await?;
    if !output.status.success() {
        return Err("forensic checkout clone failed".into());
    }
    // Forensic clones publish to an explicit trusted URL, never this local source.
    let output = execute(command().current_dir(destination).args([
        "config",
        "--remove-section",
        "remote.origin",
    ]))
    .await?;
    if !output.status.success() {
        return Err("cannot remove forensic clone source remote".into());
    }
    validate_repository(destination).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    struct Fixture(PathBuf);

    impl Fixture {
        fn new() -> Self {
            let path =
                std::env::temp_dir().join(format!("donkeyspace-git-{}", uuid::Uuid::now_v7()));
            fs::create_dir_all(&path).unwrap();
            Self(path)
        }

        async fn repo(&self, name: &str) -> PathBuf {
            let repo = self.0.join(name);
            fs::create_dir_all(&repo).unwrap();
            run(&repo, &["init", "-b", "main"], None).await.unwrap();
            run(&repo, &["config", "user.name", "Test"], None)
                .await
                .unwrap();
            run(
                &repo,
                &["config", "user.email", "test@example.invalid"],
                None,
            )
            .await
            .unwrap();
            fs::write(repo.join("README"), "initial\n").unwrap();
            run(&repo, &["add", "--", "README"], None).await.unwrap();
            run(&repo, &["commit", "-m", "initial"], None)
                .await
                .unwrap();
            repo
        }
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn hooks_never_run_and_forensic_clone_survives_source_removal() {
        use std::os::unix::fs::PermissionsExt;
        let fixture = Fixture::new();
        let repo = fixture.repo("source").await;
        // A failing hook makes invocation observable without shell path escaping.
        for hook in ["pre-commit", "post-checkout", "post-merge"] {
            let path = repo.join(".git/hooks").join(hook);
            fs::create_dir_all(path.parent().unwrap()).unwrap();
            fs::write(&path, "#!/bin/sh\ntouch hook-executed\nexit 99\n").unwrap();
            fs::set_permissions(&path, fs::Permissions::from_mode(0o755)).unwrap();
        }
        run(&repo, &["checkout", "-b", "feature"], None)
            .await
            .unwrap();
        fs::write(repo.join("README"), "changed\n").unwrap();
        run(&repo, &["add", "--", "README"], None).await.unwrap();
        run(&repo, &["commit", "-m", "change"], None).await.unwrap();
        run(&repo, &["checkout", "main"], None).await.unwrap();
        run(&repo, &["merge", "--no-ff", "-m", "merge", "feature"], None)
            .await
            .unwrap();
        assert!(!repo.join("hook-executed").exists());
        let expected = run(&repo, &["rev-parse", "HEAD"], None).await.unwrap();
        let clone = fixture.0.join("forensic");
        clone_local(&repo, &clone).await.unwrap();
        fs::remove_dir_all(repo).unwrap();
        assert_eq!(
            run(&clone, &["rev-parse", "HEAD"], None).await.unwrap(),
            expected
        );
        assert_eq!(
            run(&clone, &["show", "HEAD:README"], None).await.unwrap(),
            "changed\n"
        );
        run(&clone, &["fsck", "--full"], None).await.unwrap();
        assert!(!clone.join(".git/objects/info/alternates").exists());
    }

    #[tokio::test]
    async fn executable_or_redirecting_repository_config_is_rejected_before_git_runs() {
        let fixture = Fixture::new();
        let repo = fixture.repo("repo").await;
        let config = repo.join(".git/config");
        let initial = fs::read_to_string(&config).unwrap();
        fs::write(
            repo.join(".gitattributes"),
            "* filter=evil diff=evil merge=evil\n",
        )
        .unwrap();
        let hostile = [
            "[include]\npath = /etc/passwd\n",
            "[includeIf \"gitdir:**\"]\npath = /etc/passwd\n",
            "[core]\nhooksPath = /tmp\n",
            "[core]\nfsmonitor = touch marker\n",
            "[core]\nworktree = /tmp\n",
            "[core]\nsshCommand = touch marker\n",
            "[filter \"evil\"]\nclean = touch marker\n",
            "[diff \"evil\"]\ntextconv = touch marker\n",
            "[merge \"evil\"]\ndriver = touch marker\n",
            "[credential]\nhelper = !touch marker\n",
            "[remote \"origin\"]\nurl = ext::touch marker\n",
            "[remote \"origin\"]\npushurl = https://attacker.invalid/repo\n",
            "[url \"ext::touch marker\"]\ninsteadOf = https://github.com/\n",
            "[protocol \"ext\"]\nallow = always\n",
            "[http]\nproxy = https://attacker.invalid\n",
        ];
        for extra in hostile {
            fs::write(&config, format!("{initial}\n{extra}")).unwrap();
            for args in [&["status"][..], &["add", "-A"][..], &["diff", "HEAD"][..]] {
                let error = run(&repo, args, None).await.unwrap_err().to_string();
                assert!(error.contains("not permitted"), "{extra}: {error}");
            }
            assert!(!repo.join("marker").exists());
        }
        fs::write(&config, initial).unwrap();
        run(&repo, &["add", "-A"], None).await.unwrap();
        run(&repo, &["diff", "--cached"], None).await.unwrap();
        assert!(!repo.join("marker").exists());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn metadata_cannot_redirect_reads_or_writes_outside_checkout() {
        use std::os::unix::fs::symlink;
        let fixture = Fixture::new();
        let repo = fixture.repo("repo").await;
        let config = repo.join(".git/config");
        let external = fixture.0.join("external");
        fs::rename(&config, &external).unwrap();
        let initial = fs::read(&external).unwrap();
        symlink(&external, &config).unwrap();
        assert!(
            run(&repo, &["config", "user.name", "changed"], None)
                .await
                .is_err()
        );
        fs::remove_file(&config).unwrap();
        fs::hard_link(&external, &config).unwrap();
        assert!(
            run(&repo, &["config", "user.name", "changed"], None)
                .await
                .is_err()
        );
        fs::remove_file(&config).unwrap();
        fs::copy(&external, &config).unwrap();
        assert_eq!(fs::read(&external).unwrap(), initial);
        for name in [
            "commondir",
            "config.worktree",
            "objects/info/alternates",
            "info/grafts",
        ] {
            let path = repo.join(".git").join(name);
            fs::create_dir_all(path.parent().unwrap()).unwrap();
            fs::write(&path, external.to_str().unwrap()).unwrap();
            assert!(run(&repo, &["status"], None).await.is_err(), "{name}");
            fs::remove_file(path).unwrap();
        }
        let redirected = fixture.0.join("redirected");
        symlink(&repo, &redirected).unwrap();
        assert!(run(&redirected, &["status"], None).await.is_err());
        assert!(clone_local(&repo, &redirected.join("clone")).await.is_err());
        assert!(!repo.join("clone").exists());
        let git_dir = fixture.0.join("git-metadata");
        fs::rename(repo.join(".git"), &git_dir).unwrap();
        fs::write(repo.join(".git"), format!("gitdir: {}", git_dir.display())).unwrap();
        assert!(run(&repo, &["status"], None).await.is_err());
        assert!(run(&repo, &["init"], None).await.is_err());
    }

    #[tokio::test]
    async fn inherited_environment_cannot_override_coordinator() {
        const FIXTURE: &str = "DONKEYSPACE_GIT_ENV_TEST_REPO";
        if let Ok(repo) = std::env::var(FIXTURE) {
            let repo = Path::new(&repo);
            assert_eq!(
                run(repo, &["status", "--porcelain"], None).await.unwrap(),
                ""
            );
            assert_eq!(
                run(repo, &["show", "HEAD:README"], None).await.unwrap(),
                "initial\n"
            );
            return;
        }
        let fixture = Fixture::new();
        let repo = fixture.repo("repo").await;
        let poisoned_config = fixture.0.join("global-config");
        fs::write(
            &poisoned_config,
            "[include]\npath = /does/not/exist\n[core]\nworktree = /does/not/exist\n",
        )
        .unwrap();
        let result = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "trusted_git::tests::inherited_environment_cannot_override_coordinator",
                "--nocapture",
            ])
            .env(FIXTURE, &repo)
            .env("GIT_DIR", "/does/not/exist")
            .env("GIT_WORK_TREE", "/does/not/exist")
            .env("GIT_INDEX_FILE", "/does/not/exist")
            .env("GIT_CONFIG_GLOBAL", &poisoned_config)
            .env("GIT_CONFIG_COUNT", "1")
            .env("GIT_CONFIG_KEY_0", "core.worktree")
            .env("GIT_CONFIG_VALUE_0", "/does/not/exist")
            .output()
            .unwrap();
        assert!(
            result.status.success(),
            "{}{}",
            String::from_utf8_lossy(&result.stdout),
            String::from_utf8_lossy(&result.stderr)
        );
    }

    #[tokio::test]
    async fn authentication_is_ephemeral_and_scoped_to_github_https() {
        let fixture = Fixture::new();
        let repo = fixture.repo("repo").await;
        let token = "synthetic-installation-token";
        let header = run(
            &repo,
            &[
                "config",
                "--get-urlmatch",
                "http.extraheader",
                "https://github.com/org/repo.git",
            ],
            Some(token),
        )
        .await
        .unwrap();
        let expected =
            base64::engine::general_purpose::STANDARD.encode(format!("x-access-token:{token}"));
        assert_eq!(header.trim(), format!("Authorization: Basic {expected}"));
        for url in [
            "https://github.com.attacker.invalid/org/repo.git",
            "http://github.com/org/repo.git",
            "https://example.invalid/repo.git",
        ] {
            assert!(
                !output(
                    &repo,
                    &["config", "--get-urlmatch", "http.extraheader", url],
                    Some(token)
                )
                .await
                .unwrap()
                .status
                .success()
            );
        }
        assert!(
            !output(
                &repo,
                &[
                    "config",
                    "--get-urlmatch",
                    "http.extraheader",
                    "https://github.com/org/repo.git"
                ],
                None
            )
            .await
            .unwrap()
            .status
            .success()
        );
        let mut child = command();
        authenticate(&mut child, Some(token));
        assert!(
            child
                .as_std()
                .get_args()
                .all(|arg| !arg.to_string_lossy().contains(&expected))
        );
        let config = fs::read_to_string(repo.join(".git/config")).unwrap();
        assert!(!config.contains(token) && !config.contains(&expected));
        for (owner, repo) in [
            ("../host", "repo"),
            ("github.com@host", "repo"),
            ("org", "repo?redirect=x"),
            ("org", "../repo"),
            ("org", "."),
        ] {
            assert!(github_remote(owner, repo).is_err());
        }
    }
}
