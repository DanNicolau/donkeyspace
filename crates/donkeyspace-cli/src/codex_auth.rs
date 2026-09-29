//! Instance-owned automation login. Never import an ambient personal home.
use super::*;

const RECONNECT: &str = "connect a dedicated automation login with `donkeyspace connect codex --method api-key` or `--method chatgpt`; personal/shared Codex homes are no longer mounted";

impl CodexLoginMethod {
    pub(crate) fn name(self) -> &'static str {
        match self {
            Self::ChatGpt => "chatgpt",
            Self::ApiKey => "api-key",
        }
    }

    fn forced_method(self) -> &'static str {
        match self {
            Self::ChatGpt => "chatgpt",
            Self::ApiKey => "api",
        }
    }
}

// Ambient authentication can take precedence over saved credentials. Keep CA
// configuration, but do not allow a personal login or workload identity to make
// an empty automation home appear connected.
pub(crate) fn auth_environment_removals() -> Vec<std::ffi::OsString> {
    env::vars_os()
        .map(|(name, _)| name)
        .filter(|name| {
            let name = name.to_string_lossy();
            (name.starts_with("CODEX_") && name != "CODEX_CA_CERTIFICATE")
                || name == "OPENAI_API_KEY"
        })
        .collect()
}

impl Instance {
    fn automation_home(&self) -> Result<PathBuf, SetupError> {
        Ok(fs::canonicalize(&self.directory)?.join("codex-automation"))
    }

    pub(crate) fn codex_connection(
        &self,
        config: &InstanceConfig,
    ) -> Result<Option<(PathBuf, CodexLoginMethod)>, SetupError> {
        let (home, method) = match (&config.codex_home, config.codex_auth_method) {
            (None, None) => return Ok(None),
            (Some(home), Some(method)) => (home, method),
            _ => return Err(SetupError::Config(RECONNECT.into())),
        };
        if *home != self.automation_home()? {
            return Err(SetupError::Config(RECONNECT.into()));
        }
        check_home(home)?;
        check_auth_file(&home.join("auth.json"))?;
        Ok(Some((home.clone(), method)))
    }

    pub(crate) fn connected_codex_home(&self) -> Option<PathBuf> {
        self.config()
            .and_then(|config| self.codex_connection(config).ok().flatten())
            .map(|(home, _)| home)
    }

    pub fn connect_codex(&mut self, method: CodexLoginMethod) -> Result<(), SetupError> {
        self.require_config()?;
        let key = match method {
            CodexLoginMethod::ChatGpt => None,
            CodexLoginMethod::ApiKey => Some(read_secret("OpenAI automation project API key: ")?),
        };
        self.connect_codex_with(method, key.as_deref(), "codex")
    }

    pub fn connect_codex_api_key(&mut self, key: &str) -> Result<(), SetupError> {
        self.connect_codex_with(CodexLoginMethod::ApiKey, Some(key), "codex")
    }

    fn connect_codex_with(
        &mut self,
        method: CodexLoginMethod,
        key: Option<&str>,
        program: &str,
    ) -> Result<(), SetupError> {
        self.require_config()?;
        if matches!(method, CodexLoginMethod::ApiKey) && key.is_none_or(|key| key.trim().is_empty())
        {
            return Err(SetupError::Config("API key cannot be empty".into()));
        }
        let _configuration = self.configuration_lock()?;
        self.require_unchanged_configuration(self.require_config()?)?;
        let home = self.automation_home()?;
        fs::create_dir_all(&home)?;
        check_home(&home)?;
        set_directory_mode(&home)?;
        let auth = home.join("auth.json");
        // A stable file identity lets runtime locks coordinate with login too.
        // Never truncate an existing credential before acquiring ownership.
        match fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&auth)
        {
            Ok(mut file) => {
                set_private_file_mode(&auth)?;
                file.write_all(b"{}\n")?;
            }
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(error.into()),
        }
        check_auth_file(&auth)?;
        let file = fs::OpenOptions::new().read(true).write(true).open(&auth)?;
        file.try_lock().map_err(|_| {
            SetupError::Config(
                "automation credentials are in use; drain agent jobs before reconnecting".into(),
            )
        })?;
        let _credentials = OperationLock(file);
        let mut command = login_command(program, &home, method);
        command.arg("login");
        if let Some(key) = key {
            let mut child = command
                .arg("--with-api-key")
                .stdin(Stdio::piped())
                .spawn()?;
            if let Err(error) = child.stdin.take().unwrap().write_all(key.trim().as_bytes()) {
                // Retain credential ownership until a failed login process is
                // gone, including an early stdin failure.
                let _ = child.kill();
                let _ = child.wait();
                return Err(error.into());
            }
            let status = child.wait()?;
            if !status.success() {
                return Err(SetupError::Command {
                    command: "codex login --with-api-key".into(),
                    detail: status.to_string(),
                });
            }
        } else {
            run_status(&mut command)?;
        }
        run_status(login_command(program, &home, method).args(["login", "status"]))?;
        check_auth_file(&auth)?;
        let config = self.config.as_mut().unwrap();
        config.codex_home = Some(home);
        config.codex_auth_method = Some(method);
        self.save_unlocked()
    }

    pub fn codex_login_status(&self) -> Result<(), SetupError> {
        let (home, method) = self
            .codex_connection(self.require_config()?)?
            .ok_or_else(|| SetupError::Config(RECONNECT.into()))?;
        let _credentials = credential_read_lock(&home)?;
        run_status(login_command("codex", &home, method).args(["login", "status"]))
    }
}

// Metadata readers must not observe an in-progress credential refresh or login.
// Do not wait in the TUI: an active subscription job can own this file for minutes.
pub(crate) fn credential_read_lock(home: &Path) -> Result<OperationLock, SetupError> {
    check_auth_file(&home.join("auth.json"))?;
    let file = fs::File::open(home.join("auth.json"))?;
    file.try_lock_shared().map_err(|_| {
        SetupError::Config(
            "automation authentication is busy; retry after the active job or login finishes"
                .into(),
        )
    })?;
    Ok(OperationLock(file))
}

fn login_command(program: &str, home: &Path, method: CodexLoginMethod) -> Command {
    let mut command = Command::new(program);
    for name in auth_environment_removals() {
        command.env_remove(name);
    }
    command.env("CODEX_HOME", home).current_dir(home).args([
        "-c",
        "cli_auth_credentials_store=\"file\"",
        "-c",
        &format!("forced_login_method=\"{}\"", method.forced_method()),
    ]);
    command
}

fn check_home(home: &Path) -> Result<(), SetupError> {
    if !fs::symlink_metadata(home)?.is_dir() {
        return Err(SetupError::Config(
            "automation Codex home must be a real directory, not a link".into(),
        ));
    }
    Ok(())
}

fn check_auth_file(path: &Path) -> Result<(), SetupError> {
    if !fs::symlink_metadata(path)?.is_file() {
        return Err(SetupError::Config(
            "automation auth.json must be a regular file, not a link".into(),
        ));
    }
    check_secret_permissions(path)
}

fn set_private_file_mode(path: &Path) -> Result<(), SetupError> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
    }
    Ok(())
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    struct Fixture {
        instance: Instance,
        program: PathBuf,
    }
    impl Fixture {
        fn new() -> Self {
            let unique = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos();
            let directory = env::temp_dir().join(format!("ds-automation-login-{unique}"));
            let mut instance = Instance::open(Some(directory.clone())).unwrap();
            instance
                .init(
                    Path::new(env!("CARGO_MANIFEST_DIR")).join("../.."),
                    RuntimeSource::LocalBuild,
                )
                .unwrap();
            let program = directory.join("fake-codex");
            fs::write(
                &program,
                r#"#!/bin/sh
set -eu
test "$PWD" = "$CODEX_HOME"
test "$1" = -c
test "$2" = 'cli_auth_credentials_store="file"'
test "$3" = -c
method="$4"
shift 4
test "$1" = login
test -z "${CODEX_API_KEY-}${CODEX_ACCESS_TOKEN-}${OPENAI_API_KEY-}${CODEX_WIF_TEST_IDENTITY-}"
printf '%s\n' "$method" >> invocations
if test "${2-}" = status; then test -s auth.json; exit 0; fi
if test "${2-}" = --with-api-key; then
  test "$method" = 'forced_login_method="api"'
  key=''; read -r key || true
  test "$key" = synthetic-automation-key
  printf '%s' '{"OPENAI_API_KEY":"synthetic-automation-key"}' > auth.json
else
  test "$method" = 'forced_login_method="chatgpt"'
  printf '%s' '{"tokens":{"access_token":"synthetic-subscription-token"}}' > auth.json
fi
"#,
            )
            .unwrap();
            fs::set_permissions(&program, fs::Permissions::from_mode(0o755)).unwrap();
            Self { instance, program }
        }
        fn connect(&mut self, method: CodexLoginMethod) -> Result<(), SetupError> {
            self.instance.connect_codex_with(
                method,
                (method == CodexLoginMethod::ApiKey).then_some("synthetic-automation-key"),
                self.program.to_str().unwrap(),
            )
        }
    }
    impl Drop for Fixture {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.instance.directory);
        }
    }

    #[test]
    fn both_login_methods_use_private_automation_state_and_export_only_a_file_path() {
        for method in [CodexLoginMethod::ApiKey, CodexLoginMethod::ChatGpt] {
            let mut f = Fixture::new();
            f.connect(method).unwrap();
            let home = f.instance.directory.join("codex-automation");
            assert_eq!(f.instance.connected_codex_home(), Some(home.clone()));
            assert_eq!(
                fs::metadata(&home).unwrap().permissions().mode() & 0o777,
                0o700
            );
            assert_eq!(
                fs::metadata(home.join("auth.json"))
                    .unwrap()
                    .permissions()
                    .mode()
                    & 0o777,
                0o600
            );
            f.instance
                .write_compose_env(f.instance.config().unwrap())
                .unwrap();
            let configuration = fs::read_to_string(f.instance.config_path()).unwrap();
            let environment = fs::read_to_string(f.instance.directory.join(GENERATED_ENV)).unwrap();
            for value in [&configuration, &environment] {
                assert!(!value.contains("synthetic-automation-key"));
                assert!(!value.contains("synthetic-subscription-token"));
            }
            assert!(environment.contains(&format!(
                "DONKEYSPACE_CODEX_AUTH_SOURCE={}/auth.json",
                home.display()
            )));
            assert!(
                environment.contains(&format!("DONKEYSPACE_CODEX_AUTH_METHOD={}", method.name()))
            );
            assert!(!environment.contains("DONKEYSPACE_CODEX_HOME_SOURCE"));
            assert_eq!(
                fs::read_to_string(home.join("invocations"))
                    .unwrap()
                    .lines()
                    .count(),
                2
            );
        }
    }

    #[test]
    fn login_failure_and_active_credential_users_preserve_the_saved_connection() {
        let mut f = Fixture::new();
        f.connect(CodexLoginMethod::ApiKey).unwrap();
        let before = fs::read(f.instance.config_path()).unwrap();
        let home = f.instance.connected_codex_home().unwrap();
        let auth = home.join("auth.json");
        let bytes = fs::read(&auth).unwrap();
        let reader = fs::File::open(&auth).unwrap();
        reader.try_lock_shared().unwrap();
        // Account/status readers coexist with API-key jobs but cannot read a
        // credential while a subscription job or login can be rewriting it.
        drop(credential_read_lock(&home).unwrap());
        assert!(
            f.connect(CodexLoginMethod::ChatGpt)
                .unwrap_err()
                .to_string()
                .contains("drain agent jobs")
        );
        assert_eq!(fs::read(&auth).unwrap(), bytes);
        drop(reader);
        let writer = fs::File::open(&auth).unwrap();
        writer.try_lock().unwrap();
        assert!(credential_read_lock(&home).is_err());
        drop(writer);
        fs::write(&f.program, "#!/bin/sh\nexit 7\n").unwrap();
        assert!(f.connect(CodexLoginMethod::ChatGpt).is_err());
        assert_eq!(fs::read(f.instance.config_path()).unwrap(), before);
        assert_eq!(fs::read(&auth).unwrap(), bytes);
        // The failed operation released both locks.
        fs::File::open(auth).unwrap().try_lock().unwrap();
        f.instance.configuration_lock().unwrap();
    }

    #[test]
    fn legacy_and_redirected_homes_cannot_become_runtime_credentials() {
        let mut f = Fixture::new();
        let personal = f.instance.directory.join("personal");
        write_secret(&personal.join("auth.json"), b"personal-sentinel").unwrap();
        f.instance.config.as_mut().unwrap().codex_home = Some(personal.clone());
        f.instance.save().unwrap();
        let mut legacy: serde_json::Value =
            serde_json::from_slice(&fs::read(f.instance.config_path()).unwrap()).unwrap();
        legacy["schema_version"] = json!(8);
        legacy.as_object_mut().unwrap().remove("codex_auth_method");
        fs::write(
            f.instance.config_path(),
            serde_json::to_vec(&legacy).unwrap(),
        )
        .unwrap();
        f.instance = Instance::open(Some(f.instance.directory.clone())).unwrap();
        assert_eq!(f.instance.config().unwrap().schema_version, SCHEMA_VERSION);
        assert!(f.instance.connected_codex_home().is_none());
        assert!(
            f.instance
                .codex_login_status()
                .unwrap_err()
                .to_string()
                .contains(RECONNECT)
        );
        assert!(
            f.instance
                .write_compose_env(f.instance.config().unwrap())
                .is_err()
        );
        let home = f.instance.automation_home().unwrap();
        std::os::unix::fs::symlink(&personal, &home).unwrap();
        assert!(f.connect(CodexLoginMethod::ApiKey).is_err());
        fs::remove_file(&home).unwrap();
        fs::create_dir(&home).unwrap();
        std::os::unix::fs::symlink(personal.join("auth.json"), home.join("auth.json")).unwrap();
        assert!(f.connect(CodexLoginMethod::ApiKey).is_err());
        assert_eq!(
            fs::read(personal.join("auth.json")).unwrap(),
            b"personal-sentinel"
        );
        assert!(!personal.join("invocations").exists());
    }

    #[test]
    fn ambient_credentials_are_removed_before_automation_login() {
        let output = Command::new(env::current_exe().unwrap())
            .args([
                "--exact",
                "codex_auth::tests::ambient_auth_fixture",
                "--ignored",
                "--nocapture",
            ])
            .env("CODEX_HOME", "/not-the-automation-home")
            .env("CODEX_API_KEY", "synthetic-ambient-key")
            .env("OPENAI_API_KEY", "synthetic-other-key")
            .env("CODEX_ACCESS_TOKEN", "synthetic-access-token")
            .env("CODEX_WIF_TEST_IDENTITY", "synthetic-workload-identity")
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    #[test]
    #[ignore = "private subprocess fixture for ambient_credentials_are_removed_before_automation_login"]
    fn ambient_auth_fixture() {
        let mut f = Fixture::new();
        f.connect(CodexLoginMethod::ApiKey).unwrap();
    }
}
