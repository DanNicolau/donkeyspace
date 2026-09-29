//! Mount only the automation credential file. Every container owns its other
//! Codex state; subscription refresh is serialized on the credential inode.
use std::{env, path::Path};

pub(super) struct AgentAuthentication {
    source: String,
    subscription: bool,
    relabel: bool,
}

impl AgentAuthentication {
    pub(super) fn from_environment() -> Result<Option<Self>, String> {
        Self::parse(
            env::var("DONKEYSPACE_CODEX_AUTH_SOURCE").ok().as_deref(),
            env::var("DONKEYSPACE_CODEX_AUTH_METHOD").ok().as_deref(),
            env::var("DONKEYSPACE_CODEX_AUTH_MOUNT_SUFFIX")
                .ok()
                .as_deref(),
            ["DONKEYSPACE_CODEX_HOME_SOURCE", "DONKEYSPACE_CODEX_VOLUME"]
                .iter()
                .any(|name| env::var(name).is_ok_and(|value| !value.is_empty())),
        )
    }

    fn parse(
        source: Option<&str>,
        method: Option<&str>,
        suffix: Option<&str>,
        legacy: bool,
    ) -> Result<Option<Self>, String> {
        let source = source.filter(|value| !value.is_empty());
        let method = method.filter(|value| !value.is_empty());
        if source.is_none() && method.is_none() {
            return if legacy {
                Err("Shared/personal Codex homes are no longer mounted. Connect dedicated automation authentication and set DONKEYSPACE_CODEX_AUTH_SOURCE and DONKEYSPACE_CODEX_AUTH_METHOD.".into())
            } else {
                Ok(None)
            };
        }
        let source = source
            .ok_or("DONKEYSPACE_CODEX_AUTH_SOURCE must name the automation auth.json file")?;
        let path = Path::new(source);
        if !path.is_absolute()
            || path.file_name().is_none_or(|name| name != "auth.json")
            || source.contains([':', ',', '\n', '\r', '\0'])
            || path
                .components()
                .any(|part| matches!(part, std::path::Component::ParentDir))
        {
            return Err("DONKEYSPACE_CODEX_AUTH_SOURCE must be an absolute path to an existing automation auth.json file, not a directory or volume".into());
        }
        let subscription = match method {
            Some("chatgpt") => true,
            Some("api-key") => false,
            _ => return Err("DONKEYSPACE_CODEX_AUTH_METHOD must be chatgpt or api-key".into()),
        };
        let relabel = match suffix.unwrap_or("") {
            "" => false,
            ":z" | ":Z" => true,
            _ => return Err("DONKEYSPACE_CODEX_AUTH_MOUNT_SUFFIX must be empty or :z".into()),
        };
        Ok(Some(Self {
            source: source.into(),
            subscription,
            relabel,
        }))
    }

    pub(super) fn mount(&self) -> String {
        format!(
            "{}:/root/.codex/auth.json:{}{}",
            self.source,
            if self.subscription { "rw" } else { "ro" },
            if self.relabel { ",z" } else { "" }
        )
    }

    pub(super) fn bootstrap(&self) -> String {
        // The shell retains descriptor 9 while waiting for the command. Child
        // code closing inherited descriptors cannot accidentally drop the lock.
        // A killed container releases it even if its coordinator has crashed.
        format!(
            r#"set -eu
if ! test -f /root/.codex/auth.json; then
    echo 'Automation auth.json is missing; reconnect dedicated authentication before retrying.' >&2
    exit 2
fi
previous_umask=$(umask)
umask 077
printf '%s\n' 'cli_auth_credentials_store = "file"' 'forced_login_method = "{method}"' > /root/.codex/config.toml
umask "$previous_umask"
exec 9{access} /root/.codex/auth.json
flock {lock} 9
# An earlier refresh can temporarily truncate the file; inspect it only after locking.
if ! test -s /root/.codex/auth.json; then
    echo 'Automation auth.json is empty; reconnect dedicated authentication before retrying.' >&2
    exit 2
fi
"$@"
"#,
            method = if self.subscription { "chatgpt" } else { "api" },
            access = if self.subscription { "<>" } else { "<" },
            lock = if self.subscription { "-x" } else { "-s" },
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[ignore = "requires Docker Compose CLI; no daemon or credentials needed"]
    fn compose_keeps_credentials_out_of_worker_and_requires_explicit_agent_auth() {
        let compose = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../docker-compose.yml");
        for method in [None, Some("api-key"), Some("chatgpt")] {
            let mut command = std::process::Command::new("docker");
            command
                .args(["compose", "--env-file", "/dev/null", "-f"])
                .arg(&compose)
                .args(["config", "--format", "json"])
                .env("DONKEYSPACE_DEPLOYMENT_MODE", "minimal")
                .env(
                    "DONKEYSPACE_CODEX_AUTH_SOURCE",
                    if method.is_some() {
                        "/automation/auth.json"
                    } else {
                        ""
                    },
                )
                .env("DONKEYSPACE_CODEX_AUTH_METHOD", method.unwrap_or(""))
                .env("DONKEYSPACE_CODEX_AUTH_MOUNT_SUFFIX", ":z")
                .env("DONKEYSPACE_CODEX_HOME_SOURCE", "/old/personal/home")
                .env("DONKEYSPACE_CODEX_VOLUME", "old-personal-volume");
            let output = command.output().unwrap();
            assert!(output.status.success(), "Compose configuration failed");
            let config: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
            for service in ["api", "worker"] {
                for mount in config["services"][service]["volumes"].as_array().unwrap() {
                    let target = mount["target"].as_str().unwrap();
                    assert!(!target.starts_with("/root/.codex"));
                    assert_ne!(mount["source"].as_str(), Some("/old/personal/home"));
                    assert_ne!(mount["source"].as_str(), Some("/automation/auth.json"));
                }
            }
            assert!(
                config["volumes"]
                    .as_object()
                    .unwrap()
                    .keys()
                    .all(|name| !name.contains("codex"))
            );
            let environment = &config["services"]["worker"]["environment"];
            let auth = AgentAuthentication::parse(
                environment["DONKEYSPACE_CODEX_AUTH_SOURCE"].as_str(),
                environment["DONKEYSPACE_CODEX_AUTH_METHOD"].as_str(),
                environment["DONKEYSPACE_CODEX_AUTH_MOUNT_SUFFIX"].as_str(),
                true,
            );
            match method {
                None => assert!(auth.is_err()),
                Some(method) => assert_eq!(
                    auth.unwrap().unwrap().mount(),
                    format!(
                        "/automation/auth.json:/root/.codex/auth.json:{},z",
                        if method == "api-key" { "ro" } else { "rw" }
                    )
                ),
            }
        }
    }

    #[test]
    fn credential_file_scope_and_refresh_ownership_are_explicit() {
        for (method, access, lock) in [("api-key", "ro", "-s"), ("chatgpt", "rw", "-x")] {
            let auth = AgentAuthentication::parse(
                Some("/automation login/auth.json"),
                Some(method),
                Some(":Z"),
                true,
            )
            .unwrap()
            .unwrap();
            assert_eq!(
                auth.mount(),
                format!("/automation login/auth.json:/root/.codex/auth.json:{access},z")
            );
            assert!(auth.bootstrap().contains(&format!("flock {lock} 9")));
        }
        assert!(
            AgentAuthentication::parse(None, None, None, false)
                .unwrap()
                .is_none()
        );
        assert!(
            AgentAuthentication::parse(Some(""), Some(""), None, false)
                .unwrap()
                .is_none()
        );
        assert!(AgentAuthentication::parse(None, None, None, true).is_err());
    }

    #[test]
    fn malformed_or_incomplete_auth_configuration_never_falls_back() {
        for source in [
            "",
            "volume",
            "./auth.json",
            "/personal/home",
            "/home/../auth.json",
            "/home:ro/auth.json",
            "/home\n/auth.json",
        ] {
            assert!(
                AgentAuthentication::parse(Some(source), Some("api-key"), None, false).is_err(),
                "{source}"
            );
        }
        for method in [None, Some(""), Some("automatic")] {
            assert!(AgentAuthentication::parse(Some("/auth.json"), method, None, false).is_err());
        }
        assert!(
            AgentAuthentication::parse(Some("/auth.json"), Some("chatgpt"), Some(":ro"), false)
                .is_err()
        );
    }
}
