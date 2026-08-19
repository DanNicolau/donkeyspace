use crate::{
    ActivePlugin, InstalledPlugin, Instance, PluginFlowClass, SetupError, run_status, write_secret,
};
use donkeyspace_core::{PluginFlowSelection, PluginManifest, Policy, RepositoryEngagementPolicy};
use serde::Serialize;
use std::{collections::BTreeMap, fs, path::PathBuf, process::Command};

pub enum PluginEnvironmentInput {
    File(PathBuf),
    Value(String),
}

pub struct PluginConnectOptions {
    pub path: PathBuf,
    pub flow: Option<String>,
    pub environment: BTreeMap<String, PluginEnvironmentInput>,
}

#[derive(Serialize)]
struct Overlay {
    services: BTreeMap<&'static str, OverlayService>,
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    secrets: BTreeMap<String, OverlaySecret>,
}

#[derive(Serialize)]
struct OverlayService {
    environment: BTreeMap<&'static str, String>,
    volumes: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    secrets: Vec<OverlaySecretMount>,
}

#[derive(Serialize)]
struct OverlaySecret {
    file: String,
}

#[derive(Serialize)]
struct OverlaySecretMount {
    source: String,
    target: String,
}

impl Instance {
    pub fn connect_plugin(&mut self, options: PluginConnectOptions) -> Result<(), SetupError> {
        self.require_config()?;
        let source_path = fs::canonicalize(&options.path)?;
        let (source_path, manifest_path) = if source_path.is_dir() {
            let manifest = source_path.join("donkeyspace-plugin.yml");
            (source_path, manifest)
        } else {
            let parent = source_path
                .parent()
                .ok_or_else(|| SetupError::Config("plugin manifest has no parent".into()))?
                .to_path_buf();
            (parent, source_path)
        };
        let manifest = PluginManifest::from_path(&manifest_path)
            .map_err(|error| SetupError::Config(error.to_string()))?;
        let installation =
            manifest
                .installation
                .clone()
                .unwrap_or_else(|| donkeyspace_core::PluginInstallation {
                    build: Default::default(),
                    environment: BTreeMap::new(),
                });
        if let Some(existing) = self.require_config()?.plugins.get(&manifest.id)
            && existing.source_path != source_path
        {
            return Err(SetupError::Config(format!(
                "plugin id `{}` is already installed from {}; plugin ids are immutable",
                manifest.id,
                existing.source_path.display()
            )));
        }
        if let Some(flow) = options.flow.as_deref()
            && !manifest.flows.contains_key(flow)
        {
            return Err(SetupError::Config(format!(
                "plugin `{}` has no flow `{flow}`",
                manifest.id
            )));
        }
        for name in options.environment.keys() {
            if !installation.environment.contains_key(name) {
                return Err(SetupError::Config(format!(
                    "plugin does not declare installation environment `{name}`"
                )));
            }
        }
        let mut environment_files = BTreeMap::new();
        for (name, definition) in &installation.environment {
            let value = match options.environment.get(name) {
                Some(PluginEnvironmentInput::File(path)) => fs::read(path)?,
                Some(PluginEnvironmentInput::Value(value)) => value.as_bytes().to_vec(),
                None => definition
                    .default
                    .as_ref()
                    .map(|value| value.as_bytes().to_vec())
                    .unwrap_or_default(),
            };
            if definition.required && value.is_empty() {
                return Err(SetupError::Config(format!(
                    "plugin environment `{name}` is required; provide --environment-file {name}=PATH"
                )));
            }
            if !value.is_empty() {
                let path = self
                    .directory
                    .join("plugin-secrets")
                    .join(safe_id(&manifest.id))
                    .join(name);
                write_secret(&path, &value)?;
                environment_files.insert(name.clone(), path);
            }
        }
        let flows = manifest
            .flows
            .iter()
            .map(|(name, flow)| {
                (
                    name.clone(),
                    if flow.replaces_default_lifecycle {
                        PluginFlowClass::LifecycleReplacement
                    } else {
                        PluginFlowClass::Developer
                    },
                )
            })
            .collect();
        let plugin = InstalledPlugin {
            id: manifest.id.clone(),
            source_path: source_path.clone(),
            manifest_path,
            image: manifest.runtime.default_image,
            build_context: source_path.join(installation.build.context),
            dockerfile: source_path.join(installation.build.dockerfile),
            flows,
            environment_files,
        };
        self.ensure_plugin_image(&plugin, false)?;
        self.config
            .as_mut()
            .expect("configuration checked")
            .plugins
            .insert(plugin.id.clone(), plugin);
        self.save()?;
        if let Some(flow) = options.flow {
            self.activate_plugin(&manifest.id, &flow)?;
        }
        Ok(())
    }

    pub fn activate_plugin(&mut self, id: &str, flow: &str) -> Result<(), SetupError> {
        let config = self.require_config()?;
        let plugin = config
            .plugins
            .get(id)
            .ok_or_else(|| SetupError::Config(format!("plugin `{id}` is not installed")))?;
        let class = *plugin
            .flows
            .get(flow)
            .ok_or_else(|| SetupError::Config(format!("plugin `{id}` has no flow `{flow}`")))?;
        self.config.as_mut().unwrap().active_plugin = Some(ActivePlugin {
            id: id.into(),
            flow: flow.into(),
            class,
        });
        self.save()?;
        self.write_plugin_runtime_files()
    }

    pub fn disable_plugin(&mut self) -> Result<(), SetupError> {
        self.require_config()?;
        self.config.as_mut().unwrap().active_plugin = None;
        self.save()
    }

    pub fn rebuild_plugin(&self, id: &str) -> Result<(), SetupError> {
        let plugin = self
            .require_config()?
            .plugins
            .get(id)
            .ok_or_else(|| SetupError::Config(format!("plugin `{id}` is not installed")))?;
        self.ensure_plugin_image(plugin, true)
    }

    pub(crate) fn plugin_overlay_path(&self) -> PathBuf {
        self.directory.join("plugin-compose.yml")
    }

    pub(crate) fn write_plugin_runtime_files(&self) -> Result<(), SetupError> {
        let config = self.require_config()?;
        let base_policy_path = config.source_tree.join(".donkeyspace/policy.yml");
        let mut policy = Policy::from_yaml(&fs::read_to_string(&base_policy_path)?)
            .map_err(|error| SetupError::Config(error.to_string()))?;
        for (repository, subjects) in &config.github_access {
            let rules = policy
                .workflow
                .engagement
                .repositories
                .entry(repository.clone())
                .or_insert_with(RepositoryEngagementPolicy::default);
            rules.default.allow = subjects.iter().map(|subject| subject.selector()).collect();
        }
        let policy_path = self.directory.join("effective-policy.yml");
        let Some(active) = &config.active_plugin else {
            write_secret(&policy_path, serde_yaml::to_string(&policy)?.as_bytes())?;
            return Ok(());
        };
        let plugin = config.plugins.get(&active.id).ok_or_else(|| {
            SetupError::Config(format!("active plugin `{}` is not installed", active.id))
        })?;
        let mount_id = safe_id(&plugin.id);
        let manifest_target = format!("/plugins/{mount_id}/donkeyspace-plugin.yml");
        let environment = plugin
            .environment_files
            .keys()
            .enumerate()
            .map(|(index, name)| (name.clone(), format!("/run/secrets/plugin_env_{index}")))
            .collect();
        let selection = PluginFlowSelection {
            manifest_path: manifest_target,
            flow: active.flow.clone(),
            max_handoffs_per_edge: None,
            environment,
            parameters: BTreeMap::new(),
            task_access_overrides: BTreeMap::new(),
        };
        match active.class {
            PluginFlowClass::LifecycleReplacement => {
                policy.lifecycle.plugin = Some(selection);
                policy.agents.developer.plugin = None;
            }
            PluginFlowClass::Developer => {
                policy.lifecycle.plugin = None;
                policy.agents.developer.enabled = true;
                policy.agents.developer.command.clear();
                policy.agents.developer.plugin = Some(selection);
            }
        }
        write_secret(&policy_path, serde_yaml::to_string(&policy)?.as_bytes())?;

        let plugin_mount = format!("{}:/plugins/{mount_id}:ro", plugin.source_path.display());
        let policy_mount = format!("{}:/run/donkeyspace/policy.yml:ro", policy_path.display());
        let worker_environment = BTreeMap::from([(
            "DONKEYSPACE_POLICY_PATH",
            "/run/donkeyspace/policy.yml".into(),
        )]);
        let mut secrets = BTreeMap::new();
        let mut mounts = Vec::new();
        for (index, path) in plugin.environment_files.values().enumerate() {
            let secret_id = format!("donkeyspace_plugin_env_{index}");
            let target = format!("plugin_env_{index}");
            secrets.insert(
                secret_id.clone(),
                OverlaySecret {
                    file: path
                        .to_str()
                        .ok_or_else(|| {
                            SetupError::Config("plugin environment path is not UTF-8".into())
                        })?
                        .into(),
                },
            );
            mounts.push(OverlaySecretMount {
                source: secret_id,
                target: target.clone(),
            });
        }
        let common_environment = BTreeMap::from([(
            "DONKEYSPACE_POLICY_PATH",
            "/run/donkeyspace/policy.yml".into(),
        )]);
        let overlay = Overlay {
            services: BTreeMap::from([
                (
                    "api",
                    OverlayService {
                        environment: common_environment,
                        volumes: vec![plugin_mount.clone(), policy_mount.clone()],
                        secrets: Vec::new(),
                    },
                ),
                (
                    "worker",
                    OverlayService {
                        environment: worker_environment,
                        volumes: vec![
                            plugin_mount,
                            policy_mount,
                            "/var/run/docker.sock:/var/run/docker.sock".into(),
                        ],
                        secrets: mounts,
                    },
                ),
            ]),
            secrets,
        };
        write_secret(
            &self.plugin_overlay_path(),
            serde_yaml::to_string(&overlay)?.as_bytes(),
        )
    }

    fn ensure_plugin_image(&self, plugin: &InstalledPlugin, force: bool) -> Result<(), SetupError> {
        if !force
            && Command::new("docker")
                .args(["image", "inspect", &plugin.image])
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .status()
                .is_ok_and(|status| status.success())
        {
            return Ok(());
        }
        run_status(
            Command::new("docker")
                .arg("build")
                .args(["--tag", &plugin.image, "--file"])
                .arg(&plugin.dockerfile)
                .arg(&plugin.build_context),
        )
    }
}

fn safe_id(value: &str) -> String {
    value
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || character == '-' || character == '_' {
                character
            } else {
                '-'
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{GitHubAccessSubject, InstanceConfig, RuntimeSource};
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn generated_runtime_selects_one_flow_without_serializing_secret_values() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let directory = std::env::temp_dir().join(format!("donkeyspace-plugin-test-{unique}"));
        let plugin_root = directory.join("plugin");
        fs::create_dir_all(&plugin_root).unwrap();
        fs::write(plugin_root.join("donkeyspace-plugin.yml"), "fixture").unwrap();
        let secret = directory.join("secret");
        write_secret(&secret, b"do-not-serialize").unwrap();
        let source_tree = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .unwrap()
            .parent()
            .unwrap()
            .to_path_buf();
        let plugin = InstalledPlugin {
            id: "example.plugin".into(),
            source_path: plugin_root.clone(),
            manifest_path: plugin_root.join("donkeyspace-plugin.yml"),
            image: "example:dev".into(),
            build_context: plugin_root.clone(),
            dockerfile: plugin_root.join("Dockerfile"),
            flows: BTreeMap::from([("replacement".into(), PluginFlowClass::LifecycleReplacement)]),
            environment_files: BTreeMap::from([("API_TOKEN".into(), secret)]),
        };
        let instance = Instance {
            directory: directory.clone(),
            config: Some(InstanceConfig {
                schema_version: 4,
                source_tree,
                runtime_source: RuntimeSource::LocalBuild,
                api_port: 8080,
                web_port: 5173,
                codex_home: None,
                github: None,
                github_access: BTreeMap::from([(
                    "acme/rtl".into(),
                    vec![GitHubAccessSubject::User {
                        login: "alice".into(),
                    }],
                )]),
                plugins: BTreeMap::from([(plugin.id.clone(), plugin)]),
                active_plugin: Some(ActivePlugin {
                    id: "example.plugin".into(),
                    flow: "replacement".into(),
                    class: PluginFlowClass::LifecycleReplacement,
                }),
            }),
        };
        instance.write_plugin_runtime_files().unwrap();
        let overlay = fs::read_to_string(instance.plugin_overlay_path()).unwrap();
        let policy = fs::read_to_string(directory.join("effective-policy.yml")).unwrap();
        assert!(overlay.contains("/var/run/docker.sock"));
        assert!(overlay.contains("plugin_env_0"));
        assert!(!overlay.contains("do-not-serialize"));
        assert!(policy.contains("replacement"));
        assert!(policy.contains("/run/secrets/plugin_env_0"));
        assert!(policy.contains("acme/rtl"));
        assert!(policy.contains("alice"));
        assert!(!policy.contains("do-not-serialize"));
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn mount_ids_are_stable_and_compose_safe() {
        assert_eq!(safe_id("donkeyspace.epic-rtl"), "donkeyspace-epic-rtl");
    }
}
