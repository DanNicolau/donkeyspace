use super::*;

pub const REPOSITORIES_SAVED: &str = "Saved; awaiting controlled API/worker restart. New repositories start with empty starter/approver scopes. Configure trusted identities with `configure github-access`, then drain active work and run `configure repositories apply --confirm-drained`.";

impl Instance {
    pub fn repositories(&self) -> Result<&[String], SetupError> {
        let config = self.require_config()?;
        if config.github.is_none() {
            return Err(SetupError::Config("GitHub is not connected".into()));
        }
        Ok(github_repositories(config))
    }

    pub fn repositories_status(&self) -> Result<&'static str, SetupError> {
        Ok(if self.require_config()?.repositories_pending_apply {
            REPOSITORIES_SAVED
        } else {
            "Saved configuration; no pending repository edits. Use `status` to check running services."
        })
    }

    pub async fn accessible_repositories(&self) -> Result<Vec<GitHubRepository>, SetupError> {
        let owner = self
            .repositories()?
            .first()
            .and_then(|name| name.split_once('/'))
            .ok_or_else(|| {
                SetupError::Config("existing connection has no repository owner".into())
            })?
            .0;
        let provider = self.github_credential_provider()?;
        let repositories =
            tokio::time::timeout(std::time::Duration::from_secs(30), provider.repositories())
                .await
                .map_err(|_| {
                    SetupError::Config(
                        "GitHub repository discovery timed out; configuration was not changed"
                            .into(),
                    )
                })??;
        Ok(repositories
            .into_iter()
            .filter(|repo| repo.owner.eq_ignore_ascii_case(owner))
            .collect())
    }

    pub async fn add_repository(&mut self, repository: &str) -> Result<bool, SetupError> {
        let mut selected = self.repositories()?.to_vec();
        validate_repository_identity(repository)?;
        if selected
            .iter()
            .any(|name| name.eq_ignore_ascii_case(repository))
        {
            return Ok(false);
        }
        selected.push(repository.into());
        self.set_repositories(selected, false).await
    }

    /// Selection is validated in full before any saved configuration is changed.
    /// Existing entries may remain even after installation access is revoked.
    pub async fn set_repositories(
        &mut self,
        selected: Vec<String>,
        confirm_remove: bool,
    ) -> Result<bool, SetupError> {
        validate_selection(self.repositories()?, &selected, confirm_remove)?;
        let additions = selected.iter().any(|name| {
            !self
                .repositories()
                .unwrap()
                .iter()
                .any(|existing| existing.eq_ignore_ascii_case(name))
        });
        let accessible = if additions {
            self.accessible_repositories().await?
        } else {
            vec![]
        };
        self.save_repository_selection(selected, confirm_remove, &accessible)
    }

    pub fn remove_repository(
        &mut self,
        repository: &str,
        confirm: bool,
    ) -> Result<bool, SetupError> {
        validate_repository_identity(repository)?;
        let existing = self.repositories()?;
        if !confirm {
            return Err(SetupError::Config("removal requires --confirm; future ingestion stops after apply, history is retained, and work is not cancelled".into()));
        }
        if !existing
            .iter()
            .any(|name| name.eq_ignore_ascii_case(repository))
        {
            return Ok(false);
        }
        self.save_repository_selection(
            existing
                .iter()
                .filter(|name| !name.eq_ignore_ascii_case(repository))
                .cloned()
                .collect(),
            true,
            &[],
        )
    }

    fn save_repository_selection(
        &mut self,
        selected: Vec<String>,
        confirm_remove: bool,
        accessible: &[GitHubRepository],
    ) -> Result<bool, SetupError> {
        let original = self.require_config()?.clone();
        let existing = github_repositories(&original);
        validate_selection(existing, &selected, confirm_remove)?;
        let mut canonical = Vec::new();
        for name in selected {
            let name = if let Some(existing) =
                existing.iter().find(|old| old.eq_ignore_ascii_case(&name))
            {
                existing.clone()
            } else {
                accessible.iter().find(|repo| repo.full_name.eq_ignore_ascii_case(&name))
                    .map(|repo| repo.full_name.clone())
                    .ok_or_else(|| SetupError::Config(format!("repository `{name}` is unavailable to the saved GitHub connection. Grant this repository access in the existing App installation (or PAT), then retry; do not create another App or installation.")))?
            };
            if !canonical.contains(&name) {
                canonical.push(name);
            }
        }
        // Preserve existing ordering even when the picker returns an alphabetic list.
        let mut ordered: Vec<String> = existing
            .iter()
            .filter(|name| canonical.contains(name))
            .cloned()
            .collect();
        ordered.extend(
            canonical
                .into_iter()
                .filter(|name| !existing.contains(name)),
        );
        if ordered == existing {
            return Ok(false);
        }
        let _lock = self.configuration_lock()?;
        self.require_unchanged_configuration(&original)?;
        let mut updated = original.clone();
        // Old stray access entries must not grant a newly tracked repository trust.
        for scope in [GitHubAccessScope::Starters, GitHubAccessScope::Approvers] {
            github_access_map_mut(&mut updated, scope)
                .retain(|name, _| existing.iter().any(|old| old.eq_ignore_ascii_case(name)));
        }
        match updated.github.as_mut().unwrap() {
            GitHubInstanceConfig::App { repositories, .. }
            | GitHubInstanceConfig::Pat { repositories, .. } => *repositories = ordered,
        }
        reconcile_github_access(&mut updated);
        updated.repositories_pending_apply = true;
        self.config = Some(updated);
        if let Err(error) = self.save_unlocked() {
            self.config = Some(original);
            return Err(error);
        }
        Ok(true)
    }

    pub fn apply_repositories(&mut self, confirm_drained: bool) -> Result<(), SetupError> {
        self.apply_repositories_with(confirm_drained, |instance, args| {
            instance.compose_captured(args)
        })
    }

    fn apply_repositories_with(
        &mut self,
        confirm_drained: bool,
        mut compose: impl FnMut(&Self, &[&str]) -> Result<(), SetupError>,
    ) -> Result<(), SetupError> {
        if !confirm_drained {
            return Err(SetupError::Config("apply requires --confirm-drained: pause new submissions and let active work finish first; restarting a worker can interrupt active jobs".into()));
        }
        let _lock = self.configuration_lock()?;
        self.require_unchanged_configuration(self.require_config()?)?;
        self.config.as_mut().unwrap().repositories_pending_apply = true;
        self.save_unlocked()?;
        // Stop both consumers before starting either with the new configuration.
        // PostgreSQL, dashboard, volumes, workflow history and credentials are retained.
        compose(self, &["stop", "api", "worker"])?;
        if let Err(error) = compose(
            self,
            &[
                "up",
                "-d",
                "--no-deps",
                "--force-recreate",
                "--wait",
                "--wait-timeout",
                "60",
                "api",
                "worker",
            ],
        ) {
            let cleanup = compose(self, &["stop", "api", "worker"]);
            return Err(SetupError::Config(format!(
                "configuration is saved but apply failed: {error}; stopping both consumers: {cleanup:?}. Resolve the error and retry apply; PostgreSQL must already be running."
            )));
        }
        self.config.as_mut().unwrap().repositories_pending_apply = false;
        self.save_unlocked()
    }

    pub(crate) fn configuration_lock(&self) -> Result<fs::File, SetupError> {
        fs::create_dir_all(&self.directory)?;
        set_directory_mode(&self.directory)?;
        let lock = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(self.directory.join(".instance.lock"))?;
        lock.try_lock().map_err(|error| {
            SetupError::Config(format!(
                "another configuration operation is in progress; retry after it finishes: {error}"
            ))
        })?;
        Ok(lock)
    }

    pub(crate) fn require_unchanged_configuration(
        &self,
        expected: &InstanceConfig,
    ) -> Result<(), SetupError> {
        let current: InstanceConfig = serde_json::from_slice(&fs::read(self.config_path())?)?;
        if serde_json::to_value(current)? != serde_json::to_value(expected)? {
            return Err(SetupError::Config(
                "configuration changed while editing; reload and retry".into(),
            ));
        }
        Ok(())
    }

    pub(crate) fn ensure_repositories_can_start(&self) -> Result<(), SetupError> {
        if self.require_config()?.repositories_pending_apply {
            let status = self.deployment_status()?;
            if repositories_require_controlled_restart(&status) {
                return Err(SetupError::Config(REPOSITORIES_SAVED.into()));
            }
        }
        Ok(())
    }

    pub(crate) fn mark_repositories_applied(&mut self) -> Result<(), SetupError> {
        if self.require_config()?.repositories_pending_apply {
            self.config.as_mut().unwrap().repositories_pending_apply = false;
            self.save_unlocked()?;
        }
        Ok(())
    }
}

fn repositories_require_controlled_restart(status: &DeploymentStatus) -> bool {
    status.services.iter().any(|service| {
        matches!(service.name.as_str(), "api" | "worker")
            && !matches!(
                service.state.to_ascii_lowercase().as_str(),
                "exited" | "created"
            )
    })
}

fn validate_repository_identity(repository: &str) -> Result<(), SetupError> {
    let valid = repository.split_once('/').is_some_and(|(owner, name)| {
        !owner.is_empty()
            && !name.is_empty()
            && name != "."
            && name != ".."
            && owner
                .bytes()
                .all(|c| c.is_ascii_alphanumeric() || c == b'-')
            && name
                .bytes()
                .all(|c| c.is_ascii_alphanumeric() || b"-_.".contains(&c))
    });
    if !valid {
        return Err(SetupError::Config(format!(
            "invalid repository `{repository}`; expected owner/name"
        )));
    }
    Ok(())
}

fn validate_selection(
    existing: &[String],
    selected: &[String],
    confirm_remove: bool,
) -> Result<(), SetupError> {
    if selected.is_empty() {
        return Err(SetupError::Config("keep at least one repository in this GitHub connection; removing the last repository is not supported".into()));
    }
    for name in selected {
        validate_repository_identity(name)?;
    }
    validate_repositories(selected)?;
    let owner = existing
        .first()
        .and_then(|name| name.split_once('/'))
        .ok_or_else(|| SetupError::Config("existing connection has no repository owner".into()))?
        .0;
    if selected
        .iter()
        .any(|name| !name.split_once('/').unwrap().0.eq_ignore_ascii_case(owner))
    {
        return Err(SetupError::Config(format!(
            "repositories must remain under saved owner `{owner}` and the existing installation"
        )));
    }
    if !confirm_remove
        && existing
            .iter()
            .any(|old| !selected.iter().any(|name| name.eq_ignore_ascii_case(old)))
    {
        return Err(SetupError::Config(
            "confirm repository removal before saving; history and active work are retained".into(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use donkeyspace_core::EngagementGate;

    struct Fixture(Instance);
    impl Fixture {
        fn new(ingress: IngressMode) -> Self {
            let directory = env::temp_dir().join(format!(
                "donkeyspace-repositories-{}",
                random_hex(12).unwrap()
            ));
            fs::create_dir_all(directory.join("source/.donkeyspace")).unwrap();
            fs::write(directory.join("source/.donkeyspace/policy.yml"), "version: 1\nworkflow:\n  engagement:\n    repositories:\n      OWNER/B:\n        initial:\n          allow: [{type: user, login: intruder}]\n        needs_info_resume:\n          allow: [{type: user, login: intruder}]\n        blocked_resume:\n          allow: [{type: user, login: intruder}]\n        needs_human_resume:\n          allow: [{type: user, login: intruder}]\n").unwrap();
            let path = directory.join("source/.donkeyspace/policy.yml");
            let fragment: Value =
                serde_yaml::from_str(&fs::read_to_string(&path).unwrap()).unwrap();
            let mut base: Value =
                serde_yaml::from_str(include_str!("../../../.donkeyspace/policy.yml")).unwrap();
            base["workflow"]["engagement"] = fragment["workflow"]["engagement"].clone();
            fs::write(path, serde_yaml::to_string(&base).unwrap()).unwrap();
            let trusted = vec![GitHubAccessSubject::User {
                login: "alice".into(),
            }];
            let instance = Instance {
                saved_bytes: std::sync::Mutex::new(None),
                directory: directory.clone(),
                config: Some(InstanceConfig {
                    schema_version: SCHEMA_VERSION,
                    source_tree: directory.join("source"),
                    runtime_source: RuntimeSource::LocalBuild,
                    api_port: 18080,
                    web_port: 15173,
                    codex_home: Some(directory.join("auth")),
                    github: Some(GitHubInstanceConfig::App {
                        app_id: 42,
                        installation_id: 43,
                        private_key_file: directory.join("app.pem"),
                        webhook_secret_file: directory.join("secret"),
                        repositories: vec!["owner/a".into()],
                        ingress,
                    }),
                    repositories_pending_apply: false,
                    github_access: BTreeMap::from([("owner/a".into(), trusted.clone())]),
                    github_approvers: BTreeMap::from([("owner/a".into(), trusted)]),
                    plugins: BTreeMap::new(),
                    active_plugin: None,
                    facade: FacadeConfig::default(),
                }),
            };
            instance.save().unwrap();
            Self(instance)
        }
    }
    impl Drop for Fixture {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0.directory);
        }
    }
    fn accessible() -> Vec<GitHubRepository> {
        vec![GitHubRepository {
            owner: "owner".into(),
            name: "b".into(),
            full_name: "owner/b".into(),
            private: false,
        }]
    }
    fn add(instance: &mut Instance) -> Result<bool, SetupError> {
        instance.save_repository_selection(
            vec!["OWNER/B".into(), "owner/a".into(), "owner/b".into()],
            false,
            &accessible(),
        )
    }

    #[test]
    fn additive_selection_preserves_all_unrelated_configuration_and_both_ingress_modes() {
        for ingress in [
            IngressMode::Polling {
                interval_seconds: 137,
            },
            IngressMode::Webhook {
                public_url: "https://hooks.example/route".into(),
            },
        ] {
            let mut fixture = Fixture::new(ingress);
            let instance = &mut fixture.0;
            let before = serde_json::to_value(instance.config()).unwrap();
            assert!(add(instance).unwrap());
            assert_eq!(instance.repositories().unwrap(), ["owner/a", "owner/b"]);
            let saved = fs::read(instance.config_path()).unwrap();
            assert!(!add(instance).unwrap());
            assert_eq!(fs::read(instance.config_path()).unwrap(), saved);
            let mut expected = before;
            expected["github"]["repositories"] = json!(["owner/a", "owner/b"]);
            expected["github_access"]["owner/b"] = json!([]);
            expected["github_approvers"]["owner/b"] = json!([]);
            expected["repositories_pending_apply"] = json!(true);
            assert_eq!(serde_json::to_value(instance.config()).unwrap(), expected);
            assert_eq!(
                serde_json::to_value(
                    Instance::open(Some(instance.directory.clone()))
                        .unwrap()
                        .config()
                )
                .unwrap(),
                expected
            );
            instance
                .write_compose_env(instance.config().unwrap())
                .unwrap();
            let generated = fs::read_to_string(instance.directory.join(GENERATED_ENV)).unwrap();
            assert!(generated.contains("DONKEYSPACE_GITHUB_REPOSITORIES=owner/a,owner/b\n"));
            if expected["github"]["ingress"]["kind"] == "polling" {
                assert!(generated.contains("DONKEYSPACE_GITHUB_POLL_INTERVAL_SECONDS=137\n"));
            }
        }
    }

    #[test]
    fn invalid_inaccessible_cross_owner_and_unconfirmed_changes_leave_disk_unchanged() {
        let mut fixture = Fixture::new(IngressMode::polling());
        let instance = &mut fixture.0;
        let saved = fs::read(instance.config_path()).unwrap();
        for selected in [
            vec!["owner/a", "other/b"],
            vec!["owner/a", "owner/missing"],
            vec!["owner/a", "owner/../b"],
            vec!["owner/a", "owner/b\n"],
            vec!["owner/b"],
            vec![],
        ] {
            assert!(
                instance
                    .save_repository_selection(
                        selected.into_iter().map(str::to_string).collect(),
                        false,
                        &accessible()
                    )
                    .is_err()
            );
            assert_eq!(fs::read(instance.config_path()).unwrap(), saved);
        }
        assert!(instance.remove_repository("owner/a", false).is_err());
        assert!(instance.remove_repository("owner/a", true).is_err());
        assert_eq!(fs::read(instance.config_path()).unwrap(), saved);
    }

    #[test]
    fn removal_requires_confirmation_is_offline_and_readdition_denies_all_gates() {
        let mut fixture = Fixture::new(IngressMode::polling());
        let instance = &mut fixture.0;
        add(instance).unwrap();
        assert!(instance.remove_repository("owner/B", false).is_err());
        assert!(instance.remove_repository("owner/B", true).unwrap());
        assert!(!instance.remove_repository("owner/b", true).unwrap());
        assert!(
            !instance
                .config()
                .unwrap()
                .github_access
                .contains_key("owner/b")
        );
        add(instance).unwrap();
        instance.write_plugin_runtime_files().unwrap();
        let policy = Policy::from_yaml(
            &fs::read_to_string(instance.directory.join("effective-policy.yml")).unwrap(),
        )
        .unwrap();
        for gate in [
            EngagementGate::Initial,
            EngagementGate::NeedsInfoResume,
            EngagementGate::BlockedResume,
            EngagementGate::NeedsHumanResume,
        ] {
            assert!(
                policy
                    .workflow
                    .engagement
                    .rule(gate, Some("owner/b"))
                    .allow
                    .is_empty()
            );
            assert_eq!(
                policy
                    .workflow
                    .engagement
                    .rule(gate, Some("owner/a"))
                    .allow
                    .len(),
                1
            );
        }
    }

    #[test]
    fn concurrent_and_stale_repository_edit_cannot_overwrite_saved_work() {
        let mut fixture = Fixture::new(IngressMode::polling());
        let instance = &mut fixture.0;
        let _lock = instance.configuration_lock().unwrap();
        assert!(
            add(instance)
                .unwrap_err()
                .to_string()
                .contains("in progress")
        );
        drop(_lock);
        let mut stale = Instance::open(Some(instance.directory.clone())).unwrap();
        instance.configure_polling_interval(97).unwrap();
        assert!(
            add(&mut stale)
                .unwrap_err()
                .to_string()
                .contains("changed while editing")
        );
        assert_eq!(instance.repositories().unwrap(), ["owner/a"]);
        let mut stale_access = Instance::open(Some(instance.directory.clone())).unwrap();
        add(instance).unwrap();
        assert!(
            stale_access
                .remove_github_access(
                    "owner/a",
                    &GitHubAccessSubject::User {
                        login: "alice".into()
                    }
                )
                .unwrap_err()
                .to_string()
                .contains("changed while editing")
        );
        assert_eq!(
            Instance::open(Some(instance.directory.clone()))
                .unwrap()
                .repositories()
                .unwrap(),
            ["owner/a", "owner/b"]
        );
    }

    #[tokio::test]
    async fn repeated_add_needs_no_credential_files_or_network() {
        let mut fixture = Fixture::new(IngressMode::polling());
        assert!(!fixture.0.add_repository("OWNER/A").await.unwrap());
        assert!(!fixture.0.config().unwrap().repositories_pending_apply);
    }

    #[test]
    fn paused_restarting_and_unknown_consumers_require_controlled_apply() {
        for state in [
            "running",
            "paused",
            "restarting",
            "dead",
            "unknown",
            "exited",
            "created",
        ] {
            let status = DeploymentStatus {
                services: vec![ServiceStatus {
                    name: "worker".into(),
                    state: state.into(),
                    health: None,
                }],
            };
            assert_eq!(
                repositories_require_controlled_restart(&status),
                !matches!(state, "exited" | "created")
            );
        }
    }

    #[test]
    fn save_failure_restores_in_memory_configuration() {
        let mut fixture = Fixture::new(IngressMode::polling());
        let instance = &mut fixture.0;
        fs::create_dir(instance.directory.join(".instance.json.tmp")).unwrap();
        assert!(add(instance).is_err());
        assert_eq!(instance.repositories().unwrap(), ["owner/a"]);
        assert!(!instance.config().unwrap().repositories_pending_apply);
    }

    #[test]
    fn apply_is_explicit_stops_both_consumers_first_and_cleans_up_partial_start() {
        for failure in [None, Some(1), Some(2)] {
            let mut fixture = Fixture::new(IngressMode::polling());
            let instance = &mut fixture.0;
            add(instance).unwrap();
            instance
                .apply_repositories_with(false, |_, _| panic!("unconfirmed apply executed Docker"))
                .unwrap_err();
            let mut calls = Vec::new();
            let result = instance.apply_repositories_with(true, |_, args| {
                calls.push(args.iter().map(|s| s.to_string()).collect::<Vec<_>>());
                if failure == Some(calls.len()) {
                    Err(SetupError::Config("simulated Docker failure".into()))
                } else {
                    Ok(())
                }
            });
            assert_eq!(calls[0], ["stop", "api", "worker"]);
            if failure != Some(1) {
                assert_eq!(
                    calls[1],
                    [
                        "up",
                        "-d",
                        "--no-deps",
                        "--force-recreate",
                        "--wait",
                        "--wait-timeout",
                        "60",
                        "api",
                        "worker"
                    ]
                );
            }
            if failure == Some(2) {
                assert_eq!(calls[2], calls[0]);
            }
            assert_eq!(result.is_err(), failure.is_some());
            assert_eq!(
                Instance::open(Some(instance.directory.clone()))
                    .unwrap()
                    .config()
                    .unwrap()
                    .repositories_pending_apply,
                failure.is_some()
            );
        }
    }

    #[test]
    fn access_edits_during_pending_repository_change_only_save() {
        let mut fixture = Fixture::new(IngressMode::polling());
        let instance = &mut fixture.0;
        add(instance).unwrap();
        // No credentials or Docker are available to this fixture.
        instance
            .remove_github_access(
                "owner/a",
                &GitHubAccessSubject::User {
                    login: "alice".into(),
                },
            )
            .unwrap();
        assert!(instance.config().unwrap().repositories_pending_apply);
        assert!(!instance.directory.join(GENERATED_ENV).exists());
    }
}
