use super::*;
pub(super) use crate::repository_files::copy_root;

pub(super) const MAX_RESOURCE_FILES: usize = 1_024;
pub(super) const MAX_RESOURCE_BYTES: u64 = 32 * 1024 * 1024;

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub(super) struct MaterializedResource {
    pub(super) id: String,
    pub(super) source: PluginResourceSource,
    pub(super) source_path: String,
    pub(super) root: String,
    pub(super) available: bool,
    pub(super) inventory: Vec<String>,
    pub(super) digest: Option<String>,
}

pub(super) fn declared_diagnostics_present(
    task: &PluginTask,
    workspace_path: &Path,
    task_name: &str,
    work_item: Option<&PluginWorkItem>,
    attempt: u32,
    parameters: &BTreeMap<String, Value>,
) -> bool {
    let Ok(diagnostics) = expand_artifacts(&task.diagnostics, parameters, work_item) else {
        return false;
    };
    let repo = task_attempt_root(
        workspace_path,
        task_name,
        work_item.map(|item| item.id.as_str()),
        attempt,
    )
    .join("repo");
    diagnostics_present_at(&repo, &diagnostics)
}

pub(super) fn diagnostics_present_at(repo: &Path, diagnostics: &[PluginArtifact]) -> bool {
    diagnostics.iter().any(|diagnostic| {
        let path = repo.join(&diagnostic.path);
        match diagnostic.kind {
            PluginArtifactType::File => path.metadata().is_ok_and(|metadata| metadata.len() > 0),
            PluginArtifactType::Directory => path
                .read_dir()
                .is_ok_and(|mut entries| entries.next().is_some()),
        }
    })
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn execute_task(
    selection: &PluginFlowSelection,
    manifest: &PluginManifest,
    task_name: &str,
    task: &PluginTask,
    work_item: Option<&PluginWorkItem>,
    attempt: u32,
    repo_path: &Path,
    workspace_path: &Path,
    issue_input: &Value,
    previous: &[Value],
    parameters: &BTreeMap<String, Value>,
    plugin_root: &Path,
) -> Result<PluginTaskResult, Box<dyn std::error::Error>> {
    let role = &manifest.roles[&task.role];
    let task_root = task_attempt_root(
        workspace_path,
        task_name,
        work_item.map(|item| item.id.as_str()),
        attempt,
    );
    let task_repo = task_root.join("repo");
    fs::create_dir_all(&task_repo)?;
    let declared_read = expand_templates(&task.read, parameters, work_item)?;
    let declared_write = expand_templates(&task.write, parameters, work_item)?;
    let (read_roots, write_roots) = resolve_access(
        selection,
        task_name,
        &declared_read,
        &declared_write,
        parameters,
        &manifest.parameters,
    )?;
    let diagnostics = expand_artifacts(&task.diagnostics, parameters, work_item)?;
    if let Some(diagnostic) = diagnostics.iter().find(|diagnostic| {
        !covered(&diagnostic.path, &read_roots) && !covered(&diagnostic.path, &write_roots)
    }) {
        return Err(format!(
            "task `{task_name}` diagnostic `{}` is outside its declared roots",
            diagnostic.path
        )
        .into());
    }
    for root in read_roots.iter().chain(&write_roots) {
        copy_root(repo_path, &task_repo, root)?;
    }
    let donkeyspace = task_root.join(".donkeyspace");
    fs::create_dir_all(&donkeyspace)?;
    let resources = materialize_resources(
        manifest,
        &task.role,
        task,
        plugin_root,
        repo_path,
        &task_root,
        parameters,
    )?;
    let selected_mcp = role
        .mcp_servers
        .iter()
        .filter_map(|name| manifest.mcp_servers.get(name).map(|server| (name, server)))
        .collect::<BTreeMap<_, _>>();
    let files = donkeyspace_runner::RunFiles::prepare(&task_root, &json!({
            "run_id": issue_input.pointer("/run_id"),
            "role": task.role,
            "plugin": {"id": manifest.id, "flow": selection.flow, "task": task_name, "attempt": attempt, "allowed_handoffs": task.allowed_handoffs},
            "work_item": work_item,
            "issue": issue_input.pointer("/issue").unwrap_or(issue_input),
            "repository": issue_input.pointer("/repository"),
            "workspace": {"repo_path": "repo", "result_path": ".donkeyspace/run-result.json", "read": read_roots, "write": write_roots},
            "parameters": parameters,
            "resources": resources,
            "previous_tasks": previous.iter().rev().take(64).rev().collect::<Vec<_>>(),
            "mcp_servers": selected_mcp,
        })).await?;
    let image = role
        .image
        .as_deref()
        .unwrap_or(&manifest.runtime.default_image);
    let output = run_container(
        image,
        &role.command,
        &task_root,
        &selection.environment,
        &role.environment,
        crate::plugin_container::ExecutionKind::Agent,
    )
    .await?;
    files.write_logs(&output.stdout, &output.stderr).await?;
    if !output.status.success() {
        return Err(format!(
            "plugin task `{task_name}` exited {:?}: {}",
            output.status.code(),
            String::from_utf8_lossy(&output.stderr).trim()
        )
        .into());
    }
    let mut task_result: PluginTaskResult = files.read().await?;
    validate_resources_used(&task_result.resources_used, &resources)?;
    validate_changed_files(&task_result.result.changed_files, &write_roots)?;
    verify_resources(&task_root, &resources)?;
    if is_publishable(task_result.result.outcome) {
        let artifacts = expand_artifacts(&task.artifacts, parameters, work_item)?;
        validate_artifacts(&task_repo, &artifacts, &write_roots)?;
        let validator_results = run_validators(&task.validators, image, &task_root).await?;
        apply_validator_results(&mut task_result.result, validator_results);
    }
    task_result.result.validate_for_orchestration()?;
    let preserved = expand_artifacts(&task.preserve_on_success, parameters, work_item)?;
    retain_task_output(
        &task_repo,
        repo_path,
        &write_roots,
        &preserved,
        task_result.result.outcome,
    )?;
    Ok(task_result)
}

/// Called only after exit zero, result validation, and resource verification.
/// Preserve a narrow declaration on pauses; never copy its enclosing write root.
pub(super) fn retain_task_output(
    task_repo: &Path,
    repo_path: &Path,
    write_roots: &[String],
    preserved: &[PluginArtifact],
    outcome: Outcome,
) -> Result<(), Box<dyn std::error::Error>> {
    validate_artifacts(task_repo, preserved, write_roots)?;
    let roots = if is_publishable(outcome) {
        write_roots.to_vec()
    } else {
        preserved
            .iter()
            // An optional missing artifact must not erase an earlier report.
            .filter(|artifact| task_repo.join(&artifact.path).exists())
            .map(|artifact| artifact.path.clone())
            .collect()
    };
    crate::repository_files::replace_roots(task_repo, repo_path, &roots)
}

pub(super) fn retains_output(task: &PluginTask, outcome: Outcome) -> bool {
    is_publishable(outcome) || !task.preserve_on_success.is_empty()
}

pub(super) fn task_attempt_root(
    workspace_path: &Path,
    task_name: &str,
    work_item: Option<&str>,
    attempt: u32,
) -> PathBuf {
    let item_suffix = work_item.map(|item| format!("-{item}")).unwrap_or_default();
    workspace_path
        .join("plugin-tasks")
        .join(format!("{attempt:04}-{task_name}{item_suffix}"))
}

pub(super) fn expand_templates(
    values: &[String],
    parameters: &BTreeMap<String, Value>,
    work_item: Option<&PluginWorkItem>,
) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    values
        .iter()
        .map(|value| {
            let expanded = expand_template(value, parameters)?;
            Ok(match work_item {
                Some(item) => expanded.replace("{work_item}", &item.id),
                None => expanded,
            })
        })
        .collect()
}

pub(super) fn resolve_parameters(
    manifest: &PluginManifest,
    selection: &PluginFlowSelection,
) -> Result<BTreeMap<String, Value>, Box<dyn std::error::Error>> {
    if let Some(name) = selection
        .parameters
        .keys()
        .find(|name| !manifest.parameters.contains_key(*name))
    {
        return Err(format!("unknown plugin parameter `{name}`").into());
    }
    let mut resolved = BTreeMap::new();
    for (name, definition) in &manifest.parameters {
        let selected = selection.parameters.get(name);
        if let Some(selected) = selected {
            let valid = match definition {
                PluginParameter::Path { .. }
                | PluginParameter::Enum { .. }
                | PluginParameter::String { .. } => selected.is_string(),
                PluginParameter::Integer { .. } => selected.is_i64(),
                PluginParameter::Boolean { .. } => selected.is_boolean(),
            };
            if !valid {
                return Err(format!("invalid type for plugin parameter `{name}`").into());
            }
        }
        let value = match definition {
            PluginParameter::Path { default } => {
                let value = selected
                    .and_then(Value::as_str)
                    .map(str::to_owned)
                    .or_else(|| default.clone())
                    .ok_or_else(|| format!("missing plugin parameter `{name}`"))?;
                validate_runtime_path(&value)?;
                Value::String(value)
            }
            PluginParameter::Enum { values, default } => {
                let value = selected
                    .and_then(Value::as_str)
                    .map(str::to_owned)
                    .or_else(|| default.clone())
                    .ok_or_else(|| format!("missing plugin parameter `{name}`"))?;
                if !values.contains(&value) {
                    return Err(format!("invalid value for enum parameter `{name}`").into());
                }
                Value::String(value)
            }
            PluginParameter::String { default } => Value::String(
                selected
                    .and_then(Value::as_str)
                    .map(str::to_owned)
                    .or_else(|| default.clone())
                    .ok_or_else(|| format!("missing plugin parameter `{name}`"))?,
            ),
            PluginParameter::Integer { default } => Value::Number(
                selected
                    .and_then(Value::as_i64)
                    .or(*default)
                    .ok_or_else(|| format!("missing plugin parameter `{name}`"))?
                    .into(),
            ),
            PluginParameter::Boolean { default } => Value::Bool(
                selected
                    .and_then(Value::as_bool)
                    .or(*default)
                    .ok_or_else(|| format!("missing plugin parameter `{name}`"))?,
            ),
        };
        resolved.insert(name.clone(), value);
    }
    Ok(resolved)
}

pub(super) fn expand_template(
    template: &str,
    parameters: &BTreeMap<String, Value>,
) -> Result<String, Box<dyn std::error::Error>> {
    let mut expanded = template.to_string();
    for (name, value) in parameters {
        if let Some(value) = value.as_str() {
            expanded = expanded.replace(&format!("{{{name}}}"), value);
        }
    }
    let remainder = expanded.replace("{work_item}", "item");
    if remainder.contains('{') || remainder.contains('}') {
        return Err(format!("unknown placeholder in filesystem field `{template}`").into());
    }
    validate_runtime_path(&remainder)?;
    Ok(expanded)
}

pub(super) fn validate_runtime_path(value: &str) -> Result<(), Box<dyn std::error::Error>> {
    donkeyspace_core::plugin::validate_repository_path(value)?;
    Ok(())
}

pub(super) fn merged_resource_assignments(
    role: &[PluginResourceAssignment],
    task: &[PluginResourceAssignment],
) -> BTreeMap<String, bool> {
    let mut merged = BTreeMap::new();
    for assignment in role.iter().chain(task) {
        merged
            .entry(assignment.id.clone())
            .and_modify(|required| *required |= assignment.required)
            .or_insert(assignment.required);
    }
    merged
}

#[allow(clippy::too_many_arguments)]
pub(super) fn materialize_resources(
    manifest: &PluginManifest,
    role_name: &str,
    task: &PluginTask,
    plugin_root: &Path,
    repo_root: &Path,
    attempt_root: &Path,
    parameters: &BTreeMap<String, Value>,
) -> Result<Vec<MaterializedResource>, Box<dyn std::error::Error>> {
    let assignments =
        merged_resource_assignments(&manifest.roles[role_name].resources, &task.resources);
    let mut result = Vec::new();
    for (id, required) in assignments {
        let definition = &manifest.resources[&id];
        let source_path = expand_template(&definition.path, parameters)?;
        let source_root = match definition.source {
            PluginResourceSource::Plugin => plugin_root,
            PluginResourceSource::Repository => repo_root,
        };
        let source = source_root.join(&source_path);
        let relative_root = format!(".donkeyspace/resources/{id}");
        let target = attempt_root.join(&relative_root);
        let metadata = match fs::symlink_metadata(&source) {
            Ok(metadata) => Some(metadata),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
            Err(error) => return Err(error.into()),
        };
        let Some(metadata) = metadata else {
            if required {
                return Err(
                    format!("required resource `{id}` is missing at `{source_path}`").into(),
                );
            }
            result.push(MaterializedResource {
                id,
                source: definition.source,
                source_path,
                root: relative_root,
                available: false,
                inventory: Vec::new(),
                digest: None,
            });
            continue;
        };
        if metadata.file_type().is_symlink() {
            return Err(format!("resource `{id}` may not be a symlink").into());
        }
        fs::create_dir_all(&target)?;
        if metadata.is_file() {
            let basename = source
                .file_name()
                .ok_or_else(|| format!("resource `{id}` has no basename"))?;
            copy_resource_entry(&source, &target.join(basename))?;
        } else if metadata.is_dir() {
            copy_resource_directory(&source, &target)?;
        } else {
            return Err(format!("resource `{id}` is not a regular file or directory").into());
        }
        let (inventory, digest) = digest_resource_tree(&target)?;
        result.push(MaterializedResource {
            id,
            source: definition.source,
            source_path,
            root: relative_root,
            available: true,
            inventory,
            digest: Some(digest),
        });
    }
    Ok(result)
}

pub(super) fn copy_resource_directory(
    source: &Path,
    target: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut entries = fs::read_dir(source)?.collect::<Result<Vec<_>, _>>()?;
    entries.sort_by_key(|entry| entry.file_name());
    for entry in entries {
        let metadata = fs::symlink_metadata(entry.path())?;
        if metadata.file_type().is_symlink() {
            return Err(format!("resource contains symlink `{}`", entry.path().display()).into());
        }
        let destination = target.join(entry.file_name());
        if metadata.is_dir() {
            fs::create_dir_all(&destination)?;
            copy_resource_directory(&entry.path(), &destination)?;
        } else if metadata.is_file() {
            copy_resource_entry(&entry.path(), &destination)?;
        } else {
            return Err(format!(
                "resource contains special file `{}`",
                entry.path().display()
            )
            .into());
        }
    }
    Ok(())
}

pub(super) fn copy_resource_entry(
    source: &Path,
    target: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    if let Some(parent) = target.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::copy(source, target)?;
    Ok(())
}

pub(super) fn digest_resource_tree(
    root: &Path,
) -> Result<(Vec<String>, String), Box<dyn std::error::Error>> {
    let root_metadata = fs::symlink_metadata(root)?;
    if root_metadata.file_type().is_symlink() || !root_metadata.is_dir() {
        return Err("materialized resource root is not a regular directory".into());
    }
    fn visit(directory: &Path, files: &mut Vec<PathBuf>) -> Result<(), Box<dyn std::error::Error>> {
        let mut entries = fs::read_dir(directory)?.collect::<Result<Vec<_>, _>>()?;
        entries.sort_by_key(|entry| entry.file_name());
        for entry in entries {
            let metadata = fs::symlink_metadata(entry.path())?;
            if metadata.file_type().is_symlink() {
                return Err("materialized resource contains a symlink".into());
            }
            if metadata.is_dir() {
                visit(&entry.path(), files)?;
            } else if metadata.is_file() {
                files.push(entry.path());
            } else {
                return Err("materialized resource contains a special file".into());
            }
        }
        Ok(())
    }
    let mut files = Vec::new();
    visit(root, &mut files)?;
    if files.len() > MAX_RESOURCE_FILES {
        return Err(format!("resource exceeds {MAX_RESOURCE_FILES} files").into());
    }
    let mut inventory = Vec::with_capacity(files.len());
    let mut hasher = Sha256::new();
    let mut total = 0u64;
    for file in files {
        let relative = file
            .strip_prefix(root)?
            .to_string_lossy()
            .replace('\\', "/");
        let contents = fs::read(&file)?;
        total = total.saturating_add(contents.len() as u64);
        if total > MAX_RESOURCE_BYTES {
            return Err(format!("resource exceeds {MAX_RESOURCE_BYTES} bytes").into());
        }
        hasher.update((relative.len() as u64).to_be_bytes());
        hasher.update(relative.as_bytes());
        hasher.update((contents.len() as u64).to_be_bytes());
        hasher.update(&contents);
        inventory.push(relative);
    }
    Ok((inventory, format!("sha256:{:x}", hasher.finalize())))
}

pub(super) fn verify_resources(
    attempt_root: &Path,
    resources: &[MaterializedResource],
) -> Result<(), Box<dyn std::error::Error>> {
    for resource in resources.iter().filter(|resource| resource.available) {
        let (inventory, digest) = digest_resource_tree(&attempt_root.join(&resource.root))?;
        if inventory != resource.inventory || Some(digest) != resource.digest {
            return Err(format!("resource `{}` was modified during execution", resource.id).into());
        }
    }
    Ok(())
}

pub(super) fn validate_resources_used(
    used: &[String],
    resources: &[MaterializedResource],
) -> Result<(), Box<dyn std::error::Error>> {
    let supplied = resources
        .iter()
        .filter(|resource| resource.available)
        .map(|resource| resource.id.as_str())
        .collect::<BTreeSet<_>>();
    if let Some(id) = used.iter().find(|id| !supplied.contains(id.as_str())) {
        return Err(format!("plugin reported unsupplied resource `{id}`").into());
    }
    Ok(())
}

pub(super) fn expand_artifacts(
    artifacts: &[PluginArtifact],
    parameters: &BTreeMap<String, Value>,
    work_item: Option<&PluginWorkItem>,
) -> Result<Vec<PluginArtifact>, Box<dyn std::error::Error>> {
    artifacts
        .iter()
        .map(|artifact| {
            let mut artifact = artifact.clone();
            artifact.path = expand_template(&artifact.path, parameters)?;
            if let Some(item) = work_item {
                artifact.path = artifact.path.replace("{work_item}", &item.id);
            }
            Ok(artifact)
        })
        .collect()
}

pub(super) fn validate_artifacts(
    repo: &Path,
    artifacts: &[PluginArtifact],
    write_roots: &[String],
) -> Result<(), Box<dyn std::error::Error>> {
    for artifact in artifacts {
        if !covered(&artifact.path, write_roots) {
            return Err(format!("artifact `{}` is outside task write roots", artifact.path).into());
        }
        let path = repo.join(&artifact.path);
        let metadata = match fs::symlink_metadata(&path) {
            Ok(metadata) => Some(metadata),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
            Err(error) => return Err(error.into()),
        };
        let Some(metadata) = metadata else {
            if artifact.required {
                return Err(format!("required artifact `{}` is missing", artifact.path).into());
            }
            continue;
        };
        if metadata.file_type().is_symlink()
            || match artifact.kind {
                PluginArtifactType::File => !metadata.is_file(),
                PluginArtifactType::Directory => !metadata.is_dir(),
            }
        {
            return Err(format!("artifact `{}` has the wrong type", artifact.path).into());
        }
    }
    Ok(())
}

pub(super) fn is_publishable(outcome: Outcome) -> bool {
    outcome == Outcome::Implemented
}

pub(super) fn apply_validator_results(result: &mut RunResult, validator_results: Vec<TestResult>) {
    let validators_passed = validator_results
        .iter()
        .all(|result| result.status == TestStatus::Passed);
    result.tests.extend(validator_results);
    if !validators_passed {
        result.outcome = Outcome::Failed;
        result.blocked_reason = Some("plugin validator failed".into());
    }
}

pub(super) async fn run_validators(
    validators: &[PluginValidator],
    image: &str,
    task_root: &Path,
) -> Result<Vec<TestResult>, Box<dyn std::error::Error>> {
    let mut results = Vec::new();
    for validator in validators {
        let output = run_container(
            image,
            &validator.command,
            task_root,
            &BTreeMap::new(),
            &[],
            crate::plugin_container::ExecutionKind::Check,
        )
        .await?;
        results.push(TestResult {
            name: validator.name.clone(),
            command: validator.command.clone(),
            status: if output.status.success() {
                TestStatus::Passed
            } else {
                TestStatus::Failed
            },
            exit_code: output.status.code(),
            summary: Some(if output.status.success() {
                String::from_utf8_lossy(&output.stdout)
                    .trim()
                    .chars()
                    .take(2_000)
                    .collect()
            } else {
                String::from_utf8_lossy(&output.stderr)
                    .trim()
                    .chars()
                    .take(2_000)
                    .collect()
            }),
        });
    }
    Ok(results)
}

pub(super) fn resolve_access(
    selection: &PluginFlowSelection,
    stage: &str,
    declared_read: &[String],
    declared_write: &[String],
    parameters: &BTreeMap<String, Value>,
    parameter_definitions: &BTreeMap<String, PluginParameter>,
) -> Result<(Vec<String>, Vec<String>), Box<dyn std::error::Error>> {
    let Some(overrides) = selection.task_access_overrides.get(stage) else {
        return Ok((declared_read.to_vec(), declared_write.to_vec()));
    };
    let read = overrides
        .read
        .clone()
        .map(|values| expand_policy_roots(&values, parameters, parameter_definitions))
        .transpose()?
        .unwrap_or_else(|| declared_read.to_vec());
    let write = overrides
        .write
        .clone()
        .map(|values| expand_policy_roots(&values, parameters, parameter_definitions))
        .transpose()?
        .unwrap_or_else(|| declared_write.to_vec());
    if !read.iter().all(|path| covered(path, declared_read))
        || !write.iter().all(|path| covered(path, declared_write))
    {
        return Err(
            format!("policy access override widens plugin task `{stage}` permissions").into(),
        );
    }
    Ok((read, write))
}

pub(super) fn expand_policy_roots(
    values: &[String],
    parameters: &BTreeMap<String, Value>,
    definitions: &BTreeMap<String, PluginParameter>,
) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    for value in values {
        for segment in value.split('{').skip(1) {
            let name = segment
                .split_once('}')
                .map(|(name, _)| name)
                .ok_or_else(|| format!("unclosed placeholder in policy path `{value}`"))?;
            if !matches!(
                definitions.get(name),
                Some(PluginParameter::Path { .. } | PluginParameter::Enum { .. })
            ) {
                return Err(format!(
                    "parameter `{name}` cannot be used in a policy filesystem field"
                )
                .into());
            }
        }
    }
    expand_templates(values, parameters, None)
}

pub(super) fn covered(path: &str, roots: &[String]) -> bool {
    roots.iter().any(|root| {
        path == root
            || path
                .strip_prefix(root)
                .is_some_and(|rest| rest.starts_with('/'))
    })
}

pub(super) fn validate_changed_files(
    files: &[String],
    roots: &[String],
) -> Result<(), Box<dyn std::error::Error>> {
    if let Some(path) = files
        .iter()
        .find(|path| validate_runtime_path(path).is_err() || !covered(path, roots))
    {
        return Err(format!("plugin reported change outside task write roots: `{path}`").into());
    }
    Ok(())
}
