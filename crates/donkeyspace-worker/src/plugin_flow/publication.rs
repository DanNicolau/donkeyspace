use super::*;

pub(super) fn checkpoint_commit_title(
    flow: &PluginFlow,
    keys: &[TaskKey],
    issue_input: &Value,
    issue_number: i64,
) -> Option<String> {
    let mut tags = Vec::new();
    let mut descriptions = Vec::new();
    let mut work_items = Vec::new();
    for key in keys {
        let task = &flow.tasks[&key.task];
        if let Some(tag) = &task.publication_tag
            && !tags.contains(tag)
        {
            tags.push(tag.clone());
        }
        let description = task.display_name.as_deref().unwrap_or(&key.task);
        if !descriptions.iter().any(|value| value == description) {
            descriptions.push(description.to_string());
        }
        if let Some(work_item) = &key.work_item
            && !work_items.contains(work_item)
        {
            work_items.push(work_item.clone());
        }
    }
    if tags.is_empty() {
        return None;
    }
    let tags = tags
        .iter()
        .map(|tag| format!("[{tag}]"))
        .collect::<String>();
    let subject = if work_items.is_empty() {
        publication_issue_title(issue_input)
    } else {
        work_items.join(", ")
    };
    Some(limit_publication_title(&format!(
        "{tags} {}: {subject} (#{issue_number})",
        descriptions.join(" + ")
    )))
}

pub(super) fn publication_issue_title(issue_input: &Value) -> String {
    let value = issue_input
        .pointer("/issue/title")
        .and_then(Value::as_str)
        .unwrap_or("Implementation");
    let normalized = value.split_whitespace().collect::<Vec<_>>().join(" ");
    if normalized.is_empty() {
        "Implementation".into()
    } else {
        normalized
    }
}

pub(super) fn limit_publication_title(value: &str) -> String {
    const MAX_CHARS: usize = 240;
    let value = value.trim();
    if value.chars().count() <= MAX_CHARS {
        return value.to_string();
    }
    let mut shortened = value.chars().take(MAX_CHARS - 1).collect::<String>();
    shortened.push('…');
    shortened
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn publish_task_attempt(
    tracking: Option<&LifecycleTracking<'_>>,
    selection: &PluginFlowSelection,
    task_name: &str,
    task: &PluginTask,
    work_item: Option<&PluginWorkItem>,
    attempt: u32,
    workspace_path: &Path,
    aggregate_repo: &Path,
    parameters: &BTreeMap<String, Value>,
    parameter_definitions: &BTreeMap<String, PluginParameter>,
    job_id: Option<Uuid>,
    outcome: Option<Outcome>,
    reason: &str,
    related_issue_number: Option<i64>,
) {
    let Some(publication) = tracking.and_then(|tracking| tracking.publication.as_ref()) else {
        return;
    };
    let result = async {
        let declared_read = expand_templates(&task.read, parameters, work_item)?;
        let declared_write = expand_templates(&task.write, parameters, work_item)?;
        let (_, write_roots) = resolve_access(
            selection,
            task_name,
            &declared_read,
            &declared_write,
            parameters,
            parameter_definitions,
        )?;
        let diagnostics = expand_artifacts(&task.diagnostics, parameters, work_item)?;
        let redactions = selection
            .environment
            .values()
            .filter_map(|source| {
                if Path::new(source).is_absolute() {
                    fs::read_to_string(source).ok()
                } else {
                    env::var(source).ok()
                }
            })
            .map(|value| value.trim_end().to_string())
            .filter(|value| !value.is_empty())
            .collect::<Vec<_>>();
        let task_root = task_attempt_root(
            workspace_path,
            task_name,
            work_item.map(|item| item.id.as_str()),
            attempt,
        );
        publish_attempt(
            publication,
            aggregate_repo,
            &AttemptPublication {
                job_id,
                task: task_name,
                publication_tag: task.publication_tag.as_deref(),
                work_item: work_item.map(|item| item.id.as_str()),
                attempt,
                outcome,
                task_root: &task_root,
                write_roots: &write_roots,
                diagnostics: &diagnostics,
                reason,
                related_issue_number,
                redactions: &redactions,
            },
        )
        .await?;
        Ok::<_, Box<dyn std::error::Error>>(())
    }
    .await;
    if let Err(error) = result {
        tracing::warn!(%error, task = task_name, ?outcome, "forensic attempt publication failed");
    }
}
