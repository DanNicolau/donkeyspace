use super::*;

/// Restore only scheduling/provenance state; source files always come from the
/// current PR head. Old child execution IDs must never be reused by a new run.
pub(crate) fn prepare_pr_revision(
    selection: &PluginFlowSelection,
    snapshot: Value,
    target: &str,
    repo: &Path,
    workspace: &Path,
    changed_paths: &[String],
) -> Result<(String, Vec<i64>), Box<dyn std::error::Error>> {
    let manifest = PluginManifest::from_path(&selection.manifest_path)?;
    let flow = manifest
        .flows
        .get(&selection.flow)
        .ok_or("revision flow is missing")?;
    let mut saved: LifecycleCheckpoint = serde_json::from_value(snapshot)?;
    let effective_flow = saved
        .execution_plan
        .as_ref()
        .map(|plan| plan.apply(flow))
        .transpose()?;
    let flow = effective_flow.as_ref().unwrap_or(flow);
    let mut compatible_flow = flow.clone();
    compatible_flow.change_intake = saved.change_intake_enabled;
    let fingerprint = flow_fingerprint(&compatible_flow)?;
    if saved
        .flow_fingerprint
        .as_ref()
        .is_some_and(|prior| prior != &fingerprint)
    {
        return Err("The completed lifecycle used a different task graph; restore that plugin flow before requesting a revision.".into());
    }
    let parameters = resolve_parameters(&manifest, selection)?;
    let registry: PluginWorkItemRegistry = serde_json::from_str(&fs::read_to_string(
        repo.join(expand_template(
            flow.work_items_path
                .as_deref()
                .ok_or("missing work item catalog")?,
            &parameters,
        )?),
    )?)?;
    let items = registry
        .work_items
        .into_iter()
        .filter(|item| saved.active_work_items.contains(&item.id))
        .collect::<Vec<_>>();
    let target = if target.is_empty() && flow.change_intake {
        &flow.start
    } else {
        target
    };
    let (task, item) = target
        .split_once('/')
        .map_or((target, None), |(task, item)| {
            (task, Some(item.to_string()))
        });
    let key = TaskKey {
        task: task.into(),
        work_item: item,
    };
    let mut graph = TaskGraph::for_work_items(flow, &items);
    graph.restore_completed(&saved.completed_keys)?;
    if !flow.change_intake && !saved.completed_keys.contains(&key) {
        return Err("revision target was not completed".into());
    }
    let mut invalidated = if flow.change_intake {
        BTreeSet::new()
    } else {
        graph
            .restart_from(&key)?
            .into_iter()
            .collect::<BTreeSet<_>>()
    };
    // Human edits since the accepted snapshot can invalidate additional task
    // evidence. Use declared access roots rather than domain-specific paths.
    for candidate in graph.keys().cloned().collect::<Vec<_>>() {
        let task = &flow.tasks[&candidate.task];
        let item = candidate
            .work_item
            .as_deref()
            .and_then(|id| items.iter().find(|item| item.id == id));
        let mut roots = expand_templates(&task.read, &parameters, item)?
            .into_iter()
            .chain(expand_templates(&task.write, &parameters, item)?)
            .collect::<Vec<_>>();
        for id in
            merged_resource_assignments(&manifest.roles[&task.role].resources, &task.resources)
                .keys()
        {
            let resource = &manifest.resources[id];
            if resource.source == PluginResourceSource::Repository {
                roots.push(expand_template(&resource.path, &parameters)?);
            }
        }
        if changed_paths.iter().any(|path| covered(path, &roots)) {
            invalidated.extend(graph.restart_from(&candidate)?);
        }
    }
    // Planning changes require the start task's normal proposal/approval path.
    let decision_key = if invalidated.contains(&TaskKey {
        task: flow.start.clone(),
        work_item: None,
    }) {
        TaskKey {
            task: flow.start.clone(),
            work_item: None,
        }
    } else {
        key.clone()
    };
    saved.completed_keys = graph.completed_keys().cloned().collect();
    for entry in &mut saved.previous {
        if invalidated.iter().any(|key| {
            entry["task"] == key.task && entry["work_item"].as_str() == key.work_item.as_deref()
        }) {
            entry["superseded"] = json!(true);
        }
    }
    saved.previous.push(json!({"event":"pr_revision","superseded_tasks":invalidated,"source_evidence":"Retained in the preceding run; invalidated tests are not current validation."}));
    saved.accumulated_tests.clear();
    saved.tracked_jobs.clear();
    saved.handoffs.clear();
    saved.pending_approvals = if flow.change_intake {
        vec![]
    } else {
        vec![PendingApproval {
            key: decision_key.clone(),
            trigger: ApprovalTrigger::AgentRequested,
        }]
    };
    saved.pending_change = None;
    saved.change_intake_enabled = flow.change_intake;
    saved.revision_targets = if flow.change_intake {
        invalidated.iter().cloned().collect()
    } else {
        vec![]
    };
    saved.revision_options.clear();
    saved.start_action = StartAction::None;
    if flow.change_intake && invalidated.iter().any(|key| key.task == flow.start) {
        saved.start_approved = false;
        saved.start_action = StartAction::Revise;
    }
    saved.flow_fingerprint = Some(flow_fingerprint(flow)?);
    for key in &invalidated {
        if let Some(item) = &key.work_item {
            saved.closed_projected_issues.remove(item);
        }
    }
    write_lifecycle_checkpoint(
        &workspace.join(".donkeyspace/lifecycle-checkpoint.json"),
        &saved,
    )?;
    Ok((
        decision_key.target(),
        invalidated
            .iter()
            .filter_map(|key| key.work_item.as_ref())
            .filter_map(|item| saved.projected_issues.get(item).copied())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn pr_revision_preserves_siblings_and_reopens_planning_approval() {
        let root = std::env::temp_dir().join(format!("pr-revision-{}", Uuid::now_v7()));
        fs::create_dir_all(root.join("repo/docs")).unwrap();
        let manifest = r#"
api_version: 1
id: example.revisions
runtime: { default_image: test }
resources: { reference: { source: repository, path: references } }
roles: { worker: { command: [run] } }
flows:
  change:
    start: plan
    replaces_default_lifecycle: true
    work_items_path: docs/index.json
    tasks:
      plan: { role: worker, approval: required, write: [docs] }
      implement: { role: worker, scope: work_item, dependencies: [plan], write: ["src/{work_item}"] }
      verify: { role: worker, scope: work_item, dependencies: [implement], read: ["src/{work_item}"], resources: [{id: reference}] }
"#;
        fs::write(root.join("plugin.yml"), manifest).unwrap();
        let selection = serde_yaml::from_str::<PluginFlowSelection>(&format!(
            "manifest_path: {}\nflow: change",
            root.join("plugin.yml").display()
        ))
        .unwrap();
        fs::write(
            root.join("repo/docs/index.json"),
            json!({"work_items":[
                {"id":"queue","spec":"docs/queue.md","depends_on":[]},
                {"id":"counter","spec":"docs/counter.md","depends_on":[]}
            ]})
            .to_string(),
        )
        .unwrap();
        let keys = json!([
            {"task":"plan","work_item":null},
            {"task":"implement","work_item":"queue"},{"task":"verify","work_item":"queue"},
            {"task":"implement","work_item":"counter"},{"task":"verify","work_item":"counter"}
        ]);
        let snapshot = json!({
            "version":CHECKPOINT_VERSION,"attempt":3,"accumulated_tests":[],"previous":[],
            "aggregate_risk":"low","aggregate_confidence":"high",
            "last_result": {"outcome":"implemented","summary":"Done","risk":"low","confidence":"high","questions":[],"tests":[],"changed_files":[]},
            "completed_keys":keys,"tracked_jobs":[],"handoffs":[],
            "projected_issues":{"queue":11,"counter":12},"closed_projected_issues":["queue","counter"],
            "resume_target":{"task":"plan","work_item":null},"pending_approvals":[],"start_approved":true,
            "active_work_items":["queue","counter"]
        });
        let read = || -> LifecycleCheckpoint {
            serde_json::from_slice(
                &fs::read(root.join(".donkeyspace/lifecycle-checkpoint.json")).unwrap(),
            )
            .unwrap()
        };
        prepare_pr_revision(
            &selection,
            snapshot.clone(),
            "implement/queue",
            &root.join("repo"),
            &root,
            &[],
        )
        .unwrap();
        let saved = read();
        assert_eq!(saved.completed_keys.len(), 3);
        assert!(
            saved
                .completed_keys
                .iter()
                .all(|key| key.work_item.as_deref() != Some("queue"))
        );
        assert_eq!(saved.projected_issues["queue"], 11);
        assert!(saved.closed_projected_issues.contains("counter"));
        assert!(!saved.closed_projected_issues.contains("queue"));
        assert!(saved.start_approved);
        assert_eq!(saved.pending_approvals[0].key.task, "implement");
        prepare_pr_revision(
            &selection,
            snapshot.clone(),
            "plan",
            &root.join("repo"),
            &root,
            &[],
        )
        .unwrap();
        let saved = read();
        assert!(saved.completed_keys.is_empty());
        let decision = HumanDecision::Revise {
            target: Some("plan".into()),
            feedback: "Change the contract".into(),
        };
        let selected = select_pending_approvals(&saved.pending_approvals, &decision).unwrap();
        assert_eq!(selected.len(), 1);
        assert_eq!(selected[0].key.task, "plan");
        assert_eq!(
            PluginManifest::from_path(&selection.manifest_path)
                .unwrap()
                .flows["change"]
                .tasks["plan"]
                .approval,
            PluginApprovalMode::Required
        );
        prepare_pr_revision(
            &selection,
            snapshot.clone(),
            "implement/queue",
            &root.join("repo"),
            &root,
            &["references/rules.md".into()],
        )
        .unwrap();
        assert_eq!(
            read().completed_keys.len(),
            2,
            "shared resource edits invalidate both verification tasks"
        );
        let (target, _) = prepare_pr_revision(
            &selection,
            snapshot.clone(),
            "implement/queue",
            &root.join("repo"),
            &root,
            &["docs/queue.md".into()],
        )
        .unwrap();
        assert_eq!(target, "plan");
        assert!(read().completed_keys.is_empty());
        prepare_pr_revision(
            &selection,
            snapshot.clone(),
            "implement/queue",
            &root.join("repo"),
            &root,
            &["src/counter/lib.rs".into()],
        )
        .unwrap();
        assert_eq!(read().completed_keys.len(), 1);
        assert!(
            prepare_pr_revision(
                &selection,
                snapshot,
                "implement/unknown",
                &root.join("repo"),
                &root,
                &[],
            )
            .is_err()
        );
        fs::write(
            root.join("plugin.yml"),
            manifest.replace(
                "replaces_default_lifecycle: true",
                "replaces_default_lifecycle: true\n    change_intake: true",
            ),
        )
        .unwrap();
        let snapshot = serde_json::json!({
            "version":CHECKPOINT_VERSION,"attempt":3,"accumulated_tests":[],"previous":[],
            "aggregate_risk":"low","aggregate_confidence":"high",
            "last_result": {"outcome":"implemented","summary":"Done","risk":"low","confidence":"high","questions":[],"tests":[],"changed_files":[]},
            "completed_keys":keys,"tracked_jobs":[],"handoffs":[],
            "projected_issues":{"queue":11,"counter":12},"closed_projected_issues":["queue","counter"],
            "resume_target":{"task":"plan","work_item":null},"pending_approvals":[],"start_approved":true,
            "active_work_items":["queue","counter"]
        });
        prepare_pr_revision(
            &selection,
            snapshot.clone(),
            "",
            &root.join("repo"),
            &root,
            &[],
        )
        .unwrap();
        let retained = read();
        assert_eq!(retained.completed_keys.len(), 5);
        assert!(retained.pending_approvals.is_empty());
        assert!(retained.start_approved);
        assert!(retained.change_intake_enabled);
        prepare_pr_revision(
            &selection,
            snapshot,
            "plan",
            &root.join("repo"),
            &root,
            &["src/queue/file.rs".into()],
        )
        .unwrap();
        let edited = read();
        assert_eq!(edited.completed_keys.len(), 3);
        assert_eq!(edited.revision_targets.len(), 2);
        assert!(edited.start_approved);
        fs::remove_dir_all(root).unwrap();
    }
}

/// Import the new revision into the authoritative checkpoint store before it
/// can be marked resumable. The source run remains immutable.
pub(crate) async fn persist_checkpoint(
    selection: &PluginFlowSelection,
    workspace: &Path,
    input: &Value,
    tracking: &LifecycleTracking<'_>,
) -> Result<(), Box<dyn std::error::Error>> {
    let manifest = PluginManifest::from_path(&selection.manifest_path)?;
    let flow = manifest
        .flows
        .get(&selection.flow)
        .ok_or("revision flow is missing")?;
    let mut store = CheckpointStore::new(workspace, Some(tracking));
    store.load(true, input, flow).await?;
    Ok(())
}
