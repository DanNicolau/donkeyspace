use super::*;

pub(super) const CHECKPOINT_VERSION: u32 = 5;
#[derive(Debug, Clone, Deserialize, Serialize)]
pub(super) struct TrackedJobCheckpoint {
    pub(super) key: TaskKey,
    pub(super) job_id: Uuid,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub(super) struct HandoffCheckpoint {
    pub(super) work_item: Option<String>,
    pub(super) from: String,
    pub(super) to: String,
    pub(super) count: u32,
}

#[derive(Debug, Default, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(super) enum StartAction {
    #[default]
    None,
    Revise,
    Accept,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub(super) struct LifecycleCheckpoint {
    pub(super) version: u32,
    #[serde(default)]
    repository_snapshot: Option<crate::checkout_recovery::Snapshot>,
    pub(super) attempt: u32,
    pub(super) accumulated_tests: Vec<TestResult>,
    pub(super) previous: Vec<Value>,
    pub(super) aggregate_risk: Risk,
    pub(super) aggregate_confidence: Confidence,
    pub(super) last_result: RunResult,
    pub(super) completed_keys: Vec<TaskKey>,
    pub(super) tracked_jobs: Vec<TrackedJobCheckpoint>,
    pub(super) handoffs: Vec<HandoffCheckpoint>,
    pub(super) projected_issues: BTreeMap<String, i64>,
    pub(super) closed_projected_issues: BTreeSet<String>,
    pub(super) resume_target: TaskKey,
    #[serde(default)]
    pub(super) pending_approvals: Vec<PendingApproval>,
    #[serde(default)]
    pub(super) start_approved: bool,
    #[serde(default)]
    pub(super) start_action: StartAction,
    #[serde(default)]
    pub(super) revision_targets: Vec<TaskKey>,
    #[serde(default)]
    pub(super) active_work_items: Vec<String>,
    #[serde(default)]
    pub(super) revision_options: Vec<donkeyspace_core::RevisionOption>,
    #[serde(default)]
    pub(super) flow_fingerprint: Option<String>,
}

pub(super) fn write_lifecycle_checkpoint(
    path: &Path,
    checkpoint: &LifecycleCheckpoint,
) -> Result<(), Box<dyn std::error::Error>> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let temporary = path.with_extension("json.tmp");
    fs::write(&temporary, serde_json::to_vec_pretty(checkpoint)?)?;
    fs::rename(temporary, path)?;
    Ok(())
}

/// A single snapshot constructor is used by every pause and completed wave.
impl LifecycleCheckpoint {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn capture(
        attempt: u32,
        tests: &[TestResult],
        previous: &[Value],
        risk: Risk,
        confidence: Confidence,
        result: &RunResult,
        graph: &TaskGraph,
        jobs: &BTreeMap<TaskKey, Uuid>,
        handoffs: &BTreeMap<(Option<String>, String, String), u32>,
        projected: &BTreeMap<String, i64>,
        closed: &BTreeSet<String>,
        pending: Vec<PendingApproval>,
        start_approved: bool,
        items: &[PluginWorkItem],
    ) -> Self {
        Self {
            version: CHECKPOINT_VERSION,
            repository_snapshot: None,
            attempt,
            accumulated_tests: tests.to_vec(),
            previous: previous.to_vec(),
            aggregate_risk: risk,
            aggregate_confidence: confidence,
            last_result: result.clone(),
            completed_keys: graph.completed_keys().cloned().collect(),
            tracked_jobs: jobs
                .iter()
                .map(|(key, job_id)| TrackedJobCheckpoint {
                    key: key.clone(),
                    job_id: *job_id,
                })
                .collect(),
            handoffs: handoffs
                .iter()
                .map(|((work_item, from, to), count)| HandoffCheckpoint {
                    work_item: work_item.clone(),
                    from: from.clone(),
                    to: to.clone(),
                    count: *count,
                })
                .collect(),
            projected_issues: projected.clone(),
            closed_projected_issues: closed.clone(),
            resume_target: pending
                .first()
                .map(|approval| approval.key.clone())
                .or_else(|| graph.keys().next().cloned())
                .unwrap_or(TaskKey {
                    work_item: None,
                    task: String::new(),
                }),
            revision_options: graph
                .completed_keys()
                .filter_map(|target| {
                    if pending.iter().any(|approval| &approval.key == target) {
                        return None;
                    }
                    let affected = graph.affected_by(target).ok()?;
                    pending
                        .iter()
                        .any(|approval| affected.contains(&approval.key))
                        .then(|| donkeyspace_core::RevisionOption {
                            target: target.clone(),
                            affected,
                        })
                })
                .collect(),
            flow_fingerprint: None,
            pending_approvals: pending,
            start_approved,
            start_action: StartAction::None,
            revision_targets: Vec::new(),
            active_work_items: items.iter().map(|item| item.id.clone()).collect(),
        }
    }
}

pub(super) struct CheckpointStore<'a, 'b> {
    path: PathBuf,
    tracking: Option<&'a LifecycleTracking<'b>>,
    revision: i64,
    pub(super) effects: donkeyspace_db::lifecycle_checkpoints::Effects,
}

impl<'a, 'b> CheckpointStore<'a, 'b> {
    pub(super) fn new(workspace: &Path, tracking: Option<&'a LifecycleTracking<'b>>) -> Self {
        Self {
            path: workspace.join(".donkeyspace/lifecycle-checkpoint.json"),
            tracking,
            revision: 0,
            effects: Default::default(),
        }
    }

    pub(super) async fn load(
        &mut self,
        resume: bool,
        input: &Value,
        flow: &PluginFlow,
    ) -> Result<Option<LifecycleCheckpoint>, Box<dyn std::error::Error>> {
        if let Some(tracking) = self.tracking
            && let Some(record) =
                donkeyspace_db::lifecycle_checkpoints::load(tracking.pool, tracking.coordinator.id)
                    .await?
        {
            if !resume || record.completed {
                return Err("checkpoint cannot be replayed; inspect the retained execution and start a new run".into());
            }
            if record.version != CHECKPOINT_VERSION as i32 {
                return Err(
                    format!("unsupported database checkpoint version {}", record.version).into(),
                );
            }
            self.revision = record.revision;
            let saved: LifecycleCheckpoint = serde_json::from_value(record.state)?;
            if saved.version != CHECKPOINT_VERSION {
                return Err("checkpoint version mismatch".into());
            }
            return Ok(Some(saved));
        }
        if !resume {
            return Ok(None);
        }
        if !self.path.is_file() {
            return Err(
                "paused lifecycle checkpoint is missing; accepted work cannot be reconstructed"
                    .into(),
            );
        }
        let mut saved = import_legacy_checkpoint(&fs::read_to_string(&self.path)?, input)?;
        saved.flow_fingerprint = Some(flow_fingerprint(flow)?);
        // Import before applying the decision. Preserve the original file as
        // evidence and use only the database for all subsequent reads/writes.
        if self.tracking.is_some() {
            self.save(&saved, flow, false, false).await?;
        }
        Ok(Some(saved))
    }

    pub(super) async fn save(
        &mut self,
        saved: &LifecycleCheckpoint,
        flow: &PluginFlow,
        pause: bool,
        completed: bool,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut saved = saved.clone();
        if saved.flow_fingerprint.is_none() {
            saved.flow_fingerprint = Some(flow_fingerprint(flow)?);
        }
        if let Some(tracking) = self.tracking
            && let Some(publication) = tracking.publication.as_ref()
        {
            let repo = self
                .path
                .parent()
                .and_then(Path::parent)
                .ok_or("checkpoint workspace is missing")?
                .join("repo");
            let branch = crate::repository_default_branch(&tracking.coordinator.input);
            saved.repository_snapshot =
                Some(crate::checkout_recovery::capture(publication, &repo, &branch).await?);
        }
        if let Some(tracking) = self.tracking {
            let existing = donkeyspace_db::list_approval_requests_for_run(
                tracking.pool,
                tracking.coordinator.id,
            )
            .await?;
            self.effects.approvals = approval_requests(
                Some(tracking),
                &saved.pending_approvals,
                &saved.projected_issues,
                flow,
                &saved.last_result,
            )
            .await?
            .into_iter()
            .filter(|new| {
                !existing.iter().any(|old| {
                    old.state == "pending"
                        && old.target_task == new.target_task
                        && old.target_work_item == new.target_work_item
                })
            })
            .collect();
            if pause {
                let result = finish_result(
                    saved.last_result.clone(),
                    saved.accumulated_tests.clone(),
                    &saved.previous,
                );
                self.effects.pause_result = Some(serde_json::to_value(&result)?);
                if let Some(workflow_item_id) = tracking.coordinator.workflow_item_id {
                    self.effects.outbound = donkeyspace_core::triage_github_issue_actions(
                        tracking.policy,
                        &tracking.coordinator.input,
                        &result,
                        crate::workflow_state_for_outcome(result.outcome),
                    )
                    .into_iter()
                    .map(|action| donkeyspace_db::OutboundActionInput {
                        workflow_item_id,
                        job_id: Some(tracking.coordinator.id),
                        provider: "github".into(),
                        action_type: action.action_type,
                        payload: action.payload,
                    })
                    .collect();
                }
            }
            self.revision = donkeyspace_db::lifecycle_checkpoints::save(
                tracking.pool,
                donkeyspace_db::lifecycle_checkpoints::Commit {
                    coordinator: tracking.coordinator,
                    expected_revision: self.revision,
                    version: CHECKPOINT_VERSION as i32,
                    state: &serde_json::to_value(saved)?,
                    completed,
                    effects: &self.effects,
                },
            )
            .await?;
            self.effects = Default::default();
        } else if completed {
            if self.path.exists() {
                fs::remove_file(&self.path)?;
            }
        } else {
            write_lifecycle_checkpoint(&self.path, &saved)?;
        }
        Ok(())
    }
}

/// Compatibility is deliberately confined to this boundary, never the runtime.
fn import_legacy_checkpoint(
    source: &str,
    input: &Value,
) -> Result<LifecycleCheckpoint, Box<dyn std::error::Error>> {
    let mut saved: LifecycleCheckpoint = serde_json::from_str(source)?;
    match saved.version {
        1 => saved.previous.push(json!({
            "human_response": input.pointer("/comment/body").and_then(Value::as_str),
            "human_decision": input.pointer("/donkeyspace_human_decision"),
            "resume_target": saved.resume_target,
        })),
        2..=CHECKPOINT_VERSION => {}
        other => return Err(format!("unsupported lifecycle checkpoint version {other}").into()),
    }
    saved.version = CHECKPOINT_VERSION;
    Ok(saved)
}

/// Mutable execution state has one owner. Decisions operate on task identities;
/// persistence snapshots this state instead of reconstructing it at each exit.
pub(super) struct LifecycleState {
    pub(super) attempt: u32,
    pub(super) accumulated_tests: Vec<TestResult>,
    pub(super) previous: Vec<Value>,
    pub(super) aggregate_risk: Risk,
    pub(super) aggregate_confidence: Confidence,
    pub(super) last_result: RunResult,
    pub(super) graph: TaskGraph,
    pub(super) tracked_jobs: BTreeMap<TaskKey, Uuid>,
    pub(super) finished_jobs: BTreeSet<TaskKey>,
    pub(super) handoffs: BTreeMap<(Option<String>, String, String), u32>,
    pub(super) projected_issues: BTreeMap<String, i64>,
    pub(super) closed_projected_issues: BTreeSet<String>,
    pub(super) work_items: Vec<PluginWorkItem>,
}
impl LifecycleState {
    pub(super) fn snapshot(
        &self,
        result: &RunResult,
        pending: Vec<PendingApproval>,
        start_approved: bool,
    ) -> LifecycleCheckpoint {
        LifecycleCheckpoint::capture(
            self.attempt,
            &self.accumulated_tests,
            &self.previous,
            self.aggregate_risk,
            self.aggregate_confidence,
            result,
            &self.graph,
            &self.tracked_jobs,
            &self.handoffs,
            &self.projected_issues,
            &self.closed_projected_issues,
            pending,
            start_approved,
            &self.work_items,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    pub(super) fn flow() -> PluginFlow {
        serde_json::from_value(
            json!({"start":"architect", "replaces_default_lifecycle":true,
            "work_items_path":"work-items.json", "tasks":{
                "architect":{"role":"architect"}, "rtl":{"role":"rtl"}, "dv":{"role":"dv"}
            }}),
        )
        .unwrap()
    }
    pub(super) fn legacy(version: u32) -> Value {
        json!({"version":version,"attempt":3,"accumulated_tests":[],"previous":[],
            "aggregate_risk":"low","aggregate_confidence":"high",
            "last_result":{"outcome":"needs_human","summary":"Review output","confidence":"high","risk":"low",
                "questions":[],"tests":[],"changed_files":[],"human_review_reason":"Review output","blocked_reason":null},
            "completed_keys":[{"task":"rtl","work_item":"sibling"}],"tracked_jobs":[],"handoffs":[],
            "projected_issues":{"target":12,"sibling":13},"closed_projected_issues":[],
            "resume_target":{"task":"rtl","work_item":"target"}})
    }
    #[test]
    fn imports_supported_versions_at_one_boundary_and_rejects_future_versions() {
        for version in 1..=4 {
            let saved = import_legacy_checkpoint(
                &legacy(version).to_string(),
                &json!({"comment":{"body":"continue"}}),
            )
            .unwrap();
            assert_eq!(saved.version, CHECKPOINT_VERSION);
            assert_eq!(saved.completed_keys[0].target(), "rtl/sibling");
            assert_eq!(saved.previous.len(), usize::from(version == 1));
        }
        assert!(import_legacy_checkpoint(&legacy(99).to_string(), &json!({})).is_err());
    }
    #[test]
    fn targeted_revision_preserves_siblings_and_ambiguous_command_changes_nothing() {
        let mut saved = import_legacy_checkpoint(&legacy(4).to_string(), &json!({})).unwrap();
        saved.pending_approvals = vec![
            PendingApproval {
                key: TaskKey {
                    task: "rtl".into(),
                    work_item: Some("target".into()),
                },
                trigger: ApprovalTrigger::Required,
            },
            PendingApproval {
                key: TaskKey {
                    task: "dv".into(),
                    work_item: Some("sibling".into()),
                },
                trigger: ApprovalTrigger::Required,
            },
        ];
        let before = serde_json::to_value(&saved).unwrap();
        assert!(apply_human_decision(&mut saved, &flow(), &json!({"action":"approve"})).is_err());
        assert_eq!(serde_json::to_value(&saved).unwrap(), before);
        let decision = apply_human_decision(
            &mut saved,
            &flow(),
            &json!({"action":"revise","target":"rtl/target","feedback":"fix timing"}),
        )
        .unwrap();
        assert_eq!(decision.decisions.len(), 1);
        assert_eq!(saved.revision_targets[0].target(), "rtl/target");
        assert_eq!(saved.completed_keys[0].target(), "rtl/sibling");
        assert_eq!(saved.pending_approvals[0].key.target(), "dv/sibling");
    }
    #[test]
    fn planner_decision_survives_another_pending_approval() {
        let mut saved = import_legacy_checkpoint(&legacy(4).to_string(), &json!({})).unwrap();
        saved.pending_approvals = vec![
            PendingApproval {
                key: TaskKey {
                    task: "architect".into(),
                    work_item: None,
                },
                trigger: ApprovalTrigger::Required,
            },
            PendingApproval {
                key: TaskKey {
                    task: "rtl".into(),
                    work_item: Some("target".into()),
                },
                trigger: ApprovalTrigger::Required,
            },
        ];
        apply_human_decision(
            &mut saved,
            &flow(),
            &json!({"action":"revise","target":"architect","feedback":"change plan"}),
        )
        .unwrap();
        assert_eq!(saved.start_action, StartAction::Revise);
        let mut saved: LifecycleCheckpoint =
            serde_json::from_value(serde_json::to_value(saved).unwrap()).unwrap();
        apply_human_decision(
            &mut saved,
            &flow(),
            &json!({"action":"approve","target":"rtl/target"}),
        )
        .unwrap();
        assert_eq!(saved.start_action, StartAction::Revise);
        assert!(saved.pending_approvals.is_empty());
    }
    #[tokio::test]
    #[ignore = "requires isolated DONKEYSPACE_CANCELLATION_TEST_DATABASE_URL"]
    async fn database_checkpoint_import_preserves_file_and_ignores_later_file_changes() {
        let url = env::var("DONKEYSPACE_CANCELLATION_TEST_DATABASE_URL").unwrap();
        assert!(url.ends_with("/donkeyspace_cancellation_test"));
        let pool = donkeyspace_db::connect(&donkeyspace_db::DbConfig::from_database_url(url))
            .await
            .unwrap();
        donkeyspace_db::apply_migrations(&pool).await.unwrap();
        let job = donkeyspace_db::create_job(&pool, None, "developer", &json!({}))
            .await
            .unwrap();
        donkeyspace_db::acquire_job_lease(&pool, job.id, "import-test", 120)
            .await
            .unwrap();
        let job = donkeyspace_db::mark_job_running(&pool, job.id)
            .await
            .unwrap()
            .unwrap();
        let policy = donkeyspace_core::Policy::from_yaml(include_str!(
            "../../../../.donkeyspace/policy.yml"
        ))
        .unwrap();
        let tracking = LifecycleTracking {
            pool: &pool,
            policy: &policy,
            coordinator: &job,
            github: None,
            publication: None,
        };
        let workspace = env::temp_dir().join(format!("checkpoint-import-{}", Uuid::now_v7()));
        fs::create_dir_all(workspace.join(".donkeyspace")).unwrap();
        let path = workspace.join(".donkeyspace/lifecycle-checkpoint.json");
        let original = legacy(4).to_string();
        fs::write(&path, &original).unwrap();
        let mut store = CheckpointStore::new(&workspace, Some(&tracking));
        let saved = store
            .load(true, &json!({}), &flow())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(fs::read_to_string(&path).unwrap(), original);
        fs::write(&path, "invalid obsolete data").unwrap();
        let loaded = CheckpointStore::new(&workspace, Some(&tracking))
            .load(true, &json!({}), &flow())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            serde_json::to_value(loaded).unwrap(),
            serde_json::to_value(&saved).unwrap()
        );
        store.save(&saved, &flow(), false, true).await.unwrap();
        assert!(
            CheckpointStore::new(&workspace, Some(&tracking))
                .load(true, &json!({}), &flow())
                .await
                .is_err()
        );
        fs::remove_dir_all(workspace).unwrap();
    }
}

#[cfg(test)]
#[path = "recovery_tests.rs"]
mod recovery_tests;
