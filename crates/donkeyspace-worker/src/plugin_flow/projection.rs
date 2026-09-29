use super::*;

#[derive(Clone, Copy, PartialEq)]
pub(super) enum ProjectionPhase {
    Initial,
    Proposed,
    Accepted,
}

/// One reconciliation path for initial plans, revisions and accepted proposals.
/// Accepted work items survive a proposal until the human accepts its removal.
#[allow(clippy::too_many_arguments)]
pub(super) async fn synchronize_work_items(
    tracking: Option<&LifecycleTracking<'_>>,
    flow: &PluginFlow,
    coordinates: (Option<&str>, Option<&str>, Option<i64>),
    repo_path: &Path,
    items: &[PluginWorkItem],
    projected: &mut BTreeMap<String, i64>,
    prior_jobs: &[TrackedJobCheckpoint],
    phase: ProjectionPhase,
) -> Result<(), Box<dyn std::error::Error>> {
    let Some(tracking) = tracking.filter(|_| flow.project_github_issues) else {
        return Ok(());
    };
    let (Some(github), (Some(owner), Some(repo), Some(parent)), Some(workflow)) = (
        tracking.github,
        coordinates,
        tracking.coordinator.workflow_item_id,
    ) else {
        return Ok(());
    };
    crate::cancellation::side_effect(tracking.pool, tracking.coordinator.id, async {
        let publications = list_agent_publications_for_run(
            tracking.pool,
            tracking.coordinator.id,
            Some(tracking.coordinator.id),
        )
        .await?;
        let records =
            list_projected_work_items_for_run(tracking.pool, tracking.coordinator.id).await?;
        let proposed = publications
            .iter()
            .filter(|item| item.kind == "checkpoint" && item.status == "published")
            .max_by_key(|item| item.id);
        if phase == ProjectionPhase::Proposed && proposed.is_none() {
            return Err("architect revision has no published checkpoint".into());
        }
        if phase != ProjectionPhase::Initial {
            let active = items
                .iter()
                .map(|item| item.id.as_str())
                .collect::<BTreeSet<_>>();
            let removed = projected
                .iter()
                .filter(|(id, _)| {
                    !active.contains(id.as_str())
                        && (phase == ProjectionPhase::Accepted
                            || records
                                .iter()
                                .any(|record| record.work_item == **id && !record.accepted))
                })
                .map(|(id, number)| (id.clone(), *number))
                .collect::<Vec<_>>();
            for (id, number) in removed {
                github.close_issue(owner, repo, number).await?;
                projected.remove(&id);
                if phase == ProjectionPhase::Accepted {
                    for child in prior_jobs
                        .iter()
                        .filter(|child| child.key.work_item.as_deref() == Some(id.as_str()))
                    {
                        supersede_job(
                            tracking.pool,
                            child.job_id,
                            "The approved architect checkpoint removed this work item.",
                        )
                        .await?;
                    }
                }
            }
        }
        let accepted = phase == ProjectionPhase::Accepted;
        let desired = items
            .iter()
            .map(|item| {
                let accepted_publication = if accepted {
                    proposed
                } else {
                    records
                        .iter()
                        .find(|record| record.work_item == item.id)
                        .and_then(|record| record.accepted_publication_id)
                        .and_then(|id| publications.iter().find(|publication| publication.id == id))
                };
                Ok(GitHubWorkItem {
                    id: item.id.clone(),
                    spec: item.spec.clone(),
                    body: fs::read_to_string(repo_path.join(&item.spec))?
                        .chars()
                        .take(50_000)
                        .collect(),
                    depends_on: item.depends_on.clone(),
                    proposed_commit: proposed.map(|item| item.commit_sha.clone()),
                    proposed_commit_url: proposed.and_then(|item| item.commit_url.clone()),
                    proposed_compare_url: proposed.and_then(|item| item.compare_url.clone()),
                    accepted_commit: accepted_publication.map(|item| item.commit_sha.clone()),
                    accepted,
                })
            })
            .collect::<Result<Vec<_>, std::io::Error>>()?;
        for item in &desired {
            upsert_projected_work_item(
                tracking.pool,
                &ProjectedWorkItemInput {
                    workflow_item_id: workflow,
                    coordinator_job_id: tracking.coordinator.id,
                    work_item: item.id.clone(),
                    spec_path: item.spec.clone(),
                    body_digest: format!("{:x}", Sha256::digest(item.body.as_bytes())),
                    managed_dependencies: json!(item.depends_on),
                    proposed_publication_id: proposed.map(|item| item.id),
                },
            )
            .await?;
            if phase != ProjectionPhase::Initial
                && let Some(number) = projected.get(&item.id)
            {
                github
                    .update_projected_work_item(owner, repo, parent, *number, item)
                    .await?;
                mark_projected_work_item_applied(
                    tracking.pool,
                    tracking.coordinator.id,
                    &item.id,
                    None,
                    *number,
                    accepted,
                )
                .await?;
            }
        }
        let missing = desired
            .into_iter()
            .filter(|item| !projected.contains_key(&item.id))
            .collect::<Vec<_>>();
        let issues = github
            .project_work_items(owner, repo, parent, &missing)
            .await?;
        for (work_item, issue) in issues {
            record_github_managed_resource_for_workflow_item(
                tracking.pool,
                workflow,
                "issue",
                &issue.id.to_string(),
                &json!({"work_item": work_item, "issue_number": issue.number}),
            )
            .await?;
            mark_projected_work_item_applied(
                tracking.pool,
                tracking.coordinator.id,
                &work_item,
                Some(&issue.id.to_string()),
                issue.number,
                accepted,
            )
            .await?;
            projected.insert(work_item, issue.number);
        }
        Ok::<_, Box<dyn std::error::Error>>(())
    })
    .await
}
