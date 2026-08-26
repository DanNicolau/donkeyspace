use donkeyspace_core::{PluginFlow, PluginTaskScope, PluginWorkItem};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct TaskKey {
    pub work_item: Option<String>,
    pub task: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskState {
    Waiting,
    Running,
    Completed,
}

#[derive(Debug)]
pub struct TaskGraph {
    states: BTreeMap<TaskKey, TaskState>,
    dependencies: BTreeMap<TaskKey, BTreeSet<TaskKey>>,
}

impl TaskGraph {
    pub fn for_work_items(flow: &PluginFlow, work_items: &[PluginWorkItem]) -> Self {
        let mut states = BTreeMap::new();
        let mut dependencies = BTreeMap::new();

        for (task_name, task) in &flow.tasks {
            let item_ids = match task.scope {
                PluginTaskScope::Workflow => vec![None],
                PluginTaskScope::WorkItem => work_items
                    .iter()
                    .map(|item| Some(item.id.clone()))
                    .collect(),
            };
            for work_item in item_ids {
                let key = TaskKey {
                    work_item: work_item.clone(),
                    task: task_name.clone(),
                };
                states.insert(
                    key.clone(),
                    if task_name == &flow.start {
                        TaskState::Completed
                    } else {
                        TaskState::Waiting
                    },
                );
                let mut required = task
                    .dependencies
                    .iter()
                    .map(|dependency| TaskKey {
                        work_item: match flow.tasks[dependency].scope {
                            PluginTaskScope::Workflow => None,
                            PluginTaskScope::WorkItem => work_item.clone(),
                        },
                        task: dependency.clone(),
                    })
                    .collect::<BTreeSet<_>>();
                if task.depends_on_work_items
                    && let Some(item_id) = &work_item
                    && let Some(item) = work_items.iter().find(|item| &item.id == item_id)
                {
                    let active_items = work_items
                        .iter()
                        .map(|item| item.id.as_str())
                        .collect::<BTreeSet<_>>();
                    required.extend(item.depends_on.iter().filter_map(|dependency| {
                        active_items.contains(dependency.as_str()).then(|| TaskKey {
                            work_item: Some(dependency.clone()),
                            task: task_name.clone(),
                        })
                    }));
                }
                dependencies.insert(key, required);
            }
        }

        Self {
            states,
            dependencies,
        }
    }

    pub fn ready(&self) -> Result<Vec<TaskKey>, String> {
        self.states
            .iter()
            .filter_map(|(key, state)| {
                let dependencies = match self.dependencies.get(key) {
                    Some(dependencies) => dependencies,
                    None => {
                        return Some(Err(format!(
                            "task graph is missing dependencies for `{key:?}`"
                        )));
                    }
                };
                (*state == TaskState::Waiting
                    && dependencies.iter().all(|dependency| {
                        self.states.get(dependency) == Some(&TaskState::Completed)
                    }))
                .then(|| Ok(key.clone()))
            })
            .collect()
    }

    pub fn keys(&self) -> impl Iterator<Item = &TaskKey> {
        self.states.keys()
    }

    pub fn mark_running(&mut self, key: &TaskKey) -> Result<(), String> {
        self.set_state(key, TaskState::Running)
    }

    pub fn mark_completed(&mut self, key: &TaskKey) -> Result<(), String> {
        self.set_state(key, TaskState::Completed)
    }

    pub fn restore_completed<'a>(
        &mut self,
        keys: impl IntoIterator<Item = &'a TaskKey>,
    ) -> Result<(), String> {
        for key in keys {
            self.set_state(key, TaskState::Completed)?;
        }
        Ok(())
    }

    pub fn completed_keys(&self) -> impl Iterator<Item = &TaskKey> {
        self.states
            .iter()
            .filter_map(|(key, state)| (*state == TaskState::Completed).then_some(key))
    }

    pub fn is_completed(&self, key: &TaskKey) -> bool {
        self.states.get(key) == Some(&TaskState::Completed)
    }

    pub fn restart_from(&mut self, key: &TaskKey) -> Result<Vec<TaskKey>, String> {
        if !self.states.contains_key(key) {
            return Err(format!("task graph has no task `{key:?}`"));
        }
        let mut invalidated = BTreeSet::from([key.clone()]);
        loop {
            let before = invalidated.len();
            for (candidate, dependencies) in &self.dependencies {
                if dependencies
                    .iter()
                    .any(|dependency| invalidated.contains(dependency))
                {
                    invalidated.insert(candidate.clone());
                }
            }
            if invalidated.len() == before {
                break;
            }
        }
        for invalidated_key in &invalidated {
            self.set_state(invalidated_key, TaskState::Waiting)?;
        }
        Ok(invalidated.into_iter().collect())
    }

    pub fn is_complete(&self) -> bool {
        self.states
            .values()
            .all(|state| *state == TaskState::Completed)
    }

    pub fn work_item_is_complete(&self, work_item: &str) -> bool {
        self.states.iter().all(|(key, state)| {
            key.work_item.as_deref() != Some(work_item) || *state == TaskState::Completed
        })
    }

    fn set_state(&mut self, key: &TaskKey, state: TaskState) -> Result<(), String> {
        let existing = self
            .states
            .get_mut(key)
            .ok_or_else(|| format!("task graph has no task `{key:?}`"))?;
        *existing = state;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use donkeyspace_core::PluginManifest;

    fn flow() -> PluginFlow {
        let manifest: PluginManifest = serde_yaml::from_str(
            r#"
api_version: 1
id: test.rtl
runtime: { default_image: test:dev }
roles:
  architect: { command: [run, architect] }
  rtl: { command: [run, rtl] }
  dv: { command: [run, dv] }
  syn: { command: [run, syn] }
flows:
  blocks:
    start: architect
    replaces_default_lifecycle: true
    work_items_path: docs/design/blocks/index.json
    tasks:
      architect: { role: architect }
      rtl: { role: rtl, scope: work_item, depends_on_work_items: true }
      dv_prepare: { role: dv, scope: work_item }
      dv_verify:
        role: dv
        scope: work_item
        dependencies: [rtl, dv_prepare]
      synthesis:
        role: syn
        scope: work_item
        dependencies: [rtl]
"#,
        )
        .unwrap();
        manifest.flows["blocks"].clone()
    }

    #[test]
    fn starts_rtl_and_dv_for_independent_leaf_blocks_in_parallel() {
        let flow = flow();
        let items = vec![
            PluginWorkItem {
                id: "leaf".into(),
                spec: "leaf.md".into(),
                depends_on: vec![],
                metadata: BTreeMap::new(),
            },
            PluginWorkItem {
                id: "top".into(),
                spec: "top.md".into(),
                depends_on: vec!["leaf".into()],
                metadata: BTreeMap::new(),
            },
        ];
        let mut graph = TaskGraph::for_work_items(&flow, &items);
        let ready = graph.ready().unwrap();
        assert!(ready.contains(&TaskKey {
            work_item: Some("leaf".into()),
            task: "rtl".into()
        }));
        assert!(!ready.contains(&TaskKey {
            work_item: Some("top".into()),
            task: "rtl".into()
        }));
        assert!(ready.iter().filter(|key| key.task == "dv_prepare").count() == 2);

        let leaf_rtl = TaskKey {
            work_item: Some("leaf".into()),
            task: "rtl".into(),
        };
        graph.mark_completed(&leaf_rtl).unwrap();
        assert!(graph.ready().unwrap().contains(&TaskKey {
            work_item: Some("top".into()),
            task: "rtl".into()
        }));
    }

    #[test]
    fn rtl_feedback_invalidates_verification_and_synthesis() {
        let flow = flow();
        let items = vec![PluginWorkItem {
            id: "block".into(),
            spec: "block.md".into(),
            depends_on: vec![],
            metadata: BTreeMap::new(),
        }];
        let mut graph = TaskGraph::for_work_items(&flow, &items);
        for key in graph.states.keys().cloned().collect::<Vec<_>>() {
            graph.mark_completed(&key).unwrap();
        }
        graph
            .restart_from(&TaskKey {
                work_item: Some("block".into()),
                task: "rtl".into(),
            })
            .unwrap();
        assert_eq!(
            graph.ready().unwrap(),
            vec![TaskKey {
                work_item: Some("block".into()),
                task: "rtl".into()
            }]
        );
        assert!(!graph.is_complete());
    }

    #[test]
    fn targeted_restart_preserves_completed_parallel_sibling() {
        let flow = flow();
        let items = vec![
            PluginWorkItem {
                id: "left".into(),
                spec: "left.md".into(),
                depends_on: vec![],
                metadata: BTreeMap::new(),
            },
            PluginWorkItem {
                id: "right".into(),
                spec: "right.md".into(),
                depends_on: vec![],
                metadata: BTreeMap::new(),
            },
        ];
        let mut graph = TaskGraph::for_work_items(&flow, &items);
        for key in graph.keys().cloned().collect::<Vec<_>>() {
            graph.mark_completed(&key).unwrap();
        }

        graph
            .restart_from(&TaskKey {
                work_item: Some("left".into()),
                task: "rtl".into(),
            })
            .unwrap();

        assert!(graph.is_completed(&TaskKey {
            work_item: Some("right".into()),
            task: "rtl".into(),
        }));
        assert!(graph.is_completed(&TaskKey {
            work_item: Some("right".into()),
            task: "dv_verify".into(),
        }));
        assert!(!graph.is_completed(&TaskKey {
            work_item: Some("left".into()),
            task: "rtl".into(),
        }));
        assert!(!graph.is_completed(&TaskKey {
            work_item: Some("left".into()),
            task: "synthesis".into(),
        }));
    }

    #[test]
    fn rejects_unknown_checkpoint_and_restart_keys() {
        let flow = flow();
        let mut graph = TaskGraph::for_work_items(&flow, &[]);
        let unknown = TaskKey {
            work_item: Some("missing".into()),
            task: "rtl".into(),
        };

        assert!(graph.restore_completed([&unknown]).is_err());
        assert!(graph.restart_from(&unknown).is_err());
    }

    #[test]
    fn catalog_dependencies_outside_the_lifecycle_are_already_satisfied() {
        let flow = flow();
        let items = vec![PluginWorkItem {
            id: "top".into(),
            spec: "top.md".into(),
            depends_on: vec!["existing_leaf".into()],
            metadata: BTreeMap::new(),
        }];
        let graph = TaskGraph::for_work_items(&flow, &items);

        assert!(graph.ready().unwrap().contains(&TaskKey {
            work_item: Some("top".into()),
            task: "rtl".into(),
        }));
    }
}
