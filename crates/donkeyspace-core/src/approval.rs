use crate::Facade;
use serde::{Deserialize, Serialize};

/// Stable identity shared by scheduling, checkpoints, and approval surfaces.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct TaskKey {
    pub work_item: Option<String>,
    pub task: String,
}

pub struct ApprovalCommands {
    pub approve: String,
    pub revise: String,
}

/// A completed task that can be reopened, and the exact results it supersedes.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct RevisionOption {
    pub target: TaskKey,
    pub affected: Vec<TaskKey>,
}

#[derive(Debug, Serialize)]
pub struct RevisionPreview {
    pub target: String,
    pub affected: Vec<String>,
    pub revise_command: String,
}

/// Both lifecycle surfaces describe the same persisted invalidation scope.
pub fn revision_previews(checkpoint: &serde_json::Value, facade: &Facade) -> Vec<RevisionPreview> {
    let options: Vec<RevisionOption> = checkpoint
        .get("revision_options")
        .and_then(|value| serde_json::from_value(value.clone()).ok())
        .unwrap_or_default();
    options
        .into_iter()
        .map(|option| RevisionPreview {
            target: option.target.target(),
            affected: option.affected.iter().map(TaskKey::target).collect(),
            revise_command: option.target.approval_commands(facade).revise,
        })
        .collect()
}

impl TaskKey {
    pub fn target(&self) -> String {
        self.work_item
            .as_ref()
            .map_or_else(|| self.task.clone(), |item| format!("{}/{item}", self.task))
    }

    pub fn approval_commands(&self, facade: &Facade) -> ApprovalCommands {
        let prefix = facade.issue_command();
        let target = self.target();
        ApprovalCommands {
            approve: format!("{prefix} approve {target}"),
            revise: format!("{prefix} revise {target}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::FacadeConfig;

    #[test]
    fn parallel_targets_have_distinct_commands_with_the_effective_prefix() {
        let facade = FacadeConfig {
            command: Some("example".into()),
            ..Default::default()
        }
        .resolve();
        for item in [None, Some("fifo"), Some("cache")] {
            let key = TaskKey {
                task: "rtl".into(),
                work_item: item.map(Into::into),
            };
            let commands = key.approval_commands(&facade);
            assert_eq!(
                commands.approve,
                format!("/example approve {}", key.target())
            );
            assert_eq!(commands.revise, format!("/example revise {}", key.target()));
        }
    }
}
