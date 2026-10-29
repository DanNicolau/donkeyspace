//! Compact summaries with expandable evidence for GitHub conversations.
use donkeyspace_core::{TestResult, TestStatus};

fn fence(text: &str) -> String {
    let longest = text.split(|c| c != '`').map(str::len).max().unwrap_or(0);
    let ticks = "`".repeat(3.max(longest + 1));
    format!("{ticks}text\n{text}\n{ticks}")
}

pub fn tests(tests: &[TestResult]) -> String {
    if tests.is_empty() {
        return "No checks reported.".into();
    }
    let passed = tests
        .iter()
        .filter(|t| t.status == TestStatus::Passed)
        .count();
    let failed = tests
        .iter()
        .filter(|t| t.status == TestStatus::Failed)
        .count();
    let mut lines = vec![
        format!(
            "Recorded checks across attempts: {passed} passed, {failed} failed, {} skipped or not run. Earlier attempts remain in the history.",
            tests.len() - passed - failed
        ),
        String::new(),
        format!(
            "<details><summary>Tests and diagnostics ({} recorded results)</summary>",
            tests.len()
        ),
        String::new(),
    ];
    for test in tests {
        lines.push(format!(
            "**{} — {}**",
            test.name.replace(['\n', '\r'], " "),
            match test.status {
                TestStatus::Passed => "passed",
                TestStatus::Failed => "failed",
                TestStatus::Skipped => "skipped",
                TestStatus::NotRun => "not run",
            }
        ));
        lines.push(fence(&test.command.join(" ")));
        if let Some(summary) = test.summary.as_deref().filter(|s| !s.trim().is_empty()) {
            lines.push(fence(summary));
        }
        lines.push(String::new());
    }
    lines.push("</details>".into());
    lines.join("\n\n")
}

pub fn summary(summary: &str) -> String {
    let (history, plan) = summary
        .split_once("\n\n### Execution plan v")
        .map_or((summary, None), |(history, plan)| {
            (history, Some(format!("### Execution plan v{plan}")))
        });
    let rendered = if history
        .lines()
        .last()
        .is_some_and(|line| line.starts_with("Completed "))
        && history.lines().count() > 1
    {
        let (details, headline) = history.rsplit_once('\n').unwrap();
        format!(
            "{headline}\n\n<details><summary>Task summaries</summary>\n\n{details}\n\n</details>"
        )
    } else {
        history.to_string()
    };
    plan.map_or_else(|| rendered.clone(), |plan| format!("{rendered}\n\n{plan}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn long_multiline_diagnostics_are_collapsed_and_fenced() {
        let body = tests(&[TestResult {
            name: "Verification".into(),
            command: vec!["python".into(), "print('```')\nprint('done')".into()],
            status: TestStatus::Passed,
            exit_code: Some(0),
            summary: Some("Detailed coverage output\n</details>".into()),
        }]);
        assert!(body.starts_with("Recorded checks across attempts: 1 passed, 0 failed"));
        assert!(body.find("<details>").unwrap() < body.find("Detailed coverage output").unwrap());
        assert!(body.contains("````text"));
        assert!(body.ends_with("</details>"));
    }
    #[test]
    fn execution_history_collapses_but_the_final_plan_remains_visible() {
        let body = summary(
            "plan: Proposed design\nbuild: Implemented\nCompleted 1 work item.\n\n### Execution plan v2 — approved\npackage: excluded",
        );
        assert!(body.starts_with("Completed 1 work item."));
        assert!(body.find("</details>").unwrap() < body.find("### Execution plan").unwrap());
        assert!(body.contains("package: excluded"));
    }
}
