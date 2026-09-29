use std::collections::HashSet;

/// Repository identity shared by ingress and worker maintenance.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RepositoryName {
    pub owner: String,
    pub name: String,
}

impl RepositoryName {
    pub fn full_name(&self) -> String {
        format!("{}/{}", self.owner, self.name)
    }

    pub fn parse_list(value: &str) -> Result<Vec<Self>, String> {
        let mut seen = HashSet::new();
        let mut repositories = Vec::new();
        for entry in value.split(',').map(str::trim).filter(|s| !s.is_empty()) {
            let Some((owner, name)) = entry.split_once('/') else {
                return Err(format!(
                    "invalid github repository `{entry}`; expected owner/name"
                ));
            };
            if owner.is_empty()
                || name.is_empty()
                || name.contains('/')
                || entry.chars().any(char::is_whitespace)
            {
                return Err(format!(
                    "invalid github repository `{entry}`; expected owner/name"
                ));
            }
            if seen.insert(entry.to_ascii_lowercase()) {
                repositories.push(Self {
                    owner: owner.into(),
                    name: name.into(),
                });
            }
        }
        Ok(repositories)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn repository_selection_is_explicit_and_case_insensitive() {
        assert!(RepositoryName::parse_list("").unwrap().is_empty());
        let repos = RepositoryName::parse_list(" Org/One,org/one,org/two,, ").unwrap();
        assert_eq!(
            repos
                .iter()
                .map(RepositoryName::full_name)
                .collect::<Vec<_>>(),
            ["Org/One", "org/two"]
        );
        for invalid in [
            "owner",
            "/repo",
            "owner/",
            "owner/repo/extra",
            "owner /repo",
        ] {
            assert!(RepositoryName::parse_list(invalid).is_err(), "{invalid}");
        }
    }
}
