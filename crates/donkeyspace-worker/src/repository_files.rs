//! Copy repository content without importing Git metadata or following links.
use donkeyspace_core::plugin::validate_repository_path;
use std::{fs, path::Path};

type Error = Box<dyn std::error::Error>;

/// Validate before creating a checkout or publication directory. Existing
/// ancestors must be directories, never links supplied by execution code.
pub fn validate_directory(path: &Path) -> Result<(), Error> {
    if path
        .components()
        .any(|component| matches!(component, std::path::Component::ParentDir))
    {
        return Err("repository path contains parent traversal".into());
    }
    let mut current = std::path::PathBuf::new();
    for component in path.components() {
        current.push(component);
        match fs::symlink_metadata(&current) {
            Ok(metadata) if !metadata.is_dir() => {
                return Err(format!(
                    "repository path is not a real directory: {}",
                    current.display()
                )
                .into());
            }
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => break,
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}

pub fn regular_file(path: &Path) -> Result<Option<fs::Metadata>, Error> {
    if let Some(parent) = path.parent() {
        validate_directory(parent)?;
    }
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.is_file() => Ok(Some(metadata)),
        Ok(_) => Err(format!("repository file is not regular: {}", path.display()).into()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error.into()),
    }
}

fn validate_ancestors(repo: &Path, root: &str) -> Result<(), Error> {
    validate_directory(repo)?;
    validate_repository_path(root)?;
    let mut path = repo.to_path_buf();
    for component in Path::new(root).components() {
        path.push(component);
        match fs::symlink_metadata(&path) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                return Err(
                    format!("repository artifact contains a symlink: {}", path.display()).into(),
                );
            }
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => break,
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}

fn validate_tree(path: &Path) -> Result<(), Error> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error.into()),
    };
    if metadata.is_dir() {
        for entry in fs::read_dir(path)? {
            let entry = entry?;
            if entry.file_name().eq_ignore_ascii_case(".git") {
                return Err("repository artifacts cannot contain Git metadata".into());
            }
            validate_tree(&entry.path())?;
        }
    } else if !metadata.is_file() {
        return Err(format!(
            "repository artifact is not a regular file: {}",
            path.display()
        )
        .into());
    }
    Ok(())
}

fn validate_roots(source: &Path, target: &Path, roots: &[String]) -> Result<(), Error> {
    for root in roots {
        validate_ancestors(source, root)?;
        validate_ancestors(target, root)?;
        validate_tree(&source.join(root))?;
        validate_tree(&target.join(root))?;
    }
    Ok(())
}

// Callers terminate execution before copying; all sources/destinations are
// checked before any mutation, so one invalid artifact cannot partially import.
pub fn replace_roots(source: &Path, target: &Path, roots: &[String]) -> Result<(), Error> {
    validate_roots(source, target, roots)?;
    for root in roots {
        let destination = target.join(root);
        if destination.is_dir() {
            fs::remove_dir_all(&destination)?;
        } else if destination.exists() {
            fs::remove_file(&destination)?;
        }
        copy_entry(&source.join(root), &destination)?;
    }
    Ok(())
}

pub fn copy_root(source: &Path, target: &Path, root: &str) -> Result<(), Error> {
    validate_roots(source, target, &[root.to_string()])?;
    copy_entry(&source.join(root), &target.join(root))
}

fn copy_entry(source: &Path, target: &Path) -> Result<(), Error> {
    if source.is_dir() {
        fs::create_dir_all(target)?;
        for entry in fs::read_dir(source)? {
            let entry = entry?;
            copy_entry(&entry.path(), &target.join(entry.file_name()))?;
        }
    } else if source.is_file() {
        if let Some(parent) = target.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::copy(source, target)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metadata_and_links_are_rejected_before_any_output_is_replaced() {
        let root = std::env::temp_dir().join(format!("ds-artifacts-{}", uuid::Uuid::now_v7()));
        struct Cleanup(std::path::PathBuf);
        impl Drop for Cleanup {
            fn drop(&mut self) {
                let _ = fs::remove_dir_all(&self.0);
            }
        }
        let _cleanup = Cleanup(root.clone());
        let source = root.join("source");
        let target = root.join("target");
        fs::create_dir_all(source.join("output/nested/.git")).unwrap();
        fs::create_dir_all(&target).unwrap();
        fs::write(source.join("good"), "new").unwrap();
        fs::write(target.join("good"), "accepted").unwrap();
        let roots = ["good".to_string(), "output".to_string()];
        assert!(replace_roots(&source, &target, &roots).is_err());
        assert_eq!(fs::read_to_string(target.join("good")).unwrap(), "accepted");
        assert!(validate_directory(&source.join("absent/../escape")).is_err());
        assert!(copy_root(&source, &target, ".").is_err());
        assert!(copy_root(&source, &target, "output/nested/.git").is_err());
        fs::remove_dir_all(source.join("output/nested/.git")).unwrap();
        #[cfg(unix)]
        {
            std::os::unix::fs::symlink(&target, source.join("output/link")).unwrap();
            assert!(replace_roots(&source, &target, &roots).is_err());
            assert_eq!(fs::read_to_string(target.join("good")).unwrap(), "accepted");
            fs::remove_file(source.join("output/link")).unwrap();
            std::os::unix::fs::symlink(&source, target.join("output")).unwrap();
            assert!(replace_roots(&source, &target, &roots).is_err());
            fs::remove_file(target.join("output")).unwrap();
        }
        replace_roots(&source, &target, &roots).unwrap();
        assert_eq!(fs::read_to_string(target.join("good")).unwrap(), "new");
        fs::remove_file(source.join("good")).unwrap();
        replace_roots(&source, &target, &["good".into()]).unwrap();
        assert!(!target.join("good").exists());
    }
}
