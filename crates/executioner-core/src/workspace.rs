use crate::error::{ExecutionerError, Result};
use crate::protocol::{ExecutionPolicy, Session};
use std::path::{Component, Path, PathBuf};

#[derive(Debug, Clone)]
pub struct WorkspaceResolver {
    root: PathBuf,
    policy: ExecutionPolicy,
}

#[derive(Debug, Clone)]
pub struct ResolvedPath {
    pub host_path: PathBuf,
    pub logical_path: String,
}

#[derive(Debug, Clone, Copy)]
pub enum AccessKind {
    Read,
    Write,
}

impl WorkspaceResolver {
    pub fn for_session(session: &Session) -> Result<Self> {
        let root = PathBuf::from(&session.workspace.root);
        ensure_workspace_root_is_still_safe(&root)?;
        Ok(Self {
            root: root.canonicalize()?,
            policy: session.policy.clone(),
        })
    }

    pub fn resolve_existing(
        &self,
        cwd: Option<&str>,
        requested: &str,
        access: AccessKind,
    ) -> Result<ResolvedPath> {
        let host_path = self.logical_to_host(cwd, requested)?;
        let canonical = host_path.canonicalize()?;
        self.ensure_under_workspace(&canonical)?;
        let logical_path = self.host_to_logical(&canonical)?;
        self.ensure_policy(access, &logical_path)?;
        Ok(ResolvedPath {
            host_path: canonical,
            logical_path,
        })
    }

    pub fn resolve_read_target(&self, cwd: Option<&str>, requested: &str) -> Result<ResolvedPath> {
        let host_path = self.logical_to_host(cwd, requested)?;
        self.ensure_under_workspace(&host_path)?;
        if host_path.exists() {
            return self.resolve_existing(cwd, requested, AccessKind::Read);
        }
        let logical_path = self.host_to_logical_unchecked(&host_path)?;
        self.ensure_policy(AccessKind::Read, &logical_path)?;
        Ok(ResolvedPath {
            host_path,
            logical_path,
        })
    }

    pub fn resolve_write_target(&self, cwd: Option<&str>, requested: &str) -> Result<ResolvedPath> {
        let host_path = self.logical_to_host(cwd, requested)?;
        let parent = host_path.parent().ok_or_else(|| {
            ExecutionerError::InvalidRequest("write target has no parent".to_string())
        })?;
        let existing_parent = nearest_existing_parent(parent)?;
        let canonical_parent = existing_parent.canonicalize()?;
        self.ensure_under_workspace(&canonical_parent)?;
        let relative_tail = parent
            .strip_prefix(&existing_parent)
            .unwrap_or_else(|_| Path::new(""));
        let normalized_parent = canonical_parent.join(relative_tail);
        self.ensure_under_workspace(&normalized_parent)?;
        let final_path = normalized_parent.join(host_path.file_name().ok_or_else(|| {
            ExecutionerError::InvalidRequest("write target has no file name".to_string())
        })?);
        let logical_path = self.host_to_logical_unchecked(&final_path)?;
        self.ensure_policy(AccessKind::Write, &logical_path)?;
        Ok(ResolvedPath {
            host_path: final_path,
            logical_path,
        })
    }

    pub fn resolve_cwd(&self, cwd: Option<&str>) -> Result<ResolvedPath> {
        let host_path = match cwd {
            Some(cwd) if !cwd.trim().is_empty() => self.path_from_logical(cwd)?,
            _ => self.root.clone(),
        };
        let canonical = host_path.canonicalize()?;
        self.ensure_under_workspace(&canonical)?;
        let logical_path = self.host_to_logical(&canonical)?;
        Ok(ResolvedPath {
            host_path: canonical,
            logical_path,
        })
    }

    pub fn resolve_readable_cwd(&self, cwd: Option<&str>) -> Result<ResolvedPath> {
        let resolved = self.resolve_cwd(cwd)?;
        self.ensure_policy(AccessKind::Read, &resolved.logical_path)?;
        Ok(resolved)
    }

    pub fn resolve_writable_cwd(&self, cwd: Option<&str>) -> Result<ResolvedPath> {
        let resolved = self.resolve_cwd(cwd)?;
        self.ensure_policy(AccessKind::Write, &resolved.logical_path)?;
        Ok(resolved)
    }

    pub fn ensure_existing_parent_for_write(&self, path: &Path) -> Result<()> {
        let parent = path.parent().ok_or_else(|| {
            ExecutionerError::InvalidRequest("write target has no parent".to_string())
        })?;
        let canonical_parent = parent.canonicalize()?;
        self.ensure_under_workspace(&canonical_parent)?;
        let logical_parent = self.host_to_logical(&canonical_parent)?;
        self.ensure_policy(AccessKind::Write, &logical_parent)
    }

    pub fn ensure_parent_allowed_for_write(&self, path: &Path) -> Result<()> {
        let parent = path.parent().ok_or_else(|| {
            ExecutionerError::InvalidRequest("write target has no parent".to_string())
        })?;
        self.ensure_under_workspace(parent)?;
        let logical_parent = self.host_to_logical_unchecked(parent)?;
        self.ensure_policy(AccessKind::Write, &logical_parent)
    }

    fn logical_to_host(&self, cwd: Option<&str>, requested: &str) -> Result<PathBuf> {
        validate_logical_path_text(requested)?;
        if requested.trim().is_empty() {
            return Err(ExecutionerError::InvalidRequest(
                "path is required".to_string(),
            ));
        }

        let base = match cwd {
            Some(cwd) if !cwd.trim().is_empty() => {
                validate_logical_path_text(cwd)?;
                self.path_from_logical(cwd)?
            }
            _ => self.root.clone(),
        };

        let requested_path = Path::new(requested);
        let joined = if requested.starts_with("/workspace") {
            self.path_from_logical(requested)?
        } else if requested_path.is_absolute() {
            return Err(ExecutionerError::PolicyDenied(
                "absolute host paths are not accepted; use /workspace logical paths".to_string(),
            ));
        } else {
            base.join(requested_path)
        };

        normalize_lexically(&joined)
    }

    fn path_from_logical(&self, logical: &str) -> Result<PathBuf> {
        validate_logical_path_text(logical)?;
        if logical == "/workspace" {
            return Ok(self.root.clone());
        }
        let suffix = logical.strip_prefix("/workspace/").ok_or_else(|| {
            ExecutionerError::PolicyDenied(format!("path escapes /workspace: {logical}"))
        })?;
        normalize_lexically(&self.root.join(suffix))
    }

    fn host_to_logical(&self, host_path: &Path) -> Result<String> {
        self.ensure_under_workspace(host_path)?;
        self.host_to_logical_unchecked(host_path)
    }

    fn host_to_logical_unchecked(&self, host_path: &Path) -> Result<String> {
        let rel = host_path
            .strip_prefix(&self.root)
            .map_err(|_| ExecutionerError::PolicyDenied("path escapes workspace".to_string()))?;
        if rel.as_os_str().is_empty() {
            return Ok("/workspace".to_string());
        }
        Ok(format!(
            "/workspace/{}",
            rel.to_string_lossy().replace('\\', "/")
        ))
    }

    fn ensure_under_workspace(&self, path: &Path) -> Result<()> {
        if path.starts_with(&self.root) {
            Ok(())
        } else {
            Err(ExecutionerError::PolicyDenied(format!(
                "path escapes workspace: {}",
                path.display()
            )))
        }
    }

    fn ensure_policy(&self, access: AccessKind, logical_path: &str) -> Result<()> {
        let roots = match access {
            AccessKind::Read => &self.policy.read_roots,
            AccessKind::Write => &self.policy.write_roots,
        };

        if roots
            .iter()
            .any(|root| logical_is_under(logical_path, root))
        {
            Ok(())
        } else {
            Err(ExecutionerError::PolicyDenied(format!(
                "{access:?} denied for {logical_path}"
            )))
        }
    }
}

fn validate_logical_path_text(path: &str) -> Result<()> {
    if path.contains('\0') {
        return Err(ExecutionerError::InvalidRequest(
            "path contains invalid NUL byte".to_string(),
        ));
    }
    Ok(())
}

pub(crate) fn validate_policy_roots(label: &str, roots: &[String]) -> Result<()> {
    for root in roots {
        validate_policy_root(label, root)?;
    }
    Ok(())
}

fn validate_policy_root(label: &str, root: &str) -> Result<()> {
    validate_logical_path_text(root)?;
    let trimmed = root.trim_end_matches('/');
    if trimmed.is_empty()
        || !(trimmed == "/workspace" || trimmed.starts_with("/workspace/"))
        || trimmed
            .split('/')
            .any(|component| component == "." || component == "..")
    {
        return Err(ExecutionerError::InvalidRequest(format!(
            "{label} entries must be /workspace logical roots without . or .. components"
        )));
    }
    Ok(())
}

pub(crate) fn ensure_workspace_root_is_still_safe(root: &Path) -> Result<()> {
    let parent = root.parent().unwrap_or_else(|| Path::new("."));
    ensure_path_parent_has_no_symlinks(parent, "workspace.root parent")?;
    let metadata = root.symlink_metadata().map_err(ExecutionerError::Io)?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(ExecutionerError::InvalidRequest(
            "workspace.root must be a real directory".to_string(),
        ));
    }
    Ok(())
}

fn ensure_path_parent_has_no_symlinks(parent: &Path, label: &str) -> Result<()> {
    let mut current = if parent.is_absolute() {
        parent.to_path_buf()
    } else {
        std::env::current_dir()?.join(parent)
    };
    loop {
        match current.symlink_metadata() {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                if is_platform_root_symlink(&current) {
                    if !current.pop() {
                        return Ok(());
                    }
                    continue;
                }
                return Err(ExecutionerError::InvalidRequest(format!(
                    "{label} must not contain symlinks"
                )));
            }
            Ok(_) => {}
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => return Err(err.into()),
        }
        if !current.pop() {
            return Ok(());
        }
    }
}

fn is_platform_root_symlink(path: &Path) -> bool {
    matches!(path.to_str(), Some("/var" | "/tmp" | "/etc"))
}

fn nearest_existing_parent(path: &Path) -> Result<PathBuf> {
    let mut current = path.to_path_buf();
    loop {
        if current.exists() {
            return Ok(current);
        }
        if !current.pop() {
            return Err(ExecutionerError::InvalidRequest(format!(
                "no existing parent for {}",
                path.display()
            )));
        }
    }
}

fn logical_is_under(path: &str, root: &str) -> bool {
    let Some(root) = normalize_policy_root(root) else {
        return false;
    };
    path == root || path.starts_with(&format!("{root}/"))
}

fn normalize_policy_root(root: &str) -> Option<&str> {
    let root = root.trim_end_matches('/');
    if root == "/workspace" || root.starts_with("/workspace/") {
        Some(root)
    } else {
        None
    }
}

fn normalize_lexically(path: &Path) -> Result<PathBuf> {
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::Prefix(prefix) => normalized.push(prefix.as_os_str()),
            Component::RootDir => normalized.push(component.as_os_str()),
            Component::CurDir => {}
            Component::ParentDir => {
                if !normalized.pop() {
                    return Err(ExecutionerError::PolicyDenied(
                        "path traversal escapes workspace".to_string(),
                    ));
                }
            }
            Component::Normal(part) => normalized.push(part),
        }
    }
    Ok(normalized)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::{
        ExecutionPolicy, NetworkPolicy, ProcessPolicy, Session, SessionState, WorkspaceBinding,
        WorkspaceMode,
    };
    use serde_json::Map;
    use tempfile::TempDir;

    fn session(root: &Path) -> Session {
        Session {
            id: "sess".to_string(),
            environment_id: "env".to_string(),
            state: SessionState::Ready,
            workspace: WorkspaceBinding {
                root: root.to_string_lossy().into_owned(),
                logical_root: "/workspace".to_string(),
                mode: WorkspaceMode::New,
                fresh: true,
                managed: true,
            },
            policy: ExecutionPolicy {
                read_roots: vec!["/workspace".to_string()],
                write_roots: vec!["/workspace".to_string()],
                process: ProcessPolicy {
                    allow_exec: false,
                    allowed_commands: vec![],
                    denied_commands: vec![],
                    max_processes: None,
                },
                network: NetworkPolicy {
                    enabled: false,
                    allow_hosts: vec![],
                    deny_hosts: vec![],
                },
                ..ExecutionPolicy::default()
            },
            metadata: Map::new(),
            created_at: "now".to_string(),
            expires_at: None,
        }
    }

    #[test]
    fn denies_absolute_host_paths() {
        let temp = TempDir::new().unwrap();
        let resolver = WorkspaceResolver::for_session(&session(temp.path())).unwrap();
        let result = resolver.resolve_write_target(None, "/tmp/outside.txt");
        assert!(matches!(result, Err(ExecutionerError::PolicyDenied(_))));
    }

    #[test]
    fn resolves_workspace_paths() {
        let temp = TempDir::new().unwrap();
        let resolver = WorkspaceResolver::for_session(&session(temp.path())).unwrap();
        let resolved = resolver
            .resolve_write_target(Some("/workspace/sub"), "../file.txt")
            .unwrap();
        assert_eq!(resolved.logical_path, "/workspace/file.txt");
    }
}
