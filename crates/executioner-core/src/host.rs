use crate::effects::now_string;
use crate::error::{ExecutionerError, Result};
use crate::protocol::{
    CreateSessionRequest, CreateSessionResponse, Session, SessionState, ToolInvocationRequest,
    ToolInvocationResult, ToolResultStatus, WorkspaceArtifact, WorkspaceBinding, WorkspaceMode,
    MAX_OUTPUT_BYTES, MAX_REQUEST_JSON_BYTES, MAX_SESSION_TTL_MS, MAX_TOOL_TIMEOUT_MS,
};
use crate::tools::{bash, edit_file, glob_files, grep_files, list_files, read_file, write_file};
use crate::workspace::validate_policy_roots;
use serde_json::Map;
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct HostState {
    inner: Arc<Mutex<HostInner>>,
}

#[derive(Debug)]
struct HostInner {
    base_dir: PathBuf,
    sessions: HashMap<String, SessionRecord>,
    effects: HashMap<String, Vec<crate::protocol::Effect>>,
}

#[derive(Debug, Clone)]
struct SessionRecord {
    session: Session,
    expires_at: Option<SystemTime>,
}

impl HostState {
    pub fn new(base_dir: impl AsRef<Path>) -> Result<Self> {
        let base_dir = base_dir.as_ref().to_path_buf();
        ensure_host_base_dir_is_safe(&base_dir)?;
        fs::create_dir_all(&base_dir)?;
        let metadata = fs::symlink_metadata(&base_dir)?;
        if metadata.file_type().is_symlink() || !metadata.is_dir() {
            return Err(ExecutionerError::InvalidRequest(
                "host state directory must be a real directory".to_string(),
            ));
        }
        let base_dir = base_dir.canonicalize()?;
        Ok(Self {
            inner: Arc::new(Mutex::new(HostInner {
                base_dir,
                sessions: HashMap::new(),
                effects: HashMap::new(),
            })),
        })
    }

    pub fn create_session(&self, request: CreateSessionRequest) -> Result<CreateSessionResponse> {
        validate_serialized_request_size("create session request", &request)?;
        let mut inner = self.lock()?;
        let session_id = request
            .session_id
            .clone()
            .unwrap_or_else(|| format!("sess_{}", Uuid::new_v4().simple()));
        validate_session_id(&session_id)?;

        inner.purge_expired_sessions()?;
        if inner.sessions.contains_key(&session_id) {
            return Err(ExecutionerError::InvalidRequest(format!(
                "session already exists: {session_id}"
            )));
        }
        validate_workspace_spec(&request)?;
        validate_policy_root_config(&request)?;
        validate_network_policy_disabled(&request)?;
        validate_policy_duration_limit(&request)?;
        validate_policy_output_limit(&request)?;
        let expires_at_time = expiration_time(request.ttl_ms)?;

        let (root, fresh, managed) = match request.workspace.mode {
            WorkspaceMode::New => {
                let session_dir = inner.base_dir.join(&session_id);
                let root = session_dir.join("workspace");
                ensure_managed_workspace_path_is_safe(&inner.base_dir, &session_dir, &root)?;
                fs::create_dir_all(&root)?;
                let root = root.canonicalize()?;
                if !root.starts_with(&inner.base_dir) {
                    return Err(ExecutionerError::InvalidRequest(
                        "managed workspace root escapes host state directory".to_string(),
                    ));
                }
                (root, true, true)
            }
            WorkspaceMode::Existing => {
                let root = request.workspace.root.as_ref().ok_or_else(|| {
                    ExecutionerError::InvalidRequest(
                        "workspace.root is required for existing sessions".to_string(),
                    )
                })?;
                let root = PathBuf::from(root);
                if !root.is_absolute() {
                    return Err(ExecutionerError::InvalidRequest(
                        "workspace.root must be absolute for existing sessions".to_string(),
                    ));
                }
                ensure_existing_workspace_path_is_safe(&root)?;
                let metadata = fs::symlink_metadata(&root).map_err(|_| {
                    ExecutionerError::InvalidRequest(format!(
                        "workspace root is not a directory: {}",
                        root.display()
                    ))
                })?;
                if metadata.file_type().is_symlink() {
                    return Err(ExecutionerError::InvalidRequest(
                        "workspace.root must not be a symlink".to_string(),
                    ));
                }
                if !metadata.is_dir() {
                    return Err(ExecutionerError::InvalidRequest(format!(
                        "workspace root is not a directory: {}",
                        root.display()
                    )));
                }
                (root.canonicalize()?, false, false)
            }
            WorkspaceMode::Snapshot | WorkspaceMode::Template => {
                return Err(ExecutionerError::InvalidRequest(
                    "snapshot/template workspaces are protocol states but not implemented yet"
                        .to_string(),
                ));
            }
        };

        let created_at = now_string();
        let session = Session {
            id: session_id.clone(),
            state: SessionState::Ready,
            workspace: WorkspaceBinding {
                root: root.to_string_lossy().into_owned(),
                logical_root: "/workspace".to_string(),
                mode: request.workspace.mode,
                fresh,
                managed,
            },
            policy: request.policy,
            metadata: request.metadata,
            created_at,
            expires_at: expires_at_time.map(|expires_at| format!("{expires_at:?}")),
        };

        inner.sessions.insert(
            session_id,
            SessionRecord {
                session: session.clone(),
                expires_at: expires_at_time,
            },
        );

        Ok(CreateSessionResponse { session })
    }

    pub fn get_session(&self, session_id: &str) -> Result<Session> {
        validate_session_id(session_id)?;
        let mut inner = self.lock()?;
        inner.purge_expired_sessions()?;
        Ok(inner
            .sessions
            .get(session_id)
            .ok_or_else(|| ExecutionerError::SessionNotFound(session_id.to_string()))?
            .session
            .clone())
    }

    pub fn close_session(&self, session_id: &str) -> Result<Session> {
        validate_session_id(session_id)?;
        let mut inner = self.lock()?;
        inner.purge_expired_sessions()?;
        let record = inner
            .sessions
            .get_mut(session_id)
            .ok_or_else(|| ExecutionerError::SessionNotFound(session_id.to_string()))?;
        record.session.state = SessionState::Closed;
        Ok(record.session.clone())
    }

    pub fn destroy_session(&self, session_id: &str) -> Result<Session> {
        validate_session_id(session_id)?;
        let mut inner = self.lock()?;
        inner.purge_expired_sessions()?;
        let mut record = inner
            .sessions
            .remove(session_id)
            .ok_or_else(|| ExecutionerError::SessionNotFound(session_id.to_string()))?;
        record.session.state = SessionState::Destroyed;
        cleanup_managed_workspace(&inner.base_dir, &record.session);
        inner.effects.remove(session_id);
        Ok(record.session)
    }

    pub fn execute_invocation(
        &self,
        request: ToolInvocationRequest,
    ) -> Result<ToolInvocationResult> {
        validate_serialized_request_size("tool invocation request", &request)?;
        validate_session_id(&request.session_id)?;
        if let Some(invocation_id) = &request.invocation_id {
            validate_invocation_id(invocation_id)?;
        }
        if let Some(max_output_bytes) = request.max_output_bytes {
            validate_output_limit("maxOutputBytes", max_output_bytes)?;
        }
        if let Some(timeout_ms) = request.timeout_ms {
            validate_duration_limit("timeoutMs", timeout_ms)?;
        }
        let session = self.get_session(&request.session_id)?;
        if session.state != SessionState::Ready {
            return Err(ExecutionerError::SessionNotReady(request.session_id));
        }
        if !request.required_capabilities.is_empty() {
            return Ok(required_capabilities_denied_result(&session, &request));
        }
        if request.idempotency_key.is_some() {
            return Ok(idempotency_key_denied_result(&session, &request));
        }

        let result = match request.tool_name.as_str() {
            "Read" | "read" => read_file(&session, request)?,
            "Write" | "write" => write_file(&session, request)?,
            "Edit" | "edit" => edit_file(&session, request)?,
            "Bash" | "bash" => bash(&session, request)?,
            "List" | "list" => list_files(&session, request)?,
            "Glob" | "glob" => glob_files(&session, request)?,
            "Grep" | "grep" => grep_files(&session, request)?,
            other => return Err(ExecutionerError::ToolNotFound(other.to_string())),
        };

        let mut inner = self.lock()?;
        inner
            .effects
            .entry(result.session_id.clone())
            .or_default()
            .extend(result.effects.clone());
        Ok(result)
    }

    pub fn effects(&self, session_id: &str) -> Result<Vec<crate::protocol::Effect>> {
        validate_session_id(session_id)?;
        let mut inner = self.lock()?;
        inner.purge_expired_sessions()?;
        if !inner.sessions.contains_key(session_id) {
            return Err(ExecutionerError::SessionNotFound(session_id.to_string()));
        }
        Ok(inner.effects.get(session_id).cloned().unwrap_or_default())
    }

    pub fn export_workspace(&self, session_id: &str) -> Result<WorkspaceArtifact> {
        validate_session_id(session_id)?;
        let mut inner = self.lock()?;
        inner.purge_expired_sessions()?;
        let session = inner
            .sessions
            .get(session_id)
            .ok_or_else(|| ExecutionerError::SessionNotFound(session_id.to_string()))?
            .session
            .clone();
        let session_dir = inner.base_dir.join(session_id);
        ensure_host_state_directory(
            &inner.base_dir,
            &session_dir,
            "host state session directory",
        )?;
        let output_dir = session_dir.join("artifacts");
        ensure_host_state_directory(&inner.base_dir, &output_dir, "host artifact directory")?;
        let workspace_root = PathBuf::from(&session.workspace.root);
        let excluded_roots = if inner.base_dir.starts_with(&workspace_root) {
            vec![inner.base_dir.clone()]
        } else {
            Vec::new()
        };
        drop(inner);
        crate::artifact::export_workspace_excluding(&session, &output_dir, &excluded_roots)
    }

    fn lock(&self) -> Result<std::sync::MutexGuard<'_, HostInner>> {
        self.inner
            .lock()
            .map_err(|_| ExecutionerError::InvalidRequest("host state lock poisoned".to_string()))
    }
}

pub fn empty_metadata() -> Map<String, serde_json::Value> {
    Map::new()
}

fn validate_workspace_spec(request: &CreateSessionRequest) -> Result<()> {
    if !request.workspace.mount_as_workspace {
        return Err(ExecutionerError::InvalidRequest(
            "workspace.mountAsWorkspace=false is not implemented".to_string(),
        ));
    }
    match request.workspace.mode {
        WorkspaceMode::New => {
            if request.workspace.root.is_some()
                || request.workspace.snapshot_ref.is_some()
                || request.workspace.template_ref.is_some()
            {
                return Err(ExecutionerError::InvalidRequest(
                    "new workspaces must not include root, snapshotRef, or templateRef".to_string(),
                ));
            }
        }
        WorkspaceMode::Existing => {
            if request.workspace.snapshot_ref.is_some() || request.workspace.template_ref.is_some()
            {
                return Err(ExecutionerError::InvalidRequest(
                    "existing workspaces must not include snapshotRef or templateRef".to_string(),
                ));
            }
        }
        WorkspaceMode::Snapshot | WorkspaceMode::Template => {}
    }
    Ok(())
}

fn validate_network_policy_disabled(request: &CreateSessionRequest) -> Result<()> {
    if request.policy.network.enabled
        || !request.policy.network.allow_hosts.is_empty()
        || !request.policy.network.deny_hosts.is_empty()
    {
        return Err(ExecutionerError::InvalidRequest(
            "network policy is not enforceable yet; leave network disabled and host lists empty"
                .to_string(),
        ));
    }
    Ok(())
}

fn validate_policy_root_config(request: &CreateSessionRequest) -> Result<()> {
    validate_policy_roots("policy.readRoots", &request.policy.read_roots)?;
    validate_policy_roots("policy.writeRoots", &request.policy.write_roots)?;
    Ok(())
}

fn validate_policy_output_limit(request: &CreateSessionRequest) -> Result<()> {
    if let Some(max_output_bytes) = request.policy.max_output_bytes {
        validate_output_limit("policy.maxOutputBytes", max_output_bytes)?;
    }
    Ok(())
}

fn validate_policy_duration_limit(request: &CreateSessionRequest) -> Result<()> {
    if let Some(max_duration_ms) = request.policy.max_duration_ms {
        validate_duration_limit("policy.maxDurationMs", max_duration_ms)?;
    }
    Ok(())
}

pub(crate) fn validate_output_limit(label: &str, max_output_bytes: usize) -> Result<()> {
    if max_output_bytes > MAX_OUTPUT_BYTES {
        return Err(ExecutionerError::InvalidRequest(format!(
            "{label} exceeds maximum supported output size of {MAX_OUTPUT_BYTES} bytes"
        )));
    }
    Ok(())
}

pub(crate) fn validate_duration_limit(label: &str, duration_ms: u64) -> Result<()> {
    if duration_ms == 0 {
        return Err(ExecutionerError::InvalidRequest(format!(
            "{label} must be positive"
        )));
    }
    if duration_ms > MAX_TOOL_TIMEOUT_MS {
        return Err(ExecutionerError::InvalidRequest(format!(
            "{label} exceeds maximum supported tool timeout of {MAX_TOOL_TIMEOUT_MS}ms"
        )));
    }
    Ok(())
}

fn validate_serialized_request_size<T: serde::Serialize>(label: &str, value: &T) -> Result<()> {
    let size = serde_json::to_vec(value)?.len();
    if size > MAX_REQUEST_JSON_BYTES {
        return Err(ExecutionerError::InvalidRequest(format!(
            "{label} exceeds maximum JSON size of {MAX_REQUEST_JSON_BYTES} bytes"
        )));
    }
    Ok(())
}

fn expiration_time(ttl_ms: Option<u64>) -> Result<Option<SystemTime>> {
    let Some(ttl_ms) = ttl_ms else {
        return Ok(None);
    };
    if ttl_ms > MAX_SESSION_TTL_MS {
        return Err(ExecutionerError::InvalidRequest(format!(
            "ttlMs exceeds maximum supported session TTL of {MAX_SESSION_TTL_MS}ms"
        )));
    }
    let ttl = Duration::from_millis(ttl_ms);
    SystemTime::now().checked_add(ttl).map(Some).ok_or_else(|| {
        ExecutionerError::InvalidRequest("ttlMs is too large to represent".to_string())
    })
}

fn required_capabilities_denied_result(
    session: &Session,
    request: &ToolInvocationRequest,
) -> ToolInvocationResult {
    let invocation_id = request
        .invocation_id
        .clone()
        .unwrap_or_else(|| format!("inv_{}", Uuid::new_v4().simple()));
    let kinds = request
        .required_capabilities
        .iter()
        .map(|capability| capability.kind.as_str())
        .collect::<Vec<_>>()
        .join(", ");
    ToolInvocationResult {
        invocation_id,
        session_id: session.id.clone(),
        tool_name: request.tool_name.clone(),
        status: ToolResultStatus::PolicyDenied,
        output: String::new(),
        error: Some(format!(
            "required capabilities are not supported by this host: {kinds}"
        )),
        summary: None,
        effects: vec![],
        duration_ms: 0,
        metadata: empty_metadata(),
    }
}

fn idempotency_key_denied_result(
    session: &Session,
    request: &ToolInvocationRequest,
) -> ToolInvocationResult {
    let invocation_id = request
        .invocation_id
        .clone()
        .unwrap_or_else(|| format!("inv_{}", Uuid::new_v4().simple()));
    ToolInvocationResult {
        invocation_id,
        session_id: session.id.clone(),
        tool_name: request.tool_name.clone(),
        status: ToolResultStatus::PolicyDenied,
        output: String::new(),
        error: Some(
            "idempotencyKey is not supported by this host; refusing non-idempotent execution"
                .to_string(),
        ),
        summary: None,
        effects: vec![],
        duration_ms: 0,
        metadata: empty_metadata(),
    }
}

impl HostInner {
    fn purge_expired_sessions(&mut self) -> Result<()> {
        let now = SystemTime::now();
        let expired = self
            .sessions
            .iter()
            .filter_map(|(session_id, record)| {
                let expires_at = record.expires_at?;
                if expires_at <= now {
                    Some(session_id.clone())
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();

        for session_id in expired {
            if let Some(mut record) = self.sessions.remove(&session_id) {
                record.session.state = SessionState::Destroyed;
                cleanup_managed_workspace(&self.base_dir, &record.session);
                self.effects.remove(&session_id);
            }
        }

        Ok(())
    }
}

fn validate_session_id(session_id: &str) -> Result<()> {
    let valid = !session_id.is_empty()
        && session_id.len() <= 128
        && session_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'));
    if valid {
        Ok(())
    } else {
        Err(ExecutionerError::InvalidRequest(format!(
            "invalid session id: {session_id}"
        )))
    }
}

fn validate_invocation_id(invocation_id: &str) -> Result<()> {
    let valid = !invocation_id.is_empty()
        && invocation_id.len() <= 128
        && invocation_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'));
    if valid {
        Ok(())
    } else {
        Err(ExecutionerError::InvalidRequest(format!(
            "invalid invocation id: {invocation_id}"
        )))
    }
}

fn cleanup_managed_workspace(base_dir: &Path, session: &Session) {
    if session.workspace.managed {
        let workspace_root = PathBuf::from(&session.workspace.root);
        if let Some(session_dir) = workspace_root.parent() {
            if session_dir.starts_with(base_dir) {
                let _ = fs::remove_dir_all(session_dir);
            }
        }
    }
}

fn ensure_host_base_dir_is_safe(base_dir: &Path) -> Result<()> {
    let parent = base_dir.parent().unwrap_or_else(|| Path::new("."));
    let mut current = if parent.is_absolute() {
        parent.to_path_buf()
    } else {
        std::env::current_dir()?.join(parent)
    };
    loop {
        match fs::symlink_metadata(&current) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                if is_platform_root_symlink(&current) {
                    if !current.pop() {
                        return Ok(());
                    }
                    continue;
                }
                return Err(ExecutionerError::InvalidRequest(
                    "host state directory parent must not contain symlinks".to_string(),
                ));
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

fn ensure_existing_workspace_path_is_safe(root: &Path) -> Result<()> {
    if let Ok(metadata) = fs::symlink_metadata(root) {
        if metadata.file_type().is_symlink() {
            return Err(ExecutionerError::InvalidRequest(
                "workspace.root must not be a symlink".to_string(),
            ));
        }
    }
    let Some(parent) = root.parent() else {
        return Ok(());
    };
    ensure_path_parent_has_no_symlinks(parent, "workspace.root parent")
}

fn ensure_path_parent_has_no_symlinks(parent: &Path, label: &str) -> Result<()> {
    let mut current = if parent.is_absolute() {
        parent.to_path_buf()
    } else {
        std::env::current_dir()?.join(parent)
    };
    loop {
        match fs::symlink_metadata(&current) {
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

fn ensure_host_state_directory(base_dir: &Path, path: &Path, label: &str) -> Result<()> {
    match fs::symlink_metadata(path) {
        Ok(metadata) => {
            if metadata.file_type().is_symlink() || !metadata.is_dir() {
                return Err(ExecutionerError::InvalidRequest(format!(
                    "{label} must be a real directory"
                )));
            }
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => fs::create_dir(path)?,
        Err(err) => return Err(err.into()),
    }

    let canonical = path.canonicalize()?;
    if !canonical.starts_with(base_dir) {
        return Err(ExecutionerError::InvalidRequest(format!(
            "{label} escapes host state directory"
        )));
    }
    Ok(())
}

fn ensure_managed_workspace_path_is_safe(
    base_dir: &Path,
    session_dir: &Path,
    workspace_root: &Path,
) -> Result<()> {
    if let Ok(metadata) = fs::symlink_metadata(session_dir) {
        if metadata.file_type().is_symlink() {
            return Err(ExecutionerError::InvalidRequest(
                "managed workspace session directory must not be a symlink".to_string(),
            ));
        }
        if !metadata.is_dir() {
            return Err(ExecutionerError::InvalidRequest(
                "managed workspace session path must be a directory".to_string(),
            ));
        }
        let canonical_session_dir = session_dir.canonicalize()?;
        if !canonical_session_dir.starts_with(base_dir) {
            return Err(ExecutionerError::InvalidRequest(
                "managed workspace session directory escapes host state directory".to_string(),
            ));
        }
    }

    if let Ok(metadata) = fs::symlink_metadata(workspace_root) {
        if metadata.file_type().is_symlink() {
            return Err(ExecutionerError::InvalidRequest(
                "managed workspace root must not be a symlink".to_string(),
            ));
        }
        if !metadata.is_dir() {
            return Err(ExecutionerError::InvalidRequest(
                "managed workspace root path must be a directory".to_string(),
            ));
        }
    }

    Ok(())
}
