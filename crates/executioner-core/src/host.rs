use crate::effects::now_string;
use crate::error::{ExecutionerError, Result};
use crate::protocol::{
    CreateEnvironmentRequest, CreateEnvironmentResponse, CreateSessionRequest,
    CreateSessionResponse, Effect, EffectOperation, Environment, EnvironmentState, ExecutionPolicy,
    Session, SessionState, ToolInvocationRequest, ToolInvocationResult, ToolResultStatus,
    WorkspaceArtifact, WorkspaceBinding, WorkspaceMode, MAX_ENVIRONMENT_TTL_MS, MAX_OUTPUT_BYTES,
    MAX_REQUEST_JSON_BYTES, MAX_TOOL_TIMEOUT_MS,
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
    environment_execution_locks: Arc<Mutex<HashMap<String, Arc<Mutex<()>>>>>,
}

#[derive(Debug)]
struct HostInner {
    base_dir: PathBuf,
    environments: HashMap<String, EnvironmentRecord>,
    sessions: HashMap<String, SessionRecord>,
    effects: HashMap<String, Vec<Effect>>,
    active_environment_invocations: HashMap<String, usize>,
    active_session_invocations: HashMap<String, usize>,
}

#[derive(Debug, Clone)]
struct SessionRecord {
    session: Session,
    environment_id: String,
}

#[derive(Debug, Clone)]
struct EnvironmentRecord {
    environment: Environment,
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
                environments: HashMap::new(),
                sessions: HashMap::new(),
                effects: HashMap::new(),
                active_environment_invocations: HashMap::new(),
                active_session_invocations: HashMap::new(),
            })),
            environment_execution_locks: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    pub fn create_environment(
        &self,
        request: CreateEnvironmentRequest,
    ) -> Result<CreateEnvironmentResponse> {
        validate_serialized_request_size("create environment request", &request)?;
        let mut inner = self.lock()?;
        inner.purge_expired_environments()?;
        let environment = inner.create_environment_record(request, None)?;
        Ok(CreateEnvironmentResponse { environment })
    }

    pub fn get_environment(&self, environment_id: &str) -> Result<Environment> {
        validate_environment_id(environment_id)?;
        let mut inner = self.lock()?;
        inner.purge_expired_environments()?;
        Ok(inner
            .environments
            .get(environment_id)
            .ok_or_else(|| ExecutionerError::SessionNotFound(environment_id.to_string()))?
            .environment
            .clone())
    }

    pub fn close_environment(&self, environment_id: &str) -> Result<Environment> {
        validate_environment_id(environment_id)?;
        let mut inner = self.lock()?;
        inner.purge_expired_environments()?;
        if inner.environment_active(environment_id) {
            return Err(ExecutionerError::InvalidRequest(format!(
                "environment has active invocations: {environment_id}"
            )));
        }
        let environment = {
            let record = inner
                .environments
                .get_mut(environment_id)
                .ok_or_else(|| ExecutionerError::SessionNotFound(environment_id.to_string()))?;
            record.environment.state = EnvironmentState::Closed;
            record.environment.clone()
        };
        for session_record in inner.sessions.values_mut() {
            if session_record.environment_id == environment_id {
                session_record.session.state = SessionState::Closed;
            }
        }
        Ok(environment)
    }

    pub fn destroy_environment(&self, environment_id: &str) -> Result<Environment> {
        validate_environment_id(environment_id)?;
        let mut inner = self.lock()?;
        inner.purge_expired_environments()?;
        let environment = inner.destroy_environment(environment_id)?;
        drop(inner);
        self.remove_environment_execution_lock(environment_id);
        Ok(environment)
    }

    pub fn create_session(
        &self,
        environment_id: &str,
        request: CreateSessionRequest,
    ) -> Result<CreateSessionResponse> {
        validate_environment_id(environment_id)?;
        validate_serialized_request_size("create session request", &request)?;
        let mut inner = self.lock()?;
        inner.purge_expired_environments()?;
        let session = inner.create_session_record(environment_id.to_string(), request)?;
        Ok(CreateSessionResponse { session })
    }

    pub fn get_session(&self, session_id: &str) -> Result<Session> {
        validate_session_id(session_id)?;
        let mut inner = self.lock()?;
        inner.purge_expired_environments()?;
        let session_record = inner
            .sessions
            .get(session_id)
            .ok_or_else(|| ExecutionerError::SessionNotFound(session_id.to_string()))?
            .clone();
        let environment = inner
            .environments
            .get(&session_record.environment_id)
            .ok_or_else(|| {
                ExecutionerError::SessionNotFound(session_record.environment_id.clone())
            })?
            .environment
            .clone();
        Ok(session_with_environment(
            session_record.session,
            &environment,
        ))
    }

    pub fn close_session(&self, session_id: &str) -> Result<Session> {
        validate_session_id(session_id)?;
        let mut inner = self.lock()?;
        inner.purge_expired_environments()?;
        if inner.session_active(session_id) {
            return Err(ExecutionerError::InvalidRequest(format!(
                "session has active invocations: {session_id}"
            )));
        }
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
        inner.purge_expired_environments()?;
        if inner.session_active(session_id) {
            return Err(ExecutionerError::InvalidRequest(format!(
                "session has active invocations: {session_id}"
            )));
        }
        let mut record = inner
            .sessions
            .remove(session_id)
            .ok_or_else(|| ExecutionerError::SessionNotFound(session_id.to_string()))?;
        record.session.state = SessionState::Destroyed;
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
        let (session, environment_id) = self.execution_session(&request.session_id)?;
        if session.state != SessionState::Ready {
            return Err(ExecutionerError::SessionNotReady(request.session_id));
        }
        let _active_invocation = self.begin_active_invocation(&environment_id, &session.id)?;
        let execution_lock = self.environment_execution_lock(&environment_id)?;
        let _execution_guard = execution_lock.lock().map_err(|_| {
            ExecutionerError::InvalidRequest("environment execution lock poisoned".to_string())
        })?;
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
        if effects_advance_revision(&result.effects) {
            if let Some(record) = inner.environments.get_mut(&environment_id) {
                record.environment.revision = record.environment.revision.saturating_add(1);
            }
        }
        inner
            .effects
            .entry(environment_id)
            .or_default()
            .extend(result.effects.clone());
        Ok(result)
    }

    pub fn effects(&self, environment_id: &str) -> Result<Vec<Effect>> {
        validate_environment_id(environment_id)?;
        let mut inner = self.lock()?;
        inner.purge_expired_environments()?;
        if !inner.environments.contains_key(environment_id) {
            return Err(ExecutionerError::SessionNotFound(
                environment_id.to_string(),
            ));
        }
        Ok(inner
            .effects
            .get(environment_id)
            .cloned()
            .unwrap_or_default())
    }

    pub fn export_workspace(&self, environment_id: &str) -> Result<WorkspaceArtifact> {
        validate_environment_id(environment_id)?;
        let mut inner = self.lock()?;
        inner.purge_expired_environments()?;
        let environment = inner
            .environments
            .get(environment_id)
            .ok_or_else(|| ExecutionerError::SessionNotFound(environment_id.to_string()))?
            .environment
            .clone();
        let session = artifact_session_for_environment(&environment);
        let session_dir = inner.base_dir.join(&environment.id);
        ensure_host_state_directory(
            &inner.base_dir,
            &session_dir,
            "host state environment directory",
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
        let mut artifact =
            crate::artifact::export_workspace_excluding(&session, &output_dir, &excluded_roots)?;
        artifact.environment_id = environment.id;
        Ok(artifact)
    }

    fn lock(&self) -> Result<std::sync::MutexGuard<'_, HostInner>> {
        self.inner
            .lock()
            .map_err(|_| ExecutionerError::InvalidRequest("host state lock poisoned".to_string()))
    }

    fn environment_execution_lock(&self, environment_id: &str) -> Result<Arc<Mutex<()>>> {
        let mut locks = self.environment_execution_locks.lock().map_err(|_| {
            ExecutionerError::InvalidRequest("environment execution lock poisoned".to_string())
        })?;
        Ok(Arc::clone(
            locks
                .entry(environment_id.to_string())
                .or_insert_with(|| Arc::new(Mutex::new(()))),
        ))
    }

    fn remove_environment_execution_lock(&self, environment_id: &str) {
        if let Ok(mut locks) = self.environment_execution_locks.lock() {
            locks.remove(environment_id);
        }
    }

    fn execution_session(&self, session_id: &str) -> Result<(Session, String)> {
        validate_session_id(session_id)?;
        let mut inner = self.lock()?;
        inner.purge_expired_environments()?;
        let session_record = inner
            .sessions
            .get(session_id)
            .ok_or_else(|| ExecutionerError::SessionNotFound(session_id.to_string()))?
            .clone();
        let environment = inner
            .environments
            .get(&session_record.environment_id)
            .ok_or_else(|| {
                ExecutionerError::SessionNotFound(session_record.environment_id.clone())
            })?
            .environment
            .clone();
        if environment.state != EnvironmentState::Ready {
            return Err(ExecutionerError::SessionNotReady(session_id.to_string()));
        }
        Ok((
            session_with_environment(session_record.session, &environment),
            environment.id,
        ))
    }

    fn begin_active_invocation(
        &self,
        environment_id: &str,
        session_id: &str,
    ) -> Result<ActiveInvocationGuard> {
        let mut inner = self.lock()?;
        inner.purge_expired_environments()?;
        let session_record = inner
            .sessions
            .get(session_id)
            .ok_or_else(|| ExecutionerError::SessionNotFound(session_id.to_string()))?;
        if session_record.environment_id != environment_id {
            return Err(ExecutionerError::InvalidRequest(format!(
                "session {session_id} is not attached to environment {environment_id}"
            )));
        }
        let environment = inner
            .environments
            .get(environment_id)
            .ok_or_else(|| ExecutionerError::SessionNotFound(environment_id.to_string()))?;
        if environment.environment.state != EnvironmentState::Ready
            || session_record.session.state != SessionState::Ready
        {
            return Err(ExecutionerError::SessionNotReady(session_id.to_string()));
        }
        inner.begin_active_invocation(environment_id, session_id);
        Ok(ActiveInvocationGuard {
            state: self.clone(),
            environment_id: environment_id.to_string(),
            session_id: session_id.to_string(),
        })
    }

    fn finish_active_invocation(&self, environment_id: &str, session_id: &str) {
        if let Ok(mut inner) = self.lock() {
            inner.finish_active_invocation(environment_id, session_id);
        }
    }
}

struct ActiveInvocationGuard {
    state: HostState,
    environment_id: String,
    session_id: String,
}

impl Drop for ActiveInvocationGuard {
    fn drop(&mut self) {
        self.state
            .finish_active_invocation(&self.environment_id, &self.session_id);
    }
}

pub fn empty_metadata() -> Map<String, serde_json::Value> {
    Map::new()
}

fn validate_workspace_spec(workspace: &crate::protocol::WorkspaceSpec) -> Result<()> {
    if !workspace.mount_as_workspace {
        return Err(ExecutionerError::InvalidRequest(
            "workspace.mountAsWorkspace=false is not implemented".to_string(),
        ));
    }
    match workspace.mode {
        WorkspaceMode::New => {
            if workspace.root.is_some()
                || workspace.snapshot_ref.is_some()
                || workspace.template_ref.is_some()
            {
                return Err(ExecutionerError::InvalidRequest(
                    "new workspaces must not include root, snapshotRef, or templateRef".to_string(),
                ));
            }
        }
        WorkspaceMode::Existing => {
            if workspace.snapshot_ref.is_some() || workspace.template_ref.is_some() {
                return Err(ExecutionerError::InvalidRequest(
                    "existing workspaces must not include snapshotRef or templateRef".to_string(),
                ));
            }
        }
        WorkspaceMode::Snapshot | WorkspaceMode::Template => {}
    }
    Ok(())
}

fn validate_network_policy_disabled(policy: &ExecutionPolicy) -> Result<()> {
    if policy.network.enabled
        || !policy.network.allow_hosts.is_empty()
        || !policy.network.deny_hosts.is_empty()
    {
        return Err(ExecutionerError::InvalidRequest(
            "network policy is not enforceable yet; leave network disabled and host lists empty"
                .to_string(),
        ));
    }
    Ok(())
}

fn validate_policy_root_config(policy: &ExecutionPolicy) -> Result<()> {
    validate_policy_roots("policy.readRoots", &policy.read_roots)?;
    validate_policy_roots("policy.writeRoots", &policy.write_roots)?;
    Ok(())
}

fn validate_policy_output_limit(policy: &ExecutionPolicy) -> Result<()> {
    if let Some(max_output_bytes) = policy.max_output_bytes {
        validate_output_limit("policy.maxOutputBytes", max_output_bytes)?;
    }
    Ok(())
}

fn validate_policy_duration_limit(policy: &ExecutionPolicy) -> Result<()> {
    if let Some(max_duration_ms) = policy.max_duration_ms {
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
    if ttl_ms > MAX_ENVIRONMENT_TTL_MS {
        return Err(ExecutionerError::InvalidRequest(format!(
            "ttlMs exceeds maximum supported environment TTL of {MAX_ENVIRONMENT_TTL_MS}ms"
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
    fn create_environment_record(
        &mut self,
        request: CreateEnvironmentRequest,
        forced_environment_id: Option<&str>,
    ) -> Result<Environment> {
        let environment_id = forced_environment_id
            .map(ToOwned::to_owned)
            .or(request.environment_id.clone())
            .unwrap_or_else(|| format!("env_{}", Uuid::new_v4().simple()));
        validate_environment_id(&environment_id)?;
        if self.environments.contains_key(&environment_id) {
            return Err(ExecutionerError::InvalidRequest(format!(
                "environment already exists: {environment_id}"
            )));
        }
        validate_workspace_spec(&request.workspace)?;
        validate_policy_root_config(&request.policy)?;
        validate_network_policy_disabled(&request.policy)?;
        validate_policy_duration_limit(&request.policy)?;
        validate_policy_output_limit(&request.policy)?;
        let expires_at_time = expiration_time(request.ttl_ms)?;

        let (root, fresh, managed) = match request.workspace.mode {
            WorkspaceMode::New => {
                let environment_dir = self.base_dir.join(&environment_id);
                let root = environment_dir.join("workspace");
                ensure_managed_workspace_path_is_safe(&self.base_dir, &environment_dir, &root)?;
                fs::create_dir_all(&root)?;
                let root = root.canonicalize()?;
                if !root.starts_with(&self.base_dir) {
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

        let environment = Environment {
            id: environment_id.clone(),
            state: EnvironmentState::Ready,
            workspace: WorkspaceBinding {
                root: root.to_string_lossy().into_owned(),
                logical_root: "/workspace".to_string(),
                mode: request.workspace.mode,
                fresh,
                managed,
            },
            policy: request.policy,
            metadata: request.metadata,
            created_at: now_string(),
            expires_at: expires_at_time.map(|expires_at| format!("{expires_at:?}")),
            revision: 0,
        };
        self.environments.insert(
            environment_id,
            EnvironmentRecord {
                environment: environment.clone(),
                expires_at: expires_at_time,
            },
        );
        Ok(environment)
    }

    fn create_session_record(
        &mut self,
        environment_id: String,
        request: CreateSessionRequest,
    ) -> Result<Session> {
        let session_id = request
            .session_id
            .clone()
            .unwrap_or_else(|| format!("sess_{}", Uuid::new_v4().simple()));
        validate_session_id(&session_id)?;
        if self.sessions.contains_key(&session_id) {
            return Err(ExecutionerError::InvalidRequest(format!(
                "session already exists: {session_id}"
            )));
        }
        let environment = self
            .environments
            .get(&environment_id)
            .ok_or_else(|| ExecutionerError::SessionNotFound(environment_id.clone()))?
            .environment
            .clone();
        if environment.state != EnvironmentState::Ready {
            return Err(ExecutionerError::SessionNotReady(environment_id));
        }
        let policy = match request.policy {
            Some(policy) => effective_session_policy(&environment.policy, policy)?,
            None => environment.policy.clone(),
        };
        let session = Session {
            id: session_id.clone(),
            state: SessionState::Ready,
            workspace: environment.workspace.clone(),
            policy,
            metadata: request.metadata,
            created_at: now_string(),
            expires_at: environment.expires_at.clone(),
        };
        self.sessions.insert(
            session_id,
            SessionRecord {
                session: session.clone(),
                environment_id: environment.id,
            },
        );
        Ok(session)
    }

    fn destroy_environment(&mut self, environment_id: &str) -> Result<Environment> {
        if self.environment_active(environment_id) {
            return Err(ExecutionerError::InvalidRequest(format!(
                "environment has active invocations: {environment_id}"
            )));
        }
        let mut record = self
            .environments
            .remove(environment_id)
            .ok_or_else(|| ExecutionerError::SessionNotFound(environment_id.to_string()))?;
        record.environment.state = EnvironmentState::Destroyed;
        cleanup_managed_workspace_binding(&self.base_dir, &record.environment.workspace);
        self.effects.remove(environment_id);
        self.active_environment_invocations.remove(environment_id);
        let session_ids = self
            .sessions
            .iter()
            .filter_map(|(session_id, session_record)| {
                if session_record.environment_id == environment_id {
                    Some(session_id.clone())
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        for session_id in session_ids {
            self.sessions.remove(&session_id);
            self.active_session_invocations.remove(&session_id);
        }
        Ok(record.environment)
    }

    fn purge_expired_environments(&mut self) -> Result<()> {
        let now = SystemTime::now();
        let expired = self
            .environments
            .iter()
            .filter_map(|(environment_id, record)| {
                let expires_at = record.expires_at?;
                if expires_at <= now {
                    Some(environment_id.clone())
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();

        for environment_id in expired {
            let _ = self.destroy_environment(&environment_id);
        }

        Ok(())
    }

    fn begin_active_invocation(&mut self, environment_id: &str, session_id: &str) {
        *self
            .active_environment_invocations
            .entry(environment_id.to_string())
            .or_default() += 1;
        *self
            .active_session_invocations
            .entry(session_id.to_string())
            .or_default() += 1;
    }

    fn finish_active_invocation(&mut self, environment_id: &str, session_id: &str) {
        decrement_active_count(&mut self.active_environment_invocations, environment_id);
        decrement_active_count(&mut self.active_session_invocations, session_id);
    }

    fn environment_active(&self, environment_id: &str) -> bool {
        self.active_environment_invocations
            .get(environment_id)
            .copied()
            .unwrap_or_default()
            > 0
    }

    fn session_active(&self, session_id: &str) -> bool {
        self.active_session_invocations
            .get(session_id)
            .copied()
            .unwrap_or_default()
            > 0
    }
}

fn decrement_active_count(counts: &mut HashMap<String, usize>, id: &str) {
    if let Some(count) = counts.get_mut(id) {
        *count = count.saturating_sub(1);
        if *count == 0 {
            counts.remove(id);
        }
    }
}

fn session_with_environment(mut session: Session, environment: &Environment) -> Session {
    session.workspace = environment.workspace.clone();
    session.expires_at = environment.expires_at.clone();
    session
}

fn artifact_session_for_environment(environment: &Environment) -> Session {
    Session {
        id: environment.id.clone(),
        state: match environment.state {
            EnvironmentState::Starting => SessionState::Starting,
            EnvironmentState::Ready => SessionState::Ready,
            EnvironmentState::Closing => SessionState::Closing,
            EnvironmentState::Closed => SessionState::Closed,
            EnvironmentState::Destroyed => SessionState::Destroyed,
            EnvironmentState::Failed => SessionState::Failed,
        },
        workspace: environment.workspace.clone(),
        policy: environment.policy.clone(),
        metadata: environment.metadata.clone(),
        created_at: environment.created_at.clone(),
        expires_at: environment.expires_at.clone(),
    }
}

fn effective_session_policy(
    environment_policy: &ExecutionPolicy,
    session_policy: ExecutionPolicy,
) -> Result<ExecutionPolicy> {
    validate_policy_root_config(&session_policy)?;
    validate_network_policy_disabled(&session_policy)?;
    validate_policy_duration_limit(&session_policy)?;
    validate_policy_output_limit(&session_policy)?;
    ensure_roots_within_environment(
        "policy.readRoots",
        &session_policy.read_roots,
        &environment_policy.read_roots,
    )?;
    ensure_roots_within_environment(
        "policy.writeRoots",
        &session_policy.write_roots,
        &environment_policy.write_roots,
    )?;

    let mut effective = session_policy;
    effective.process.allow_exec =
        effective.process.allow_exec && environment_policy.process.allow_exec;
    effective.process.allowed_commands = effective
        .process
        .allowed_commands
        .into_iter()
        .filter(|command| {
            environment_policy
                .process
                .allowed_commands
                .iter()
                .any(|allowed| allowed == command)
        })
        .collect();
    effective
        .process
        .denied_commands
        .extend(environment_policy.process.denied_commands.clone());
    effective.process.max_processes = match (
        environment_policy.process.max_processes,
        effective.process.max_processes,
    ) {
        (Some(environment), Some(session)) => Some(environment.min(session)),
        (Some(environment), None) => Some(environment),
        (None, session) => session,
    };
    effective.max_duration_ms = min_optional_u64(
        environment_policy.max_duration_ms,
        effective.max_duration_ms,
    );
    effective.max_output_bytes = min_optional_usize(
        environment_policy.max_output_bytes,
        effective.max_output_bytes,
    );
    effective.env.allowlist = effective
        .env
        .allowlist
        .into_iter()
        .filter(|name| {
            environment_policy
                .env
                .allowlist
                .iter()
                .any(|allowed| allowed == name)
        })
        .collect();
    effective
        .env
        .denylist
        .extend(environment_policy.env.denylist.clone());
    effective.env.injected.retain(|name, _| {
        environment_policy.env.injected.contains_key(name)
            || effective
                .env
                .allowlist
                .iter()
                .any(|allowed| allowed == name)
    });
    Ok(effective)
}

fn ensure_roots_within_environment(
    label: &str,
    requested_roots: &[String],
    environment_roots: &[String],
) -> Result<()> {
    for requested in requested_roots {
        if !environment_roots
            .iter()
            .any(|root| requested == root || requested.starts_with(&format!("{root}/")))
        {
            return Err(ExecutionerError::InvalidRequest(format!(
                "{label} entry is outside the environment policy ceiling: {requested}"
            )));
        }
    }
    Ok(())
}

fn min_optional_u64(left: Option<u64>, right: Option<u64>) -> Option<u64> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.min(right)),
        (Some(value), None) | (None, Some(value)) => Some(value),
        (None, None) => None,
    }
}

fn min_optional_usize(left: Option<usize>, right: Option<usize>) -> Option<usize> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.min(right)),
        (Some(value), None) | (None, Some(value)) => Some(value),
        (None, None) => None,
    }
}

fn effects_advance_revision(effects: &[Effect]) -> bool {
    effects.iter().any(|effect| {
        matches!(
            effect.operation,
            EffectOperation::Create
                | EffectOperation::Update
                | EffectOperation::Delete
                | EffectOperation::Execute
        )
    })
}

fn validate_session_id(session_id: &str) -> Result<()> {
    validate_identifier("session id", session_id)
}

fn validate_environment_id(environment_id: &str) -> Result<()> {
    validate_identifier("environment id", environment_id)
}

fn validate_invocation_id(invocation_id: &str) -> Result<()> {
    validate_identifier("invocation id", invocation_id)
}

fn validate_identifier(label: &str, value: &str) -> Result<()> {
    let valid = !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'));
    if valid {
        Ok(())
    } else {
        Err(ExecutionerError::InvalidRequest(format!(
            "invalid {label}: {value}"
        )))
    }
}

fn cleanup_managed_workspace_binding(base_dir: &Path, workspace: &WorkspaceBinding) {
    if workspace.managed {
        let workspace_root = PathBuf::from(&workspace.root);
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
                "managed workspace environment directory must not be a symlink".to_string(),
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
                "managed workspace environment directory escapes host state directory".to_string(),
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
