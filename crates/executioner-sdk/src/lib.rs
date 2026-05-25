use anyhow::{bail, Context};
use async_trait::async_trait;
use executioner_core::{
    CreateEnvironmentRequest, CreateEnvironmentResponse, CreateSessionRequest,
    CreateSessionResponse, EffectOperation, Environment, EnvironmentState, ExecutionPolicy,
    HostState, NetworkPolicy, ProcessPolicy, Session, SessionState, ToolInvocationCompleted,
    ToolInvocationFailed, ToolInvocationRequest, ToolInvocationResult, ToolResultStatus,
    WorkspaceArtifact, WorkspaceMode, WorkspaceSpec, MAX_ENVIRONMENT_TTL_MS, MAX_OUTPUT_BYTES,
    MAX_REQUEST_JSON_BYTES, MAX_TOOL_TIMEOUT_MS,
};
use executioner_worker::{ClaimedInvocation, FileBroker, InvocationBroker, ToolHostClient, Worker};
use reqwest::Url;
use serde::Serialize;
use serde_json::{Map, Value};
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use thiserror::Error;
use tokio::task::JoinHandle;
use uuid::Uuid;

const MAX_HTTP_ERROR_BODY_BYTES: usize = 64 * 1024;
const MAX_HTTP_JSON_BODY_BYTES: usize = 10 * 1024 * 1024;
const MIN_IDLE_SLEEP: Duration = Duration::from_millis(1);
const MIN_SUBMIT_TIMEOUT: Duration = Duration::from_millis(1);

fn normalize_idle_sleep(idle_sleep: Duration) -> Duration {
    idle_sleep.max(MIN_IDLE_SLEEP)
}

fn validate_submit_timeout(timeout: Duration) -> Result<Duration> {
    if timeout < MIN_SUBMIT_TIMEOUT {
        return Err(SdkError::Config(
            "submit timeout must be positive".to_string(),
        ));
    }
    Ok(timeout)
}

pub type Result<T> = std::result::Result<T, SdkError>;

#[derive(Debug, Error)]
pub enum SdkError {
    #[error("invalid environment config: {0}")]
    Config(String),
    #[error("host error: {0}")]
    Host(String),
    #[error("broker error: {0}")]
    Broker(String),
    #[error("transport error: {0}")]
    Transport(String),
    #[error("worker error: {0}")]
    Worker(String),
    #[error("tool invocation failed: {message}")]
    InvocationFailed { code: String, message: String },
    #[error("{tool_name} failed with status {status:?}: {message}")]
    ToolUnsuccessful {
        tool_name: String,
        status: ToolStatus,
        message: String,
    },
    #[error("timed out waiting for tool invocation result after {timeout:?}: {invocation_id}")]
    Timeout {
        invocation_id: String,
        timeout: Duration,
    },
    #[error("expected JSON object arguments")]
    ExpectedJsonObject,
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}

#[derive(Debug, Clone)]
pub struct EnvironmentConfig {
    pub backend: BackendConfig,
    pub host: HostConfig,
    pub worker: WorkerConfig,
    pub workspace: WorkspaceConfig,
    pub policy: PolicyConfig,
    pub lifecycle: LifecycleConfig,
    pub submit_timeout: Duration,
}

#[derive(Debug, Clone)]
pub struct AttachedEnvironmentConfig {
    pub base_url: String,
    pub environment_id: String,
    pub submit_timeout: Duration,
}

impl AttachedEnvironmentConfig {
    pub fn http_direct(base_url: impl Into<String>, environment_id: impl Into<String>) -> Self {
        Self {
            base_url: base_url.into(),
            environment_id: environment_id.into(),
            submit_timeout: Duration::from_secs(30),
        }
    }

    pub fn submit_timeout(mut self, timeout: Duration) -> Self {
        self.submit_timeout = timeout;
        self
    }
}

impl EnvironmentConfig {
    pub fn builder() -> EnvironmentConfigBuilder {
        EnvironmentConfigBuilder::default()
    }

    pub fn local_file(queue_dir: impl Into<PathBuf>, state_dir: impl Into<PathBuf>) -> Self {
        Self {
            backend: BackendConfig::File {
                queue_dir: queue_dir.into(),
            },
            host: HostConfig::InProcess {
                state_dir: state_dir.into(),
            },
            worker: WorkerConfig::InProcess {
                id: "executioner-sdk-worker".to_string(),
                idle_sleep: Duration::from_millis(10),
            },
            workspace: WorkspaceConfig::New,
            policy: PolicyConfig::default(),
            lifecycle: LifecycleConfig::default(),
            submit_timeout: Duration::from_secs(30),
        }
    }
}

#[derive(Debug, Default)]
pub struct EnvironmentConfigBuilder {
    backend: Option<BackendConfig>,
    host: Option<HostConfig>,
    worker: Option<WorkerConfig>,
    workspace: Option<WorkspaceConfig>,
    policy: Option<PolicyConfig>,
    lifecycle: Option<LifecycleConfig>,
    submit_timeout: Option<Duration>,
}

impl EnvironmentConfigBuilder {
    pub fn file_backend(mut self, queue_dir: impl Into<PathBuf>) -> Self {
        self.backend = Some(BackendConfig::File {
            queue_dir: queue_dir.into(),
        });
        self
    }

    pub fn in_process_host(mut self, state_dir: impl Into<PathBuf>) -> Self {
        self.host = Some(HostConfig::InProcess {
            state_dir: state_dir.into(),
        });
        self
    }

    pub fn http_host(mut self, base_url: impl Into<String>) -> Self {
        self.host = Some(HostConfig::ConnectHttp {
            base_url: base_url.into(),
        });
        self
    }

    pub fn in_process_worker(mut self, id: impl Into<String>) -> Self {
        self.worker = Some(WorkerConfig::InProcess {
            id: id.into(),
            idle_sleep: Duration::from_millis(10),
        });
        self
    }

    pub fn in_process_worker_with_sleep(
        mut self,
        id: impl Into<String>,
        idle_sleep: Duration,
    ) -> Self {
        self.worker = Some(WorkerConfig::InProcess {
            id: id.into(),
            idle_sleep: normalize_idle_sleep(idle_sleep),
        });
        self
    }

    pub fn managed_worker(mut self, id: impl Into<String>) -> Self {
        self.worker = Some(WorkerConfig::Managed {
            id: id.into(),
            idle_sleep: Duration::from_millis(10),
        });
        self
    }

    pub fn managed_worker_with_sleep(
        mut self,
        id: impl Into<String>,
        idle_sleep: Duration,
    ) -> Self {
        self.worker = Some(WorkerConfig::Managed {
            id: id.into(),
            idle_sleep: normalize_idle_sleep(idle_sleep),
        });
        self
    }

    pub fn external_worker(mut self) -> Self {
        self.worker = Some(WorkerConfig::External);
        self
    }

    pub fn new_workspace(mut self) -> Self {
        self.workspace = Some(WorkspaceConfig::New);
        self
    }

    pub fn existing_workspace(mut self, root: impl Into<PathBuf>) -> Self {
        self.workspace = Some(WorkspaceConfig::Existing { root: root.into() });
        self
    }

    pub fn policy(mut self, policy: PolicyConfig) -> Self {
        self.policy = Some(policy);
        self
    }

    pub fn lifecycle(mut self, lifecycle: LifecycleConfig) -> Self {
        self.lifecycle = Some(lifecycle);
        self
    }

    pub fn submit_timeout(mut self, timeout: Duration) -> Self {
        self.submit_timeout = Some(timeout);
        self
    }

    pub fn build(self) -> Result<EnvironmentConfig> {
        let submit_timeout =
            validate_submit_timeout(self.submit_timeout.unwrap_or(Duration::from_secs(30)))?;
        let worker = self.worker.unwrap_or_else(|| WorkerConfig::InProcess {
            id: "executioner-sdk-worker".to_string(),
            idle_sleep: Duration::from_millis(10),
        });
        validate_worker_config(&worker)?;
        Ok(EnvironmentConfig {
            backend: self
                .backend
                .ok_or_else(|| SdkError::Config("backend is required".to_string()))?,
            host: self
                .host
                .ok_or_else(|| SdkError::Config("host is required".to_string()))?,
            worker,
            workspace: self.workspace.unwrap_or(WorkspaceConfig::New),
            policy: self.policy.unwrap_or_default(),
            lifecycle: self.lifecycle.unwrap_or_default(),
            submit_timeout,
        })
    }
}

#[derive(Debug, Clone)]
pub struct WorkerRuntimeConfig {
    pub backend: BackendConfig,
    pub host: HostConfig,
    pub id: String,
    pub idle_sleep: Duration,
}

impl WorkerRuntimeConfig {
    pub fn builder() -> WorkerRuntimeConfigBuilder {
        WorkerRuntimeConfigBuilder::default()
    }
}

#[derive(Debug, Default)]
pub struct WorkerRuntimeConfigBuilder {
    backend: Option<BackendConfig>,
    host: Option<HostConfig>,
    id: Option<String>,
    idle_sleep: Option<Duration>,
}

impl WorkerRuntimeConfigBuilder {
    pub fn file_backend(mut self, queue_dir: impl Into<PathBuf>) -> Self {
        self.backend = Some(BackendConfig::File {
            queue_dir: queue_dir.into(),
        });
        self
    }

    pub fn http_host(mut self, base_url: impl Into<String>) -> Self {
        self.host = Some(HostConfig::ConnectHttp {
            base_url: base_url.into(),
        });
        self
    }

    pub fn id(mut self, id: impl Into<String>) -> Self {
        self.id = Some(id.into());
        self
    }

    pub fn idle_sleep(mut self, idle_sleep: Duration) -> Self {
        self.idle_sleep = Some(normalize_idle_sleep(idle_sleep));
        self
    }

    pub fn build(self) -> Result<WorkerRuntimeConfig> {
        let id = self.id.unwrap_or_else(|| "executioner-worker".to_string());
        validate_worker_id(&id)?;
        Ok(WorkerRuntimeConfig {
            backend: self
                .backend
                .ok_or_else(|| SdkError::Config("worker backend is required".to_string()))?,
            host: self
                .host
                .ok_or_else(|| SdkError::Config("worker host is required".to_string()))?,
            id,
            idle_sleep: self.idle_sleep.unwrap_or(Duration::from_millis(250)),
        })
    }
}

#[derive(Debug, Clone)]
pub enum BackendConfig {
    File { queue_dir: PathBuf },
}

#[derive(Debug, Clone)]
pub enum HostConfig {
    InProcess { state_dir: PathBuf },
    ConnectHttp { base_url: String },
}

#[derive(Debug, Clone)]
pub enum WorkerConfig {
    InProcess { id: String, idle_sleep: Duration },
    Managed { id: String, idle_sleep: Duration },
    External,
}

#[derive(Debug, Clone)]
pub enum WorkspaceConfig {
    New,
    Existing { root: PathBuf },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PolicyConfig {
    pub allow_exec: bool,
    pub allowed_commands: Vec<String>,
    pub env_allowlist: Vec<String>,
    pub env_denylist: Vec<String>,
    pub env_injected: HashMap<String, String>,
    pub network_enabled: bool,
    pub read_roots: Vec<String>,
    pub write_roots: Vec<String>,
    pub max_duration_ms: Option<u64>,
    pub max_output_bytes: Option<usize>,
}

impl Default for PolicyConfig {
    fn default() -> Self {
        Self {
            allow_exec: false,
            allowed_commands: vec![],
            env_allowlist: vec![],
            env_denylist: vec![],
            env_injected: HashMap::new(),
            network_enabled: false,
            read_roots: vec!["/workspace".to_string()],
            write_roots: vec!["/workspace".to_string()],
            max_duration_ms: Some(300_000),
            max_output_bytes: Some(100_000),
        }
    }
}

impl PolicyConfig {
    pub fn allow_exec(mut self, allow_exec: bool) -> Self {
        self.allow_exec = allow_exec;
        self
    }

    pub fn allowed_commands<I, S>(mut self, allowed_commands: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.allowed_commands = allowed_commands.into_iter().map(Into::into).collect();
        self
    }

    pub fn env_allowlist<I, S>(mut self, allowlist: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.env_allowlist = allowlist.into_iter().map(Into::into).collect();
        self
    }

    pub fn env_denylist<I, S>(mut self, denylist: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.env_denylist = denylist.into_iter().map(Into::into).collect();
        self
    }

    pub fn inject_env(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.env_injected.insert(name.into(), value.into());
        self
    }

    pub fn network_enabled(mut self, network_enabled: bool) -> Self {
        self.network_enabled = network_enabled;
        self
    }
}

#[derive(Debug, Clone)]
pub struct LifecycleConfig {
    pub close_behavior: CloseBehavior,
    pub queue_cleanup: QueueCleanup,
    pub ttl_ms: Option<u64>,
}

impl Default for LifecycleConfig {
    fn default() -> Self {
        Self {
            close_behavior: CloseBehavior::DestroyEnvironment,
            queue_cleanup: QueueCleanup::Preserve,
            ttl_ms: None,
        }
    }
}

impl LifecycleConfig {
    pub fn close_environment() -> Self {
        Self {
            close_behavior: CloseBehavior::CloseEnvironment,
            ..Self::default()
        }
    }

    pub fn destroy_environment() -> Self {
        Self::default()
    }

    pub fn delete_queue_on_close(mut self) -> Self {
        self.queue_cleanup = QueueCleanup::DeleteOnClose;
        self
    }

    pub fn ttl_ms(mut self, ttl_ms: u64) -> Self {
        self.ttl_ms = Some(ttl_ms);
        self
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CloseBehavior {
    CloseEnvironment,
    DestroyEnvironment,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueueCleanup {
    Preserve,
    DeleteOnClose,
}

#[derive(Debug, Clone)]
pub struct ToolCall {
    pub tool_name: String,
    pub arguments: Map<String, Value>,
    pub cwd: Option<String>,
    pub invocation_id: Option<String>,
    pub timeout_ms: Option<u64>,
    pub max_output_bytes: Option<usize>,
    pub metadata: Map<String, Value>,
}

impl ToolCall {
    pub fn new(tool_name: impl Into<String>, arguments: Map<String, Value>) -> Self {
        Self {
            tool_name: tool_name.into(),
            arguments,
            cwd: Some("/workspace".to_string()),
            invocation_id: None,
            timeout_ms: None,
            max_output_bytes: None,
            metadata: Map::new(),
        }
    }

    pub fn json(tool_name: impl Into<String>, arguments: Value) -> Result<Self> {
        Ok(Self::new(tool_name, json_object(arguments)?))
    }

    pub fn cwd(mut self, cwd: impl Into<String>) -> Self {
        self.cwd = Some(cwd.into());
        self
    }

    pub fn no_cwd(mut self) -> Self {
        self.cwd = None;
        self
    }

    pub fn invocation_id(mut self, invocation_id: impl Into<String>) -> Self {
        self.invocation_id = Some(invocation_id.into());
        self
    }

    pub fn timeout_ms(mut self, timeout_ms: u64) -> Self {
        self.timeout_ms = Some(timeout_ms);
        self
    }

    pub fn max_output_bytes(mut self, max_output_bytes: usize) -> Self {
        self.max_output_bytes = Some(max_output_bytes);
        self
    }

    pub fn metadata(mut self, key: impl Into<String>, value: Value) -> Self {
        self.metadata.insert(key.into(), value);
        self
    }
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct SessionInfo {
    pub id: String,
    pub state: SessionStatus,
    pub workspace: WorkspaceInfo,
    pub created_at: String,
    pub expires_at: Option<String>,
    pub metadata: Map<String, Value>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct EnvironmentInfo {
    pub id: String,
    pub state: EnvironmentStatus,
    pub workspace: WorkspaceInfo,
    pub created_at: String,
    pub expires_at: Option<String>,
    pub metadata: Map<String, Value>,
    pub revision: u64,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SessionStatus {
    Starting,
    Ready,
    Closing,
    Closed,
    Destroyed,
    Failed,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum EnvironmentStatus {
    Starting,
    Ready,
    Closing,
    Closed,
    Destroyed,
    Failed,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct WorkspaceInfo {
    pub root: String,
    pub logical_root: String,
    pub mode: WorkspaceKind,
    pub fresh: bool,
    pub managed: bool,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum WorkspaceKind {
    New,
    Existing,
    Snapshot,
    Template,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct SubmitResult {
    pub invocation_id: String,
    pub tool_name: String,
    pub status: ToolStatus,
    pub output: String,
    pub error: Option<String>,
    pub summary: Option<String>,
    pub effects: Vec<StateEffect>,
    pub duration_ms: u64,
    pub metadata: Map<String, Value>,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ToolStatus {
    Success,
    Error,
    Timeout,
    Cancelled,
    PolicyDenied,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct StateEffect {
    pub id: String,
    pub invocation_id: String,
    pub kind: String,
    pub resource_type: String,
    pub uri: String,
    pub operation: EffectKind,
    pub summary: Option<String>,
    pub reversible: bool,
    pub occurred_at: String,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum EffectKind {
    Read,
    Create,
    Update,
    Delete,
    Execute,
}

#[derive(Debug)]
pub struct ExecutionerEnvironment {
    environment: EnvironmentInfo,
    session_transport: SessionTransport,
    queue_dir: Option<QueueDir>,
    host: Arc<HostBackend>,
    worker: WorkerDriver,
    lifecycle: LifecycleConfig,
    submit_timeout: Duration,
    owns_environment: bool,
}

#[derive(Debug, Clone)]
pub struct ExecutionerSession {
    session: SessionInfo,
    transport: SessionTransport,
    host: Arc<HostBackend>,
    inline_worker: Option<Worker>,
    submit_timeout: Duration,
}

#[derive(Debug, Clone)]
enum SessionTransport {
    Broker(Arc<BackendClient>),
    Direct,
}

#[derive(Debug, Clone)]
struct QueueDir {
    path: PathBuf,
    created_by_sdk: bool,
}

#[derive(Debug)]
pub struct ExecutionerWorker {
    task: ManagedWorker,
}

impl ExecutionerWorker {
    pub fn builder() -> WorkerRuntimeConfigBuilder {
        WorkerRuntimeConfig::builder()
    }

    pub fn start(config: WorkerRuntimeConfig) -> Result<Self> {
        validate_worker_id(&config.id)?;
        let backend = Arc::new(BackendClient::from_config(config.backend)?);
        let host = Arc::new(HostBackend::from_config(config.host)?);
        Ok(Self {
            task: ManagedWorker::spawn(
                config.id,
                normalize_idle_sleep(config.idle_sleep),
                backend,
                host,
            ),
        })
    }

    pub async fn shutdown(&self) -> Result<()> {
        self.task.shutdown().await
    }
}

impl ExecutionerEnvironment {
    pub fn builder() -> EnvironmentConfigBuilder {
        EnvironmentConfig::builder()
    }

    pub async fn create(config: EnvironmentConfig) -> Result<Self> {
        validate_environment_config(&config)?;
        let workspace = config.workspace.into_spec()?;
        let backend = Arc::new(BackendClient::from_config(config.backend)?);
        let queue_dir = backend.queue_dir();

        let mut host = HostBackend::from_config(config.host)?;
        let environment = host
            .create_environment(CreateEnvironmentRequest {
                environment_id: None,
                workspace,
                policy: config.policy.into_execution_policy(),
                ttl_ms: config.lifecycle.ttl_ms,
                metadata: Map::new(),
            })
            .await?
            .environment;
        validate_environment_id(&environment.id)?;
        let host = Arc::new(host);
        let worker =
            WorkerDriver::from_config(config.worker, Arc::clone(&backend), Arc::clone(&host));

        Ok(Self {
            environment: environment.into(),
            session_transport: SessionTransport::Broker(Arc::clone(&backend)),
            queue_dir,
            host,
            worker,
            lifecycle: config.lifecycle,
            submit_timeout: validate_submit_timeout(config.submit_timeout)?,
            owns_environment: true,
        })
    }

    pub async fn attach(config: AttachedEnvironmentConfig) -> Result<Self> {
        validate_environment_id(&config.environment_id)?;
        let host = Arc::new(HostBackend::from_config(HostConfig::ConnectHttp {
            base_url: config.base_url,
        })?);
        let environment = host.get_environment(&config.environment_id).await?;
        validate_environment_id(&environment.id)?;
        Ok(Self {
            environment: environment.into(),
            session_transport: SessionTransport::Direct,
            queue_dir: None,
            host,
            worker: WorkerDriver::External,
            lifecycle: LifecycleConfig::close_environment(),
            submit_timeout: validate_submit_timeout(config.submit_timeout)?,
            owns_environment: false,
        })
    }

    pub fn environment(&self) -> &EnvironmentInfo {
        &self.environment
    }

    pub async fn create_session(&self) -> Result<ExecutionerSession> {
        self.create_session_with_policy(None).await
    }

    pub async fn create_session_with_policy(
        &self,
        policy: Option<PolicyConfig>,
    ) -> Result<ExecutionerSession> {
        let session = self
            .host
            .create_session(
                &self.environment.id,
                CreateSessionRequest {
                    session_id: None,
                    policy: policy.map(PolicyConfig::into_execution_policy),
                    metadata: Map::new(),
                },
            )
            .await?
            .session;
        validate_session_id(&session.id)?;
        Ok(ExecutionerSession {
            session: session.into(),
            transport: self.session_transport.clone(),
            host: Arc::clone(&self.host),
            inline_worker: match &self.worker {
                WorkerDriver::InProcess(worker) => Some(worker.clone()),
                WorkerDriver::Managed(_) | WorkerDriver::External => None,
            },
            submit_timeout: self.submit_timeout,
        })
    }

    pub async fn close(&self) -> Result<EnvironmentInfo> {
        let worker_result = self.worker.shutdown().await;

        let environment_result: Result<EnvironmentInfo> = if self.owns_environment {
            match self.lifecycle.close_behavior {
                CloseBehavior::DestroyEnvironment => {
                    self.host.destroy_environment(&self.environment.id).await
                }
                CloseBehavior::CloseEnvironment => {
                    self.host.close_environment(&self.environment.id).await
                }
            }
            .map(Into::into)
        } else {
            Ok(self.environment.clone())
        };

        let cleanup_result: Result<()> =
            if self.lifecycle.queue_cleanup == QueueCleanup::DeleteOnClose {
                if let Some(queue_dir) = &self.queue_dir {
                    cleanup_queue_dir(queue_dir)
                } else {
                    Ok(())
                }
            } else {
                Ok(())
            };

        worker_result?;
        match (environment_result, cleanup_result) {
            (Ok(environment), Ok(())) => Ok(environment),
            (Err(err), _) => Err(err),
            (Ok(_), Err(err)) => Err(err),
        }
    }

    pub async fn export_workspace(&self) -> Result<WorkspaceArtifact> {
        self.host.export_workspace(&self.environment.id).await
    }

    pub fn materialize_workspace_artifact(
        &self,
        artifact: &WorkspaceArtifact,
        destination: impl AsRef<Path>,
    ) -> Result<()> {
        executioner_core::artifact::materialize_workspace_artifact(artifact, destination.as_ref())
            .map_err(|err| SdkError::Host(err.to_string()))
    }
}

impl ExecutionerSession {
    pub fn session(&self) -> &SessionInfo {
        &self.session
    }

    pub async fn submit(&self, call: ToolCall) -> Result<SubmitResult> {
        validate_tool_name(&call.tool_name)?;
        validate_tool_call_limits(&call)?;
        let tool_name = call.tool_name.clone();
        let invocation_id = call
            .invocation_id
            .unwrap_or_else(|| format!("inv_{}", Uuid::new_v4().simple()));
        validate_identifier("invocationId", &invocation_id)?;
        let request = ToolInvocationRequest {
            invocation_id: Some(invocation_id.clone()),
            session_id: self.session.id.clone(),
            tool_name: tool_name.clone(),
            arguments: call.arguments,
            cwd: call.cwd,
            timeout_ms: call.timeout_ms,
            max_output_bytes: call.max_output_bytes,
            idempotency_key: None,
            required_capabilities: vec![],
            metadata: call.metadata,
        };
        validate_serialized_request_size("tool invocation request", &request)?;

        match &self.transport {
            SessionTransport::Broker(backend) => {
                backend
                    .enqueue(&request)
                    .map_err(|err| SdkError::Broker(err.to_string()))?;
                self.wait_for_result(backend, &invocation_id, &tool_name)
                    .await
            }
            SessionTransport::Direct => self
                .host
                .execute(request)
                .await
                .map(Into::into)
                .map_err(|err| SdkError::Host(err.to_string())),
        }
    }

    pub async fn list_files(&self, cwd: impl Into<String>) -> Result<Vec<String>> {
        let result = self
            .submit(ToolCall::json("List", serde_json::json!({}))?.cwd(cwd))
            .await?;
        parse_list_files_result(&result)
    }

    pub async fn list(&self, cwd: impl Into<String>) -> Result<Vec<String>> {
        self.list_files(cwd).await
    }

    pub async fn close(&self) -> Result<SessionInfo> {
        self.host
            .close_session(&self.session.id)
            .await
            .map(Into::into)
    }

    async fn wait_for_result(
        &self,
        backend: &BackendClient,
        invocation_id: &str,
        tool_name: &str,
    ) -> Result<SubmitResult> {
        let started_at = Instant::now();
        loop {
            if let Some(completed) = backend
                .read_completed(invocation_id)
                .map_err(|err| SdkError::Broker(err.to_string()))?
            {
                if completed.session_id != self.session.id
                    || completed.result.session_id != self.session.id
                {
                    return Err(SdkError::Broker(format!(
                        "terminal result session mismatch for invocation {invocation_id}"
                    )));
                }
                if completed.result.tool_name != tool_name {
                    return Err(SdkError::Broker(format!(
                        "terminal result toolName mismatch for invocation {invocation_id}"
                    )));
                }
                return Ok(completed.result.into());
            }

            if let Some(failed) = backend
                .read_failed(invocation_id)
                .map_err(|err| SdkError::Broker(err.to_string()))?
            {
                if failed.session_id != self.session.id {
                    return Err(SdkError::Broker(format!(
                        "terminal failure session mismatch for invocation {invocation_id}"
                    )));
                }
                return Err(SdkError::InvocationFailed {
                    code: failed.error.code,
                    message: failed.error.message,
                });
            }

            if started_at.elapsed() >= self.submit_timeout {
                return Err(SdkError::Timeout {
                    invocation_id: invocation_id.to_string(),
                    timeout: self.submit_timeout,
                });
            }

            if let Some(worker) = &self.inline_worker {
                worker
                    .run_once(backend, &*self.host)
                    .await
                    .map_err(|err| SdkError::Broker(err.to_string()))?;
            }

            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
}

#[derive(Debug)]
enum WorkerDriver {
    InProcess(Worker),
    Managed(ManagedWorker),
    External,
}

impl WorkerDriver {
    fn from_config(
        config: WorkerConfig,
        backend: Arc<BackendClient>,
        host: Arc<HostBackend>,
    ) -> Self {
        match config {
            WorkerConfig::InProcess { id, idle_sleep } => {
                Self::InProcess(Worker::new(id).with_idle_sleep(idle_sleep))
            }
            WorkerConfig::Managed { id, idle_sleep } => {
                Self::Managed(ManagedWorker::spawn(id, idle_sleep, backend, host))
            }
            WorkerConfig::External => Self::External,
        }
    }

    async fn shutdown(&self) -> Result<()> {
        match self {
            Self::Managed(worker) => worker.shutdown().await,
            Self::InProcess(_) | Self::External => Ok(()),
        }
    }
}

#[derive(Debug)]
struct ManagedWorker {
    task: Mutex<Option<JoinHandle<anyhow::Result<()>>>>,
}

impl ManagedWorker {
    fn spawn(
        id: String,
        idle_sleep: Duration,
        backend: Arc<BackendClient>,
        host: Arc<HostBackend>,
    ) -> Self {
        let worker = Worker::new(id).with_idle_sleep(idle_sleep);
        let task = tokio::spawn(async move { worker.run(&*backend, &*host).await });
        Self {
            task: Mutex::new(Some(task)),
        }
    }

    async fn shutdown(&self) -> Result<()> {
        let task = self
            .task
            .lock()
            .map_err(|_| SdkError::Worker("managed worker lock poisoned".to_string()))?
            .take();
        if let Some(task) = task {
            task.abort();
            match task.await {
                Ok(Ok(())) => Ok(()),
                Ok(Err(err)) => Err(SdkError::Worker(err.to_string())),
                Err(err) if err.is_cancelled() => Ok(()),
                Err(err) => Err(SdkError::Worker(err.to_string())),
            }
        } else {
            Ok(())
        }
    }
}

impl Drop for ManagedWorker {
    fn drop(&mut self) {
        if let Ok(mut task) = self.task.lock() {
            if let Some(task) = task.take() {
                task.abort();
            }
        }
    }
}

fn cleanup_queue_dir(queue_dir: &QueueDir) -> Result<()> {
    if queue_dir.created_by_sdk {
        return remove_path_without_following(&queue_dir.path);
    }

    if fs::symlink_metadata(&queue_dir.path)
        .map(|metadata| metadata.file_type().is_symlink())
        .unwrap_or(false)
    {
        return remove_path_without_following(&queue_dir.path);
    }

    for child in ["pending", "claimed", "completed", "failed", "rejected"] {
        remove_path_without_following(&queue_dir.path.join(child))?;
    }
    Ok(())
}

fn remove_path_without_following(path: &Path) -> Result<()> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(err) => return Err(SdkError::Io(err)),
    };
    if metadata.file_type().is_symlink() || metadata.file_type().is_file() {
        fs::remove_file(path).map_err(SdkError::Io)
    } else {
        fs::remove_dir_all(path).map_err(SdkError::Io)
    }
}

#[derive(Debug)]
enum BackendClient {
    File(FileBackendClient),
}

impl BackendClient {
    fn from_config(config: BackendConfig) -> Result<Self> {
        match config {
            BackendConfig::File { queue_dir } => {
                let created_by_sdk = match fs::symlink_metadata(&queue_dir) {
                    Ok(_) => false,
                    Err(err) if err.kind() == std::io::ErrorKind::NotFound => true,
                    Err(err) => return Err(SdkError::Io(err)),
                };
                Ok(Self::File(FileBackendClient {
                    queue_dir: QueueDir {
                        path: queue_dir.clone(),
                        created_by_sdk,
                    },
                    broker: FileBroker::new(queue_dir)
                        .map_err(|err| SdkError::Broker(err.to_string()))?,
                }))
            }
        }
    }

    fn queue_dir(&self) -> Option<QueueDir> {
        match self {
            Self::File(file) => Some(file.queue_dir.clone()),
        }
    }

    fn enqueue(&self, request: &ToolInvocationRequest) -> anyhow::Result<PathBuf> {
        match self {
            Self::File(file) => file.broker.enqueue(request),
        }
    }

    fn read_completed(
        &self,
        invocation_id: &str,
    ) -> anyhow::Result<Option<ToolInvocationCompleted>> {
        match self {
            Self::File(file) => file.broker.read_completed(invocation_id),
        }
    }

    fn read_failed(&self, invocation_id: &str) -> anyhow::Result<Option<ToolInvocationFailed>> {
        match self {
            Self::File(file) => file.broker.read_failed(invocation_id),
        }
    }
}

#[derive(Debug)]
struct FileBackendClient {
    queue_dir: QueueDir,
    broker: FileBroker,
}

#[async_trait]
impl InvocationBroker for BackendClient {
    async fn claim_next(&self, worker_id: &str) -> anyhow::Result<Option<ClaimedInvocation>> {
        match self {
            Self::File(file) => file.broker.claim_next(worker_id).await,
        }
    }

    async fn complete(&self, event: ToolInvocationCompleted) -> anyhow::Result<()> {
        match self {
            Self::File(file) => file.broker.complete(event).await,
        }
    }

    async fn fail(&self, event: ToolInvocationFailed) -> anyhow::Result<()> {
        match self {
            Self::File(file) => file.broker.fail(event).await,
        }
    }
}

#[derive(Debug)]
enum HostBackend {
    InProcess(HostState),
    Http(HttpHostBackend),
}

impl HostBackend {
    fn from_config(config: HostConfig) -> Result<Self> {
        match config {
            HostConfig::InProcess { state_dir } => HostState::new(state_dir)
                .map(Self::InProcess)
                .map_err(|err| SdkError::Host(err.to_string())),
            HostConfig::ConnectHttp { base_url } => Ok(Self::Http(HttpHostBackend::new(base_url)?)),
        }
    }

    async fn create_environment(
        &mut self,
        request: CreateEnvironmentRequest,
    ) -> Result<CreateEnvironmentResponse> {
        match self {
            Self::InProcess(state) => state
                .create_environment(request)
                .map_err(|err| SdkError::Host(err.to_string())),
            Self::Http(host) => host.create_environment(request).await,
        }
    }

    async fn create_session(
        &self,
        environment_id: &str,
        request: CreateSessionRequest,
    ) -> Result<CreateSessionResponse> {
        match self {
            Self::InProcess(state) => state
                .create_session(environment_id, request)
                .map_err(|err| SdkError::Host(err.to_string())),
            Self::Http(host) => host.create_session(environment_id, request).await,
        }
    }

    async fn get_environment(&self, environment_id: &str) -> Result<Environment> {
        match self {
            Self::InProcess(state) => state
                .get_environment(environment_id)
                .map_err(|err| SdkError::Host(err.to_string())),
            Self::Http(host) => host.get_environment(environment_id).await,
        }
    }

    async fn close_environment(&self, environment_id: &str) -> Result<Environment> {
        match self {
            Self::InProcess(state) => state
                .close_environment(environment_id)
                .map_err(|err| SdkError::Host(err.to_string())),
            Self::Http(host) => host.close_environment(environment_id).await,
        }
    }

    async fn destroy_environment(&self, environment_id: &str) -> Result<Environment> {
        match self {
            Self::InProcess(state) => state
                .destroy_environment(environment_id)
                .map_err(|err| SdkError::Host(err.to_string())),
            Self::Http(host) => host.destroy_environment(environment_id).await,
        }
    }

    async fn close_session(&self, session_id: &str) -> Result<Session> {
        match self {
            Self::InProcess(state) => state
                .close_session(session_id)
                .map_err(|err| SdkError::Host(err.to_string())),
            Self::Http(host) => host.close_session(session_id).await,
        }
    }

    async fn export_workspace(&self, environment_id: &str) -> Result<WorkspaceArtifact> {
        match self {
            Self::InProcess(state) => state
                .export_workspace(environment_id)
                .map_err(|err| SdkError::Host(err.to_string())),
            Self::Http(host) => host.export_workspace(environment_id).await,
        }
    }
}

#[async_trait]
impl ToolHostClient for HostBackend {
    async fn execute(
        &self,
        request: ToolInvocationRequest,
    ) -> anyhow::Result<ToolInvocationResult> {
        match self {
            Self::InProcess(state) => Ok(state.execute_invocation(request)?),
            Self::Http(host) => host.execute(request).await,
        }
    }
}

#[derive(Debug)]
struct HttpHostBackend {
    base_url: Url,
    client: reqwest::Client,
}

impl HttpHostBackend {
    fn new(base_url: impl AsRef<str>) -> Result<Self> {
        Ok(Self {
            base_url: normalize_http_base_url(base_url.as_ref())
                .map_err(|err| SdkError::Config(format!("invalid host base url: {err}")))?,
            client: reqwest::Client::builder()
                .redirect(reqwest::redirect::Policy::none())
                .build()
                .map_err(|err| SdkError::Config(format!("invalid HTTP client config: {err}")))?,
        })
    }

    async fn create_environment(
        &self,
        request: CreateEnvironmentRequest,
    ) -> Result<CreateEnvironmentResponse> {
        self.post_json("environments", &request).await
    }

    async fn create_session(
        &self,
        environment_id: &str,
        request: CreateSessionRequest,
    ) -> Result<CreateSessionResponse> {
        validate_environment_id(environment_id)?;
        self.post_json(&format!("environments/{environment_id}/sessions"), &request)
            .await
    }

    async fn get_environment(&self, environment_id: &str) -> Result<Environment> {
        validate_environment_id(environment_id)?;
        self.get_json(&format!("environments/{environment_id}"))
            .await
    }

    async fn close_environment(&self, environment_id: &str) -> Result<Environment> {
        validate_environment_id(environment_id)?;
        self.post_json(
            &format!("environments/{environment_id}/close"),
            &Value::Null,
        )
        .await
    }

    async fn destroy_environment(&self, environment_id: &str) -> Result<Environment> {
        validate_environment_id(environment_id)?;
        let url = self
            .base_url
            .join(&format!("environments/{environment_id}"))
            .map_err(|err| SdkError::Config(format!("invalid environment destroy url: {err}")))?;
        let response = self
            .client
            .delete(url)
            .send()
            .await
            .map_err(|err| SdkError::Transport(err.to_string()))?;
        let status = response.status();
        if !status.is_success() {
            let text = capped_response_text(response, MAX_HTTP_ERROR_BODY_BYTES).await;
            return Err(SdkError::Host(format!("host returned {status}: {text}")));
        }
        read_capped_json_response::<Environment>(response, MAX_HTTP_JSON_BODY_BYTES)
            .await
            .map_err(|err| SdkError::Transport(err.to_string()))
    }

    async fn close_session(&self, session_id: &str) -> Result<Session> {
        validate_session_id(session_id)?;
        self.post_json(&format!("sessions/{session_id}/close"), &Value::Null)
            .await
    }

    #[cfg(test)]
    async fn destroy_session(&self, session_id: &str) -> Result<Session> {
        validate_session_id(session_id)?;
        let url = self
            .base_url
            .join(&format!("sessions/{session_id}"))
            .map_err(|err| SdkError::Config(format!("invalid session destroy url: {err}")))?;
        let response = self
            .client
            .delete(url)
            .send()
            .await
            .map_err(|err| SdkError::Transport(err.to_string()))?;
        let status = response.status();
        if !status.is_success() {
            let text = capped_response_text(response, MAX_HTTP_ERROR_BODY_BYTES).await;
            return Err(SdkError::Host(format!("host returned {status}: {text}")));
        }
        read_capped_json_response::<Session>(response, MAX_HTTP_JSON_BODY_BYTES)
            .await
            .map_err(|err| SdkError::Transport(err.to_string()))
    }

    async fn export_workspace(&self, environment_id: &str) -> Result<WorkspaceArtifact> {
        validate_environment_id(environment_id)?;
        self.post_json(
            &format!("environments/{environment_id}/artifacts/workspace"),
            &Value::Null,
        )
        .await
    }

    async fn execute(
        &self,
        request: ToolInvocationRequest,
    ) -> anyhow::Result<ToolInvocationResult> {
        validate_session_id(&request.session_id).map_err(|err| anyhow::anyhow!(err.to_string()))?;
        self.post_json_anyhow(
            &format!("sessions/{}/invocations", request.session_id),
            &request,
        )
        .await
    }

    async fn post_json<T, B>(&self, path: &str, body: &B) -> Result<T>
    where
        T: serde::de::DeserializeOwned,
        B: serde::Serialize + ?Sized,
    {
        self.post_json_anyhow(path, body)
            .await
            .map_err(|err| SdkError::Transport(err.to_string()))
    }

    async fn get_json<T>(&self, path: &str) -> Result<T>
    where
        T: serde::de::DeserializeOwned,
    {
        let url = self
            .base_url
            .join(path)
            .map_err(|err| SdkError::Config(format!("invalid host url: {err}")))?;
        let response = self
            .client
            .get(url)
            .send()
            .await
            .map_err(|err| SdkError::Transport(err.to_string()))?;
        let status = response.status();
        if !status.is_success() {
            let text = capped_response_text(response, MAX_HTTP_ERROR_BODY_BYTES).await;
            return Err(SdkError::Host(format!("host returned {status}: {text}")));
        }
        read_capped_json_response::<T>(response, MAX_HTTP_JSON_BODY_BYTES)
            .await
            .map_err(|err| SdkError::Transport(err.to_string()))
    }

    async fn post_json_anyhow<T, B>(&self, path: &str, body: &B) -> anyhow::Result<T>
    where
        T: serde::de::DeserializeOwned,
        B: serde::Serialize + ?Sized,
    {
        let url = self.base_url.join(path).context("invalid host url")?;
        let response = self.client.post(url).json(body).send().await?;
        let status = response.status();
        if !status.is_success() {
            let text = capped_response_text(response, MAX_HTTP_ERROR_BODY_BYTES).await;
            bail!("host returned {status}: {text}");
        }
        read_capped_json_response::<T>(response, MAX_HTTP_JSON_BODY_BYTES).await
    }
}

fn normalize_http_base_url(base_url: &str) -> anyhow::Result<Url> {
    if base_url.starts_with("http:///") || base_url.starts_with("https:///") {
        bail!("host is required");
    }
    let mut url = Url::parse(base_url)?;
    match url.scheme() {
        "http" | "https" => {}
        scheme => bail!("unsupported scheme: {scheme}"),
    }
    if url.host_str().is_none() {
        bail!("host is required");
    }
    if !url.username().is_empty() || url.password().is_some() {
        bail!("credentials are not allowed");
    }
    if url.query().is_some() || url.fragment().is_some() {
        bail!("query strings and fragments are not allowed");
    }
    if !url.path().ends_with('/') {
        let mut path = url.path().to_string();
        path.push('/');
        url.set_path(&path);
    }
    Ok(url)
}

async fn capped_response_text(mut response: reqwest::Response, max_bytes: usize) -> String {
    let mut bytes = Vec::new();
    let mut truncated = false;
    loop {
        match response.chunk().await {
            Ok(Some(chunk)) => {
                let remaining = max_bytes.saturating_sub(bytes.len());
                if chunk.len() > remaining {
                    bytes.extend_from_slice(&chunk[..remaining]);
                    truncated = true;
                    break;
                }
                bytes.extend_from_slice(&chunk);
            }
            Ok(None) => break,
            Err(err) => return format!("failed to read error body: {err}"),
        }
    }
    let mut text = String::from_utf8_lossy(&bytes).into_owned();
    if truncated {
        text.push_str("\n...[truncated]");
    }
    text
}

async fn read_capped_json_response<T>(
    response: reqwest::Response,
    max_bytes: usize,
) -> anyhow::Result<T>
where
    T: serde::de::DeserializeOwned,
{
    let bytes = capped_response_bytes(response, max_bytes).await?;
    Ok(serde_json::from_slice(&bytes)?)
}

async fn capped_response_bytes(
    mut response: reqwest::Response,
    max_bytes: usize,
) -> anyhow::Result<Vec<u8>> {
    let mut bytes = Vec::new();
    loop {
        match response.chunk().await {
            Ok(Some(chunk)) => {
                let remaining = max_bytes.saturating_sub(bytes.len());
                if chunk.len() > remaining {
                    bail!("response body exceeds maximum size of {max_bytes} bytes");
                }
                bytes.extend_from_slice(&chunk);
            }
            Ok(None) => return Ok(bytes),
            Err(err) => return Err(err.into()),
        }
    }
}

impl PolicyConfig {
    fn into_execution_policy(self) -> ExecutionPolicy {
        ExecutionPolicy {
            read_roots: self.read_roots,
            write_roots: self.write_roots,
            process: ProcessPolicy {
                allow_exec: self.allow_exec,
                allowed_commands: self.allowed_commands,
                denied_commands: vec![],
                max_processes: None,
            },
            network: NetworkPolicy {
                enabled: self.network_enabled,
                allow_hosts: vec![],
                deny_hosts: vec![],
            },
            env: executioner_core::EnvPolicy {
                allowlist: self.env_allowlist,
                denylist: self.env_denylist,
                injected: self.env_injected,
            },
            max_duration_ms: self.max_duration_ms,
            max_output_bytes: self.max_output_bytes,
        }
    }
}

impl WorkspaceConfig {
    fn into_spec(self) -> Result<WorkspaceSpec> {
        match self {
            Self::New => Ok(WorkspaceSpec {
                mode: WorkspaceMode::New,
                root: None,
                snapshot_ref: None,
                template_ref: None,
                mount_as_workspace: true,
            }),
            Self::Existing { root } => {
                if !root.is_absolute() {
                    return Err(SdkError::Config(
                        "workspace.root must be absolute for existing sessions".to_string(),
                    ));
                }
                validate_workspace_root_has_no_symlink_parent(&root)?;
                if root
                    .symlink_metadata()
                    .map(|metadata| metadata.file_type().is_symlink())
                    .unwrap_or(false)
                {
                    return Err(SdkError::Config(
                        "workspace.root must not be a symlink".to_string(),
                    ));
                }
                Ok(WorkspaceSpec {
                    mode: WorkspaceMode::Existing,
                    root: Some(root.to_string_lossy().into_owned()),
                    snapshot_ref: None,
                    template_ref: None,
                    mount_as_workspace: true,
                })
            }
        }
    }
}

impl From<Session> for SessionInfo {
    fn from(session: Session) -> Self {
        Self {
            id: session.id,
            state: session.state.into(),
            workspace: WorkspaceInfo {
                root: session.workspace.root,
                logical_root: session.workspace.logical_root,
                mode: session.workspace.mode.into(),
                fresh: session.workspace.fresh,
                managed: session.workspace.managed,
            },
            created_at: session.created_at,
            expires_at: session.expires_at,
            metadata: session.metadata,
        }
    }
}

impl From<Environment> for EnvironmentInfo {
    fn from(environment: Environment) -> Self {
        Self {
            id: environment.id,
            state: environment.state.into(),
            workspace: WorkspaceInfo {
                root: environment.workspace.root,
                logical_root: environment.workspace.logical_root,
                mode: environment.workspace.mode.into(),
                fresh: environment.workspace.fresh,
                managed: environment.workspace.managed,
            },
            created_at: environment.created_at,
            expires_at: environment.expires_at,
            metadata: environment.metadata,
            revision: environment.revision,
        }
    }
}

impl From<SessionState> for SessionStatus {
    fn from(state: SessionState) -> Self {
        match state {
            SessionState::Starting => Self::Starting,
            SessionState::Ready => Self::Ready,
            SessionState::Closing => Self::Closing,
            SessionState::Closed => Self::Closed,
            SessionState::Destroyed => Self::Destroyed,
            SessionState::Failed => Self::Failed,
        }
    }
}

impl From<EnvironmentState> for EnvironmentStatus {
    fn from(state: EnvironmentState) -> Self {
        match state {
            EnvironmentState::Starting => Self::Starting,
            EnvironmentState::Ready => Self::Ready,
            EnvironmentState::Closing => Self::Closing,
            EnvironmentState::Closed => Self::Closed,
            EnvironmentState::Destroyed => Self::Destroyed,
            EnvironmentState::Failed => Self::Failed,
        }
    }
}

impl From<WorkspaceMode> for WorkspaceKind {
    fn from(mode: WorkspaceMode) -> Self {
        match mode {
            WorkspaceMode::New => Self::New,
            WorkspaceMode::Existing => Self::Existing,
            WorkspaceMode::Snapshot => Self::Snapshot,
            WorkspaceMode::Template => Self::Template,
        }
    }
}

impl From<ToolInvocationResult> for SubmitResult {
    fn from(result: ToolInvocationResult) -> Self {
        Self {
            invocation_id: result.invocation_id,
            tool_name: result.tool_name,
            status: result.status.into(),
            output: result.output,
            error: result.error,
            summary: result.summary,
            effects: result.effects.into_iter().map(Into::into).collect(),
            duration_ms: result.duration_ms,
            metadata: result.metadata,
        }
    }
}

impl From<ToolResultStatus> for ToolStatus {
    fn from(status: ToolResultStatus) -> Self {
        match status {
            ToolResultStatus::Success => Self::Success,
            ToolResultStatus::Error => Self::Error,
            ToolResultStatus::Timeout => Self::Timeout,
            ToolResultStatus::Cancelled => Self::Cancelled,
            ToolResultStatus::PolicyDenied => Self::PolicyDenied,
        }
    }
}

impl From<executioner_core::Effect> for StateEffect {
    fn from(effect: executioner_core::Effect) -> Self {
        Self {
            id: effect.id,
            invocation_id: effect.invocation_id,
            kind: effect.kind,
            resource_type: effect.resource.resource_type,
            uri: effect.resource.uri,
            operation: effect.operation.into(),
            summary: effect.summary,
            reversible: effect.reversible,
            occurred_at: effect.occurred_at,
        }
    }
}

impl From<EffectOperation> for EffectKind {
    fn from(operation: EffectOperation) -> Self {
        match operation {
            EffectOperation::Read => Self::Read,
            EffectOperation::Create => Self::Create,
            EffectOperation::Update => Self::Update,
            EffectOperation::Delete => Self::Delete,
            EffectOperation::Execute => Self::Execute,
        }
    }
}

pub fn json_object(value: Value) -> Result<Map<String, Value>> {
    value
        .as_object()
        .cloned()
        .ok_or(SdkError::ExpectedJsonObject)
}

fn parse_list_files_result(result: &SubmitResult) -> Result<Vec<String>> {
    if result.status != ToolStatus::Success {
        return Err(SdkError::ToolUnsuccessful {
            tool_name: result.tool_name.clone(),
            status: result.status,
            message: result
                .error
                .clone()
                .unwrap_or_else(|| result.output.clone()),
        });
    }
    let truncated = result.metadata.get("truncated");
    if truncated.is_some_and(|value| !value.is_boolean()) {
        return Err(SdkError::Config(
            "List truncated metadata must be a boolean".into(),
        ));
    }
    if truncated.and_then(|value| value.as_bool()).unwrap_or(false) {
        return Err(SdkError::Config(
            "List result was truncated; refusing partial directory listing".into(),
        ));
    }
    if let Some(entries_value) = result.metadata.get("entries") {
        let Some(entries) = entries_value.as_array() else {
            return Err(SdkError::Config(
                "List metadata entries must be an array".into(),
            ));
        };
        return entries
            .iter()
            .map(|entry| {
                entry
                    .as_str()
                    .map(str::to_string)
                    .ok_or_else(|| SdkError::Config("List metadata entries must be strings".into()))
            })
            .collect();
    }
    parse_list_files_output(&result.output)
}

fn parse_list_files_output(output: &str) -> Result<Vec<String>> {
    if output.lines().any(|line| line.starts_with("...[truncated")) {
        return Err(SdkError::Config(
            "List result was truncated; refusing partial directory listing".into(),
        ));
    }
    Ok(output
        .lines()
        .filter(|line| !line.starts_with("...[truncated"))
        .map(str::to_string)
        .collect())
}

fn validate_session_id(session_id: &str) -> Result<()> {
    validate_identifier("session id", session_id)
}

fn validate_environment_id(environment_id: &str) -> Result<()> {
    validate_identifier("environment id", environment_id)
}

fn validate_worker_id(worker_id: &str) -> Result<()> {
    validate_identifier("worker id", worker_id)
}

fn validate_worker_config(worker: &WorkerConfig) -> Result<()> {
    match worker {
        WorkerConfig::InProcess { id, .. } | WorkerConfig::Managed { id, .. } => {
            validate_worker_id(id)
        }
        WorkerConfig::External => Ok(()),
    }
}

fn validate_environment_config(config: &EnvironmentConfig) -> Result<()> {
    validate_worker_config(&config.worker)?;
    validate_submit_timeout(config.submit_timeout)?;
    validate_policy_config(&config.policy)?;
    validate_lifecycle_config(&config.lifecycle)?;
    Ok(())
}

fn validate_workspace_root_has_no_symlink_parent(root: &Path) -> Result<()> {
    let Some(parent) = root.parent() else {
        return Ok(());
    };
    validate_path_parent_has_no_symlinks(parent, "workspace.root parent")
}

fn validate_path_parent_has_no_symlinks(parent: &Path, label: &str) -> Result<()> {
    let mut current = if parent.is_absolute() {
        parent.to_path_buf()
    } else {
        std::env::current_dir().map_err(SdkError::Io)?.join(parent)
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
                return Err(SdkError::Config(format!(
                    "{label} must not contain symlinks"
                )));
            }
            Ok(_) => {}
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => return Err(SdkError::Io(err)),
        }
        if !current.pop() {
            return Ok(());
        }
    }
}

fn is_platform_root_symlink(path: &Path) -> bool {
    matches!(path.to_str(), Some("/var" | "/tmp" | "/etc"))
}

fn validate_policy_config(policy: &PolicyConfig) -> Result<()> {
    validate_policy_roots("policy.readRoots", &policy.read_roots)?;
    validate_policy_roots("policy.writeRoots", &policy.write_roots)?;
    if policy.network_enabled {
        return Err(SdkError::Config(
            "network policy is not enforceable yet; leave network disabled".to_string(),
        ));
    }
    if let Some(max_duration_ms) = policy.max_duration_ms {
        if max_duration_ms == 0 {
            return Err(SdkError::Config(
                "policy.maxDurationMs must be positive".to_string(),
            ));
        }
        if max_duration_ms > MAX_TOOL_TIMEOUT_MS {
            return Err(SdkError::Config(format!(
                "policy.maxDurationMs exceeds maximum supported tool timeout of {MAX_TOOL_TIMEOUT_MS}ms"
            )));
        }
    }
    if let Some(max_output_bytes) = policy.max_output_bytes {
        if max_output_bytes > MAX_OUTPUT_BYTES {
            return Err(SdkError::Config(format!(
                "policy.maxOutputBytes exceeds maximum supported output size of {MAX_OUTPUT_BYTES} bytes"
            )));
        }
    }
    Ok(())
}

fn validate_tool_call_limits(call: &ToolCall) -> Result<()> {
    if let Some(timeout_ms) = call.timeout_ms {
        if timeout_ms == 0 {
            return Err(SdkError::Config("timeoutMs must be positive".to_string()));
        }
        if timeout_ms > MAX_TOOL_TIMEOUT_MS {
            return Err(SdkError::Config(format!(
                "timeoutMs exceeds maximum supported tool timeout of {MAX_TOOL_TIMEOUT_MS}ms"
            )));
        }
    }
    if let Some(max_output_bytes) = call.max_output_bytes {
        if max_output_bytes > MAX_OUTPUT_BYTES {
            return Err(SdkError::Config(format!(
                "maxOutputBytes exceeds maximum supported output size of {MAX_OUTPUT_BYTES} bytes"
            )));
        }
    }
    Ok(())
}

fn validate_policy_roots(label: &str, roots: &[String]) -> Result<()> {
    for root in roots {
        let trimmed = root.trim_end_matches('/');
        if trimmed.is_empty()
            || !(trimmed == "/workspace" || trimmed.starts_with("/workspace/"))
            || trimmed.contains('\0')
            || trimmed
                .split('/')
                .any(|component| component == "." || component == "..")
        {
            return Err(SdkError::Config(format!(
                "{label} entries must be /workspace logical roots without . or .. components"
            )));
        }
    }
    Ok(())
}

fn validate_lifecycle_config(lifecycle: &LifecycleConfig) -> Result<()> {
    if let Some(ttl_ms) = lifecycle.ttl_ms {
        if ttl_ms > MAX_ENVIRONMENT_TTL_MS {
            return Err(SdkError::Config(format!(
                "ttlMs exceeds maximum supported environment TTL of {MAX_ENVIRONMENT_TTL_MS}ms"
            )));
        }
    }
    Ok(())
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
        Err(SdkError::Config(format!("invalid {label}: {value}")))
    }
}

fn validate_tool_name(tool_name: &str) -> Result<()> {
    if tool_name.is_empty() {
        return Err(SdkError::Config(
            "toolName must be a non-empty string".to_string(),
        ));
    }
    Ok(())
}

fn validate_serialized_request_size<T: Serialize>(label: &str, value: &T) -> Result<()> {
    let size = serde_json::to_vec(value)
        .map_err(|err| SdkError::Config(format!("failed to serialize {label}: {err}")))?
        .len();
    if size > MAX_REQUEST_JSON_BYTES {
        return Err(SdkError::Config(format!(
            "{label} exceeds maximum JSON size of {MAX_REQUEST_JSON_BYTES} bytes"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::fs;

    #[test]
    fn environment_builder_clamps_zero_worker_idle_sleep() {
        let config = ExecutionerEnvironment::builder()
            .file_backend("queue")
            .in_process_host("state")
            .managed_worker_with_sleep("worker", Duration::ZERO)
            .new_workspace()
            .build()
            .unwrap();

        match config.worker {
            WorkerConfig::Managed { idle_sleep, .. } => {
                assert_eq!(idle_sleep, Duration::from_millis(1));
            }
            other => panic!("expected managed worker config, got {other:?}"),
        }
    }

    #[test]
    fn worker_runtime_builder_clamps_zero_idle_sleep() {
        let config = ExecutionerWorker::builder()
            .file_backend("queue")
            .http_host("http://127.0.0.1:1/")
            .idle_sleep(Duration::ZERO)
            .build()
            .unwrap();

        assert_eq!(config.idle_sleep, Duration::from_millis(1));
    }

    #[test]
    fn environment_builder_rejects_zero_submit_timeout() {
        let err = ExecutionerEnvironment::builder()
            .file_backend("queue")
            .in_process_host("state")
            .new_workspace()
            .submit_timeout(Duration::ZERO)
            .build()
            .unwrap_err();

        assert!(err.to_string().contains("submit timeout must be positive"));
    }

    #[test]
    fn environment_builder_rejects_invalid_worker_id() {
        let err = ExecutionerEnvironment::builder()
            .file_backend("queue")
            .in_process_host("state")
            .managed_worker("../escaped")
            .new_workspace()
            .build()
            .unwrap_err();

        assert!(err.to_string().contains("invalid worker id"));
    }

    #[test]
    fn worker_runtime_builder_rejects_invalid_worker_id() {
        let err = ExecutionerWorker::builder()
            .file_backend("queue")
            .http_host("http://127.0.0.1:1/")
            .id("../escaped")
            .build()
            .unwrap_err();

        assert!(err.to_string().contains("invalid worker id"));
    }

    #[tokio::test]
    async fn environment_create_rejects_direct_config_invalid_worker_id_without_side_effects() {
        let temp = tempfile::TempDir::new().unwrap();
        let queue = temp.path().join("queue");
        let state = temp.path().join("state");
        let mut config = EnvironmentConfig::local_file(&queue, &state);
        config.worker = WorkerConfig::Managed {
            id: "../escaped".to_string(),
            idle_sleep: Duration::from_millis(10),
        };

        let err = ExecutionerEnvironment::create(config).await.unwrap_err();

        assert!(err.to_string().contains("invalid worker id"));
        assert!(!queue.exists());
        assert!(!state.exists());
    }

    #[tokio::test]
    async fn environment_create_rejects_direct_config_zero_submit_timeout_without_side_effects() {
        let temp = tempfile::TempDir::new().unwrap();
        let queue = temp.path().join("queue");
        let state = temp.path().join("state");
        let mut config = EnvironmentConfig::local_file(&queue, &state);
        config.submit_timeout = Duration::ZERO;

        let err = ExecutionerEnvironment::create(config).await.unwrap_err();

        assert!(err.to_string().contains("submit timeout must be positive"));
        assert!(!queue.exists());
        assert!(!state.exists());
    }

    #[tokio::test]
    async fn environment_create_rejects_excessive_policy_limits_without_side_effects() {
        for (label, policy) in [
            (
                "policy.maxOutputBytes",
                PolicyConfig {
                    max_output_bytes: Some(MAX_OUTPUT_BYTES + 1),
                    ..PolicyConfig::default()
                },
            ),
            (
                "policy.maxDurationMs",
                PolicyConfig {
                    max_duration_ms: Some(MAX_TOOL_TIMEOUT_MS + 1),
                    ..PolicyConfig::default()
                },
            ),
        ] {
            let temp = tempfile::TempDir::new().unwrap();
            let queue = temp.path().join("queue");
            let state = temp.path().join("state");
            let mut config = EnvironmentConfig::local_file(&queue, &state);
            config.policy = policy;

            let err = ExecutionerEnvironment::create(config).await.unwrap_err();

            assert!(err.to_string().contains(label), "{label}: {err}");
            assert!(!queue.exists(), "{label}");
            assert!(!state.exists(), "{label}");
        }
    }

    #[tokio::test]
    async fn environment_create_rejects_zero_policy_duration_without_side_effects() {
        let temp = tempfile::TempDir::new().unwrap();
        let queue = temp.path().join("queue");
        let state = temp.path().join("state");
        let mut config = EnvironmentConfig::local_file(&queue, &state);
        config.policy = PolicyConfig {
            max_duration_ms: Some(0),
            ..PolicyConfig::default()
        };

        let err = ExecutionerEnvironment::create(config).await.unwrap_err();

        assert!(err
            .to_string()
            .contains("policy.maxDurationMs must be positive"));
        assert!(!queue.exists());
        assert!(!state.exists());
    }

    #[tokio::test]
    async fn environment_create_rejects_invalid_policy_roots_without_side_effects() {
        for (label, policy) in [
            (
                "policy.readRoots",
                PolicyConfig {
                    read_roots: vec!["/workspace/../outside".to_string()],
                    ..PolicyConfig::default()
                },
            ),
            (
                "policy.writeRoots",
                PolicyConfig {
                    write_roots: vec!["/workspace/.".to_string()],
                    ..PolicyConfig::default()
                },
            ),
        ] {
            let temp = tempfile::TempDir::new().unwrap();
            let queue = temp.path().join("queue");
            let state = temp.path().join("state");
            let mut config = EnvironmentConfig::local_file(&queue, &state);
            config.policy = policy;

            let err = ExecutionerEnvironment::create(config).await.unwrap_err();

            assert!(err.to_string().contains(label), "{label}: {err}");
            assert!(!queue.exists(), "{label}");
            assert!(!state.exists(), "{label}");
        }
    }

    #[tokio::test]
    async fn environment_create_rejects_network_policy_without_side_effects() {
        let temp = tempfile::TempDir::new().unwrap();
        let queue = temp.path().join("queue");
        let state = temp.path().join("state");
        let mut config = EnvironmentConfig::local_file(&queue, &state);
        config.policy = PolicyConfig {
            network_enabled: true,
            ..PolicyConfig::default()
        };

        let err = ExecutionerEnvironment::create(config).await.unwrap_err();

        assert!(err.to_string().contains("network policy"));
        assert!(!queue.exists());
        assert!(!state.exists());
    }

    #[tokio::test]
    async fn environment_create_rejects_excessive_ttl_without_side_effects() {
        let temp = tempfile::TempDir::new().unwrap();
        let queue = temp.path().join("queue");
        let state = temp.path().join("state");
        let mut config = EnvironmentConfig::local_file(&queue, &state);
        config.lifecycle.ttl_ms = Some(MAX_ENVIRONMENT_TTL_MS + 1);

        let err = ExecutionerEnvironment::create(config).await.unwrap_err();

        assert!(err.to_string().contains("ttlMs exceeds maximum"));
        assert!(!queue.exists());
        assert!(!state.exists());
    }

    #[test]
    fn worker_start_rejects_direct_config_invalid_worker_id_without_side_effects() {
        let temp = tempfile::TempDir::new().unwrap();
        let queue = temp.path().join("queue");
        let config = WorkerRuntimeConfig {
            backend: BackendConfig::File {
                queue_dir: queue.clone(),
            },
            host: HostConfig::ConnectHttp {
                base_url: "http://127.0.0.1:1/".to_string(),
            },
            id: "../escaped".to_string(),
            idle_sleep: Duration::from_millis(10),
        };

        let err = ExecutionerWorker::start(config).unwrap_err();

        assert!(err.to_string().contains("invalid worker id"));
        assert!(!queue.exists());
    }

    #[tokio::test]
    async fn local_file_environment_writes_and_reads() {
        let temp = tempfile::TempDir::new().unwrap();
        let env = ExecutionerEnvironment::create(EnvironmentConfig::local_file(
            temp.path().join("queue"),
            temp.path().join("state"),
        ))
        .await
        .unwrap();
        let session = env.create_session().await.unwrap();

        let write = session
            .submit(
                ToolCall::json(
                    "Write",
                    json!({ "path": "hello.txt", "content": "hello from sdk" }),
                )
                .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(write.status, ToolStatus::Success);
        assert_eq!(write.effects.len(), 1);

        let read = session
            .submit(ToolCall::json("Read", json!({ "path": "hello.txt" })).unwrap())
            .await
            .unwrap();

        assert_eq!(read.output, "hello from sdk");
        let workspace_root = PathBuf::from(&session.session().workspace.root);
        assert!(workspace_root.exists());

        let closed = env.close().await.unwrap();
        assert_eq!(closed.state, EnvironmentStatus::Destroyed);
        assert!(!workspace_root.exists());
    }

    #[tokio::test]
    async fn builder_constructs_local_file_environment() {
        let temp = tempfile::TempDir::new().unwrap();
        let config = ExecutionerEnvironment::builder()
            .file_backend(temp.path().join("queue"))
            .in_process_host(temp.path().join("state"))
            .in_process_worker("worker")
            .new_workspace()
            .submit_timeout(Duration::from_secs(5))
            .build()
            .unwrap();

        let env = ExecutionerEnvironment::create(config).await.unwrap();
        let session = env.create_session().await.unwrap();
        assert_eq!(session.session().state, SessionStatus::Ready);
        env.close().await.unwrap();
    }

    #[test]
    fn policy_config_preserves_allowed_commands() {
        let policy = PolicyConfig::default()
            .allow_exec(true)
            .allowed_commands(["printf hello"])
            .env_allowlist(["PATH"])
            .env_denylist(["SECRET"])
            .inject_env("SUBSTRATE_VALUE", "ok")
            .into_execution_policy();

        assert!(policy.process.allow_exec);
        assert_eq!(policy.process.allowed_commands, vec!["printf hello"]);
        assert_eq!(policy.env.allowlist, vec!["PATH"]);
        assert_eq!(policy.env.denylist, vec!["SECRET"]);
        assert_eq!(policy.env.injected["SUBSTRATE_VALUE"], "ok");
    }

    #[tokio::test]
    async fn http_backend_rejects_invalid_session_id_before_building_url() {
        let backend = HttpHostBackend::new("http://127.0.0.1:9/").unwrap();

        let close = backend.close_session("../escaped").await.unwrap_err();
        let destroy = backend.destroy_session("../escaped").await.unwrap_err();
        let export = backend.export_workspace("../escaped").await.unwrap_err();
        let execute = backend
            .execute(ToolInvocationRequest {
                invocation_id: Some("inv".to_string()),
                session_id: "../escaped".to_string(),
                tool_name: "Read".to_string(),
                arguments: Map::new(),
                cwd: None,
                timeout_ms: None,
                max_output_bytes: None,
                idempotency_key: None,
                required_capabilities: vec![],
                metadata: Map::new(),
            })
            .await
            .unwrap_err();

        assert!(close.to_string().contains("invalid session id"));
        assert!(destroy.to_string().contains("invalid session id"));
        assert!(export.to_string().contains("invalid environment id"));
        assert!(execute.to_string().contains("invalid session id"));
    }

    #[test]
    fn http_backend_rejects_unsafe_base_urls() {
        for base_url in [
            "file:///tmp/executioner",
            "http:///tmp/executioner",
            "http://user:pass@127.0.0.1:9/",
            "http://127.0.0.1:9/?token=secret",
            "http://127.0.0.1:9/#fragment",
        ] {
            let err = HttpHostBackend::new(base_url).unwrap_err();
            assert!(
                err.to_string().contains("invalid host base url"),
                "unexpected error for {base_url}: {err}"
            );
        }
    }

    #[tokio::test]
    async fn http_backend_preserves_base_url_path_prefix_without_trailing_slash() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut buffer = [0_u8; 4096];
            let bytes = socket.read(&mut buffer).await.unwrap();
            let request = String::from_utf8_lossy(&buffer[..bytes]);
            assert!(
                request.starts_with("POST /api/environments HTTP/1.1"),
                "{request}"
            );
            let response = "HTTP/1.1 500 Internal Server Error\r\ncontent-length: 0\r\nconnection: close\r\n\r\n";
            socket.write_all(response.as_bytes()).await.unwrap();
        });
        let backend = HttpHostBackend::new(format!("http://{addr}/api")).unwrap();

        let _ = backend
            .create_environment(CreateEnvironmentRequest {
                environment_id: None,
                workspace: WorkspaceSpec {
                    mode: WorkspaceMode::New,
                    root: None,
                    snapshot_ref: None,
                    template_ref: None,
                    mount_as_workspace: true,
                },
                policy: PolicyConfig::default().into_execution_policy(),
                ttl_ms: None,
                metadata: Map::new(),
            })
            .await
            .unwrap_err();

        server.await.unwrap();
    }

    #[tokio::test]
    async fn http_backend_caps_error_response_body() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut buffer = [0_u8; 2048];
            let _ = socket.read(&mut buffer).await.unwrap();
            let body = "x".repeat(256 * 1024);
            let response = format!(
                "HTTP/1.1 500 Internal Server Error\r\ncontent-type: text/plain\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            let _ = socket.write_all(response.as_bytes()).await;
        });
        let backend = HttpHostBackend::new(format!("http://{addr}/")).unwrap();

        let err = backend
            .create_environment(CreateEnvironmentRequest {
                environment_id: None,
                workspace: WorkspaceSpec {
                    mode: WorkspaceMode::New,
                    root: None,
                    snapshot_ref: None,
                    template_ref: None,
                    mount_as_workspace: true,
                },
                policy: PolicyConfig::default().into_execution_policy(),
                ttl_ms: None,
                metadata: Map::new(),
            })
            .await
            .unwrap_err();

        let message = err.to_string();
        assert!(message.len() < 80 * 1024);
        assert!(message.contains("truncated"));
        server.await.unwrap();
    }

    #[tokio::test]
    async fn http_backend_does_not_follow_redirects_with_request_body() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::time::{timeout, Duration};

        let redirect_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let redirect_addr = redirect_listener.local_addr().unwrap();
        let capture_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let capture_addr = capture_listener.local_addr().unwrap();
        let redirect_server = tokio::spawn(async move {
            let (mut socket, _) = redirect_listener.accept().await.unwrap();
            let mut buffer = [0_u8; 2048];
            let _ = socket.read(&mut buffer).await.unwrap();
            let response = format!(
                "HTTP/1.1 307 Temporary Redirect\r\nlocation: http://{capture_addr}/capture\r\ncontent-length: 0\r\nconnection: close\r\n\r\n"
            );
            socket.write_all(response.as_bytes()).await.unwrap();
        });
        let capture_server = tokio::spawn(async move {
            match timeout(Duration::from_millis(500), capture_listener.accept()).await {
                Ok(Ok((mut socket, _))) => {
                    let mut buffer = [0_u8; 2048];
                    let _ = socket.read(&mut buffer).await.unwrap();
                    let response = "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: 2\r\nconnection: close\r\n\r\n{}";
                    socket.write_all(response.as_bytes()).await.unwrap();
                    true
                }
                Ok(Err(err)) => panic!("capture listener failed: {err}"),
                Err(_) => false,
            }
        });
        let backend = HttpHostBackend::new(format!("http://{redirect_addr}/")).unwrap();

        let err = backend
            .create_environment(CreateEnvironmentRequest {
                environment_id: Some("env_redirect".to_string()),
                workspace: WorkspaceSpec {
                    mode: WorkspaceMode::New,
                    root: None,
                    snapshot_ref: None,
                    template_ref: None,
                    mount_as_workspace: true,
                },
                policy: PolicyConfig::default().into_execution_policy(),
                ttl_ms: None,
                metadata: Map::new(),
            })
            .await
            .unwrap_err();

        redirect_server.await.unwrap();
        let captured = capture_server.await.unwrap();
        assert!(err.to_string().contains("307"));
        assert!(
            !captured,
            "redirect target received the create-environment body"
        );
    }

    #[tokio::test]
    async fn http_backend_rejects_oversized_success_response_body() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut buffer = [0_u8; 2048];
            let _ = socket.read(&mut buffer).await.unwrap();
            let body = format!(r#"{{"padding":"{}"}}"#, "x".repeat(11 * 1024 * 1024));
            let response = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            socket.write_all(response.as_bytes()).await.unwrap();
        });
        let backend = HttpHostBackend::new(format!("http://{addr}/")).unwrap();

        let err = backend
            .create_environment(CreateEnvironmentRequest {
                environment_id: None,
                workspace: WorkspaceSpec {
                    mode: WorkspaceMode::New,
                    root: None,
                    snapshot_ref: None,
                    template_ref: None,
                    mount_as_workspace: true,
                },
                policy: PolicyConfig::default().into_execution_policy(),
                ttl_ms: None,
                metadata: Map::new(),
            })
            .await
            .unwrap_err();

        assert!(err.to_string().contains("response body exceeds"));
        server.await.unwrap();
    }

    #[tokio::test]
    async fn environment_create_rejects_invalid_returned_environment_id() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut buffer = [0_u8; 2048];
            let _ = socket.read(&mut buffer).await.unwrap();
            let body = serde_json::json!({
                "environment": {
                    "id": "../escaped",
                    "state": "ready",
                    "workspace": {
                        "root": "/tmp/workspace",
                        "logicalRoot": "/workspace",
                        "mode": "new",
                        "fresh": true,
                        "managed": true
                    },
                    "policy": {
                        "readRoots": ["/workspace"],
                        "writeRoots": ["/workspace"],
                        "process": {
                            "allowExec": false,
                            "allowedCommands": [],
                            "deniedCommands": [],
                            "maxProcesses": null
                        },
                        "network": {
                            "enabled": false,
                            "allowHosts": [],
                            "denyHosts": []
                        },
                        "env": {
                            "allowlist": [],
                            "denylist": [],
                            "injected": {}
                        },
                        "maxDurationMs": 300000,
                        "maxOutputBytes": 100000
                    },
                    "metadata": {},
                    "createdAt": "now",
                    "revision": 0
                }
            })
            .to_string();
            let response = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            socket.write_all(response.as_bytes()).await.unwrap();
        });
        let temp = tempfile::TempDir::new().unwrap();

        let err = ExecutionerEnvironment::create(
            ExecutionerEnvironment::builder()
                .file_backend(temp.path().join("queue"))
                .http_host(format!("http://{addr}/"))
                .external_worker()
                .new_workspace()
                .build()
                .unwrap(),
        )
        .await
        .unwrap_err();

        assert!(err.to_string().contains("invalid environment id"));
        server.await.unwrap();
    }

    #[tokio::test]
    async fn environment_exports_workspace_artifact() {
        let temp = tempfile::TempDir::new().unwrap();
        let env = ExecutionerEnvironment::create(EnvironmentConfig::local_file(
            temp.path().join("queue"),
            temp.path().join("state"),
        ))
        .await
        .unwrap();
        let session = env.create_session().await.unwrap();

        session
            .submit(
                ToolCall::json(
                    "Write",
                    json!({ "path": "artifact.txt", "content": "hello" }),
                )
                .unwrap(),
            )
            .await
            .unwrap();

        let artifact = env.export_workspace().await.unwrap();

        assert_eq!(artifact.file_count, 1);
        assert!(artifact
            .entries
            .iter()
            .any(|entry| entry.logical_path == "/workspace/artifact.txt"));
        assert!(
            std::path::Path::new(artifact.artifact.uri.strip_prefix("file://").unwrap()).exists()
        );
        env.close().await.unwrap();
    }

    #[tokio::test]
    async fn environment_materializes_exported_workspace_artifact() {
        let temp = tempfile::TempDir::new().unwrap();
        let env = ExecutionerEnvironment::create(EnvironmentConfig::local_file(
            temp.path().join("queue"),
            temp.path().join("state"),
        ))
        .await
        .unwrap();
        let session = env.create_session().await.unwrap();

        session
            .submit(
                ToolCall::json(
                    "Write",
                    json!({ "path": "restore.txt", "content": "restored" }),
                )
                .unwrap(),
            )
            .await
            .unwrap();

        let artifact = env.export_workspace().await.unwrap();
        let destination = temp.path().join("restored-workspace");

        env.materialize_workspace_artifact(&artifact, &destination)
            .unwrap();

        assert_eq!(
            fs::read_to_string(destination.join("restore.txt")).unwrap(),
            "restored"
        );
        env.close().await.unwrap();
    }

    #[tokio::test]
    async fn environment_lists_files_at_cwd() {
        let temp = tempfile::TempDir::new().unwrap();
        let env = ExecutionerEnvironment::create(EnvironmentConfig::local_file(
            temp.path().join("queue"),
            temp.path().join("state"),
        ))
        .await
        .unwrap();
        let session = env.create_session().await.unwrap();

        session
            .submit(ToolCall::json("Write", json!({ "path": "a.txt", "content": "a" })).unwrap())
            .await
            .unwrap();
        session
            .submit(
                ToolCall::json("Write", json!({ "path": "dir/b.txt", "content": "b" })).unwrap(),
            )
            .await
            .unwrap();

        let root_files = session.list("/workspace").await.unwrap();
        let dir_files = session.list_files("/workspace/dir").await.unwrap();

        assert_eq!(root_files, vec!["a.txt".to_string(), "dir/".to_string()]);
        assert_eq!(dir_files, vec!["b.txt".to_string()]);
        env.close().await.unwrap();
    }

    #[tokio::test]
    async fn environment_list_files_preserves_newline_filenames() {
        let temp = tempfile::TempDir::new().unwrap();
        let env = ExecutionerEnvironment::create(EnvironmentConfig::local_file(
            temp.path().join("queue"),
            temp.path().join("state"),
        ))
        .await
        .unwrap();
        let session = env.create_session().await.unwrap();

        session
            .submit(
                ToolCall::json(
                    "Write",
                    json!({ "path": "line\nbreak.txt", "content": "weird but legal" }),
                )
                .unwrap(),
            )
            .await
            .unwrap();

        let files = session.list_files("/workspace").await.unwrap();

        assert_eq!(files, vec!["line\nbreak.txt".to_string()]);
        env.close().await.unwrap();
    }

    #[test]
    fn list_files_parser_rejects_malformed_structured_entries() {
        let result = SubmitResult {
            invocation_id: "inv".to_string(),
            tool_name: "List".to_string(),
            status: ToolStatus::Success,
            output: "fallback.txt".to_string(),
            error: None,
            summary: None,
            effects: vec![],
            duration_ms: 0,
            metadata: Map::from_iter([(
                "entries".to_string(),
                json!(["visible.txt", 42, "hidden.txt"]),
            )]),
        };

        let err = parse_list_files_result(&result).unwrap_err();

        assert!(err.to_string().contains("entries must be strings"));
    }

    #[test]
    fn list_files_parser_rejects_non_array_structured_entries() {
        let result = SubmitResult {
            invocation_id: "inv".to_string(),
            tool_name: "List".to_string(),
            status: ToolStatus::Success,
            output: "fallback.txt".to_string(),
            error: None,
            summary: None,
            effects: vec![],
            duration_ms: 0,
            metadata: Map::from_iter([("entries".to_string(), json!("visible.txt"))]),
        };

        let err = parse_list_files_result(&result).unwrap_err();

        assert!(err.to_string().contains("entries must be an array"));
    }

    #[test]
    fn list_files_parser_rejects_truncated_structured_entries() {
        let result = SubmitResult {
            invocation_id: "inv".to_string(),
            tool_name: "List".to_string(),
            status: ToolStatus::Success,
            output: "visible.txt\n...[truncated at 1000 entries, 1005 total]".to_string(),
            error: None,
            summary: None,
            effects: vec![],
            duration_ms: 0,
            metadata: Map::from_iter([
                ("entries".to_string(), json!(["visible.txt"])),
                ("truncated".to_string(), json!(true)),
            ]),
        };

        let err = parse_list_files_result(&result).unwrap_err();

        assert!(err.to_string().contains("truncated"));
    }

    #[test]
    fn list_files_parser_rejects_malformed_truncated_metadata() {
        let result = SubmitResult {
            invocation_id: "inv".to_string(),
            tool_name: "List".to_string(),
            status: ToolStatus::Success,
            output: "visible.txt".to_string(),
            error: None,
            summary: None,
            effects: vec![],
            duration_ms: 0,
            metadata: Map::from_iter([
                ("entries".to_string(), json!(["visible.txt"])),
                ("truncated".to_string(), json!("true")),
            ]),
        };

        let err = parse_list_files_result(&result).unwrap_err();

        assert!(err
            .to_string()
            .contains("truncated metadata must be a boolean"));
    }

    #[test]
    fn list_files_parser_rejects_truncated_output_fallback() {
        let result = SubmitResult {
            invocation_id: "inv".to_string(),
            tool_name: "List".to_string(),
            status: ToolStatus::Success,
            output: "visible.txt\n...[truncated at 1000 entries, 1005 total]".to_string(),
            error: None,
            summary: None,
            effects: vec![],
            duration_ms: 0,
            metadata: Map::new(),
        };

        let err = parse_list_files_result(&result).unwrap_err();

        assert!(err.to_string().contains("truncated"));
    }

    #[test]
    fn list_files_parser_preserves_empty_message_like_filename_fallback() {
        let result = SubmitResult {
            invocation_id: "inv".to_string(),
            tool_name: "List".to_string(),
            status: ToolStatus::Success,
            output: "No files found matching pattern: notes.txt".to_string(),
            error: None,
            summary: None,
            effects: vec![],
            duration_ms: 0,
            metadata: Map::new(),
        };

        let files = parse_list_files_result(&result).unwrap();

        assert_eq!(
            files,
            vec!["No files found matching pattern: notes.txt".to_string()]
        );
    }

    #[tokio::test]
    async fn environment_list_files_returns_error_on_policy_denied() {
        let temp = tempfile::TempDir::new().unwrap();
        let policy = PolicyConfig {
            read_roots: vec![],
            ..PolicyConfig::default()
        };
        let env = ExecutionerEnvironment::create(
            ExecutionerEnvironment::builder()
                .file_backend(temp.path().join("queue"))
                .in_process_host(temp.path().join("state"))
                .in_process_worker("worker")
                .new_workspace()
                .policy(policy)
                .build()
                .unwrap(),
        )
        .await
        .unwrap();
        let session = env.create_session().await.unwrap();

        let err = session.list_files("/workspace").await.unwrap_err();

        assert!(err.to_string().contains("List"));
        env.close().await.unwrap();
    }

    #[tokio::test]
    async fn environment_submit_rejects_empty_tool_name_without_queue_entry() {
        let temp = tempfile::TempDir::new().unwrap();
        let queue = temp.path().join("queue");
        let env = ExecutionerEnvironment::create(
            ExecutionerEnvironment::builder()
                .file_backend(queue.clone())
                .in_process_host(temp.path().join("state"))
                .external_worker()
                .new_workspace()
                .build()
                .unwrap(),
        )
        .await
        .unwrap();
        let session = env.create_session().await.unwrap();

        let err = session
            .submit(ToolCall::new("", Map::new()).invocation_id("empty_tool"))
            .await
            .unwrap_err();

        assert!(err.to_string().contains("toolName must be"));
        assert!(!queue.join("pending/empty_tool.json").exists());
        env.close().await.unwrap();
    }

    #[tokio::test]
    async fn environment_submit_rejects_invalid_invocation_id_without_queue_entry() {
        let temp = tempfile::TempDir::new().unwrap();
        let queue = temp.path().join("queue");
        let env = ExecutionerEnvironment::create(
            ExecutionerEnvironment::builder()
                .file_backend(queue.clone())
                .in_process_host(temp.path().join("state"))
                .external_worker()
                .new_workspace()
                .build()
                .unwrap(),
        )
        .await
        .unwrap();
        let session = env.create_session().await.unwrap();

        let err = session
            .submit(
                ToolCall::json("Read", serde_json::json!({ "path": "missing.txt" }))
                    .unwrap()
                    .invocation_id("../escaped"),
            )
            .await
            .unwrap_err();

        assert!(err.to_string().contains("invalid invocationId"));
        assert!(!queue.join("pending/escaped.json").exists());
        assert!(!queue.join("pending/../escaped.json").exists());
        env.close().await.unwrap();
    }

    #[tokio::test]
    async fn environment_submit_rejects_oversized_request_without_queue_entry() {
        let temp = tempfile::TempDir::new().unwrap();
        let queue = temp.path().join("queue");
        let env = ExecutionerEnvironment::create(
            ExecutionerEnvironment::builder()
                .file_backend(queue.clone())
                .in_process_host(temp.path().join("state"))
                .external_worker()
                .new_workspace()
                .build()
                .unwrap(),
        )
        .await
        .unwrap();
        let session = env.create_session().await.unwrap();

        let err = session
            .submit(
                ToolCall::json("Read", serde_json::json!({ "path": "missing.txt" }))
                    .unwrap()
                    .invocation_id("oversized_request")
                    .metadata("padding", serde_json::json!("x".repeat(1024 * 1024))),
            )
            .await
            .unwrap_err();

        assert!(err.to_string().contains("maximum JSON size"));
        assert!(!queue.join("pending/oversized_request.json").exists());
        env.close().await.unwrap();
    }

    #[tokio::test]
    async fn environment_submit_rejects_zero_timeout_without_queue_entry() {
        let temp = tempfile::TempDir::new().unwrap();
        let queue = temp.path().join("queue");
        let env = ExecutionerEnvironment::create(
            ExecutionerEnvironment::builder()
                .file_backend(queue.clone())
                .in_process_host(temp.path().join("state"))
                .external_worker()
                .new_workspace()
                .build()
                .unwrap(),
        )
        .await
        .unwrap();
        let session = env.create_session().await.unwrap();

        let err = session
            .submit(
                ToolCall::json("Read", serde_json::json!({ "path": "missing.txt" }))
                    .unwrap()
                    .invocation_id("zero_timeout")
                    .timeout_ms(0),
            )
            .await
            .unwrap_err();

        assert!(err.to_string().contains("timeoutMs must be positive"));
        assert!(!queue.join("pending/zero_timeout.json").exists());
        env.close().await.unwrap();
    }

    #[tokio::test]
    async fn managed_worker_processes_queue_without_inline_submit_execution() {
        let temp = tempfile::TempDir::new().unwrap();
        let config = ExecutionerEnvironment::builder()
            .file_backend(temp.path().join("queue"))
            .in_process_host(temp.path().join("state"))
            .managed_worker_with_sleep("managed-worker", Duration::from_millis(1))
            .new_workspace()
            .submit_timeout(Duration::from_secs(5))
            .build()
            .unwrap();

        let env = ExecutionerEnvironment::create(config).await.unwrap();
        let session = env.create_session().await.unwrap();
        let write = session
            .submit(
                ToolCall::json(
                    "Write",
                    json!({ "path": "managed.txt", "content": "background worker" }),
                )
                .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(write.status, ToolStatus::Success);

        let read = session
            .submit(ToolCall::json("Read", json!({ "path": "managed.txt" })).unwrap())
            .await
            .unwrap();

        assert_eq!(read.output, "background worker");
        env.close().await.unwrap();
    }

    #[tokio::test]
    async fn external_worker_runtime_processes_environment_submissions_over_transport() {
        let temp = tempfile::TempDir::new().unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let state = HostState::new(temp.path().join("host-state")).unwrap();
        let app = executioner_host::HostServer::new(state).router();
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let host_url = format!("http://{addr}/");
        let queue = temp.path().join("queue");

        let worker = ExecutionerWorker::start(
            ExecutionerWorker::builder()
                .file_backend(queue.clone())
                .http_host(host_url.clone())
                .id("external-worker")
                .idle_sleep(Duration::from_millis(1))
                .build()
                .unwrap(),
        )
        .unwrap();

        let env = ExecutionerEnvironment::create(
            ExecutionerEnvironment::builder()
                .file_backend(queue)
                .http_host(host_url)
                .external_worker()
                .new_workspace()
                .submit_timeout(Duration::from_secs(5))
                .build()
                .unwrap(),
        )
        .await
        .unwrap();
        let session = env.create_session().await.unwrap();

        let write = session
            .submit(
                ToolCall::json(
                    "Write",
                    json!({ "path": "external.txt", "content": "transport worker" }),
                )
                .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(write.status, ToolStatus::Success);
        let read = session
            .submit(ToolCall::json("Read", json!({ "path": "external.txt" })).unwrap())
            .await
            .unwrap();
        assert_eq!(read.output, "transport worker");

        env.close().await.unwrap();
        worker.shutdown().await.unwrap();
        server.abort();
    }

    #[tokio::test]
    async fn attached_environment_submits_directly_without_owning_lifecycle() {
        let temp = tempfile::TempDir::new().unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let state = HostState::new(temp.path().join("host-state")).unwrap();
        let environment = state
            .create_environment(CreateEnvironmentRequest {
                environment_id: Some("env_attached".to_string()),
                workspace: WorkspaceSpec {
                    mode: WorkspaceMode::New,
                    root: None,
                    snapshot_ref: None,
                    template_ref: None,
                    mount_as_workspace: true,
                },
                policy: PolicyConfig::default().into_execution_policy(),
                ttl_ms: None,
                metadata: Map::new(),
            })
            .unwrap()
            .environment;
        let app = executioner_host::HostServer::new(state.clone()).router();
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let host_url = format!("http://{addr}/");

        let env = ExecutionerEnvironment::attach(AttachedEnvironmentConfig::http_direct(
            host_url,
            environment.id.clone(),
        ))
        .await
        .unwrap();
        let session = env.create_session().await.unwrap();
        let write = session
            .submit(
                ToolCall::json(
                    "Write",
                    json!({ "path": "attached.txt", "content": "direct" }),
                )
                .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(write.status, ToolStatus::Success);
        assert_eq!(env.close().await.unwrap().id, environment.id);
        assert_eq!(
            state.get_environment(&environment.id).unwrap().state,
            EnvironmentState::Ready
        );
        server.abort();
    }

    #[tokio::test]
    async fn environment_rejects_terminal_result_for_wrong_session() {
        let temp = tempfile::TempDir::new().unwrap();
        let queue = temp.path().join("queue");
        let env = ExecutionerEnvironment::create(
            ExecutionerEnvironment::builder()
                .file_backend(queue.clone())
                .in_process_host(temp.path().join("state"))
                .external_worker()
                .new_workspace()
                .submit_timeout(Duration::from_millis(100))
                .build()
                .unwrap(),
        )
        .await
        .unwrap();
        let session = env.create_session().await.unwrap();
        let invocation_id = "sdk_wrong_session";
        let submit = tokio::spawn(async move {
            session
                .submit(
                    ToolCall::json("Read", json!({ "path": "missing.txt" }))
                        .unwrap()
                        .invocation_id(invocation_id),
                )
                .await
        });

        let pending_path = queue.join("pending/sdk_wrong_session.json");
        let started = Instant::now();
        while !pending_path.exists() && started.elapsed() < Duration::from_secs(1) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(pending_path.exists());
        let claim = FileBroker::new(&queue)
            .unwrap()
            .claim_next("sdk-test-worker")
            .await
            .unwrap()
            .unwrap();
        fs::write(
            queue.join("completed/sdk_wrong_session.json"),
            serde_json::to_vec_pretty(&ToolInvocationCompleted {
                event_type: "tool.invocation.completed".to_string(),
                invocation_id: invocation_id.to_string(),
                session_id: "other_session".to_string(),
                attempt_id: Some(claim.attempt_id),
                lease_token: Some(claim.lease_token),
                result: ToolInvocationResult {
                    invocation_id: invocation_id.to_string(),
                    session_id: "other_session".to_string(),
                    tool_name: "Read".to_string(),
                    status: ToolResultStatus::Success,
                    output: "wrong session".to_string(),
                    error: None,
                    summary: None,
                    effects: vec![],
                    duration_ms: 0,
                    metadata: Map::new(),
                },
                completed_at: "now".to_string(),
            })
            .unwrap(),
        )
        .unwrap();

        let err = submit.await.unwrap().unwrap_err();

        assert!(matches!(err, SdkError::Timeout { .. }));
        assert!(!queue.join("completed/sdk_wrong_session.json").exists());
        assert!(queue.join("claimed/sdk_wrong_session.json").exists());
        assert_eq!(fs::read_dir(queue.join("rejected")).unwrap().count(), 1);
    }

    #[tokio::test]
    async fn environment_rejects_terminal_failure_for_wrong_session() {
        let temp = tempfile::TempDir::new().unwrap();
        let queue = temp.path().join("queue");
        let env = ExecutionerEnvironment::create(
            ExecutionerEnvironment::builder()
                .file_backend(queue.clone())
                .in_process_host(temp.path().join("state"))
                .external_worker()
                .new_workspace()
                .submit_timeout(Duration::from_secs(1))
                .build()
                .unwrap(),
        )
        .await
        .unwrap();
        let session = env.create_session().await.unwrap();
        let invocation_id = "sdk_wrong_failed_session";
        let submit = tokio::spawn(async move {
            session
                .submit(
                    ToolCall::json("Read", json!({ "path": "missing.txt" }))
                        .unwrap()
                        .invocation_id(invocation_id),
                )
                .await
        });

        let pending_path = queue.join("pending/sdk_wrong_failed_session.json");
        let started = Instant::now();
        while !pending_path.exists() && started.elapsed() < Duration::from_secs(1) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(pending_path.exists());
        let claim = FileBroker::new(&queue)
            .unwrap()
            .claim_next("sdk-test-worker")
            .await
            .unwrap()
            .unwrap();
        fs::write(
            queue.join("failed/sdk_wrong_failed_session.json"),
            serde_json::to_vec_pretty(&ToolInvocationFailed {
                event_type: "tool.invocation.failed".to_string(),
                invocation_id: invocation_id.to_string(),
                session_id: "other_session".to_string(),
                attempt_id: Some(claim.attempt_id),
                lease_token: Some(claim.lease_token),
                error: executioner_core::ErrorEnvelope {
                    code: "wrong_session".to_string(),
                    message: "wrong session".to_string(),
                    retryable: false,
                },
                failed_at: "now".to_string(),
            })
            .unwrap(),
        )
        .unwrap();

        let err = submit.await.unwrap().unwrap_err();

        assert!(matches!(err, SdkError::Timeout { .. }));
        assert!(!queue.join("failed/sdk_wrong_failed_session.json").exists());
        assert!(queue.join("claimed/sdk_wrong_failed_session.json").exists());
        assert_eq!(fs::read_dir(queue.join("rejected")).unwrap().count(), 1);
    }

    #[tokio::test]
    async fn environment_quarantines_terminal_result_for_wrong_tool_name() {
        let temp = tempfile::TempDir::new().unwrap();
        let queue = temp.path().join("queue");
        let env = ExecutionerEnvironment::create(
            ExecutionerEnvironment::builder()
                .file_backend(queue.clone())
                .in_process_host(temp.path().join("state"))
                .external_worker()
                .new_workspace()
                .submit_timeout(Duration::from_secs(1))
                .build()
                .unwrap(),
        )
        .await
        .unwrap();
        let session = env.create_session().await.unwrap();
        let invocation_id = "sdk_wrong_tool";
        let session_id = session.session().id.clone();
        let submit = tokio::spawn(async move {
            session
                .submit(
                    ToolCall::json("List", json!({}))
                        .unwrap()
                        .invocation_id(invocation_id),
                )
                .await
        });

        let pending_path = queue.join("pending/sdk_wrong_tool.json");
        let started = Instant::now();
        while !pending_path.exists() && started.elapsed() < Duration::from_secs(1) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(pending_path.exists());
        let claim = FileBroker::new(&queue)
            .unwrap()
            .claim_next("sdk-test-worker")
            .await
            .unwrap()
            .unwrap();
        fs::write(
            queue.join("completed/sdk_wrong_tool.json"),
            serde_json::to_vec_pretty(&ToolInvocationCompleted {
                event_type: "tool.invocation.completed".to_string(),
                invocation_id: invocation_id.to_string(),
                session_id: session_id.clone(),
                attempt_id: Some(claim.attempt_id),
                lease_token: Some(claim.lease_token),
                result: ToolInvocationResult {
                    invocation_id: invocation_id.to_string(),
                    session_id,
                    tool_name: "Read".to_string(),
                    status: ToolResultStatus::Success,
                    output: "forged".to_string(),
                    error: None,
                    summary: None,
                    effects: vec![],
                    duration_ms: 0,
                    metadata: Map::new(),
                },
                completed_at: "now".to_string(),
            })
            .unwrap(),
        )
        .unwrap();

        let err = submit.await.unwrap().unwrap_err();

        assert!(matches!(err, SdkError::Timeout { .. }));
        assert!(!queue.join("completed/sdk_wrong_tool.json").exists());
        assert!(queue.join("claimed/sdk_wrong_tool.json").exists());
        assert_eq!(fs::read_dir(queue.join("rejected")).unwrap().count(), 1);
    }

    #[tokio::test]
    async fn environment_rejects_forged_completed_terminal_without_claim() {
        let temp = tempfile::TempDir::new().unwrap();
        let queue = temp.path().join("queue");
        let env = ExecutionerEnvironment::create(
            ExecutionerEnvironment::builder()
                .file_backend(queue.clone())
                .in_process_host(temp.path().join("state"))
                .external_worker()
                .new_workspace()
                .submit_timeout(Duration::from_millis(100))
                .build()
                .unwrap(),
        )
        .await
        .unwrap();
        let session = env.create_session().await.unwrap();
        let invocation_id = "sdk_orphan_completed";
        let session_id = session.session().id.clone();
        let submit = tokio::spawn(async move {
            session
                .submit(
                    ToolCall::json("Read", json!({ "path": "missing.txt" }))
                        .unwrap()
                        .invocation_id(invocation_id),
                )
                .await
        });

        let pending_path = queue.join("pending/sdk_orphan_completed.json");
        let started = Instant::now();
        while !pending_path.exists() && started.elapsed() < Duration::from_secs(1) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(pending_path.exists());
        fs::remove_file(&pending_path).unwrap();
        fs::write(
            queue.join("completed/sdk_orphan_completed.json"),
            serde_json::to_vec_pretty(&ToolInvocationCompleted {
                event_type: "tool.invocation.completed".to_string(),
                invocation_id: invocation_id.to_string(),
                session_id: session_id.clone(),
                attempt_id: Some("attempt_forged".to_string()),
                lease_token: Some("lease_forged".to_string()),
                result: ToolInvocationResult {
                    invocation_id: invocation_id.to_string(),
                    session_id,
                    tool_name: "Read".to_string(),
                    status: ToolResultStatus::Success,
                    output: "forged without a claim".to_string(),
                    error: None,
                    summary: None,
                    effects: vec![],
                    duration_ms: 0,
                    metadata: Map::new(),
                },
                completed_at: "now".to_string(),
            })
            .unwrap(),
        )
        .unwrap();

        let err = submit.await.unwrap().unwrap_err();

        assert!(matches!(err, SdkError::Timeout { .. }));
        assert!(!queue.join("completed/sdk_orphan_completed.json").exists());
        assert_eq!(fs::read_dir(queue.join("rejected")).unwrap().count(), 1);
    }

    #[tokio::test]
    async fn environment_rejects_forged_failed_terminal_without_claim() {
        let temp = tempfile::TempDir::new().unwrap();
        let queue = temp.path().join("queue");
        let env = ExecutionerEnvironment::create(
            ExecutionerEnvironment::builder()
                .file_backend(queue.clone())
                .in_process_host(temp.path().join("state"))
                .external_worker()
                .new_workspace()
                .submit_timeout(Duration::from_millis(100))
                .build()
                .unwrap(),
        )
        .await
        .unwrap();
        let session = env.create_session().await.unwrap();
        let invocation_id = "sdk_orphan_failed";
        let session_id = session.session().id.clone();
        let submit = tokio::spawn(async move {
            session
                .submit(
                    ToolCall::json("Read", json!({ "path": "missing.txt" }))
                        .unwrap()
                        .invocation_id(invocation_id),
                )
                .await
        });

        let pending_path = queue.join("pending/sdk_orphan_failed.json");
        let started = Instant::now();
        while !pending_path.exists() && started.elapsed() < Duration::from_secs(1) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(pending_path.exists());
        fs::remove_file(&pending_path).unwrap();
        fs::write(
            queue.join("failed/sdk_orphan_failed.json"),
            serde_json::to_vec_pretty(&ToolInvocationFailed {
                event_type: "tool.invocation.failed".to_string(),
                invocation_id: invocation_id.to_string(),
                session_id,
                attempt_id: Some("attempt_forged".to_string()),
                lease_token: Some("lease_forged".to_string()),
                error: executioner_core::ErrorEnvelope {
                    code: "forged".to_string(),
                    message: "forged without a claim".to_string(),
                    retryable: false,
                },
                failed_at: "now".to_string(),
            })
            .unwrap(),
        )
        .unwrap();

        let err = submit.await.unwrap().unwrap_err();

        assert!(matches!(err, SdkError::Timeout { .. }));
        assert!(!queue.join("failed/sdk_orphan_failed.json").exists());
        assert_eq!(fs::read_dir(queue.join("rejected")).unwrap().count(), 1);
    }

    #[tokio::test]
    async fn existing_workspace_is_preserved_after_destroy() {
        let temp = tempfile::TempDir::new().unwrap();
        let workspace = temp.path().join("workspace");
        fs::create_dir_all(&workspace).unwrap();

        let config = EnvironmentConfig::builder()
            .file_backend(temp.path().join("queue"))
            .in_process_host(temp.path().join("state"))
            .existing_workspace(workspace.clone())
            .build()
            .unwrap();

        let env = ExecutionerEnvironment::create(config).await.unwrap();
        let session = env.create_session().await.unwrap();
        session
            .submit(
                ToolCall::json(
                    "Write",
                    json!({ "path": "kept.txt", "content": "preserve me" }),
                )
                .unwrap(),
            )
            .await
            .unwrap();

        env.close().await.unwrap();

        assert!(workspace.exists());
        assert_eq!(
            fs::read_to_string(workspace.join("kept.txt")).unwrap(),
            "preserve me"
        );
    }

    #[tokio::test]
    async fn environment_rejects_relative_existing_workspace_before_queue_creation() {
        let temp = tempfile::TempDir::new().unwrap();
        let queue = temp.path().join("queue");
        let state = temp.path().join("state");
        let config = EnvironmentConfig::builder()
            .file_backend(queue.clone())
            .in_process_host(state.clone())
            .existing_workspace("relative-workspace")
            .build()
            .unwrap();

        let err = ExecutionerEnvironment::create(config).await.unwrap_err();

        assert!(err.to_string().contains("workspace.root must be absolute"));
        assert!(!queue.exists());
        assert!(!state.exists());
    }

    #[tokio::test]
    async fn environment_rejects_existing_workspace_with_symlinked_parent_before_queue_creation() {
        #[cfg(unix)]
        {
            let temp = tempfile::TempDir::new().unwrap();
            let outside = temp.path().join("outside");
            let link_parent = temp.path().join("link-parent");
            let queue = temp.path().join("queue");
            let state = temp.path().join("state");
            fs::create_dir_all(outside.join("workspace")).unwrap();
            std::os::unix::fs::symlink(&outside, &link_parent).unwrap();
            let config = EnvironmentConfig::builder()
                .file_backend(queue.clone())
                .in_process_host(state.clone())
                .existing_workspace(link_parent.join("workspace"))
                .build()
                .unwrap();

            let err = ExecutionerEnvironment::create(config).await.unwrap_err();

            assert!(err.to_string().contains("workspace.root parent"));
            assert!(!queue.exists());
            assert!(!state.exists());
        }
    }

    #[tokio::test]
    async fn lifecycle_can_delete_queue_on_close() {
        let temp = tempfile::TempDir::new().unwrap();
        let queue = temp.path().join("queue");
        let lifecycle = LifecycleConfig::destroy_environment().delete_queue_on_close();
        let config = EnvironmentConfig::builder()
            .file_backend(queue.clone())
            .in_process_host(temp.path().join("state"))
            .lifecycle(lifecycle)
            .build()
            .unwrap();

        let env = ExecutionerEnvironment::create(config).await.unwrap();
        let _session = env.create_session().await.unwrap();
        assert!(queue.exists());
        env.close().await.unwrap();
        assert!(!queue.exists());
    }

    #[tokio::test]
    async fn lifecycle_cleanup_preserves_preexisting_queue_root_contents() {
        let temp = tempfile::TempDir::new().unwrap();
        let queue = temp.path().join("queue");
        fs::create_dir_all(&queue).unwrap();
        fs::write(queue.join("sentinel.txt"), "do not delete").unwrap();
        let lifecycle = LifecycleConfig::destroy_environment().delete_queue_on_close();
        let config = EnvironmentConfig::builder()
            .file_backend(queue.clone())
            .in_process_host(temp.path().join("state"))
            .lifecycle(lifecycle)
            .build()
            .unwrap();

        let env = ExecutionerEnvironment::create(config).await.unwrap();
        let _session = env.create_session().await.unwrap();
        env.close().await.unwrap();

        assert_eq!(
            fs::read_to_string(queue.join("sentinel.txt")).unwrap(),
            "do not delete"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn lifecycle_cleanup_unlinks_swapped_queue_child_symlink_without_following_it() {
        let temp = tempfile::TempDir::new().unwrap();
        let queue = temp.path().join("queue");
        let outside = temp.path().join("outside");
        fs::create_dir_all(&queue).unwrap();
        fs::create_dir_all(&outside).unwrap();
        fs::write(queue.join("sentinel.txt"), "do not delete").unwrap();
        fs::write(outside.join("secret.txt"), "keep me").unwrap();
        let lifecycle = LifecycleConfig::destroy_environment().delete_queue_on_close();
        let config = EnvironmentConfig::builder()
            .file_backend(queue.clone())
            .in_process_host(temp.path().join("state"))
            .lifecycle(lifecycle)
            .build()
            .unwrap();

        let env = ExecutionerEnvironment::create(config).await.unwrap();
        let _session = env.create_session().await.unwrap();
        fs::remove_dir_all(queue.join("pending")).unwrap();
        std::os::unix::fs::symlink(&outside, queue.join("pending")).unwrap();
        env.close().await.unwrap();

        assert_eq!(
            fs::read_to_string(queue.join("sentinel.txt")).unwrap(),
            "do not delete"
        );
        assert!(!queue.join("pending").exists());
        assert_eq!(
            fs::read_to_string(outside.join("secret.txt")).unwrap(),
            "keep me"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn lifecycle_cleanup_unlinks_swapped_queue_root_symlink_without_following_it() {
        let temp = tempfile::TempDir::new().unwrap();
        let queue = temp.path().join("queue");
        let outside = temp.path().join("outside");
        fs::create_dir_all(&queue).unwrap();
        for child in ["pending", "claimed", "completed", "failed", "rejected"] {
            fs::create_dir_all(outside.join(child)).unwrap();
        }
        fs::write(outside.join("pending/secret.txt"), "keep me").unwrap();
        let lifecycle = LifecycleConfig::destroy_environment().delete_queue_on_close();
        let config = EnvironmentConfig::builder()
            .file_backend(queue.clone())
            .in_process_host(temp.path().join("state"))
            .lifecycle(lifecycle)
            .build()
            .unwrap();

        let env = ExecutionerEnvironment::create(config).await.unwrap();
        let _session = env.create_session().await.unwrap();
        fs::remove_dir_all(&queue).unwrap();
        std::os::unix::fs::symlink(&outside, &queue).unwrap();
        env.close().await.unwrap();

        assert!(!queue.exists());
        assert_eq!(
            fs::read_to_string(outside.join("pending/secret.txt")).unwrap(),
            "keep me"
        );
    }

    #[tokio::test]
    async fn close_deletes_queue_when_remote_destroy_fails() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let environment_body = serde_json::json!({
                "environment": {
                    "id": "env_close_failure",
                    "state": "ready",
                    "workspace": {
                        "root": "/tmp/workspace",
                        "logicalRoot": "/workspace",
                        "mode": "new",
                        "fresh": true,
                        "managed": true
                    },
                    "policy": {
                        "readRoots": ["/workspace"],
                        "writeRoots": ["/workspace"],
                        "process": {
                            "allowExec": false,
                            "allowedCommands": [],
                            "deniedCommands": [],
                            "maxProcesses": null
                        },
                        "network": {
                            "enabled": false,
                            "allowHosts": [],
                            "denyHosts": []
                        },
                        "env": {
                            "allowlist": [],
                            "denylist": [],
                            "injected": {}
                        },
                        "maxDurationMs": 300000,
                        "maxOutputBytes": 100000
                    },
                    "metadata": {},
                    "createdAt": "now",
                    "revision": 0
                }
            })
            .to_string();
            let responses = [
                format!(
                    "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                    environment_body.len(),
                    environment_body
                ),
                "HTTP/1.1 500 Internal Server Error\r\ncontent-length: 14\r\nconnection: close\r\n\r\ndestroy failed".to_string(),
            ];

            for response in responses {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut buffer = [0_u8; 2048];
                let _ = socket.read(&mut buffer).await.unwrap();
                socket.write_all(response.as_bytes()).await.unwrap();
            }
        });
        let temp = tempfile::TempDir::new().unwrap();
        let queue = temp.path().join("queue");
        let env = ExecutionerEnvironment::create(
            ExecutionerEnvironment::builder()
                .file_backend(queue.clone())
                .http_host(format!("http://{addr}/"))
                .external_worker()
                .new_workspace()
                .lifecycle(LifecycleConfig::destroy_environment().delete_queue_on_close())
                .build()
                .unwrap(),
        )
        .await
        .unwrap();

        let err = env.close().await.unwrap_err();

        assert!(err.to_string().contains("host returned"));
        assert!(!queue.exists());
        server.await.unwrap();
    }

    #[test]
    fn json_arguments_must_be_objects() {
        let err = ToolCall::json("Read", json!(["not", "an", "object"])).unwrap_err();
        assert!(matches!(err, SdkError::ExpectedJsonObject));
    }
}
