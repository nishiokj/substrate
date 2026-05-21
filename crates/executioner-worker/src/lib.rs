use anyhow::Context;
use async_trait::async_trait;
use executioner_core::{
    ErrorEnvelope, ToolInvocationCompleted, ToolInvocationFailed, ToolInvocationRequest,
    ToolInvocationResult,
};
use reqwest::Url;
use serde::{Deserialize, Serialize};
use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::time::Duration;
use uuid::Uuid;

const MAX_QUEUE_JSON_BYTES: u64 = 10 * 1024 * 1024;
const MAX_HTTP_ERROR_BODY_BYTES: usize = 64 * 1024;
const MAX_HTTP_JSON_BODY_BYTES: usize = 10 * 1024 * 1024;
const MIN_IDLE_SLEEP: Duration = Duration::from_millis(1);

#[async_trait]
pub trait InvocationBroker: Send + Sync {
    async fn claim_next(&self, worker_id: &str) -> anyhow::Result<Option<ClaimedInvocation>>;
    async fn complete(&self, event: ToolInvocationCompleted) -> anyhow::Result<()>;
    async fn fail(&self, event: ToolInvocationFailed) -> anyhow::Result<()>;
}

#[async_trait]
pub trait ToolHostClient: Send + Sync {
    async fn execute(&self, request: ToolInvocationRequest)
        -> anyhow::Result<ToolInvocationResult>;
}

#[derive(Debug, Clone)]
pub struct ClaimedInvocation {
    pub request: ToolInvocationRequest,
    pub attempt_id: String,
    pub lease_token: String,
}

#[derive(Debug, Clone)]
pub struct Worker {
    pub id: String,
    pub idle_sleep: Duration,
}

impl Worker {
    pub fn new(id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            idle_sleep: Duration::from_millis(250),
        }
    }

    pub fn with_idle_sleep(mut self, idle_sleep: Duration) -> Self {
        self.idle_sleep = idle_sleep.max(MIN_IDLE_SLEEP);
        self
    }

    pub async fn run_once<B, H>(&self, broker: &B, host: &H) -> anyhow::Result<WorkerRunOnce>
    where
        B: InvocationBroker,
        H: ToolHostClient,
    {
        let Some(claim) = broker.claim_next(&self.id).await? else {
            return Ok(WorkerRunOnce::Idle);
        };

        let invocation_id = claim
            .request
            .invocation_id
            .clone()
            .unwrap_or_else(|| "unknown".to_string());
        let session_id = claim.request.session_id.clone();
        let tool_name = claim.request.tool_name.clone();

        match host.execute(claim.request).await {
            Ok(result) => {
                if result.invocation_id != invocation_id
                    || result.session_id != session_id
                    || result.tool_name != tool_name
                {
                    broker
                        .fail(ToolInvocationFailed {
                            event_type: "tool.invocation.failed".to_string(),
                            invocation_id,
                            session_id,
                            attempt_id: Some(claim.attempt_id),
                            lease_token: Some(claim.lease_token),
                            error: ErrorEnvelope {
                                code: "host_result_mismatch".to_string(),
                                message: format!(
                                    "host returned result for invocation {} in session {} from tool {}",
                                    result.invocation_id, result.session_id, result.tool_name
                                ),
                                retryable: false,
                            },
                            failed_at: format!("{:?}", std::time::SystemTime::now()),
                        })
                        .await?;
                    return Ok(WorkerRunOnce::Failed);
                }
                broker
                    .complete(ToolInvocationCompleted {
                        event_type: "tool.invocation.completed".to_string(),
                        invocation_id: result.invocation_id.clone(),
                        session_id: result.session_id.clone(),
                        attempt_id: Some(claim.attempt_id),
                        lease_token: Some(claim.lease_token),
                        result,
                        completed_at: format!("{:?}", std::time::SystemTime::now()),
                    })
                    .await?;
                Ok(WorkerRunOnce::Completed)
            }
            Err(err) => {
                broker
                    .fail(ToolInvocationFailed {
                        event_type: "tool.invocation.failed".to_string(),
                        invocation_id,
                        session_id,
                        attempt_id: Some(claim.attempt_id),
                        lease_token: Some(claim.lease_token),
                        error: ErrorEnvelope {
                            code: "host_execute_failed".to_string(),
                            message: err.to_string(),
                            retryable: true,
                        },
                        failed_at: format!("{:?}", std::time::SystemTime::now()),
                    })
                    .await?;
                Ok(WorkerRunOnce::Failed)
            }
        }
    }

    pub async fn run<B, H>(&self, broker: &B, host: &H) -> anyhow::Result<()>
    where
        B: InvocationBroker,
        H: ToolHostClient,
    {
        loop {
            match self.run_once(broker, host).await? {
                WorkerRunOnce::Idle => {
                    tokio::time::sleep(self.idle_sleep).await;
                }
                WorkerRunOnce::Completed | WorkerRunOnce::Failed => {}
            }
        }
    }

    pub async fn run_until_idle<B, H>(
        &self,
        broker: &B,
        host: &H,
        max_idle_ticks: usize,
    ) -> anyhow::Result<WorkerStats>
    where
        B: InvocationBroker,
        H: ToolHostClient,
    {
        let mut stats = WorkerStats::default();
        loop {
            match self.run_once(broker, host).await? {
                WorkerRunOnce::Idle => {
                    stats.idle_ticks += 1;
                    if stats.idle_ticks >= max_idle_ticks {
                        return Ok(stats);
                    }
                    tokio::time::sleep(self.idle_sleep).await;
                }
                WorkerRunOnce::Completed => stats.completed += 1,
                WorkerRunOnce::Failed => stats.failed += 1,
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkerRunOnce {
    Idle,
    Completed,
    Failed,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct WorkerStats {
    pub completed: usize,
    pub failed: usize,
    pub idle_ticks: usize,
}

#[derive(Debug, Clone)]
pub struct HttpHostClient {
    base_url: Url,
    client: reqwest::Client,
}

impl HttpHostClient {
    pub fn new(base_url: impl AsRef<str>) -> anyhow::Result<Self> {
        Ok(Self {
            base_url: normalize_http_base_url(base_url.as_ref())
                .context("invalid host base url")?,
            client: reqwest::Client::builder()
                .redirect(reqwest::redirect::Policy::none())
                .build()?,
        })
    }
}

#[async_trait]
impl ToolHostClient for HttpHostClient {
    async fn execute(
        &self,
        request: ToolInvocationRequest,
    ) -> anyhow::Result<ToolInvocationResult> {
        validate_session_id(&request.session_id)?;
        let url = self
            .base_url
            .join(&format!("sessions/{}/invocations", request.session_id))
            .context("invalid invocation url")?;
        let response = self.client.post(url).json(&request).send().await?;
        let status = response.status();
        if !status.is_success() {
            let text = capped_response_text(response, MAX_HTTP_ERROR_BODY_BYTES).await;
            anyhow::bail!("host returned {status}: {text}");
        }
        read_capped_json_response::<ToolInvocationResult>(response, MAX_HTTP_JSON_BODY_BYTES).await
    }
}

#[derive(Debug, Clone)]
pub struct FileBroker {
    queue_dir: PathBuf,
    pending_dir: PathBuf,
    claimed_dir: PathBuf,
    completed_dir: PathBuf,
    failed_dir: PathBuf,
    rejected_dir: PathBuf,
}

impl FileBroker {
    pub fn new(queue_dir: impl AsRef<Path>) -> anyhow::Result<Self> {
        let queue_dir = queue_dir.as_ref();
        ensure_queue_root_dir(queue_dir)?;
        let pending_dir = queue_dir.join("pending");
        let claimed_dir = queue_dir.join("claimed");
        let completed_dir = queue_dir.join("completed");
        let failed_dir = queue_dir.join("failed");
        let rejected_dir = queue_dir.join("rejected");
        ensure_queue_state_dir(&pending_dir)?;
        ensure_queue_state_dir(&claimed_dir)?;
        ensure_queue_state_dir(&completed_dir)?;
        ensure_queue_state_dir(&failed_dir)?;
        ensure_queue_state_dir(&rejected_dir)?;
        Ok(Self {
            queue_dir: queue_dir.to_path_buf(),
            pending_dir,
            claimed_dir,
            completed_dir,
            failed_dir,
            rejected_dir,
        })
    }

    pub fn enqueue(&self, request: &ToolInvocationRequest) -> anyhow::Result<PathBuf> {
        self.ensure_dirs_safe()?;
        let invocation_id = request
            .invocation_id
            .clone()
            .unwrap_or_else(|| format!("inv_{}", Uuid::new_v4().simple()));
        validate_invocation_id(&invocation_id)?;
        validate_session_id(&request.session_id)?;
        validate_tool_name(&request.tool_name)?;
        self.ensure_invocation_id_unused(&invocation_id)?;
        let mut request = request.clone();
        request.invocation_id = Some(invocation_id.clone());
        let path = self.pending_dir.join(format!("{invocation_id}.json"));
        write_json_atomic(&path, &request)?;
        Ok(path)
    }

    pub fn completed_path(&self, invocation_id: &str) -> anyhow::Result<PathBuf> {
        validate_invocation_id(invocation_id)?;
        Ok(self.completed_dir.join(format!("{invocation_id}.json")))
    }

    pub fn failed_path(&self, invocation_id: &str) -> anyhow::Result<PathBuf> {
        validate_invocation_id(invocation_id)?;
        Ok(self.failed_dir.join(format!("{invocation_id}.json")))
    }

    fn claimed_path(&self, invocation_id: &str) -> anyhow::Result<PathBuf> {
        validate_invocation_id(invocation_id)?;
        Ok(self.claimed_dir.join(format!("{invocation_id}.json")))
    }

    fn pending_path(&self, invocation_id: &str) -> anyhow::Result<PathBuf> {
        validate_invocation_id(invocation_id)?;
        Ok(self.pending_dir.join(format!("{invocation_id}.json")))
    }

    fn ensure_invocation_id_unused(&self, invocation_id: &str) -> anyhow::Result<()> {
        let paths = [
            self.pending_path(invocation_id)?,
            self.claimed_path(invocation_id)?,
            self.completed_path(invocation_id)?,
            self.failed_path(invocation_id)?,
        ];
        if paths.iter().any(|path| path_occupied(path)) {
            anyhow::bail!("duplicate invocation id: {invocation_id}");
        }
        Ok(())
    }

    pub fn read_completed(
        &self,
        invocation_id: &str,
    ) -> anyhow::Result<Option<ToolInvocationCompleted>> {
        validate_invocation_id(invocation_id)?;
        self.ensure_dirs_safe()?;
        let path = self.completed_path(invocation_id)?;
        let Some(event) = self.read_terminal_json::<ToolInvocationCompleted>(&path)? else {
            return Ok(None);
        };
        if event.event_type == "tool.invocation.completed"
            && event.invocation_id == invocation_id
            && validate_session_id(&event.session_id).is_ok()
            && event.result.invocation_id == invocation_id
            && validate_session_id(&event.result.session_id).is_ok()
            && event.result.session_id == event.session_id
            && validate_tool_name(&event.result.tool_name).is_ok()
            && !path_occupied(&self.pending_path(invocation_id)?)
            && self.terminal_matches_claim(
                invocation_id,
                event.session_id.as_str(),
                event.attempt_id.as_deref(),
                event.lease_token.as_deref(),
                Some(event.result.tool_name.as_str()),
            )?
        {
            Ok(Some(event))
        } else {
            self.quarantine_terminal(&path);
            Ok(None)
        }
    }

    pub fn read_failed(&self, invocation_id: &str) -> anyhow::Result<Option<ToolInvocationFailed>> {
        validate_invocation_id(invocation_id)?;
        self.ensure_dirs_safe()?;
        let path = self.failed_path(invocation_id)?;
        let Some(event) = self.read_terminal_json::<ToolInvocationFailed>(&path)? else {
            return Ok(None);
        };
        if event.event_type == "tool.invocation.failed"
            && event.invocation_id == invocation_id
            && validate_session_id(&event.session_id).is_ok()
            && validate_error_envelope(&event.error)
            && !path_occupied(&self.pending_path(invocation_id)?)
            && self.terminal_matches_claim(
                invocation_id,
                event.session_id.as_str(),
                event.attempt_id.as_deref(),
                event.lease_token.as_deref(),
                None,
            )?
        {
            Ok(Some(event))
        } else {
            self.quarantine_terminal(&path);
            Ok(None)
        }
    }

    fn next_pending(&self) -> anyhow::Result<Option<PathBuf>> {
        let mut entries = fs::read_dir(&self.pending_dir)?
            .filter_map(|entry| entry.ok().map(|entry| entry.path()))
            .filter(|path| path.extension().and_then(|ext| ext.to_str()) == Some("json"))
            .collect::<Vec<_>>();
        entries.sort();
        Ok(entries.into_iter().next())
    }

    fn quarantine_pending(&self, path: &Path) {
        let name = path
            .file_stem()
            .and_then(|name| name.to_str())
            .unwrap_or("pending");
        let rejected_path = self
            .rejected_dir
            .join(format!("{name}.rejected.{}.json", Uuid::new_v4().simple()));
        if fs::rename(path, rejected_path).is_err() {
            let _ = fs::remove_file(path);
        }
    }

    fn quarantine_terminal(&self, path: &Path) {
        let name = path
            .file_stem()
            .and_then(|name| name.to_str())
            .unwrap_or("terminal");
        let rejected_path = self.rejected_dir.join(format!(
            "{name}.terminal.rejected.{}.json",
            Uuid::new_v4().simple()
        ));
        if fs::rename(path, rejected_path).is_err() {
            let _ = fs::remove_file(path);
        }
    }

    fn restore_or_quarantine_claim_staging(&self, staging_path: &Path, pending_path: &Path) {
        if !path_occupied(pending_path) && fs::rename(staging_path, pending_path).is_ok() {
            return;
        }
        self.quarantine_pending(staging_path);
    }

    fn read_terminal_json<T>(&self, path: &Path) -> anyhow::Result<Option<T>>
    where
        T: for<'de> Deserialize<'de>,
    {
        match fs::symlink_metadata(path) {
            Ok(metadata)
                if metadata.file_type().is_file() && metadata.len() <= MAX_QUEUE_JSON_BYTES => {}
            Ok(metadata) if metadata.file_type().is_file() => {
                self.quarantine_terminal(path);
                return Ok(None);
            }
            Ok(_) => {
                self.quarantine_terminal(path);
                return Ok(None);
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(_) => {
                self.quarantine_terminal(path);
                return Ok(None);
            }
        }
        let bytes = match read_regular_file_no_follow(path, MAX_QUEUE_JSON_BYTES) {
            Ok(bytes) => bytes,
            Err(_) => {
                self.quarantine_terminal(path);
                return Ok(None);
            }
        };
        match serde_json::from_slice(&bytes) {
            Ok(event) => Ok(Some(event)),
            Err(_) => {
                self.quarantine_terminal(path);
                Ok(None)
            }
        }
    }

    fn claim_staging_path(&self) -> PathBuf {
        self.claimed_dir
            .join(format!(".claiming-{}.json", Uuid::new_v4().simple()))
    }

    fn validate_claim_lease(
        &self,
        invocation_id: &str,
        attempt_id: Option<&str>,
        lease_token: Option<&str>,
    ) -> anyhow::Result<(PathBuf, ClaimEnvelope)> {
        let claimed_path = self.claimed_path(invocation_id)?;
        let envelope = read_json_required::<ClaimEnvelope>(&claimed_path)
            .with_context(|| format!("missing claimed lease for invocation: {invocation_id}"))?;
        let matches_attempt = attempt_id == Some(envelope.attempt_id.as_str());
        let matches_lease = lease_token == Some(envelope.lease_token.as_str());
        if !matches_attempt || !matches_lease {
            anyhow::bail!("lease validation failed for invocation: {invocation_id}");
        }
        Ok((claimed_path, envelope))
    }

    fn ensure_terminal_absent(&self, invocation_id: &str) -> anyhow::Result<()> {
        for path in [
            self.completed_path(invocation_id)?,
            self.failed_path(invocation_id)?,
        ] {
            if fs::symlink_metadata(&path).is_ok() {
                anyhow::bail!("terminal invocation already exists: {invocation_id}");
            }
        }
        Ok(())
    }

    fn terminal_matches_claim(
        &self,
        invocation_id: &str,
        session_id: &str,
        attempt_id: Option<&str>,
        lease_token: Option<&str>,
        tool_name: Option<&str>,
    ) -> anyhow::Result<bool> {
        let claimed_path = self.claimed_path(invocation_id)?;
        if !path_occupied(&claimed_path) {
            return Ok(false);
        }
        let Ok(claim) = read_json_required::<ClaimEnvelope>(&claimed_path) else {
            return Ok(false);
        };
        Ok(
            claim.request.invocation_id.as_deref() == Some(invocation_id)
                && claim.request.session_id == session_id
                && attempt_id == Some(claim.attempt_id.as_str())
                && lease_token == Some(claim.lease_token.as_str())
                && tool_name.is_none_or(|tool_name| claim.request.tool_name == tool_name),
        )
    }

    fn ensure_dirs_safe(&self) -> anyhow::Result<()> {
        ensure_queue_root_dir(&self.queue_dir)?;
        ensure_queue_state_dir(&self.pending_dir)?;
        ensure_queue_state_dir(&self.claimed_dir)?;
        ensure_queue_state_dir(&self.completed_dir)?;
        ensure_queue_state_dir(&self.failed_dir)?;
        ensure_queue_state_dir(&self.rejected_dir)?;
        Ok(())
    }
}

#[cfg(unix)]
fn read_regular_file_no_follow(path: &Path, max_bytes: u64) -> std::io::Result<Vec<u8>> {
    use std::fs::OpenOptions;
    use std::os::unix::fs::OpenOptionsExt;

    let file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)?;
    let metadata = file.metadata()?;
    if !metadata.file_type().is_file()
        || metadata.file_type().is_symlink()
        || metadata.len() > max_bytes
    {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "terminal file must be a regular bounded file",
        ));
    }
    let mut bytes = Vec::new();
    file.take(max_bytes + 1).read_to_end(&mut bytes)?;
    if bytes.len() as u64 > max_bytes {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "terminal file exceeds maximum size",
        ));
    }
    Ok(bytes)
}

#[cfg(not(unix))]
fn read_regular_file_no_follow(path: &Path, max_bytes: u64) -> std::io::Result<Vec<u8>> {
    let mut file = fs::File::open(path)?;
    let metadata = file.metadata()?;
    if !metadata.file_type().is_file() || metadata.len() > max_bytes {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "terminal file must be a regular bounded file",
        ));
    }
    let mut bytes = Vec::new();
    file.take(max_bytes + 1).read_to_end(&mut bytes)?;
    if bytes.len() as u64 > max_bytes {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "terminal file exceeds maximum size",
        ));
    }
    Ok(bytes)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ClaimEnvelope {
    worker_id: String,
    attempt_id: String,
    lease_token: String,
    claimed_at: String,
    request: ToolInvocationRequest,
}

#[async_trait]
impl InvocationBroker for FileBroker {
    async fn claim_next(&self, worker_id: &str) -> anyhow::Result<Option<ClaimedInvocation>> {
        validate_worker_id(worker_id)?;
        self.ensure_dirs_safe()?;
        loop {
            let Some(path) = self.next_pending()? else {
                return Ok(None);
            };
            let staging_path = self.claim_staging_path();
            match fs::rename(&path, &staging_path) {
                Ok(()) => {}
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue,
                Err(err) => return Err(err.into()),
            }
            if !is_regular_file(&staging_path) {
                self.quarantine_pending(&staging_path);
                continue;
            }
            if queue_json_too_large(&staging_path)? {
                self.quarantine_pending(&staging_path);
                continue;
            }
            let bytes = match read_regular_file_no_follow(&staging_path, MAX_QUEUE_JSON_BYTES) {
                Ok(bytes) => bytes,
                Err(_) => {
                    self.quarantine_pending(&staging_path);
                    continue;
                }
            };
            let mut request = match serde_json::from_slice::<ToolInvocationRequest>(&bytes) {
                Ok(request) => request,
                Err(_) => {
                    self.quarantine_pending(&staging_path);
                    continue;
                }
            };
            let invocation_id = request
                .invocation_id
                .clone()
                .unwrap_or_else(|| format!("inv_{}", Uuid::new_v4().simple()));
            if validate_invocation_id(&invocation_id).is_err() {
                self.quarantine_pending(&staging_path);
                continue;
            }
            if validate_session_id(&request.session_id).is_err() {
                self.quarantine_pending(&staging_path);
                continue;
            }
            if validate_tool_name(&request.tool_name).is_err() {
                self.quarantine_pending(&staging_path);
                continue;
            }
            request.invocation_id = Some(invocation_id.clone());
            let attempt_id = format!("attempt_{}", Uuid::new_v4().simple());
            let lease_token = format!("lease_{}", Uuid::new_v4().simple());
            if path_occupied(&self.claimed_path(&invocation_id)?)
                || path_occupied(&self.completed_path(&invocation_id)?)
                || path_occupied(&self.failed_path(&invocation_id)?)
            {
                self.quarantine_pending(&staging_path);
                continue;
            }
            let claimed_path = self.claimed_path(&invocation_id)?;
            let envelope = ClaimEnvelope {
                worker_id: worker_id.to_string(),
                attempt_id: attempt_id.clone(),
                lease_token: lease_token.clone(),
                claimed_at: format!("{:?}", std::time::SystemTime::now()),
                request: request.clone(),
            };
            match write_json_atomic(&claimed_path, &envelope) {
                Ok(()) => {}
                Err(err) if is_queue_json_too_large_error(&err) => {
                    self.quarantine_pending(&staging_path);
                    continue;
                }
                Err(err) => {
                    self.restore_or_quarantine_claim_staging(&staging_path, &path);
                    return Err(err);
                }
            }
            fs::remove_file(staging_path)?;
            return Ok(Some(ClaimedInvocation {
                request,
                attempt_id,
                lease_token,
            }));
        }
    }

    async fn complete(&self, event: ToolInvocationCompleted) -> anyhow::Result<()> {
        validate_invocation_id(&event.invocation_id)?;
        validate_session_id(&event.session_id)?;
        validate_session_id(&event.result.session_id)?;
        self.ensure_dirs_safe()?;
        if event.event_type != "tool.invocation.completed" {
            anyhow::bail!(
                "completed event type mismatch for invocation: {}",
                event.invocation_id
            );
        }
        let (_claimed_path, claim) = self.validate_claim_lease(
            &event.invocation_id,
            event.attempt_id.as_deref(),
            event.lease_token.as_deref(),
        )?;
        validate_terminal_claim_identity(&claim.request, &event.invocation_id, &event.session_id)?;
        if event.result.invocation_id != event.invocation_id
            || event.result.session_id != event.session_id
            || event.result.tool_name != claim.request.tool_name
        {
            anyhow::bail!(
                "result identity mismatch for invocation: {}",
                event.invocation_id
            );
        }
        self.ensure_terminal_absent(&event.invocation_id)?;
        let path = self.completed_path(&event.invocation_id)?;
        write_json_atomic(&path, &event)?;
        Ok(())
    }

    async fn fail(&self, event: ToolInvocationFailed) -> anyhow::Result<()> {
        validate_invocation_id(&event.invocation_id)?;
        validate_session_id(&event.session_id)?;
        self.ensure_dirs_safe()?;
        if !validate_error_envelope(&event.error) {
            anyhow::bail!("failed event error envelope malformed");
        }
        if event.event_type != "tool.invocation.failed" {
            anyhow::bail!(
                "failed event type mismatch for invocation: {}",
                event.invocation_id
            );
        }
        let (_claimed_path, claim) = self.validate_claim_lease(
            &event.invocation_id,
            event.attempt_id.as_deref(),
            event.lease_token.as_deref(),
        )?;
        validate_terminal_claim_identity(&claim.request, &event.invocation_id, &event.session_id)?;
        self.ensure_terminal_absent(&event.invocation_id)?;
        let path = self.failed_path(&event.invocation_id)?;
        write_json_atomic(&path, &event)?;
        Ok(())
    }
}

fn validate_terminal_claim_identity(
    request: &ToolInvocationRequest,
    invocation_id: &str,
    session_id: &str,
) -> anyhow::Result<()> {
    if request.invocation_id.as_deref() != Some(invocation_id) || request.session_id != session_id {
        anyhow::bail!("claim identity mismatch for invocation: {invocation_id}");
    }
    Ok(())
}

fn validate_invocation_id(invocation_id: &str) -> anyhow::Result<()> {
    let is_valid = !invocation_id.is_empty()
        && invocation_id.len() <= 128
        && invocation_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-');
    if !is_valid {
        anyhow::bail!(
            "invalid invocation id: only ASCII letters, numbers, '-' and '_' are allowed"
        );
    }
    Ok(())
}

fn validate_session_id(session_id: &str) -> anyhow::Result<()> {
    let is_valid = !session_id.is_empty()
        && session_id.len() <= 128
        && session_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-');
    if !is_valid {
        anyhow::bail!("invalid session id: only ASCII letters, numbers, '-' and '_' are allowed");
    }
    Ok(())
}

fn validate_worker_id(worker_id: &str) -> anyhow::Result<()> {
    let is_valid = !worker_id.is_empty()
        && worker_id.len() <= 128
        && worker_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-');
    if !is_valid {
        anyhow::bail!("invalid worker id: only ASCII letters, numbers, '-' and '_' are allowed");
    }
    Ok(())
}

fn validate_tool_name(tool_name: &str) -> anyhow::Result<()> {
    if tool_name.is_empty() {
        anyhow::bail!("tool name must be non-empty");
    }
    Ok(())
}

fn validate_error_envelope(error: &ErrorEnvelope) -> bool {
    !error.code.trim().is_empty() && !error.message.trim().is_empty()
}

fn ensure_queue_state_dir(path: &Path) -> anyhow::Result<()> {
    ensure_no_symlinked_parent(path.parent().unwrap_or_else(|| Path::new(".")))?;
    fs::create_dir_all(path)?;
    let metadata = fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        anyhow::bail!(
            "queue state directory must be a real directory: {}",
            path.display()
        );
    }
    Ok(())
}

fn ensure_queue_root_dir(path: &Path) -> anyhow::Result<()> {
    ensure_no_symlinked_parent(path.parent().unwrap_or_else(|| Path::new(".")))?;
    fs::create_dir_all(path)?;
    let metadata = fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        anyhow::bail!(
            "queue directory must be a real directory: {}",
            path.display()
        );
    }
    Ok(())
}

fn ensure_no_symlinked_parent(parent: &Path) -> anyhow::Result<()> {
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
                anyhow::bail!(
                    "queue directory parent must not contain symlinks: {}",
                    current.display()
                );
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

fn normalize_http_base_url(base_url: &str) -> anyhow::Result<Url> {
    if base_url.starts_with("http:///") || base_url.starts_with("https:///") {
        anyhow::bail!("host is required");
    }
    let mut url = Url::parse(base_url)?;
    match url.scheme() {
        "http" | "https" => {}
        scheme => anyhow::bail!("unsupported scheme: {scheme}"),
    }
    if url.host_str().is_none() {
        anyhow::bail!("host is required");
    }
    if !url.username().is_empty() || url.password().is_some() {
        anyhow::bail!("credentials are not allowed");
    }
    if url.query().is_some() || url.fragment().is_some() {
        anyhow::bail!("query strings and fragments are not allowed");
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
                    anyhow::bail!("response body exceeds maximum size of {max_bytes} bytes");
                }
                bytes.extend_from_slice(&chunk);
            }
            Ok(None) => return Ok(bytes),
            Err(err) => return Err(err.into()),
        }
    }
}

fn path_occupied(path: &Path) -> bool {
    fs::symlink_metadata(path).is_ok()
}

fn write_json_atomic(path: &Path, value: &impl Serialize) -> anyhow::Result<()> {
    let tmp_path = path.with_extension(format!("json.tmp.{}", Uuid::new_v4().simple()));
    let bytes = serde_json::to_vec_pretty(value)?;
    if bytes.len() as u64 > MAX_QUEUE_JSON_BYTES {
        anyhow::bail!("queue json exceeds maximum size of {MAX_QUEUE_JSON_BYTES} bytes");
    }
    fs::write(&tmp_path, bytes)?;
    let result = fs::hard_link(&tmp_path, path).map_err(anyhow::Error::from);
    let _ = fs::remove_file(&tmp_path);
    result
}

fn is_queue_json_too_large_error(err: &anyhow::Error) -> bool {
    err.to_string().contains("queue json exceeds maximum size")
}

fn read_json_required<T>(path: &Path) -> anyhow::Result<T>
where
    T: for<'de> Deserialize<'de>,
{
    if !is_regular_file(path) {
        anyhow::bail!("queue json path is not a regular file: {}", path.display());
    }
    if queue_json_too_large(path)? {
        anyhow::bail!("queue json exceeds maximum size of {MAX_QUEUE_JSON_BYTES} bytes");
    }
    let bytes = read_regular_file_no_follow(path, MAX_QUEUE_JSON_BYTES)?;
    Ok(serde_json::from_slice(&bytes)?)
}

fn queue_json_too_large(path: &Path) -> anyhow::Result<bool> {
    Ok(fs::symlink_metadata(path)?.len() > MAX_QUEUE_JSON_BYTES)
}

fn is_regular_file(path: &Path) -> bool {
    fs::symlink_metadata(path)
        .map(|metadata| metadata.file_type().is_file())
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use executioner_core::{ToolInvocationResult, ToolResultStatus};
    use serde_json::{Map, Value};
    use std::sync::{Arc, Mutex};

    #[derive(Default)]
    struct MemoryBroker {
        request: Mutex<Option<ToolInvocationRequest>>,
        completed: Mutex<usize>,
        failed: Mutex<Vec<ToolInvocationFailed>>,
    }

    #[async_trait]
    impl InvocationBroker for Arc<MemoryBroker> {
        async fn claim_next(&self, _worker_id: &str) -> anyhow::Result<Option<ClaimedInvocation>> {
            Ok(self
                .request
                .lock()
                .unwrap()
                .take()
                .map(|request| ClaimedInvocation {
                    request,
                    attempt_id: "attempt".to_string(),
                    lease_token: "lease".to_string(),
                }))
        }

        async fn complete(&self, _event: ToolInvocationCompleted) -> anyhow::Result<()> {
            *self.completed.lock().unwrap() += 1;
            Ok(())
        }

        async fn fail(&self, event: ToolInvocationFailed) -> anyhow::Result<()> {
            self.failed.lock().unwrap().push(event);
            Ok(())
        }
    }

    struct EchoHost;

    #[async_trait]
    impl ToolHostClient for EchoHost {
        async fn execute(
            &self,
            request: ToolInvocationRequest,
        ) -> anyhow::Result<ToolInvocationResult> {
            Ok(ToolInvocationResult {
                invocation_id: request.invocation_id.unwrap_or_else(|| "inv".to_string()),
                session_id: request.session_id,
                tool_name: request.tool_name,
                status: ToolResultStatus::Success,
                output: "ok".to_string(),
                error: None,
                summary: None,
                effects: vec![],
                duration_ms: 0,
                metadata: Map::new(),
            })
        }
    }

    struct WrongIdentityHost;

    #[async_trait]
    impl ToolHostClient for WrongIdentityHost {
        async fn execute(
            &self,
            request: ToolInvocationRequest,
        ) -> anyhow::Result<ToolInvocationResult> {
            Ok(ToolInvocationResult {
                invocation_id: "other_invocation".to_string(),
                session_id: request.session_id,
                tool_name: request.tool_name,
                status: ToolResultStatus::Success,
                output: "ok".to_string(),
                error: None,
                summary: None,
                effects: vec![],
                duration_ms: 0,
                metadata: Map::new(),
            })
        }
    }

    struct WrongToolHost;

    #[async_trait]
    impl ToolHostClient for WrongToolHost {
        async fn execute(
            &self,
            request: ToolInvocationRequest,
        ) -> anyhow::Result<ToolInvocationResult> {
            Ok(ToolInvocationResult {
                invocation_id: request.invocation_id.unwrap_or_else(|| "inv".to_string()),
                session_id: request.session_id,
                tool_name: "Write".to_string(),
                status: ToolResultStatus::Success,
                output: "wrong tool".to_string(),
                error: None,
                summary: None,
                effects: vec![],
                duration_ms: 0,
                metadata: Map::new(),
            })
        }
    }

    #[tokio::test]
    async fn worker_claims_executes_and_completes() {
        let broker = Arc::new(MemoryBroker::default());
        *broker.request.lock().unwrap() = Some(ToolInvocationRequest {
            invocation_id: Some("inv".to_string()),
            session_id: "sess".to_string(),
            tool_name: "Read".to_string(),
            arguments: Map::new(),
            cwd: None,
            timeout_ms: None,
            max_output_bytes: None,
            idempotency_key: None,
            required_capabilities: vec![],
            metadata: Map::new(),
        });

        let worker = Worker::new("worker");
        let result = worker.run_once(&broker, &EchoHost).await.unwrap();

        assert_eq!(result, WorkerRunOnce::Completed);
        assert_eq!(*broker.completed.lock().unwrap(), 1);
    }

    #[tokio::test]
    async fn worker_runs_until_idle_after_processing_queue() {
        let broker = Arc::new(MemoryBroker::default());
        *broker.request.lock().unwrap() = Some(ToolInvocationRequest {
            invocation_id: Some("inv".to_string()),
            session_id: "sess".to_string(),
            tool_name: "Read".to_string(),
            arguments: Map::new(),
            cwd: None,
            timeout_ms: None,
            max_output_bytes: None,
            idempotency_key: None,
            required_capabilities: vec![],
            metadata: Map::new(),
        });

        let worker = Worker::new("worker").with_idle_sleep(Duration::from_millis(1));
        let stats = worker.run_until_idle(&broker, &EchoHost, 1).await.unwrap();

        assert_eq!(stats.completed, 1);
        assert_eq!(stats.failed, 0);
        assert_eq!(stats.idle_ticks, 1);
    }

    #[test]
    fn worker_clamps_zero_idle_sleep_to_prevent_idle_spin() {
        let worker = Worker::new("worker").with_idle_sleep(Duration::ZERO);

        assert_eq!(worker.idle_sleep, Duration::from_millis(1));
    }

    #[test]
    fn http_host_client_rejects_unsafe_base_urls() {
        for base_url in [
            "file:///tmp/executioner",
            "http:///tmp/executioner",
            "http://user:pass@127.0.0.1:9/",
            "http://127.0.0.1:9/?token=secret",
            "http://127.0.0.1:9/#fragment",
        ] {
            let err = HttpHostClient::new(base_url).unwrap_err();
            assert!(
                err.to_string().contains("invalid host base url"),
                "unexpected error for {base_url}: {err}"
            );
        }
    }

    #[tokio::test]
    async fn http_host_client_preserves_base_url_path_prefix_without_trailing_slash() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut buffer = [0_u8; 4096];
            let bytes = socket.read(&mut buffer).await.unwrap();
            let request = String::from_utf8_lossy(&buffer[..bytes]);
            assert!(
                request.starts_with("POST /api/sessions/sess/invocations HTTP/1.1"),
                "{request}"
            );
            let response = "HTTP/1.1 500 Internal Server Error\r\ncontent-length: 0\r\nconnection: close\r\n\r\n";
            socket.write_all(response.as_bytes()).await.unwrap();
        });
        let client = HttpHostClient::new(format!("http://{addr}/api")).unwrap();

        let _ = client
            .execute(ToolInvocationRequest {
                invocation_id: Some("inv".to_string()),
                session_id: "sess".to_string(),
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

        server.await.unwrap();
    }

    #[tokio::test]
    async fn worker_fails_original_claim_when_host_returns_mismatched_invocation_id() {
        let broker = Arc::new(MemoryBroker::default());
        *broker.request.lock().unwrap() = Some(ToolInvocationRequest {
            invocation_id: Some("original".to_string()),
            session_id: "sess".to_string(),
            tool_name: "Read".to_string(),
            arguments: Map::new(),
            cwd: None,
            timeout_ms: None,
            max_output_bytes: None,
            idempotency_key: None,
            required_capabilities: vec![],
            metadata: Map::new(),
        });

        let result = Worker::new("worker")
            .run_once(&broker, &WrongIdentityHost)
            .await
            .unwrap();
        let failed = broker.failed.lock().unwrap();

        assert_eq!(result, WorkerRunOnce::Failed);
        assert_eq!(*broker.completed.lock().unwrap(), 0);
        assert_eq!(failed.len(), 1);
        assert_eq!(failed[0].invocation_id, "original");
        assert_eq!(failed[0].error.code, "host_result_mismatch");
    }

    #[tokio::test]
    async fn worker_fails_original_claim_when_host_returns_mismatched_tool_name() {
        let broker = Arc::new(MemoryBroker::default());
        *broker.request.lock().unwrap() = Some(ToolInvocationRequest {
            invocation_id: Some("original_tool".to_string()),
            session_id: "sess".to_string(),
            tool_name: "Read".to_string(),
            arguments: Map::new(),
            cwd: None,
            timeout_ms: None,
            max_output_bytes: None,
            idempotency_key: None,
            required_capabilities: vec![],
            metadata: Map::new(),
        });

        let result = Worker::new("worker")
            .run_once(&broker, &WrongToolHost)
            .await
            .unwrap();
        let failed = broker.failed.lock().unwrap();

        assert_eq!(result, WorkerRunOnce::Failed);
        assert_eq!(*broker.completed.lock().unwrap(), 0);
        assert_eq!(failed.len(), 1);
        assert_eq!(failed[0].invocation_id, "original_tool");
        assert_eq!(failed[0].error.code, "host_result_mismatch");
    }

    #[tokio::test]
    async fn file_broker_claims_to_claimed_and_completes_to_completed() {
        let temp = tempfile::TempDir::new().unwrap();
        let broker = FileBroker::new(temp.path()).unwrap();
        let request = ToolInvocationRequest {
            invocation_id: Some("inv_file".to_string()),
            session_id: "sess".to_string(),
            tool_name: "Read".to_string(),
            arguments: Map::new(),
            cwd: None,
            timeout_ms: None,
            max_output_bytes: None,
            idempotency_key: None,
            required_capabilities: vec![],
            metadata: Map::new(),
        };

        broker.enqueue(&request).unwrap();
        let claim = broker.claim_next("worker").await.unwrap().unwrap();

        assert_eq!(claim.request.invocation_id.as_deref(), Some("inv_file"));
        assert!(temp.path().join("claimed/inv_file.json").exists());
        assert!(!temp.path().join("pending/inv_file.json").exists());

        broker
            .complete(ToolInvocationCompleted {
                event_type: "tool.invocation.completed".to_string(),
                invocation_id: "inv_file".to_string(),
                session_id: "sess".to_string(),
                attempt_id: Some(claim.attempt_id),
                lease_token: Some(claim.lease_token),
                result: ToolInvocationResult {
                    invocation_id: "inv_file".to_string(),
                    session_id: "sess".to_string(),
                    tool_name: "Read".to_string(),
                    status: ToolResultStatus::Success,
                    output: "ok".to_string(),
                    error: None,
                    summary: None,
                    effects: vec![],
                    duration_ms: 0,
                    metadata: Map::new(),
                },
                completed_at: "now".to_string(),
            })
            .await
            .unwrap();

        assert!(temp.path().join("completed/inv_file.json").exists());
        assert!(temp.path().join("claimed/inv_file.json").exists());
        assert!(broker.read_completed("inv_file").unwrap().is_some());
    }

    #[tokio::test]
    async fn file_broker_claim_normalizes_missing_invocation_id() {
        let temp = tempfile::TempDir::new().unwrap();
        let broker = FileBroker::new(temp.path()).unwrap();
        let request = ToolInvocationRequest {
            invocation_id: None,
            session_id: "sess".to_string(),
            tool_name: "Read".to_string(),
            arguments: Map::new(),
            cwd: None,
            timeout_ms: None,
            max_output_bytes: None,
            idempotency_key: None,
            required_capabilities: vec![],
            metadata: Map::new(),
        };
        write_json_atomic(
            &temp.path().join("pending/manual_missing_id.json"),
            &request,
        )
        .unwrap();

        let claim = broker.claim_next("worker").await.unwrap().unwrap();
        let invocation_id = claim.request.invocation_id.as_deref().unwrap();
        let claimed_path = temp.path().join(format!("claimed/{invocation_id}.json"));
        let envelope = read_json_required::<ClaimEnvelope>(&claimed_path).unwrap();

        assert!(invocation_id.starts_with("inv_"));
        assert_eq!(
            envelope.request.invocation_id.as_deref(),
            Some(invocation_id)
        );
        assert!(!temp.path().join("pending/manual_missing_id.json").exists());
    }

    #[test]
    fn file_broker_rejects_symlink_queue_state_directory() {
        #[cfg(unix)]
        {
            let temp = tempfile::TempDir::new().unwrap();
            let queue_dir = temp.path().join("queue");
            let outside_pending = temp.path().join("outside-pending");
            fs::create_dir_all(&queue_dir).unwrap();
            fs::create_dir_all(&outside_pending).unwrap();
            std::os::unix::fs::symlink(&outside_pending, queue_dir.join("pending")).unwrap();

            let err = FileBroker::new(&queue_dir).unwrap_err();

            assert!(err.to_string().contains("queue state directory"));
            assert!(outside_pending.read_dir().unwrap().next().is_none());
        }
    }

    #[test]
    fn file_broker_rejects_swapped_queue_state_directory_before_enqueue() {
        #[cfg(unix)]
        {
            let temp = tempfile::TempDir::new().unwrap();
            let queue_dir = temp.path().join("queue");
            let outside_pending = temp.path().join("outside-pending");
            let broker = FileBroker::new(&queue_dir).unwrap();
            fs::create_dir_all(&outside_pending).unwrap();
            fs::remove_dir(queue_dir.join("pending")).unwrap();
            std::os::unix::fs::symlink(&outside_pending, queue_dir.join("pending")).unwrap();
            let request = ToolInvocationRequest {
                invocation_id: Some("swapped".to_string()),
                session_id: "sess".to_string(),
                tool_name: "Read".to_string(),
                arguments: Map::new(),
                cwd: None,
                timeout_ms: None,
                max_output_bytes: None,
                idempotency_key: None,
                required_capabilities: vec![],
                metadata: Map::new(),
            };

            let err = broker.enqueue(&request).unwrap_err();

            assert!(err.to_string().contains("queue state directory"));
            assert!(!outside_pending.join("swapped.json").exists());
        }
    }

    #[test]
    fn file_broker_rejects_symlink_queue_root_directory() {
        #[cfg(unix)]
        {
            let temp = tempfile::TempDir::new().unwrap();
            let queue_dir = temp.path().join("queue");
            let outside_queue = temp.path().join("outside-queue");
            fs::create_dir_all(&outside_queue).unwrap();
            std::os::unix::fs::symlink(&outside_queue, &queue_dir).unwrap();

            let err = FileBroker::new(&queue_dir).unwrap_err();

            assert!(err.to_string().contains("queue directory"));
            assert!(outside_queue.read_dir().unwrap().next().is_none());
        }
    }

    #[test]
    fn file_broker_rejects_symlink_queue_parent_directory() {
        #[cfg(unix)]
        {
            let temp = tempfile::TempDir::new().unwrap();
            let outside_queue = temp.path().join("outside-queue");
            let link_parent = temp.path().join("link-parent");
            fs::create_dir_all(&outside_queue).unwrap();
            std::os::unix::fs::symlink(&outside_queue, &link_parent).unwrap();

            let err = FileBroker::new(link_parent.join("queue")).unwrap_err();

            assert!(err.to_string().contains("parent must not contain symlinks"));
            assert!(outside_queue.read_dir().unwrap().next().is_none());
        }
    }

    #[test]
    fn file_broker_rejects_symlink_queue_ancestor_directory() {
        #[cfg(unix)]
        {
            let temp = tempfile::TempDir::new().unwrap();
            let outside_queue = temp.path().join("outside-queue");
            let link_parent = temp.path().join("link-parent");
            fs::create_dir_all(outside_queue.join("existing")).unwrap();
            std::os::unix::fs::symlink(&outside_queue, &link_parent).unwrap();

            let err = FileBroker::new(link_parent.join("existing/queue")).unwrap_err();

            assert!(err.to_string().contains("parent must not contain symlinks"));
            assert!(outside_queue
                .join("existing")
                .read_dir()
                .unwrap()
                .next()
                .is_none());
        }
    }

    #[test]
    fn file_broker_rejects_dangling_symlink_invocation_id_as_duplicate() {
        #[cfg(unix)]
        {
            let temp = tempfile::TempDir::new().unwrap();
            let broker = FileBroker::new(temp.path().join("queue")).unwrap();
            let dangling = temp.path().join("queue/pending/dangling.json");
            std::os::unix::fs::symlink(temp.path().join("missing.json"), &dangling).unwrap();
            let request = ToolInvocationRequest {
                invocation_id: Some("dangling".to_string()),
                session_id: "sess".to_string(),
                tool_name: "Read".to_string(),
                arguments: Map::new(),
                cwd: None,
                timeout_ms: None,
                max_output_bytes: None,
                idempotency_key: None,
                required_capabilities: vec![],
                metadata: Map::new(),
            };

            let err = broker.enqueue(&request).unwrap_err();

            assert!(err.to_string().contains("duplicate invocation id"));
            assert!(dangling
                .symlink_metadata()
                .unwrap()
                .file_type()
                .is_symlink());
        }
    }

    #[tokio::test]
    async fn worker_records_failed_event_for_mismatched_host_result_in_file_broker() {
        let temp = tempfile::TempDir::new().unwrap();
        let broker = FileBroker::new(temp.path().join("queue")).unwrap();
        broker
            .enqueue(&ToolInvocationRequest {
                invocation_id: Some("file_original".to_string()),
                session_id: "sess".to_string(),
                tool_name: "Read".to_string(),
                arguments: Map::new(),
                cwd: None,
                timeout_ms: None,
                max_output_bytes: None,
                idempotency_key: None,
                required_capabilities: vec![],
                metadata: Map::new(),
            })
            .unwrap();

        let result = Worker::new("worker")
            .run_once(&broker, &WrongIdentityHost)
            .await
            .unwrap();
        let failed = broker.read_failed("file_original").unwrap().unwrap();

        assert_eq!(result, WorkerRunOnce::Failed);
        assert_eq!(failed.invocation_id, "file_original");
        assert_eq!(failed.session_id, "sess");
        assert_eq!(failed.error.code, "host_result_mismatch");
        assert!(temp
            .path()
            .join("queue/claimed/file_original.json")
            .exists());
        assert!(!temp
            .path()
            .join("queue/completed/file_original.json")
            .exists());
        assert!(!temp
            .path()
            .join("queue/completed/other_invocation.json")
            .exists());
    }

    #[tokio::test]
    async fn worker_records_failed_event_for_mismatched_host_tool_name_in_file_broker() {
        let temp = tempfile::TempDir::new().unwrap();
        let broker = FileBroker::new(temp.path().join("queue")).unwrap();
        broker
            .enqueue(&ToolInvocationRequest {
                invocation_id: Some("file_original_tool".to_string()),
                session_id: "sess".to_string(),
                tool_name: "Read".to_string(),
                arguments: Map::new(),
                cwd: None,
                timeout_ms: None,
                max_output_bytes: None,
                idempotency_key: None,
                required_capabilities: vec![],
                metadata: Map::new(),
            })
            .unwrap();

        let result = Worker::new("worker")
            .run_once(&broker, &WrongToolHost)
            .await
            .unwrap();
        let failed = broker.read_failed("file_original_tool").unwrap().unwrap();

        assert_eq!(result, WorkerRunOnce::Failed);
        assert_eq!(failed.invocation_id, "file_original_tool");
        assert_eq!(failed.session_id, "sess");
        assert_eq!(failed.error.code, "host_result_mismatch");
        assert!(temp
            .path()
            .join("queue/claimed/file_original_tool.json")
            .exists());
        assert!(!temp
            .path()
            .join("queue/completed/file_original_tool.json")
            .exists());
    }

    #[tokio::test]
    async fn http_host_client_rejects_invalid_session_id_before_building_url() {
        let client = HttpHostClient::new("http://127.0.0.1:9/").unwrap();

        let err = client
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

        assert!(err.to_string().contains("invalid session id"));
    }

    #[tokio::test]
    async fn http_host_client_caps_error_response_body() {
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
            socket.write_all(response.as_bytes()).await.unwrap();
        });
        let client = HttpHostClient::new(format!("http://{addr}/")).unwrap();

        let err = client
            .execute(ToolInvocationRequest {
                invocation_id: Some("inv".to_string()),
                session_id: "sess".to_string(),
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

        let message = err.to_string();
        assert!(message.len() < 80 * 1024);
        assert!(message.contains("truncated"));
        server.await.unwrap();
    }

    #[tokio::test]
    async fn http_host_client_does_not_follow_redirects_with_request_body() {
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
        let client = HttpHostClient::new(format!("http://{redirect_addr}/")).unwrap();

        let err = client
            .execute(ToolInvocationRequest {
                invocation_id: Some("inv".to_string()),
                session_id: "sess".to_string(),
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

        redirect_server.await.unwrap();
        let captured = capture_server.await.unwrap();
        assert!(err.to_string().contains("307"));
        assert!(!captured, "redirect target received the invocation body");
    }

    #[tokio::test]
    async fn http_host_client_rejects_oversized_success_response_body() {
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
            let _ = socket.write_all(response.as_bytes()).await;
        });
        let client = HttpHostClient::new(format!("http://{addr}/")).unwrap();

        let err = client
            .execute(ToolInvocationRequest {
                invocation_id: Some("inv".to_string()),
                session_id: "sess".to_string(),
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

        assert!(err.to_string().contains("response body exceeds"));
        server.await.unwrap();
    }

    #[test]
    fn file_broker_rejects_path_traversal_invocation_id_on_enqueue() {
        let temp = tempfile::TempDir::new().unwrap();
        let broker = FileBroker::new(temp.path().join("queue")).unwrap();
        let request = ToolInvocationRequest {
            invocation_id: Some("../escaped".to_string()),
            session_id: "sess".to_string(),
            tool_name: "Read".to_string(),
            arguments: Map::new(),
            cwd: None,
            timeout_ms: None,
            max_output_bytes: None,
            idempotency_key: None,
            required_capabilities: vec![],
            metadata: Map::new(),
        };

        let err = broker.enqueue(&request).unwrap_err();

        assert!(err.to_string().contains("invalid invocation id"));
        assert!(!temp.path().join("queue/escaped.json").exists());
    }

    #[test]
    fn file_broker_rejects_duplicate_pending_invocation_id_without_overwriting() {
        let temp = tempfile::TempDir::new().unwrap();
        let broker = FileBroker::new(temp.path().join("queue")).unwrap();
        let first = ToolInvocationRequest {
            invocation_id: Some("dup_pending".to_string()),
            session_id: "sess".to_string(),
            tool_name: "Read".to_string(),
            arguments: Map::new(),
            cwd: None,
            timeout_ms: None,
            max_output_bytes: None,
            idempotency_key: None,
            required_capabilities: vec![],
            metadata: Map::new(),
        };
        let mut second = first.clone();
        second.tool_name = "Write".to_string();

        broker.enqueue(&first).unwrap();
        let err = broker.enqueue(&second).unwrap_err();
        let pending: ToolInvocationRequest =
            read_json_required(&temp.path().join("queue/pending/dup_pending.json")).unwrap();

        assert!(err.to_string().contains("duplicate invocation id"));
        assert_eq!(pending.tool_name, "Read");
    }

    #[tokio::test]
    async fn file_broker_enqueue_assigns_generated_invocation_id_to_request() {
        let temp = tempfile::TempDir::new().unwrap();
        let broker = FileBroker::new(temp.path().join("queue")).unwrap();
        let request = ToolInvocationRequest {
            invocation_id: None,
            session_id: "sess".to_string(),
            tool_name: "Read".to_string(),
            arguments: Map::new(),
            cwd: None,
            timeout_ms: None,
            max_output_bytes: None,
            idempotency_key: None,
            required_capabilities: vec![],
            metadata: Map::new(),
        };

        let pending_path = broker.enqueue(&request).unwrap();
        let pending: ToolInvocationRequest = read_json_required(&pending_path).unwrap();
        let claim = broker.claim_next("worker").await.unwrap().unwrap();

        assert!(pending.invocation_id.is_some());
        assert_eq!(pending.invocation_id, claim.request.invocation_id);
        assert!(claim.request.invocation_id.unwrap().starts_with("inv_"));
    }

    #[tokio::test]
    async fn file_broker_rejects_duplicate_completed_invocation_id_on_enqueue() {
        let temp = tempfile::TempDir::new().unwrap();
        let broker = FileBroker::new(temp.path().join("queue")).unwrap();
        let request = ToolInvocationRequest {
            invocation_id: Some("done".to_string()),
            session_id: "sess".to_string(),
            tool_name: "Read".to_string(),
            arguments: Map::new(),
            cwd: None,
            timeout_ms: None,
            max_output_bytes: None,
            idempotency_key: None,
            required_capabilities: vec![],
            metadata: Map::new(),
        };
        broker.enqueue(&request).unwrap();
        let claim = broker.claim_next("worker").await.unwrap().unwrap();
        broker
            .complete(ToolInvocationCompleted {
                event_type: "tool.invocation.completed".to_string(),
                invocation_id: "done".to_string(),
                session_id: "sess".to_string(),
                attempt_id: Some(claim.attempt_id),
                lease_token: Some(claim.lease_token),
                result: ToolInvocationResult {
                    invocation_id: "done".to_string(),
                    session_id: "sess".to_string(),
                    tool_name: "Read".to_string(),
                    status: ToolResultStatus::Success,
                    output: "ok".to_string(),
                    error: None,
                    summary: None,
                    effects: vec![],
                    duration_ms: 0,
                    metadata: Map::new(),
                },
                completed_at: "now".to_string(),
            })
            .await
            .unwrap();

        let err = broker.enqueue(&request).unwrap_err();

        assert!(err.to_string().contains("duplicate invocation id"));
        assert!(!temp.path().join("queue/pending/done.json").exists());
    }

    #[test]
    fn file_broker_quarantines_completed_event_with_mismatched_result_identity() {
        let temp = tempfile::TempDir::new().unwrap();
        let broker = FileBroker::new(temp.path().join("queue")).unwrap();
        write_json_atomic(
            &temp.path().join("queue/completed/original.json"),
            &ToolInvocationCompleted {
                event_type: "tool.invocation.completed".to_string(),
                invocation_id: "original".to_string(),
                session_id: "sess".to_string(),
                attempt_id: Some("attempt".to_string()),
                lease_token: Some("lease".to_string()),
                result: ToolInvocationResult {
                    invocation_id: "other".to_string(),
                    session_id: "sess".to_string(),
                    tool_name: "Read".to_string(),
                    status: ToolResultStatus::Success,
                    output: "wrong".to_string(),
                    error: None,
                    summary: None,
                    effects: vec![],
                    duration_ms: 0,
                    metadata: Map::new(),
                },
                completed_at: "now".to_string(),
            },
        )
        .unwrap();

        let completed = broker.read_completed("original").unwrap();

        assert!(completed.is_none());
        assert!(!temp.path().join("queue/completed/original.json").exists());
        assert_eq!(
            fs::read_dir(temp.path().join("queue/rejected"))
                .unwrap()
                .count(),
            1
        );
    }

    #[test]
    fn file_broker_quarantines_completed_event_with_invalid_terminal_identity() {
        let temp = tempfile::TempDir::new().unwrap();
        let broker = FileBroker::new(temp.path().join("queue")).unwrap();
        write_json_atomic(
            &temp.path().join("queue/completed/bad_session.json"),
            &ToolInvocationCompleted {
                event_type: "tool.invocation.completed".to_string(),
                invocation_id: "bad_session".to_string(),
                session_id: "../escaped".to_string(),
                attempt_id: None,
                lease_token: None,
                result: ToolInvocationResult {
                    invocation_id: "bad_session".to_string(),
                    session_id: "../escaped".to_string(),
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
            },
        )
        .unwrap();

        let completed = broker.read_completed("bad_session").unwrap();

        assert!(completed.is_none());
        assert!(!temp
            .path()
            .join("queue/completed/bad_session.json")
            .exists());
        assert_eq!(
            fs::read_dir(temp.path().join("queue/rejected"))
                .unwrap()
                .count(),
            1
        );
    }

    #[test]
    fn file_broker_quarantines_oversized_completed_event_without_accepting_it() {
        let temp = tempfile::TempDir::new().unwrap();
        let broker = FileBroker::new(temp.path().join("queue")).unwrap();
        fs::write(
            temp.path().join("queue/completed/huge_completed.json"),
            serde_json::to_vec_pretty(&ToolInvocationCompleted {
                event_type: "tool.invocation.completed".to_string(),
                invocation_id: "huge_completed".to_string(),
                session_id: "sess".to_string(),
                attempt_id: None,
                lease_token: None,
                result: ToolInvocationResult {
                    invocation_id: "huge_completed".to_string(),
                    session_id: "sess".to_string(),
                    tool_name: "Read".to_string(),
                    status: ToolResultStatus::Success,
                    output: "x".repeat(10 * 1024 * 1024),
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

        let completed = broker.read_completed("huge_completed").unwrap();

        assert!(completed.is_none());
        assert!(!temp
            .path()
            .join("queue/completed/huge_completed.json")
            .exists());
        assert_eq!(
            fs::read_dir(temp.path().join("queue/rejected"))
                .unwrap()
                .count(),
            1
        );
    }

    #[test]
    fn file_broker_quarantines_completed_event_while_invocation_is_pending() {
        let temp = tempfile::TempDir::new().unwrap();
        let broker = FileBroker::new(temp.path().join("queue")).unwrap();
        let request = ToolInvocationRequest {
            invocation_id: Some("pending_done".to_string()),
            session_id: "sess".to_string(),
            tool_name: "Read".to_string(),
            arguments: Map::new(),
            cwd: None,
            timeout_ms: None,
            max_output_bytes: None,
            idempotency_key: None,
            required_capabilities: vec![],
            metadata: Map::new(),
        };
        broker.enqueue(&request).unwrap();
        write_json_atomic(
            &temp.path().join("queue/completed/pending_done.json"),
            &ToolInvocationCompleted {
                event_type: "tool.invocation.completed".to_string(),
                invocation_id: "pending_done".to_string(),
                session_id: "sess".to_string(),
                attempt_id: Some("attempt".to_string()),
                lease_token: Some("lease".to_string()),
                result: ToolInvocationResult {
                    invocation_id: "pending_done".to_string(),
                    session_id: "sess".to_string(),
                    tool_name: "Read".to_string(),
                    status: ToolResultStatus::Success,
                    output: "forged before claim".to_string(),
                    error: None,
                    summary: None,
                    effects: vec![],
                    duration_ms: 0,
                    metadata: Map::new(),
                },
                completed_at: "now".to_string(),
            },
        )
        .unwrap();

        let completed = broker.read_completed("pending_done").unwrap();

        assert!(completed.is_none());
        assert!(temp.path().join("queue/pending/pending_done.json").exists());
        assert!(!temp
            .path()
            .join("queue/completed/pending_done.json")
            .exists());
        assert_eq!(
            fs::read_dir(temp.path().join("queue/rejected"))
                .unwrap()
                .count(),
            1
        );
    }

    #[test]
    fn file_broker_quarantines_unclaimed_completed_event_without_lease_material() {
        let temp = tempfile::TempDir::new().unwrap();
        let broker = FileBroker::new(temp.path().join("queue")).unwrap();
        write_json_atomic(
            &temp.path().join("queue/completed/orphan_done.json"),
            &ToolInvocationCompleted {
                event_type: "tool.invocation.completed".to_string(),
                invocation_id: "orphan_done".to_string(),
                session_id: "sess".to_string(),
                attempt_id: None,
                lease_token: None,
                result: ToolInvocationResult {
                    invocation_id: "orphan_done".to_string(),
                    session_id: "sess".to_string(),
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
            },
        )
        .unwrap();

        let completed = broker.read_completed("orphan_done").unwrap();

        assert!(completed.is_none());
        assert!(!temp
            .path()
            .join("queue/completed/orphan_done.json")
            .exists());
        assert_eq!(
            fs::read_dir(temp.path().join("queue/rejected"))
                .unwrap()
                .count(),
            1
        );
    }

    #[test]
    fn file_broker_quarantines_unclaimed_completed_event_with_forged_lease_material() {
        let temp = tempfile::TempDir::new().unwrap();
        let broker = FileBroker::new(temp.path().join("queue")).unwrap();
        write_json_atomic(
            &temp.path().join("queue/completed/orphan_done.json"),
            &ToolInvocationCompleted {
                event_type: "tool.invocation.completed".to_string(),
                invocation_id: "orphan_done".to_string(),
                session_id: "sess".to_string(),
                attempt_id: Some("attempt_forged".to_string()),
                lease_token: Some("lease_forged".to_string()),
                result: ToolInvocationResult {
                    invocation_id: "orphan_done".to_string(),
                    session_id: "sess".to_string(),
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
            },
        )
        .unwrap();

        let completed = broker.read_completed("orphan_done").unwrap();

        assert!(completed.is_none());
        assert!(!temp
            .path()
            .join("queue/completed/orphan_done.json")
            .exists());
        assert_eq!(
            fs::read_dir(temp.path().join("queue/rejected"))
                .unwrap()
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn file_broker_quarantines_completed_event_with_wrong_lease_while_claimed() {
        let temp = tempfile::TempDir::new().unwrap();
        let broker = FileBroker::new(temp.path().join("queue")).unwrap();
        let request = ToolInvocationRequest {
            invocation_id: Some("claimed_done".to_string()),
            session_id: "sess".to_string(),
            tool_name: "Read".to_string(),
            arguments: Map::new(),
            cwd: None,
            timeout_ms: None,
            max_output_bytes: None,
            idempotency_key: None,
            required_capabilities: vec![],
            metadata: Map::new(),
        };
        broker.enqueue(&request).unwrap();
        let claim = broker.claim_next("worker").await.unwrap().unwrap();
        write_json_atomic(
            &temp.path().join("queue/completed/claimed_done.json"),
            &ToolInvocationCompleted {
                event_type: "tool.invocation.completed".to_string(),
                invocation_id: "claimed_done".to_string(),
                session_id: "sess".to_string(),
                attempt_id: Some(claim.attempt_id),
                lease_token: Some("wrong_lease".to_string()),
                result: ToolInvocationResult {
                    invocation_id: "claimed_done".to_string(),
                    session_id: "sess".to_string(),
                    tool_name: "Read".to_string(),
                    status: ToolResultStatus::Success,
                    output: "forged while claimed".to_string(),
                    error: None,
                    summary: None,
                    effects: vec![],
                    duration_ms: 0,
                    metadata: Map::new(),
                },
                completed_at: "now".to_string(),
            },
        )
        .unwrap();

        let completed = broker.read_completed("claimed_done").unwrap();

        assert!(completed.is_none());
        assert!(temp.path().join("queue/claimed/claimed_done.json").exists());
        assert!(!temp
            .path()
            .join("queue/completed/claimed_done.json")
            .exists());
        assert_eq!(
            fs::read_dir(temp.path().join("queue/rejected"))
                .unwrap()
                .count(),
            1
        );
    }

    #[test]
    fn file_broker_quarantines_completed_event_when_claim_lease_is_malformed() {
        let temp = tempfile::TempDir::new().unwrap();
        let broker = FileBroker::new(temp.path().join("queue")).unwrap();
        fs::write(
            temp.path().join("queue/claimed/malformed_claim.json"),
            b"{not json",
        )
        .unwrap();
        write_json_atomic(
            &temp.path().join("queue/completed/malformed_claim.json"),
            &ToolInvocationCompleted {
                event_type: "tool.invocation.completed".to_string(),
                invocation_id: "malformed_claim".to_string(),
                session_id: "sess".to_string(),
                attempt_id: Some("attempt".to_string()),
                lease_token: Some("lease".to_string()),
                result: ToolInvocationResult {
                    invocation_id: "malformed_claim".to_string(),
                    session_id: "sess".to_string(),
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
            },
        )
        .unwrap();

        let completed = broker.read_completed("malformed_claim").unwrap();

        assert!(completed.is_none());
        assert!(temp
            .path()
            .join("queue/claimed/malformed_claim.json")
            .exists());
        assert!(!temp
            .path()
            .join("queue/completed/malformed_claim.json")
            .exists());
        assert_eq!(
            fs::read_dir(temp.path().join("queue/rejected"))
                .unwrap()
                .count(),
            1
        );
    }

    #[test]
    fn file_broker_quarantines_completed_event_when_claim_lease_has_unknown_fields() {
        let temp = tempfile::TempDir::new().unwrap();
        let broker = FileBroker::new(temp.path().join("queue")).unwrap();
        write_json_atomic(
            &temp.path().join("queue/claimed/unknown_claim_field.json"),
            &serde_json::json!({
                "workerId": "worker",
                "attemptId": "attempt",
                "leaseToken": "lease",
                "claimedAt": "now",
                "request": {
                    "invocationId": "unknown_claim_field",
                    "sessionId": "sess",
                    "toolName": "Read",
                    "arguments": {},
                    "requiredCapabilities": [],
                    "metadata": {}
                },
                "nextLeaseToken": "forged"
            }),
        )
        .unwrap();
        write_json_atomic(
            &temp.path().join("queue/completed/unknown_claim_field.json"),
            &ToolInvocationCompleted {
                event_type: "tool.invocation.completed".to_string(),
                invocation_id: "unknown_claim_field".to_string(),
                session_id: "sess".to_string(),
                attempt_id: Some("attempt".to_string()),
                lease_token: Some("lease".to_string()),
                result: ToolInvocationResult {
                    invocation_id: "unknown_claim_field".to_string(),
                    session_id: "sess".to_string(),
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
            },
        )
        .unwrap();

        let completed = broker.read_completed("unknown_claim_field").unwrap();

        assert!(completed.is_none());
        assert!(temp
            .path()
            .join("queue/claimed/unknown_claim_field.json")
            .exists());
        assert!(!temp
            .path()
            .join("queue/completed/unknown_claim_field.json")
            .exists());
        assert_eq!(
            fs::read_dir(temp.path().join("queue/rejected"))
                .unwrap()
                .count(),
            1
        );
    }

    #[test]
    fn file_broker_quarantines_failed_event_with_mismatched_identity() {
        let temp = tempfile::TempDir::new().unwrap();
        let broker = FileBroker::new(temp.path().join("queue")).unwrap();
        write_json_atomic(
            &temp.path().join("queue/failed/original.json"),
            &ToolInvocationFailed {
                event_type: "tool.invocation.failed".to_string(),
                invocation_id: "other".to_string(),
                session_id: "sess".to_string(),
                attempt_id: Some("attempt".to_string()),
                lease_token: Some("lease".to_string()),
                error: ErrorEnvelope {
                    code: "bad".to_string(),
                    message: "wrong invocation".to_string(),
                    retryable: false,
                },
                failed_at: "now".to_string(),
            },
        )
        .unwrap();

        let failed = broker.read_failed("original").unwrap();

        assert!(failed.is_none());
        assert!(!temp.path().join("queue/failed/original.json").exists());
        assert_eq!(
            fs::read_dir(temp.path().join("queue/rejected"))
                .unwrap()
                .count(),
            1
        );
    }

    #[test]
    fn file_broker_quarantines_failed_event_with_invalid_terminal_identity() {
        let temp = tempfile::TempDir::new().unwrap();
        let broker = FileBroker::new(temp.path().join("queue")).unwrap();
        write_json_atomic(
            &temp.path().join("queue/failed/bad_failed_session.json"),
            &ToolInvocationFailed {
                event_type: "tool.invocation.failed".to_string(),
                invocation_id: "bad_failed_session".to_string(),
                session_id: "../escaped".to_string(),
                attempt_id: None,
                lease_token: None,
                error: ErrorEnvelope {
                    code: "bad".to_string(),
                    message: "forged failure".to_string(),
                    retryable: false,
                },
                failed_at: "now".to_string(),
            },
        )
        .unwrap();

        let failed = broker.read_failed("bad_failed_session").unwrap();

        assert!(failed.is_none());
        assert!(!temp
            .path()
            .join("queue/failed/bad_failed_session.json")
            .exists());
        assert_eq!(
            fs::read_dir(temp.path().join("queue/rejected"))
                .unwrap()
                .count(),
            1
        );
    }

    #[test]
    fn file_broker_quarantines_failed_event_with_empty_error_payload() {
        let temp = tempfile::TempDir::new().unwrap();
        let broker = FileBroker::new(temp.path().join("queue")).unwrap();
        write_json_atomic(
            &temp.path().join("queue/failed/empty_error.json"),
            &ToolInvocationFailed {
                event_type: "tool.invocation.failed".to_string(),
                invocation_id: "empty_error".to_string(),
                session_id: "sess".to_string(),
                attempt_id: None,
                lease_token: None,
                error: ErrorEnvelope {
                    code: " ".to_string(),
                    message: "".to_string(),
                    retryable: false,
                },
                failed_at: "now".to_string(),
            },
        )
        .unwrap();

        let failed = broker.read_failed("empty_error").unwrap();

        assert!(failed.is_none());
        assert!(!temp.path().join("queue/failed/empty_error.json").exists());
        assert_eq!(
            fs::read_dir(temp.path().join("queue/rejected"))
                .unwrap()
                .count(),
            1
        );
    }

    #[test]
    fn file_broker_quarantines_failed_event_while_invocation_is_pending() {
        let temp = tempfile::TempDir::new().unwrap();
        let broker = FileBroker::new(temp.path().join("queue")).unwrap();
        let request = ToolInvocationRequest {
            invocation_id: Some("pending_failed".to_string()),
            session_id: "sess".to_string(),
            tool_name: "Read".to_string(),
            arguments: Map::new(),
            cwd: None,
            timeout_ms: None,
            max_output_bytes: None,
            idempotency_key: None,
            required_capabilities: vec![],
            metadata: Map::new(),
        };
        broker.enqueue(&request).unwrap();
        write_json_atomic(
            &temp.path().join("queue/failed/pending_failed.json"),
            &ToolInvocationFailed {
                event_type: "tool.invocation.failed".to_string(),
                invocation_id: "pending_failed".to_string(),
                session_id: "sess".to_string(),
                attempt_id: Some("attempt".to_string()),
                lease_token: Some("lease".to_string()),
                error: ErrorEnvelope {
                    code: "forged".to_string(),
                    message: "forged before claim".to_string(),
                    retryable: false,
                },
                failed_at: "now".to_string(),
            },
        )
        .unwrap();

        let failed = broker.read_failed("pending_failed").unwrap();

        assert!(failed.is_none());
        assert!(temp
            .path()
            .join("queue/pending/pending_failed.json")
            .exists());
        assert!(!temp
            .path()
            .join("queue/failed/pending_failed.json")
            .exists());
        assert_eq!(
            fs::read_dir(temp.path().join("queue/rejected"))
                .unwrap()
                .count(),
            1
        );
    }

    #[test]
    fn file_broker_quarantines_unclaimed_failed_event_without_lease_material() {
        let temp = tempfile::TempDir::new().unwrap();
        let broker = FileBroker::new(temp.path().join("queue")).unwrap();
        write_json_atomic(
            &temp.path().join("queue/failed/orphan_failed.json"),
            &ToolInvocationFailed {
                event_type: "tool.invocation.failed".to_string(),
                invocation_id: "orphan_failed".to_string(),
                session_id: "sess".to_string(),
                attempt_id: None,
                lease_token: None,
                error: ErrorEnvelope {
                    code: "forged".to_string(),
                    message: "forged without a claim".to_string(),
                    retryable: false,
                },
                failed_at: "now".to_string(),
            },
        )
        .unwrap();

        let failed = broker.read_failed("orphan_failed").unwrap();

        assert!(failed.is_none());
        assert!(!temp.path().join("queue/failed/orphan_failed.json").exists());
        assert_eq!(
            fs::read_dir(temp.path().join("queue/rejected"))
                .unwrap()
                .count(),
            1
        );
    }

    #[test]
    fn file_broker_quarantines_unclaimed_failed_event_with_forged_lease_material() {
        let temp = tempfile::TempDir::new().unwrap();
        let broker = FileBroker::new(temp.path().join("queue")).unwrap();
        write_json_atomic(
            &temp.path().join("queue/failed/orphan_failed.json"),
            &ToolInvocationFailed {
                event_type: "tool.invocation.failed".to_string(),
                invocation_id: "orphan_failed".to_string(),
                session_id: "sess".to_string(),
                attempt_id: Some("attempt_forged".to_string()),
                lease_token: Some("lease_forged".to_string()),
                error: ErrorEnvelope {
                    code: "forged".to_string(),
                    message: "forged without a claim".to_string(),
                    retryable: false,
                },
                failed_at: "now".to_string(),
            },
        )
        .unwrap();

        let failed = broker.read_failed("orphan_failed").unwrap();

        assert!(failed.is_none());
        assert!(!temp.path().join("queue/failed/orphan_failed.json").exists());
        assert_eq!(
            fs::read_dir(temp.path().join("queue/rejected"))
                .unwrap()
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn file_broker_quarantines_failed_event_with_wrong_lease_while_claimed() {
        let temp = tempfile::TempDir::new().unwrap();
        let broker = FileBroker::new(temp.path().join("queue")).unwrap();
        let request = ToolInvocationRequest {
            invocation_id: Some("claimed_failed".to_string()),
            session_id: "sess".to_string(),
            tool_name: "Read".to_string(),
            arguments: Map::new(),
            cwd: None,
            timeout_ms: None,
            max_output_bytes: None,
            idempotency_key: None,
            required_capabilities: vec![],
            metadata: Map::new(),
        };
        broker.enqueue(&request).unwrap();
        let claim = broker.claim_next("worker").await.unwrap().unwrap();
        write_json_atomic(
            &temp.path().join("queue/failed/claimed_failed.json"),
            &ToolInvocationFailed {
                event_type: "tool.invocation.failed".to_string(),
                invocation_id: "claimed_failed".to_string(),
                session_id: "sess".to_string(),
                attempt_id: Some(claim.attempt_id),
                lease_token: Some("wrong_lease".to_string()),
                error: ErrorEnvelope {
                    code: "forged".to_string(),
                    message: "forged while claimed".to_string(),
                    retryable: false,
                },
                failed_at: "now".to_string(),
            },
        )
        .unwrap();

        let failed = broker.read_failed("claimed_failed").unwrap();

        assert!(failed.is_none());
        assert!(temp
            .path()
            .join("queue/claimed/claimed_failed.json")
            .exists());
        assert!(!temp
            .path()
            .join("queue/failed/claimed_failed.json")
            .exists());
        assert_eq!(
            fs::read_dir(temp.path().join("queue/rejected"))
                .unwrap()
                .count(),
            1
        );
    }

    #[test]
    fn file_broker_quarantines_failed_event_when_claim_lease_is_malformed() {
        let temp = tempfile::TempDir::new().unwrap();
        let broker = FileBroker::new(temp.path().join("queue")).unwrap();
        fs::write(
            temp.path()
                .join("queue/claimed/malformed_failed_claim.json"),
            b"{not json",
        )
        .unwrap();
        write_json_atomic(
            &temp.path().join("queue/failed/malformed_failed_claim.json"),
            &ToolInvocationFailed {
                event_type: "tool.invocation.failed".to_string(),
                invocation_id: "malformed_failed_claim".to_string(),
                session_id: "sess".to_string(),
                attempt_id: Some("attempt".to_string()),
                lease_token: Some("lease".to_string()),
                error: ErrorEnvelope {
                    code: "forged".to_string(),
                    message: "forged".to_string(),
                    retryable: false,
                },
                failed_at: "now".to_string(),
            },
        )
        .unwrap();

        let failed = broker.read_failed("malformed_failed_claim").unwrap();

        assert!(failed.is_none());
        assert!(temp
            .path()
            .join("queue/claimed/malformed_failed_claim.json")
            .exists());
        assert!(!temp
            .path()
            .join("queue/failed/malformed_failed_claim.json")
            .exists());
        assert_eq!(
            fs::read_dir(temp.path().join("queue/rejected"))
                .unwrap()
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn file_broker_quarantines_manually_added_duplicate_completed_invocation() {
        let temp = tempfile::TempDir::new().unwrap();
        let broker = FileBroker::new(temp.path().join("queue")).unwrap();
        let request = ToolInvocationRequest {
            invocation_id: Some("manual_done".to_string()),
            session_id: "sess".to_string(),
            tool_name: "Read".to_string(),
            arguments: Map::new(),
            cwd: None,
            timeout_ms: None,
            max_output_bytes: None,
            idempotency_key: None,
            required_capabilities: vec![],
            metadata: Map::new(),
        };
        broker.enqueue(&request).unwrap();
        let claim = broker.claim_next("worker").await.unwrap().unwrap();
        broker
            .complete(ToolInvocationCompleted {
                event_type: "tool.invocation.completed".to_string(),
                invocation_id: "manual_done".to_string(),
                session_id: "sess".to_string(),
                attempt_id: Some(claim.attempt_id),
                lease_token: Some(claim.lease_token),
                result: ToolInvocationResult {
                    invocation_id: "manual_done".to_string(),
                    session_id: "sess".to_string(),
                    tool_name: "Read".to_string(),
                    status: ToolResultStatus::Success,
                    output: "ok".to_string(),
                    error: None,
                    summary: None,
                    effects: vec![],
                    duration_ms: 0,
                    metadata: Map::new(),
                },
                completed_at: "now".to_string(),
            })
            .await
            .unwrap();
        write_json_atomic(&temp.path().join("queue/pending/manual.json"), &request).unwrap();

        let claim = broker.claim_next("worker").await.unwrap();

        assert!(claim.is_none());
        assert!(temp
            .path()
            .join("queue/completed/manual_done.json")
            .exists());
        assert_eq!(
            fs::read_dir(temp.path().join("queue/rejected"))
                .unwrap()
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn file_broker_skips_malformed_pending_file_and_claims_next_valid_invocation() {
        let temp = tempfile::TempDir::new().unwrap();
        let broker = FileBroker::new(temp.path().join("queue")).unwrap();
        fs::write(temp.path().join("queue/pending/000_bad.json"), b"{not json").unwrap();
        let request = ToolInvocationRequest {
            invocation_id: Some("valid".to_string()),
            session_id: "sess".to_string(),
            tool_name: "Read".to_string(),
            arguments: Map::new(),
            cwd: None,
            timeout_ms: None,
            max_output_bytes: None,
            idempotency_key: None,
            required_capabilities: vec![],
            metadata: Map::new(),
        };
        broker.enqueue(&request).unwrap();

        let claim = broker.claim_next("worker").await.unwrap().unwrap();

        assert_eq!(claim.request.invocation_id.as_deref(), Some("valid"));
        assert!(!temp.path().join("queue/pending/000_bad.json").exists());
        assert_eq!(
            fs::read_dir(temp.path().join("queue/rejected"))
                .unwrap()
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn file_broker_quarantines_pending_request_when_claim_envelope_exceeds_cap() {
        let temp = tempfile::TempDir::new().unwrap();
        let broker = FileBroker::new(temp.path().join("queue")).unwrap();
        let mut request = ToolInvocationRequest {
            invocation_id: Some("huge_claim".to_string()),
            session_id: "sess".to_string(),
            tool_name: "Read".to_string(),
            arguments: Map::new(),
            cwd: None,
            timeout_ms: None,
            max_output_bytes: None,
            idempotency_key: None,
            required_capabilities: vec![],
            metadata: Map::new(),
        };
        request
            .metadata
            .insert("padding".to_string(), Value::String(String::new()));
        let empty_request_bytes = serde_json::to_vec_pretty(&request).unwrap().len();
        request.metadata.insert(
            "padding".to_string(),
            Value::String("x".repeat(MAX_QUEUE_JSON_BYTES as usize - empty_request_bytes - 1)),
        );
        assert!(
            serde_json::to_vec_pretty(&request).unwrap().len() <= MAX_QUEUE_JSON_BYTES as usize
        );
        broker.enqueue(&request).unwrap();

        let claim = broker.claim_next("worker").await.unwrap();

        assert!(claim.is_none());
        assert!(!temp.path().join("queue/pending/huge_claim.json").exists());
        assert!(!temp.path().join("queue/claimed/huge_claim.json").exists());
        assert!(fs::read_dir(temp.path().join("queue/claimed"))
            .unwrap()
            .all(|entry| !entry
                .unwrap()
                .file_name()
                .to_string_lossy()
                .starts_with(".claiming-")));
        assert_eq!(
            fs::read_dir(temp.path().join("queue/rejected"))
                .unwrap()
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn file_broker_claims_each_pending_file_at_most_once_under_concurrency() {
        let temp = tempfile::TempDir::new().unwrap();
        let broker = Arc::new(FileBroker::new(temp.path().join("queue")).unwrap());
        let request = ToolInvocationRequest {
            invocation_id: Some("race".to_string()),
            session_id: "sess".to_string(),
            tool_name: "Read".to_string(),
            arguments: Map::new(),
            cwd: None,
            timeout_ms: None,
            max_output_bytes: None,
            idempotency_key: None,
            required_capabilities: vec![],
            metadata: Map::new(),
        };
        broker.enqueue(&request).unwrap();

        let first = {
            let broker = broker.clone();
            tokio::spawn(async move { broker.claim_next("worker-1").await.unwrap() })
        };
        let second = {
            let broker = broker.clone();
            tokio::spawn(async move { broker.claim_next("worker-2").await.unwrap() })
        };
        let (first, second) = tokio::join!(first, second);
        let claimed = [first.unwrap(), second.unwrap()]
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();

        assert_eq!(claimed.len(), 1);
        assert_eq!(claimed[0].request.invocation_id.as_deref(), Some("race"));
    }

    #[tokio::test]
    async fn file_broker_rejects_invalid_worker_id_without_claiming() {
        let temp = tempfile::TempDir::new().unwrap();
        let broker = FileBroker::new(temp.path().join("queue")).unwrap();
        let request = ToolInvocationRequest {
            invocation_id: Some("worker_id_claim".to_string()),
            session_id: "sess".to_string(),
            tool_name: "Read".to_string(),
            arguments: Map::new(),
            cwd: None,
            timeout_ms: None,
            max_output_bytes: None,
            idempotency_key: None,
            required_capabilities: vec![],
            metadata: Map::new(),
        };
        broker.enqueue(&request).unwrap();

        let err = broker.claim_next("../worker").await.unwrap_err();

        assert!(err.to_string().contains("invalid worker id"));
        assert!(temp
            .path()
            .join("queue/pending/worker_id_claim.json")
            .exists());
        assert!(!temp
            .path()
            .join("queue/claimed/worker_id_claim.json")
            .exists());
    }

    #[tokio::test]
    async fn file_broker_rejects_duplicate_invocation_id_while_claimed_on_enqueue() {
        let temp = tempfile::TempDir::new().unwrap();
        let broker = FileBroker::new(temp.path().join("queue")).unwrap();
        let request = ToolInvocationRequest {
            invocation_id: Some("dup".to_string()),
            session_id: "sess".to_string(),
            tool_name: "Read".to_string(),
            arguments: Map::new(),
            cwd: None,
            timeout_ms: None,
            max_output_bytes: None,
            idempotency_key: None,
            required_capabilities: vec![],
            metadata: Map::new(),
        };
        broker.enqueue(&request).unwrap();
        let first = broker.claim_next("worker-1").await.unwrap().unwrap();

        let err = broker.enqueue(&request).unwrap_err();

        assert_eq!(first.request.invocation_id.as_deref(), Some("dup"));
        assert!(err.to_string().contains("duplicate invocation id"));
        assert!(temp.path().join("queue/claimed/dup.json").exists());
        assert!(!temp.path().join("queue/pending/dup.json").exists());
    }

    #[tokio::test]
    async fn file_broker_rejects_complete_with_wrong_lease_token() {
        let temp = tempfile::TempDir::new().unwrap();
        let broker = FileBroker::new(temp.path().join("queue")).unwrap();
        let request = ToolInvocationRequest {
            invocation_id: Some("leased".to_string()),
            session_id: "sess".to_string(),
            tool_name: "Read".to_string(),
            arguments: Map::new(),
            cwd: None,
            timeout_ms: None,
            max_output_bytes: None,
            idempotency_key: None,
            required_capabilities: vec![],
            metadata: Map::new(),
        };
        broker.enqueue(&request).unwrap();
        let claim = broker.claim_next("worker").await.unwrap().unwrap();

        let err = broker
            .complete(ToolInvocationCompleted {
                event_type: "tool.invocation.completed".to_string(),
                invocation_id: "leased".to_string(),
                session_id: "sess".to_string(),
                attempt_id: Some(claim.attempt_id),
                lease_token: Some("wrong-lease".to_string()),
                result: ToolInvocationResult {
                    invocation_id: "leased".to_string(),
                    session_id: "sess".to_string(),
                    tool_name: "Read".to_string(),
                    status: ToolResultStatus::Success,
                    output: "ok".to_string(),
                    error: None,
                    summary: None,
                    effects: vec![],
                    duration_ms: 0,
                    metadata: Map::new(),
                },
                completed_at: "now".to_string(),
            })
            .await
            .unwrap_err();

        assert!(err.to_string().contains("lease"));
        assert!(!temp.path().join("queue/completed/leased.json").exists());
        assert!(temp.path().join("queue/claimed/leased.json").exists());
    }

    #[tokio::test]
    async fn file_broker_rejects_complete_with_mismatched_claim_session() {
        let temp = tempfile::TempDir::new().unwrap();
        let broker = FileBroker::new(temp.path().join("queue")).unwrap();
        let request = ToolInvocationRequest {
            invocation_id: Some("complete_wrong_session".to_string()),
            session_id: "sess".to_string(),
            tool_name: "Read".to_string(),
            arguments: Map::new(),
            cwd: None,
            timeout_ms: None,
            max_output_bytes: None,
            idempotency_key: None,
            required_capabilities: vec![],
            metadata: Map::new(),
        };
        broker.enqueue(&request).unwrap();
        let claim = broker.claim_next("worker").await.unwrap().unwrap();

        let err = broker
            .complete(ToolInvocationCompleted {
                event_type: "tool.invocation.completed".to_string(),
                invocation_id: "complete_wrong_session".to_string(),
                session_id: "other_session".to_string(),
                attempt_id: Some(claim.attempt_id),
                lease_token: Some(claim.lease_token),
                result: ToolInvocationResult {
                    invocation_id: "complete_wrong_session".to_string(),
                    session_id: "other_session".to_string(),
                    tool_name: "Read".to_string(),
                    status: ToolResultStatus::Success,
                    output: "wrong".to_string(),
                    error: None,
                    summary: None,
                    effects: vec![],
                    duration_ms: 0,
                    metadata: Map::new(),
                },
                completed_at: "now".to_string(),
            })
            .await
            .unwrap_err();

        assert!(err.to_string().contains("claim identity"));
        assert!(!temp
            .path()
            .join("queue/completed/complete_wrong_session.json")
            .exists());
        assert!(temp
            .path()
            .join("queue/claimed/complete_wrong_session.json")
            .exists());
    }

    #[tokio::test]
    async fn file_broker_rejects_complete_with_mismatched_result_identity() {
        let temp = tempfile::TempDir::new().unwrap();
        let broker = FileBroker::new(temp.path().join("queue")).unwrap();
        let request = ToolInvocationRequest {
            invocation_id: Some("complete_wrong_result".to_string()),
            session_id: "sess".to_string(),
            tool_name: "Read".to_string(),
            arguments: Map::new(),
            cwd: None,
            timeout_ms: None,
            max_output_bytes: None,
            idempotency_key: None,
            required_capabilities: vec![],
            metadata: Map::new(),
        };
        broker.enqueue(&request).unwrap();
        let claim = broker.claim_next("worker").await.unwrap().unwrap();

        let err = broker
            .complete(ToolInvocationCompleted {
                event_type: "tool.invocation.completed".to_string(),
                invocation_id: "complete_wrong_result".to_string(),
                session_id: "sess".to_string(),
                attempt_id: Some(claim.attempt_id),
                lease_token: Some(claim.lease_token),
                result: ToolInvocationResult {
                    invocation_id: "other_invocation".to_string(),
                    session_id: "sess".to_string(),
                    tool_name: "Read".to_string(),
                    status: ToolResultStatus::Success,
                    output: "wrong".to_string(),
                    error: None,
                    summary: None,
                    effects: vec![],
                    duration_ms: 0,
                    metadata: Map::new(),
                },
                completed_at: "now".to_string(),
            })
            .await
            .unwrap_err();

        assert!(err.to_string().contains("result identity"));
        assert!(!temp
            .path()
            .join("queue/completed/complete_wrong_result.json")
            .exists());
        assert!(temp
            .path()
            .join("queue/claimed/complete_wrong_result.json")
            .exists());
    }

    #[tokio::test]
    async fn file_broker_rejects_complete_with_mismatched_result_tool_name() {
        let temp = tempfile::TempDir::new().unwrap();
        let broker = FileBroker::new(temp.path().join("queue")).unwrap();
        let request = ToolInvocationRequest {
            invocation_id: Some("complete_wrong_tool".to_string()),
            session_id: "sess".to_string(),
            tool_name: "List".to_string(),
            arguments: Map::new(),
            cwd: None,
            timeout_ms: None,
            max_output_bytes: None,
            idempotency_key: None,
            required_capabilities: vec![],
            metadata: Map::new(),
        };
        broker.enqueue(&request).unwrap();
        let claim = broker.claim_next("worker").await.unwrap().unwrap();

        let err = broker
            .complete(ToolInvocationCompleted {
                event_type: "tool.invocation.completed".to_string(),
                invocation_id: "complete_wrong_tool".to_string(),
                session_id: "sess".to_string(),
                attempt_id: Some(claim.attempt_id),
                lease_token: Some(claim.lease_token),
                result: ToolInvocationResult {
                    invocation_id: "complete_wrong_tool".to_string(),
                    session_id: "sess".to_string(),
                    tool_name: "Read".to_string(),
                    status: ToolResultStatus::Success,
                    output: "wrong tool".to_string(),
                    error: None,
                    summary: None,
                    effects: vec![],
                    duration_ms: 0,
                    metadata: Map::new(),
                },
                completed_at: "now".to_string(),
            })
            .await
            .unwrap_err();

        assert!(err.to_string().contains("result identity"));
        assert!(!temp
            .path()
            .join("queue/completed/complete_wrong_tool.json")
            .exists());
        assert!(temp
            .path()
            .join("queue/claimed/complete_wrong_tool.json")
            .exists());
    }

    #[tokio::test]
    async fn file_broker_rejects_complete_without_overwriting_existing_terminal() {
        let temp = tempfile::TempDir::new().unwrap();
        let broker = FileBroker::new(temp.path().join("queue")).unwrap();
        let request = ToolInvocationRequest {
            invocation_id: Some("already_completed".to_string()),
            session_id: "sess".to_string(),
            tool_name: "Read".to_string(),
            arguments: Map::new(),
            cwd: None,
            timeout_ms: None,
            max_output_bytes: None,
            idempotency_key: None,
            required_capabilities: vec![],
            metadata: Map::new(),
        };
        broker.enqueue(&request).unwrap();
        let claim = broker.claim_next("worker").await.unwrap().unwrap();
        let completed_path = temp.path().join("queue/completed/already_completed.json");
        write_json_atomic(
            &completed_path,
            &ToolInvocationCompleted {
                event_type: "tool.invocation.completed".to_string(),
                invocation_id: "already_completed".to_string(),
                session_id: "sess".to_string(),
                attempt_id: None,
                lease_token: None,
                result: ToolInvocationResult {
                    invocation_id: "already_completed".to_string(),
                    session_id: "sess".to_string(),
                    tool_name: "Read".to_string(),
                    status: ToolResultStatus::Success,
                    output: "original".to_string(),
                    error: None,
                    summary: None,
                    effects: vec![],
                    duration_ms: 0,
                    metadata: Map::new(),
                },
                completed_at: "already".to_string(),
            },
        )
        .unwrap();

        let err = broker
            .complete(ToolInvocationCompleted {
                event_type: "tool.invocation.completed".to_string(),
                invocation_id: "already_completed".to_string(),
                session_id: "sess".to_string(),
                attempt_id: Some(claim.attempt_id),
                lease_token: Some(claim.lease_token),
                result: ToolInvocationResult {
                    invocation_id: "already_completed".to_string(),
                    session_id: "sess".to_string(),
                    tool_name: "Read".to_string(),
                    status: ToolResultStatus::Success,
                    output: "replacement".to_string(),
                    error: None,
                    summary: None,
                    effects: vec![],
                    duration_ms: 0,
                    metadata: Map::new(),
                },
                completed_at: "now".to_string(),
            })
            .await
            .unwrap_err();
        let completed: ToolInvocationCompleted = read_json_required(&completed_path).unwrap();

        assert!(err.to_string().contains("terminal"));
        assert_eq!(completed.result.output, "original");
        assert!(temp
            .path()
            .join("queue/claimed/already_completed.json")
            .exists());
    }

    #[tokio::test]
    async fn file_broker_rejects_fail_with_wrong_attempt_id() {
        let temp = tempfile::TempDir::new().unwrap();
        let broker = FileBroker::new(temp.path().join("queue")).unwrap();
        let request = ToolInvocationRequest {
            invocation_id: Some("failed_lease".to_string()),
            session_id: "sess".to_string(),
            tool_name: "Read".to_string(),
            arguments: Map::new(),
            cwd: None,
            timeout_ms: None,
            max_output_bytes: None,
            idempotency_key: None,
            required_capabilities: vec![],
            metadata: Map::new(),
        };
        broker.enqueue(&request).unwrap();
        let claim = broker.claim_next("worker").await.unwrap().unwrap();

        let err = broker
            .fail(ToolInvocationFailed {
                event_type: "tool.invocation.failed".to_string(),
                invocation_id: "failed_lease".to_string(),
                session_id: "sess".to_string(),
                attempt_id: Some("wrong-attempt".to_string()),
                lease_token: Some(claim.lease_token),
                error: ErrorEnvelope {
                    code: "failed".to_string(),
                    message: "failed".to_string(),
                    retryable: false,
                },
                failed_at: "now".to_string(),
            })
            .await
            .unwrap_err();

        assert!(err.to_string().contains("lease"));
        assert!(!temp.path().join("queue/failed/failed_lease.json").exists());
        assert!(temp.path().join("queue/claimed/failed_lease.json").exists());
    }

    #[tokio::test]
    async fn file_broker_rejects_fail_with_malformed_error_without_consuming_claim() {
        let temp = tempfile::TempDir::new().unwrap();
        let broker = FileBroker::new(temp.path().join("queue")).unwrap();
        let request = ToolInvocationRequest {
            invocation_id: Some("failed_malformed_error".to_string()),
            session_id: "sess".to_string(),
            tool_name: "Read".to_string(),
            arguments: Map::new(),
            cwd: None,
            timeout_ms: None,
            max_output_bytes: None,
            idempotency_key: None,
            required_capabilities: vec![],
            metadata: Map::new(),
        };
        broker.enqueue(&request).unwrap();
        let claim = broker.claim_next("worker").await.unwrap().unwrap();

        let err = broker
            .fail(ToolInvocationFailed {
                event_type: "tool.invocation.failed".to_string(),
                invocation_id: "failed_malformed_error".to_string(),
                session_id: "sess".to_string(),
                attempt_id: Some(claim.attempt_id),
                lease_token: Some(claim.lease_token),
                error: ErrorEnvelope {
                    code: String::new(),
                    message: String::new(),
                    retryable: false,
                },
                failed_at: "now".to_string(),
            })
            .await
            .unwrap_err();

        assert!(err.to_string().contains("error envelope"));
        assert!(!temp
            .path()
            .join("queue/failed/failed_malformed_error.json")
            .exists());
        assert!(temp
            .path()
            .join("queue/claimed/failed_malformed_error.json")
            .exists());
    }

    #[tokio::test]
    async fn file_broker_rejects_fail_with_mismatched_claim_session() {
        let temp = tempfile::TempDir::new().unwrap();
        let broker = FileBroker::new(temp.path().join("queue")).unwrap();
        let request = ToolInvocationRequest {
            invocation_id: Some("fail_wrong_session".to_string()),
            session_id: "sess".to_string(),
            tool_name: "Read".to_string(),
            arguments: Map::new(),
            cwd: None,
            timeout_ms: None,
            max_output_bytes: None,
            idempotency_key: None,
            required_capabilities: vec![],
            metadata: Map::new(),
        };
        broker.enqueue(&request).unwrap();
        let claim = broker.claim_next("worker").await.unwrap().unwrap();

        let err = broker
            .fail(ToolInvocationFailed {
                event_type: "tool.invocation.failed".to_string(),
                invocation_id: "fail_wrong_session".to_string(),
                session_id: "other_session".to_string(),
                attempt_id: Some(claim.attempt_id),
                lease_token: Some(claim.lease_token),
                error: ErrorEnvelope {
                    code: "failed".to_string(),
                    message: "wrong session".to_string(),
                    retryable: false,
                },
                failed_at: "now".to_string(),
            })
            .await
            .unwrap_err();

        assert!(err.to_string().contains("claim identity"));
        assert!(!temp
            .path()
            .join("queue/failed/fail_wrong_session.json")
            .exists());
        assert!(temp
            .path()
            .join("queue/claimed/fail_wrong_session.json")
            .exists());
    }

    #[tokio::test]
    async fn file_broker_rejects_fail_without_overwriting_existing_terminal() {
        let temp = tempfile::TempDir::new().unwrap();
        let broker = FileBroker::new(temp.path().join("queue")).unwrap();
        let request = ToolInvocationRequest {
            invocation_id: Some("already_failed".to_string()),
            session_id: "sess".to_string(),
            tool_name: "Read".to_string(),
            arguments: Map::new(),
            cwd: None,
            timeout_ms: None,
            max_output_bytes: None,
            idempotency_key: None,
            required_capabilities: vec![],
            metadata: Map::new(),
        };
        broker.enqueue(&request).unwrap();
        let claim = broker.claim_next("worker").await.unwrap().unwrap();
        let failed_path = temp.path().join("queue/failed/already_failed.json");
        write_json_atomic(
            &failed_path,
            &ToolInvocationFailed {
                event_type: "tool.invocation.failed".to_string(),
                invocation_id: "already_failed".to_string(),
                session_id: "sess".to_string(),
                attempt_id: None,
                lease_token: None,
                error: ErrorEnvelope {
                    code: "original".to_string(),
                    message: "original".to_string(),
                    retryable: false,
                },
                failed_at: "already".to_string(),
            },
        )
        .unwrap();

        let err = broker
            .fail(ToolInvocationFailed {
                event_type: "tool.invocation.failed".to_string(),
                invocation_id: "already_failed".to_string(),
                session_id: "sess".to_string(),
                attempt_id: Some(claim.attempt_id),
                lease_token: Some(claim.lease_token),
                error: ErrorEnvelope {
                    code: "replacement".to_string(),
                    message: "replacement".to_string(),
                    retryable: false,
                },
                failed_at: "now".to_string(),
            })
            .await
            .unwrap_err();
        let failed: ToolInvocationFailed = read_json_required(&failed_path).unwrap();

        assert!(err.to_string().contains("terminal"));
        assert_eq!(failed.error.code, "original");
        assert!(temp
            .path()
            .join("queue/claimed/already_failed.json")
            .exists());
    }

    #[tokio::test]
    async fn file_broker_quarantines_invalid_invocation_id_pending_file_without_error() {
        let temp = tempfile::TempDir::new().unwrap();
        let broker = FileBroker::new(temp.path().join("queue")).unwrap();
        let request = ToolInvocationRequest {
            invocation_id: Some("../escaped".to_string()),
            session_id: "sess".to_string(),
            tool_name: "Read".to_string(),
            arguments: Map::new(),
            cwd: None,
            timeout_ms: None,
            max_output_bytes: None,
            idempotency_key: None,
            required_capabilities: vec![],
            metadata: Map::new(),
        };
        write_json_atomic(&temp.path().join("queue/pending/unsafe.json"), &request).unwrap();

        let claim = broker.claim_next("worker").await.unwrap();

        assert!(claim.is_none());
        assert!(!temp.path().join("queue/pending/unsafe.json").exists());
        assert!(!temp.path().join("queue/escaped.json").exists());
        assert_eq!(
            fs::read_dir(temp.path().join("queue/rejected"))
                .unwrap()
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn file_broker_quarantines_invalid_session_id_pending_file_without_error() {
        let temp = tempfile::TempDir::new().unwrap();
        let broker = FileBroker::new(temp.path().join("queue")).unwrap();
        let request = ToolInvocationRequest {
            invocation_id: Some("bad_session".to_string()),
            session_id: "../escaped".to_string(),
            tool_name: "Read".to_string(),
            arguments: Map::new(),
            cwd: None,
            timeout_ms: None,
            max_output_bytes: None,
            idempotency_key: None,
            required_capabilities: vec![],
            metadata: Map::new(),
        };
        write_json_atomic(
            &temp.path().join("queue/pending/bad_session.json"),
            &request,
        )
        .unwrap();

        let claim = broker.claim_next("worker").await.unwrap();

        assert!(claim.is_none());
        assert!(!temp.path().join("queue/pending/bad_session.json").exists());
        assert_eq!(
            fs::read_dir(temp.path().join("queue/rejected"))
                .unwrap()
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn file_broker_rejects_empty_tool_name_on_enqueue_without_pending_file() {
        let temp = tempfile::TempDir::new().unwrap();
        let broker = FileBroker::new(temp.path().join("queue")).unwrap();
        let request = ToolInvocationRequest {
            invocation_id: Some("empty_tool".to_string()),
            session_id: "sess".to_string(),
            tool_name: String::new(),
            arguments: Map::new(),
            cwd: None,
            timeout_ms: None,
            max_output_bytes: None,
            idempotency_key: None,
            required_capabilities: vec![],
            metadata: Map::new(),
        };

        let err = broker.enqueue(&request).unwrap_err();

        assert!(err.to_string().contains("tool name"));
        assert!(!temp.path().join("queue/pending/empty_tool.json").exists());
    }

    #[tokio::test]
    async fn file_broker_quarantines_empty_tool_name_pending_file_without_claiming() {
        let temp = tempfile::TempDir::new().unwrap();
        let broker = FileBroker::new(temp.path().join("queue")).unwrap();
        let request = ToolInvocationRequest {
            invocation_id: Some("empty_tool_pending".to_string()),
            session_id: "sess".to_string(),
            tool_name: String::new(),
            arguments: Map::new(),
            cwd: None,
            timeout_ms: None,
            max_output_bytes: None,
            idempotency_key: None,
            required_capabilities: vec![],
            metadata: Map::new(),
        };
        write_json_atomic(
            &temp.path().join("queue/pending/empty_tool_pending.json"),
            &request,
        )
        .unwrap();

        let claim = broker.claim_next("worker").await.unwrap();

        assert!(claim.is_none());
        assert!(!temp
            .path()
            .join("queue/pending/empty_tool_pending.json")
            .exists());
        assert!(!temp
            .path()
            .join("queue/claimed/empty_tool_pending.json")
            .exists());
        assert_eq!(
            fs::read_dir(temp.path().join("queue/rejected"))
                .unwrap()
                .count(),
            1
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn file_broker_quarantines_symlink_pending_file_without_following_it() {
        let temp = tempfile::TempDir::new().unwrap();
        let broker = FileBroker::new(temp.path().join("queue")).unwrap();
        let outside_request = temp.path().join("outside-request.json");
        let request = ToolInvocationRequest {
            invocation_id: Some("linked_pending".to_string()),
            session_id: "sess".to_string(),
            tool_name: "Read".to_string(),
            arguments: Map::new(),
            cwd: None,
            timeout_ms: None,
            max_output_bytes: None,
            idempotency_key: None,
            required_capabilities: vec![],
            metadata: Map::new(),
        };
        write_json_atomic(&outside_request, &request).unwrap();
        std::os::unix::fs::symlink(
            &outside_request,
            temp.path().join("queue/pending/linked_pending.json"),
        )
        .unwrap();

        let claim = broker.claim_next("worker").await.unwrap();

        assert!(claim.is_none());
        assert!(outside_request.exists());
        assert!(!temp
            .path()
            .join("queue/pending/linked_pending.json")
            .exists());
        assert_eq!(
            fs::read_dir(temp.path().join("queue/rejected"))
                .unwrap()
                .count(),
            1
        );
    }

    #[test]
    fn file_broker_quarantines_terminal_events_with_wrong_event_type() {
        let temp = tempfile::TempDir::new().unwrap();
        let broker = FileBroker::new(temp.path().join("queue")).unwrap();
        write_json_atomic(
            &temp
                .path()
                .join("queue/completed/wrong_completed_type.json"),
            &ToolInvocationCompleted {
                event_type: "tool.invocation.failed".to_string(),
                invocation_id: "wrong_completed_type".to_string(),
                session_id: "sess".to_string(),
                attempt_id: Some("attempt".to_string()),
                lease_token: Some("lease".to_string()),
                result: ToolInvocationResult {
                    invocation_id: "wrong_completed_type".to_string(),
                    session_id: "sess".to_string(),
                    tool_name: "Read".to_string(),
                    status: ToolResultStatus::Success,
                    output: "wrong event type".to_string(),
                    error: None,
                    summary: None,
                    effects: vec![],
                    duration_ms: 0,
                    metadata: Map::new(),
                },
                completed_at: "now".to_string(),
            },
        )
        .unwrap();
        write_json_atomic(
            &temp.path().join("queue/failed/wrong_failed_type.json"),
            &ToolInvocationFailed {
                event_type: "tool.invocation.completed".to_string(),
                invocation_id: "wrong_failed_type".to_string(),
                session_id: "sess".to_string(),
                attempt_id: Some("attempt".to_string()),
                lease_token: Some("lease".to_string()),
                error: ErrorEnvelope {
                    code: "wrong".to_string(),
                    message: "wrong event type".to_string(),
                    retryable: false,
                },
                failed_at: "now".to_string(),
            },
        )
        .unwrap();

        let completed = broker.read_completed("wrong_completed_type").unwrap();
        let failed = broker.read_failed("wrong_failed_type").unwrap();

        assert!(completed.is_none());
        assert!(failed.is_none());
        assert!(!temp
            .path()
            .join("queue/completed/wrong_completed_type.json")
            .exists());
        assert!(!temp
            .path()
            .join("queue/failed/wrong_failed_type.json")
            .exists());
        assert_eq!(
            fs::read_dir(temp.path().join("queue/rejected"))
                .unwrap()
                .count(),
            2
        );
    }

    #[cfg(unix)]
    #[test]
    fn file_broker_quarantines_symlink_terminal_file_without_following_it() {
        let temp = tempfile::TempDir::new().unwrap();
        let broker = FileBroker::new(temp.path().join("queue")).unwrap();
        let outside_completed = temp.path().join("outside-completed.json");
        let event = ToolInvocationCompleted {
            event_type: "tool.invocation.completed".to_string(),
            invocation_id: "linked_terminal".to_string(),
            session_id: "sess".to_string(),
            attempt_id: None,
            lease_token: None,
            result: ToolInvocationResult {
                invocation_id: "linked_terminal".to_string(),
                session_id: "sess".to_string(),
                tool_name: "Read".to_string(),
                status: ToolResultStatus::Success,
                output: "forged outside queue".to_string(),
                error: None,
                summary: None,
                effects: vec![],
                duration_ms: 0,
                metadata: Map::new(),
            },
            completed_at: "now".to_string(),
        };
        write_json_atomic(&outside_completed, &event).unwrap();
        std::os::unix::fs::symlink(
            &outside_completed,
            temp.path().join("queue/completed/linked_terminal.json"),
        )
        .unwrap();

        let completed = broker.read_completed("linked_terminal").unwrap();

        assert!(completed.is_none());
        assert!(outside_completed.exists());
        assert!(!temp
            .path()
            .join("queue/completed/linked_terminal.json")
            .exists());
        assert_eq!(
            fs::read_dir(temp.path().join("queue/rejected"))
                .unwrap()
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn file_broker_rejects_path_traversal_invocation_id_on_complete_and_fail() {
        let temp = tempfile::TempDir::new().unwrap();
        let broker = FileBroker::new(temp.path().join("queue")).unwrap();
        let result = ToolInvocationResult {
            invocation_id: "../escaped".to_string(),
            session_id: "sess".to_string(),
            tool_name: "Read".to_string(),
            status: ToolResultStatus::Success,
            output: "ok".to_string(),
            error: None,
            summary: None,
            effects: vec![],
            duration_ms: 0,
            metadata: Map::new(),
        };

        let complete_err = broker
            .complete(ToolInvocationCompleted {
                event_type: "tool.invocation.completed".to_string(),
                invocation_id: "../escaped".to_string(),
                session_id: "sess".to_string(),
                attempt_id: None,
                lease_token: None,
                result,
                completed_at: "now".to_string(),
            })
            .await
            .unwrap_err();
        let fail_err = broker
            .fail(ToolInvocationFailed {
                event_type: "tool.invocation.failed".to_string(),
                invocation_id: "../escaped".to_string(),
                session_id: "sess".to_string(),
                attempt_id: None,
                lease_token: None,
                error: ErrorEnvelope {
                    code: "failed".to_string(),
                    message: "failed".to_string(),
                    retryable: false,
                },
                failed_at: "now".to_string(),
            })
            .await
            .unwrap_err();

        assert!(complete_err.to_string().contains("invalid invocation id"));
        assert!(fail_err.to_string().contains("invalid invocation id"));
        assert!(!temp.path().join("queue/escaped.json").exists());
    }
}
