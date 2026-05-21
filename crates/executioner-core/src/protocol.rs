use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::collections::HashMap;

pub const PROTOCOL_NAME: &str = "executioner";
pub const PROTOCOL_VERSION: &str = "executioner.v1";
pub const PROTOCOL_MAJOR_VERSION: u16 = 1;
pub const EVENT_SCHEMA_VERSION: u16 = 1;
pub const MAX_OUTPUT_BYTES: usize = 10 * 1024 * 1024;
pub const MAX_REQUEST_JSON_BYTES: usize = 1024 * 1024;
pub const MAX_TOOL_TIMEOUT_MS: u64 = 60 * 60 * 1000;
pub const MAX_SESSION_TTL_MS: u64 = 365 * 24 * 60 * 60 * 1000;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ProtocolVersion {
    pub name: String,
    pub version: String,
    pub major: u16,
}

impl Default for ProtocolVersion {
    fn default() -> Self {
        Self {
            name: PROTOCOL_NAME.to_string(),
            version: PROTOCOL_VERSION.to_string(),
            major: PROTOCOL_MAJOR_VERSION,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct EventEnvelope<T> {
    pub protocol: ProtocolVersion,
    pub schema_version: u16,
    pub event_id: String,
    #[serde(rename = "type")]
    pub event_type: String,
    pub occurred_at: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub invocation_id: Option<String>,
    pub payload: T,
    #[serde(default)]
    pub metadata: Map<String, Value>,
}

impl<T> EventEnvelope<T> {
    pub fn new(event_type: impl Into<String>, event_id: impl Into<String>, payload: T) -> Self {
        Self {
            protocol: ProtocolVersion::default(),
            schema_version: EVENT_SCHEMA_VERSION,
            event_id: event_id.into(),
            event_type: event_type.into(),
            occurred_at: crate::effects::now_string(),
            session_id: None,
            invocation_id: None,
            payload,
            metadata: Map::new(),
        }
    }

    pub fn session_id(mut self, session_id: impl Into<String>) -> Self {
        self.session_id = Some(session_id.into());
        self
    }

    pub fn invocation_id(mut self, invocation_id: impl Into<String>) -> Self {
        self.invocation_id = Some(invocation_id.into());
        self
    }

    pub fn metadata(mut self, key: impl Into<String>, value: Value) -> Self {
        self.metadata.insert(key.into(), value);
        self
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", content = "data", rename_all = "camelCase")]
pub enum ExecutionerEvent {
    ToolInvocationRequested(ToolInvocationRequested),
    ToolInvocationClaimed(ToolInvocationClaimed),
    ToolInvocationCompleted(ToolInvocationCompleted),
    ToolInvocationFailed(ToolInvocationFailed),
    SessionCreated(CreateSessionResponse),
    SessionClosed(Session),
    SessionDestroyed(Session),
    EffectsRecorded(Vec<Effect>),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum WorkspaceMode {
    New,
    Existing,
    Snapshot,
    Template,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SessionState {
    Starting,
    Ready,
    Closing,
    Closed,
    Destroyed,
    Failed,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WorkspaceSpec {
    pub mode: WorkspaceMode,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub root: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub snapshot_ref: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub template_ref: Option<String>,
    #[serde(default)]
    pub mount_as_workspace: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct EnvPolicy {
    #[serde(default)]
    pub allowlist: Vec<String>,
    #[serde(default)]
    pub denylist: Vec<String>,
    #[serde(default)]
    pub injected: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct NetworkPolicy {
    pub enabled: bool,
    #[serde(default)]
    pub allow_hosts: Vec<String>,
    #[serde(default)]
    pub deny_hosts: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ProcessPolicy {
    pub allow_exec: bool,
    #[serde(default)]
    pub allowed_commands: Vec<String>,
    #[serde(default)]
    pub denied_commands: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_processes: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ExecutionPolicy {
    pub read_roots: Vec<String>,
    pub write_roots: Vec<String>,
    pub process: ProcessPolicy,
    pub network: NetworkPolicy,
    #[serde(default)]
    pub env: EnvPolicy,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_duration_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_output_bytes: Option<usize>,
}

impl Default for ExecutionPolicy {
    fn default() -> Self {
        Self {
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
            env: EnvPolicy::default(),
            max_duration_ms: Some(300_000),
            max_output_bytes: Some(100_000),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CreateSessionRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    pub workspace: WorkspaceSpec,
    #[serde(default)]
    pub policy: ExecutionPolicy,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ttl_ms: Option<u64>,
    #[serde(default)]
    pub metadata: Map<String, Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WorkspaceBinding {
    pub root: String,
    pub logical_root: String,
    pub mode: WorkspaceMode,
    pub fresh: bool,
    pub managed: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Session {
    pub id: String,
    pub state: SessionState,
    pub workspace: WorkspaceBinding,
    pub policy: ExecutionPolicy,
    #[serde(default)]
    pub metadata: Map<String, Value>,
    pub created_at: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CreateSessionResponse {
    pub session: Session,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WorkspaceArtifact {
    pub session_id: String,
    pub artifact: ResourceRef,
    pub manifest: ResourceRef,
    pub format: String,
    pub bytes: u64,
    pub hash: String,
    pub file_count: usize,
    pub directory_count: usize,
    pub symlink_count: usize,
    pub entries: Vec<WorkspaceArtifactEntry>,
    pub created_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WorkspaceArtifactEntry {
    pub logical_path: String,
    pub archive_path: String,
    pub kind: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub link_target: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hash: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ToolResultStatus {
    Success,
    Error,
    Timeout,
    Cancelled,
    PolicyDenied,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum InvocationState {
    Queued,
    Leased,
    Running,
    Completed,
    Failed,
    Timeout,
    Cancelled,
    PolicyDenied,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ToolCapability {
    pub kind: String,
    #[serde(default)]
    pub scope: Map<String, Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ToolInvocationRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub invocation_id: Option<String>,
    pub session_id: String,
    pub tool_name: String,
    pub arguments: Map<String, Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cwd: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timeout_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_output_bytes: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub idempotency_key: Option<String>,
    #[serde(default)]
    pub required_capabilities: Vec<ToolCapability>,
    #[serde(default)]
    pub metadata: Map<String, Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ToolInvocationRequested {
    #[serde(rename = "type")]
    pub event_type: String,
    #[serde(flatten)]
    pub request: ToolInvocationRequest,
    pub created_at: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deadline_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub agent_run_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ToolInvocationClaimed {
    #[serde(rename = "type")]
    pub event_type: String,
    pub invocation_id: String,
    pub session_id: String,
    pub attempt_id: String,
    pub worker_id: String,
    pub lease_token: String,
    pub attempt_number: u32,
    pub leased_until: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ToolInvocationCompleted {
    #[serde(rename = "type")]
    pub event_type: String,
    pub invocation_id: String,
    pub session_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub attempt_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lease_token: Option<String>,
    pub result: ToolInvocationResult,
    pub completed_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ToolInvocationFailed {
    #[serde(rename = "type")]
    pub event_type: String,
    pub invocation_id: String,
    pub session_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub attempt_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lease_token: Option<String>,
    pub error: ErrorEnvelope,
    pub failed_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ErrorEnvelope {
    pub code: String,
    pub message: String,
    pub retryable: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ToolWorker {
    pub id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub language: Option<String>,
    pub transport: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
    pub last_seen_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ToolInvocationResult {
    pub invocation_id: String,
    pub session_id: String,
    pub tool_name: String,
    pub status: ToolResultStatus,
    pub output: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub summary: Option<String>,
    pub effects: Vec<Effect>,
    pub duration_ms: u64,
    #[serde(default)]
    pub metadata: Map<String, Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum EffectOperation {
    Read,
    Create,
    Update,
    Delete,
    Execute,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct StateRef {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hash: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content_ref: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub snapshot_ref: Option<String>,
    #[serde(default)]
    pub metadata: Map<String, Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ResourceRef {
    pub resource_type: String,
    pub uri: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Effect {
    pub id: String,
    pub invocation_id: String,
    pub kind: String,
    pub resource: ResourceRef,
    pub operation: EffectOperation,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub before: Option<StateRef>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub after: Option<StateRef>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub summary: Option<String>,
    pub reversible: bool,
    pub occurred_at: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn event_envelope_carries_protocol_version_and_correlation_ids() {
        let envelope = EventEnvelope::new(
            "tool.invocation.requested",
            "evt_1",
            json!({ "toolName": "Read" }),
        )
        .session_id("sess_1")
        .invocation_id("inv_1")
        .metadata("source", json!("test"));

        assert_eq!(envelope.protocol.version, PROTOCOL_VERSION);
        assert_eq!(envelope.schema_version, EVENT_SCHEMA_VERSION);
        assert_eq!(envelope.session_id.as_deref(), Some("sess_1"));
        assert_eq!(envelope.invocation_id.as_deref(), Some("inv_1"));
        assert_eq!(envelope.metadata.get("source"), Some(&json!("test")));
    }

    #[test]
    fn event_envelope_rejects_unknown_fields() {
        let envelope = json!({
            "protocol": {
                "name": "executioner",
                "version": "executioner.v1",
                "major": 1
            },
            "schemaVersion": 1,
            "eventId": "evt_1",
            "type": "tool.invocation.requested",
            "occurredAt": "now",
            "payload": { "toolName": "Read" },
            "metadata": {},
            "padding": "unexpected"
        });
        let protocol = json!({
            "name": "executioner",
            "version": "executioner.v1",
            "major": 1,
            "padding": "unexpected"
        });

        let envelope_err = serde_json::from_value::<EventEnvelope<Value>>(envelope).unwrap_err();
        let protocol_err = serde_json::from_value::<ProtocolVersion>(protocol).unwrap_err();

        assert!(envelope_err.to_string().contains("unknown field"));
        assert!(protocol_err.to_string().contains("unknown field"));
    }

    #[test]
    fn workspace_artifact_rejects_unknown_fields() {
        let artifact = json!({
            "sessionId": "sess",
            "artifact": {
                "resourceType": "artifact",
                "uri": "file:///tmp/workspace.tar"
            },
            "manifest": {
                "resourceType": "artifact_manifest",
                "uri": "file:///tmp/workspace.manifest.json"
            },
            "format": "tar",
            "bytes": 0,
            "hash": "sha256:empty",
            "fileCount": 0,
            "directoryCount": 0,
            "symlinkCount": 0,
            "entries": [],
            "createdAt": "now",
            "padding": "unexpected"
        });
        let entry = json!({
            "logicalPath": "/workspace/file.txt",
            "archivePath": "file.txt",
            "kind": "file",
            "bytes": 0,
            "hash": "sha256:empty",
            "padding": "unexpected"
        });
        let resource = json!({
            "resourceType": "artifact",
            "uri": "file:///tmp/workspace.tar",
            "padding": "unexpected"
        });
        let resource_with_smuggled_metadata = json!({
            "resourceType": "artifact",
            "uri": "file:///tmp/workspace.tar",
            "hash": "sha256:smuggled",
            "bytes": 0,
            "metadata": { "smuggled": true }
        });

        let artifact_err = serde_json::from_value::<WorkspaceArtifact>(artifact).unwrap_err();
        let entry_err = serde_json::from_value::<WorkspaceArtifactEntry>(entry).unwrap_err();
        let resource_err = serde_json::from_value::<ResourceRef>(resource).unwrap_err();
        let resource_metadata_err =
            serde_json::from_value::<ResourceRef>(resource_with_smuggled_metadata).unwrap_err();

        assert!(artifact_err.to_string().contains("unknown field"));
        assert!(entry_err.to_string().contains("unknown field"));
        assert!(resource_err.to_string().contains("unknown field"));
        assert!(resource_metadata_err.to_string().contains("unknown field"));
    }

    #[test]
    fn session_responses_reject_unknown_fields() {
        let response = json!({
            "session": {
                "id": "sess",
                "state": "ready",
                "workspace": {
                    "root": "/tmp/workspace",
                    "logicalRoot": "/workspace",
                    "mode": "new",
                    "fresh": true,
                    "managed": true
                },
                "policy": ExecutionPolicy::default(),
                "metadata": {},
                "createdAt": "now"
            },
            "padding": "unexpected"
        });
        let session = json!({
            "id": "sess",
            "state": "ready",
            "workspace": {
                "root": "/tmp/workspace",
                "logicalRoot": "/workspace",
                "mode": "new",
                "fresh": true,
                "managed": true
            },
            "policy": ExecutionPolicy::default(),
            "metadata": {},
            "createdAt": "now",
            "padding": "unexpected"
        });
        let workspace = json!({
            "root": "/tmp/workspace",
            "logicalRoot": "/workspace",
            "mode": "new",
            "fresh": true,
            "managed": true,
            "padding": "unexpected"
        });

        let response_err = serde_json::from_value::<CreateSessionResponse>(response).unwrap_err();
        let session_err = serde_json::from_value::<Session>(session).unwrap_err();
        let workspace_err = serde_json::from_value::<WorkspaceBinding>(workspace).unwrap_err();

        assert!(response_err.to_string().contains("unknown field"));
        assert!(session_err.to_string().contains("unknown field"));
        assert!(workspace_err.to_string().contains("unknown field"));
    }

    #[test]
    fn terminal_events_reject_unknown_fields() {
        let completed = json!({
            "type": "tool.invocation.completed",
            "invocationId": "inv",
            "sessionId": "sess",
            "result": {
                "invocationId": "inv",
                "sessionId": "sess",
                "toolName": "Read",
                "status": "success",
                "output": "ok",
                "effects": [],
                "durationMs": 0,
                "metadata": {}
            },
            "completedAt": "now",
            "padding": "unexpected"
        });
        let completed_result = json!({
            "type": "tool.invocation.completed",
            "invocationId": "inv",
            "sessionId": "sess",
            "result": {
                "invocationId": "inv",
                "sessionId": "sess",
                "toolName": "Read",
                "status": "success",
                "output": "ok",
                "effects": [],
                "durationMs": 0,
                "metadata": {},
                "padding": "unexpected"
            },
            "completedAt": "now"
        });
        let failed = json!({
            "type": "tool.invocation.failed",
            "invocationId": "inv",
            "sessionId": "sess",
            "error": {
                "code": "failed",
                "message": "failed",
                "retryable": false,
                "padding": "unexpected"
            },
            "failedAt": "now"
        });
        let completed_effect = json!({
            "type": "tool.invocation.completed",
            "invocationId": "inv",
            "sessionId": "sess",
            "result": {
                "invocationId": "inv",
                "sessionId": "sess",
                "toolName": "Read",
                "status": "success",
                "output": "ok",
                "effects": [{
                    "id": "effect",
                    "invocationId": "inv",
                    "kind": "file.read",
                    "resource": {
                        "resourceType": "file",
                        "uri": "file:///workspace/file.txt"
                    },
                    "operation": "read",
                    "before": {
                        "hash": "sha256:empty",
                        "padding": "unexpected"
                    },
                    "reversible": false,
                    "occurredAt": "now",
                    "padding": "unexpected"
                }],
                "durationMs": 0,
                "metadata": {}
            },
            "completedAt": "now"
        });

        let completed_err =
            serde_json::from_value::<ToolInvocationCompleted>(completed).unwrap_err();
        let completed_result_err =
            serde_json::from_value::<ToolInvocationCompleted>(completed_result).unwrap_err();
        let failed_err = serde_json::from_value::<ToolInvocationFailed>(failed).unwrap_err();
        let completed_effect_err =
            serde_json::from_value::<ToolInvocationCompleted>(completed_effect).unwrap_err();

        assert!(completed_err.to_string().contains("unknown field"));
        assert!(completed_result_err.to_string().contains("unknown field"));
        assert!(failed_err.to_string().contains("unknown field"));
        assert!(completed_effect_err.to_string().contains("unknown field"));
    }

    #[test]
    fn lifecycle_events_reject_unknown_fields() {
        let requested = json!({
            "type": "tool.invocation.requested",
            "invocationId": "inv",
            "sessionId": "sess",
            "toolName": "Read",
            "arguments": {},
            "createdAt": "now",
            "requiredCapabilities": [],
            "metadata": {},
            "padding": "unexpected"
        });
        let claimed = json!({
            "type": "tool.invocation.claimed",
            "invocationId": "inv",
            "sessionId": "sess",
            "attemptId": "attempt",
            "workerId": "worker",
            "leaseToken": "lease",
            "attemptNumber": 1,
            "leasedUntil": "soon",
            "padding": "unexpected"
        });
        let worker = json!({
            "id": "worker",
            "transport": "file",
            "lastSeenAt": "now",
            "padding": "unexpected"
        });

        let requested_err =
            serde_json::from_value::<ToolInvocationRequested>(requested).unwrap_err();
        let claimed_err = serde_json::from_value::<ToolInvocationClaimed>(claimed).unwrap_err();
        let worker_err = serde_json::from_value::<ToolWorker>(worker).unwrap_err();

        assert!(requested_err.to_string().contains("unknown field"));
        assert!(claimed_err.to_string().contains("unknown field"));
        assert!(worker_err.to_string().contains("unknown field"));
    }
}
