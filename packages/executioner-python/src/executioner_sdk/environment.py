from __future__ import annotations

import io
import json
import hashlib
from importlib import resources as importlib_resources
import os
import re
import shutil
import socket
import stat
import subprocess
import tarfile
import tempfile
import time
import uuid
from dataclasses import asdict, dataclass, field
from pathlib import Path, PurePosixPath
from typing import Any, Literal, Mapping, TypedDict
from urllib import error as urlerror
from urllib import parse as urlparse
from urllib import request as urlrequest

class _NoRedirectHandler(urlrequest.HTTPRedirectHandler):
    def redirect_request(self, *args: Any, **kwargs: Any) -> None:
        return None


_NO_REDIRECT_OPENER = urlrequest.build_opener(_NoRedirectHandler)

WorkspaceKind = Literal["new", "existing"]
WorkerKind = Literal["managed", "external"]
HostKind = Literal["managed", "http"]
BackendKind = Literal["file"]
ToolStatus = Literal["success", "error", "timeout", "cancelled", "policy_denied"]
EffectOperation = Literal["read", "create", "update", "delete", "execute"]
WorkspaceMode = Literal["new", "existing", "snapshot", "template"]
EnvironmentState = Literal["starting", "ready", "closing", "closed", "destroyed", "failed"]
SessionState = Literal["starting", "ready", "closing", "closed", "destroyed", "failed"]
_TOOL_STATUSES = {"success", "error", "timeout", "cancelled", "policy_denied"}
_EFFECT_OPERATIONS = {"read", "create", "update", "delete", "execute"}
_WORKSPACE_MODES = {"new", "existing", "snapshot", "template"}
_ENVIRONMENT_STATES = {"starting", "ready", "closing", "closed", "destroyed", "failed"}
_SESSION_STATES = {"starting", "ready", "closing", "closed", "destroyed", "failed"}
_RUNTIME_PACKAGE_NAMES = ("substrate_runtime", "executioner_runtime")


def _json_mapping(value: Any, label: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise TypeError(f"{label} must be a JSON object")
    return value


def _required_field(value: Mapping[str, Any], key: str, label: str) -> Any:
    if key not in value or value[key] is None:
        raise ValueError(f"{label} is required")
    return value[key]


def _reject_unknown_fields(value: Mapping[str, Any], allowed: set[str], label: str) -> None:
    unknown = sorted(set(value) - allowed)
    if unknown:
        raise ValueError(f"unknown {label} field: {unknown[0]}")


def _json_list(value: Any, label: str) -> list[Any]:
    if not isinstance(value, list):
        raise TypeError(f"{label} must be a JSON array")
    return value


def _json_string(value: Any, label: str) -> str:
    if not isinstance(value, str):
        raise TypeError(f"{label} must be a string")
    return value


def _json_optional_string(value: Any, label: str) -> str | None:
    if value is None:
        return None
    return _json_string(value, label)


def _json_non_empty_string(value: Any, label: str) -> str:
    value = _json_string(value, label)
    if not value:
        raise ValueError(f"{label} must be non-empty")
    return value


def _json_optional_non_empty_string(value: Any, label: str) -> str | None:
    if value is None:
        return None
    return _json_non_empty_string(value, label)


def _json_optional_identifier(value: Any, label: str) -> str | None:
    value = _json_optional_non_empty_string(value, label)
    if value is not None and not _IDENTIFIER_RE.match(value):
        raise ValueError(f"invalid {label}: only ASCII letters, numbers, '-' and '_' are allowed")
    return value


def _json_absolute_path_string(value: Any, label: str) -> str:
    value = _json_non_empty_string(value, label)
    if not Path(value).is_absolute():
        raise ValueError(f"{label} must be absolute")
    return value


def _json_bool(value: Any, label: str) -> bool:
    if not isinstance(value, bool):
        raise TypeError(f"{label} must be a boolean")
    return value


def _json_int(value: Any, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{label} must be an integer")
    return value


def _json_non_negative_int(value: Any, label: str) -> int:
    value = _json_int(value, label)
    if value < 0:
        raise ValueError(f"{label} must be non-negative")
    return value


def _json_positive_int(value: Any, label: str) -> int:
    value = _json_non_negative_int(value, label)
    if value == 0:
        raise ValueError(f"{label} must be positive")
    return value


def _json_tcp_port(value: Any, label: str) -> int:
    port = _json_non_negative_int(value, label)
    if port < 1 or port > 65535:
        raise ValueError(f"{label} must be between 1 and 65535")
    return port


def _json_output_limit(value: Any, label: str) -> int:
    limit = _json_non_negative_int(value, label)
    if limit > _MAX_OUTPUT_BYTES:
        raise ValueError(
            f"{label} exceeds maximum supported output size of {_MAX_OUTPUT_BYTES} bytes"
        )
    return limit


def _json_tool_timeout(value: Any, label: str) -> int:
    timeout = _json_non_negative_int(value, label)
    if timeout == 0:
        raise ValueError(f"{label} must be positive")
    if timeout > _MAX_TOOL_TIMEOUT_MS:
        raise ValueError(
            f"{label} exceeds maximum supported tool timeout of {_MAX_TOOL_TIMEOUT_MS}ms"
        )
    return timeout


def _json_process_count(value: Any, label: str) -> int:
    count = _json_non_negative_int(value, label)
    if count > _MAX_PROCESS_COUNT:
        raise ValueError(
            f"{label} exceeds maximum supported process count of {_MAX_PROCESS_COUNT}"
        )
    return count


def _optional_bool(value: Any, label: str) -> bool | None:
    if value is None:
        return None
    return _json_bool(value, label)


def _optional_string_list(value: Any, label: str) -> list[str] | None:
    if value is None:
        return None
    if not isinstance(value, list) or not all(isinstance(entry, str) for entry in value):
        raise TypeError(f"{label} must be a string list")
    return value


def _optional_string_dict(value: Any, label: str) -> dict[str, str] | None:
    if value is None:
        return None
    if not isinstance(value, dict):
        raise TypeError(f"{label} must be a string map")
    for key, entry in value.items():
        if not isinstance(entry, str):
            raise TypeError(f"{label}.{key} must be a string")
    return value


def _require_kind(value: Any, label: str, allowed: tuple[str, ...]) -> str:
    if not isinstance(value, str) or value not in allowed:
        raise ValueError(f"{label} must be one of: {', '.join(allowed)}")
    return value


class WorkspaceConfig(TypedDict, total=False):
    kind: WorkspaceKind
    root: str


class WorkerConfig(TypedDict, total=False):
    kind: WorkerKind
    id: str
    idleSleepMs: int


class HostConfig(TypedDict, total=False):
    kind: HostKind
    stateDir: str
    host: str
    port: int
    baseUrl: str


class AttachedHostConfig(TypedDict):
    kind: Literal["http"]
    baseUrl: str


class BackendConfig(TypedDict, total=False):
    kind: BackendKind
    queueDir: str


class AttachedEnvironmentConfig(TypedDict, total=False):
    host: AttachedHostConfig
    environmentId: str
    submitTimeoutMs: int


class LifecycleConfig(TypedDict, total=False):
    destroyOnClose: bool
    cleanupQueueOnClose: bool
    cleanupStateOnClose: bool


class ProcessPolicyConfig(TypedDict, total=False):
    allowExec: bool
    allowedCommands: list[str]
    deniedCommands: list[str]
    maxProcesses: int


class NetworkPolicyConfig(TypedDict, total=False):
    enabled: bool
    allowHosts: list[str]
    denyHosts: list[str]


class EnvPolicyConfig(TypedDict, total=False):
    allowlist: list[str]
    denylist: list[str]
    injected: dict[str, str]


class PolicyConfig(TypedDict, total=False):
    readRoots: list[str]
    writeRoots: list[str]
    process: ProcessPolicyConfig
    network: NetworkPolicyConfig
    env: EnvPolicyConfig
    maxDurationMs: int
    maxOutputBytes: int


class ToolCall(TypedDict, total=False):
    toolName: str
    arguments: dict[str, Any]
    cwd: str
    invocationId: str
    timeoutMs: int
    maxOutputBytes: int
    metadata: dict[str, Any]


class ToolSchema(TypedDict):
    name: str
    description: str
    input_schema: dict[str, Any]


class ToolSubmitOptions(TypedDict, total=False):
    cwd: str
    invocationId: str
    timeoutMs: int
    maxOutputBytes: int
    metadata: dict[str, Any]


class EditToolArguments(TypedDict, total=False):
    path: str
    oldString: str
    newString: str
    replaceAll: bool


_TOOL_SCHEMAS: tuple[ToolSchema, ...] = (
    {
        "name": "Read",
        "description": "Read a UTF-8 text file from the workspace.",
        "input_schema": {
            "type": "object",
            "properties": {
                "path": {"type": "string", "description": "Workspace-relative or /workspace path to read."},
                "maxBytes": {"type": "integer", "minimum": 1},
                "startLine": {"type": "integer", "minimum": 1},
                "endLine": {"type": "integer", "minimum": 1},
            },
            "required": ["path"],
            "additionalProperties": False,
        },
    },
    {
        "name": "Write",
        "description": "Create a new UTF-8 text file in the workspace.",
        "input_schema": {
            "type": "object",
            "properties": {
                "path": {"type": "string"},
                "content": {"type": "string"},
            },
            "required": ["path", "content"],
            "additionalProperties": False,
        },
    },
    {
        "name": "Edit",
        "description": "Replace text in an existing workspace file.",
        "input_schema": {
            "type": "object",
            "properties": {
                "path": {"type": "string"},
                "oldString": {"type": "string"},
                "newString": {"type": "string"},
                "replaceAll": {"type": "boolean"},
            },
            "required": ["path", "oldString", "newString"],
            "additionalProperties": False,
        },
    },
    {
        "name": "List",
        "description": "List entries in the current workspace directory.",
        "input_schema": {
            "type": "object",
            "properties": {},
            "additionalProperties": False,
        },
    },
    {
        "name": "Glob",
        "description": "Find workspace files whose relative paths match a glob pattern.",
        "input_schema": {
            "type": "object",
            "properties": {
                "pattern": {"type": "string"},
                "maxResults": {"type": "integer", "minimum": 1},
                "maxDepth": {"type": "integer", "minimum": 1},
                "includeHidden": {"type": "boolean"},
            },
            "required": ["pattern"],
            "additionalProperties": False,
        },
    },
    {
        "name": "Grep",
        "description": "Search workspace files for a regular expression.",
        "input_schema": {
            "type": "object",
            "properties": {
                "pattern": {"type": "string"},
                "caseSensitive": {"type": "boolean"},
                "maxResults": {"type": "integer", "minimum": 1},
                "path": {"type": "string"},
                "glob": {"type": "string"},
                "type": {"type": "string"},
            },
            "required": ["pattern"],
            "additionalProperties": False,
        },
    },
    {
        "name": "Bash",
        "description": "Run a shell command allowed by the session policy inside the workspace.",
        "input_schema": {
            "type": "object",
            "properties": {
                "command": {"type": "string"},
                "timeout": {"type": "integer", "minimum": 1},
            },
            "required": ["command"],
            "additionalProperties": False,
        },
    },
)


def tool(name: str, **arguments: Any) -> ToolCall:
    """Create a tool call without exposing the wire envelope shape."""
    if not isinstance(name, str) or not name:
        raise TypeError("tool name must be a non-empty string")
    return {
        "toolName": name,
        "arguments": dict(arguments),
    }


def tool_schemas() -> list[ToolSchema]:
    return [
        {
            "name": schema["name"],
            "description": schema["description"],
            "input_schema": dict(schema["input_schema"]),
        }
        for schema in _TOOL_SCHEMAS
    ]


@dataclass(frozen=True)
class StateEffect:
    id: str
    invocationId: str
    kind: str
    resourceType: str
    uri: str
    operation: EffectOperation
    summary: str | None = None
    reversible: bool = False
    occurredAt: str = ""

    @classmethod
    def from_json(cls, value: Mapping[str, Any]) -> "StateEffect":
        value = _json_mapping(value, "state effect")
        _reject_unknown_fields(
            value,
            {"id", "invocationId", "kind", "resource", "operation", "before", "after", "summary", "reversible", "occurredAt"},
            "state effect",
        )
        resource = _json_mapping(_required_field(value, "resource", "state effect resource"), "state effect resource")
        _reject_unknown_fields(resource, {"resourceType", "uri"}, "state effect resource")
        _validate_optional_state_ref(value.get("before"), "state effect before")
        _validate_optional_state_ref(value.get("after"), "state effect after")
        operation = _json_string(_required_field(value, "operation", "state effect operation"), "state effect operation")
        if operation not in _EFFECT_OPERATIONS:
            raise ValueError(f"unknown state effect operation: {operation}")
        return cls(
            id=_json_string(_required_field(value, "id", "state effect id"), "state effect id"),
            invocationId=_json_string(_required_field(value, "invocationId", "state effect invocationId"), "state effect invocationId"),
            kind=_json_string(_required_field(value, "kind", "state effect kind"), "state effect kind"),
            resourceType=_json_string(_required_field(resource, "resourceType", "state effect resourceType"), "state effect resourceType"),
            uri=_json_string(_required_field(resource, "uri", "state effect uri"), "state effect uri"),
            operation=operation,  # type: ignore[arg-type]
            summary=_json_optional_string(value.get("summary"), "state effect summary"),
            reversible=_json_bool(_required_field(value, "reversible", "state effect reversible"), "state effect reversible"),
            occurredAt=_json_string(_required_field(value, "occurredAt", "state effect occurredAt"), "state effect occurredAt"),
        )


def _validate_optional_state_ref(value: Any, label: str) -> None:
    if value is None:
        return
    state_ref = _json_mapping(value, label)
    _reject_unknown_fields(state_ref, {"hash", "bytes", "contentRef", "snapshotRef", "metadata"}, label)
    if state_ref.get("hash") is not None:
        _json_string(state_ref["hash"], f"{label} hash")
    if state_ref.get("bytes") is not None:
        _json_non_negative_int(state_ref["bytes"], f"{label} bytes")
    if state_ref.get("contentRef") is not None:
        _json_string(state_ref["contentRef"], f"{label} contentRef")
    if state_ref.get("snapshotRef") is not None:
        _json_string(state_ref["snapshotRef"], f"{label} snapshotRef")
    if state_ref.get("metadata") is not None:
        _json_mapping(state_ref["metadata"], f"{label} metadata")


@dataclass(frozen=True)
class WorkspaceInfo:
    root: str
    logicalRoot: str
    mode: WorkspaceMode
    fresh: bool
    managed: bool

    @classmethod
    def from_json(cls, value: Mapping[str, Any]) -> "WorkspaceInfo":
        value = _json_mapping(value, "workspace")
        _reject_unknown_fields(
            value,
            {"root", "logicalRoot", "mode", "fresh", "managed"},
            "workspace",
        )
        mode = _json_string(_required_field(value, "mode", "workspace mode"), "workspace mode")
        if mode not in _WORKSPACE_MODES:
            raise ValueError(f"unknown workspace mode: {mode}")
        return cls(
            root=_json_string(_required_field(value, "root", "workspace root"), "workspace root"),
            logicalRoot=_json_string(_required_field(value, "logicalRoot", "workspace logicalRoot"), "workspace logicalRoot"),
            mode=mode,  # type: ignore[arg-type]
            fresh=_json_bool(_required_field(value, "fresh", "workspace fresh"), "workspace fresh"),
            managed=_json_bool(_required_field(value, "managed", "workspace managed"), "workspace managed"),
        )


@dataclass(frozen=True)
class EnvironmentInfo:
    id: str
    state: EnvironmentState
    workspace: WorkspaceInfo
    createdAt: str
    revision: int
    expiresAt: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_json(cls, value: Mapping[str, Any]) -> "EnvironmentInfo":
        value = _json_mapping(value, "environment")
        _reject_unknown_fields(
            value,
            {"id", "state", "workspace", "policy", "createdAt", "expiresAt", "metadata", "revision"},
            "environment",
        )
        if value.get("policy") is not None:
            _json_mapping(value["policy"], "environment policy")
        state = _json_string(_required_field(value, "state", "environment state"), "environment state")
        if state not in _ENVIRONMENT_STATES:
            raise ValueError(f"unknown environment state: {state}")
        return cls(
            id=_json_string(_required_field(value, "id", "environment id"), "environment id"),
            state=state,  # type: ignore[arg-type]
            workspace=WorkspaceInfo.from_json(_json_mapping(_required_field(value, "workspace", "environment workspace"), "environment workspace")),
            createdAt=_json_string(_required_field(value, "createdAt", "environment createdAt"), "environment createdAt"),
            revision=_json_non_negative_int(_required_field(value, "revision", "environment revision"), "environment revision"),
            expiresAt=_json_optional_string(value.get("expiresAt"), "environment expiresAt"),
            metadata=dict(_json_mapping(value.get("metadata", {}), "environment metadata")),
        )


@dataclass(frozen=True)
class SessionInfo:
    id: str
    state: SessionState
    workspace: WorkspaceInfo
    createdAt: str
    expiresAt: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_json(cls, value: Mapping[str, Any]) -> "SessionInfo":
        value = _json_mapping(value, "session")
        _reject_unknown_fields(
            value,
            {"id", "state", "workspace", "policy", "createdAt", "expiresAt", "metadata"},
            "session",
        )
        if value.get("policy") is not None:
            _json_mapping(value["policy"], "session policy")
        state = _json_string(_required_field(value, "state", "session state"), "session state")
        if state not in _SESSION_STATES:
            raise ValueError(f"unknown session state: {state}")
        return cls(
            id=_json_string(_required_field(value, "id", "session id"), "session id"),
            state=state,  # type: ignore[arg-type]
            workspace=WorkspaceInfo.from_json(_json_mapping(_required_field(value, "workspace", "session workspace"), "session workspace")),
            createdAt=_json_string(_required_field(value, "createdAt", "session createdAt"), "session createdAt"),
            expiresAt=_json_optional_string(value.get("expiresAt"), "session expiresAt"),
            metadata=dict(_json_mapping(value.get("metadata", {}), "session metadata")),
        )


@dataclass(frozen=True)
class SubmitResult:
    invocationId: str
    sessionId: str
    toolName: str
    status: ToolStatus
    output: str
    error: str | None
    summary: str | None
    effects: list[StateEffect]
    durationMs: int
    metadata: dict[str, Any]

    @classmethod
    def from_json(cls, value: Mapping[str, Any]) -> "SubmitResult":
        value = _json_mapping(value, "submit result")
        _reject_unknown_fields(
            value,
            {"invocationId", "sessionId", "toolName", "status", "output", "error", "summary", "effects", "durationMs", "metadata"},
            "submit result",
        )
        status = _json_string(_required_field(value, "status", "submit result status"), "submit result status")
        if status not in _TOOL_STATUSES:
            raise ValueError(f"unknown submit result status: {status}")
        return cls(
            invocationId=_json_string(_required_field(value, "invocationId", "submit result invocationId"), "submit result invocationId"),
            sessionId=_json_string(_required_field(value, "sessionId", "submit result sessionId"), "submit result sessionId"),
            toolName=_json_string(_required_field(value, "toolName", "submit result toolName"), "submit result toolName"),
            status=status,  # type: ignore[arg-type]
            output=_json_string(_required_field(value, "output", "submit result output"), "submit result output"),
            error=_json_optional_string(value.get("error"), "submit result error"),
            summary=_json_optional_string(value.get("summary"), "submit result summary"),
            effects=[
                StateEffect.from_json(effect)
                for effect in _json_list(_required_field(value, "effects", "submit result effects"), "submit result effects")
            ],
            durationMs=_json_non_negative_int(_required_field(value, "durationMs", "submit result durationMs"), "submit result durationMs"),
            metadata=dict(_json_mapping(value.get("metadata", {}), "submit result metadata")),
        )


@dataclass(frozen=True)
class ResourceRef:
    resourceType: str
    uri: str

    @classmethod
    def from_json(cls, value: Mapping[str, Any]) -> "ResourceRef":
        value = _json_mapping(value, "resource ref")
        _reject_unknown_fields(
            value,
            {"resourceType", "uri"},
            "resource ref",
        )
        return cls(
            resourceType=_json_string(_required_field(value, "resourceType", "resource type"), "resource type"),
            uri=_json_string(_required_field(value, "uri", "resource uri"), "resource uri"),
        )


@dataclass(frozen=True)
class WorkspaceArtifactEntry:
    logicalPath: str
    archivePath: str
    kind: str
    linkTarget: str | None = None
    bytes: int | None = None
    hash: str | None = None

    @classmethod
    def from_json(cls, value: Mapping[str, Any]) -> "WorkspaceArtifactEntry":
        value = _json_mapping(value, "workspace artifact entry")
        _reject_unknown_fields(
            value,
            {"logicalPath", "archivePath", "kind", "linkTarget", "bytes", "hash"},
            "workspace artifact entry",
        )
        bytes_value = value.get("bytes")
        return cls(
            logicalPath=_json_string(_required_field(value, "logicalPath", "artifact entry logicalPath"), "artifact entry logicalPath"),
            archivePath=_json_string(_required_field(value, "archivePath", "artifact entry archivePath"), "artifact entry archivePath"),
            kind=_json_string(_required_field(value, "kind", "artifact entry kind"), "artifact entry kind"),
            linkTarget=_json_optional_string(value.get("linkTarget"), "artifact entry linkTarget"),
            bytes=None if bytes_value is None else _json_non_negative_int(bytes_value, "artifact entry bytes"),
            hash=_json_optional_string(value.get("hash"), "artifact entry hash"),
        )


@dataclass(frozen=True)
class WorkspaceArtifact:
    environmentId: str
    artifact: ResourceRef
    manifest: ResourceRef
    format: str
    bytes: int
    hash: str
    fileCount: int
    directoryCount: int
    symlinkCount: int
    entries: list[WorkspaceArtifactEntry]
    createdAt: str

    @classmethod
    def from_json(cls, value: Mapping[str, Any]) -> "WorkspaceArtifact":
        value = _json_mapping(value, "workspace artifact")
        _reject_unknown_fields(
            value,
            {
                "environmentId",
                "artifact",
                "manifest",
                "format",
                "bytes",
                "hash",
                "fileCount",
                "directoryCount",
                "symlinkCount",
                "entries",
                "createdAt",
            },
            "workspace artifact",
        )
        return cls(
            environmentId=_json_string(_required_field(value, "environmentId", "artifact environmentId"), "artifact environmentId"),
            artifact=ResourceRef.from_json(_json_mapping(_required_field(value, "artifact", "artifact resource"), "artifact resource")),
            manifest=ResourceRef.from_json(_json_mapping(_required_field(value, "manifest", "artifact manifest"), "artifact manifest")),
            format=_json_string(_required_field(value, "format", "artifact format"), "artifact format"),
            bytes=_json_non_negative_int(_required_field(value, "bytes", "artifact bytes"), "artifact bytes"),
            hash=_json_string(_required_field(value, "hash", "artifact hash"), "artifact hash"),
            fileCount=_json_non_negative_int(_required_field(value, "fileCount", "artifact fileCount"), "artifact fileCount"),
            directoryCount=_json_non_negative_int(_required_field(value, "directoryCount", "artifact directoryCount"), "artifact directoryCount"),
            symlinkCount=_json_non_negative_int(_required_field(value, "symlinkCount", "artifact symlinkCount"), "artifact symlinkCount"),
            entries=[
                WorkspaceArtifactEntry.from_json(entry)
                for entry in _json_list(_required_field(value, "entries", "artifact entries"), "artifact entries")
            ],
            createdAt=_json_string(_required_field(value, "createdAt", "artifact createdAt"), "artifact createdAt"),
        )


@dataclass
class _ManagedProcess:
    process: subprocess.Popen[bytes]
    name: str


@dataclass
class _RuntimeConfig:
    binaryPath: str
    queueDir: str | None
    sdkCreatedQueueDir: bool
    sdkCreatedStateDir: bool
    baseUrl: str
    host: dict[str, Any]
    worker: dict[str, Any]
    workspace: WorkspaceConfig
    policy: dict[str, Any]
    lifecycle: dict[str, bool]
    submitTimeoutMs: int
    transport: dict[str, Any] = field(default_factory=lambda: {"kind": "file"})


class ExecutionerEnvironment:
    def __init__(
        self,
        config: _RuntimeConfig,
        environment: EnvironmentInfo,
        processes: list[_ManagedProcess],
        owns_environment: bool = True,
    ) -> None:
        self._config = config
        self._environment = environment
        self._processes = processes
        self._owns_environment = owns_environment

    @classmethod
    def create(
        cls,
        *,
        binaryPath: str | None = None,
        backend: BackendConfig | None = None,
        host: HostConfig | None = None,
        worker: WorkerConfig | None = None,
        workspace: WorkspaceConfig | None = None,
        policy: PolicyConfig | None = None,
        lifecycle: LifecycleConfig | None = None,
        submitTimeoutMs: int | None = None,
    ) -> "ExecutionerEnvironment":
        runtime = _materialize_config(
            binary_path=binaryPath,
            backend=backend,
            host=host,
            worker=worker,
            workspace=workspace,
            policy=policy,
            lifecycle=lifecycle,
            submit_timeout_ms=submitTimeoutMs,
        )
        processes: list[_ManagedProcess] = []
        environment: EnvironmentInfo | None = None

        try:
            if runtime.host["kind"] == "managed":
                processes.append(
                    _spawn_process(
                        runtime.binaryPath,
                        [
                            "host",
                            "--addr",
                            f"{runtime.host['host']}:{runtime.host['port']}",
                            "--state-dir",
                            runtime.host["stateDir"],
                        ],
                        "executioner-host",
                    )
                )
                _wait_for_health(runtime.baseUrl, runtime.submitTimeoutMs)

            queue_dir = _required_queue_dir(runtime)
            _ensure_file_queue(queue_dir)
            environment = _create_environment(runtime)

            if runtime.worker["kind"] == "managed":
                processes.append(
                    _spawn_process(
                        runtime.binaryPath,
                        [
                            "worker",
                            "run",
                            "--id",
                            runtime.worker["id"],
                            "--host-url",
                            runtime.baseUrl,
                            "--queue-dir",
                            queue_dir,
                            "--idle-sleep-ms",
                            str(runtime.worker["idleSleepMs"]),
                        ],
                        "executioner-worker",
                    )
                )

            return cls(runtime, environment, processes, owns_environment=True)
        except Exception:
            _cleanup_partial_create(runtime, processes, environment)
            raise

    @classmethod
    def attach(
        cls,
        *,
        host: AttachedHostConfig,
        environmentId: str,
        submitTimeoutMs: int | None = None,
    ) -> "ExecutionerEnvironment":
        host_config = _json_mapping(host, "host")
        _reject_unknown_fields(host_config, {"kind", "baseUrl"}, "host")
        _require_kind(host_config.get("kind"), "host.kind", ("http",))
        environment_id = _json_non_empty_string(environmentId, "environmentId")
        _assert_environment_id(environment_id)
        base_url = _normalize_base_url(_json_non_empty_string(host_config.get("baseUrl"), "host.baseUrl"))
        environment = EnvironmentInfo.from_json(_get_json(f"{base_url}environments/{environment_id}"))
        _assert_environment_id(environment.id)
        runtime = _RuntimeConfig(
            binaryPath="",
            queueDir=None,
            sdkCreatedQueueDir=False,
            sdkCreatedStateDir=False,
            baseUrl=base_url,
            host={"kind": "http", "baseUrl": base_url},
            worker={"kind": "external"},
            workspace={"kind": "new"},
            policy=_materialize_policy(None),
            lifecycle={"destroyOnClose": False, "cleanupQueueOnClose": False, "cleanupStateOnClose": False},
            submitTimeoutMs=_json_positive_int(submitTimeoutMs, "submitTimeoutMs") if submitTimeoutMs is not None else 30_000,
            transport={"kind": "direct"},
        )
        return cls(runtime, environment, [], owns_environment=False)

    @property
    def environment(self) -> EnvironmentInfo:
        return self._environment

    def create_session(self, policy: PolicyConfig | None = None) -> "ExecutionerSession":
        return ExecutionerSession(
            self._config,
            _create_session(self._config, self._environment.id, policy),
        )

    def export_workspace(self) -> WorkspaceArtifact:
        _assert_environment_id(self._environment.id)
        artifact = _post_json(
            f"{self._config.baseUrl}environments/{self._environment.id}/artifacts/workspace",
            None,
        )
        return WorkspaceArtifact.from_json(artifact)

    def materialize_workspace_artifact(
        self,
        artifact: WorkspaceArtifact,
        destination: str | os.PathLike[str],
    ) -> None:
        materialize_workspace_artifact(artifact, destination)

    def close(self) -> EnvironmentInfo:
        _assert_environment_id(self._environment.id)
        worker_processes = [
            managed for managed in self._processes if managed.name != "executioner-host"
        ]
        host_processes = [
            managed for managed in self._processes if managed.name == "executioner-host"
        ]
        for managed in reversed(worker_processes):
            _terminate_process(managed)

        try:
            if not self._owns_environment:
                environment = self._environment
            elif self._config.lifecycle["destroyOnClose"]:
                environment_data = _delete_json(f"{self._config.baseUrl}environments/{self._environment.id}")
                environment = EnvironmentInfo.from_json(environment_data)
            else:
                environment_data = _post_json(f"{self._config.baseUrl}environments/{self._environment.id}/close", None)
                environment = EnvironmentInfo.from_json(environment_data)
        finally:
            for managed in reversed(host_processes):
                _terminate_process(managed)

            if self._config.lifecycle["cleanupQueueOnClose"]:
                _cleanup_queue_dir(_required_queue_dir(self._config), self._config.sdkCreatedQueueDir)
            if self._config.lifecycle["cleanupStateOnClose"] and self._config.host["kind"] == "managed":
                _cleanup_state_dir(self._config.host["stateDir"], self._config.sdkCreatedStateDir)

        return environment

    def __enter__(self) -> "ExecutionerEnvironment":
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        self.close()


class ExecutionerSession:
    def __init__(self, config: _RuntimeConfig, session: SessionInfo) -> None:
        self._config = config
        self._session = session

    @property
    def session(self) -> SessionInfo:
        return self._session

    def submit(self, call: ToolCall) -> SubmitResult:
        call = _json_mapping(call, "tool call")
        _reject_unknown_fields(
            call,
            {"toolName", "arguments", "cwd", "invocationId", "timeoutMs", "maxOutputBytes", "metadata"},
            "tool call",
        )
        arguments = call.get("arguments")
        if not isinstance(arguments, dict):
            raise TypeError("tool call arguments must be a JSON object")
        tool_name = call.get("toolName")
        if not isinstance(tool_name, str) or not tool_name:
            raise TypeError("toolName must be a non-empty string")
        cwd = call.get("cwd", "/workspace")
        if not isinstance(cwd, str):
            raise TypeError("cwd must be a string")
        timeout_ms = call.get("timeoutMs")
        if timeout_ms is not None:
            timeout_ms = _json_tool_timeout(timeout_ms, "timeoutMs")
        max_output_bytes = call.get("maxOutputBytes")
        if max_output_bytes is not None:
            max_output_bytes = _json_output_limit(max_output_bytes, "maxOutputBytes")
        metadata = call.get("metadata", {})
        if not isinstance(metadata, dict):
            raise TypeError("metadata must be a JSON object")

        invocation_id = call.get("invocationId") or f"inv_{uuid.uuid4().hex}"
        _assert_invocation_id(invocation_id)
        request = {
            "invocationId": invocation_id,
            "sessionId": self._session.id,
            "toolName": tool_name,
            "arguments": arguments,
            "cwd": cwd,
            "timeoutMs": timeout_ms,
            "maxOutputBytes": max_output_bytes,
            "metadata": metadata,
        }
        _assert_serialized_json_size("tool invocation request", request, _MAX_REQUEST_JSON_BYTES)
        if self._config.transport["kind"] == "direct":
            return SubmitResult.from_json(
                _post_json(f"{self._config.baseUrl}sessions/{self._session.id}/invocations", request)
            )

        queue_dir = _required_queue_dir(self._config)
        _ensure_file_queue(queue_dir)
        _ensure_invocation_id_unused(queue_dir, invocation_id)
        _write_json_atomic(Path(queue_dir) / "pending" / f"{invocation_id}.json", request)
        return _wait_for_result(
            queue_dir,
            invocation_id,
            self._session.id,
            self._config.submitTimeoutMs,
            tool_name=tool_name,
        )

    def execute(self, tool_call: Mapping[str, Any]) -> SubmitResult:
        normalized = _normalize_agent_tool_call(tool_call)
        return self.submit(normalized)

    def tool_schemas(self) -> list[ToolSchema]:
        return tool_schemas()

    def edit(self, args: EditToolArguments, options: ToolSubmitOptions | None = None) -> SubmitResult:
        call: ToolCall = {
            **(options or {}),
            "toolName": "Edit",
            "arguments": dict(args),
        }
        return self.submit(call)

    def submit_tool(
        self,
        name: str,
        *,
        cwd: str = "/workspace",
        timeout_ms: int | None = None,
        max_output_bytes: int | None = None,
        metadata: dict[str, Any] | None = None,
        **arguments: Any,
    ) -> SubmitResult:
        call: ToolCall = {
            "toolName": name,
            "arguments": dict(arguments),
            "cwd": cwd,
        }
        if timeout_ms is not None:
            call["timeoutMs"] = timeout_ms
        if max_output_bytes is not None:
            call["maxOutputBytes"] = max_output_bytes
        if metadata is not None:
            call["metadata"] = metadata
        return self.submit(call)

    def read(self, path: str | os.PathLike[str], *, cwd: str = "/workspace") -> str:
        return self.submit_tool("Read", cwd=cwd, path=os.fspath(path)).output

    def write(
        self,
        path: str | os.PathLike[str],
        content: str,
        *,
        cwd: str = "/workspace",
    ) -> SubmitResult:
        return self.submit_tool("Write", cwd=cwd, path=os.fspath(path), content=content)

    def bash(
        self,
        command: str,
        *,
        cwd: str = "/workspace",
        timeout_ms: int | None = None,
        max_output_bytes: int | None = None,
    ) -> str:
        return self.submit_tool(
            "Bash",
            cwd=cwd,
            timeout_ms=timeout_ms,
            max_output_bytes=max_output_bytes,
            command=command,
        ).output

    def list_files(self, cwd: str = "/workspace") -> list[str]:
        result = self.submit({
            "toolName": "List",
            "arguments": {},
            "cwd": cwd,
        })
        return _parse_list_files_result(result)

    def list(self, cwd: str = "/workspace") -> list[str]:
        return self.list_files(cwd)

    def close(self) -> SessionInfo:
        _assert_session_id(self._session.id)
        return SessionInfo.from_json(
            _post_json(f"{self._config.baseUrl}sessions/{self._session.id}/close", None)
        )


def _normalize_agent_tool_call(tool_call: Mapping[str, Any]) -> ToolCall:
    call = _json_mapping(tool_call, "agent tool call")
    name = call.get("toolName", call.get("name"))
    if not isinstance(name, str) or not name:
        raise TypeError("agent tool call name must be a non-empty string")
    arguments = call.get("arguments", call.get("args", call.get("input", {})))
    if not isinstance(arguments, dict):
        raise TypeError("agent tool call input must be a JSON object")
    normalized: ToolCall = {
        "toolName": name,
        "arguments": dict(arguments),
    }
    if isinstance(call.get("id"), str):
        normalized["metadata"] = {"toolCallId": call["id"]}
    return normalized


def _cleanup_partial_create(
    runtime: _RuntimeConfig,
    processes: list[_ManagedProcess],
    environment: EnvironmentInfo | None,
) -> None:
    worker_processes = [
        managed for managed in processes if managed.name != "executioner-host"
    ]
    host_processes = [
        managed for managed in processes if managed.name == "executioner-host"
    ]
    for managed in reversed(worker_processes):
        _terminate_process(managed)
    try:
        if environment is not None and _SESSION_ID_RE.match(environment.id):
            try:
                if runtime.lifecycle["destroyOnClose"]:
                    _delete_json(f"{runtime.baseUrl}environments/{environment.id}")
                else:
                    _post_json(f"{runtime.baseUrl}environments/{environment.id}/close", None)
            except Exception:
                pass
    finally:
        for managed in reversed(host_processes):
            _terminate_process(managed)
    if runtime.lifecycle["cleanupQueueOnClose"]:
        _cleanup_queue_dir(_required_queue_dir(runtime), runtime.sdkCreatedQueueDir)
    if runtime.lifecycle["cleanupStateOnClose"] and runtime.host["kind"] == "managed":
        _cleanup_state_dir(runtime.host["stateDir"], runtime.sdkCreatedStateDir)


def _materialize_config(
    *,
    binary_path: str | None,
    backend: BackendConfig | None,
    host: HostConfig | None,
    worker: WorkerConfig | None,
    workspace: WorkspaceConfig | None,
    policy: PolicyConfig | None,
    lifecycle: LifecycleConfig | None,
    submit_timeout_ms: int | None,
) -> _RuntimeConfig:
    backend = _json_mapping(backend or {"kind": "file"}, "backend")
    _reject_unknown_fields(backend, {"kind", "queueDir"}, "backend")
    host = _json_mapping(host or {"kind": "managed"}, "host")
    _reject_unknown_fields(host, {"kind", "stateDir", "host", "port", "baseUrl"}, "host")
    worker = _json_mapping(worker or {"kind": "managed"}, "worker")
    _reject_unknown_fields(worker, {"kind", "id", "idleSleepMs"}, "worker")
    workspace = _json_mapping(workspace or {"kind": "new"}, "workspace")
    _reject_unknown_fields(workspace, {"kind", "root"}, "workspace")
    lifecycle = _json_mapping(lifecycle or {}, "lifecycle")
    _reject_unknown_fields(lifecycle, {"destroyOnClose", "cleanupQueueOnClose", "cleanupStateOnClose"}, "lifecycle")
    _require_kind(backend.get("kind"), "backend.kind", ("file",))
    host_kind = _require_kind(host.get("kind"), "host.kind", ("managed", "http"))
    worker_kind = _require_kind(worker.get("kind"), "worker.kind", ("managed", "external"))
    _require_kind(workspace.get("kind"), "workspace.kind", ("new", "existing"))
    if workspace["kind"] == "existing":
        workspace_root = _json_absolute_path_string(workspace.get("root"), "workspace.root")
        _validate_no_symlinked_parent(Path(workspace_root).parent, "workspace.root parent")

    resolved_policy = _materialize_policy(policy)
    if host_kind == "http":
        base_url = _normalize_base_url(_json_non_empty_string(host["baseUrl"], "host.baseUrl"))
        resolved_host = {"kind": "http", "baseUrl": base_url}
        sdk_created_state_dir = False
    else:
        host_name = _json_optional_non_empty_string(host.get("host"), "host.host") or "127.0.0.1"
        port = _json_tcp_port(host.get("port"), "host.port") if host.get("port") is not None else _free_port()
        sdk_created_state_dir = host.get("stateDir") is None or not Path(_json_non_empty_string(host["stateDir"], "host.stateDir")).exists()
        if worker_kind != "external":
            _json_optional_identifier(worker.get("id"), "worker.id")
            if "idleSleepMs" in worker:
                _json_positive_int(worker.get("idleSleepMs"), "worker.idleSleepMs")
        resolved_lifecycle = {
            "destroyOnClose": _optional_bool(lifecycle.get("destroyOnClose"), "destroyOnClose") if "destroyOnClose" in lifecycle else True,
            "cleanupQueueOnClose": _optional_bool(lifecycle.get("cleanupQueueOnClose"), "cleanupQueueOnClose") if "cleanupQueueOnClose" in lifecycle else backend.get("queueDir") is None,
            "cleanupStateOnClose": _optional_bool(lifecycle.get("cleanupStateOnClose"), "cleanupStateOnClose") if "cleanupStateOnClose" in lifecycle else sdk_created_state_dir,
        }
        if host.get("stateDir") is None:
            state_dir = tempfile.mkdtemp(prefix="executioner-state-")
        else:
            state_dir = _json_non_empty_string(host["stateDir"], "host.stateDir")
        base_url = f"http://{host_name}:{port}/"
        resolved_host = {
            "kind": "managed",
            "stateDir": state_dir,
            "host": host_name,
            "port": port,
        }
    if host_kind == "http":
        if worker_kind != "external":
            _json_optional_identifier(worker.get("id"), "worker.id")
            if "idleSleepMs" in worker:
                _json_positive_int(worker.get("idleSleepMs"), "worker.idleSleepMs")
        resolved_lifecycle = {
            "destroyOnClose": _optional_bool(lifecycle.get("destroyOnClose"), "destroyOnClose") if "destroyOnClose" in lifecycle else True,
            "cleanupQueueOnClose": _optional_bool(lifecycle.get("cleanupQueueOnClose"), "cleanupQueueOnClose") if "cleanupQueueOnClose" in lifecycle else backend.get("queueDir") is None,
            "cleanupStateOnClose": _optional_bool(lifecycle.get("cleanupStateOnClose"), "cleanupStateOnClose") if "cleanupStateOnClose" in lifecycle else sdk_created_state_dir,
        }

    if backend.get("queueDir") is None:
        queue_dir = tempfile.mkdtemp(prefix="executioner-queue-")
        sdk_created_queue_dir = True
    else:
        queue_dir = _json_non_empty_string(backend["queueDir"], "backend.queueDir")
        sdk_created_queue_dir = not Path(queue_dir).exists()

    if worker_kind == "external":
        resolved_worker = {"kind": "external"}
    else:
        resolved_worker = {
            "kind": "managed",
            "id": _json_optional_identifier(worker.get("id"), "worker.id") or "executioner-python-worker",
            "idleSleepMs": _json_positive_int(worker.get("idleSleepMs"), "worker.idleSleepMs") if "idleSleepMs" in worker else 10,
        }
    return _RuntimeConfig(
        binaryPath=_resolve_binary_path(_json_optional_non_empty_string(binary_path, "binaryPath")),
        queueDir=queue_dir,
        sdkCreatedQueueDir=sdk_created_queue_dir,
        sdkCreatedStateDir=sdk_created_state_dir,
        baseUrl=base_url,
        host=resolved_host,
        worker=resolved_worker,
        workspace=workspace,
        policy=resolved_policy,
        lifecycle=resolved_lifecycle,
        submitTimeoutMs=_json_positive_int(submit_timeout_ms, "submitTimeoutMs") if submit_timeout_ms is not None else 30_000,
        transport={"kind": "file", "queueDir": queue_dir},
    )


def _required_queue_dir(config: _RuntimeConfig) -> str:
    if config.transport.get("kind") == "file":
        queue_dir = config.transport.get("queueDir")
        if isinstance(queue_dir, str):
            return queue_dir
    if config.queueDir is not None:
        return config.queueDir
    raise RuntimeError("Executioner environment has no file queue")


def _materialize_policy(policy: PolicyConfig | None) -> dict[str, Any]:
    policy = _json_mapping(policy or {}, "policy")
    _reject_unknown_fields(
        policy,
        {"readRoots", "writeRoots", "process", "network", "env", "maxDurationMs", "maxOutputBytes"},
        "policy",
    )
    process = _json_mapping(policy.get("process", {}), "process")
    _reject_unknown_fields(process, {"allowExec", "allowedCommands", "deniedCommands", "maxProcesses"}, "process")
    network = _json_mapping(policy.get("network", {}), "network")
    _reject_unknown_fields(network, {"enabled", "allowHosts", "denyHosts"}, "network")
    env = _json_mapping(policy.get("env", {}), "env")
    _reject_unknown_fields(env, {"allowlist", "denylist", "injected"}, "env")
    network_enabled = _optional_bool(network.get("enabled"), "network.enabled") if "enabled" in network else False
    network_allow_hosts = _optional_string_list(network.get("allowHosts"), "network.allowHosts") if "allowHosts" in network else []
    network_deny_hosts = _optional_string_list(network.get("denyHosts"), "network.denyHosts") if "denyHosts" in network else []
    if network_enabled or network_allow_hosts or network_deny_hosts:
        raise ValueError("network policy is not enforceable yet; leave network disabled and host lists empty")
    read_roots = _optional_string_list(policy.get("readRoots"), "readRoots") if "readRoots" in policy else ["/workspace"]
    write_roots = _optional_string_list(policy.get("writeRoots"), "writeRoots") if "writeRoots" in policy else ["/workspace"]
    _validate_policy_roots(read_roots, "policy.readRoots")
    _validate_policy_roots(write_roots, "policy.writeRoots")
    process_policy: dict[str, Any] = {
        "allowExec": _optional_bool(process.get("allowExec"), "process.allowExec") if "allowExec" in process else False,
        "allowedCommands": _optional_string_list(process.get("allowedCommands"), "process.allowedCommands") if "allowedCommands" in process else [],
        "deniedCommands": _optional_string_list(process.get("deniedCommands"), "process.deniedCommands") if "deniedCommands" in process else [],
    }
    if "maxProcesses" in process:
        process_policy["maxProcesses"] = _json_process_count(process.get("maxProcesses"), "process.maxProcesses")
    return {
        "readRoots": read_roots,
        "writeRoots": write_roots,
        "process": process_policy,
        "network": {
            "enabled": network_enabled,
            "allowHosts": network_allow_hosts,
            "denyHosts": network_deny_hosts,
        },
        "env": {
            "allowlist": _optional_string_list(env.get("allowlist"), "env.allowlist") if "allowlist" in env else [],
            "denylist": _optional_string_list(env.get("denylist"), "env.denylist") if "denylist" in env else [],
            "injected": _optional_string_dict(env.get("injected"), "env.injected") if "injected" in env else {},
        },
        "maxDurationMs": _json_tool_timeout(policy.get("maxDurationMs"), "maxDurationMs") if "maxDurationMs" in policy else 300_000,
        "maxOutputBytes": _json_output_limit(policy.get("maxOutputBytes"), "maxOutputBytes") if "maxOutputBytes" in policy else 100_000,
    }


def _validate_policy_roots(roots: list[str], label: str) -> None:
    for root in roots:
        trimmed = root.rstrip("/")
        if (
            not trimmed
            or not (trimmed == "/workspace" or trimmed.startswith("/workspace/"))
            or "\0" in root
            or any(component in {".", ".."} for component in trimmed.split("/"))
        ):
            raise ValueError(f"{label} entries must be /workspace logical roots without . or .. components")


def _create_environment(config: _RuntimeConfig) -> EnvironmentInfo:
    workspace = (
        {
            "mode": "existing",
            "root": config.workspace["root"],
            "mountAsWorkspace": True,
        }
        if config.workspace.get("kind") == "existing"
        else {
            "mode": "new",
            "mountAsWorkspace": True,
        }
    )
    response = _post_json(
        f"{config.baseUrl}environments",
        {
            "workspace": workspace,
            "policy": config.policy,
            "metadata": {},
        },
    )
    environment = _parse_create_environment_response(response)
    _assert_environment_id(environment.id)
    return environment


def _create_session(
    config: _RuntimeConfig,
    environment_id: str,
    policy: PolicyConfig | None = None,
) -> SessionInfo:
    _assert_environment_id(environment_id)
    response = _post_json(
        f"{config.baseUrl}environments/{environment_id}/sessions",
        {
            "policy": None if policy is None else _materialize_policy(policy),
            "metadata": {},
        },
    )
    session = _parse_create_session_response(response)
    _assert_session_id(session.id)
    return session


def _parse_create_environment_response(value: Mapping[str, Any]) -> EnvironmentInfo:
    response = _json_mapping(value, "create environment response")
    _reject_unknown_fields(response, {"environment"}, "create environment response")
    return EnvironmentInfo.from_json(_json_mapping(_required_field(response, "environment", "environment"), "environment"))


def _parse_create_session_response(value: Mapping[str, Any]) -> SessionInfo:
    response = _json_mapping(value, "create session response")
    _reject_unknown_fields(response, {"session"}, "create session response")
    return SessionInfo.from_json(_json_mapping(_required_field(response, "session", "session"), "session"))


def _wait_for_result(
    queue_dir: str,
    invocation_id: str,
    session_id: str,
    timeout_ms: int,
    tool_name: str | None = None,
) -> SubmitResult:
    _assert_invocation_id(invocation_id)
    started = time.monotonic()
    completed_path = Path(queue_dir) / "completed" / f"{invocation_id}.json"
    failed_path = Path(queue_dir) / "failed" / f"{invocation_id}.json"
    pending_path = Path(queue_dir) / "pending" / f"{invocation_id}.json"
    claimed_path = Path(queue_dir) / "claimed" / f"{invocation_id}.json"
    timeout_s = timeout_ms / 1000

    while time.monotonic() - started < timeout_s:
        _ensure_file_queue(queue_dir)
        completed = _read_terminal_json(queue_dir, completed_path, pending_path)
        if completed is not None:
            _assert_completed_envelope_matches(completed, invocation_id, session_id)
            result = SubmitResult.from_json(completed["result"])
            if tool_name is not None and result.toolName != tool_name:
                _quarantine_terminal(queue_dir, completed_path)
                continue
            _assert_terminal_lease_material(completed, invocation_id, "result")
            if not _terminal_matches_claim(claimed_path, completed, invocation_id, session_id, tool_name):
                _quarantine_terminal(queue_dir, completed_path)
                continue
            return result
        failed = _read_terminal_json(queue_dir, failed_path, pending_path)
        if failed is not None:
            _assert_failed_envelope_matches(failed, invocation_id, session_id)
            _assert_terminal_lease_material(failed, invocation_id, "failure")
            if not _terminal_matches_claim(claimed_path, failed, invocation_id, session_id, None):
                _quarantine_terminal(queue_dir, failed_path)
                continue
            message = failed.get("error", {}).get("message", "unknown error")
            raise RuntimeError(f"Executioner invocation failed: {message}")
        time.sleep(0.01)

    raise TimeoutError(f"Timed out waiting for Executioner invocation {invocation_id}")


def _assert_completed_envelope_matches(
    completed: Mapping[str, Any],
    invocation_id: str,
    session_id: str,
) -> None:
    _reject_unknown_fields(
        completed,
        {"type", "eventType", "invocationId", "sessionId", "attemptId", "leaseToken", "result", "completedAt"},
        "completed terminal envelope",
    )
    _json_string(_required_field(completed, "completedAt", "completed terminal envelope completedAt"), "completed terminal envelope completedAt")
    result = completed.get("result", {})
    if not isinstance(result, Mapping):
        raise RuntimeError(f"Executioner terminal result malformed for invocation {invocation_id}")
    if _terminal_event_type(completed) != "tool.invocation.completed":
        raise RuntimeError(f"Executioner terminal result event type mismatch for invocation {invocation_id}")
    if completed.get("invocationId") != invocation_id or result.get("invocationId") != invocation_id:
        raise RuntimeError(f"Executioner terminal result invocation mismatch for invocation {invocation_id}")
    if completed.get("sessionId") != session_id or result.get("sessionId") != session_id:
        raise RuntimeError(f"Executioner terminal result session mismatch for invocation {invocation_id}")

def _assert_failed_envelope_matches(
    failed: Mapping[str, Any],
    invocation_id: str,
    session_id: str,
) -> None:
    _reject_unknown_fields(
        failed,
        {"type", "eventType", "invocationId", "sessionId", "attemptId", "leaseToken", "error", "failedAt"},
        "failed terminal envelope",
    )
    _json_string(_required_field(failed, "failedAt", "failed terminal envelope failedAt"), "failed terminal envelope failedAt")
    if _terminal_event_type(failed) != "tool.invocation.failed":
        raise RuntimeError(f"Executioner terminal failure event type mismatch for invocation {invocation_id}")
    if failed.get("invocationId") != invocation_id:
        raise RuntimeError(f"Executioner terminal failure invocation mismatch for invocation {invocation_id}")
    if failed.get("sessionId") != session_id:
        raise RuntimeError(f"Executioner terminal failure session mismatch for invocation {invocation_id}")
    error = failed.get("error")
    if (
        not isinstance(error, Mapping)
        or not isinstance(error.get("code"), str)
        or not error["code"].strip()
        or not isinstance(error.get("message"), str)
        or not error["message"].strip()
        or not isinstance(error.get("retryable"), bool)
    ):
        raise RuntimeError(f"Executioner terminal failure malformed for invocation {invocation_id}")
    _reject_unknown_fields(error, {"code", "message", "retryable"}, "failed terminal error")


def _assert_terminal_lease_material(
    envelope: Mapping[str, Any],
    invocation_id: str,
    terminal_kind: str,
) -> None:
    if (
        not isinstance(envelope.get("attemptId"), str)
        or not envelope["attemptId"]
        or not isinstance(envelope.get("leaseToken"), str)
        or not envelope["leaseToken"]
    ):
        raise RuntimeError(
            f"Executioner terminal {terminal_kind} missing lease material for invocation {invocation_id}"
        )


def _terminal_matches_claim(
    claimed_path: Path,
    envelope: Mapping[str, Any],
    invocation_id: str,
    session_id: str,
    tool_name: str | None,
) -> bool:
    try:
        claim = _read_capped_json(claimed_path, _MAX_QUEUE_JSON_BYTES, "claimed lease")
    except FileNotFoundError as exc:
        raise RuntimeError(
            f"Executioner terminal claim missing or malformed for invocation {invocation_id}: {exc}"
        ) from exc
    except Exception as exc:
        return False
    try:
        claim = _json_mapping(claim, "claimed lease")
        _reject_unknown_fields(claim, {"workerId", "attemptId", "leaseToken", "claimedAt", "request"}, "claimed lease")
        _json_string(_required_field(claim, "workerId", "claimed lease workerId"), "claimed lease workerId")
        _json_string(_required_field(claim, "claimedAt", "claimed lease claimedAt"), "claimed lease claimedAt")
        request = _json_mapping(_required_field(claim, "request", "claimed lease request"), "claimed lease request")
    except (TypeError, ValueError):
        return False
    if claim.get("attemptId") != envelope.get("attemptId") or claim.get("leaseToken") != envelope.get("leaseToken"):
        return False
    return _claimed_request_matches(request, invocation_id, session_id, tool_name)


def _claimed_request_matches(
    request: Mapping[str, Any],
    invocation_id: str,
    session_id: str,
    tool_name: str | None,
) -> bool:
    try:
        _reject_unknown_fields(
            request,
            {
                "invocationId",
                "sessionId",
                "toolName",
                "arguments",
                "cwd",
                "timeoutMs",
                "maxOutputBytes",
                "idempotencyKey",
                "requiredCapabilities",
                "metadata",
            },
            "claimed lease request",
        )
        claimed_invocation_id = _json_string(
            _required_field(request, "invocationId", "claimed lease request invocationId"),
            "claimed lease request invocationId",
        )
        claimed_session_id = _json_string(
            _required_field(request, "sessionId", "claimed lease request sessionId"),
            "claimed lease request sessionId",
        )
        claimed_tool_name = _json_string(
            _required_field(request, "toolName", "claimed lease request toolName"),
            "claimed lease request toolName",
        )
        _json_mapping(
            _required_field(request, "arguments", "claimed lease request arguments"),
            "claimed lease request arguments",
        )
        if request.get("cwd") is not None:
            _json_string(request.get("cwd"), "claimed lease request cwd")
        if request.get("timeoutMs") is not None:
            _json_tool_timeout(request.get("timeoutMs"), "claimed lease request timeoutMs")
        if request.get("maxOutputBytes") is not None:
            _json_output_limit(request.get("maxOutputBytes"), "claimed lease request maxOutputBytes")
        if request.get("idempotencyKey") is not None:
            _json_string(request.get("idempotencyKey"), "claimed lease request idempotencyKey")
        if request.get("requiredCapabilities") is not None:
            for capability in _json_list(
                request.get("requiredCapabilities"),
                "claimed lease request requiredCapabilities",
            ):
                capability = _json_mapping(capability, "claimed lease request capability")
                _reject_unknown_fields(
                    capability,
                    {"kind", "scope"},
                    "claimed lease request capability",
                )
                _json_string(
                    _required_field(capability, "kind", "claimed lease request capability kind"),
                    "claimed lease request capability kind",
                )
                _json_mapping(capability.get("scope", {}), "claimed lease request capability scope")
        _json_mapping(request.get("metadata", {}), "claimed lease request metadata")
    except (TypeError, ValueError):
        return False
    return (
        claimed_invocation_id == invocation_id
        and claimed_session_id == session_id
        and (tool_name is None or claimed_tool_name == tool_name)
    )


def _terminal_event_type(envelope: Mapping[str, Any]) -> object:
    event_type = envelope.get("eventType")
    legacy_type = envelope.get("type")
    if event_type is not None and not isinstance(event_type, str):
        return None
    if legacy_type is not None and not isinstance(legacy_type, str):
        return None
    if event_type is not None and legacy_type is not None and event_type != legacy_type:
        return None
    return event_type if event_type is not None else legacy_type


def _read_terminal_json(queue_dir: str, path: Path, pending_path: Path) -> dict[str, Any] | None:
    try:
        metadata = path.lstat()
    except FileNotFoundError:
        return None
    except OSError:
        _quarantine_terminal(queue_dir, path)
        return None
    if not path.is_file() or os.path.islink(path):
        _quarantine_terminal(queue_dir, path)
        return None
    if metadata.st_size > _MAX_QUEUE_JSON_BYTES:
        _quarantine_terminal(queue_dir, path)
        return None
    if _path_occupied(pending_path):
        _quarantine_terminal(queue_dir, path)
        return None
    try:
        return _read_terminal_json_no_follow(path)
    except (OSError, json.JSONDecodeError, TypeError, ValueError):
        _quarantine_terminal(queue_dir, path)
        return None


def _read_terminal_json_no_follow(path: Path) -> dict[str, Any]:
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    fd = os.open(path, flags)
    try:
        metadata = os.fstat(fd)
        if not stat.S_ISREG(metadata.st_mode) or metadata.st_size > _MAX_QUEUE_JSON_BYTES:
            raise OSError("terminal file must be a regular bounded file")
        with os.fdopen(fd, "rb") as file:
            fd = -1
            body = file.read(_MAX_QUEUE_JSON_BYTES + 1)
        if len(body) > _MAX_QUEUE_JSON_BYTES:
            raise OSError("terminal file exceeds maximum size")
        return dict(_json_mapping(json.loads(body.decode("utf-8")), "terminal file"))
    finally:
        if fd >= 0:
            os.close(fd)


def _path_occupied(path: Path) -> bool:
    try:
        path.lstat()
        return True
    except FileNotFoundError:
        return False
    except OSError:
        return True


def _quarantine_terminal(queue_dir: str, path: Path) -> None:
    _ensure_file_queue(queue_dir)
    rejected_dir = Path(queue_dir) / "rejected"
    rejected_dir.mkdir(parents=True, exist_ok=True)
    stem = path.stem or "terminal"
    rejected_path = rejected_dir / f"{stem}.terminal.rejected.{uuid.uuid4().hex}.json"
    try:
        path.replace(rejected_path)
    except OSError:
        try:
            path.unlink()
        except OSError:
            pass


_INVOCATION_ID_RE = re.compile(r"^[A-Za-z0-9_-]{1,128}$")
_SESSION_ID_RE = re.compile(r"^[A-Za-z0-9_-]{1,128}$")
_IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9_-]{1,128}$")
_MAX_HTTP_ERROR_BODY_BYTES = 64 * 1024
_MAX_HTTP_JSON_BODY_BYTES = 10 * 1024 * 1024
_MAX_QUEUE_JSON_BYTES = 10 * 1024 * 1024
_MAX_REQUEST_JSON_BYTES = 1024 * 1024
_MAX_WORKSPACE_ARTIFACT_ENTRIES = 10_000
_MAX_WORKSPACE_ARTIFACT_DEPTH = 256
_MAX_WORKSPACE_ARTIFACT_MANIFEST_BYTES = 10 * 1024 * 1024
_MAX_WORKSPACE_ARTIFACT_BYTES = 100 * 1024 * 1024
_MAX_OUTPUT_BYTES = 10 * 1024 * 1024
_MAX_TOOL_TIMEOUT_MS = 60 * 60 * 1000
_MAX_PROCESS_COUNT = 2 ** 32 - 1


def _assert_invocation_id(invocation_id: str) -> None:
    if not isinstance(invocation_id, str) or not _INVOCATION_ID_RE.match(invocation_id):
        raise ValueError("invalid invocationId: only ASCII letters, numbers, '-' and '_' are allowed")


def _assert_session_id(session_id: str) -> None:
    if not isinstance(session_id, str) or not _SESSION_ID_RE.match(session_id):
        raise ValueError("invalid session id: only ASCII letters, numbers, '-' and '_' are allowed")


def _assert_environment_id(environment_id: str) -> None:
    if not isinstance(environment_id, str) or not _SESSION_ID_RE.match(environment_id):
        raise ValueError("invalid environment id: only ASCII letters, numbers, '-' and '_' are allowed")


def _ensure_invocation_id_unused(queue_dir: str, invocation_id: str) -> None:
    _assert_invocation_id(invocation_id)
    for child in ["pending", "claimed", "completed", "failed"]:
        try:
            Path(queue_dir, child, f"{invocation_id}.json").lstat()
            exists = True
        except FileNotFoundError:
            exists = False
        if exists:
            raise RuntimeError(f"duplicate invocationId: {invocation_id}")


def _cleanup_queue_dir(queue_dir: str, sdk_created_queue_dir: bool) -> None:
    if sdk_created_queue_dir:
        _remove_path_without_following(Path(queue_dir))
        return

    queue_path = Path(queue_dir)
    try:
        metadata = queue_path.lstat()
    except FileNotFoundError:
        return
    if stat.S_ISLNK(metadata.st_mode):
        _remove_path_without_following(queue_path)
        return

    for child in ["pending", "claimed", "completed", "failed", "rejected"]:
        _remove_path_without_following(Path(queue_path, child))


def _cleanup_state_dir(state_dir: str, sdk_created_state_dir: bool) -> None:
    if sdk_created_state_dir:
        _remove_path_without_following(Path(state_dir))


def _remove_path_without_following(path: Path) -> None:
    try:
        metadata = path.lstat()
    except FileNotFoundError:
        return
    if stat.S_ISLNK(metadata.st_mode) or stat.S_ISREG(metadata.st_mode):
        path.unlink(missing_ok=True)
    elif stat.S_ISDIR(metadata.st_mode):
        shutil.rmtree(path)
    else:
        path.unlink(missing_ok=True)


def _ensure_file_queue(queue_dir: str) -> None:
    _ensure_queue_root_dir(Path(queue_dir))
    for child in ["pending", "claimed", "completed", "failed", "rejected"]:
        _ensure_queue_state_dir(Path(queue_dir, child))


def _ensure_queue_root_dir(path: Path) -> None:
    _validate_no_symlinked_parent(path.parent, "queue directory parent")
    path.mkdir(parents=True, exist_ok=True)
    metadata = path.lstat()
    if os.path.islink(path) or not stat.S_ISDIR(metadata.st_mode):
        raise RuntimeError(f"queue directory must be a real directory: {path}")


def _ensure_queue_state_dir(path: Path) -> None:
    _validate_no_symlinked_parent(path.parent, "queue state directory parent")
    path.mkdir(parents=True, exist_ok=True)
    metadata = path.lstat()
    if os.path.islink(path) or not stat.S_ISDIR(metadata.st_mode):
        raise RuntimeError(f"queue state directory must be a real directory: {path}")


def _resolve_binary_path(binary_path: str | None) -> str:
    if binary_path:
        return binary_path
    env_binary = os.environ.get("EXECUTIONER_BIN")
    if env_binary:
        return env_binary

    bundled_binary = _bundled_runtime_binary_path()
    if bundled_binary:
        return bundled_binary

    sidecar_binary = _sidecar_runtime_binary_path()
    if sidecar_binary:
        return sidecar_binary

    return _runtime_binary_name()


def _runtime_binary_name() -> str:
    return "executioner.exe" if os.name == "nt" else "executioner"


def _bundled_runtime_binary_path() -> str | None:
    candidate = Path(__file__).resolve().parent / "bin" / _runtime_binary_name()
    return str(candidate) if candidate.is_file() else None


def _sidecar_runtime_binary_path() -> str | None:
    for package_name in _RUNTIME_PACKAGE_NAMES:
        try:
            root = importlib_resources.files(package_name)
        except ModuleNotFoundError:
            continue

        candidate = root / "bin" / _runtime_binary_name()
        if candidate.is_file():
            return str(candidate)
    return None


def _spawn_process(binary_path: str, args: list[str], name: str) -> _ManagedProcess:
    try:
        process = subprocess.Popen(
            [binary_path, *args],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
    except OSError as exc:
        raise RuntimeError(
            f'Unable to start the Executioner runtime binary at "{binary_path}". '
            "Install a package that includes the runtime binary, install the "
            "`executioner` CLI on PATH, or pass binaryPath/EXECUTIONER_BIN."
        ) from exc
    return _ManagedProcess(process=process, name=name)


def _close_process_pipes(process: subprocess.Popen[bytes]) -> None:
    for pipe in [process.stdout, process.stderr]:
        if pipe is not None and not pipe.closed:
            pipe.close()


def _terminate_process(managed: _ManagedProcess) -> None:
    if managed.process.poll() is not None:
        _close_process_pipes(managed.process)
        return
    managed.process.terminate()
    try:
        managed.process.wait(timeout=2)
    except subprocess.TimeoutExpired:
        managed.process.kill()
        managed.process.wait(timeout=2)
    _close_process_pipes(managed.process)


def _wait_for_health(base_url: str, timeout_ms: int) -> None:
    started = time.monotonic()
    timeout_s = timeout_ms / 1000
    while time.monotonic() - started < timeout_s:
        try:
            with _urlopen_no_redirect(f"{base_url}health", timeout=1) as response:
                if 200 <= response.status < 300:
                    return
        except (OSError, urlerror.URLError):
            pass
        time.sleep(0.025)
    raise TimeoutError(f"Timed out waiting for Executioner host at {base_url}")


def _post_json(url: str, body: Any) -> dict[str, Any]:
    data = json.dumps(body).encode("utf-8") if body is not None else b"null"
    request = urlrequest.Request(
        url,
        data=data,
        method="POST",
        headers={"content-type": "application/json"},
    )
    return _request_json(request)


def _get_json(url: str) -> dict[str, Any]:
    return _request_json(urlrequest.Request(url, method="GET"))


def _delete_json(url: str) -> dict[str, Any]:
    return _request_json(urlrequest.Request(url, method="DELETE"))


def _request_json(request: urlrequest.Request) -> dict[str, Any]:
    try:
        with _urlopen_no_redirect(request) as response:
            return dict(_json_mapping(
                json.loads(_read_capped_http_json_body(response).decode("utf-8")),
                "host response",
            ))
    except urlerror.HTTPError as error:
        body = _read_capped_http_error_body(error)
        raise RuntimeError(f"Executioner host returned {error.code}: {body}") from error


def _urlopen_no_redirect(
    request: str | urlrequest.Request,
    timeout: float | None = None,
) -> Any:
    kwargs = {} if timeout is None else {"timeout": timeout}
    return _NO_REDIRECT_OPENER.open(request, **kwargs)


def _read_capped_http_json_body(response: Any) -> bytes:
    body = response.read(_MAX_HTTP_JSON_BODY_BYTES + 1)
    if len(body) > _MAX_HTTP_JSON_BODY_BYTES:
        raise ValueError(
            f"response body exceeds maximum size of {_MAX_HTTP_JSON_BODY_BYTES} bytes"
        )
    return body


def _read_capped_http_error_body(error: urlerror.HTTPError) -> str:
    body = error.read(_MAX_HTTP_ERROR_BODY_BYTES + 1)
    truncated = len(body) > _MAX_HTTP_ERROR_BODY_BYTES
    body = body[:_MAX_HTTP_ERROR_BODY_BYTES]
    text = body.decode("utf-8", errors="replace")
    if truncated:
        text += "\n...[truncated]"
    return text


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _read_capped_json(path: Path, max_bytes: int, label: str) -> dict[str, Any]:
    with _open_regular_file_no_follow(path, label) as file:
        body = file.read(max_bytes + 1)
    if len(body) > max_bytes:
        raise ValueError(f"{label} exceeds maximum size of {max_bytes} bytes")
    return json.loads(body.decode("utf-8"))


def _write_json_atomic(path: Path, value: Mapping[str, Any]) -> None:
    tmp_path = path.with_name(f"{path.name}.tmp.{uuid.uuid4().hex}")
    try:
        payload = json.dumps(value, indent=2)
        if len(payload.encode("utf-8")) > _MAX_QUEUE_JSON_BYTES:
            raise ValueError(f"queue json exceeds maximum size of {_MAX_QUEUE_JSON_BYTES} bytes")
        tmp_path.write_text(payload, encoding="utf-8")
        os.link(tmp_path, path)
    finally:
        try:
            tmp_path.unlink()
        except FileNotFoundError:
            pass


def _assert_serialized_json_size(label: str, value: Mapping[str, Any], max_bytes: int) -> None:
    if len(json.dumps(value).encode("utf-8")) > max_bytes:
        raise ValueError(f"{label} exceeds maximum JSON size of {max_bytes} bytes")


def _normalize_base_url(url: str) -> str:
    if url.startswith(("http:///", "https:///")):
        raise ValueError("invalid host.baseUrl: host is required")
    parsed = urlparse.urlsplit(url)
    if parsed.scheme not in {"http", "https"}:
        raise ValueError(f"invalid host.baseUrl: unsupported scheme {parsed.scheme}")
    if not parsed.netloc or not parsed.hostname:
        raise ValueError("invalid host.baseUrl: host is required")
    if parsed.username or parsed.password:
        raise ValueError("invalid host.baseUrl: credentials are not allowed")
    if parsed.query or parsed.fragment:
        raise ValueError("invalid host.baseUrl: query strings and fragments are not allowed")
    path = parsed.path if parsed.path.endswith("/") else f"{parsed.path}/"
    return urlparse.urlunsplit((parsed.scheme, parsed.netloc, path, "", ""))


def _parse_list_files_result(result: SubmitResult) -> list[str]:
    if result.status != "success":
        message = result.error or result.output
        raise RuntimeError(f"List failed with status {result.status}: {message}")
    truncated = result.metadata.get("truncated")
    if truncated is not None and not isinstance(truncated, bool):
        raise TypeError("List truncated metadata must be a boolean")
    if truncated is True:
        raise RuntimeError("List result was truncated; refusing partial directory listing")
    entries = result.metadata.get("entries")
    if entries is not None and not isinstance(entries, list):
        raise TypeError("List metadata entries must be an array")
    if entries is not None and not all(isinstance(entry, str) for entry in entries):
        raise ValueError("List metadata entries must be strings")
    if entries is not None:
        return entries
    return _parse_list_files_output(result.output)


def _parse_list_files_output(output: str) -> list[str]:
    if any(line.startswith("...[truncated") for line in output.splitlines()):
        raise RuntimeError("List result was truncated; refusing partial directory listing")
    return [
        line
        for line in output.splitlines()
        if line and not line.startswith("...[truncated")
    ]


def materialize_workspace_artifact(
    artifact: WorkspaceArtifact,
    destination: str | os.PathLike[str],
) -> None:
    artifact = _normalize_workspace_artifact(artifact)
    destination_path = Path(destination)
    _validate_materialize_destination(destination_path)
    parent = destination_path.parent
    if str(parent) == "":
        raise ValueError("materialize destination must have a parent")
    _validate_no_symlinked_parent(parent, "materialize destination parent")
    cleanup_parent = parent if parent.is_absolute() else Path.cwd() / parent
    cleanup_stop = _nearest_existing_ancestor(cleanup_parent)
    staging: Path | None = None

    try:
        parent.mkdir(parents=True, exist_ok=True)
        staging = parent / f".substrate-materialize-{uuid.uuid4().hex}"
        staging.mkdir()
        _materialize_workspace_artifact_into(artifact, staging)
        if destination_path.exists():
            destination_path.rmdir()
        staging.replace(destination_path)
    except Exception:
        if staging is not None:
            shutil.rmtree(staging, ignore_errors=True)
        _cleanup_created_empty_parents(cleanup_parent, cleanup_stop)
        raise


def _normalize_workspace_artifact(artifact: WorkspaceArtifact) -> WorkspaceArtifact:
    if not isinstance(artifact, WorkspaceArtifact):
        raise TypeError("workspace artifact must be a WorkspaceArtifact")
    return WorkspaceArtifact.from_json(asdict(artifact))


def _materialize_workspace_artifact_into(artifact: WorkspaceArtifact, destination: Path) -> None:
    destination = destination.resolve(strict=True)
    _validate_artifact_header(artifact)
    tar_path = _path_from_file_uri(artifact.artifact.uri)
    tar_bytes = _read_workspace_artifact_bytes(tar_path)
    actual_hash = _sha256_bytes(tar_bytes)
    if actual_hash != artifact.hash or len(tar_bytes) != artifact.bytes:
        raise ValueError("workspace artifact hash or byte length mismatch")
    _validate_tar_end_of_archive(tar_bytes)
    _validate_manifest_resource_if_available(artifact)

    entries = _validate_manifest_entries(artifact, destination)
    seen_archive_entries: set[str] = set()

    try:
        archive = tarfile.open(fileobj=io.BytesIO(tar_bytes), mode="r:")
    except tarfile.TarError as exc:
        raise ValueError("workspace artifact must be an uncompressed tar archive") from exc

    with archive:
        for member in archive:
            if _contains_surrogate(member.name):
                raise ValueError("unsafe artifact path: path is not valid UTF-8")
            archive_path = _safe_archive_path(member.name)
            manifest_entry = entries.get(archive_path)
            if manifest_entry is None:
                raise ValueError(f"artifact contains entry missing from manifest: {archive_path}")
            if archive_path in seen_archive_entries:
                raise ValueError(f"duplicate artifact entry: {archive_path}")
            seen_archive_entries.add(archive_path)

            target_path = destination / archive_path
            if manifest_entry.kind == "directory" and member.isdir():
                target_path.mkdir(parents=True, exist_ok=True)
            elif manifest_entry.kind == "file" and member.isfile():
                target_path.parent.mkdir(parents=True, exist_ok=True)
                extracted = archive.extractfile(member)
                if extracted is None:
                    raise ValueError(f"artifact file entry cannot be read: {archive_path}")
                file_hash = hashlib.sha256()
                bytes_written = 0
                with extracted, target_path.open("xb") as output:
                    while True:
                        chunk = extracted.read(1024 * 1024)
                        if not chunk:
                            break
                        output.write(chunk)
                        file_hash.update(chunk)
                        bytes_written += len(chunk)
                if (
                    manifest_entry.bytes != bytes_written
                    or manifest_entry.hash != f"sha256:{file_hash.hexdigest()}"
                ):
                    raise ValueError(
                        f"artifact entry hash or byte length mismatch: {archive_path}"
                    )
            else:
                raise ValueError(f"artifact entry type does not match manifest: {archive_path}")

    for entry in artifact.entries:
        if entry.kind == "file" and entry.archivePath not in seen_archive_entries:
            raise ValueError(f"manifest file missing from artifact: {entry.archivePath}")
        if entry.kind == "directory" and entry.archivePath not in seen_archive_entries:
            raise ValueError(f"manifest directory missing from artifact: {entry.archivePath}")

    _materialize_manifest_symlinks(artifact, destination)


def _path_from_file_uri(uri: str) -> Path:
    if not uri.startswith("file://"):
        raise ValueError("artifact uri must be file://")
    path_text = uri[len("file://"):]
    if not path_text.startswith("/"):
        raise ValueError("artifact file uri must be absolute")
    if path_text.startswith("//") or "?" in path_text or "#" in path_text:
        raise ValueError(
            "artifact file uri must be a local file:/// absolute path without authority, query, or fragment"
        )
    path = Path(path_text)
    if not path.is_absolute():
        raise ValueError("artifact file uri must be absolute")
    return path


def _hash_file(path: Path) -> tuple[str, int]:
    digest = hashlib.sha256()
    byte_count = 0
    with _open_regular_file_no_follow(path, "workspace artifact resource") as file:
        if os.fstat(file.fileno()).st_size > _MAX_WORKSPACE_ARTIFACT_BYTES:
            raise ValueError(
                f"workspace artifact exceeds maximum size of {_MAX_WORKSPACE_ARTIFACT_BYTES} bytes"
            )
        while True:
            chunk = file.read(1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
            byte_count += len(chunk)
    return f"sha256:{digest.hexdigest()}", byte_count


def _sha256_bytes(data: bytes) -> str:
    return f"sha256:{hashlib.sha256(data).hexdigest()}"


def _read_workspace_artifact_bytes(path: Path) -> bytes:
    with _open_regular_file_no_follow(path, "workspace artifact resource") as file:
        if os.fstat(file.fileno()).st_size > _MAX_WORKSPACE_ARTIFACT_BYTES:
            raise ValueError(
                f"workspace artifact exceeds maximum size of {_MAX_WORKSPACE_ARTIFACT_BYTES} bytes"
            )
        data = file.read(_MAX_WORKSPACE_ARTIFACT_BYTES + 1)
    if len(data) > _MAX_WORKSPACE_ARTIFACT_BYTES:
        raise ValueError(
            f"workspace artifact exceeds maximum size of {_MAX_WORKSPACE_ARTIFACT_BYTES} bytes"
        )
    return data


def _validate_tar_end_of_archive(data: bytes) -> None:
    block_bytes = 512
    if len(data) % block_bytes != 0:
        raise ValueError("artifact tar is missing end-of-archive marker")
    first_zero_block = None
    for index in range(0, len(data), block_bytes):
        block = data[index:index + block_bytes]
        if all(byte == 0 for byte in block):
            first_zero_block = index
            break
    if first_zero_block is None or first_zero_block + block_bytes >= len(data):
        raise ValueError("artifact tar is missing end-of-archive marker")
    if any(byte != 0 for byte in data[first_zero_block:]):
        raise ValueError("artifact tar contains trailing data after end of archive")


def _open_regular_file_no_follow(path: Path, label: str) -> Any:
    try:
        metadata = path.lstat()
    except FileNotFoundError:
        raise
    if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISREG(metadata.st_mode):
        raise ValueError(f"{label} must be a regular file")
    flags = os.O_RDONLY
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    fd = os.open(path, flags)
    try:
        opened = os.fdopen(fd, "rb")
    except Exception:
        os.close(fd)
        raise
    opened_metadata = os.fstat(opened.fileno())
    if not stat.S_ISREG(opened_metadata.st_mode):
        opened.close()
        raise ValueError(f"{label} must be a regular file")
    return opened


def _validate_materialize_destination(destination: Path) -> None:
    try:
        metadata = destination.lstat()
    except FileNotFoundError:
        return
    if os.path.islink(destination):
        raise ValueError("materialize destination must not be a symlink")
    if not destination.is_dir():
        raise ValueError("materialize destination must be a directory")
    if any(destination.iterdir()):
        raise ValueError("materialize destination must be empty")


def _validate_no_symlinked_parent(parent: Path, label: str) -> None:
    current = parent if parent.is_absolute() else Path.cwd() / parent
    while True:
        try:
            metadata = current.lstat()
        except FileNotFoundError:
            pass
        else:
            if stat.S_ISLNK(metadata.st_mode) and not _is_platform_root_symlink(current):
                raise ValueError(f"{label} must not contain symlinks")
        if current.parent == current:
            return
        current = current.parent


def _is_platform_root_symlink(path: Path) -> bool:
    return str(path) in {"/var", "/tmp", "/etc"}


def _nearest_existing_ancestor(path: Path) -> Path | None:
    current = path
    while True:
        if current.exists():
            return current
        if current.parent == current:
            return None
        current = current.parent


def _cleanup_created_empty_parents(parent: Path, stop: Path | None) -> None:
    current = parent
    while True:
        if stop is not None and current == stop:
            return
        try:
            current.rmdir()
        except OSError:
            return
        if current.parent == current:
            return
        current = current.parent


def _validate_manifest_entries(
    artifact: WorkspaceArtifact,
    destination: Path,
) -> dict[str, WorkspaceArtifactEntry]:
    _validate_artifact_header(artifact)
    _validate_manifest_counts(artifact)
    entries: dict[str, WorkspaceArtifactEntry] = {}
    total_file_bytes = 0

    for entry in artifact.entries:
        archive_path = _safe_archive_path(entry.archivePath)
        if archive_path != entry.archivePath:
            raise ValueError(f"manifest entry path is not canonical: {entry.archivePath}")
        if not entry.logicalPath.startswith("/workspace/"):
            raise ValueError(f"manifest logical path must be under /workspace: {entry.logicalPath}")
        if entry.logicalPath != f"/workspace/{archive_path}":
            raise ValueError(f"manifest logical path does not match archive path: {entry.archivePath}")

        if entry.kind == "file":
            if entry.bytes is None or entry.hash is None or entry.linkTarget is not None:
                raise ValueError(f"manifest file entry is incomplete: {entry.archivePath}")
            total_file_bytes += entry.bytes
            if (
                entry.bytes > _MAX_WORKSPACE_ARTIFACT_BYTES
                or total_file_bytes > _MAX_WORKSPACE_ARTIFACT_BYTES
            ):
                raise ValueError(
                    f"workspace artifact manifest file bytes exceed maximum size of {_MAX_WORKSPACE_ARTIFACT_BYTES} bytes"
                )
        elif entry.kind == "directory":
            if entry.bytes is not None or entry.hash is not None or entry.linkTarget is not None:
                raise ValueError(f"manifest directory entry has file metadata: {entry.archivePath}")
        elif entry.kind == "symlink":
            if entry.linkTarget is None:
                raise ValueError(f"manifest symlink entry is incomplete: {entry.archivePath}")
            if entry.bytes is not None or entry.hash is not None:
                raise ValueError(f"manifest symlink entry has file metadata: {entry.archivePath}")
            _validate_materialized_symlink_target(destination, archive_path, entry.linkTarget)
        else:
            raise ValueError(f"unknown manifest entry kind: {entry.kind}")

        if archive_path in entries:
            raise ValueError(f"duplicate manifest entry: {entry.archivePath}")
        entries[archive_path] = entry

    _validate_manifest_parent_directories(entries)
    return entries


def _validate_manifest_parent_directories(
    entries: dict[str, WorkspaceArtifactEntry],
) -> None:
    for archive_path in entries:
        parent = PurePosixPath(archive_path).parent
        while str(parent) not in ("", "."):
            parent_archive_path = parent.as_posix()
            parent_entry = entries.get(parent_archive_path)
            if parent_entry is None:
                raise ValueError(
                    f"manifest parent directory missing for {archive_path}: {parent_archive_path}"
                )
            if parent_entry.kind != "directory":
                raise ValueError(
                    f"manifest parent path is not a directory for {archive_path}: {parent_archive_path}"
                )
            parent = parent.parent


def _validate_artifact_header(artifact: WorkspaceArtifact) -> None:
    if artifact.format != "tar":
        raise ValueError(f"unsupported workspace artifact format: {artifact.format}")
    if artifact.artifact.resourceType != "artifact":
        raise ValueError("workspace artifact resource type must be artifact")
    if artifact.manifest.resourceType != "artifact_manifest":
        raise ValueError("workspace artifact manifest resource type must be artifact_manifest")
    if artifact.bytes > _MAX_WORKSPACE_ARTIFACT_BYTES:
        raise ValueError(
            f"workspace artifact exceeds maximum size of {_MAX_WORKSPACE_ARTIFACT_BYTES} bytes"
        )


def _validate_manifest_resource_if_available(artifact: WorkspaceArtifact) -> None:
    if not artifact.manifest.uri.startswith("file://"):
        raise ValueError("workspace artifact manifest uri must be file://")
    manifest_path = _path_from_file_uri(artifact.manifest.uri)
    try:
        manifest_path.lstat()
    except FileNotFoundError:
        return
    manifest_artifact = WorkspaceArtifact.from_json(_read_capped_json(
        manifest_path,
        _MAX_WORKSPACE_ARTIFACT_MANIFEST_BYTES,
        "workspace artifact manifest resource",
    ))
    if manifest_artifact != artifact:
        raise ValueError("workspace artifact manifest resource does not match artifact metadata")


def _validate_manifest_counts(artifact: WorkspaceArtifact) -> None:
    if len(artifact.entries) > _MAX_WORKSPACE_ARTIFACT_ENTRIES:
        raise ValueError(
            f"workspace artifact exceeds maximum entry count of {_MAX_WORKSPACE_ARTIFACT_ENTRIES}"
        )
    file_count = sum(1 for entry in artifact.entries if entry.kind == "file")
    directory_count = sum(1 for entry in artifact.entries if entry.kind == "directory")
    symlink_count = sum(1 for entry in artifact.entries if entry.kind == "symlink")
    if (
        file_count != artifact.fileCount
        or directory_count != artifact.directoryCount
        or symlink_count != artifact.symlinkCount
    ):
        raise ValueError("manifest counts do not match entries")


def _safe_archive_path(path: str) -> str:
    if _contains_surrogate(path):
        raise ValueError("unsafe artifact path: path is not valid UTF-8")
    if "\\" in path or "\0" in path:
        raise ValueError(f"unsafe artifact path: {path}")
    archive_path = PurePosixPath(path)
    if archive_path.is_absolute():
        raise ValueError(f"unsafe artifact path: {path}")
    parts: list[str] = []
    for part in archive_path.parts:
        if part in ("", "."):
            continue
        if part == "..":
            raise ValueError(f"unsafe artifact path: {path}")
        parts.append(part)
    if not parts:
        raise ValueError("artifact path must not be empty")
    if len(parts) > _MAX_WORKSPACE_ARTIFACT_DEPTH:
        raise ValueError(
            f"artifact path exceeds maximum path depth of {_MAX_WORKSPACE_ARTIFACT_DEPTH}: {path}"
        )
    return "/".join(parts)


def _materialize_manifest_symlinks(artifact: WorkspaceArtifact, destination: Path) -> None:
    for entry in artifact.entries:
        if entry.kind != "symlink":
            continue
        assert entry.linkTarget is not None
        archive_path = _safe_archive_path(entry.archivePath)
        _validate_materialized_symlink_target(destination, archive_path, entry.linkTarget)
        link_path = destination / archive_path
        link_path.parent.mkdir(parents=True, exist_ok=True)
        os.symlink(entry.linkTarget, link_path)


def _validate_materialized_symlink_target(
    destination: Path,
    archive_path: str,
    target: str,
) -> None:
    if _contains_surrogate(target):
        raise ValueError(f"unsafe symlink target in manifest: {archive_path}")
    target_path = PurePosixPath(target)
    if target_path.is_absolute() or "\\" in target or "\0" in target:
        raise ValueError(f"unsafe symlink target in manifest: {archive_path}")
    link_parent = PurePosixPath(archive_path).parent
    normalized_target = _normalize_posix_parts((*link_parent.parts, *target_path.parts))
    if normalized_target is None:
        raise ValueError(f"unsafe symlink target in manifest: {archive_path}")
    target_on_disk = destination.joinpath(*normalized_target)
    try:
        target_on_disk.relative_to(destination)
    except ValueError:
        raise ValueError(f"unsafe symlink target in manifest: {archive_path}") from None


def _normalize_posix_parts(parts: tuple[str, ...]) -> tuple[str, ...] | None:
    normalized: list[str] = []
    for part in parts:
        if part in ("", "."):
            continue
        if part == "..":
            if not normalized:
                return None
            normalized.pop()
        else:
            normalized.append(part)
    return tuple(normalized)


def _contains_surrogate(value: str) -> bool:
    return any("\ud800" <= char <= "\udfff" for char in value)


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])
