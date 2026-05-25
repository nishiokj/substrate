from __future__ import annotations

import hashlib
import http.server
import importlib
import io
import json
import os
import subprocess
import sys
import tarfile
import tempfile
import threading
import unittest
from pathlib import Path
from urllib import error as urlerror
from urllib import request as urlrequest
from unittest.mock import patch

from executioner_sdk import ExecutionerEnvironment, ExecutionerSession, tool, tool_schemas
from executioner_sdk.environment import (
    EnvironmentInfo,
    ResourceRef,
    SessionInfo,
    SubmitResult,
    WorkspaceArtifact,
    WorkspaceArtifactEntry,
    WorkspaceInfo,
    _RuntimeConfig,
    _create_environment,
    _ensure_invocation_id_unused,
    _assert_session_id,
    _ensure_file_queue,
    _parse_list_files_result,
    _parse_create_environment_response,
    _parse_create_session_response,
    _request_json,
    _wait_for_result,
    _materialize_config,
    _normalize_base_url,
    _path_from_file_uri,
    _cleanup_queue_dir,
    _resolve_binary_path,
    _runtime_binary_name,
    _spawn_process,
    _terminate_process,
    _write_json_atomic,
    materialize_workspace_artifact,
)


def executioner_binary() -> str:
    debug_binary = Path(__file__).resolve().parents[3] / "target" / "debug" / "executioner"
    if "EXECUTIONER_BIN" not in os.environ and debug_binary.exists():
        return str(debug_binary)
    return os.environ.get(
        "EXECUTIONER_BIN",
        str(Path(__file__).resolve().parents[3] / "target" / "release" / "executioner"),
    )


class ExecutionerEnvironmentTests(unittest.TestCase):
    def test_tool_helper_builds_tool_call_envelope(self) -> None:
        self.assertEqual(
            tool("Write", path="notes.txt", content="hello"),
            {
                "toolName": "Write",
                "arguments": {"path": "notes.txt", "content": "hello"},
            },
        )

    def test_tool_schemas_expose_builtin_tools(self) -> None:
        schemas = tool_schemas()
        self.assertIn("Read", [schema["name"] for schema in schemas])
        self.assertIn("Bash", [schema["name"] for schema in schemas])
        read_schema = next(schema for schema in schemas if schema["name"] == "Read")
        self.assertEqual(read_schema["input_schema"]["required"], ["path"])

    def test_execute_accepts_agent_tool_call_shapes(self) -> None:
        config = _RuntimeConfig(
            binaryPath="executioner",
            queueDir="/tmp/queue",
            sdkCreatedQueueDir=False,
            sdkCreatedStateDir=False,
            baseUrl="http://127.0.0.1:1/",
            host={"kind": "http", "baseUrl": "http://127.0.0.1:1/"},
            worker={"kind": "external"},
            workspace={"kind": "new"},
            policy={},
            lifecycle={"destroyOnClose": True, "cleanupQueueOnClose": False, "cleanupStateOnClose": False},
            submitTimeoutMs=30_000,
        )
        session_info = SessionInfo.from_json({
            "id": "sess",
            "state": "ready",
            "workspace": {
                "root": "/tmp/workspace",
                "logicalRoot": "/workspace",
                "mode": "new",
                "fresh": True,
                "managed": True,
            },
            "createdAt": "now",
            "metadata": {},
        })
        session = ExecutionerSession(config, session_info)

        submitted: list[dict[str, object]] = []

        def fake_submit(call: dict[str, object]) -> SubmitResult:
            submitted.append(call)
            return SubmitResult.from_json({
                "invocationId": "inv",
                "sessionId": "sess",
                "toolName": call["toolName"],
                "status": "success",
                "output": "ok",
                "error": None,
                "summary": None,
                "effects": [],
                "durationMs": 1,
                "metadata": {},
            })

        with patch.object(session, "submit", side_effect=fake_submit):
            result = session.execute({"id": "call_1", "name": "Read", "input": {"path": "notes.txt"}})

        self.assertEqual(result.output, "ok")
        self.assertEqual(submitted, [{
            "toolName": "Read",
            "arguments": {"path": "notes.txt"},
            "metadata": {"toolCallId": "call_1"},
        }])

    def test_rejects_invalid_session_id_before_url_construction(self) -> None:
        with self.assertRaisesRegex(ValueError, "invalid session id"):
            _assert_session_id("../escaped")

    def test_normalize_base_url_rejects_unsafe_urls(self) -> None:
        for base_url in (
            "file:///tmp/executioner",
            "http:///tmp/executioner",
            "http://user:pass@127.0.0.1:1/",
            "http://127.0.0.1:1/?token=secret",
            "http://127.0.0.1:1/#fragment",
        ):
            with self.subTest(base_url=base_url):
                with self.assertRaisesRegex(ValueError, "invalid host.baseUrl"):
                    _normalize_base_url(base_url)

    def test_normalize_base_url_preserves_path_prefix(self) -> None:
        self.assertEqual(
            _normalize_base_url("http://127.0.0.1:1/api"),
            "http://127.0.0.1:1/api/",
        )

    def test_runtime_binary_resolution_does_not_use_repo_target_directory(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            resolved = _resolve_binary_path(None)

        self.assertNotIn("target/release", resolved)
        self.assertEqual(Path(resolved).name, _runtime_binary_name())

    def test_runtime_binary_resolution_respects_explicit_overrides(self) -> None:
        self.assertEqual(_resolve_binary_path("/opt/substrate/executioner"), "/opt/substrate/executioner")

        with patch.dict(os.environ, {"EXECUTIONER_BIN": "/opt/substrate/env-executioner"}):
            self.assertEqual(_resolve_binary_path(None), "/opt/substrate/env-executioner")

    def test_runtime_binary_resolution_uses_bundled_package_binary(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            package_dir = Path(temp_dir) / "executioner_sdk"
            bin_dir = package_dir / "bin"
            bin_dir.mkdir(parents=True)
            bundled_binary = bin_dir / _runtime_binary_name()
            bundled_binary.write_text("", encoding="utf-8")

            with patch.dict(os.environ, {}, clear=True), \
                patch("executioner_sdk.environment.__file__", str(package_dir / "environment.py")), \
                patch("executioner_sdk.environment._sidecar_runtime_binary_path", return_value=None):
                self.assertEqual(_resolve_binary_path(None), str(bundled_binary.resolve()))

    def test_runtime_binary_resolution_uses_python_sidecar_package(self) -> None:
        package_name = "test_substrate_runtime"
        with tempfile.TemporaryDirectory() as temp_dir:
            package_dir = Path(temp_dir) / package_name
            bin_dir = package_dir / "bin"
            bin_dir.mkdir(parents=True)
            (package_dir / "__init__.py").write_text("", encoding="utf-8")
            sidecar_binary = bin_dir / _runtime_binary_name()
            sidecar_binary.write_text("", encoding="utf-8")

            sys.path.insert(0, temp_dir)
            importlib.invalidate_caches()
            try:
                with patch.dict(os.environ, {}, clear=True), \
                    patch("executioner_sdk.environment._bundled_runtime_binary_path", return_value=None), \
                    patch("executioner_sdk.environment._RUNTIME_PACKAGE_NAMES", (package_name,)):
                    self.assertEqual(_resolve_binary_path(None), str(sidecar_binary))
            finally:
                sys.modules.pop(package_name, None)
                sys.path.remove(temp_dir)
                importlib.invalidate_caches()

    def test_spawned_managed_process_stdout_cannot_block_on_undrained_pipe(self) -> None:
        managed = _spawn_process(
            sys.executable,
            [
                "-c",
                "import sys; sys.stdout.buffer.write(b'x' * (1024 * 1024)); sys.stdout.flush()",
            ],
            "noisy-child",
        )
        try:
            try:
                returncode = managed.process.wait(timeout=2)
            except subprocess.TimeoutExpired:
                self.fail("managed child blocked because stdout was not drained")
            self.assertEqual(returncode, 0)
        finally:
            _terminate_process(managed)

    def test_materialize_config_rejects_malformed_values(self) -> None:
        with self.assertRaisesRegex(TypeError, "cleanupQueueOnClose must be a boolean"):
            _materialize_config(
                binary_path="executioner",
                backend={"kind": "file"},
                host={"kind": "http", "baseUrl": "http://127.0.0.1:1/"},
                worker={"kind": "external"},
                workspace={"kind": "new"},
                policy=None,
                lifecycle={"cleanupQueueOnClose": "yes"},  # type: ignore[typeddict-item]
                submit_timeout_ms=None,
            )

        with self.assertRaisesRegex(TypeError, "process.allowExec must be a boolean"):
            _materialize_config(
                binary_path="executioner",
                backend={"kind": "file"},
                host={"kind": "http", "baseUrl": "http://127.0.0.1:1/"},
                worker={"kind": "external"},
                workspace={"kind": "new"},
                policy={"process": {"allowExec": "yes"}},  # type: ignore[typeddict-item]
                lifecycle=None,
                submit_timeout_ms=None,
            )

        with self.assertRaisesRegex(ValueError, "process.maxProcesses must be non-negative"):
            _materialize_config(
                binary_path="executioner",
                backend={"kind": "file"},
                host={"kind": "http", "baseUrl": "http://127.0.0.1:1/"},
                worker={"kind": "external"},
                workspace={"kind": "new"},
                policy={"process": {"maxProcesses": -1}},
                lifecycle=None,
                submit_timeout_ms=None,
            )

        with self.assertRaisesRegex(ValueError, "process.maxProcesses exceeds maximum supported process count"):
            _materialize_config(
                binary_path="executioner",
                backend={"kind": "file"},
                host={"kind": "http", "baseUrl": "http://127.0.0.1:1/"},
                worker={"kind": "external"},
                workspace={"kind": "new"},
                policy={"process": {"maxProcesses": 2 ** 32}},
                lifecycle=None,
                submit_timeout_ms=None,
            )

        with self.assertRaisesRegex(TypeError, "readRoots must be a string list"):
            _materialize_config(
                binary_path="executioner",
                backend={"kind": "file"},
                host={"kind": "http", "baseUrl": "http://127.0.0.1:1/"},
                worker={"kind": "external"},
                workspace={"kind": "new"},
                policy={"readRoots": "/workspace"},  # type: ignore[typeddict-item]
                lifecycle=None,
                submit_timeout_ms=None,
            )

        with self.assertRaisesRegex(ValueError, "policy.readRoots"):
            _materialize_config(
                binary_path="executioner",
                backend={"kind": "file"},
                host={"kind": "http", "baseUrl": "http://127.0.0.1:1/"},
                worker={"kind": "external"},
                workspace={"kind": "new"},
                policy={"readRoots": ["/workspace/../outside"]},
                lifecycle=None,
                submit_timeout_ms=None,
            )

        with self.assertRaisesRegex(ValueError, "policy.writeRoots"):
            _materialize_config(
                binary_path="executioner",
                backend={"kind": "file"},
                host={"kind": "http", "baseUrl": "http://127.0.0.1:1/"},
                worker={"kind": "external"},
                workspace={"kind": "new"},
                policy={"writeRoots": ["/workspace/."]},
                lifecycle=None,
                submit_timeout_ms=None,
            )

        with self.assertRaisesRegex(ValueError, "maxOutputBytes exceeds maximum supported output size"):
            _materialize_config(
                binary_path="executioner",
                backend={"kind": "file"},
                host={"kind": "http", "baseUrl": "http://127.0.0.1:1/"},
                worker={"kind": "external"},
                workspace={"kind": "new"},
                policy={"maxOutputBytes": 10 * 1024 * 1024 + 1},
                lifecycle=None,
                submit_timeout_ms=None,
            )

        with self.assertRaisesRegex(ValueError, "maxDurationMs exceeds maximum supported tool timeout"):
            _materialize_config(
                binary_path="executioner",
                backend={"kind": "file"},
                host={"kind": "http", "baseUrl": "http://127.0.0.1:1/"},
                worker={"kind": "external"},
                workspace={"kind": "new"},
                policy={"maxDurationMs": 60 * 60 * 1000 + 1},
                lifecycle=None,
                submit_timeout_ms=None,
            )

        with self.assertRaisesRegex(ValueError, "maxDurationMs must be positive"):
            _materialize_config(
                binary_path="executioner",
                backend={"kind": "file"},
                host={"kind": "http", "baseUrl": "http://127.0.0.1:1/"},
                worker={"kind": "external"},
                workspace={"kind": "new"},
                policy={"maxDurationMs": 0},
                lifecycle=None,
                submit_timeout_ms=None,
            )

        with self.assertRaisesRegex(ValueError, "network policy is not enforceable yet"):
            _materialize_config(
                binary_path="executioner",
                backend={"kind": "file"},
                host={"kind": "http", "baseUrl": "http://127.0.0.1:1/"},
                worker={"kind": "external"},
                workspace={"kind": "new"},
                policy={"network": {"enabled": True}},
                lifecycle=None,
                submit_timeout_ms=None,
            )

        with self.assertRaisesRegex(ValueError, "worker.idleSleepMs must be non-negative"):
            _materialize_config(
                binary_path="executioner",
                backend={"kind": "file"},
                host={"kind": "http", "baseUrl": "http://127.0.0.1:1/"},
                worker={"kind": "managed", "idleSleepMs": -1},
                workspace={"kind": "new"},
                policy=None,
                lifecycle=None,
                submit_timeout_ms=None,
            )

        with self.assertRaisesRegex(ValueError, "worker.idleSleepMs must be positive"):
            _materialize_config(
                binary_path="executioner",
                backend={"kind": "file"},
                host={"kind": "http", "baseUrl": "http://127.0.0.1:1/"},
                worker={"kind": "managed", "idleSleepMs": 0},
                workspace={"kind": "new"},
                policy=None,
                lifecycle=None,
                submit_timeout_ms=None,
            )

        with self.assertRaisesRegex(ValueError, "submitTimeoutMs must be positive"):
            _materialize_config(
                binary_path="executioner",
                backend={"kind": "file"},
                host={"kind": "http", "baseUrl": "http://127.0.0.1:1/"},
                worker={"kind": "external"},
                workspace={"kind": "new"},
                policy=None,
                lifecycle=None,
                submit_timeout_ms=0,
            )

        with self.assertRaisesRegex(ValueError, "invalid worker.id"):
            _materialize_config(
                binary_path="executioner",
                backend={"kind": "file"},
                host={"kind": "http", "baseUrl": "http://127.0.0.1:1/"},
                worker={"kind": "managed", "id": "../escaped"},
                workspace={"kind": "new"},
                policy=None,
                lifecycle=None,
                submit_timeout_ms=None,
            )

        malformed_string_cases = [
            (
                42,
                {"kind": "file"},
                {"kind": "http", "baseUrl": "http://127.0.0.1:1/"},
                {"kind": "external"},
                {"kind": "new"},
                "binaryPath must be a string",
            ),
            (
                "executioner",
                {"kind": "file", "queueDir": 42},
                {"kind": "http", "baseUrl": "http://127.0.0.1:1/"},
                {"kind": "external"},
                {"kind": "new"},
                "backend.queueDir must be a string",
            ),
            (
                "executioner",
                {"kind": "file"},
                {"kind": "http", "baseUrl": 42},
                {"kind": "external"},
                {"kind": "new"},
                "host.baseUrl must be a string",
            ),
            (
                "executioner",
                {"kind": "file"},
                {"kind": "managed", "stateDir": 42},
                {"kind": "external"},
                {"kind": "new"},
                "host.stateDir must be a string",
            ),
            (
                "executioner",
                {"kind": "file"},
                {"kind": "managed", "host": 42},
                {"kind": "external"},
                {"kind": "new"},
                "host.host must be a string",
            ),
            (
                "executioner",
                {"kind": "file"},
                {"kind": "http", "baseUrl": "http://127.0.0.1:1/"},
                {"kind": "managed", "id": 42},
                {"kind": "new"},
                "worker.id must be a string",
            ),
            (
                "executioner",
                {"kind": "file"},
                {"kind": "http", "baseUrl": "http://127.0.0.1:1/"},
                {"kind": "external"},
                {"kind": "existing", "root": 42},
                "workspace.root must be a string",
            ),
        ]
        for binary_path, backend, host, worker, workspace, message in malformed_string_cases:
            with self.assertRaisesRegex(TypeError, message):
                _materialize_config(
                    binary_path=binary_path,  # type: ignore[arg-type]
                    backend=backend,  # type: ignore[arg-type]
                    host=host,  # type: ignore[arg-type]
                    worker=worker,  # type: ignore[arg-type]
                    workspace=workspace,  # type: ignore[arg-type]
                    policy=None,
                    lifecycle=None,
                    submit_timeout_ms=None,
                )

        with self.assertRaisesRegex(ValueError, "workspace.root must be absolute"):
            _materialize_config(
                binary_path="executioner",
                backend={"kind": "file"},
                host={"kind": "managed", "stateDir": "/tmp/executioner-python-state-should-not-exist"},
                worker={"kind": "external"},
                workspace={"kind": "existing", "root": "relative-workspace"},
                policy=None,
                lifecycle=None,
                submit_timeout_ms=None,
            )

        if hasattr(os, "symlink"):
            with tempfile.TemporaryDirectory() as temp_dir:
                root = Path(temp_dir)
                outside = root / "outside"
                link_parent = root / "link-parent"
                (outside / "workspace").mkdir(parents=True)
                os.symlink(outside, link_parent)

                with self.assertRaisesRegex(ValueError, "workspace.root parent must not contain symlinks"):
                    _materialize_config(
                        binary_path="executioner",
                        backend={"kind": "file", "queueDir": str(root / "queue")},
                        host={"kind": "managed", "stateDir": str(root / "state")},
                        worker={"kind": "external"},
                        workspace={"kind": "existing", "root": str(link_parent / "workspace")},
                        policy=None,
                        lifecycle=None,
                        submit_timeout_ms=None,
                    )

        for port in (0, 70000):
            with self.assertRaisesRegex(ValueError, "host.port must be between 1 and 65535"):
                _materialize_config(
                    binary_path="executioner",
                    backend={"kind": "file"},
                    host={"kind": "managed", "port": port},
                    worker={"kind": "external"},
                    workspace={"kind": "new"},
                    policy=None,
                    lifecycle=None,
                    submit_timeout_ms=None,
                )

        kind_cases = [
            (
                {"kind": "sqlite"},
                {"kind": "http", "baseUrl": "http://127.0.0.1:1/"},
                {"kind": "external"},
                {"kind": "new"},
                "backend.kind must be one of: file",
            ),
            (
                {"kind": "file"},
                {"kind": "stdio"},
                {"kind": "external"},
                {"kind": "new"},
                "host.kind must be one of: managed, http",
            ),
            (
                {"kind": "file"},
                {"kind": "http", "baseUrl": "http://127.0.0.1:1/"},
                {"kind": "daemon"},
                {"kind": "new"},
                "worker.kind must be one of: managed, external",
            ),
            (
                {"kind": "file"},
                {"kind": "http", "baseUrl": "http://127.0.0.1:1/"},
                {"kind": "external"},
                {"kind": "snapshot"},
                "workspace.kind must be one of: new, existing",
            ),
        ]
        for backend, host, worker, workspace, message in kind_cases:
            with self.assertRaisesRegex(ValueError, message):
                _materialize_config(
                    binary_path="executioner",
                    backend=backend,  # type: ignore[arg-type]
                    host=host,  # type: ignore[arg-type]
                    worker=worker,  # type: ignore[arg-type]
                    workspace=workspace,  # type: ignore[arg-type]
                    policy=None,
                    lifecycle=None,
                    submit_timeout_ms=None,
                )

        unknown_field_cases = [
            (
                {"kind": "file"},
                {"kind": "http", "baseUrl": "http://127.0.0.1:1/"},
                {"kind": "external"},
                {"kind": "new"},
                {"process": {"allowExec": False, "requiredCapabilities": ["file.read"]}},
                {},
                "unknown process field",
            ),
            (
                {"kind": "file"},
                {"kind": "http", "baseUrl": "http://127.0.0.1:1/"},
                {"kind": "external"},
                {"kind": "new"},
                None,
                {"cleanupQueueOnClose": False, "preserveState": True},
                "unknown lifecycle field",
            ),
            (
                {"kind": "file"},
                {"kind": "http", "baseUrl": "http://127.0.0.1:1/"},
                {"kind": "external", "sandbox": "none"},
                {"kind": "new"},
                None,
                {},
                "unknown worker field",
            ),
        ]
        for backend, host, worker, workspace, policy, lifecycle, message in unknown_field_cases:
            with self.assertRaisesRegex(ValueError, message):
                _materialize_config(
                    binary_path="executioner",
                    backend=backend,  # type: ignore[arg-type]
                    host=host,  # type: ignore[arg-type]
                    worker=worker,  # type: ignore[arg-type]
                    workspace=workspace,  # type: ignore[arg-type]
                    policy=policy,  # type: ignore[arg-type]
                    lifecycle=lifecycle,  # type: ignore[arg-type]
                    submit_timeout_ms=None,
                )

    def test_file_artifact_uri_must_be_absolute(self) -> None:
        with self.assertRaisesRegex(ValueError, "absolute"):
            _path_from_file_uri("file://relative.tar")

    def test_file_artifact_uri_rejects_authority_like_or_decorated_paths(self) -> None:
        for uri in (
            "file:////tmp/workspace.tar",
            "file:///tmp/workspace.tar?download=1",
            "file:///tmp/workspace.tar#fragment",
        ):
            with self.subTest(uri=uri):
                with self.assertRaisesRegex(ValueError, "without authority"):
                    _path_from_file_uri(uri)

    def test_http_error_body_is_capped(self) -> None:
        request = urlrequest.Request("http://127.0.0.1/error")
        error = urlerror.HTTPError(
            request.full_url,
            500,
            "Internal Server Error",
            {},
            io.BytesIO(b"x" * (256 * 1024)),
        )

        with patch("executioner_sdk.environment._urlopen_no_redirect", side_effect=error):
            with self.assertRaisesRegex(RuntimeError, "truncated") as raised:
                _request_json(request)

        self.assertLess(len(str(raised.exception)), 80 * 1024)

    def test_http_success_body_is_capped(self) -> None:
        class HugeResponse:
            def __enter__(self) -> "HugeResponse":
                return self

            def __exit__(self, *args: object) -> None:
                return None

            def read(self, size: int = -1) -> bytes:
                body = b'{"padding":"' + (b"x" * (11 * 1024 * 1024)) + b'"}'
                if size < 0:
                    return body
                return body[:size]

        request = urlrequest.Request("http://127.0.0.1/success")

        with patch("executioner_sdk.environment._urlopen_no_redirect", return_value=HugeResponse()):
            with self.assertRaisesRegex(ValueError, "response body exceeds"):
                _request_json(request)

    def test_http_success_body_must_be_json_object(self) -> None:
        class ArrayResponse:
            def __enter__(self) -> "ArrayResponse":
                return self

            def __exit__(self, *args: object) -> None:
                return None

            def read(self, size: int = -1) -> bytes:
                return b'["not", "an", "object"]'

        request = urlrequest.Request("http://127.0.0.1/success")

        with patch("executioner_sdk.environment._urlopen_no_redirect", return_value=ArrayResponse()):
            with self.assertRaisesRegex(TypeError, "host response must be a JSON object"):
                _request_json(request)

    def test_http_client_does_not_follow_redirects_with_request_body(self) -> None:
        captured = {"value": False}

        class CaptureHandler(http.server.BaseHTTPRequestHandler):
            def do_POST(self) -> None:
                captured["value"] = True
                self.send_response(200)
                self.send_header("content-type", "application/json")
                self.end_headers()
                self.wfile.write(b'{"ok":true}')

            def log_message(self, format: str, *args: object) -> None:
                return

        capture_server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), CaptureHandler)
        capture_thread = threading.Thread(target=capture_server.serve_forever, daemon=True)
        capture_thread.start()

        class RedirectHandler(http.server.BaseHTTPRequestHandler):
            def do_POST(self) -> None:
                self.send_response(307)
                self.send_header(
                    "location",
                    f"http://127.0.0.1:{capture_server.server_address[1]}/capture",
                )
                self.end_headers()

            def log_message(self, format: str, *args: object) -> None:
                return

        redirect_server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), RedirectHandler)
        redirect_thread = threading.Thread(target=redirect_server.serve_forever, daemon=True)
        redirect_thread.start()
        request = urlrequest.Request(
            f"http://127.0.0.1:{redirect_server.server_address[1]}/sessions",
            data=b'{"secret":"do not forward"}',
            method="POST",
            headers={"content-type": "application/json"},
        )

        try:
            with self.assertRaisesRegex(RuntimeError, "307"):
                _request_json(request)
            self.assertFalse(captured["value"])
        finally:
            redirect_server.shutdown()
            capture_server.shutdown()
            redirect_server.server_close()
            capture_server.server_close()
            redirect_thread.join(timeout=1)
            capture_thread.join(timeout=1)

    def test_duplicate_invocation_id_in_any_queue_state_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as queue_dir:
            queue = Path(queue_dir)
            for child in ["pending", "claimed", "completed", "failed"]:
                (queue / child).mkdir()
            invocation_id = "py_duplicate_invocation"

            for child in ["pending", "claimed", "completed", "failed"]:
                existing = queue / child / f"{invocation_id}.json"
                existing.write_text('{"original": true}', encoding="utf-8")

                with self.assertRaisesRegex(RuntimeError, "duplicate invocationId"):
                    _ensure_invocation_id_unused(str(queue), invocation_id)

                self.assertEqual(existing.read_text(encoding="utf-8"), '{"original": true}')
                existing.unlink()

    @unittest.skipUnless(hasattr(os, "symlink"), "symlinks are not available")
    def test_duplicate_invocation_id_detects_dangling_queue_symlink(self) -> None:
        with tempfile.TemporaryDirectory() as queue_dir:
            queue = Path(queue_dir)
            for child in ["pending", "claimed", "completed", "failed"]:
                (queue / child).mkdir()
            dangling = queue / "pending" / "py_dangling.json"
            os.symlink(queue / "missing-target.json", dangling)

            with self.assertRaisesRegex(RuntimeError, "duplicate invocationId"):
                _ensure_invocation_id_unused(str(queue), "py_dangling")

            self.assertTrue(dangling.is_symlink())

    def test_write_json_atomic_does_not_overwrite_existing_queue_entry(self) -> None:
        with tempfile.TemporaryDirectory() as queue_dir:
            path = Path(queue_dir) / "pending.json"
            path.write_text('{"original": true}', encoding="utf-8")

            with self.assertRaises(FileExistsError):
                _write_json_atomic(path, {"replacement": True})

            self.assertEqual(path.read_text(encoding="utf-8"), '{"original": true}')

    def test_submit_rejects_malformed_protocol_options_without_writing_queue_entry(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            queue = root / "queue"
            _ensure_file_queue(str(queue))
            config = _RuntimeConfig(
                binaryPath="executioner",
                queueDir=str(queue),
                sdkCreatedQueueDir=False,
                sdkCreatedStateDir=False,
                baseUrl="http://127.0.0.1:1/",
                host={"kind": "http", "baseUrl": "http://127.0.0.1:1/"},
                worker={"kind": "external"},
                workspace={"kind": "new"},
                policy={},
                lifecycle={
                    "destroyOnClose": True,
                    "cleanupQueueOnClose": False,
                    "cleanupStateOnClose": False,
                },
                submitTimeoutMs=50,
            )
            session = SessionInfo(
                id="sess_bad_options",
                state="ready",
                workspace=WorkspaceInfo(
                    root="/workspace",
                    logicalRoot="/workspace",
                    mode="new",
                    fresh=True,
                    managed=True,
                ),
                createdAt="now",
            )
            client = ExecutionerSession(config, session)

            cases = [
                (
                    {
                        "invocationId": "py_bad_cwd",
                        "toolName": "Read",
                        "arguments": {"path": "missing.txt"},
                        "cwd": 42,
                    },
                    TypeError,
                    "cwd must be a string",
                    "py_bad_cwd",
                ),
                (
                    {
                        "invocationId": "py_bad_metadata",
                        "toolName": "Read",
                        "arguments": {"path": "missing.txt"},
                        "metadata": "not metadata",
                    },
                    TypeError,
                    "metadata must be a JSON object",
                    "py_bad_metadata",
                ),
                (
                    {
                        "invocationId": "py_bad_timeout",
                        "toolName": "Read",
                        "arguments": {"path": "missing.txt"},
                        "timeoutMs": -1,
                    },
                    ValueError,
                    "timeoutMs must be non-negative",
                    "py_bad_timeout",
                ),
                (
                    {
                        "invocationId": "py_bad_timeout_cap",
                        "toolName": "Read",
                        "arguments": {"path": "missing.txt"},
                        "timeoutMs": 60 * 60 * 1000 + 1,
                    },
                    ValueError,
                    "timeoutMs exceeds maximum supported tool timeout",
                    "py_bad_timeout_cap",
                ),
                (
                    {
                        "invocationId": "py_zero_timeout",
                        "toolName": "Read",
                        "arguments": {"path": "missing.txt"},
                        "timeoutMs": 0,
                    },
                    ValueError,
                    "timeoutMs must be positive",
                    "py_zero_timeout",
                ),
                (
                    {
                        "invocationId": "py_bad_output_limit",
                        "toolName": "Read",
                        "arguments": {"path": "missing.txt"},
                        "maxOutputBytes": 10 * 1024 * 1024 + 1,
                    },
                    ValueError,
                    "maxOutputBytes exceeds maximum supported output size",
                    "py_bad_output_limit",
                ),
                (
                    {
                        "invocationId": "py_bad_unknown",
                        "toolName": "Read",
                        "arguments": {"path": "missing.txt"},
                        "requiredCapabilities": [{"kind": "file.read"}],
                    },
                    ValueError,
                    "unknown tool call field",
                    "py_bad_unknown",
                ),
                (
                    {
                        "invocationId": "py_oversized_request",
                        "toolName": "Read",
                        "arguments": {"path": "missing.txt"},
                        "metadata": {"padding": "x" * (1024 * 1024)},
                    },
                    ValueError,
                    "tool invocation request exceeds maximum JSON size",
                    "py_oversized_request",
                ),
            ]

            for call, error_type, message, invocation_id in cases:
                with self.assertRaisesRegex(error_type, message):
                    client.submit(call)  # type: ignore[arg-type]
                self.assertFalse((queue / "pending" / f"{invocation_id}.json").exists())

    @unittest.skipUnless(hasattr(os, "symlink"), "symlinks are not available")
    def test_submit_rejects_swapped_queue_state_directory_without_writing_through_it(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            queue = root / "queue"
            outside_pending = root / "outside-pending"
            _ensure_file_queue(str(queue))
            config = _RuntimeConfig(
                binaryPath="executioner",
                queueDir=str(queue),
                sdkCreatedQueueDir=False,
                sdkCreatedStateDir=False,
                baseUrl="http://127.0.0.1:1/",
                host={"kind": "http", "baseUrl": "http://127.0.0.1:1/"},
                worker={"kind": "external"},
                workspace={"kind": "new"},
                policy={},
                lifecycle={
                    "destroyOnClose": True,
                    "cleanupQueueOnClose": False,
                    "cleanupStateOnClose": False,
                },
                submitTimeoutMs=50,
            )
            session = SessionInfo(
                id="sess_swapped_queue",
                state="ready",
                workspace=WorkspaceInfo(
                    root="/workspace",
                    logicalRoot="/workspace",
                    mode="new",
                    fresh=True,
                    managed=True,
                ),
                createdAt="now",
            )
            client = ExecutionerSession(config, session)
            outside_pending.mkdir()
            (queue / "pending").rmdir()
            os.symlink(outside_pending, queue / "pending")

            with self.assertRaisesRegex(RuntimeError, "queue state directory"):
                client.submit({
                    "invocationId": "py_swapped_pending",
                    "toolName": "Read",
                    "arguments": {"path": "missing.txt"},
                })

            self.assertFalse((outside_pending / "py_swapped_pending.json").exists())

    @unittest.skipUnless(hasattr(os, "symlink"), "symlinks are not available")
    def test_file_queue_rejects_symlink_state_directory(self) -> None:
        with tempfile.TemporaryDirectory() as queue_dir:
            queue = Path(queue_dir) / "queue"
            outside_pending = Path(queue_dir) / "outside-pending"
            queue.mkdir()
            outside_pending.mkdir()
            os.symlink(outside_pending, queue / "pending")

            with self.assertRaisesRegex(RuntimeError, "queue state directory"):
                _ensure_file_queue(str(queue))

            self.assertEqual(list(outside_pending.iterdir()), [])

    @unittest.skipUnless(hasattr(os, "symlink"), "symlinks are not available")
    def test_file_queue_rejects_symlink_root_directory(self) -> None:
        with tempfile.TemporaryDirectory() as queue_dir:
            queue = Path(queue_dir) / "queue"
            outside_queue = Path(queue_dir) / "outside-queue"
            outside_queue.mkdir()
            os.symlink(outside_queue, queue)

            with self.assertRaisesRegex(RuntimeError, "queue directory"):
                _ensure_file_queue(str(queue))

            self.assertEqual(list(outside_queue.iterdir()), [])

    @unittest.skipUnless(hasattr(os, "symlink"), "symlinks are not available")
    def test_file_queue_rejects_symlink_parent_directory(self) -> None:
        with tempfile.TemporaryDirectory() as queue_dir:
            outside_queue = Path(queue_dir) / "outside-queue"
            link_parent = Path(queue_dir) / "link-parent"
            outside_queue.mkdir()
            os.symlink(outside_queue, link_parent)

            with self.assertRaisesRegex(ValueError, "parent must not contain symlinks"):
                _ensure_file_queue(str(link_parent / "queue"))

            self.assertEqual(list(outside_queue.iterdir()), [])

    def test_user_provided_managed_state_dir_is_not_cleanup_default(self) -> None:
        with tempfile.TemporaryDirectory() as state_dir:
            runtime = _materialize_config(
                binary_path="executioner",
                backend={"kind": "file"},
                host={"kind": "managed", "stateDir": state_dir},
                worker={"kind": "external"},
                workspace={"kind": "new"},
                policy=None,
                lifecycle=None,
                submit_timeout_ms=None,
            )

            self.assertFalse(runtime.lifecycle["cleanupStateOnClose"])

    def test_sdk_created_managed_state_dir_is_cleanup_default(self) -> None:
        runtime = _materialize_config(
            binary_path="executioner",
            backend={"kind": "file"},
            host={"kind": "managed"},
            worker={"kind": "external"},
            workspace={"kind": "new"},
            policy=None,
            lifecycle=None,
            submit_timeout_ms=None,
        )

        try:
            self.assertTrue(runtime.lifecycle["cleanupStateOnClose"])
        finally:
            Path(runtime.host["stateDir"]).rmdir()

    def test_materialize_config_serializes_max_processes_policy(self) -> None:
        runtime = _materialize_config(
            binary_path="executioner",
            backend={"kind": "file"},
            host={"kind": "http", "baseUrl": "http://127.0.0.1:1/"},
            worker={"kind": "external"},
            workspace={"kind": "new"},
            policy={"process": {"allowExec": True, "allowedCommands": ["printf ok"], "maxProcesses": 0}},
            lifecycle=None,
            submit_timeout_ms=None,
        )

        self.assertEqual(runtime.policy["process"]["maxProcesses"], 0)

    def test_create_environment_rejects_invalid_returned_environment_id(self) -> None:
        runtime = _materialize_config(
            binary_path="executioner",
            backend={"kind": "file"},
            host={"kind": "http", "baseUrl": "http://127.0.0.1:1/"},
            worker={"kind": "external"},
            workspace={"kind": "new"},
            policy=None,
            lifecycle=None,
            submit_timeout_ms=None,
        )

        def post(_url: str, _body: object) -> dict[str, object]:
            return {
                "environment": {
                    "id": "../escaped",
                    "state": "ready",
                    "workspace": {
                        "root": "/tmp/workspace",
                        "logicalRoot": "/workspace",
                        "mode": "new",
                        "fresh": True,
                        "managed": True,
                    },
                    "createdAt": "now",
                    "revision": 0,
                    "metadata": {},
                }
            }

        with patch("executioner_sdk.environment._post_json", post):
            with self.assertRaisesRegex(ValueError, "invalid environment id"):
                _create_environment(runtime)

    def test_create_cleans_up_started_resources_on_failure(self) -> None:
        class Managed:
            name = "executioner-host"

        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            queue = root / "queue"
            state = root / "state"
            state.mkdir()
            runtime = _RuntimeConfig(
                binaryPath="executioner",
                queueDir=str(queue),
                sdkCreatedQueueDir=True,
                sdkCreatedStateDir=True,
                baseUrl="http://127.0.0.1:1/",
                host={
                    "kind": "managed",
                    "stateDir": str(state),
                    "host": "127.0.0.1",
                    "port": 1,
                },
                worker={"kind": "external"},
                workspace={"kind": "new"},
                policy={},
                lifecycle={
                    "destroyOnClose": True,
                    "cleanupQueueOnClose": True,
                    "cleanupStateOnClose": True,
                },
                submitTimeoutMs=100,
            )
            terminated: list[str] = []

            def materialize(**_kwargs: object) -> _RuntimeConfig:
                return runtime

            def spawn(*_args: object) -> Managed:
                return Managed()

            def terminate(managed: Managed) -> None:
                terminated.append(managed.name)

            def fail_create(_runtime: _RuntimeConfig) -> EnvironmentInfo:
                raise RuntimeError("environment create failed")

            with (
                patch("executioner_sdk.environment._materialize_config", materialize),
                patch("executioner_sdk.environment._spawn_process", spawn),
                patch("executioner_sdk.environment._wait_for_health", lambda *_args: None),
                patch("executioner_sdk.environment._create_environment", fail_create),
                patch("executioner_sdk.environment._terminate_process", terminate),
            ):
                with self.assertRaisesRegex(RuntimeError, "environment create failed"):
                    ExecutionerEnvironment.create()

            self.assertEqual(terminated, ["executioner-host"])
            self.assertFalse(queue.exists())
            self.assertFalse(state.exists())

    def test_create_destroys_session_before_stopping_managed_host_on_worker_start_failure(self) -> None:
        class Managed:
            def __init__(self, name: str) -> None:
                self.name = name

        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            queue = root / "queue"
            state = root / "state"
            runtime = _RuntimeConfig(
                binaryPath="executioner",
                queueDir=str(queue),
                sdkCreatedQueueDir=True,
                sdkCreatedStateDir=True,
                baseUrl="http://127.0.0.1:1/",
                host={
                    "kind": "managed",
                    "stateDir": str(state),
                    "host": "127.0.0.1",
                    "port": 1,
                },
                worker={"kind": "managed", "id": "worker", "idleSleepMs": 1},
                workspace={"kind": "new"},
                policy={},
                lifecycle={
                    "destroyOnClose": True,
                    "cleanupQueueOnClose": True,
                    "cleanupStateOnClose": True,
                },
                submitTimeoutMs=100,
            )
            environment = EnvironmentInfo(
                id="env_partial",
                state="ready",
                workspace=WorkspaceInfo(
                    root="/workspace",
                    logicalRoot="/workspace",
                    mode="new",
                    fresh=True,
                    managed=True,
                ),
                createdAt="now",
                revision=0,
            )
            events: list[str] = []

            def materialize(**_kwargs: object) -> _RuntimeConfig:
                return runtime

            def spawn(_binary: str, _args: list[str], name: str) -> Managed:
                if name == "executioner-worker":
                    raise RuntimeError("worker start failed")
                return Managed(name)

            def terminate(managed: Managed) -> None:
                events.append(f"terminate:{managed.name}")

            def destroy(url: str) -> dict[str, object]:
                events.append(f"destroy:{url.rsplit('/', 1)[-1]}")
                return {
                    "id": environment.id,
                    "state": "destroyed",
                    "workspace": environment.workspace.__dict__,
                    "createdAt": environment.createdAt,
                    "revision": 1,
                    "metadata": {},
                }

            with (
                patch("executioner_sdk.environment._materialize_config", materialize),
                patch("executioner_sdk.environment._spawn_process", spawn),
                patch("executioner_sdk.environment._wait_for_health", lambda *_args: None),
                patch("executioner_sdk.environment._create_environment", lambda *_args: environment),
                patch("executioner_sdk.environment._terminate_process", terminate),
                patch("executioner_sdk.environment._delete_json", destroy),
            ):
                with self.assertRaisesRegex(RuntimeError, "worker start failed"):
                    ExecutionerEnvironment.create()

            self.assertEqual(events, ["destroy:env_partial", "terminate:executioner-host"])

    def test_list_files_parser_prefers_structured_metadata_entries(self) -> None:
        result = SubmitResult.from_json({
            "invocationId": "inv",
            "sessionId": "sess",
            "toolName": "List",
            "status": "success",
            "output": "line\nbreak.txt",
            "error": None,
            "summary": None,
            "effects": [],
            "durationMs": 0,
            "metadata": {"entries": ["line\nbreak.txt"]},
        })

        self.assertEqual(_parse_list_files_result(result), ["line\nbreak.txt"])

    def test_list_files_parser_rejects_malformed_structured_entries(self) -> None:
        result = SubmitResult.from_json({
            "invocationId": "inv",
            "sessionId": "sess",
            "toolName": "List",
            "status": "success",
            "output": "fallback.txt",
            "error": None,
            "summary": None,
            "effects": [],
            "durationMs": 0,
            "metadata": {"entries": ["visible.txt", 42, "hidden.txt"]},
        })

        with self.assertRaisesRegex(ValueError, "entries must be strings"):
            _parse_list_files_result(result)

    def test_list_files_parser_rejects_non_array_structured_entries(self) -> None:
        result = SubmitResult.from_json({
            "invocationId": "inv",
            "sessionId": "sess",
            "toolName": "List",
            "status": "success",
            "output": "fallback.txt",
            "error": None,
            "summary": None,
            "effects": [],
            "durationMs": 0,
            "metadata": {"entries": "visible.txt"},
        })

        with self.assertRaisesRegex(TypeError, "entries must be an array"):
            _parse_list_files_result(result)

    def test_list_files_parser_rejects_truncated_structured_entries(self) -> None:
        result = SubmitResult.from_json({
            "invocationId": "inv",
            "sessionId": "sess",
            "toolName": "List",
            "status": "success",
            "output": "visible.txt\n...[truncated at 1000 entries, 1005 total]",
            "error": None,
            "summary": None,
            "effects": [],
            "durationMs": 0,
            "metadata": {"entries": ["visible.txt"], "truncated": True},
        })

        with self.assertRaisesRegex(RuntimeError, "truncated"):
            _parse_list_files_result(result)

    def test_list_files_parser_rejects_malformed_truncated_metadata(self) -> None:
        result = SubmitResult.from_json({
            "invocationId": "inv",
            "sessionId": "sess",
            "toolName": "List",
            "status": "success",
            "output": "visible.txt",
            "error": None,
            "summary": None,
            "effects": [],
            "durationMs": 0,
            "metadata": {"entries": ["visible.txt"], "truncated": "true"},
        })

        with self.assertRaisesRegex(TypeError, "truncated metadata must be a boolean"):
            _parse_list_files_result(result)

    def test_list_files_parser_rejects_truncated_output_fallback(self) -> None:
        result = SubmitResult.from_json({
            "invocationId": "inv",
            "sessionId": "sess",
            "toolName": "List",
            "status": "success",
            "output": "visible.txt\n...[truncated at 1000 entries, 1005 total]",
            "error": None,
            "summary": None,
            "effects": [],
            "durationMs": 0,
            "metadata": {},
        })

        with self.assertRaisesRegex(RuntimeError, "truncated"):
            _parse_list_files_result(result)

    def test_list_files_parser_preserves_empty_message_like_filename_fallback(self) -> None:
        result = SubmitResult.from_json({
            "invocationId": "inv",
            "sessionId": "sess",
            "toolName": "List",
            "status": "success",
            "output": "No files found matching pattern: notes.txt",
            "error": None,
            "summary": None,
            "effects": [],
            "durationMs": 0,
            "metadata": {},
        })

        self.assertEqual(
            _parse_list_files_result(result),
            ["No files found matching pattern: notes.txt"],
        )

    def test_list_files_parser_rejects_unsuccessful_results(self) -> None:
        result = SubmitResult.from_json({
            "invocationId": "inv",
            "sessionId": "sess",
            "toolName": "List",
            "status": "policy_denied",
            "output": "",
            "error": "Read denied for /workspace",
            "summary": None,
            "effects": [],
            "durationMs": 0,
            "metadata": {},
        })

        with self.assertRaisesRegex(RuntimeError, "List failed"):
            _parse_list_files_result(result)

    def test_session_parser_rejects_string_booleans_instead_of_coercing(self) -> None:
        with self.assertRaisesRegex(TypeError, "workspace fresh"):
            SessionInfo.from_json({
                "id": "sess",
                "state": "ready",
                "workspace": {
                    "root": "/tmp/workspace",
                    "logicalRoot": "/workspace",
                    "mode": "new",
                    "fresh": "false",
                    "managed": True,
                },
                "createdAt": "now",
                "metadata": {},
            })

    def test_session_parser_rejects_missing_required_fields_instead_of_defaulting(self) -> None:
        with self.assertRaisesRegex(ValueError, "workspace root is required"):
            SessionInfo.from_json({
                "id": "sess",
                "state": "ready",
                "workspace": {
                    "logicalRoot": "/workspace",
                    "mode": "new",
                    "fresh": True,
                    "managed": True,
                },
                "createdAt": "now",
                "metadata": {},
            })

    def test_session_parser_rejects_unknown_fields(self) -> None:
        session = {
            "id": "sess",
            "state": "ready",
            "workspace": {
                "root": "/tmp/workspace",
                "logicalRoot": "/workspace",
                "mode": "new",
                "fresh": True,
                "managed": True,
            },
            "createdAt": "now",
            "metadata": {},
        }

        with self.assertRaisesRegex(ValueError, "unknown create session response field"):
            _parse_create_session_response({
                "session": session,
                "padding": "unexpected",
            })

        with self.assertRaisesRegex(ValueError, "unknown session field"):
            SessionInfo.from_json({
                **session,
                "padding": "unexpected",
            })

        with self.assertRaisesRegex(ValueError, "unknown workspace field"):
            SessionInfo.from_json({
                **session,
                "workspace": {
                    **session["workspace"],
                    "padding": "unexpected",
                },
            })

    def test_session_parser_rejects_unknown_enum_values(self) -> None:
        session = {
            "id": "sess",
            "state": "ready",
            "workspace": {
                "root": "/tmp/workspace",
                "logicalRoot": "/workspace",
                "mode": "new",
                "fresh": True,
                "managed": True,
            },
            "createdAt": "now",
            "metadata": {},
        }

        with self.assertRaisesRegex(ValueError, "unknown session state"):
            SessionInfo.from_json({
                **session,
                "state": "rooted",
            })

        with self.assertRaisesRegex(ValueError, "unknown workspace mode"):
            SessionInfo.from_json({
                **session,
                "workspace": {
                    **session["workspace"],
                    "mode": "mounted",
                },
            })

    def test_submit_result_parser_rejects_malformed_protocol_types(self) -> None:
        with self.assertRaisesRegex(TypeError, "submit result effects"):
            SubmitResult.from_json({
                "invocationId": "inv",
                "sessionId": "sess",
                "toolName": "Read",
                "status": "success",
                "output": "ok",
                "error": None,
                "summary": None,
                "effects": "not an array",
                "durationMs": 0,
                "metadata": {},
            })

    def test_submit_result_parser_rejects_negative_duration(self) -> None:
        with self.assertRaisesRegex(ValueError, "durationMs must be non-negative"):
            SubmitResult.from_json({
                "invocationId": "inv",
                "sessionId": "sess",
                "toolName": "Read",
                "status": "success",
                "output": "ok",
                "error": None,
                "summary": None,
                "effects": [],
                "durationMs": -1,
                "metadata": {},
            })

    def test_submit_result_parser_rejects_unknown_result_and_effect_fields(self) -> None:
        with self.assertRaisesRegex(ValueError, "unknown submit result field"):
            SubmitResult.from_json({
                "invocationId": "inv",
                "sessionId": "sess",
                "toolName": "Read",
                "status": "success",
                "output": "ok",
                "error": None,
                "summary": None,
                "effects": [],
                "durationMs": 0,
                "metadata": {},
                "padding": "unexpected",
            })

        with self.assertRaisesRegex(ValueError, "unknown state effect field"):
            SubmitResult.from_json({
                "invocationId": "inv",
                "sessionId": "sess",
                "toolName": "Read",
                "status": "success",
                "output": "ok",
                "error": None,
                "summary": None,
                "effects": [{
                    "id": "effect",
                    "invocationId": "inv",
                    "kind": "file.read",
                    "resource": {
                        "resourceType": "file",
                        "uri": "file:///workspace/file.txt",
                    },
                    "operation": "read",
                    "before": {
                        "hash": "sha256:empty",
                        "metadata": {},
                    },
                    "reversible": False,
                    "occurredAt": "now",
                    "padding": "unexpected",
                }],
                "durationMs": 0,
                "metadata": {},
            })

        with self.assertRaisesRegex(ValueError, "unknown state effect before field"):
            SubmitResult.from_json({
                "invocationId": "inv",
                "sessionId": "sess",
                "toolName": "Read",
                "status": "success",
                "output": "ok",
                "error": None,
                "summary": None,
                "effects": [{
                    "id": "effect",
                    "invocationId": "inv",
                    "kind": "file.read",
                    "resource": {
                        "resourceType": "file",
                        "uri": "file:///workspace/file.txt",
                    },
                    "operation": "read",
                    "before": {
                        "hash": "sha256:empty",
                        "padding": "unexpected",
                    },
                    "reversible": False,
                    "occurredAt": "now",
                }],
                "durationMs": 0,
                "metadata": {},
            })

        with self.assertRaisesRegex(ValueError, "unknown state effect resource field"):
            SubmitResult.from_json({
                "invocationId": "inv",
                "sessionId": "sess",
                "toolName": "Read",
                "status": "success",
                "output": "ok",
                "error": None,
                "summary": None,
                "effects": [{
                    "id": "effect",
                    "invocationId": "inv",
                    "kind": "file.read",
                    "resource": {
                        "resourceType": "file",
                        "uri": "file:///workspace/file.txt",
                        "padding": "unexpected",
                    },
                    "operation": "read",
                    "reversible": False,
                    "occurredAt": "now",
                }],
                "durationMs": 0,
                "metadata": {},
            })

    def test_artifact_parser_rejects_malformed_entries_type(self) -> None:
        with self.assertRaisesRegex(TypeError, "artifact entries"):
            WorkspaceArtifact.from_json({
                "environmentId": "sess",
                "artifact": {"resourceType": "artifact", "uri": "file:///tmp/workspace.tar"},
                "manifest": {"resourceType": "artifact_manifest", "uri": "file:///tmp/workspace.manifest.json"},
                "format": "tar",
                "bytes": 0,
                "hash": "sha256:empty",
                "fileCount": 0,
                "directoryCount": 0,
                "symlinkCount": 0,
                "entries": {"archivePath": "hidden.txt"},
                "createdAt": "now",
            })

    def test_artifact_parser_rejects_negative_counts(self) -> None:
        with self.assertRaisesRegex(ValueError, "artifact bytes must be non-negative"):
            WorkspaceArtifact.from_json({
                "environmentId": "sess",
                "artifact": {"resourceType": "artifact", "uri": "file:///tmp/workspace.tar"},
                "manifest": {"resourceType": "artifact_manifest", "uri": "file:///tmp/workspace.manifest.json"},
                "format": "tar",
                "bytes": -1,
                "hash": "sha256:empty",
                "fileCount": 0,
                "directoryCount": 0,
                "symlinkCount": 0,
                "entries": [],
                "createdAt": "now",
            })

    def test_artifact_parser_rejects_missing_required_fields_instead_of_defaulting(self) -> None:
        with self.assertRaisesRegex(ValueError, "artifact hash is required"):
            WorkspaceArtifact.from_json({
                "environmentId": "sess",
                "artifact": {"resourceType": "artifact", "uri": "file:///tmp/workspace.tar"},
                "manifest": {"resourceType": "artifact_manifest", "uri": "file:///tmp/workspace.manifest.json"},
                "format": "tar",
                "bytes": 0,
                "fileCount": 0,
                "directoryCount": 0,
                "symlinkCount": 0,
                "entries": [],
                "createdAt": "now",
            })

    def test_artifact_parser_rejects_unknown_fields(self) -> None:
        artifact = {
            "environmentId": "sess",
            "artifact": {"resourceType": "artifact", "uri": "file:///tmp/workspace.tar"},
            "manifest": {"resourceType": "artifact_manifest", "uri": "file:///tmp/workspace.manifest.json"},
            "format": "tar",
            "bytes": 0,
            "hash": "sha256:empty",
            "fileCount": 0,
            "directoryCount": 0,
            "symlinkCount": 0,
            "entries": [],
            "createdAt": "now",
            "padding": "unexpected",
        }
        resource = {
            "resourceType": "artifact",
            "uri": "file:///tmp/workspace.tar",
            "padding": "unexpected",
        }
        entry = {
            "logicalPath": "/workspace/file.txt",
            "archivePath": "file.txt",
            "kind": "file",
            "bytes": 0,
            "hash": "sha256:empty",
            "padding": "unexpected",
        }

        with self.assertRaisesRegex(ValueError, "unknown workspace artifact field"):
            WorkspaceArtifact.from_json(artifact)
        with self.assertRaisesRegex(ValueError, "unknown resource ref field"):
            ResourceRef.from_json(resource)
        with self.assertRaisesRegex(ValueError, "unknown workspace artifact entry field"):
            WorkspaceArtifactEntry.from_json(entry)

    def test_artifact_resource_parser_rejects_hash_bytes_and_metadata_fields(self) -> None:
        resource = {
            "resourceType": "artifact",
            "uri": "file:///tmp/workspace.tar",
            "hash": "sha256:smuggled",
            "bytes": 0,
            "metadata": {"smuggled": True},
        }

        with self.assertRaisesRegex(ValueError, "unknown resource ref field"):
            ResourceRef.from_json(resource)

    def test_wait_rejects_completed_event_for_wrong_session(self) -> None:
        with tempfile.TemporaryDirectory() as queue_dir:
            queue = Path(queue_dir)
            (queue / "completed").mkdir()
            (queue / "failed").mkdir()
            invocation_id = "py_wrong_completed_session"
            (queue / "completed" / f"{invocation_id}.json").write_text(
                json.dumps({
                    "type": "tool.invocation.completed",
                    "invocationId": invocation_id,
                    "sessionId": "other_session",
                    "result": {
                        "invocationId": invocation_id,
                        "sessionId": "other_session",
                        "toolName": "Read",
                        "status": "success",
                        "output": "wrong session",
                        "error": None,
                        "summary": None,
                        "effects": [],
                        "durationMs": 0,
                        "metadata": {},
                    },
                    "completedAt": "now",
                }),
                encoding="utf-8",
            )

            with self.assertRaisesRegex(RuntimeError, "session mismatch"):
                _wait_for_result(str(queue), invocation_id, "expected_session", 100)

    def test_wait_quarantines_completed_event_for_wrong_tool_name(self) -> None:
        with tempfile.TemporaryDirectory() as queue_dir:
            queue = Path(queue_dir)
            (queue / "claimed").mkdir()
            (queue / "completed").mkdir()
            (queue / "failed").mkdir()
            (queue / "rejected").mkdir()
            invocation_id = "py_wrong_completed_tool"
            _write_claim(queue, invocation_id, "sess", "List")
            (queue / "completed" / f"{invocation_id}.json").write_text(
                json.dumps({
                    "type": "tool.invocation.completed",
                    "invocationId": invocation_id,
                    "sessionId": "sess",
                    "attemptId": "attempt",
                    "leaseToken": "lease",
                    "result": {
                        "invocationId": invocation_id,
                        "sessionId": "sess",
                        "toolName": "Read",
                        "status": "success",
                        "output": "forged",
                        "error": None,
                        "summary": None,
                        "effects": [],
                        "durationMs": 0,
                        "metadata": {"entries": ["forged.txt"]},
                    },
                    "completedAt": "now",
                }),
                encoding="utf-8",
            )

            with self.assertRaisesRegex(TimeoutError, "Timed out waiting"):
                _wait_for_result(str(queue), invocation_id, "sess", 20, tool_name="List")

            self.assertFalse((queue / "completed" / f"{invocation_id}.json").exists())
            self.assertEqual(len(list((queue / "rejected").iterdir())), 1)

    def test_wait_rejects_terminal_events_with_unknown_wrapper_fields(self) -> None:
        with tempfile.TemporaryDirectory() as queue_dir:
            queue = Path(queue_dir)
            (queue / "completed").mkdir()
            (queue / "failed").mkdir()
            invocation_id = "py_unknown_completed_wrapper"
            (queue / "completed" / f"{invocation_id}.json").write_text(
                json.dumps({
                    "type": "tool.invocation.completed",
                    "invocationId": invocation_id,
                    "sessionId": "sess",
                    "result": {
                        "invocationId": invocation_id,
                        "sessionId": "sess",
                        "toolName": "Read",
                        "status": "success",
                        "output": "accepted with wrapper padding",
                        "error": None,
                        "summary": None,
                        "effects": [],
                        "durationMs": 0,
                        "metadata": {},
                    },
                    "completedAt": "now",
                    "padding": "unexpected",
                }),
                encoding="utf-8",
            )

            with self.assertRaisesRegex(ValueError, "unknown completed terminal envelope field"):
                _wait_for_result(str(queue), invocation_id, "sess", 100)

    def test_wait_quarantines_non_object_terminal_json(self) -> None:
        with tempfile.TemporaryDirectory() as queue_dir:
            queue = Path(queue_dir)
            for child in ["pending", "completed", "failed", "rejected"]:
                (queue / child).mkdir()
            invocation_id = "py_terminal_array"
            terminal_path = queue / "completed" / f"{invocation_id}.json"
            terminal_path.write_text('["not", "an", "object"]', encoding="utf-8")

            with self.assertRaisesRegex(TimeoutError, "Timed out"):
                _wait_for_result(str(queue), invocation_id, "sess", 20)

            self.assertFalse(terminal_path.exists())
            self.assertEqual(len(list((queue / "rejected").iterdir())), 1)

    def test_wait_rejects_failed_event_for_wrong_session(self) -> None:
        with tempfile.TemporaryDirectory() as queue_dir:
            queue = Path(queue_dir)
            (queue / "completed").mkdir()
            (queue / "failed").mkdir()
            invocation_id = "py_wrong_failed_session"
            (queue / "failed" / f"{invocation_id}.json").write_text(
                json.dumps({
                    "type": "tool.invocation.failed",
                    "invocationId": invocation_id,
                    "sessionId": "other_session",
                    "error": {
                        "code": "wrong_session",
                        "message": "wrong session",
                        "retryable": False,
                    },
                    "failedAt": "now",
                }),
                encoding="utf-8",
            )

            with self.assertRaisesRegex(RuntimeError, "session mismatch"):
                _wait_for_result(str(queue), invocation_id, "expected_session", 100)

    def test_wait_rejects_malformed_failed_event_error_payload(self) -> None:
        with tempfile.TemporaryDirectory() as queue_dir:
            queue = Path(queue_dir)
            (queue / "completed").mkdir()
            (queue / "failed").mkdir()
            invocation_id = "py_malformed_failed_error"
            (queue / "failed" / f"{invocation_id}.json").write_text(
                json.dumps({
                    "type": "tool.invocation.failed",
                    "invocationId": invocation_id,
                    "sessionId": "sess",
                    "error": "not an error object",
                    "failedAt": "now",
                }),
                encoding="utf-8",
            )

            with self.assertRaisesRegex(RuntimeError, "failure malformed"):
                _wait_for_result(str(queue), invocation_id, "sess", 100)

    def test_wait_rejects_failed_event_with_unknown_wrapper_and_error_fields(self) -> None:
        with tempfile.TemporaryDirectory() as queue_dir:
            queue = Path(queue_dir)
            (queue / "completed").mkdir()
            (queue / "failed").mkdir()
            cases = [
                (
                    "py_unknown_failed_wrapper",
                    {
                        "padding": "unexpected",
                        "error": {
                            "code": "failed",
                            "message": "failed",
                            "retryable": False,
                        },
                    },
                    "unknown failed terminal envelope field",
                ),
                (
                    "py_unknown_failed_error",
                    {
                        "error": {
                            "code": "failed",
                            "message": "failed",
                            "retryable": False,
                            "padding": "unexpected",
                        },
                    },
                    "unknown failed terminal error field",
                ),
            ]
            for invocation_id, payload, message in cases:
                (queue / "failed" / f"{invocation_id}.json").write_text(
                    json.dumps({
                        "type": "tool.invocation.failed",
                        "invocationId": invocation_id,
                        "sessionId": "sess",
                        "failedAt": "now",
                        **payload,
                    }),
                    encoding="utf-8",
                )

                with self.assertRaisesRegex(ValueError, message):
                    _wait_for_result(str(queue), invocation_id, "sess", 100)

    def test_wait_rejects_failed_event_missing_error_code(self) -> None:
        with tempfile.TemporaryDirectory() as queue_dir:
            queue = Path(queue_dir)
            (queue / "completed").mkdir()
            (queue / "failed").mkdir()
            invocation_id = "py_failed_error_missing_code"
            (queue / "failed" / f"{invocation_id}.json").write_text(
                json.dumps({
                    "type": "tool.invocation.failed",
                    "invocationId": invocation_id,
                    "sessionId": "sess",
                    "error": {
                        "message": "missing code",
                        "retryable": False,
                    },
                    "failedAt": "now",
                }),
                encoding="utf-8",
            )

            with self.assertRaisesRegex(RuntimeError, "failure malformed"):
                _wait_for_result(str(queue), invocation_id, "sess", 100)

    def test_wait_rejects_terminal_events_with_wrong_type(self) -> None:
        with tempfile.TemporaryDirectory() as queue_dir:
            queue = Path(queue_dir)
            (queue / "completed").mkdir()
            (queue / "failed").mkdir()
            completed_id = "py_wrong_completed_type"
            failed_id = "py_wrong_failed_type"
            (queue / "completed" / f"{completed_id}.json").write_text(
                json.dumps({
                    "type": "tool.invocation.failed",
                    "invocationId": completed_id,
                    "sessionId": "sess",
                    "result": {
                        "invocationId": completed_id,
                        "sessionId": "sess",
                        "toolName": "Read",
                        "status": "success",
                        "output": "wrong event type",
                        "error": None,
                        "summary": None,
                        "effects": [],
                        "durationMs": 0,
                        "metadata": {},
                    },
                    "completedAt": "now",
                }),
                encoding="utf-8",
            )
            (queue / "failed" / f"{failed_id}.json").write_text(
                json.dumps({
                    "type": "tool.invocation.completed",
                    "invocationId": failed_id,
                    "sessionId": "sess",
                    "error": {
                        "code": "wrong_type",
                        "message": "wrong event type",
                        "retryable": False,
                    },
                    "failedAt": "now",
                }),
                encoding="utf-8",
            )

            with self.assertRaisesRegex(RuntimeError, "event type"):
                _wait_for_result(str(queue), completed_id, "sess", 100)
            with self.assertRaisesRegex(RuntimeError, "event type"):
                _wait_for_result(str(queue), failed_id, "sess", 100)

    def test_wait_rejects_completed_event_without_lease_material(self) -> None:
        with tempfile.TemporaryDirectory() as queue_dir:
            queue = Path(queue_dir)
            (queue / "completed").mkdir()
            (queue / "failed").mkdir()
            invocation_id = "py_completed_missing_lease"
            (queue / "completed" / f"{invocation_id}.json").write_text(
                json.dumps({
                    "type": "tool.invocation.completed",
                    "invocationId": invocation_id,
                    "sessionId": "sess",
                    "result": {
                        "invocationId": invocation_id,
                        "sessionId": "sess",
                        "toolName": "Read",
                        "status": "success",
                        "output": "forged without a lease",
                        "error": None,
                        "summary": None,
                        "effects": [],
                        "durationMs": 0,
                        "metadata": {},
                    },
                    "completedAt": "now",
                }),
                encoding="utf-8",
            )

            with self.assertRaisesRegex(RuntimeError, "missing lease material"):
                _wait_for_result(str(queue), invocation_id, "sess", 100)

    def test_wait_rejects_failed_event_without_lease_material(self) -> None:
        with tempfile.TemporaryDirectory() as queue_dir:
            queue = Path(queue_dir)
            (queue / "completed").mkdir()
            (queue / "failed").mkdir()
            invocation_id = "py_failed_missing_lease"
            (queue / "failed" / f"{invocation_id}.json").write_text(
                json.dumps({
                    "type": "tool.invocation.failed",
                    "invocationId": invocation_id,
                    "sessionId": "sess",
                    "error": {
                        "code": "forged",
                        "message": "forged without a lease",
                        "retryable": False,
                    },
                    "failedAt": "now",
                }),
                encoding="utf-8",
            )

            with self.assertRaisesRegex(RuntimeError, "missing lease material"):
                _wait_for_result(str(queue), invocation_id, "sess", 100)

    def test_wait_rejects_completed_event_with_forged_lease_but_no_claim(self) -> None:
        with tempfile.TemporaryDirectory() as queue_dir:
            queue = Path(queue_dir)
            (queue / "completed").mkdir()
            (queue / "failed").mkdir()
            invocation_id = "py_completed_forged_orphan_lease"
            (queue / "completed" / f"{invocation_id}.json").write_text(
                json.dumps({
                    "type": "tool.invocation.completed",
                    "invocationId": invocation_id,
                    "sessionId": "sess",
                    "attemptId": "attempt_forged",
                    "leaseToken": "lease_forged",
                    "result": {
                        "invocationId": invocation_id,
                        "sessionId": "sess",
                        "toolName": "Read",
                        "status": "success",
                        "output": "forged without a claim",
                        "error": None,
                        "summary": None,
                        "effects": [],
                        "durationMs": 0,
                        "metadata": {},
                    },
                    "completedAt": "now",
                }),
                encoding="utf-8",
            )

            with self.assertRaisesRegex(RuntimeError, "claim"):
                _wait_for_result(str(queue), invocation_id, "sess", 100)

    def test_wait_rejects_failed_event_with_forged_lease_but_no_claim(self) -> None:
        with tempfile.TemporaryDirectory() as queue_dir:
            queue = Path(queue_dir)
            (queue / "completed").mkdir()
            (queue / "failed").mkdir()
            invocation_id = "py_failed_forged_orphan_lease"
            (queue / "failed" / f"{invocation_id}.json").write_text(
                json.dumps({
                    "type": "tool.invocation.failed",
                    "invocationId": invocation_id,
                    "sessionId": "sess",
                    "attemptId": "attempt_forged",
                    "leaseToken": "lease_forged",
                    "error": {
                        "code": "forged",
                        "message": "forged without a claim",
                        "retryable": False,
                    },
                    "failedAt": "now",
                }),
                encoding="utf-8",
            )

            with self.assertRaisesRegex(RuntimeError, "claim"):
                _wait_for_result(str(queue), invocation_id, "sess", 100)

    def test_wait_quarantines_terminal_events_when_claimed_lease_is_malformed(self) -> None:
        for terminal_kind in ["completed", "failed"]:
            with self.subTest(terminal_kind=terminal_kind):
                with tempfile.TemporaryDirectory() as queue_dir:
                    queue = Path(queue_dir)
                    for child in ["claimed", "completed", "failed", "rejected"]:
                        (queue / child).mkdir()
                    invocation_id = f"py_malformed_claim_{terminal_kind}"
                    (queue / "claimed" / f"{invocation_id}.json").write_text("{not json", encoding="utf-8")
                    terminal = {
                        "type": f"tool.invocation.{terminal_kind}",
                        "invocationId": invocation_id,
                        "sessionId": "sess",
                        "attemptId": "attempt",
                        "leaseToken": "lease",
                    }
                    if terminal_kind == "completed":
                        terminal["result"] = {
                            "invocationId": invocation_id,
                            "sessionId": "sess",
                            "toolName": "Read",
                            "status": "success",
                            "output": "forged behind malformed claim",
                            "error": None,
                            "summary": None,
                            "effects": [],
                            "durationMs": 0,
                            "metadata": {},
                        }
                        terminal["completedAt"] = "now"
                    else:
                        terminal["error"] = {
                            "code": "failed",
                            "message": "forged behind malformed claim",
                            "retryable": False,
                        }
                        terminal["failedAt"] = "now"
                    (queue / terminal_kind / f"{invocation_id}.json").write_text(
                        json.dumps(terminal),
                        encoding="utf-8",
                    )

                    with self.assertRaisesRegex(TimeoutError, "Timed out waiting"):
                        _wait_for_result(str(queue), invocation_id, "sess", 20)

                    self.assertFalse((queue / terminal_kind / f"{invocation_id}.json").exists())
                    self.assertEqual(len(list((queue / "rejected").iterdir())), 1)

    def test_wait_quarantines_terminal_event_when_claimed_request_is_malformed(self) -> None:
        with tempfile.TemporaryDirectory() as queue_dir:
            queue = Path(queue_dir)
            for child in ["claimed", "completed", "failed", "rejected"]:
                (queue / child).mkdir()
            invocation_id = "py_malformed_claim_request"
            claim = {
                "workerId": "py-test-worker",
                "attemptId": "attempt",
                "leaseToken": "lease",
                "claimedAt": "now",
                "request": {
                    "invocationId": invocation_id,
                    "sessionId": "sess",
                    "toolName": "Read",
                    "arguments": {},
                    "cwd": None,
                    "timeoutMs": None,
                    "maxOutputBytes": None,
                    "idempotencyKey": None,
                    "requiredCapabilities": [],
                    "metadata": {},
                    "padding": "unexpected",
                },
            }
            (queue / "claimed" / f"{invocation_id}.json").write_text(
                json.dumps(claim),
                encoding="utf-8",
            )
            (queue / "completed" / f"{invocation_id}.json").write_text(
                json.dumps({
                    "type": "tool.invocation.completed",
                    "invocationId": invocation_id,
                    "sessionId": "sess",
                    "attemptId": "attempt",
                    "leaseToken": "lease",
                    "result": {
                        "invocationId": invocation_id,
                        "sessionId": "sess",
                        "toolName": "Read",
                        "status": "success",
                        "output": "forged behind malformed claim request",
                        "error": None,
                        "summary": None,
                        "effects": [],
                        "durationMs": 0,
                        "metadata": {},
                    },
                    "completedAt": "now",
                }),
                encoding="utf-8",
            )

            with self.assertRaisesRegex(TimeoutError, "Timed out waiting"):
                _wait_for_result(str(queue), invocation_id, "sess", 20)

            self.assertFalse((queue / "completed" / f"{invocation_id}.json").exists())
            self.assertEqual(len(list((queue / "rejected").iterdir())), 1)

    def test_wait_accepts_protocol_event_type_terminal_envelope(self) -> None:
        with tempfile.TemporaryDirectory() as queue_dir:
            queue = Path(queue_dir)
            (queue / "completed").mkdir()
            (queue / "failed").mkdir()
            invocation_id = "py_protocol_event_type"
            _write_claim(queue, invocation_id, "sess", "Read")
            (queue / "completed" / f"{invocation_id}.json").write_text(
                json.dumps({
                    "eventType": "tool.invocation.completed",
                    "invocationId": invocation_id,
                    "sessionId": "sess",
                    "attemptId": "attempt",
                    "leaseToken": "lease",
                    "result": {
                        "invocationId": invocation_id,
                        "sessionId": "sess",
                        "toolName": "Read",
                        "status": "success",
                        "output": "from rust worker",
                        "error": None,
                        "summary": None,
                        "effects": [],
                        "durationMs": 0,
                        "metadata": {},
                    },
                    "completedAt": "now",
                }),
                encoding="utf-8",
            )

            result = _wait_for_result(str(queue), invocation_id, "sess", 100)

            self.assertEqual(result.output, "from rust worker")

    def test_wait_quarantines_terminal_event_while_invocation_is_pending(self) -> None:
        with tempfile.TemporaryDirectory() as queue_dir:
            queue = Path(queue_dir)
            for child in ["pending", "completed", "failed", "rejected"]:
                (queue / child).mkdir()
            invocation_id = "py_pending_completed"
            (queue / "pending" / f"{invocation_id}.json").write_text("{}", encoding="utf-8")
            (queue / "completed" / f"{invocation_id}.json").write_text(
                json.dumps({
                    "type": "tool.invocation.completed",
                    "invocationId": invocation_id,
                    "sessionId": "sess",
                    "result": {
                        "invocationId": invocation_id,
                        "sessionId": "sess",
                        "toolName": "Read",
                        "status": "success",
                        "output": "forged before claim",
                        "error": None,
                        "summary": None,
                        "effects": [],
                        "durationMs": 0,
                        "metadata": {},
                    },
                    "completedAt": "now",
                }),
                encoding="utf-8",
            )

            with self.assertRaises(TimeoutError):
                _wait_for_result(str(queue), invocation_id, "sess", 20)

            self.assertTrue((queue / "pending" / f"{invocation_id}.json").exists())
            self.assertFalse((queue / "completed" / f"{invocation_id}.json").exists())
            self.assertEqual(len(list((queue / "rejected").iterdir())), 1)

    def test_wait_quarantines_oversized_terminal_event_without_accepting_it(self) -> None:
        with tempfile.TemporaryDirectory() as queue_dir:
            queue = Path(queue_dir)
            for child in ["completed", "failed", "rejected"]:
                (queue / child).mkdir()
            invocation_id = "py_huge_completed"
            (queue / "completed" / f"{invocation_id}.json").write_text(
                json.dumps({
                    "type": "tool.invocation.completed",
                    "invocationId": invocation_id,
                    "sessionId": "sess",
                    "result": {
                        "invocationId": invocation_id,
                        "sessionId": "sess",
                        "toolName": "Read",
                        "status": "success",
                        "output": "x" * (10 * 1024 * 1024),
                        "error": None,
                        "summary": None,
                        "effects": [],
                        "durationMs": 0,
                        "metadata": {},
                    },
                    "completedAt": "now",
                }),
                encoding="utf-8",
            )

            with self.assertRaises(TimeoutError):
                _wait_for_result(str(queue), invocation_id, "sess", 20)

            self.assertFalse((queue / "completed" / f"{invocation_id}.json").exists())
            self.assertEqual(len(list((queue / "rejected").iterdir())), 1)

    @unittest.skipUnless(hasattr(os, "symlink"), "symlinks are not available")
    def test_wait_quarantines_symlink_terminal_file_without_following_it(self) -> None:
        with tempfile.TemporaryDirectory() as queue_dir:
            queue = Path(queue_dir)
            (queue / "completed").mkdir()
            (queue / "failed").mkdir()
            (queue / "rejected").mkdir()
            outside_completed = queue.parent / "outside-completed.json"
            invocation_id = "py_linked_terminal"
            outside_completed.write_text(
                json.dumps({
                    "type": "tool.invocation.completed",
                    "invocationId": invocation_id,
                    "sessionId": "expected_session",
                    "result": {
                        "invocationId": invocation_id,
                        "sessionId": "expected_session",
                        "toolName": "Read",
                        "status": "success",
                        "output": "forged outside queue",
                        "error": None,
                        "summary": None,
                        "effects": [],
                        "durationMs": 0,
                        "metadata": {},
                    },
                    "completedAt": "now",
                }),
                encoding="utf-8",
            )
            os.symlink(outside_completed, queue / "completed" / f"{invocation_id}.json")

            with self.assertRaises(TimeoutError):
                _wait_for_result(str(queue), invocation_id, "expected_session", 10)

            self.assertTrue(outside_completed.exists())
            self.assertFalse((queue / "completed" / f"{invocation_id}.json").exists())
            self.assertEqual(len(list((queue / "rejected").iterdir())), 1)

    def test_close_stops_managed_processes_before_destroying_session(self) -> None:
        class ProcessRef:
            def __init__(self, name: str) -> None:
                self.name = name

        events: list[str] = []
        config = _RuntimeConfig(
            binaryPath="executioner",
            queueDir="/tmp/substrate-test-queue",
            sdkCreatedQueueDir=False,
            sdkCreatedStateDir=False,
            baseUrl="http://127.0.0.1:1/",
            host={"kind": "http", "baseUrl": "http://127.0.0.1:1/"},
            worker={"kind": "managed", "id": "worker", "idleSleepMs": 10},
            workspace={"kind": "new"},
            policy={},
            lifecycle={
                "destroyOnClose": True,
                "cleanupQueueOnClose": False,
                "cleanupStateOnClose": False,
            },
            submitTimeoutMs=100,
        )
        session = SessionInfo(
            id="sess_close_order",
            state="ready",
            workspace=WorkspaceInfo(
                root="/workspace",
                logicalRoot="/workspace",
                mode="new",
                fresh=True,
                managed=True,
            ),
            createdAt="now",
        )
        env = ExecutionerEnvironment(config, session, [ProcessRef("executioner-worker")])  # type: ignore[list-item]

        def terminate(_managed: object) -> None:
            events.append("terminate")

        def delete(_url: str) -> dict[str, object]:
            events.append("destroy")
            return {
                "id": session.id,
                "state": "destroyed",
                "workspace": session.workspace.__dict__,
                "createdAt": session.createdAt,
                "revision": 1,
                "metadata": {},
            }

        with (
            patch("executioner_sdk.environment._terminate_process", terminate),
            patch("executioner_sdk.environment._delete_json", delete),
        ):
            closed = env.close()

        self.assertEqual(closed.state, "destroyed")
        self.assertEqual(events, ["terminate", "destroy"])

    def test_close_cleans_resources_when_destroy_fails(self) -> None:
        class ProcessRef:
            def __init__(self, name: str) -> None:
                self.name = name

        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            queue = root / "queue"
            state = root / "state"
            queue.mkdir()
            state.mkdir()
            config = _RuntimeConfig(
                binaryPath="executioner",
                queueDir=str(queue),
                sdkCreatedQueueDir=True,
                sdkCreatedStateDir=True,
                baseUrl="http://127.0.0.1:1/",
                host={"kind": "managed", "stateDir": str(state)},
                worker={"kind": "managed", "id": "worker", "idleSleepMs": 10},
                workspace={"kind": "new"},
                policy={},
                lifecycle={
                    "destroyOnClose": True,
                    "cleanupQueueOnClose": True,
                    "cleanupStateOnClose": True,
                },
                submitTimeoutMs=100,
            )
            session = SessionInfo(
                id="sess_close_failure",
                state="ready",
                workspace=WorkspaceInfo(
                    root="/workspace",
                    logicalRoot="/workspace",
                    mode="new",
                    fresh=True,
                    managed=True,
                ),
                createdAt="now",
            )
            env = ExecutionerEnvironment(config, session, [
                ProcessRef("executioner-host"),
                ProcessRef("executioner-worker"),
            ])  # type: ignore[list-item]
            terminated: list[str] = []

            def terminate(managed: object) -> None:
                terminated.append(managed.name)  # type: ignore[attr-defined]

            def fail_destroy(_url: str) -> dict[str, object]:
                raise RuntimeError("destroy failed")

            with (
                patch("executioner_sdk.environment._terminate_process", terminate),
                patch("executioner_sdk.environment._delete_json", fail_destroy),
            ):
                with self.assertRaisesRegex(RuntimeError, "destroy failed"):
                    env.close()

            self.assertEqual(terminated, ["executioner-worker", "executioner-host"])
            self.assertFalse(queue.exists())
            self.assertFalse(state.exists())

    def test_close_preserves_preexisting_queue_root_contents(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            queue = root / "queue"
            queue.mkdir()
            (queue / "sentinel.txt").write_text("do not delete")
            config = _RuntimeConfig(
                binaryPath="executioner",
                queueDir=str(queue),
                sdkCreatedQueueDir=False,
                sdkCreatedStateDir=False,
                baseUrl="http://127.0.0.1:1/",
                host={"kind": "http", "baseUrl": "http://127.0.0.1:1/"},
                worker={"kind": "external"},
                workspace={"kind": "new"},
                policy={},
                lifecycle={
                    "destroyOnClose": True,
                    "cleanupQueueOnClose": True,
                    "cleanupStateOnClose": False,
                },
                submitTimeoutMs=100,
            )
            session = SessionInfo(
                id="sess_queue_preserve",
                state="ready",
                workspace=WorkspaceInfo(
                    root="/workspace",
                    logicalRoot="/workspace",
                    mode="new",
                    fresh=True,
                    managed=True,
                ),
                createdAt="now",
            )
            env = ExecutionerEnvironment(config, session, [])

            with patch("executioner_sdk.environment._delete_json", return_value={
                "id": "sess_queue_preserve",
                "state": "destroyed",
                "workspace": {
                    "root": "/workspace",
                    "logicalRoot": "/workspace",
                    "mode": "new",
                    "fresh": True,
                    "managed": True,
                },
                "createdAt": "now",
                "revision": 1,
                "metadata": {},
            }):
                env.close()

            self.assertEqual((queue / "sentinel.txt").read_text(), "do not delete")

    @unittest.skipUnless(hasattr(os, "symlink"), "requires symlink support")
    def test_queue_cleanup_unlinks_swapped_child_symlink_without_following_it(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            queue = root / "queue"
            outside = root / "outside"
            queue.mkdir()
            outside.mkdir()
            (queue / "sentinel.txt").write_text("do not delete")
            (outside / "secret.txt").write_text("keep me")
            for child in ["pending", "claimed", "completed", "failed", "rejected"]:
                (queue / child).mkdir()
            (queue / "pending").rmdir()
            os.symlink(outside, queue / "pending")

            _cleanup_queue_dir(str(queue), sdk_created_queue_dir=False)

            self.assertEqual((queue / "sentinel.txt").read_text(), "do not delete")
            self.assertFalse((queue / "pending").exists())
            self.assertEqual((outside / "secret.txt").read_text(), "keep me")

    @unittest.skipUnless(hasattr(os, "symlink"), "requires symlink support")
    def test_queue_cleanup_unlinks_swapped_root_symlink_without_following_it(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            queue = root / "queue"
            outside = root / "outside"
            queue.mkdir()
            (outside / "pending").mkdir(parents=True)
            (outside / "pending" / "secret.txt").write_text("keep me")
            queue.rmdir()
            os.symlink(outside, queue)

            _cleanup_queue_dir(str(queue), sdk_created_queue_dir=False)

            self.assertFalse(queue.exists())
            self.assertEqual((outside / "pending" / "secret.txt").read_text(), "keep me")

    def test_close_preserves_preexisting_state_root_contents(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            state = root / "state"
            state.mkdir()
            (state / "sentinel.txt").write_text("do not delete")
            config = _RuntimeConfig(
                binaryPath="executioner",
                queueDir=str(root / "queue"),
                sdkCreatedQueueDir=True,
                sdkCreatedStateDir=False,
                baseUrl="http://127.0.0.1:1/",
                host={"kind": "managed", "stateDir": str(state)},
                worker={"kind": "external"},
                workspace={"kind": "new"},
                policy={},
                lifecycle={
                    "destroyOnClose": True,
                    "cleanupQueueOnClose": False,
                    "cleanupStateOnClose": True,
                },
                submitTimeoutMs=100,
            )
            session = SessionInfo(
                id="sess_state_preserve",
                state="ready",
                workspace=WorkspaceInfo(
                    root="/workspace",
                    logicalRoot="/workspace",
                    mode="new",
                    fresh=True,
                    managed=True,
                ),
                createdAt="now",
            )
            env = ExecutionerEnvironment(config, session, [])

            with patch("executioner_sdk.environment._delete_json", return_value={
                "id": "sess_state_preserve",
                "state": "destroyed",
                "workspace": {
                    "root": "/workspace",
                    "logicalRoot": "/workspace",
                    "mode": "new",
                    "fresh": True,
                    "managed": True,
                },
                "createdAt": "now",
                "revision": 1,
                "metadata": {},
            }):
                env.close()

            self.assertEqual((state / "sentinel.txt").read_text(), "do not delete")

    def test_write_read_edit_with_managed_worker(self) -> None:
        with tempfile.TemporaryDirectory() as workspace:
            with ExecutionerEnvironment.create(
                binaryPath=executioner_binary(),
                workspace={"kind": "existing", "root": workspace},
                worker={"kind": "managed", "id": "executioner-python-test-worker", "idleSleepMs": 1},
            ) as env:
                session = env.create_session()
                write = session.submit({
                    "toolName": "Write",
                    "arguments": {
                        "path": "hello.txt",
                        "content": "hello from python",
                    },
                })
                read = session.submit({
                    "toolName": "Read",
                    "arguments": {"path": "hello.txt"},
                })
                edit = session.edit({
                    "path": "hello.txt",
                    "oldString": "hello from python",
                    "newString": "hello from python edit",
                })
                edited = session.submit({
                    "toolName": "Read",
                    "arguments": {"path": "hello.txt"},
                })
                listing = session.list()

            self.assertEqual(write.status, "success")
            self.assertEqual(read.output, "hello from python")
            self.assertEqual(edit.status, "success")
            self.assertEqual(edited.output, "hello from python edit")
            self.assertEqual(listing, ["hello.txt"])
            self.assertEqual(Path(workspace, "hello.txt").read_text(), "hello from python edit")

    def test_export_workspace_materializes_artifact(self) -> None:
        with tempfile.TemporaryDirectory() as workspace, tempfile.TemporaryDirectory() as output_dir:
            with ExecutionerEnvironment.create(
                binaryPath=executioner_binary(),
                workspace={"kind": "existing", "root": workspace},
                worker={"kind": "managed", "id": "executioner-python-artifact-worker", "idleSleepMs": 1},
            ) as env:
                session = env.create_session()
                session.submit({
                    "toolName": "Write",
                    "arguments": {
                        "path": "artifact.txt",
                        "content": "hello artifact",
                    },
                })
                artifact = env.export_workspace()
                env.materialize_workspace_artifact(artifact, Path(output_dir) / "restored")

            self.assertEqual(artifact.fileCount, 1)
            self.assertEqual(
                Path(output_dir, "restored", "artifact.txt").read_text(encoding="utf-8"),
                "hello artifact",
            )

    def test_materialize_rejects_compressed_tar_even_with_matching_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            tar_path = root / "workspace.tar.gz"
            _write_gzip_tar_file(tar_path, "file.txt", "payload")
            tar_hash, tar_bytes = _hash_file(tar_path)
            artifact = WorkspaceArtifact(
                environmentId="sess_test",
                artifact=ResourceRef(resourceType="artifact", uri=f"file://{tar_path}"),
                manifest=ResourceRef(resourceType="artifact_manifest", uri="file:///unused"),
                format="tar",
                bytes=tar_bytes,
                hash=tar_hash,
                fileCount=1,
                directoryCount=0,
                symlinkCount=0,
                entries=[
                    WorkspaceArtifactEntry(
                        logicalPath="/workspace/file.txt",
                        archivePath="file.txt",
                        kind="file",
                        bytes=len("payload"),
                        hash=_hash_bytes(b"payload"),
                    )
                ],
                createdAt="now",
            )

            with self.assertRaisesRegex(ValueError, "end-of-archive|uncompressed tar"):
                materialize_workspace_artifact(artifact, root / "restored")

            self.assertFalse((root / "restored").exists())

    def test_materialize_rejects_invalid_artifact_without_leaving_created_parents(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            tar_path = root / "workspace.tar"
            _write_tar_file_raw_path(tar_path, b"file.txt", b"payload")
            tar_hash, tar_bytes = _hash_file(tar_path)
            destination_parent = root / "new-parent" / "nested"
            artifact = WorkspaceArtifact(
                environmentId="sess_test",
                artifact=ResourceRef(resourceType="artifact", uri=f"file://{tar_path}"),
                manifest=ResourceRef(resourceType="artifact_manifest", uri="file:///unused"),
                format="zip",
                bytes=tar_bytes,
                hash=tar_hash,
                fileCount=1,
                directoryCount=0,
                symlinkCount=0,
                entries=[
                    WorkspaceArtifactEntry(
                        logicalPath="/workspace/file.txt",
                        archivePath="file.txt",
                        kind="file",
                        bytes=len("payload"),
                        hash=_hash_bytes(b"payload"),
                    )
                ],
                createdAt="now",
            )

            with self.assertRaisesRegex(ValueError, "unsupported workspace artifact format"):
                materialize_workspace_artifact(artifact, destination_parent / "restored")

            self.assertFalse((destination_parent / "restored").exists())
            self.assertFalse(destination_parent.exists())
            self.assertFalse((root / "new-parent").exists())

    def test_materialize_rejects_tar_missing_end_of_archive_marker(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            tar_path = root / "workspace.tar"
            _write_tar_file_raw_path(tar_path, b"file.txt", b"payload")
            tar_path.write_bytes(tar_path.read_bytes()[:-1024])
            tar_hash, tar_bytes = _hash_file(tar_path)
            artifact = WorkspaceArtifact(
                environmentId="sess_test",
                artifact=ResourceRef(resourceType="artifact", uri=f"file://{tar_path}"),
                manifest=ResourceRef(resourceType="artifact_manifest", uri="file:///unused"),
                format="tar",
                bytes=tar_bytes,
                hash=tar_hash,
                fileCount=1,
                directoryCount=0,
                symlinkCount=0,
                entries=[
                    WorkspaceArtifactEntry(
                        logicalPath="/workspace/file.txt",
                        archivePath="file.txt",
                        kind="file",
                        bytes=len("payload"),
                        hash=_hash_bytes(b"payload"),
                    )
                ],
                createdAt="now",
            )

            with self.assertRaisesRegex(ValueError, "end-of-archive"):
                materialize_workspace_artifact(artifact, root / "restored")

            self.assertFalse((root / "restored").exists())

    def test_materialize_rejects_tar_trailing_data_after_end_of_archive(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            tar_path = root / "workspace.tar"
            _write_tar_file(tar_path, "file.txt", "payload")
            tar_path.write_bytes(tar_path.read_bytes() + (b"x" * 512))
            tar_hash, tar_bytes = _hash_file(tar_path)
            artifact = WorkspaceArtifact(
                environmentId="sess_test",
                artifact=ResourceRef(resourceType="artifact", uri=f"file://{tar_path}"),
                manifest=ResourceRef(resourceType="artifact_manifest", uri="file:///unused"),
                format="tar",
                bytes=tar_bytes,
                hash=tar_hash,
                fileCount=1,
                directoryCount=0,
                symlinkCount=0,
                entries=[
                    WorkspaceArtifactEntry(
                        logicalPath="/workspace/file.txt",
                        archivePath="file.txt",
                        kind="file",
                        bytes=len("payload"),
                        hash=_hash_bytes(b"payload"),
                    )
                ],
                createdAt="now",
            )

            with self.assertRaisesRegex(ValueError, "trailing data"):
                materialize_workspace_artifact(artifact, root / "restored")

            self.assertFalse((root / "restored").exists())

    @unittest.skipUnless(hasattr(os, "symlink"), "requires symlink support")
    def test_materialize_rejects_symlinked_destination_parent(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            tar_path = root / "workspace.tar"
            _write_tar_file(tar_path, "file.txt", "payload")
            tar_hash, tar_bytes = _hash_file(tar_path)
            outside = root / "outside"
            link_parent = root / "link-parent"
            outside.mkdir()
            os.symlink(outside, link_parent)
            artifact = WorkspaceArtifact(
                environmentId="sess_test",
                artifact=ResourceRef(resourceType="artifact", uri=f"file://{tar_path}"),
                manifest=ResourceRef(resourceType="artifact_manifest", uri="file:///unused"),
                format="tar",
                bytes=tar_bytes,
                hash=tar_hash,
                fileCount=1,
                directoryCount=0,
                symlinkCount=0,
                entries=[
                    WorkspaceArtifactEntry(
                        logicalPath="/workspace/file.txt",
                        archivePath="file.txt",
                        kind="file",
                        bytes=len("payload"),
                        hash=_hash_bytes(b"payload"),
                    )
                ],
                createdAt="now",
            )

            with self.assertRaisesRegex(ValueError, "parent must not contain symlinks"):
                materialize_workspace_artifact(artifact, link_parent / "restored")

            self.assertFalse((outside / "restored").exists())

    @unittest.skipUnless(hasattr(os, "symlink"), "requires symlink support")
    def test_materialize_rejects_symlinked_destination_ancestor(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            tar_path = root / "workspace.tar"
            _write_tar_file(tar_path, "file.txt", "payload")
            tar_hash, tar_bytes = _hash_file(tar_path)
            outside = root / "outside"
            link_parent = root / "link-parent"
            (outside / "existing").mkdir(parents=True)
            os.symlink(outside, link_parent)
            artifact = WorkspaceArtifact(
                environmentId="sess_test",
                artifact=ResourceRef(resourceType="artifact", uri=f"file://{tar_path}"),
                manifest=ResourceRef(resourceType="artifact_manifest", uri="file:///unused"),
                format="tar",
                bytes=tar_bytes,
                hash=tar_hash,
                fileCount=1,
                directoryCount=0,
                symlinkCount=0,
                entries=[
                    WorkspaceArtifactEntry(
                        logicalPath="/workspace/file.txt",
                        archivePath="file.txt",
                        kind="file",
                        bytes=len("payload"),
                        hash=_hash_bytes(b"payload"),
                    )
                ],
                createdAt="now",
            )

            with self.assertRaisesRegex(ValueError, "parent must not contain symlinks"):
                materialize_workspace_artifact(artifact, link_parent / "existing" / "restored")

            self.assertFalse((outside / "existing" / "restored").exists())

    def test_materialize_rejects_manifest_path_traversal(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            tar_path = root / "workspace.tar"
            _write_tar_file(tar_path, "safe.txt", "safe")
            tar_hash, tar_bytes = _hash_file(tar_path)
            artifact = WorkspaceArtifact(
                environmentId="sess_test",
                artifact=ResourceRef(resourceType="artifact", uri=f"file://{tar_path}"),
                manifest=ResourceRef(resourceType="artifact_manifest", uri=f"file://{root / 'manifest.json'}"),
                format="tar",
                bytes=tar_bytes,
                hash=tar_hash,
                fileCount=1,
                directoryCount=0,
                symlinkCount=0,
                entries=[
                    WorkspaceArtifactEntry(
                        logicalPath="/workspace/../escape.txt",
                        archivePath="../escape.txt",
                        kind="file",
                        bytes=4,
                        hash="sha256:invalid",
                    )
                ],
                createdAt="now",
            )

            with self.assertRaisesRegex(ValueError, "unsafe artifact path"):
                materialize_workspace_artifact(artifact, root / "restored")

            self.assertFalse((root / "restored").exists())
            self.assertFalse((root / "escape.txt").exists())

    def test_materialize_rejects_runtime_artifact_with_boolean_entry_bytes(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            tar_path = root / "workspace.tar"
            _write_tar_file(tar_path, "one.txt", "x")
            tar_hash, tar_bytes = _hash_file(tar_path)
            artifact = WorkspaceArtifact(
                environmentId="sess_test",
                artifact=ResourceRef(resourceType="artifact", uri=f"file://{tar_path}"),
                manifest=ResourceRef(resourceType="artifact_manifest", uri="file:///unused"),
                format="tar",
                bytes=tar_bytes,
                hash=tar_hash,
                fileCount=1,
                directoryCount=0,
                symlinkCount=0,
                entries=[
                    WorkspaceArtifactEntry(
                        logicalPath="/workspace/one.txt",
                        archivePath="one.txt",
                        kind="file",
                        bytes=True,  # type: ignore[arg-type]
                        hash=_hash_bytes(b"x"),
                    )
                ],
                createdAt="now",
            )

            with self.assertRaisesRegex(TypeError, "artifact entry bytes must be an integer"):
                materialize_workspace_artifact(artifact, root / "restored")

            self.assertFalse((root / "restored").exists())

    def test_materialize_rejects_runtime_artifact_with_boolean_counts(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            tar_path = root / "workspace.tar"
            _write_tar_file(tar_path, "one.txt", "x")
            tar_hash, tar_bytes = _hash_file(tar_path)
            artifact = WorkspaceArtifact(
                environmentId="sess_test",
                artifact=ResourceRef(resourceType="artifact", uri=f"file://{tar_path}"),
                manifest=ResourceRef(resourceType="artifact_manifest", uri="file:///unused"),
                format="tar",
                bytes=tar_bytes,
                hash=tar_hash,
                fileCount=True,  # type: ignore[arg-type]
                directoryCount=0,
                symlinkCount=0,
                entries=[
                    WorkspaceArtifactEntry(
                        logicalPath="/workspace/one.txt",
                        archivePath="one.txt",
                        kind="file",
                        bytes=1,
                        hash=_hash_bytes(b"x"),
                    )
                ],
                createdAt="now",
            )

            with self.assertRaisesRegex(TypeError, "artifact fileCount must be an integer"):
                materialize_workspace_artifact(artifact, root / "restored")

            self.assertFalse((root / "restored").exists())

    @unittest.skipUnless(hasattr(os, "symlink"), "requires symlink support")
    def test_materialize_rejects_symlink_artifact_resource(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            tar_path = root / "workspace.tar"
            link_path = root / "workspace-link.tar"
            _write_tar_file(tar_path, "file.txt", "payload")
            tar_hash, tar_bytes = _hash_file(tar_path)
            os.symlink(tar_path, link_path)
            artifact = WorkspaceArtifact(
                environmentId="sess_test",
                artifact=ResourceRef(resourceType="artifact", uri=f"file://{link_path}"),
                manifest=ResourceRef(resourceType="artifact_manifest", uri="file:///unused"),
                format="tar",
                bytes=tar_bytes,
                hash=tar_hash,
                fileCount=1,
                directoryCount=0,
                symlinkCount=0,
                entries=[
                    WorkspaceArtifactEntry(
                        logicalPath="/workspace/file.txt",
                        archivePath="file.txt",
                        kind="file",
                        bytes=len("payload"),
                        hash=_hash_bytes(b"payload"),
                    )
                ],
                createdAt="now",
            )

            with self.assertRaisesRegex(ValueError, "must be a regular file"):
                materialize_workspace_artifact(artifact, root / "restored")

            self.assertFalse((root / "restored").exists())

    @unittest.skipUnless(hasattr(os, "symlink"), "requires symlink support")
    def test_materialize_rejects_symlink_manifest_resource(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            tar_path = root / "workspace.tar"
            manifest_path = root / "workspace.manifest.json"
            manifest_link = root / "workspace.manifest-link.json"
            _write_tar_file(tar_path, "file.txt", "payload")
            tar_hash, tar_bytes = _hash_file(tar_path)
            artifact = WorkspaceArtifact(
                environmentId="sess_test",
                artifact=ResourceRef(resourceType="artifact", uri=f"file://{tar_path}"),
                manifest=ResourceRef(resourceType="artifact_manifest", uri=f"file://{manifest_link}"),
                format="tar",
                bytes=tar_bytes,
                hash=tar_hash,
                fileCount=1,
                directoryCount=0,
                symlinkCount=0,
                entries=[
                    WorkspaceArtifactEntry(
                        logicalPath="/workspace/file.txt",
                        archivePath="file.txt",
                        kind="file",
                        bytes=len("payload"),
                        hash=_hash_bytes(b"payload"),
                    )
                ],
                createdAt="now",
            )
            manifest_path.write_text(
                json.dumps({
                    "environmentId": artifact.environmentId,
                    "artifact": artifact.artifact.__dict__,
                    "manifest": artifact.manifest.__dict__,
                    "format": artifact.format,
                    "bytes": artifact.bytes,
                    "hash": artifact.hash,
                    "fileCount": artifact.fileCount,
                    "directoryCount": artifact.directoryCount,
                    "symlinkCount": artifact.symlinkCount,
                    "entries": [artifact.entries[0].__dict__],
                    "createdAt": artifact.createdAt,
                }),
                encoding="utf-8",
            )
            os.symlink(manifest_path, manifest_link)

            with self.assertRaisesRegex(ValueError, "must be a regular file"):
                materialize_workspace_artifact(artifact, root / "restored")

            self.assertFalse((root / "restored").exists())

    def test_materialize_leaves_no_partial_files_on_failure(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            tar_path = root / "workspace.tar"
            _write_tar_file(tar_path, "partial.txt", "partial")
            tar_hash, tar_bytes = _hash_file(tar_path)
            artifact = WorkspaceArtifact(
                environmentId="sess_test",
                artifact=ResourceRef(resourceType="artifact", uri=f"file://{tar_path}"),
                manifest=ResourceRef(resourceType="artifact_manifest", uri=f"file://{root / 'manifest.json'}"),
                format="tar",
                bytes=tar_bytes,
                hash=tar_hash,
                fileCount=2,
                directoryCount=0,
                symlinkCount=0,
                entries=[
                    WorkspaceArtifactEntry(
                        logicalPath="/workspace/partial.txt",
                        archivePath="partial.txt",
                        kind="file",
                        bytes=len("partial"),
                        hash=_hash_bytes(b"partial"),
                    ),
                    WorkspaceArtifactEntry(
                        logicalPath="/workspace/missing.txt",
                        archivePath="missing.txt",
                        kind="file",
                        bytes=7,
                        hash=_hash_bytes(b"missing"),
                    ),
                ],
                createdAt="now",
            )

            with self.assertRaisesRegex(ValueError, "manifest file missing"):
                materialize_workspace_artifact(artifact, root / "restored")

            self.assertFalse((root / "restored").exists())
            self.assertFalse((root / "partial.txt").exists())

    def test_materialize_rejects_manifest_file_with_missing_parent_directory(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            tar_path = root / "workspace.tar"
            _write_tar_file(tar_path, "dir/file.txt", "payload")
            tar_hash, tar_bytes = _hash_file(tar_path)
            artifact = WorkspaceArtifact(
                environmentId="sess_test",
                artifact=ResourceRef(resourceType="artifact", uri=f"file://{tar_path}"),
                manifest=ResourceRef(resourceType="artifact_manifest", uri="file:///unused"),
                format="tar",
                bytes=tar_bytes,
                hash=tar_hash,
                fileCount=1,
                directoryCount=0,
                symlinkCount=0,
                entries=[
                    WorkspaceArtifactEntry(
                        logicalPath="/workspace/dir/file.txt",
                        archivePath="dir/file.txt",
                        kind="file",
                        bytes=len("payload"),
                        hash=_hash_bytes(b"payload"),
                    )
                ],
                createdAt="now",
            )

            with self.assertRaisesRegex(ValueError, "manifest parent directory missing"):
                materialize_workspace_artifact(artifact, root / "restored")

            self.assertFalse((root / "restored").exists())

    def test_materialize_rejects_stale_manifest_resource(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            tar_path = root / "workspace.tar"
            manifest_path = root / "workspace.manifest.json"
            _write_tar_file(tar_path, "file.txt", "payload")
            tar_hash, tar_bytes = _hash_file(tar_path)
            artifact = WorkspaceArtifact(
                environmentId="sess_test",
                artifact=ResourceRef(resourceType="artifact", uri=f"file://{tar_path}"),
                manifest=ResourceRef(resourceType="artifact_manifest", uri=f"file://{manifest_path}"),
                format="tar",
                bytes=tar_bytes,
                hash=tar_hash,
                fileCount=1,
                directoryCount=0,
                symlinkCount=0,
                entries=[
                    WorkspaceArtifactEntry(
                        logicalPath="/workspace/file.txt",
                        archivePath="file.txt",
                        kind="file",
                        bytes=len("payload"),
                        hash=_hash_bytes(b"payload"),
                    )
                ],
                createdAt="now",
            )
            manifest_path.write_text(
                json.dumps({
                    "environmentId": artifact.environmentId,
                    "artifact": artifact.artifact.__dict__,
                    "manifest": artifact.manifest.__dict__,
                    "format": artifact.format,
                    "bytes": artifact.bytes,
                    "hash": artifact.hash,
                    "fileCount": artifact.fileCount,
                    "directoryCount": artifact.directoryCount,
                    "symlinkCount": artifact.symlinkCount,
                    "entries": [{
                        **artifact.entries[0].__dict__,
                        "logicalPath": "/workspace/stale.txt",
                    }],
                    "createdAt": artifact.createdAt,
                }),
                encoding="utf-8",
            )

            with self.assertRaisesRegex(ValueError, "manifest resource"):
                materialize_workspace_artifact(artifact, root / "restored")

            self.assertFalse((root / "restored").exists())

    def test_materialize_rejects_oversized_manifest_resource(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            tar_path = root / "workspace.tar"
            manifest_path = root / "workspace.manifest.json"
            _write_tar_file(tar_path, "file.txt", "payload")
            tar_hash, tar_bytes = _hash_file(tar_path)
            artifact = WorkspaceArtifact(
                environmentId="sess_test",
                artifact=ResourceRef(resourceType="artifact", uri=f"file://{tar_path}"),
                manifest=ResourceRef(resourceType="artifact_manifest", uri=f"file://{manifest_path}"),
                format="tar",
                bytes=tar_bytes,
                hash=tar_hash,
                fileCount=1,
                directoryCount=0,
                symlinkCount=0,
                entries=[
                    WorkspaceArtifactEntry(
                        logicalPath="/workspace/file.txt",
                        archivePath="file.txt",
                        kind="file",
                        bytes=len("payload"),
                        hash=_hash_bytes(b"payload"),
                    )
                ],
                createdAt="now",
            )
            manifest_path.write_text(
                json.dumps({
                    "environmentId": artifact.environmentId,
                    "artifact": artifact.artifact.__dict__,
                    "manifest": artifact.manifest.__dict__,
                    "format": artifact.format,
                    "bytes": artifact.bytes,
                    "hash": artifact.hash,
                    "fileCount": artifact.fileCount,
                    "directoryCount": artifact.directoryCount,
                    "symlinkCount": artifact.symlinkCount,
                    "entries": [artifact.entries[0].__dict__],
                    "createdAt": artifact.createdAt,
                    "padding": "x" * (11 * 1024 * 1024),
                }),
                encoding="utf-8",
            )

            with self.assertRaisesRegex(ValueError, "manifest resource exceeds"):
                materialize_workspace_artifact(artifact, root / "restored")

            self.assertFalse((root / "restored").exists())

    def test_materialize_rejects_oversized_declared_artifact_before_reading(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            tar_path = root / "workspace.tar"
            _write_tar_file(tar_path, "file.txt", "payload")
            artifact = WorkspaceArtifact(
                environmentId="sess_test",
                artifact=ResourceRef(resourceType="artifact", uri=f"file://{tar_path}"),
                manifest=ResourceRef(resourceType="artifact_manifest", uri="file:///unused"),
                format="tar",
                bytes=100 * 1024 * 1024 + 1,
                hash="sha256:declared-too-large",
                fileCount=1,
                directoryCount=0,
                symlinkCount=0,
                entries=[
                    WorkspaceArtifactEntry(
                        logicalPath="/workspace/file.txt",
                        archivePath="file.txt",
                        kind="file",
                        bytes=len("payload"),
                        hash=_hash_bytes(b"payload"),
                    )
                ],
                createdAt="now",
            )

            with self.assertRaisesRegex(ValueError, "maximum size"):
                materialize_workspace_artifact(artifact, root / "restored")

            self.assertFalse((root / "restored").exists())

    def test_materialize_rejects_oversized_manifest_file_entry_before_extracting(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            tar_path = root / "workspace.tar"
            _write_empty_tar(tar_path)
            tar_hash, tar_bytes = _hash_file(tar_path)
            artifact = WorkspaceArtifact(
                environmentId="sess_test",
                artifact=ResourceRef(resourceType="artifact", uri=f"file://{tar_path}"),
                manifest=ResourceRef(resourceType="artifact_manifest", uri="file:///unused"),
                format="tar",
                bytes=tar_bytes,
                hash=tar_hash,
                fileCount=1,
                directoryCount=0,
                symlinkCount=0,
                entries=[
                    WorkspaceArtifactEntry(
                        logicalPath="/workspace/huge.bin",
                        archivePath="huge.bin",
                        kind="file",
                        bytes=100 * 1024 * 1024 + 1,
                        hash=_hash_bytes(b""),
                    )
                ],
                createdAt="now",
            )

            with self.assertRaisesRegex(ValueError, "maximum size"):
                materialize_workspace_artifact(artifact, root / "restored")

            self.assertFalse((root / "restored").exists())

    def test_materialize_rejects_excessive_manifest_path_depth(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            tar_path = root / "workspace.tar"
            _write_empty_tar(tar_path)
            tar_hash, tar_bytes = _hash_file(tar_path)
            archive_path = "/".join(["d"] * 257)
            artifact = WorkspaceArtifact(
                environmentId="sess_test",
                artifact=ResourceRef(resourceType="artifact", uri=f"file://{tar_path}"),
                manifest=ResourceRef(resourceType="artifact_manifest", uri="file:///unused"),
                format="tar",
                bytes=tar_bytes,
                hash=tar_hash,
                fileCount=0,
                directoryCount=0,
                symlinkCount=1,
                entries=[
                    WorkspaceArtifactEntry(
                        logicalPath=f"/workspace/{archive_path}",
                        archivePath=archive_path,
                        kind="symlink",
                        linkTarget="target.txt",
                    )
                ],
                createdAt="now",
            )

            with self.assertRaisesRegex(ValueError, "maximum path depth"):
                materialize_workspace_artifact(artifact, root / "restored")

            self.assertFalse((root / "restored").exists())

    def test_materialize_rejects_oversized_actual_artifact_before_hashing(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            tar_path = root / "workspace.tar"
            with tar_path.open("wb") as file:
                file.truncate(100 * 1024 * 1024 + 1)
            artifact = WorkspaceArtifact(
                environmentId="sess_test",
                artifact=ResourceRef(resourceType="artifact", uri=f"file://{tar_path}"),
                manifest=ResourceRef(resourceType="artifact_manifest", uri="file:///unused"),
                format="tar",
                bytes=0,
                hash=_hash_bytes(b""),
                fileCount=0,
                directoryCount=0,
                symlinkCount=0,
                entries=[],
                createdAt="now",
            )

            with self.assertRaisesRegex(ValueError, "maximum size"):
                materialize_workspace_artifact(artifact, root / "restored")

            self.assertFalse((root / "restored").exists())

    def test_materialize_rejects_relative_file_manifest_uri(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            tar_path = root / "workspace.tar"
            _write_tar_file(tar_path, "file.txt", "payload")
            tar_hash, tar_bytes = _hash_file(tar_path)
            artifact = WorkspaceArtifact(
                environmentId="sess_test",
                artifact=ResourceRef(resourceType="artifact", uri=f"file://{tar_path}"),
                manifest=ResourceRef(
                    resourceType="artifact_manifest",
                    uri="file://relative.manifest.json",
                ),
                format="tar",
                bytes=tar_bytes,
                hash=tar_hash,
                fileCount=1,
                directoryCount=0,
                symlinkCount=0,
                entries=[
                    WorkspaceArtifactEntry(
                        logicalPath="/workspace/file.txt",
                        archivePath="file.txt",
                        kind="file",
                        bytes=len("payload"),
                        hash=_hash_bytes(b"payload"),
                    )
                ],
                createdAt="now",
            )

            with self.assertRaisesRegex(ValueError, "artifact file uri must be absolute"):
                materialize_workspace_artifact(artifact, root / "restored")

            self.assertFalse((root / "restored").exists())

    def test_materialize_rejects_unsupported_manifest_uri_scheme(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            tar_path = root / "workspace.tar"
            _write_tar_file(tar_path, "file.txt", "payload")
            tar_hash, tar_bytes = _hash_file(tar_path)
            artifact = WorkspaceArtifact(
                environmentId="sess_test",
                artifact=ResourceRef(resourceType="artifact", uri=f"file://{tar_path}"),
                manifest=ResourceRef(
                    resourceType="artifact_manifest",
                    uri="https://example.invalid/workspace.manifest.json",
                ),
                format="tar",
                bytes=tar_bytes,
                hash=tar_hash,
                fileCount=1,
                directoryCount=0,
                symlinkCount=0,
                entries=[
                    WorkspaceArtifactEntry(
                        logicalPath="/workspace/file.txt",
                        archivePath="file.txt",
                        kind="file",
                        bytes=len("payload"),
                        hash=_hash_bytes(b"payload"),
                    )
                ],
                createdAt="now",
            )

            with self.assertRaisesRegex(ValueError, "manifest uri must be file://"):
                materialize_workspace_artifact(artifact, root / "restored")

            self.assertFalse((root / "restored").exists())

    def test_materialize_rejects_symlink_entry_without_target(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            tar_path = root / "workspace.tar"
            _write_empty_tar(tar_path)
            tar_hash, tar_bytes = _hash_file(tar_path)
            artifact = WorkspaceArtifact(
                environmentId="sess_test",
                artifact=ResourceRef(resourceType="artifact", uri=f"file://{tar_path}"),
                manifest=ResourceRef(resourceType="artifact_manifest", uri="file:///unused"),
                format="tar",
                bytes=tar_bytes,
                hash=tar_hash,
                fileCount=0,
                directoryCount=0,
                symlinkCount=1,
                entries=[
                    WorkspaceArtifactEntry(
                        logicalPath="/workspace/missing-link",
                        archivePath="missing-link",
                        kind="symlink",
                    )
                ],
                createdAt="now",
            )

            with self.assertRaisesRegex(ValueError, "manifest symlink entry is incomplete"):
                materialize_workspace_artifact(artifact, root / "restored")

            self.assertFalse((root / "restored").exists())

    def test_materialize_rejects_nul_manifest_archive_path(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            tar_path = root / "workspace.tar"
            _write_empty_tar(tar_path)
            tar_hash, tar_bytes = _hash_file(tar_path)
            artifact = WorkspaceArtifact(
                environmentId="sess_test",
                artifact=ResourceRef(resourceType="artifact", uri=f"file://{tar_path}"),
                manifest=ResourceRef(resourceType="artifact_manifest", uri="file:///unused"),
                format="tar",
                bytes=tar_bytes,
                hash=tar_hash,
                fileCount=0,
                directoryCount=0,
                symlinkCount=1,
                entries=[
                    WorkspaceArtifactEntry(
                        logicalPath="/workspace/bad\0link",
                        archivePath="bad\0link",
                        kind="symlink",
                        linkTarget="target.txt",
                    )
                ],
                createdAt="now",
            )

            with self.assertRaisesRegex(ValueError, "unsafe artifact path"):
                materialize_workspace_artifact(artifact, root / "restored")

            self.assertFalse((root / "restored").exists())

    def test_materialize_rejects_non_utf8_archive_paths_instead_of_rewriting_them(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            tar_path = root / "workspace.tar"
            _write_tar_file_raw_path(tar_path, b"bad-\xff.txt", b"payload")
            tar_hash, tar_bytes = _hash_file(tar_path)
            artifact = WorkspaceArtifact(
                environmentId="sess_test",
                artifact=ResourceRef(resourceType="artifact", uri=f"file://{tar_path}"),
                manifest=ResourceRef(resourceType="artifact_manifest", uri="file:///unused"),
                format="tar",
                bytes=tar_bytes,
                hash=tar_hash,
                fileCount=1,
                directoryCount=0,
                symlinkCount=0,
                entries=[
                    WorkspaceArtifactEntry(
                        logicalPath="/workspace/bad-\ufffd.txt",
                        archivePath="bad-\ufffd.txt",
                        kind="file",
                        bytes=len("payload"),
                        hash=_hash_bytes(b"payload"),
                    )
                ],
                createdAt="now",
            )

            with self.assertRaisesRegex(ValueError, "not valid UTF-8"):
                materialize_workspace_artifact(artifact, root / "restored")

            self.assertFalse((root / "restored").exists())

    def test_materialize_rejects_nul_manifest_symlink_target(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            tar_path = root / "workspace.tar"
            _write_empty_tar(tar_path)
            tar_hash, tar_bytes = _hash_file(tar_path)
            artifact = WorkspaceArtifact(
                environmentId="sess_test",
                artifact=ResourceRef(resourceType="artifact", uri=f"file://{tar_path}"),
                manifest=ResourceRef(resourceType="artifact_manifest", uri="file:///unused"),
                format="tar",
                bytes=tar_bytes,
                hash=tar_hash,
                fileCount=0,
                directoryCount=0,
                symlinkCount=1,
                entries=[
                    WorkspaceArtifactEntry(
                        logicalPath="/workspace/link",
                        archivePath="link",
                        kind="symlink",
                        linkTarget="target\0.txt",
                    )
                ],
                createdAt="now",
            )

            with self.assertRaisesRegex(ValueError, "unsafe symlink target"):
                materialize_workspace_artifact(artifact, root / "restored")

            self.assertFalse((root / "restored").exists())

    def test_materialize_rejects_backslash_manifest_symlink_target(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            tar_path = root / "workspace.tar"
            _write_empty_tar(tar_path)
            tar_hash, tar_bytes = _hash_file(tar_path)
            artifact = WorkspaceArtifact(
                environmentId="sess_test",
                artifact=ResourceRef(resourceType="artifact", uri=f"file://{tar_path}"),
                manifest=ResourceRef(resourceType="artifact_manifest", uri="file:///unused"),
                format="tar",
                bytes=tar_bytes,
                hash=tar_hash,
                fileCount=0,
                directoryCount=0,
                symlinkCount=1,
                entries=[
                    WorkspaceArtifactEntry(
                        logicalPath="/workspace/link",
                        archivePath="link",
                        kind="symlink",
                        linkTarget="dir\\target.txt",
                    )
                ],
                createdAt="now",
            )

            with self.assertRaisesRegex(ValueError, "unsafe symlink target"):
                materialize_workspace_artifact(artifact, root / "restored")

            self.assertFalse((root / "restored").exists())

    def test_materialize_rejects_manifest_directory_missing_from_artifact(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            tar_path = root / "workspace.tar"
            _write_empty_tar(tar_path)
            tar_hash, tar_bytes = _hash_file(tar_path)
            artifact = WorkspaceArtifact(
                environmentId="sess_test",
                artifact=ResourceRef(resourceType="artifact", uri=f"file://{tar_path}"),
                manifest=ResourceRef(resourceType="artifact_manifest", uri="file:///unused"),
                format="tar",
                bytes=tar_bytes,
                hash=tar_hash,
                fileCount=0,
                directoryCount=1,
                symlinkCount=0,
                entries=[
                    WorkspaceArtifactEntry(
                        logicalPath="/workspace/empty-dir",
                        archivePath="empty-dir",
                        kind="directory",
                    )
                ],
                createdAt="now",
            )

            with self.assertRaisesRegex(ValueError, "manifest directory missing from artifact"):
                materialize_workspace_artifact(artifact, root / "restored")

            self.assertFalse((root / "restored").exists())

    @unittest.skipUnless(hasattr(os, "symlink"), "requires symlink support")
    def test_materialize_rejects_excessive_manifest_entries(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            tar_path = root / "workspace.tar"
            _write_empty_tar(tar_path)
            tar_hash, tar_bytes = _hash_file(tar_path)
            entries = [
                WorkspaceArtifactEntry(
                    logicalPath=f"/workspace/link-{index}.txt",
                    archivePath=f"link-{index}.txt",
                    kind="symlink",
                    linkTarget="target.txt",
                )
                for index in range(10_001)
            ]
            artifact = WorkspaceArtifact(
                environmentId="sess_test",
                artifact=ResourceRef(resourceType="artifact", uri=f"file://{tar_path}"),
                manifest=ResourceRef(resourceType="artifact_manifest", uri="file:///unused"),
                format="tar",
                bytes=tar_bytes,
                hash=tar_hash,
                fileCount=0,
                directoryCount=0,
                symlinkCount=len(entries),
                entries=entries,
                createdAt="now",
            )

            with self.assertRaisesRegex(ValueError, "maximum entry count"):
                materialize_workspace_artifact(artifact, root / "restored")

            self.assertFalse((root / "restored").exists())

def _write_empty_tar(path: Path) -> None:
    with tarfile.open(path, "w"):
        pass

def _write_tar_file(path: Path, archive_path: str, content: str) -> None:
    with tarfile.open(path, "w") as archive:
        data = content.encode("utf-8")
        info = tarfile.TarInfo(archive_path)
        info.size = len(data)
        with tempfile.TemporaryFile() as file:
            file.write(data)
            file.seek(0)
            archive.addfile(info, file)


def _write_gzip_tar_file(path: Path, archive_path: str, content: str) -> None:
    with tarfile.open(path, "w:gz") as archive:
        data = content.encode("utf-8")
        info = tarfile.TarInfo(archive_path)
        info.size = len(data)
        with tempfile.TemporaryFile() as file:
            file.write(data)
            file.seek(0)
            archive.addfile(info, file)


def _write_tar_file_raw_path(path: Path, archive_path: bytes, content: bytes) -> None:
    header = bytearray(512)
    header[0:len(archive_path)] = archive_path[:100]
    _write_tar_ascii(header, 100, 8, "0000644")
    _write_tar_ascii(header, 108, 8, "0000000")
    _write_tar_ascii(header, 116, 8, "0000000")
    _write_tar_ascii(header, 124, 12, format(len(content), "011o"))
    _write_tar_ascii(header, 136, 12, "00000000000")
    header[148:156] = b"        "
    header[156] = ord("0")
    _write_tar_ascii(header, 257, 6, "ustar")
    _write_tar_ascii(header, 263, 2, "00")
    checksum = sum(header)
    _write_tar_ascii(header, 148, 8, format(checksum, "06o"))
    header[154] = 0
    header[155] = 0x20
    padding = b"\0" * ((512 - (len(content) % 512)) % 512)
    path.write_bytes(bytes(header) + content + padding + (b"\0" * 1024))


def _write_claim(queue: Path, invocation_id: str, session_id: str, tool_name: str) -> None:
    (queue / "claimed").mkdir(exist_ok=True)
    (queue / "claimed" / f"{invocation_id}.json").write_text(
        json.dumps({
            "workerId": "py-test-worker",
            "attemptId": "attempt",
            "leaseToken": "lease",
            "claimedAt": "now",
            "request": {
                "invocationId": invocation_id,
                "sessionId": session_id,
                "toolName": tool_name,
                "arguments": {},
                "cwd": None,
                "timeoutMs": None,
                "maxOutputBytes": None,
                "idempotencyKey": None,
                "requiredCapabilities": [],
                "metadata": {},
            },
        }),
        encoding="utf-8",
    )


def _write_tar_ascii(header: bytearray, offset: int, length: int, value: str) -> None:
    data = value.encode("ascii")
    header[offset:offset + min(len(data), length)] = data[:length]


def _hash_file(path: Path) -> tuple[str, int]:
    data = path.read_bytes()
    return _hash_bytes(data), len(data)


def _hash_bytes(data: bytes) -> str:
    return f"sha256:{hashlib.sha256(data).hexdigest()}"


if __name__ == "__main__":
    unittest.main()
