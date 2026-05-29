# Substrate

[![PyPI](https://img.shields.io/pypi/v/substrate-sdk?style=flat-square)](https://pypi.org/project/substrate-sdk/)
[![Tests](https://img.shields.io/github/actions/workflow/status/nishiokj/substrate/tests.yml?branch=master&label=tests&style=flat-square)](https://github.com/nishiokj/substrate/actions/workflows/tests.yml)
[![License](https://img.shields.io/badge/license-MIT-green?style=flat-square)](LICENSE)


Substrate is a standalone Rust tool execution layer for agent applications.

It separates agent control from host-side execution. An agent or worker asks for
a tool invocation. The host executes that invocation inside an
environment-scoped workspace through a session attached to that environment,
enforces policy, and returns a result plus an effect ledger describing what
durable state changed.

This repository is intentionally decoupled from any one agent implementation.
The agent adapter is a consumer of this protocol, not the owner of it.

## Workspace

- `crates/runtime-core`: protocol types, environment/session lifecycle,
  workspace path resolution, effect ledger, and the built-in tool
  implementations.
- `crates/runtime-host`: HTTP host server over `substrate-runtime-core`.
- `crates/runtime-worker`: broker/host abstractions, reusable file-backed
  broker, and pull worker loops.
- `crates/runtime-cli`: CLI for starting a host, calling a host, and running
  a file-backed worker once.
- `packages/substrate-js`: TypeScript SDK that manages environment, session,
  worker, and file-backed queue lifecycle.
- `packages/substrate-python`: Python SDK that manages environment, session,
  worker, and file-backed queue lifecycle.
- `docs/`: architecture and lifecycle notes.
- `examples/`: minimal SDK and agent-loop samples, plus JSON requests for
  manual testing.

## Core Invariant

The agent describes intent. The substrate enforces authority, performs the work,
and records what actually happened.

Tool names are not treated as the source of truth for state changes. Durable
effects are reported separately from success or failure.

## Tool Surface

Substrate currently exposes a small built-in tool surface:

- `Read`
- `Write`
- `Edit`
- `List`
- `Glob`
- `Grep`
- `Bash`

The host executes local filesystem/process tools directly. `Bash` is disabled
by default, and enabling it also requires a non-empty `allowedCommands` process
policy. Command-name allowlist entries reject shell control syntax and obvious
host-path escapes such as absolute paths, parent-directory paths, and escaped or
quoted shell fragments; exact full-command entries are treated as deliberate
trust decisions. Path-like arguments are resolved through the workspace
resolver, so symlink escapes are rejected. `process.maxProcesses` is accepted
only as a non-negative `u32`-sized count. Bash also starts with an empty environment:
host variables are copied only when listed in `env.allowlist`, injected values
come from `env.injected`, and `env.denylist` wins over both. Bash duration is
capped by the minimum of tool `timeout`, invocation `timeoutMs`, and
`policy.maxDurationMs`. Control-plane and agent-memory tools are intentionally
outside the execution substrate.

## Try It

Install SDKs from registries without compiling Rust:

```sh
npm install @substrate/sdk
pip install substrate-sdk
```

The SDKs are pure TypeScript/Python packages. Local managed mode starts a
prebuilt `substrate-runtime` binary discovered from an explicit binary path,
installed runtime packages, or `substrate-runtime` on `PATH`. Remote-host mode
does not need a local runtime binary.

Minimal SDK usage:

```py
from substrate import Environment

with Environment.create(
    workspace={"kind": "new"},
    policy={"process": {"allowExec": True, "allowedCommands": ["ls"]}},
) as env:
    session = env.create_session()
    session.write("notes.txt", "hello")
    print(session.read("notes.txt"))
    print(session.bash("ls /workspace"))
```

`Environment.create(...)` creates and owns an environment. Sessions
are created under that environment:

```py
session_a = env.create_session()
session_b = env.create_session()
```

To connect another client to an existing live environment, use `attach` rather
than creating another environment. Attached handles create their own sessions,
but do not own environment shutdown. Multiple sessions may share one
environment; the host serializes tool execution per environment so they share
one mutable workspace through an ordered stream.

Sessions are durable within the live host: a dropped SDK handle or HTTP
connection does not close the session. Clients can recover a known
participation context through the parent environment:

```py
env = Environment.attach(
    host={"kind": "http", "baseUrl": "http://127.0.0.1:8765/"},
    environmentId="env_shared",
)
session = env.create_session()
sessions = env.sessions()
same_session = env.attach_session(session.session.id)
effects = env.effects()
```

Minimal agent-loop shape:

```py
from anthropic import Anthropic
from substrate import Environment, tool_schemas

client = Anthropic()
messages = [{"role": "user", "content": "Create notes.txt and read it back."}]

with Environment.create(
    workspace={"kind": "new"},
    policy={"process": {"allowExec": True, "allowedCommands": ["python", "pytest"]}},
) as env:
    session = env.create_session()
    response = client.messages.create(
        model="...",
        max_tokens=1024,
        tools=tool_schemas(),
        messages=messages,
    )

    for block in response.content:
        if block.type == "tool_use":
            result = session.execute({
                "id": block.id,
                "name": block.name,
                "input": block.input,
            })
```

Run tests:

```sh
cargo test
```

Start a host:

```sh
cargo run -p substrate-runtime -- host --addr 127.0.0.1:8765 --state-dir /tmp/substrate-runtime
```

Create an environment over HTTP:

```sh
curl -sS http://127.0.0.1:8765/environments \
  -H 'content-type: application/json' \
  -d '{
    "workspace": {"mode": "new", "mountAsWorkspace": true},
    "policy": {
      "readRoots": ["/workspace"],
      "writeRoots": ["/workspace"],
      "process": {
        "allowExec": false,
        "allowedCommands": [],
        "deniedCommands": [],
        "maxProcesses": null
      },
      "network": {"enabled": false, "allowHosts": [], "denyHosts": []},
      "env": {"allowlist": [], "denylist": [], "injected": {}},
      "maxDurationMs": 300000,
      "maxOutputBytes": 100000
    },
    "metadata": {}
  }'
```

The response contains `environment.id`; use that id for session creation,
workspace export, attach, and explicit environment destruction.

Inspect live environments and their sessions:

```sh
cargo run -p substrate-runtime -- env list \
  --host-url http://127.0.0.1:8765

cargo run -p substrate-runtime -- session list \
  --host-url http://127.0.0.1:8765 \
  --environment-id env_...

cargo run -p substrate-runtime -- env effects \
  --host-url http://127.0.0.1:8765 \
  --environment-id env_...
```

Create a session attached to that environment:

```sh
cargo run -p substrate-runtime -- session create \
  --host-url http://127.0.0.1:8765 \
  --environment-id env_...
```

Invoke a write:

```sh
cargo run -p substrate-runtime -- invoke \
  --host-url http://127.0.0.1:8765 \
  --session-id sess_... \
  --tool Write \
  --args-json '{"path":"hello.txt","content":"hello"}'
```

Export the environment workspace to an artifact. Artifacts belong to the
environment, so this command takes `--environment-id`:

```sh
cargo run -p substrate-runtime -- session export \
  --host-url http://127.0.0.1:8765 \
  --environment-id env_...
```

The Rust, TypeScript, and Python SDKs can also materialize a verified workspace
artifact into a new empty destination. The TypeScript SDK exposes artifact
export with `env.exportWorkspace()` and materialization with
`env.materializeWorkspaceArtifact(...)`.
SDKs expose `list(cwd=...)` / `list({ cwd })` helpers for one-level directory
listings, with `list_files(cwd=...)` / `listFiles({ cwd })` kept as explicit
aliases. These helpers delegate to the built-in `List` tool.

Run a worker daemon against a file-backed queue and a remote or local host:

```sh
cargo run -p substrate-runtime -- worker run \
  --host-url http://127.0.0.1:8765 \
  --queue-dir /tmp/runtime-queue
```

Process one queued item and exit:

```sh
cargo run -p substrate-runtime -- worker run-once \
  --host-url http://127.0.0.1:8765 \
  --queue-dir /tmp/runtime-queue
```

## Documents

- [Architecture](docs/architecture.md)
- [Environment/session lifecycle](docs/lifecycle.md)
- [Agent adapter boundary](docs/agent-adapter.md)
- [Packaging](docs/packaging.md)
