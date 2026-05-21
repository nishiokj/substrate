# Substrate

Substrate is a standalone Rust tool execution layer for agent applications.

It separates agent control from host-side execution. An agent or worker asks for
a tool invocation. The host executes that invocation inside a session-scoped
workspace, enforces policy, and returns a result plus an effect ledger describing
what durable state changed.

This repository is intentionally decoupled from any one agent implementation.
The agent adapter is a consumer of this protocol, not the owner of it.

## Workspace

- `crates/executioner-core`: protocol types, session lifecycle, workspace path
  resolution, effect ledger, and the built-in tool implementations.
- `crates/executioner-host`: HTTP host server over `executioner-core`.
- `crates/executioner-worker`: broker/host abstractions, reusable file-backed
  broker, and pull worker loops.
- `crates/executioner-cli`: CLI for starting a host, calling a host, and running
  a file-backed worker once.
- `packages/executioner-js`: TypeScript SDK that manages host, session,
  worker, and file-backed queue lifecycle.
- `packages/executioner-python`: Python SDK that manages host, session,
  worker, and file-backed queue lifecycle.
- `docs/`: architecture and lifecycle notes.
- `examples/`: JSON requests for manual testing.

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

Run tests:

```sh
cargo test
```

Start a host:

```sh
cargo run -p executioner -- host --addr 127.0.0.1:8765 --state-dir /tmp/executioner
```

Create a fresh session:

```sh
cargo run -p executioner -- session create --host-url http://127.0.0.1:8765
```

Invoke a write:

```sh
cargo run -p executioner -- invoke \
  --host-url http://127.0.0.1:8765 \
  --session-id sess_... \
  --tool Write \
  --args-json '{"path":"hello.txt","content":"hello"}'
```

Export the current workspace to an artifact:

```sh
cargo run -p executioner -- session export \
  --host-url http://127.0.0.1:8765 \
  --session-id sess_...
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
cargo run -p executioner -- worker run \
  --host-url http://127.0.0.1:8765 \
  --queue-dir /tmp/executioner-queue
```

Process one queued item and exit:

```sh
cargo run -p executioner -- worker run-once \
  --host-url http://127.0.0.1:8765 \
  --queue-dir /tmp/executioner-queue
```

## Documents

- [Architecture](docs/architecture.md)
- [Session lifecycle](docs/lifecycle.md)
- [Agent adapter boundary](docs/agent-adapter.md)
