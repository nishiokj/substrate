# Agent Adapter Boundary

Substrate should not be shaped around a single agent implementation. Agent apps
integrate by creating an environment, handing the model Substrate's tool schemas,
creating one or more sessions attached to that environment, and executing
matching tool-use responses through a session.

## SDK Boundary

Agent applications should bind environment lifetime separately from session
lifetime. The environment owns the workspace and host/worker lifecycle; sessions
submit work against that live environment:

```ts
import { ExecutionerEnvironment } from "@substrate/sdk";

const env = await ExecutionerEnvironment.create({
    workspace: { kind: "new" },
    policy: { process: { allowExec: true, allowedCommands: ["python", "pytest"] } },
});
const session = await env.createSession();

const result = await session.execute({
    name: "Write",
    input: {
        path: "notes.txt",
        content: "hello",
    },
});
```

The SDK intentionally exposes SDK-owned types such as `ToolCall`, `ToolSchema`,
`SubmitResult`, `EnvironmentInfo`, `SessionInfo`, and `StateEffect`. Protocol
structs remain owned by `executioner-core` and are used at transport,
persistence, and schema boundaries.

## Agent Loop Shape

Agent SDKs usually emit a tool-use object with a tool name and JSON input. If
the model sees Substrate's tool schemas, the session can execute that object
directly:

```ts
const client = new Anthropic();
const env = await ExecutionerEnvironment.create({ workspace: { kind: "new" } });
const session = await env.createSession();
const tools = toolSchemas().map((schema) => ({
    name: schema.name,
    description: schema.description,
    input_schema: schema.inputSchema,
}));

const response = await client.messages.create({
    model,
    max_tokens: 1024,
    tools,
    messages,
});

for (const block of response.content) {
    if (block.type !== "tool_use") continue;
    const result = await session.execute(block);
    messages.push({
        role: "user",
        content: [{
            type: "tool_result",
            tool_use_id: block.id,
            content: result.output,
        }],
    });
}
```

If an application wants domain-specific tool names or schemas, it can map those
calls manually while still using the session as the execution authority:

```ts
if (toolUse.name === "read_project_file") {
    const result = await session.read(toolUse.input.path);
    messages.push({
        role: "user",
        content: [{
            type: "tool_result",
            tool_use_id: toolUse.id,
            content: result,
        }],
    });
}
```

That keeps the SDK small: schema export and execution for Substrate's own tools,
manual mapping for custom tool vocabularies, and no binding DSL until repeated
integration code proves one is needed.

## Advanced Worker Modes

`ExecutionerEnvironment.create(...)` starts a managed local host and managed
worker by default. Callers can choose worker and backend behavior explicitly:

```text
managed worker  -> SDK starts a background pull worker process
external worker -> SDK only enqueues and waits; another worker runtime pulls
```

Both use the same internal backend and host traits. The public facade does not
hand agent applications a `FileBroker`, `HttpHostClient`, or queue directory
API.

An external worker can also be started with the CLI:

```sh
executioner worker run \
    --host-url http://127.0.0.1:8765 \
    --queue-dir /path/to/queue \
    --id agent-worker
```

When a Unix socket or another broker is added, it should become another
`HostConfig` or `BackendConfig` variant without changing `session.execute(...)`.

## Generic Runtime Boundary

The Rust protocol structs in `executioner-core::protocol` are the wire and
storage contract. Language SDKs can be generated later from the same schema,
but the execution layer is not TypeScript-owned.

An agent runtime can then choose between local and remote execution:

```text
Agent loop -> host client -> Executioner host
Agent loop -> event broker -> Executioner worker -> Executioner host
```

The worker can be spawned by the CLI:

```sh
executioner worker run --host-url http://host:8765 --queue-dir /path/to/queue
```

It can also be embedded by constructing `executioner_worker::Worker` with any
implementation of `InvocationBroker` and `ToolHostClient`.

## Changes Required In An Existing Agent

The integration should be deliberately small:

1. Create or attach an environment when an agent run starts.
2. Create a session attached to that environment for the client or agent run.
3. Pass `tool_schemas()` / `toolSchemas()` to the model.
4. Execute matching tool calls with `session.execute(...)`.
5. Map the agent cwd to `/workspace` logical paths.
6. Feed only semantic output and summaries into the model context.
7. Store effects separately for trace, audit, cache invalidation, and UI state.

## What The Agent Should Not Own

The agent should not:

- Trust path strings without substrate validation.
- Infer writes from tool names.
- Decide host filesystem authority.
- Replay or rollback state from model context.
- Treat successful tool status as proof that no unintended mutation happened.

## Context Window Policy

The model usually gets:

```text
Updated reports/model.xlsx with a Q4 Forecast sheet and recalculated totals.
```

The system stores:

```json
{
  "effects": [
    {
      "kind": "file.write",
      "resource": {
        "type": "file",
        "uri": "file:///workspace/reports/model.xlsx"
      },
      "operation": "update",
      "reversible": true
    }
  ]
}
```

The context window should be a consumer of summarized state, not the ledger
itself.
