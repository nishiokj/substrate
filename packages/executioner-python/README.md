# substrate

Python SDK for the Substrate agent execution environment.

Install:

```sh
pip install substrate
```

The package is pure Python. It does not compile Rust during install. For local
managed execution, the SDK discovers a prebuilt `executioner` runtime from a
bundled `executioner_sdk/bin/executioner`, an installed `substrate-runtime`
package, or `executioner` on `PATH`. Remote-host usage does not need a local
runtime.

The public API separates environment lifetime from session lifetime:

```py
from substrate import ExecutionerEnvironment

with ExecutionerEnvironment.create(workspace={"kind": "new"}, policy={"process": {"allowExec": True, "allowedCommands": ["ls"]}}) as env:
    session = env.create_session()
    session.write("hello.txt", "hello")
    print(session.read("hello.txt"))

    session.edit({
        "path": "hello.txt",
        "oldString": "hello",
        "newString": "hello from Substrate",
    })

    print(session.bash("ls /workspace"))
    files = session.list()
    artifact = env.export_workspace()
    env.materialize_workspace_artifact(artifact, "/tmp/restored-workspace")
```

For an agent loop, pass Substrate's schemas into the model request, then execute
matching tool-use blocks directly:

```py
from anthropic import Anthropic
from substrate import ExecutionerEnvironment, tool_schemas

client = Anthropic()
messages = [{"role": "user", "content": "Create notes.txt and read it back."}]

with ExecutionerEnvironment.create(
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
            messages.append({
                "role": "user",
                "content": [{
                    "type": "tool_result",
                    "tool_use_id": block.id,
                    "content": result.output,
                }],
            })
```

The package hides the file-backed queue and worker transport, but keeps
environment and session lifecycles explicit. Multiple sessions can attach to the
same environment.
