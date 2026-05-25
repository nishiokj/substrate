# @substrate/sdk

TypeScript SDK for the Substrate agent execution environment.

Install:

```sh
npm install @substrate/sdk
```

The package is pure TypeScript/JavaScript. It does not compile Rust during
install. For local managed execution, the SDK discovers a prebuilt `executioner`
runtime from optional platform packages, a bundled `bin/executioner`, or
`executioner` on `PATH`. Remote-host usage does not need a local runtime.

The public API separates environment lifetime from session lifetime:

```ts
import { ExecutionerEnvironment } from '@substrate/sdk';

const env = await ExecutionerEnvironment.create({
  workspace: { kind: 'new' },
  policy: { process: { allowExec: true, allowedCommands: ['ls'] } },
});
const session = await env.createSession();

await session.write('hello.txt', 'hello');
console.log(await session.read('hello.txt'));

const edit = await session.edit({
  path: 'hello.txt',
  oldString: 'hello',
  newString: 'hello from Substrate',
});

console.log(await session.bash('ls /workspace'));
const files = await session.list();
const artifact = await env.exportWorkspace();
await env.materializeWorkspaceArtifact(artifact, '/tmp/restored-workspace');

await env.close();
```

For an agent loop, pass Substrate's schemas into the model request, then execute
matching tool-use blocks directly:

```ts
import Anthropic from '@anthropic-ai/sdk';
import { ExecutionerEnvironment, toolSchemas } from '@substrate/sdk';

const client = new Anthropic();
const env = await ExecutionerEnvironment.create({
  workspace: { kind: 'new' },
  policy: { process: { allowExec: true, allowedCommands: ['python', 'pytest'] } },
});
const session = await env.createSession();
const messages = [{ role: 'user' as const, content: 'Create notes.txt and read it back.' }];
const tools = toolSchemas().map((schema) => ({
  name: schema.name,
  description: schema.description,
  input_schema: schema.inputSchema,
}));

try {
  const response = await client.messages.create({
    model: '...',
    max_tokens: 1024,
    tools,
    messages,
  });

  for (const block of response.content) {
    if (block.type === 'tool_use') {
      const result = await session.execute(block);
      messages.push({
        role: 'user',
        content: [{
          type: 'tool_result',
          tool_use_id: block.id,
          content: result.output,
        }],
      });
    }
  }
} finally {
  await env.close();
}
```

The package hides the file-backed queue and worker transport, but keeps
environment and session lifecycles explicit. Multiple sessions can attach to the
same environment.
