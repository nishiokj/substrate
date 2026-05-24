# @executioner/sdk

TypeScript bindings for Executioner.

The public API exposes a small environment facade:

```ts
import { Executioner } from '@executioner/sdk';

const env = await Executioner.create({
  workspace: 'new',
  allowCommands: ['ls'],
});

await env.write('hello.txt', 'hello');
console.log(await env.read('hello.txt'));

const edit = await env.edit({
  path: 'hello.txt',
  oldString: 'hello',
  newString: 'hello from Executioner',
});

console.log(await env.bash('ls /workspace'));
const files = await env.list();
const artifact = await env.exportWorkspace();
await env.materializeWorkspaceArtifact(artifact, '/tmp/restored-workspace');

await env.close();
```

For an agent loop, pass Substrate's schemas into the model request, then execute
matching tool-use blocks directly:

```ts
import Anthropic from '@anthropic-ai/sdk';
import { Executioner } from '@executioner/sdk';

const client = new Anthropic();
const env = await Executioner.create({
  workspace: 'new',
  allowCommands: ['python', 'pytest'],
});
const messages = [{ role: 'user' as const, content: 'Create notes.txt and read it back.' }];
const tools = env.toolSchemas().map((schema) => ({
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
      const result = await env.execute(block);
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

The package hides the file-backed queue and worker transport behind the facade.
`ExecutionerEnvironment.create(...)` remains available for advanced host,
worker, and backend configuration.
