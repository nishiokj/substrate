# @executioner/sdk

TypeScript bindings for Executioner.

The public API exposes an environment object:

```ts
import { ExecutionerEnvironment } from '@executioner/sdk';

const env = await ExecutionerEnvironment.create({
  binaryPath: '/path/to/executioner',
  workspace: { kind: 'existing', root: process.cwd() },
});

const result = await env.submit({
  toolName: 'Write',
  arguments: { path: 'hello.txt', content: 'hello' },
});

const edit = await env.edit({
  path: 'hello.txt',
  oldString: 'hello',
  newString: 'hello from Executioner',
});

const files = await env.list({ cwd: '/workspace' });
const artifact = await env.exportWorkspace();
await env.materializeWorkspaceArtifact(artifact, '/tmp/restored-workspace');

await env.close();
```

The package hides the file-backed queue and worker transport behind config. Agent
apps should not write broker files directly.
