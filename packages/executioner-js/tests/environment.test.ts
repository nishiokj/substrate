import { afterEach, describe, expect, test } from 'bun:test';
import { mkdir, mkdtemp, readdir, readFile, readlink, rename, rmdir, rm, symlink, writeFile } from 'node:fs/promises';
import { existsSync, writeFileSync } from 'node:fs';
import { createHash } from 'node:crypto';
import { EventEmitter } from 'node:events';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import {
  Executioner,
  ExecutionerEnvironment,
  Environment,
  materializeWorkspaceArtifact,
  tool,
  toolSchemas,
  type LifecycleConfig,
  type ListFilesOptions,
  type PolicyConfig,
  type ToolCall,
  type WorkerConfig,
  type WorkspaceArtifact,
} from '../src/index.ts';

const cleanup: string[] = [];

afterEach(async () => {
  while (cleanup.length > 0) {
    const path = cleanup.pop();
    if (path) {
      await rm(path, { recursive: true, force: true });
    }
  }
});

describe('ExecutionerEnvironment file queue validation', () => {
  test('Environment alias points to friendly facade', () => {
    expect(Environment).toBe(Executioner);
  });

  test('tool helper builds tool call envelope', () => {
    expect(tool('Write', { path: 'notes.txt', content: 'hello' })).toEqual({
      toolName: 'Write',
      arguments: { path: 'notes.txt', content: 'hello' },
    });
  });

  test('toolSchemas expose built-in tools', () => {
    const schemas = toolSchemas();
    expect(schemas.map((schema) => schema.name)).toContain('Read');
    expect(schemas.map((schema) => schema.name)).toContain('Bash');
    expect(schemas.find((schema) => schema.name === 'Read')?.inputSchema).toMatchObject({
      required: ['path'],
    });
  });

  test('friendly Executioner.create lowers to environment config', async () => {
    const originalCreate = ExecutionerEnvironment.create;
    const calls: unknown[] = [];
    const sentinel = {};
    (ExecutionerEnvironment as unknown as { create: (config: unknown) => Promise<unknown> }).create = async (config: unknown) => {
      calls.push(config);
      return sentinel;
    };
    try {
      const env = await Executioner.create({
        workspace: '/tmp/substrate-demo',
        host: 'http://127.0.0.1:8765/api',
        allowCommands: ['python', 'pytest'],
        env: { TOKEN: 'secret' },
        lifecycle: { destroyOnClose: false },
        binaryPath: '/bin/executioner',
        submitTimeoutMs: 1234,
        advanced: { worker: { kind: 'external' } },
      });

      expect(env).toBe(sentinel as ExecutionerEnvironment);
      expect(calls).toEqual([{
        binaryPath: '/bin/executioner',
        backend: undefined,
        host: { kind: 'http', baseUrl: 'http://127.0.0.1:8765/api' },
        worker: { kind: 'external' },
        workspace: { kind: 'existing', root: '/tmp/substrate-demo' },
        policy: {
          process: {
            allowExec: true,
            allowedCommands: ['python', 'pytest'],
          },
          env: { injected: { TOKEN: 'secret' } },
        },
        lifecycle: { destroyOnClose: false },
        submitTimeoutMs: 1234,
      }]);
    } finally {
      (ExecutionerEnvironment as unknown as { create: typeof originalCreate }).create = originalCreate;
    }
  });

  test('execute accepts agent tool call shapes', async () => {
    const env = new (ExecutionerEnvironment as unknown as new (
      config: unknown,
      session: unknown,
      processes: unknown[],
    ) => ExecutionerEnvironment)(
      {
        queueDir: '/tmp/queue',
        submitTimeoutMs: 30_000,
      },
      {
        id: 'sess',
      },
      [],
    );
    const calls: unknown[] = [];
    const originalSubmit = env.submit;
    env.submit = async (call: ToolCall) => {
      calls.push(call);
      return {
        invocationId: 'inv',
        sessionId: 'sess',
        toolName: call.toolName,
        status: 'success',
        output: 'ok',
        effects: [],
        durationMs: 1,
        metadata: {},
      };
    };
    try {
      const result = await env.execute({
        id: 'call_1',
        name: 'Read',
        input: { path: 'notes.txt' },
      });
      expect(result.output).toBe('ok');
      expect(calls).toEqual([{
        toolName: 'Read',
        arguments: { path: 'notes.txt' },
        metadata: { toolCallId: 'call_1' },
      }]);
    } finally {
      env.submit = originalSubmit;
    }
  });

  test('create rejects malformed config values before spawning processes', async () => {
    await expect(ExecutionerEnvironment.create({
      host: { kind: 'http', baseUrl: 'http://127.0.0.1:1/' },
      worker: { kind: 'external' },
      lifecycle: { cleanupQueueOnClose: 'yes' as unknown as boolean },
    })).rejects.toThrow('cleanupQueueOnClose must be a boolean');

    await expect(ExecutionerEnvironment.create({
      host: { kind: 'http', baseUrl: 'http://127.0.0.1:1/' },
      worker: { kind: 'external' },
      policy: {
        process: { allowExec: 'yes' as unknown as boolean },
      },
    })).rejects.toThrow('process.allowExec must be a boolean');

    await expect(ExecutionerEnvironment.create({
      host: { kind: 'http', baseUrl: 'http://127.0.0.1:1/' },
      worker: { kind: 'external' },
      policy: {
        process: { maxProcesses: -1 },
      },
    })).rejects.toThrow('process.maxProcesses must be non-negative');

    await expect(ExecutionerEnvironment.create({
      host: { kind: 'http', baseUrl: 'http://127.0.0.1:1/' },
      worker: { kind: 'external' },
      policy: {
        process: { maxProcesses: 2 ** 32 },
      },
    })).rejects.toThrow('process.maxProcesses exceeds maximum supported process count');

    await expect(ExecutionerEnvironment.create({
      host: { kind: 'http', baseUrl: 'http://127.0.0.1:1/' },
      worker: { kind: 'external' },
      policy: {
        readRoots: '/workspace' as unknown as string[],
      },
    })).rejects.toThrow('readRoots must be a string array');

    await expect(ExecutionerEnvironment.create({
      host: { kind: 'http', baseUrl: 'http://127.0.0.1:1/' },
      worker: { kind: 'external' },
      policy: {
        readRoots: ['/workspace/../outside'],
      },
    })).rejects.toThrow('policy.readRoots');

    await expect(ExecutionerEnvironment.create({
      host: { kind: 'http', baseUrl: 'http://127.0.0.1:1/' },
      worker: { kind: 'external' },
      policy: {
        writeRoots: ['/workspace/.'],
      },
    })).rejects.toThrow('policy.writeRoots');

    await expect(ExecutionerEnvironment.create({
      host: { kind: 'http', baseUrl: 'http://127.0.0.1:1/' },
      worker: { kind: 'external' },
      policy: {
        maxOutputBytes: 10 * 1024 * 1024 + 1,
      },
    })).rejects.toThrow('maxOutputBytes exceeds maximum supported output size');

    await expect(ExecutionerEnvironment.create({
      host: { kind: 'http', baseUrl: 'http://127.0.0.1:1/' },
      worker: { kind: 'external' },
      policy: {
        maxDurationMs: 60 * 60 * 1000 + 1,
      },
    })).rejects.toThrow('maxDurationMs exceeds maximum supported tool timeout');

    await expect(ExecutionerEnvironment.create({
      host: { kind: 'http', baseUrl: 'http://127.0.0.1:1/' },
      worker: { kind: 'external' },
      policy: {
        maxDurationMs: 0,
      },
    })).rejects.toThrow('maxDurationMs must be positive');

    await expect(ExecutionerEnvironment.create({
      host: { kind: 'http', baseUrl: 'http://127.0.0.1:1/' },
      worker: { kind: 'external' },
      policy: {
        network: { enabled: true },
      },
    })).rejects.toThrow('network policy is not enforceable yet');

    await expect(ExecutionerEnvironment.create({
      host: { kind: 'http', baseUrl: 'http://127.0.0.1:1/' },
      worker: { kind: 'managed', idleSleepMs: -1 },
    })).rejects.toThrow('worker.idleSleepMs must be non-negative');

    await expect(ExecutionerEnvironment.create({
      host: { kind: 'http', baseUrl: 'http://127.0.0.1:1/' },
      worker: { kind: 'managed', idleSleepMs: 0 },
    })).rejects.toThrow('worker.idleSleepMs must be positive');

    await expect(ExecutionerEnvironment.create({
      host: { kind: 'http', baseUrl: 'http://127.0.0.1:1/' },
      worker: { kind: 'external' },
      submitTimeoutMs: 0,
    })).rejects.toThrow('submitTimeoutMs must be positive');

    await expect(ExecutionerEnvironment.create({
      host: { kind: 'http', baseUrl: 'http://127.0.0.1:1/' },
      worker: { kind: 'managed', id: '../escaped' },
    })).rejects.toThrow('Invalid worker.id');

    await expect(ExecutionerEnvironment.create({
      binaryPath: 42 as unknown as string,
      host: { kind: 'http', baseUrl: 'http://127.0.0.1:1/' },
      worker: { kind: 'external' },
    })).rejects.toThrow('binaryPath must be a string');

    await expect(ExecutionerEnvironment.create({
      backend: { kind: 'file', queueDir: 42 as unknown as string },
      host: { kind: 'http', baseUrl: 'http://127.0.0.1:1/' },
      worker: { kind: 'external' },
    })).rejects.toThrow('backend.queueDir must be a string');

    await expect(ExecutionerEnvironment.create({
      host: { kind: 'http', baseUrl: 42 as unknown as string },
      worker: { kind: 'external' },
    })).rejects.toThrow('host.baseUrl must be a string');

    for (const baseUrl of [
      'file:///tmp/executioner',
      'http:///tmp/executioner',
      'http://user:pass@127.0.0.1:1/',
      'http://127.0.0.1:1/?token=secret',
      'http://127.0.0.1:1/#fragment',
    ]) {
      await expect(ExecutionerEnvironment.create({
        host: { kind: 'http', baseUrl },
        worker: { kind: 'external' },
      })).rejects.toThrow('invalid host.baseUrl');
    }

    await expect(ExecutionerEnvironment.create({
      host: { kind: 'managed', stateDir: 42 as unknown as string },
      worker: { kind: 'external' },
    })).rejects.toThrow('host.stateDir must be a string');

    await expect(ExecutionerEnvironment.create({
      host: { kind: 'managed', host: 42 as unknown as string },
      worker: { kind: 'external' },
    })).rejects.toThrow('host.host must be a string');

    await expect(ExecutionerEnvironment.create({
      host: { kind: 'managed', port: 0 },
      worker: { kind: 'external' },
    })).rejects.toThrow('host.port must be between 1 and 65535');

    await expect(ExecutionerEnvironment.create({
      host: { kind: 'managed', port: 70000 },
      worker: { kind: 'external' },
    })).rejects.toThrow('host.port must be between 1 and 65535');

    await expect(ExecutionerEnvironment.create({
      host: { kind: 'http', baseUrl: 'http://127.0.0.1:1/' },
      worker: { kind: 'managed', id: 42 as unknown as string },
    })).rejects.toThrow('worker.id must be a string');

    await expect(ExecutionerEnvironment.create({
      host: { kind: 'http', baseUrl: 'http://127.0.0.1:1/' },
      worker: { kind: 'external' },
      workspace: { kind: 'existing', root: 42 as unknown as string },
    })).rejects.toThrow('workspace.root must be a string');

    await expect(ExecutionerEnvironment.create({
      backend: { kind: 'file', queueDir: join(tmpdir(), 'executioner-js-should-not-exist') },
      host: { kind: 'managed', stateDir: join(tmpdir(), 'executioner-js-state-should-not-exist') },
      worker: { kind: 'external' },
      workspace: { kind: 'existing', root: 'relative-workspace' },
    })).rejects.toThrow('workspace.root must be absolute');

    const symlinkRoot = await mkdtemp(join(tmpdir(), 'executioner-js-workspace-parent-'));
    const outside = join(symlinkRoot, 'outside');
    const linkParent = join(symlinkRoot, 'link-parent');
    await mkdir(join(outside, 'workspace'), { recursive: true });
    await symlink(outside, linkParent);
    await expect(ExecutionerEnvironment.create({
      backend: { kind: 'file', queueDir: join(symlinkRoot, 'queue') },
      host: { kind: 'managed', stateDir: join(symlinkRoot, 'state') },
      worker: { kind: 'external' },
      workspace: { kind: 'existing', root: join(linkParent, 'workspace') },
    })).rejects.toThrow('workspace.root parent must not contain symlinks');

    await expect(ExecutionerEnvironment.create({
      backend: { kind: 'sqlite' as unknown as 'file' },
      host: { kind: 'http', baseUrl: 'http://127.0.0.1:1/' },
      worker: { kind: 'external' },
    })).rejects.toThrow('backend.kind must be one of: file');

    await expect(ExecutionerEnvironment.create({
      host: { kind: 'stdio' as unknown as 'managed' },
      worker: { kind: 'external' },
    })).rejects.toThrow('host.kind must be one of: managed, http');

    await expect(ExecutionerEnvironment.create({
      host: { kind: 'http', baseUrl: 'http://127.0.0.1:1/' },
      worker: { kind: 'daemon' as unknown as 'managed' },
    })).rejects.toThrow('worker.kind must be one of: managed, external');

    await expect(ExecutionerEnvironment.create({
      host: { kind: 'http', baseUrl: 'http://127.0.0.1:1/' },
      worker: { kind: 'external' },
      workspace: { kind: 'snapshot' as unknown as 'new' },
    })).rejects.toThrow('workspace.kind must be one of: new, existing');

    await expect(ExecutionerEnvironment.create({
      host: { kind: 'http', baseUrl: 'http://127.0.0.1:1/' },
      worker: { kind: 'external' },
      policy: {
        process: {
          allowExec: false,
          requiredCapabilities: ['file.read'],
        },
      } as unknown as PolicyConfig,
    })).rejects.toThrow('unknown process field');

    await expect(ExecutionerEnvironment.create({
      host: { kind: 'http', baseUrl: 'http://127.0.0.1:1/' },
      worker: { kind: 'external' },
      lifecycle: {
        cleanupQueueOnClose: false,
        preserveState: true,
      } as unknown as LifecycleConfig,
    })).rejects.toThrow('unknown lifecycle field');

    await expect(ExecutionerEnvironment.create({
      host: { kind: 'http', baseUrl: 'http://127.0.0.1:1/' },
      worker: {
        kind: 'external',
        sandbox: 'none',
      } as unknown as WorkerConfig,
    })).rejects.toThrow('unknown worker field');
  });

  test('create serializes maxProcesses process policy', async () => {
    const root = await mkdtemp(join(tmpdir(), 'executioner-js-test-'));
    cleanup.push(root);
    const queueDir = join(root, 'queue');
    let body: Record<string, unknown> | undefined;
    const server = Bun.serve({
      port: 0,
      async fetch(request) {
        const url = new URL(request.url);
        if (request.method === 'POST' && url.pathname === '/sessions') {
          body = await request.json() as Record<string, unknown>;
          return Response.json({
            session: {
              id: 'sess_max_processes',
              state: 'ready',
              workspace: {
                root: '/tmp/workspace',
                logicalRoot: '/workspace',
                mode: 'new',
                fresh: true,
                managed: true,
              },
              createdAt: 'now',
            },
          });
        }
        if (request.method === 'DELETE' && url.pathname === '/sessions/sess_max_processes') {
          return Response.json({
            id: 'sess_max_processes',
            state: 'destroyed',
            workspace: {
              root: '/tmp/workspace',
              logicalRoot: '/workspace',
              mode: 'new',
              fresh: true,
              managed: true,
            },
            createdAt: 'now',
          });
        }
        return new Response('not found', { status: 404 });
      },
    });

    try {
      const env = await ExecutionerEnvironment.create({
        backend: { kind: 'file', queueDir },
        host: { kind: 'http', baseUrl: `http://127.0.0.1:${server.port}/` },
        worker: { kind: 'external' },
        policy: { process: { allowExec: true, allowedCommands: ['printf ok'], maxProcesses: 0 } },
        lifecycle: { cleanupQueueOnClose: false, cleanupStateOnClose: false },
      });
      await env.close();

      expect(((body?.policy as Record<string, unknown>).process as Record<string, unknown>).maxProcesses).toBe(0);
    } finally {
      server.stop(true);
    }
  });

  test('rejects terminal envelopes with the wrong event type', async () => {
    const root = await mkdtemp(join(tmpdir(), 'executioner-js-test-'));
    cleanup.push(root);
    const queueDir = join(root, 'queue');
    const stateDir = join(root, 'state');
    const invocationId = 'js_wrong_completed_type';
    const env = await ExecutionerEnvironment.create({
      backend: { kind: 'file', queueDir },
      host: { kind: 'managed', stateDir },
      worker: { kind: 'external' },
      lifecycle: { cleanupQueueOnClose: false, cleanupStateOnClose: false },
      submitTimeoutMs: 100,
    });

    try {
      const submit = env.submit({
        invocationId,
        toolName: 'Read',
        arguments: { path: 'missing.txt' },
      }).catch((error: Error) => error);
      const pendingPath = join(queueDir, 'pending', `${invocationId}.json`);
      await waitForPath(pendingPath);
      await claimPendingInvocation(queueDir, invocationId);
      await writeFile(
        join(queueDir, 'completed', `${invocationId}.json`),
        JSON.stringify({
          type: 'tool.invocation.failed',
          invocationId,
          sessionId: env.session.id,
          result: {
            invocationId,
            sessionId: env.session.id,
            toolName: 'Read',
            status: 'success',
            output: 'wrong event type',
            error: null,
            summary: null,
            effects: [],
            durationMs: 0,
            metadata: {},
          },
          completedAt: 'now',
        }),
      );

      const error = await submit;
      expect(error).toBeInstanceOf(Error);
      expect(String((error as Error).message)).toContain('event type');
    } finally {
      await env.close();
    }
  });

  test('rejects completed envelopes without lease material', async () => {
    const root = await mkdtemp(join(tmpdir(), 'executioner-js-test-'));
    cleanup.push(root);
    const queueDir = join(root, 'queue');
    const stateDir = join(root, 'state');
    const invocationId = 'js_completed_missing_lease';
    const env = await ExecutionerEnvironment.create({
      backend: { kind: 'file', queueDir },
      host: { kind: 'managed', stateDir },
      worker: { kind: 'external' },
      lifecycle: { cleanupQueueOnClose: false, cleanupStateOnClose: false },
      submitTimeoutMs: 100,
    });

    try {
      const submit = env.submit({
        invocationId,
        toolName: 'Read',
        arguments: { path: 'missing.txt' },
      }).catch((error: Error) => error);
      const pendingPath = join(queueDir, 'pending', `${invocationId}.json`);
      await waitForPath(pendingPath);
      await claimPendingInvocation(queueDir, invocationId);
      await writeFile(
        join(queueDir, 'completed', `${invocationId}.json`),
        JSON.stringify({
          type: 'tool.invocation.completed',
          invocationId,
          sessionId: env.session.id,
          result: {
            invocationId,
            sessionId: env.session.id,
            toolName: 'Read',
            status: 'success',
            output: 'forged without a lease',
            error: null,
            summary: null,
            effects: [],
            durationMs: 0,
            metadata: {},
          },
          completedAt: 'now',
        }),
      );

      const error = await submit;
      expect(error).toBeInstanceOf(Error);
      expect(String((error as Error).message)).toContain('missing lease material');
    } finally {
      await env.close();
    }
  });

  test('rejects completed envelopes with forged lease material but no claim', async () => {
    const root = await mkdtemp(join(tmpdir(), 'executioner-js-test-'));
    cleanup.push(root);
    const queueDir = join(root, 'queue');
    const stateDir = join(root, 'state');
    const invocationId = 'js_completed_forged_orphan_lease';
    const env = await ExecutionerEnvironment.create({
      backend: { kind: 'file', queueDir },
      host: { kind: 'managed', stateDir },
      worker: { kind: 'external' },
      lifecycle: { cleanupQueueOnClose: false, cleanupStateOnClose: false },
      submitTimeoutMs: 1_000,
    });

    try {
      const submit = env.submit({
        invocationId,
        toolName: 'Read',
        arguments: { path: 'missing.txt' },
      }).catch((error: Error) => error);
      const pendingPath = join(queueDir, 'pending', `${invocationId}.json`);
      await waitForPath(pendingPath);
      await rm(pendingPath, { force: true });
      await writeFile(
        join(queueDir, 'completed', `${invocationId}.json`),
        JSON.stringify({
          type: 'tool.invocation.completed',
          invocationId,
          sessionId: env.session.id,
          attemptId: 'attempt_forged',
          leaseToken: 'lease_forged',
          result: {
            invocationId,
            sessionId: env.session.id,
            toolName: 'Read',
            status: 'success',
            output: 'forged without a claim',
            error: null,
            summary: null,
            effects: [],
            durationMs: 0,
            metadata: {},
          },
          completedAt: 'now',
        }),
      );

      const error = await submit;
      expect(error).toBeInstanceOf(Error);
      expect(String((error as Error).message)).toContain('claim');
    } finally {
      await env.close();
    }
  });

  test('rejects failed envelopes with the wrong event type', async () => {
    const root = await mkdtemp(join(tmpdir(), 'executioner-js-test-'));
    cleanup.push(root);
    const queueDir = join(root, 'queue');
    const stateDir = join(root, 'state');
    const invocationId = 'js_wrong_failed_type';
    const env = await ExecutionerEnvironment.create({
      backend: { kind: 'file', queueDir },
      host: { kind: 'managed', stateDir },
      worker: { kind: 'external' },
      lifecycle: { cleanupQueueOnClose: false, cleanupStateOnClose: false },
      submitTimeoutMs: 1_000,
    });

    try {
      const submit = env.submit({
        invocationId,
        toolName: 'Read',
        arguments: { path: 'missing.txt' },
      }).catch((error: Error) => error);
      const pendingPath = join(queueDir, 'pending', `${invocationId}.json`);
      await waitForPath(pendingPath);
      await claimPendingInvocation(queueDir, invocationId);
      await writeFile(
        join(queueDir, 'failed', `${invocationId}.json`),
        JSON.stringify({
          type: 'tool.invocation.completed',
          invocationId,
          sessionId: env.session.id,
          error: {
            code: 'wrong_type',
            message: 'wrong event type',
            retryable: false,
          },
          failedAt: 'now',
        }),
      );

      const error = await submit;
      expect(error).toBeInstanceOf(Error);
      expect(String((error as Error).message)).toContain('event type');
    } finally {
      await env.close();
    }
  });

  test('rejects failed envelopes without lease material', async () => {
    const root = await mkdtemp(join(tmpdir(), 'executioner-js-test-'));
    cleanup.push(root);
    const queueDir = join(root, 'queue');
    const stateDir = join(root, 'state');
    const invocationId = 'js_failed_missing_lease';
    const env = await ExecutionerEnvironment.create({
      backend: { kind: 'file', queueDir },
      host: { kind: 'managed', stateDir },
      worker: { kind: 'external' },
      lifecycle: { cleanupQueueOnClose: false, cleanupStateOnClose: false },
      submitTimeoutMs: 1_000,
    });

    try {
      const submit = env.submit({
        invocationId,
        toolName: 'Read',
        arguments: { path: 'missing.txt' },
      }).catch((error: Error) => error);
      const pendingPath = join(queueDir, 'pending', `${invocationId}.json`);
      await waitForPath(pendingPath);
      await claimPendingInvocation(queueDir, invocationId);
      await writeFile(
        join(queueDir, 'failed', `${invocationId}.json`),
        JSON.stringify({
          type: 'tool.invocation.failed',
          invocationId,
          sessionId: env.session.id,
          error: {
            code: 'forged',
            message: 'forged without a lease',
            retryable: false,
          },
          failedAt: 'now',
        }),
      );

      const error = await submit;
      expect(error).toBeInstanceOf(Error);
      expect(String((error as Error).message)).toContain('missing lease material');
    } finally {
      await env.close();
    }
  });

  test('rejects failed envelopes with forged lease material but no claim', async () => {
    const root = await mkdtemp(join(tmpdir(), 'executioner-js-test-'));
    cleanup.push(root);
    const queueDir = join(root, 'queue');
    const stateDir = join(root, 'state');
    const invocationId = 'js_failed_forged_orphan_lease';
    const env = await ExecutionerEnvironment.create({
      backend: { kind: 'file', queueDir },
      host: { kind: 'managed', stateDir },
      worker: { kind: 'external' },
      lifecycle: { cleanupQueueOnClose: false, cleanupStateOnClose: false },
      submitTimeoutMs: 1_000,
    });

    try {
      const submit = env.submit({
        invocationId,
        toolName: 'Read',
        arguments: { path: 'missing.txt' },
      }).catch((error: Error) => error);
      const pendingPath = join(queueDir, 'pending', `${invocationId}.json`);
      await waitForPath(pendingPath);
      await rm(pendingPath, { force: true });
      await writeFile(
        join(queueDir, 'failed', `${invocationId}.json`),
        JSON.stringify({
          type: 'tool.invocation.failed',
          invocationId,
          sessionId: env.session.id,
          attemptId: 'attempt_forged',
          leaseToken: 'lease_forged',
          error: {
            code: 'forged',
            message: 'forged without a claim',
            retryable: false,
          },
          failedAt: 'now',
        }),
      );

      const error = await submit;
      expect(error).toBeInstanceOf(Error);
      expect(String((error as Error).message)).toContain('claim');
    } finally {
      await env.close();
    }
  });

  test('quarantines terminal envelopes when claimed lease is malformed', async () => {
    for (const terminalKind of ['completed', 'failed'] as const) {
      const root = await mkdtemp(join(tmpdir(), 'executioner-js-test-'));
      cleanup.push(root);
      const queueDir = join(root, 'queue');
      const stateDir = join(root, 'state');
      const invocationId = `js_malformed_claim_${terminalKind}`;
      const env = await ExecutionerEnvironment.create({
        backend: { kind: 'file', queueDir },
        host: { kind: 'managed', stateDir },
        worker: { kind: 'external' },
        lifecycle: { cleanupQueueOnClose: false, cleanupStateOnClose: false },
        submitTimeoutMs: 100,
      });

      try {
        const submit = env.submit({
          invocationId,
          toolName: 'Read',
          arguments: { path: 'missing.txt' },
        }).catch((error: Error) => error);
        const pendingPath = join(queueDir, 'pending', `${invocationId}.json`);
        await waitForPath(pendingPath);
        await writeFile(join(queueDir, 'claimed', `${invocationId}.json`), '{not json');
        await rm(pendingPath, { force: true });
        await writeFile(
          join(queueDir, terminalKind, `${invocationId}.json`),
          JSON.stringify(terminalKind === 'completed'
            ? {
                type: 'tool.invocation.completed',
                invocationId,
                sessionId: env.session.id,
                attemptId: 'attempt',
                leaseToken: 'lease',
                result: {
                  invocationId,
                  sessionId: env.session.id,
                  toolName: 'Read',
                  status: 'success',
                  output: 'forged behind malformed claim',
                  error: null,
                  summary: null,
                  effects: [],
                  durationMs: 0,
                  metadata: {},
                },
                completedAt: 'now',
              }
            : {
                type: 'tool.invocation.failed',
                invocationId,
                sessionId: env.session.id,
                attemptId: 'attempt',
                leaseToken: 'lease',
                error: {
                  code: 'failed',
                  message: 'forged behind malformed claim',
                  retryable: false,
                },
                failedAt: 'now',
              }),
        );

        const error = await submit;
        expect(error).toBeInstanceOf(Error);
        expect(String((error as Error).message)).toContain('Timed out waiting');
        expect(existsSync(join(queueDir, terminalKind, `${invocationId}.json`))).toBe(false);
        expect((await readdir(join(queueDir, 'rejected'))).length).toBe(1);
      } finally {
        await env.close();
      }
    }
  });

  test('quarantines terminal envelopes when claimed request is malformed', async () => {
    const root = await mkdtemp(join(tmpdir(), 'executioner-js-test-'));
    cleanup.push(root);
    const queueDir = join(root, 'queue');
    const stateDir = join(root, 'state');
    const invocationId = 'js_malformed_claim_request';
    const env = await ExecutionerEnvironment.create({
      backend: { kind: 'file', queueDir },
      host: { kind: 'managed', stateDir },
      worker: { kind: 'external' },
      lifecycle: { cleanupQueueOnClose: false, cleanupStateOnClose: false },
      submitTimeoutMs: 100,
    });

    try {
      const submit = env.submit({
        invocationId,
        toolName: 'Read',
        arguments: { path: 'missing.txt' },
      }).catch((error: Error) => error);
      const pendingPath = join(queueDir, 'pending', `${invocationId}.json`);
      await waitForPath(pendingPath);
      const request = JSON.parse(await readFile(pendingPath, 'utf8'));
      await writeFile(
        join(queueDir, 'claimed', `${invocationId}.json`),
        JSON.stringify({
          workerId: 'js-test-worker',
          attemptId: 'attempt',
          leaseToken: 'lease',
          claimedAt: 'now',
          request: { ...request, padding: 'unexpected' },
        }),
      );
      await rm(pendingPath, { force: true });
      await writeFile(
        join(queueDir, 'completed', `${invocationId}.json`),
        JSON.stringify({
          type: 'tool.invocation.completed',
          invocationId,
          sessionId: env.session.id,
          attemptId: 'attempt',
          leaseToken: 'lease',
          result: {
            invocationId,
            sessionId: env.session.id,
            toolName: 'Read',
            status: 'success',
            output: 'forged behind malformed claim request',
            error: null,
            summary: null,
            effects: [],
            durationMs: 0,
            metadata: {},
          },
          completedAt: 'now',
        }),
      );

      const error = await submit;
      expect(error).toBeInstanceOf(Error);
      expect(String((error as Error).message)).toContain('Timed out waiting');
      expect(existsSync(join(queueDir, 'completed', `${invocationId}.json`))).toBe(false);
      expect((await readdir(join(queueDir, 'rejected'))).length).toBe(1);
    } finally {
      await env.close();
    }
  });

  test('rejects completed envelopes missing required result status', async () => {
    const root = await mkdtemp(join(tmpdir(), 'executioner-js-test-'));
    cleanup.push(root);
    const queueDir = join(root, 'queue');
    const stateDir = join(root, 'state');
    const invocationId = 'js_missing_result_status';
    const env = await ExecutionerEnvironment.create({
      backend: { kind: 'file', queueDir },
      host: { kind: 'managed', stateDir },
      worker: { kind: 'external' },
      lifecycle: { cleanupQueueOnClose: false, cleanupStateOnClose: false },
      submitTimeoutMs: 1_000,
    });

    try {
      const submit = env.submit({
        invocationId,
        toolName: 'Read',
        arguments: { path: 'missing.txt' },
      }).catch((error: Error) => error);
      const pendingPath = join(queueDir, 'pending', `${invocationId}.json`);
      await waitForPath(pendingPath);
      await claimPendingInvocation(queueDir, invocationId);
      await writeFile(
        join(queueDir, 'completed', `${invocationId}.json`),
        JSON.stringify({
          type: 'tool.invocation.completed',
          invocationId,
          sessionId: env.session.id,
          result: {
            invocationId,
            sessionId: env.session.id,
            toolName: 'Read',
            output: 'accepted without status',
            error: null,
            summary: null,
            effects: [],
            durationMs: 0,
            metadata: {},
          },
          completedAt: 'now',
        }),
      );

      const error = await submit;
      expect(error).toBeInstanceOf(Error);
      expect(String((error as Error).message)).toContain('status');
    } finally {
      await env.close();
    }
  });

  test('rejects completed envelopes with unknown result and effect fields', async () => {
    const root = await mkdtemp(join(tmpdir(), 'executioner-js-test-'));
    cleanup.push(root);
    const queueDir = join(root, 'queue');
    const stateDir = join(root, 'state');
    const invocationId = 'js_unknown_result_fields';
    const env = await ExecutionerEnvironment.create({
      backend: { kind: 'file', queueDir },
      host: { kind: 'managed', stateDir },
      worker: { kind: 'external' },
      lifecycle: { cleanupQueueOnClose: false, cleanupStateOnClose: false },
      submitTimeoutMs: 1_000,
    });

    try {
      const submit = env.submit({
        invocationId,
        toolName: 'Read',
        arguments: { path: 'missing.txt' },
      }).catch((error: Error) => error);
      const pendingPath = join(queueDir, 'pending', `${invocationId}.json`);
      await waitForPath(pendingPath);
      await claimPendingInvocation(queueDir, invocationId);
      await writeFile(
        join(queueDir, 'completed', `${invocationId}.json`),
        JSON.stringify({
          type: 'tool.invocation.completed',
          invocationId,
          sessionId: env.session.id,
          result: {
            invocationId,
            sessionId: env.session.id,
            toolName: 'Read',
            status: 'success',
            output: 'accepted with unknowns',
            error: null,
            summary: null,
            effects: [{
              id: 'effect',
              invocationId,
              kind: 'file.read',
              resource: {
                resourceType: 'file',
                uri: 'file:///workspace/file.txt',
                padding: 'unexpected',
              },
              operation: 'read',
              before: {
                hash: 'sha256:empty',
                metadata: {},
              },
              reversible: false,
              occurredAt: 'now',
              padding: 'unexpected',
            }],
            durationMs: 0,
            metadata: {},
            padding: 'unexpected',
          },
          completedAt: 'now',
        }),
      );

      const error = await submit;
      expect(error).toBeInstanceOf(Error);
      expect(String((error as Error).message)).toContain('unknown submit result field');
    } finally {
      await env.close();
    }
  });

  test('rejects terminal envelopes with unknown wrapper fields', async () => {
    const root = await mkdtemp(join(tmpdir(), 'executioner-js-test-'));
    cleanup.push(root);
    const queueDir = join(root, 'queue');
    const stateDir = join(root, 'state');
    const invocationId = 'js_unknown_completed_wrapper_fields';
    const env = await ExecutionerEnvironment.create({
      backend: { kind: 'file', queueDir },
      host: { kind: 'managed', stateDir },
      worker: { kind: 'external' },
      lifecycle: { cleanupQueueOnClose: false, cleanupStateOnClose: false },
      submitTimeoutMs: 1_000,
    });

    try {
      const submit = env.submit({
        invocationId,
        toolName: 'Read',
        arguments: { path: 'missing.txt' },
      }).catch((error: Error) => error);
      const pendingPath = join(queueDir, 'pending', `${invocationId}.json`);
      await waitForPath(pendingPath);
      await claimPendingInvocation(queueDir, invocationId);
      await writeFile(
        join(queueDir, 'completed', `${invocationId}.json`),
        JSON.stringify({
          type: 'tool.invocation.completed',
          invocationId,
          sessionId: env.session.id,
          result: {
            invocationId,
            sessionId: env.session.id,
            toolName: 'Read',
            status: 'success',
            output: 'accepted with wrapper padding',
            error: null,
            summary: null,
            effects: [],
            durationMs: 0,
            metadata: {},
          },
          completedAt: 'now',
          padding: 'unexpected',
        }),
      );

      const error = await submit;
      expect(error).toBeInstanceOf(Error);
      expect(String((error as Error).message)).toContain('unknown completed terminal envelope field');
    } finally {
      await env.close();
    }
  });

  test('rejects failed envelopes with malformed error payloads', async () => {
    const root = await mkdtemp(join(tmpdir(), 'executioner-js-test-'));
    cleanup.push(root);
    const queueDir = join(root, 'queue');
    const stateDir = join(root, 'state');
    const invocationId = 'js_malformed_failed_error';
    const env = await ExecutionerEnvironment.create({
      backend: { kind: 'file', queueDir },
      host: { kind: 'managed', stateDir },
      worker: { kind: 'external' },
      lifecycle: { cleanupQueueOnClose: false, cleanupStateOnClose: false },
      submitTimeoutMs: 1_000,
    });

    try {
      const submit = env.submit({
        invocationId,
        toolName: 'Read',
        arguments: { path: 'missing.txt' },
      }).catch((error: Error) => error);
      const pendingPath = join(queueDir, 'pending', `${invocationId}.json`);
      await waitForPath(pendingPath);
      await claimPendingInvocation(queueDir, invocationId);
      await writeFile(
        join(queueDir, 'failed', `${invocationId}.json`),
        JSON.stringify({
          type: 'tool.invocation.failed',
          invocationId,
          sessionId: env.session.id,
          error: 'not an error object',
          failedAt: 'now',
        }),
      );

      const error = await submit;
      expect(error).toBeInstanceOf(Error);
      expect(String((error as Error).message)).toContain('failure malformed');
    } finally {
      await env.close();
    }
  });

  test('rejects failed envelopes with unknown wrapper and error fields', async () => {
    const root = await mkdtemp(join(tmpdir(), 'executioner-js-test-'));
    cleanup.push(root);
    const queueDir = join(root, 'queue');
    const stateDir = join(root, 'state');
    const cases = [
      {
        invocationId: 'js_unknown_failed_wrapper_fields',
        event: {
          padding: 'unexpected',
          error: {
            code: 'failed',
            message: 'failed',
            retryable: false,
          },
        },
        message: 'unknown failed terminal envelope field',
      },
      {
        invocationId: 'js_unknown_failed_error_fields',
        event: {
          error: {
            code: 'failed',
            message: 'failed',
            retryable: false,
            padding: 'unexpected',
          },
        },
        message: 'unknown failed terminal error field',
      },
    ];
    const env = await ExecutionerEnvironment.create({
      backend: { kind: 'file', queueDir },
      host: { kind: 'managed', stateDir },
      worker: { kind: 'external' },
      lifecycle: { cleanupQueueOnClose: false, cleanupStateOnClose: false },
      submitTimeoutMs: 1_000,
    });

    try {
      for (const testCase of cases) {
        const submit = env.submit({
          invocationId: testCase.invocationId,
          toolName: 'Read',
          arguments: { path: 'missing.txt' },
        }).catch((error: Error) => error);
        const pendingPath = join(queueDir, 'pending', `${testCase.invocationId}.json`);
        await waitForPath(pendingPath);
        await claimPendingInvocation(queueDir, testCase.invocationId);
        await writeFile(
          join(queueDir, 'failed', `${testCase.invocationId}.json`),
          JSON.stringify({
            type: 'tool.invocation.failed',
            invocationId: testCase.invocationId,
            sessionId: env.session.id,
            failedAt: 'now',
            ...testCase.event,
          }),
        );

        const error = await submit;
        expect(error).toBeInstanceOf(Error);
        expect(String((error as Error).message)).toContain(testCase.message);
      }
    } finally {
      await env.close();
    }
  });

  test('rejects failed envelopes missing required error fields', async () => {
    const root = await mkdtemp(join(tmpdir(), 'executioner-js-test-'));
    cleanup.push(root);
    const queueDir = join(root, 'queue');
    const stateDir = join(root, 'state');
    const invocationId = 'js_failed_error_missing_code';
    const env = await ExecutionerEnvironment.create({
      backend: { kind: 'file', queueDir },
      host: { kind: 'managed', stateDir },
      worker: { kind: 'external' },
      lifecycle: { cleanupQueueOnClose: false, cleanupStateOnClose: false },
      submitTimeoutMs: 1_000,
    });

    try {
      const submit = env.submit({
        invocationId,
        toolName: 'Read',
        arguments: { path: 'missing.txt' },
      }).catch((error: Error) => error);
      const pendingPath = join(queueDir, 'pending', `${invocationId}.json`);
      await waitForPath(pendingPath);
      await claimPendingInvocation(queueDir, invocationId);
      await writeFile(
        join(queueDir, 'failed', `${invocationId}.json`),
        JSON.stringify({
          type: 'tool.invocation.failed',
          invocationId,
          sessionId: env.session.id,
          error: {
            message: 'missing code',
            retryable: false,
          },
          failedAt: 'now',
        }),
      );

      const error = await submit;
      expect(error).toBeInstanceOf(Error);
      expect(String((error as Error).message)).toContain('failure malformed');
    } finally {
      await env.close();
    }
  });

  test('listFiles prefers structured metadata entries', async () => {
    const root = await mkdtemp(join(tmpdir(), 'executioner-js-test-'));
    cleanup.push(root);
    const queueDir = join(root, 'queue');
    const stateDir = join(root, 'state');
    const env = await ExecutionerEnvironment.create({
      backend: { kind: 'file', queueDir },
      host: { kind: 'managed', stateDir },
      worker: { kind: 'external' },
      lifecycle: { cleanupQueueOnClose: false, cleanupStateOnClose: false },
      submitTimeoutMs: 1_000,
    });

    try {
      const list = env.listFiles();
      const pending = await waitForPendingInvocation(queueDir);
      await claimPendingInvocation(queueDir, pending.invocationId);
      await writeFile(
        join(queueDir, 'completed', `${pending.invocationId}.json`),
        JSON.stringify({
          type: 'tool.invocation.completed',
          invocationId: pending.invocationId,
          sessionId: env.session.id,
          attemptId: 'attempt',
          leaseToken: 'lease',
          result: {
            invocationId: pending.invocationId,
            sessionId: env.session.id,
            toolName: 'List',
            status: 'success',
            output: 'line\nbreak.txt',
            error: null,
            summary: null,
            effects: [],
            durationMs: 0,
            metadata: { entries: ['line\nbreak.txt'] },
          },
          completedAt: 'now',
        }),
      );

      await expect(list).resolves.toEqual(['line\nbreak.txt']);
    } finally {
      await env.close();
    }
  });

  test('list delegates to listFiles with cwd', async () => {
    const root = await mkdtemp(join(tmpdir(), 'executioner-js-test-'));
    cleanup.push(root);
    const queueDir = join(root, 'queue');
    const stateDir = join(root, 'state');
    const env = await ExecutionerEnvironment.create({
      backend: { kind: 'file', queueDir },
      host: { kind: 'managed', stateDir },
      worker: { kind: 'external' },
      lifecycle: { cleanupQueueOnClose: false, cleanupStateOnClose: false },
      submitTimeoutMs: 1_000,
    });

    try {
      const list = env.list({ cwd: '/workspace/src' });
      const pending = await waitForPendingInvocation(queueDir);
      expect(pending.invocationId).toBeTruthy();
      const request = JSON.parse(await readFile(join(queueDir, 'pending', `${pending.invocationId}.json`), 'utf8'));
      expect(request.toolName).toBe('List');
      expect(request.cwd).toBe('/workspace/src');
      await claimPendingInvocation(queueDir, pending.invocationId);
      await writeFile(
        join(queueDir, 'completed', `${pending.invocationId}.json`),
        JSON.stringify({
          type: 'tool.invocation.completed',
          invocationId: pending.invocationId,
          sessionId: env.session.id,
          attemptId: 'attempt',
          leaseToken: 'lease',
          result: {
            invocationId: pending.invocationId,
            sessionId: env.session.id,
            toolName: 'List',
            status: 'success',
            output: 'main.ts',
            error: null,
            summary: null,
            effects: [],
            durationMs: 0,
            metadata: { entries: ['main.ts'] },
          },
          completedAt: 'now',
        }),
      );

      await expect(list).resolves.toEqual(['main.ts']);
    } finally {
      await env.close();
    }
  });

  test('listFiles rejects malformed structured metadata entries', async () => {
    const root = await mkdtemp(join(tmpdir(), 'executioner-js-test-'));
    cleanup.push(root);
    const queueDir = join(root, 'queue');
    const stateDir = join(root, 'state');
    const env = await ExecutionerEnvironment.create({
      backend: { kind: 'file', queueDir },
      host: { kind: 'managed', stateDir },
      worker: { kind: 'external' },
      lifecycle: { cleanupQueueOnClose: false, cleanupStateOnClose: false },
      submitTimeoutMs: 1_000,
    });

    try {
      const list = env.listFiles().catch((error: Error) => error);
      const pending = await waitForPendingInvocation(queueDir);
      await claimPendingInvocation(queueDir, pending.invocationId);
      await writeFile(
        join(queueDir, 'completed', `${pending.invocationId}.json`),
        JSON.stringify({
          type: 'tool.invocation.completed',
          invocationId: pending.invocationId,
          sessionId: env.session.id,
          attemptId: 'attempt',
          leaseToken: 'lease',
          result: {
            invocationId: pending.invocationId,
            sessionId: env.session.id,
            toolName: 'List',
            status: 'success',
            output: 'fallback.txt',
            error: null,
            summary: null,
            effects: [],
            durationMs: 0,
            metadata: { entries: ['visible.txt', 42, 'hidden.txt'] },
          },
          completedAt: 'now',
        }),
      );

      const error = await list;
      expect(error).toBeInstanceOf(Error);
      expect(String((error as Error).message)).toContain('entries must be strings');
    } finally {
      await env.close();
    }
  });

  test('listFiles rejects non-array structured metadata entries', async () => {
    const root = await mkdtemp(join(tmpdir(), 'executioner-js-test-'));
    cleanup.push(root);
    const queueDir = join(root, 'queue');
    const stateDir = join(root, 'state');
    const env = await ExecutionerEnvironment.create({
      backend: { kind: 'file', queueDir },
      host: { kind: 'managed', stateDir },
      worker: { kind: 'external' },
      lifecycle: { cleanupQueueOnClose: false, cleanupStateOnClose: false },
      submitTimeoutMs: 1_000,
    });

    try {
      const list = env.listFiles().catch((error: Error) => error);
      const pending = await waitForPendingInvocation(queueDir);
      await claimPendingInvocation(queueDir, pending.invocationId);
      await writeFile(
        join(queueDir, 'completed', `${pending.invocationId}.json`),
        JSON.stringify({
          type: 'tool.invocation.completed',
          invocationId: pending.invocationId,
          sessionId: env.session.id,
          attemptId: 'attempt',
          leaseToken: 'lease',
          result: {
            invocationId: pending.invocationId,
            sessionId: env.session.id,
            toolName: 'List',
            status: 'success',
            output: 'fallback.txt',
            error: null,
            summary: null,
            effects: [],
            durationMs: 0,
            metadata: { entries: 'visible.txt' },
          },
          completedAt: 'now',
        }),
      );

      const error = await list;
      expect(error).toBeInstanceOf(Error);
      expect(String((error as Error).message)).toContain('entries must be an array');
    } finally {
      await env.close();
    }
  });

  test('listFiles rejects truncated structured metadata entries', async () => {
    const root = await mkdtemp(join(tmpdir(), 'executioner-js-test-'));
    cleanup.push(root);
    const queueDir = join(root, 'queue');
    const stateDir = join(root, 'state');
    const env = await ExecutionerEnvironment.create({
      backend: { kind: 'file', queueDir },
      host: { kind: 'managed', stateDir },
      worker: { kind: 'external' },
      lifecycle: { cleanupQueueOnClose: false, cleanupStateOnClose: false },
      submitTimeoutMs: 1_000,
    });

    try {
      const list = env.listFiles().catch((error: Error) => error);
      const pending = await waitForPendingInvocation(queueDir);
      await claimPendingInvocation(queueDir, pending.invocationId);
      await writeFile(
        join(queueDir, 'completed', `${pending.invocationId}.json`),
        JSON.stringify({
          type: 'tool.invocation.completed',
          invocationId: pending.invocationId,
          sessionId: env.session.id,
          attemptId: 'attempt',
          leaseToken: 'lease',
          result: {
            invocationId: pending.invocationId,
            sessionId: env.session.id,
            toolName: 'List',
            status: 'success',
            output: 'visible.txt\n...[truncated at 1000 entries, 1005 total]',
            error: null,
            summary: null,
            effects: [],
            durationMs: 0,
            metadata: { entries: ['visible.txt'], truncated: true },
          },
          completedAt: 'now',
        }),
      );

      const error = await list;
      expect(error).toBeInstanceOf(Error);
      expect(String((error as Error).message)).toContain('truncated');
    } finally {
      await env.close();
    }
  });

  test('listFiles rejects malformed truncated metadata', async () => {
    const root = await mkdtemp(join(tmpdir(), 'executioner-js-test-'));
    cleanup.push(root);
    const queueDir = join(root, 'queue');
    const stateDir = join(root, 'state');
    const env = await ExecutionerEnvironment.create({
      backend: { kind: 'file', queueDir },
      host: { kind: 'managed', stateDir },
      worker: { kind: 'external' },
      lifecycle: { cleanupQueueOnClose: false, cleanupStateOnClose: false },
      submitTimeoutMs: 1_000,
    });

    try {
      const list = env.listFiles().catch((error: Error) => error);
      const pending = await waitForPendingInvocation(queueDir);
      await claimPendingInvocation(queueDir, pending.invocationId);
      await writeFile(
        join(queueDir, 'completed', `${pending.invocationId}.json`),
        JSON.stringify({
          type: 'tool.invocation.completed',
          invocationId: pending.invocationId,
          sessionId: env.session.id,
          attemptId: 'attempt',
          leaseToken: 'lease',
          result: {
            invocationId: pending.invocationId,
            sessionId: env.session.id,
            toolName: 'List',
            status: 'success',
            output: 'visible.txt',
            error: null,
            summary: null,
            effects: [],
            durationMs: 0,
            metadata: { entries: ['visible.txt'], truncated: 'true' },
          },
          completedAt: 'now',
        }),
      );

      const error = await list;
      expect(error).toBeInstanceOf(Error);
      expect(String((error as Error).message)).toContain('truncated metadata must be a boolean');
    } finally {
      await env.close();
    }
  });

  test('listFiles rejects truncated output fallback', async () => {
    const root = await mkdtemp(join(tmpdir(), 'executioner-js-test-'));
    cleanup.push(root);
    const queueDir = join(root, 'queue');
    const stateDir = join(root, 'state');
    const env = await ExecutionerEnvironment.create({
      backend: { kind: 'file', queueDir },
      host: { kind: 'managed', stateDir },
      worker: { kind: 'external' },
      lifecycle: { cleanupQueueOnClose: false, cleanupStateOnClose: false },
      submitTimeoutMs: 1_000,
    });

    try {
      const list = env.listFiles().catch((error: Error) => error);
      const pending = await waitForPendingInvocation(queueDir);
      await claimPendingInvocation(queueDir, pending.invocationId);
      await writeFile(
        join(queueDir, 'completed', `${pending.invocationId}.json`),
        JSON.stringify({
          type: 'tool.invocation.completed',
          invocationId: pending.invocationId,
          sessionId: env.session.id,
          attemptId: 'attempt',
          leaseToken: 'lease',
          result: {
            invocationId: pending.invocationId,
            sessionId: env.session.id,
            toolName: 'List',
            status: 'success',
            output: 'visible.txt\n...[truncated at 1000 entries, 1005 total]',
            error: null,
            summary: null,
            effects: [],
            durationMs: 0,
            metadata: {},
          },
          completedAt: 'now',
        }),
      );

      const error = await list;
      expect(error).toBeInstanceOf(Error);
      expect(String((error as Error).message)).toContain('truncated');
    } finally {
      await env.close();
    }
  });

  test('listFiles fallback preserves filenames that look like old empty-list messages', async () => {
    const root = await mkdtemp(join(tmpdir(), 'executioner-js-test-'));
    cleanup.push(root);
    const queueDir = join(root, 'queue');
    const stateDir = join(root, 'state');
    const env = await ExecutionerEnvironment.create({
      backend: { kind: 'file', queueDir },
      host: { kind: 'managed', stateDir },
      worker: { kind: 'external' },
      lifecycle: { cleanupQueueOnClose: false, cleanupStateOnClose: false },
      submitTimeoutMs: 1_000,
    });

    try {
      const list = env.listFiles();
      const pending = await waitForPendingInvocation(queueDir);
      await claimPendingInvocation(queueDir, pending.invocationId);
      await writeFile(
        join(queueDir, 'completed', `${pending.invocationId}.json`),
        JSON.stringify({
          type: 'tool.invocation.completed',
          invocationId: pending.invocationId,
          sessionId: env.session.id,
          attemptId: 'attempt',
          leaseToken: 'lease',
          result: {
            invocationId: pending.invocationId,
            sessionId: env.session.id,
            toolName: 'List',
            status: 'success',
            output: 'No files found matching pattern: notes.txt',
            error: null,
            summary: null,
            effects: [],
            durationMs: 0,
            metadata: {},
          },
          completedAt: 'now',
        }),
      );

      await expect(list).resolves.toEqual(['No files found matching pattern: notes.txt']);
    } finally {
      await env.close();
    }
  });

  test('listFiles rejects negative protocol durations', async () => {
    const root = await mkdtemp(join(tmpdir(), 'executioner-js-test-'));
    cleanup.push(root);
    const queueDir = join(root, 'queue');
    const stateDir = join(root, 'state');
    const env = await ExecutionerEnvironment.create({
      backend: { kind: 'file', queueDir },
      host: { kind: 'managed', stateDir },
      worker: { kind: 'external' },
      lifecycle: { cleanupQueueOnClose: false, cleanupStateOnClose: false },
      submitTimeoutMs: 1_000,
    });

    try {
      const list = env.listFiles().catch((error: Error) => error);
      const pending = await waitForPendingInvocation(queueDir);
      await claimPendingInvocation(queueDir, pending.invocationId);
      await writeFile(
        join(queueDir, 'completed', `${pending.invocationId}.json`),
        JSON.stringify({
          type: 'tool.invocation.completed',
          invocationId: pending.invocationId,
          sessionId: env.session.id,
          attemptId: 'attempt',
          leaseToken: 'lease',
          result: {
            invocationId: pending.invocationId,
            sessionId: env.session.id,
            toolName: 'List',
            status: 'success',
            output: 'visible.txt',
            error: null,
            summary: null,
            effects: [],
            durationMs: -1,
            metadata: { entries: ['visible.txt'] },
          },
          completedAt: 'now',
        }),
      );

      const error = await list;
      expect(error).toBeInstanceOf(Error);
      expect(String((error as Error).message)).toContain('durationMs must be non-negative');
    } finally {
      await env.close();
    }
  });

  test('accepts protocol eventType terminal envelopes', async () => {
    const root = await mkdtemp(join(tmpdir(), 'executioner-js-test-'));
    cleanup.push(root);
    const queueDir = join(root, 'queue');
    const stateDir = join(root, 'state');
    const env = await ExecutionerEnvironment.create({
      backend: { kind: 'file', queueDir },
      host: { kind: 'managed', stateDir },
      worker: { kind: 'external' },
      lifecycle: { cleanupQueueOnClose: false, cleanupStateOnClose: false },
      submitTimeoutMs: 1_000,
    });

    try {
      const list = env.listFiles();
      const pending = await waitForPendingInvocation(queueDir);
      await claimPendingInvocation(queueDir, pending.invocationId);
      await writeFile(
        join(queueDir, 'completed', `${pending.invocationId}.json`),
        JSON.stringify({
          eventType: 'tool.invocation.completed',
          invocationId: pending.invocationId,
          sessionId: env.session.id,
          attemptId: 'attempt',
          leaseToken: 'lease',
          result: {
            invocationId: pending.invocationId,
            sessionId: env.session.id,
            toolName: 'List',
            status: 'success',
            output: 'from-rust-worker.txt',
            error: null,
            summary: null,
            effects: [],
            durationMs: 0,
            metadata: { entries: ['from-rust-worker.txt'] },
          },
          completedAt: 'now',
        }),
      );

      await expect(list).resolves.toEqual(['from-rust-worker.txt']);
    } finally {
      await env.close();
    }
  });

  test('create rejects invalid returned session id', async () => {
    const root = await mkdtemp(join(tmpdir(), 'executioner-js-test-'));
    cleanup.push(root);
    const queueDir = join(root, 'queue');
    const server = Bun.serve({
      port: 0,
      fetch(request) {
        const url = new URL(request.url);
        if (url.pathname === '/sessions') {
          return Response.json({
            session: {
              id: '../escaped',
              state: 'ready',
              workspace: {
                root: '/tmp/workspace',
                logicalRoot: '/workspace',
                mode: 'new',
                fresh: true,
                managed: true,
              },
              createdAt: 'now',
            },
          });
        }
        return new Response('not found', { status: 404 });
      },
    });

    try {
      await expect(ExecutionerEnvironment.create({
        backend: { kind: 'file', queueDir },
        host: { kind: 'http', baseUrl: `http://127.0.0.1:${server.port}/` },
        worker: { kind: 'external' },
        lifecycle: { cleanupQueueOnClose: false, cleanupStateOnClose: false },
      })).rejects.toThrow('Invalid session id');
    } finally {
      server.stop(true);
    }
  });

  test('http error body is capped', async () => {
    const root = await mkdtemp(join(tmpdir(), 'executioner-js-test-'));
    cleanup.push(root);
    const queueDir = join(root, 'queue');
    const server = Bun.serve({
      port: 0,
      fetch(request) {
        const url = new URL(request.url);
        if (url.pathname === '/sessions') {
          return new Response('x'.repeat(256 * 1024), { status: 500 });
        }
        return new Response('not found', { status: 404 });
      },
    });

    try {
      const error = await ExecutionerEnvironment.create({
        backend: { kind: 'file', queueDir },
        host: { kind: 'http', baseUrl: `http://127.0.0.1:${server.port}/` },
        worker: { kind: 'external' },
        lifecycle: { cleanupQueueOnClose: false, cleanupStateOnClose: false },
      }).catch((caught: Error) => caught);

      expect(error).toBeInstanceOf(Error);
      expect(String((error as Error).message).length).toBeLessThan(80 * 1024);
      expect(String((error as Error).message)).toContain('truncated');
    } finally {
      server.stop(true);
    }
  });

  test('http success body is capped', async () => {
    const root = await mkdtemp(join(tmpdir(), 'executioner-js-test-'));
    cleanup.push(root);
    const queueDir = join(root, 'queue');
    const server = Bun.serve({
      port: 0,
      fetch(request) {
        const url = new URL(request.url);
        if (url.pathname === '/sessions') {
          return Response.json({ padding: 'x'.repeat(11 * 1024 * 1024) });
        }
        return new Response('not found', { status: 404 });
      },
    });

    try {
      await expect(ExecutionerEnvironment.create({
        backend: { kind: 'file', queueDir },
        host: { kind: 'http', baseUrl: `http://127.0.0.1:${server.port}/` },
        worker: { kind: 'external' },
        lifecycle: { cleanupQueueOnClose: false, cleanupStateOnClose: false },
      })).rejects.toThrow('response body exceeds');
    } finally {
      server.stop(true);
    }
  });

  test('http client does not follow redirects with request body', async () => {
    const root = await mkdtemp(join(tmpdir(), 'executioner-js-test-'));
    cleanup.push(root);
    const queueDir = join(root, 'queue');
    let captured = false;
    const captureServer = Bun.serve({
      port: 0,
      fetch() {
        captured = true;
        return Response.json({
          session: {
            id: 'sess_redirected',
            state: 'ready',
            workspace: {
              root: '/tmp/workspace',
              logicalRoot: '/workspace',
              mode: 'new',
              fresh: true,
              managed: true,
            },
            createdAt: 'now',
            metadata: {},
          },
        });
      },
    });
    const redirectServer = Bun.serve({
      port: 0,
      fetch(request) {
        const url = new URL(request.url);
        if (url.pathname === '/sessions') {
          return new Response(null, {
            status: 307,
            headers: { location: `http://127.0.0.1:${captureServer.port}/capture` },
          });
        }
        return new Response('not found', { status: 404 });
      },
    });

    try {
      await expect(ExecutionerEnvironment.create({
        backend: { kind: 'file', queueDir },
        host: { kind: 'http', baseUrl: `http://127.0.0.1:${redirectServer.port}/` },
        worker: { kind: 'external' },
        lifecycle: { cleanupQueueOnClose: false, cleanupStateOnClose: false },
      })).rejects.toThrow();
      await Bun.sleep(50);
      expect(captured).toBe(false);
    } finally {
      redirectServer.stop(true);
      captureServer.stop(true);
    }
  });

  test('create rejects malformed session booleans instead of coercing', async () => {
    const root = await mkdtemp(join(tmpdir(), 'executioner-js-test-'));
    cleanup.push(root);
    const queueDir = join(root, 'queue');
    const server = Bun.serve({
      port: 0,
      fetch(request) {
        const url = new URL(request.url);
        if (url.pathname === '/sessions') {
          return Response.json({
            session: {
              id: 'sess_malformed_bool',
              state: 'ready',
              workspace: {
                root: '/tmp/workspace',
                logicalRoot: '/workspace',
                mode: 'new',
                fresh: 'false',
                managed: true,
              },
              createdAt: 'now',
              metadata: {},
            },
          });
        }
        return new Response('not found', { status: 404 });
      },
    });

    try {
      await expect(ExecutionerEnvironment.create({
        backend: { kind: 'file', queueDir },
        host: { kind: 'http', baseUrl: `http://127.0.0.1:${server.port}/` },
        worker: { kind: 'external' },
        lifecycle: { cleanupQueueOnClose: false, cleanupStateOnClose: false },
      })).rejects.toThrow('workspace fresh');
    } finally {
      server.stop(true);
    }
  });

  test('create rejects missing required session fields instead of defaulting', async () => {
    const root = await mkdtemp(join(tmpdir(), 'executioner-js-test-'));
    cleanup.push(root);
    const queueDir = join(root, 'queue');
    const server = Bun.serve({
      port: 0,
      fetch(request) {
        const url = new URL(request.url);
        if (url.pathname === '/sessions') {
          return Response.json({
            session: {
              id: 'sess_missing_workspace_root',
              state: 'ready',
              workspace: {
                logicalRoot: '/workspace',
                mode: 'new',
                fresh: true,
                managed: true,
              },
              createdAt: 'now',
              metadata: {},
            },
          });
        }
        return new Response('not found', { status: 404 });
      },
    });

    try {
      await expect(ExecutionerEnvironment.create({
        backend: { kind: 'file', queueDir },
        host: { kind: 'http', baseUrl: `http://127.0.0.1:${server.port}/` },
        worker: { kind: 'external' },
        lifecycle: { cleanupQueueOnClose: false, cleanupStateOnClose: false },
      })).rejects.toThrow('workspace root is required');
    } finally {
      server.stop(true);
    }
  });

  test('create rejects unknown session response fields', async () => {
    const root = await mkdtemp(join(tmpdir(), 'executioner-js-test-'));
    cleanup.push(root);
    const baseSession = {
      id: 'sess_unknown_fields',
      state: 'ready',
      workspace: {
        root: '/tmp/workspace',
        logicalRoot: '/workspace',
        mode: 'new',
        fresh: true,
        managed: true,
      },
      createdAt: 'now',
      metadata: {},
    };
    const cases = [
      {
        name: 'response',
        payload: { session: baseSession, padding: 'unexpected' },
        message: 'unknown create session response field',
      },
      {
        name: 'session',
        payload: { session: { ...baseSession, padding: 'unexpected' } },
        message: 'unknown session field',
      },
      {
        name: 'workspace',
        payload: {
          session: {
            ...baseSession,
            workspace: { ...baseSession.workspace, padding: 'unexpected' },
          },
        },
        message: 'unknown session workspace field',
      },
    ];

    for (const testCase of cases) {
      const queueDir = join(root, `queue-${testCase.name}`);
      const server = Bun.serve({
        port: 0,
        fetch(request) {
          const url = new URL(request.url);
          if (url.pathname === '/sessions') {
            return Response.json(testCase.payload);
          }
          return new Response('not found', { status: 404 });
        },
      });

      try {
        await expect(ExecutionerEnvironment.create({
          backend: { kind: 'file', queueDir },
          host: { kind: 'http', baseUrl: `http://127.0.0.1:${server.port}/` },
          worker: { kind: 'external' },
          lifecycle: { cleanupQueueOnClose: false, cleanupStateOnClose: false },
        })).rejects.toThrow(testCase.message);
      } finally {
        server.stop(true);
      }
    }
  });

  test('create rejects unknown session enum values', async () => {
    const root = await mkdtemp(join(tmpdir(), 'executioner-js-test-'));
    cleanup.push(root);
    const baseSession = {
      id: 'sess_unknown_enum',
      state: 'ready',
      workspace: {
        root: '/tmp/workspace',
        logicalRoot: '/workspace',
        mode: 'new',
        fresh: true,
        managed: true,
      },
      createdAt: 'now',
      metadata: {},
    };
    const cases = [
      {
        name: 'state',
        payload: { session: { ...baseSession, state: 'rooted' } },
        message: 'unknown session state',
      },
      {
        name: 'mode',
        payload: {
          session: {
            ...baseSession,
            workspace: { ...baseSession.workspace, mode: 'mounted' },
          },
        },
        message: 'unknown workspace mode',
      },
    ];

    for (const testCase of cases) {
      const queueDir = join(root, `queue-${testCase.name}`);
      const server = Bun.serve({
        port: 0,
        fetch(request) {
          const url = new URL(request.url);
          if (url.pathname === '/sessions') {
            return Response.json(testCase.payload);
          }
          return new Response('not found', { status: 404 });
        },
      });

      try {
        await expect(ExecutionerEnvironment.create({
          backend: { kind: 'file', queueDir },
          host: { kind: 'http', baseUrl: `http://127.0.0.1:${server.port}/` },
          worker: { kind: 'external' },
          lifecycle: { cleanupQueueOnClose: false, cleanupStateOnClose: false },
        })).rejects.toThrow(testCase.message);
      } finally {
        server.stop(true);
      }
    }
  });

  test('listFiles rejects unsuccessful results', async () => {
    const root = await mkdtemp(join(tmpdir(), 'executioner-js-test-'));
    cleanup.push(root);
    const queueDir = join(root, 'queue');
    const stateDir = join(root, 'state');
    const env = await ExecutionerEnvironment.create({
      backend: { kind: 'file', queueDir },
      host: { kind: 'managed', stateDir },
      worker: { kind: 'external' },
      lifecycle: { cleanupQueueOnClose: false, cleanupStateOnClose: false },
      submitTimeoutMs: 1_000,
    });

    try {
      const list = env.listFiles().catch((error: Error) => error);
      const pending = await waitForPendingInvocation(queueDir);
      await claimPendingInvocation(queueDir, pending.invocationId);
      await writeFile(
        join(queueDir, 'completed', `${pending.invocationId}.json`),
        JSON.stringify({
          type: 'tool.invocation.completed',
          invocationId: pending.invocationId,
          sessionId: env.session.id,
          attemptId: 'attempt',
          leaseToken: 'lease',
          result: {
            invocationId: pending.invocationId,
            sessionId: env.session.id,
            toolName: 'List',
            status: 'policy_denied',
            output: '',
            error: 'Read denied for /workspace',
            summary: null,
            effects: [],
            durationMs: 0,
            metadata: {},
          },
          completedAt: 'now',
        }),
      );

      const error = await list;
      expect(error).toBeInstanceOf(Error);
      expect(String((error as Error).message)).toContain('List failed');
    } finally {
      await env.close();
    }
  });

  test('listFiles rejects unknown options without writing queue entry', async () => {
    const root = await mkdtemp(join(tmpdir(), 'executioner-js-test-'));
    cleanup.push(root);
    const queueDir = join(root, 'queue');
    const stateDir = join(root, 'state');
    const env = await ExecutionerEnvironment.create({
      backend: { kind: 'file', queueDir },
      host: { kind: 'managed', stateDir },
      worker: { kind: 'external' },
      lifecycle: { cleanupQueueOnClose: false, cleanupStateOnClose: false },
      submitTimeoutMs: 50,
    });

    try {
      await expect(env.listFiles({
        cwd: '/workspace',
        recursive: true,
      } as unknown as ListFilesOptions)).rejects.toThrow('unknown listFiles options field');
      expect(await readdir(join(queueDir, 'pending'))).toEqual([]);
    } finally {
      await env.close();
    }
  });

  test('listFiles quarantines symlink terminal files without following them', async () => {
    const root = await mkdtemp(join(tmpdir(), 'executioner-js-test-'));
    cleanup.push(root);
    const queueDir = join(root, 'queue');
    const stateDir = join(root, 'state');
    const outsideCompleted = join(root, 'outside-completed.json');
    const env = await ExecutionerEnvironment.create({
      backend: { kind: 'file', queueDir },
      host: { kind: 'managed', stateDir },
      worker: { kind: 'external' },
      lifecycle: { cleanupQueueOnClose: false, cleanupStateOnClose: false },
      submitTimeoutMs: 50,
    });

    try {
      const list = env.listFiles().catch((error: Error) => error);
      const pending = await waitForPendingInvocation(queueDir);
      await claimPendingInvocation(queueDir, pending.invocationId);
      await writeFile(
        outsideCompleted,
        JSON.stringify({
          type: 'tool.invocation.completed',
          invocationId: pending.invocationId,
          sessionId: env.session.id,
          result: {
            invocationId: pending.invocationId,
            sessionId: env.session.id,
            toolName: 'List',
            status: 'success',
            output: 'forged outside queue',
            error: null,
            summary: null,
            effects: [],
            durationMs: 0,
            metadata: { entries: ['forged.txt'] },
          },
          completedAt: 'now',
        }),
      );
      await symlink(outsideCompleted, join(queueDir, 'completed', `${pending.invocationId}.json`));

      const error = await list;
      expect(error).toBeInstanceOf(Error);
      expect(String((error as Error).message)).toContain('Timed out');
      expect(existsSync(outsideCompleted)).toBe(true);
      expect(existsSync(join(queueDir, 'completed', `${pending.invocationId}.json`))).toBe(false);
      expect((await readdir(join(queueDir, 'rejected'))).length).toBe(1);
    } finally {
      await env.close();
    }
  });

  test('listFiles quarantines completed results for the wrong toolName', async () => {
    const root = await mkdtemp(join(tmpdir(), 'executioner-js-test-'));
    cleanup.push(root);
    const queueDir = join(root, 'queue');
    const stateDir = join(root, 'state');
    const env = await ExecutionerEnvironment.create({
      backend: { kind: 'file', queueDir },
      host: { kind: 'managed', stateDir },
      worker: { kind: 'external' },
      lifecycle: { cleanupQueueOnClose: false, cleanupStateOnClose: false },
      submitTimeoutMs: 100,
    });

    try {
      const list = env.listFiles().catch((error: Error) => error);
      const pending = await waitForPendingInvocation(queueDir);
      await claimPendingInvocation(queueDir, pending.invocationId);
      await writeFile(
        join(queueDir, 'completed', `${pending.invocationId}.json`),
        JSON.stringify({
          type: 'tool.invocation.completed',
          invocationId: pending.invocationId,
          sessionId: env.session.id,
          attemptId: 'attempt',
          leaseToken: 'lease',
          result: {
            invocationId: pending.invocationId,
            sessionId: env.session.id,
            toolName: 'Read',
            status: 'success',
            output: 'forged.txt',
            error: null,
            summary: null,
            effects: [],
            durationMs: 0,
            metadata: { entries: ['forged.txt'] },
          },
          completedAt: 'now',
        }),
      );

      const error = await list;
      expect(error).toBeInstanceOf(Error);
      expect(String((error as Error).message)).toContain('Timed out waiting');
      expect(existsSync(join(queueDir, 'completed', `${pending.invocationId}.json`))).toBe(false);
      expect((await readdir(join(queueDir, 'rejected'))).length).toBe(1);
    } finally {
      await env.close();
    }
  });

  test('submit rejects empty toolName without writing queue entry', async () => {
    const root = await mkdtemp(join(tmpdir(), 'executioner-js-test-'));
    cleanup.push(root);
    const queueDir = join(root, 'queue');
    const stateDir = join(root, 'state');
    const env = await ExecutionerEnvironment.create({
      backend: { kind: 'file', queueDir },
      host: { kind: 'managed', stateDir },
      worker: { kind: 'external' },
      lifecycle: { cleanupQueueOnClose: false, cleanupStateOnClose: false },
      submitTimeoutMs: 1_000,
    });

    try {
      await expect(env.submit({
        invocationId: 'js_empty_tool',
        toolName: '',
        arguments: {},
      })).rejects.toThrow('toolName must be a non-empty string');
      expect(existsSync(join(queueDir, 'pending', 'js_empty_tool.json'))).toBe(false);
    } finally {
      await env.close();
    }
  });

  test('submit rejects malformed protocol options without writing queue entry', async () => {
    const root = await mkdtemp(join(tmpdir(), 'executioner-js-test-'));
    cleanup.push(root);
    const queueDir = join(root, 'queue');
    const stateDir = join(root, 'state');
    const env = await ExecutionerEnvironment.create({
      backend: { kind: 'file', queueDir },
      host: { kind: 'managed', stateDir },
      worker: { kind: 'external' },
      lifecycle: { cleanupQueueOnClose: false, cleanupStateOnClose: false },
      submitTimeoutMs: 50,
    });

    try {
      await expect(env.submit({
        invocationId: 'js_bad_cwd',
        toolName: 'Read',
        arguments: { path: 'missing.txt' },
        cwd: 42 as unknown as string,
      })).rejects.toThrow('cwd must be a string');
      await expect(env.submit({
        invocationId: 'js_bad_metadata',
        toolName: 'Read',
        arguments: { path: 'missing.txt' },
        metadata: 'not metadata' as unknown as Record<string, unknown>,
      })).rejects.toThrow('metadata must be a JSON object');
      await expect(env.submit({
        invocationId: 'js_bad_timeout',
        toolName: 'Read',
        arguments: { path: 'missing.txt' },
        timeoutMs: -1,
      })).rejects.toThrow('timeoutMs must be non-negative');
      await expect(env.submit({
        invocationId: 'js_zero_timeout',
        toolName: 'Read',
        arguments: { path: 'missing.txt' },
        timeoutMs: 0,
      })).rejects.toThrow('timeoutMs must be positive');
      await expect(env.submit({
        invocationId: 'js_bad_timeout_cap',
        toolName: 'Read',
        arguments: { path: 'missing.txt' },
        timeoutMs: 60 * 60 * 1000 + 1,
      })).rejects.toThrow('timeoutMs exceeds maximum supported tool timeout');
      await expect(env.submit({
        invocationId: 'js_bad_output_limit',
        toolName: 'Read',
        arguments: { path: 'missing.txt' },
        maxOutputBytes: 10 * 1024 * 1024 + 1,
      })).rejects.toThrow('maxOutputBytes exceeds maximum supported output size');
      await expect(env.submit({
        invocationId: 'js_bad_unknown',
        toolName: 'Read',
        arguments: { path: 'missing.txt' },
        requiredCapabilities: [{ kind: 'file.read' }],
      } as unknown as ToolCall)).rejects.toThrow('unknown tool call field');
      await expect(env.submit({
        invocationId: 'js_oversized_request',
        toolName: 'Read',
        arguments: { path: 'missing.txt' },
        metadata: { padding: 'x'.repeat(1024 * 1024) },
      })).rejects.toThrow('tool invocation request exceeds maximum JSON size');

      expect(existsSync(join(queueDir, 'pending', 'js_bad_cwd.json'))).toBe(false);
      expect(existsSync(join(queueDir, 'pending', 'js_bad_metadata.json'))).toBe(false);
      expect(existsSync(join(queueDir, 'pending', 'js_bad_timeout.json'))).toBe(false);
      expect(existsSync(join(queueDir, 'pending', 'js_zero_timeout.json'))).toBe(false);
      expect(existsSync(join(queueDir, 'pending', 'js_bad_timeout_cap.json'))).toBe(false);
      expect(existsSync(join(queueDir, 'pending', 'js_bad_output_limit.json'))).toBe(false);
      expect(existsSync(join(queueDir, 'pending', 'js_bad_unknown.json'))).toBe(false);
      expect(existsSync(join(queueDir, 'pending', 'js_oversized_request.json'))).toBe(false);
    } finally {
      await env.close();
    }
  });

  test('submit rejects swapped queue state directory without writing through it', async () => {
    const root = await mkdtemp(join(tmpdir(), 'executioner-js-test-'));
    cleanup.push(root);
    const queueDir = join(root, 'queue');
    const outsidePending = join(root, 'outside-pending');
    const server = Bun.serve({
      port: 0,
      fetch(request) {
        const url = new URL(request.url);
        if (request.method === 'POST' && url.pathname === '/sessions') {
          return Response.json({
            session: {
              id: 'sess_swapped_queue',
              state: 'ready',
              workspace: {
                root: '/tmp/workspace',
                logicalRoot: '/workspace',
                mode: 'new',
                fresh: true,
                managed: true,
              },
              createdAt: 'now',
            },
          });
        }
        if (request.method === 'DELETE' && url.pathname === '/sessions/sess_swapped_queue') {
          return Response.json({
            session: {
              id: 'sess_swapped_queue',
              state: 'destroyed',
              workspace: {
                root: '/tmp/workspace',
                logicalRoot: '/workspace',
                mode: 'new',
                fresh: true,
                managed: true,
              },
              createdAt: 'now',
            },
          });
        }
        return new Response('not found', { status: 404 });
      },
    });
    const env = await ExecutionerEnvironment.create({
      backend: { kind: 'file', queueDir },
      host: { kind: 'http', baseUrl: `http://127.0.0.1:${server.port}/` },
      worker: { kind: 'external' },
      lifecycle: { cleanupQueueOnClose: false, cleanupStateOnClose: false },
      submitTimeoutMs: 50,
    });

    try {
      await mkdir(outsidePending);
      await rm(join(queueDir, 'pending'), { recursive: true, force: true });
      await symlink(outsidePending, join(queueDir, 'pending'));

      await expect(env.submit({
        invocationId: 'js_swapped_pending',
        toolName: 'Read',
        arguments: { path: 'missing.txt' },
      })).rejects.toThrow('queue state directory');
      expect(existsSync(join(outsidePending, 'js_swapped_pending.json'))).toBe(false);
    } finally {
      await env.close().catch(() => undefined);
      server.stop(true);
    }
  });

  test('close cleans queue when destroy fails', async () => {
    const root = await mkdtemp(join(tmpdir(), 'executioner-js-test-'));
    cleanup.push(root);
    const queueDir = join(root, 'queue');
    const server = Bun.serve({
      port: 0,
      fetch(request) {
        const url = new URL(request.url);
        if (request.method === 'POST' && url.pathname === '/sessions') {
          return Response.json({
            session: {
              id: 'sess_close_failure',
              state: 'ready',
              workspace: {
                root: '/tmp/workspace',
                logicalRoot: '/workspace',
                mode: 'new',
                fresh: true,
                managed: true,
              },
              createdAt: 'now',
            },
          });
        }
        if (request.method === 'DELETE' && url.pathname === '/sessions/sess_close_failure') {
          return new Response('destroy failed', { status: 500 });
        }
        return new Response('not found', { status: 404 });
      },
    });
    const env = await ExecutionerEnvironment.create({
      backend: { kind: 'file', queueDir },
      host: { kind: 'http', baseUrl: `http://127.0.0.1:${server.port}/` },
      worker: { kind: 'external' },
      lifecycle: { cleanupQueueOnClose: true, cleanupStateOnClose: false },
    });

    try {
      await expect(env.close()).rejects.toThrow('host returned 500');
      expect(existsSync(queueDir)).toBe(false);
    } finally {
      server.stop(true);
    }
  });

  test('create destroys session and cleans queue when managed worker spawn throws', async () => {
    const root = await mkdtemp(join(tmpdir(), 'executioner-js-test-'));
    cleanup.push(root);
    const queueDir = join(root, 'queue');
    const events: string[] = [];
    const server = Bun.serve({
      port: 0,
      fetch(request) {
        const url = new URL(request.url);
        if (request.method === 'POST' && url.pathname === '/sessions') {
          events.push('create');
          return Response.json({
            session: {
              id: 'sess_partial_worker_spawn',
              state: 'ready',
              workspace: {
                root: '/tmp/workspace',
                logicalRoot: '/workspace',
                mode: 'new',
                fresh: true,
                managed: true,
              },
              createdAt: 'now',
              metadata: {},
            },
          });
        }
        if (request.method === 'DELETE' && url.pathname === '/sessions/sess_partial_worker_spawn') {
          events.push('destroy');
          return Response.json({
            id: 'sess_partial_worker_spawn',
            state: 'destroyed',
            workspace: {
              root: '/tmp/workspace',
              logicalRoot: '/workspace',
              mode: 'new',
              fresh: true,
              managed: true,
            },
            createdAt: 'now',
            metadata: {},
          });
        }
        return new Response('not found', { status: 404 });
      },
    });

    try {
      await expect(ExecutionerEnvironment.create({
        binaryPath: 'bad\0binary',
        backend: { kind: 'file', queueDir },
        host: { kind: 'http', baseUrl: `http://127.0.0.1:${server.port}/` },
        worker: { kind: 'managed', id: 'worker', idleSleepMs: 1 },
        lifecycle: { cleanupQueueOnClose: true, cleanupStateOnClose: false },
      })).rejects.toThrow();

      expect(events).toEqual(['create', 'destroy']);
      expect(existsSync(queueDir)).toBe(false);
    } finally {
      server.stop(true);
    }
  });

  test('close preserves preexisting queue root contents', async () => {
    const root = await mkdtemp(join(tmpdir(), 'executioner-js-test-'));
    cleanup.push(root);
    const queueDir = join(root, 'queue');
    await mkdir(queueDir);
    await writeFile(join(queueDir, 'sentinel.txt'), 'do not delete');
    const server = Bun.serve({
      port: 0,
      fetch(request) {
        const url = new URL(request.url);
        if (request.method === 'POST' && url.pathname === '/sessions') {
          return Response.json({
            session: {
              id: 'sess_queue_preserve',
              state: 'ready',
              workspace: {
                root: '/tmp/workspace',
                logicalRoot: '/workspace',
                mode: 'new',
                fresh: true,
                managed: true,
              },
              createdAt: 'now',
            },
          });
        }
        if (request.method === 'DELETE' && url.pathname === '/sessions/sess_queue_preserve') {
          return Response.json({
            id: 'sess_queue_preserve',
            state: 'destroyed',
            workspace: {
              root: '/tmp/workspace',
              logicalRoot: '/workspace',
              mode: 'new',
              fresh: true,
              managed: true,
            },
            createdAt: 'now',
          });
        }
        return new Response('not found', { status: 404 });
      },
    });
    const env = await ExecutionerEnvironment.create({
      backend: { kind: 'file', queueDir },
      host: { kind: 'http', baseUrl: `http://127.0.0.1:${server.port}/` },
      worker: { kind: 'external' },
      lifecycle: { cleanupQueueOnClose: true, cleanupStateOnClose: false },
    });

    try {
      await env.close();
      expect(await readFile(join(queueDir, 'sentinel.txt'), 'utf8')).toBe('do not delete');
    } finally {
      server.stop(true);
    }
  });

  test('close unlinks swapped queue child symlinks without following them', async () => {
    const root = await mkdtemp(join(tmpdir(), 'executioner-js-test-'));
    cleanup.push(root);
    const queueDir = join(root, 'queue');
    const outside = join(root, 'outside');
    await mkdir(queueDir);
    await mkdir(outside);
    await writeFile(join(queueDir, 'sentinel.txt'), 'do not delete');
    await writeFile(join(outside, 'secret.txt'), 'keep me');
    const server = Bun.serve({
      port: 0,
      fetch(request) {
        const url = new URL(request.url);
        if (request.method === 'POST' && url.pathname === '/sessions') {
          return Response.json({
            session: {
              id: 'sess_queue_symlink_cleanup',
              state: 'ready',
              workspace: {
                root: '/tmp/workspace',
                logicalRoot: '/workspace',
                mode: 'new',
                fresh: true,
                managed: true,
              },
              createdAt: 'now',
            },
          });
        }
        if (request.method === 'DELETE' && url.pathname === '/sessions/sess_queue_symlink_cleanup') {
          return Response.json({
            id: 'sess_queue_symlink_cleanup',
            state: 'destroyed',
            workspace: {
              root: '/tmp/workspace',
              logicalRoot: '/workspace',
              mode: 'new',
              fresh: true,
              managed: true,
            },
            createdAt: 'now',
          });
        }
        return new Response('not found', { status: 404 });
      },
    });
    const env = await ExecutionerEnvironment.create({
      backend: { kind: 'file', queueDir },
      host: { kind: 'http', baseUrl: `http://127.0.0.1:${server.port}/` },
      worker: { kind: 'external' },
      lifecycle: { cleanupQueueOnClose: true, cleanupStateOnClose: false },
    });

    try {
      await rmdir(join(queueDir, 'pending'));
      await symlink(outside, join(queueDir, 'pending'));
      await env.close();
      expect(await readFile(join(queueDir, 'sentinel.txt'), 'utf8')).toBe('do not delete');
      expect(existsSync(join(queueDir, 'pending'))).toBe(false);
      expect(await readFile(join(outside, 'secret.txt'), 'utf8')).toBe('keep me');
    } finally {
      server.stop(true);
    }
  });

  test('close unlinks swapped queue root symlink without following it', async () => {
    const root = await mkdtemp(join(tmpdir(), 'executioner-js-test-'));
    cleanup.push(root);
    const queueDir = join(root, 'queue');
    const outside = join(root, 'outside');
    await mkdir(queueDir);
    await mkdir(join(outside, 'pending'), { recursive: true });
    await writeFile(join(outside, 'pending', 'secret.txt'), 'keep me');
    const server = Bun.serve({
      port: 0,
      fetch(request) {
        const url = new URL(request.url);
        if (request.method === 'POST' && url.pathname === '/sessions') {
          return Response.json({
            session: {
              id: 'sess_queue_root_symlink_cleanup',
              state: 'ready',
              workspace: {
                root: '/tmp/workspace',
                logicalRoot: '/workspace',
                mode: 'new',
                fresh: true,
                managed: true,
              },
              createdAt: 'now',
            },
          });
        }
        if (request.method === 'DELETE' && url.pathname === '/sessions/sess_queue_root_symlink_cleanup') {
          return Response.json({
            id: 'sess_queue_root_symlink_cleanup',
            state: 'destroyed',
            workspace: {
              root: '/tmp/workspace',
              logicalRoot: '/workspace',
              mode: 'new',
              fresh: true,
              managed: true,
            },
            createdAt: 'now',
          });
        }
        return new Response('not found', { status: 404 });
      },
    });
    const env = await ExecutionerEnvironment.create({
      backend: { kind: 'file', queueDir },
      host: { kind: 'http', baseUrl: `http://127.0.0.1:${server.port}/` },
      worker: { kind: 'external' },
      lifecycle: { cleanupQueueOnClose: true, cleanupStateOnClose: false },
    });

    try {
      await rm(queueDir, { recursive: true, force: true });
      await symlink(outside, queueDir);
      await env.close();
      expect(existsSync(queueDir)).toBe(false);
      expect(await readFile(join(outside, 'pending', 'secret.txt'), 'utf8')).toBe('keep me');
    } finally {
      server.stop(true);
    }
  });

  test('close preserves preexisting managed state root contents', async () => {
    const root = await mkdtemp(join(tmpdir(), 'executioner-js-test-'));
    cleanup.push(root);
    const queueDir = join(root, 'queue');
    const stateDir = join(root, 'state');
    await mkdir(stateDir);
    await writeFile(join(stateDir, 'sentinel.txt'), 'do not delete');
    const server = Bun.serve({
      port: 0,
      fetch(request) {
        const url = new URL(request.url);
        if (request.method === 'DELETE' && url.pathname === '/sessions/sess_state_preserve') {
          return Response.json({
            id: 'sess_state_preserve',
            state: 'destroyed',
            workspace: {
              root: '/tmp/workspace',
              logicalRoot: '/workspace',
              mode: 'new',
              fresh: true,
              managed: true,
            },
            createdAt: 'now',
          });
        }
        return new Response('not found', { status: 404 });
      },
    });
    const env = new (ExecutionerEnvironment as unknown as new (
      config: unknown,
      session: unknown,
      processes: unknown[],
    ) => ExecutionerEnvironment)(
      {
        binaryPath: 'executioner',
        queueDir,
        sdkCreatedQueueDir: true,
        sdkCreatedStateDir: false,
        baseUrl: `http://127.0.0.1:${server.port}/`,
        host: { kind: 'managed', stateDir, host: '127.0.0.1', port: server.port },
        worker: { kind: 'external' },
        workspace: { kind: 'new' },
        policy: {
          readRoots: ['/workspace'],
          writeRoots: ['/workspace'],
          process: { allowExec: false, allowedCommands: [], deniedCommands: [] },
          network: { enabled: false, allowHosts: [], denyHosts: [] },
          env: { allowlist: [], denylist: [], injected: {} },
        },
        lifecycle: { destroyOnClose: true, cleanupQueueOnClose: false, cleanupStateOnClose: true },
        submitTimeoutMs: 1_000,
      },
      {
        id: 'sess_state_preserve',
        state: 'ready',
        workspace: {
          root: '/tmp/workspace',
          logicalRoot: '/workspace',
          mode: 'new',
          fresh: true,
          managed: true,
        },
        createdAt: 'now',
        metadata: {},
      },
      [],
    );

    try {
      await env.close();
      expect(await readFile(join(stateDir, 'sentinel.txt'), 'utf8')).toBe('do not delete');
    } finally {
      server.stop(true);
    }
  });

  test('close waits for managed child processes to exit after termination', async () => {
    const root = await mkdtemp(join(tmpdir(), 'executioner-js-test-'));
    cleanup.push(root);
    const queueDir = join(root, 'queue');
    const server = Bun.serve({
      port: 0,
      fetch(request) {
        const url = new URL(request.url);
        if (request.method === 'DELETE' && url.pathname === '/sessions/sess_close_wait') {
          return Response.json({
            id: 'sess_close_wait',
            state: 'destroyed',
            workspace: {
              root: '/tmp/workspace',
              logicalRoot: '/workspace',
              mode: 'new',
              fresh: true,
              managed: true,
            },
            createdAt: 'now',
            metadata: {},
          });
        }
        return new Response('not found', { status: 404 });
      },
    });
    class FakeProcess extends EventEmitter {
      killed = false;
      exitCode: number | null = null;

      kill(signal?: NodeJS.Signals): boolean {
        this.killed = true;
        expect(signal).toBe('SIGTERM');
        setTimeout(() => {
          this.exitCode = 0;
          this.emit('exit', 0, signal);
        }, 25);
        return true;
      }
    }
    const fakeProcess = new FakeProcess();
    const env = new (ExecutionerEnvironment as unknown as new (
      config: unknown,
      session: unknown,
      processes: unknown[],
    ) => ExecutionerEnvironment)(
      {
        binaryPath: 'executioner',
        queueDir,
        baseUrl: `http://127.0.0.1:${server.port}/`,
        host: { kind: 'http', baseUrl: `http://127.0.0.1:${server.port}/` },
        worker: { kind: 'external' },
        workspace: { kind: 'new' },
        policy: {
          readRoots: ['/workspace'],
          writeRoots: ['/workspace'],
          process: { allowExec: false, allowedCommands: [], deniedCommands: [] },
          network: { enabled: false, allowHosts: [], denyHosts: [] },
          env: { allowlist: [], denylist: [], injected: {} },
        },
        lifecycle: { destroyOnClose: true, cleanupQueueOnClose: false, cleanupStateOnClose: false },
        submitTimeoutMs: 1_000,
      },
      {
        id: 'sess_close_wait',
        state: 'ready',
        workspace: {
          root: '/tmp/workspace',
          logicalRoot: '/workspace',
          mode: 'new',
          fresh: true,
          managed: true,
        },
        createdAt: 'now',
        metadata: {},
      },
      [{ name: 'executioner-worker', process: fakeProcess }],
    );

    try {
      await env.close();
      expect(fakeProcess.killed).toBe(true);
      expect(fakeProcess.exitCode).toBe(0);
    } finally {
      server.stop(true);
    }
  });

  test('close does not skip live managed child processes that were already signaled', async () => {
    const root = await mkdtemp(join(tmpdir(), 'executioner-js-test-'));
    cleanup.push(root);
    const queueDir = join(root, 'queue');
    const server = Bun.serve({
      port: 0,
      fetch(request) {
        const url = new URL(request.url);
        if (request.method === 'DELETE' && url.pathname === '/sessions/sess_close_presignaled') {
          return Response.json({
            id: 'sess_close_presignaled',
            state: 'destroyed',
            workspace: {
              root: '/tmp/workspace',
              logicalRoot: '/workspace',
              mode: 'new',
              fresh: true,
              managed: true,
            },
            createdAt: 'now',
            metadata: {},
          });
        }
        return new Response('not found', { status: 404 });
      },
    });
    class FakeProcess extends EventEmitter {
      killed = true;
      exitCode: number | null = null;
      signalCode: NodeJS.Signals | null = null;
      signals: (NodeJS.Signals | undefined)[] = [];

      kill(signal?: NodeJS.Signals): boolean {
        this.signals.push(signal);
        setTimeout(() => {
          this.exitCode = 0;
          this.emit('exit', 0, signal);
        }, 25);
        return true;
      }
    }
    const fakeProcess = new FakeProcess();
    const env = new (ExecutionerEnvironment as unknown as new (
      config: unknown,
      session: unknown,
      processes: unknown[],
    ) => ExecutionerEnvironment)(
      {
        binaryPath: 'executioner',
        queueDir,
        baseUrl: `http://127.0.0.1:${server.port}/`,
        host: { kind: 'http', baseUrl: `http://127.0.0.1:${server.port}/` },
        worker: { kind: 'external' },
        workspace: { kind: 'new' },
        policy: {
          readRoots: ['/workspace'],
          writeRoots: ['/workspace'],
          process: { allowExec: false, allowedCommands: [], deniedCommands: [] },
          network: { enabled: false, allowHosts: [], denyHosts: [] },
          env: { allowlist: [], denylist: [], injected: {} },
        },
        lifecycle: { destroyOnClose: true, cleanupQueueOnClose: false, cleanupStateOnClose: false },
        submitTimeoutMs: 1_000,
      },
      {
        id: 'sess_close_presignaled',
        state: 'ready',
        workspace: {
          root: '/tmp/workspace',
          logicalRoot: '/workspace',
          mode: 'new',
          fresh: true,
          managed: true,
        },
        createdAt: 'now',
        metadata: {},
      },
      [{ name: 'executioner-worker', process: fakeProcess }],
    );

    try {
      await env.close();
      expect(fakeProcess.signals).toEqual(['SIGTERM']);
      expect(fakeProcess.exitCode).toBe(0);
    } finally {
      server.stop(true);
    }
  });

  test('exportWorkspace rejects malformed artifact entries', async () => {
    const root = await mkdtemp(join(tmpdir(), 'executioner-js-test-'));
    cleanup.push(root);
    const queueDir = join(root, 'queue');
    const server = Bun.serve({
      port: 0,
      fetch(request) {
        const url = new URL(request.url);
        if (request.method === 'POST' && url.pathname === '/sessions') {
          return Response.json({
            session: {
              id: 'sess_bad_artifact',
              state: 'ready',
              workspace: {
                root: '/tmp/workspace',
                logicalRoot: '/workspace',
                mode: 'new',
                fresh: true,
                managed: true,
              },
              createdAt: 'now',
              metadata: {},
            },
          });
        }
        if (request.method === 'POST' && url.pathname === '/sessions/sess_bad_artifact/artifacts/workspace') {
          return Response.json({
            sessionId: 'sess_bad_artifact',
            artifact: { resourceType: 'artifact', uri: 'file:///tmp/workspace.tar' },
            manifest: { resourceType: 'artifact_manifest', uri: 'file:///tmp/workspace.manifest.json' },
            format: 'tar',
            bytes: 0,
            hash: 'sha256:empty',
            fileCount: 0,
            directoryCount: 0,
            symlinkCount: 0,
            entries: { archivePath: 'hidden.txt' },
            createdAt: 'now',
          });
        }
        if (request.method === 'DELETE' && url.pathname === '/sessions/sess_bad_artifact') {
          return Response.json({
            id: 'sess_bad_artifact',
            state: 'destroyed',
            workspace: {
              root: '/tmp/workspace',
              logicalRoot: '/workspace',
              mode: 'new',
              fresh: true,
              managed: true,
            },
            createdAt: 'now',
            metadata: {},
          });
        }
        return new Response('not found', { status: 404 });
      },
    });
    const env = await ExecutionerEnvironment.create({
      backend: { kind: 'file', queueDir },
      host: { kind: 'http', baseUrl: `http://127.0.0.1:${server.port}/` },
      worker: { kind: 'external' },
      lifecycle: { cleanupQueueOnClose: true, cleanupStateOnClose: false },
    });

    try {
      await expect(env.exportWorkspace()).rejects.toThrow('artifact entries');
    } finally {
      await env.close();
      server.stop(true);
    }
  });

  test('exportWorkspace rejects missing required artifact fields instead of defaulting', async () => {
    const root = await mkdtemp(join(tmpdir(), 'executioner-js-test-'));
    cleanup.push(root);
    const queueDir = join(root, 'queue');
    const server = Bun.serve({
      port: 0,
      fetch(request) {
        const url = new URL(request.url);
        if (request.method === 'POST' && url.pathname === '/sessions') {
          return Response.json({
            session: {
              id: 'sess_missing_artifact_hash',
              state: 'ready',
              workspace: {
                root: '/tmp/workspace',
                logicalRoot: '/workspace',
                mode: 'new',
                fresh: true,
                managed: true,
              },
              createdAt: 'now',
              metadata: {},
            },
          });
        }
        if (request.method === 'POST' && url.pathname === '/sessions/sess_missing_artifact_hash/artifacts/workspace') {
          return Response.json({
            sessionId: 'sess_missing_artifact_hash',
            artifact: { resourceType: 'artifact', uri: 'file:///tmp/workspace.tar' },
            manifest: { resourceType: 'artifact_manifest', uri: 'file:///tmp/workspace.manifest.json' },
            format: 'tar',
            bytes: 0,
            fileCount: 0,
            directoryCount: 0,
            symlinkCount: 0,
            entries: [],
            createdAt: 'now',
          });
        }
        if (request.method === 'DELETE' && url.pathname === '/sessions/sess_missing_artifact_hash') {
          return Response.json({
            id: 'sess_missing_artifact_hash',
            state: 'destroyed',
            workspace: {
              root: '/tmp/workspace',
              logicalRoot: '/workspace',
              mode: 'new',
              fresh: true,
              managed: true,
            },
            createdAt: 'now',
            metadata: {},
          });
        }
        return new Response('not found', { status: 404 });
      },
    });
    const env = await ExecutionerEnvironment.create({
      backend: { kind: 'file', queueDir },
      host: { kind: 'http', baseUrl: `http://127.0.0.1:${server.port}/` },
      worker: { kind: 'external' },
      lifecycle: { cleanupQueueOnClose: true, cleanupStateOnClose: false },
    });

    try {
      await expect(env.exportWorkspace()).rejects.toThrow('artifact hash is required');
    } finally {
      await env.close();
      server.stop(true);
    }
  });

  test('rejects symlink queue root directory', async () => {
    const root = await mkdtemp(join(tmpdir(), 'executioner-js-test-'));
    cleanup.push(root);
    const queueDir = join(root, 'queue');
    const outsideQueue = join(root, 'outside-queue');
    await mkdir(outsideQueue);
    await symlink(outsideQueue, queueDir);

    await expect(ExecutionerEnvironment.create({
      backend: { kind: 'file', queueDir },
      host: { kind: 'http', baseUrl: 'http://127.0.0.1:9/' },
      worker: { kind: 'external' },
      lifecycle: { cleanupQueueOnClose: false, cleanupStateOnClose: false },
    })).rejects.toThrow('queue directory');
    expect((await readdir(outsideQueue)).length).toBe(0);
  });

  test('rejects symlink queue parent directory', async () => {
    const root = await mkdtemp(join(tmpdir(), 'executioner-js-test-'));
    cleanup.push(root);
    const outsideQueue = join(root, 'outside-queue');
    const linkParent = join(root, 'link-parent');
    await mkdir(outsideQueue);
    await symlink(outsideQueue, linkParent);

    await expect(ExecutionerEnvironment.create({
      backend: { kind: 'file', queueDir: join(linkParent, 'queue') },
      host: { kind: 'http', baseUrl: 'http://127.0.0.1:9/' },
      worker: { kind: 'external' },
      lifecycle: { cleanupQueueOnClose: false, cleanupStateOnClose: false },
    })).rejects.toThrow('parent must not contain symlinks');
    expect((await readdir(outsideQueue)).length).toBe(0);
  });

  test('quarantines terminal events while invocation is still pending', async () => {
    const root = await mkdtemp(join(tmpdir(), 'executioner-js-test-'));
    cleanup.push(root);
    const queueDir = join(root, 'queue');
    const stateDir = join(root, 'state');
    const invocationId = 'js_pending_completed';
    const env = await ExecutionerEnvironment.create({
      backend: { kind: 'file', queueDir },
      host: { kind: 'managed', stateDir },
      worker: { kind: 'external' },
      lifecycle: { cleanupQueueOnClose: false, cleanupStateOnClose: false },
      submitTimeoutMs: 100,
    });

    try {
      const submit = env.submit({
        invocationId,
        toolName: 'Read',
        arguments: { path: 'missing.txt' },
      }).catch((error: Error) => error);
      const pendingPath = join(queueDir, 'pending', `${invocationId}.json`);
      await waitForPath(pendingPath);
      await writeFile(
        join(queueDir, 'completed', `${invocationId}.json`),
        JSON.stringify({
          type: 'tool.invocation.completed',
          invocationId,
          sessionId: env.session.id,
          result: {
            invocationId,
            sessionId: env.session.id,
            toolName: 'Read',
            status: 'success',
            output: 'forged before claim',
            error: null,
            summary: null,
            effects: [],
            durationMs: 0,
            metadata: {},
          },
          completedAt: 'now',
        }),
      );

      const error = await submit;
      expect(error).toBeInstanceOf(Error);
      expect(String((error as Error).message)).toContain('Timed out');
      expect(existsSync(pendingPath)).toBe(true);
      expect(existsSync(join(queueDir, 'completed', `${invocationId}.json`))).toBe(false);
      expect((await readdir(join(queueDir, 'rejected'))).length).toBe(1);
    } finally {
      await env.close();
    }
  });

  test('quarantines oversized terminal events without accepting them', async () => {
    const root = await mkdtemp(join(tmpdir(), 'executioner-js-test-'));
    cleanup.push(root);
    const queueDir = join(root, 'queue');
    const stateDir = join(root, 'state');
    const invocationId = 'js_huge_completed';
    const env = await ExecutionerEnvironment.create({
      backend: { kind: 'file', queueDir },
      host: { kind: 'managed', stateDir },
      worker: { kind: 'external' },
      lifecycle: { cleanupQueueOnClose: false, cleanupStateOnClose: false },
      submitTimeoutMs: 100,
    });

    try {
      const submit = env.submit({
        invocationId,
        toolName: 'Read',
        arguments: { path: 'missing.txt' },
      }).catch((error: Error) => error);
      const pendingPath = join(queueDir, 'pending', `${invocationId}.json`);
      await waitForPath(pendingPath);
      await claimPendingInvocation(queueDir, invocationId);
      await writeFile(
        join(queueDir, 'completed', `${invocationId}.json`),
        JSON.stringify({
          type: 'tool.invocation.completed',
          invocationId,
          sessionId: env.session.id,
          result: {
            invocationId,
            sessionId: env.session.id,
            toolName: 'Read',
            status: 'success',
            output: 'x'.repeat(10 * 1024 * 1024),
            error: null,
            summary: null,
            effects: [],
            durationMs: 0,
            metadata: {},
          },
          completedAt: 'now',
        }),
      );

      const error = await submit;
      expect(error).toBeInstanceOf(Error);
      expect(String((error as Error).message)).toContain('Timed out');
      expect(existsSync(join(queueDir, 'completed', `${invocationId}.json`))).toBe(false);
      expect((await readdir(join(queueDir, 'rejected'))).length).toBe(1);
    } finally {
      await env.close();
    }
  });
});

describe('workspace artifact materialization', () => {
  test('rejects ambiguous file artifact URIs before reading', async () => {
    const root = await mkdtemp(join(tmpdir(), 'executioner-js-artifact-'));
    cleanup.push(root);
    const baseArtifact: WorkspaceArtifact = {
      sessionId: 'sess_test',
      artifact: { resourceType: 'artifact', uri: 'file:////tmp/workspace.tar' },
      manifest: { resourceType: 'artifact_manifest', uri: 'file:///unused' },
      format: 'tar',
      bytes: 0,
      hash: 'sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855',
      fileCount: 0,
      directoryCount: 0,
      symlinkCount: 0,
      entries: [],
      createdAt: 'now',
    };

    for (const uri of [
      'file:////tmp/workspace.tar',
      'file:///tmp/workspace.tar?download=1',
      'file:///tmp/workspace.tar#fragment',
    ]) {
      await expect(materializeWorkspaceArtifact({
        ...baseArtifact,
        artifact: { resourceType: 'artifact', uri },
      }, join(root, `restored-${uri.length}`))).rejects.toThrow('without authority');
    }
  });

  test('restores files and safe symlinks', async () => {
    const root = await mkdtemp(join(tmpdir(), 'executioner-js-artifact-'));
    cleanup.push(root);
    const tarPath = join(root, 'workspace.tar');
    const fileData = Buffer.from('hello artifact', 'utf8');
    const tarData = writeTar(tarPath, [
      { name: 'src', kind: 'directory', data: Buffer.alloc(0) },
      { name: 'src/main.txt', kind: 'file', data: fileData },
    ]);
    const artifact: WorkspaceArtifact = {
      sessionId: 'sess_test',
      artifact: { resourceType: 'artifact', uri: `file://${tarPath}` },
      manifest: { resourceType: 'artifact_manifest', uri: 'file:///unused' },
      format: 'tar',
      bytes: tarData.byteLength,
      hash: sha256(tarData),
      fileCount: 1,
      directoryCount: 1,
      symlinkCount: 1,
      entries: [
        {
          logicalPath: '/workspace/src',
          archivePath: 'src',
          kind: 'directory',
        },
        {
          logicalPath: '/workspace/src/main.txt',
          archivePath: 'src/main.txt',
          kind: 'file',
          bytes: fileData.byteLength,
          hash: sha256(fileData),
        },
        {
          logicalPath: '/workspace/main-link',
          archivePath: 'main-link',
          kind: 'symlink',
          linkTarget: 'src/main.txt',
        },
      ],
      createdAt: 'now',
    };

    await materializeWorkspaceArtifact(artifact, join(root, 'restored'));

    expect(await readFile(join(root, 'restored', 'src', 'main.txt'), 'utf8')).toBe('hello artifact');
    expect(await readlink(join(root, 'restored', 'main-link'))).toBe('src/main.txt');
  });

  test('accepts equivalent manifest resource regardless of JSON key order', async () => {
    const root = await mkdtemp(join(tmpdir(), 'executioner-js-artifact-'));
    cleanup.push(root);
    const tarPath = join(root, 'workspace.tar');
    const manifestPath = join(root, 'workspace.manifest.json');
    const fileData = Buffer.from('manifest order payload', 'utf8');
    const tarData = writeTar(tarPath, [{ name: 'file.txt', kind: 'file', data: fileData }]);
    const artifact: WorkspaceArtifact = {
      sessionId: 'sess_test',
      artifact: { resourceType: 'artifact', uri: `file://${tarPath}` },
      manifest: { resourceType: 'artifact_manifest', uri: `file://${manifestPath}` },
      format: 'tar',
      bytes: tarData.byteLength,
      hash: sha256(tarData),
      fileCount: 1,
      directoryCount: 0,
      symlinkCount: 0,
      entries: [
        {
          logicalPath: '/workspace/file.txt',
          archivePath: 'file.txt',
          kind: 'file',
          bytes: fileData.byteLength,
          hash: sha256(fileData),
        },
      ],
      createdAt: 'now',
    };
    await writeFile(manifestPath, JSON.stringify({
      createdAt: artifact.createdAt,
      entries: artifact.entries.map((entry) => ({
        hash: entry.hash,
        bytes: entry.bytes,
        kind: entry.kind,
        archivePath: entry.archivePath,
        logicalPath: entry.logicalPath,
      })),
      symlinkCount: artifact.symlinkCount,
      directoryCount: artifact.directoryCount,
      fileCount: artifact.fileCount,
      hash: artifact.hash,
      bytes: artifact.bytes,
      format: artifact.format,
      manifest: artifact.manifest,
      artifact: artifact.artifact,
      sessionId: artifact.sessionId,
    }));

    await materializeWorkspaceArtifact(artifact, join(root, 'restored'));

    expect(await readFile(join(root, 'restored', 'file.txt'), 'utf8')).toBe('manifest order payload');
  });

  test('restores files with GNU long-name tar headers', async () => {
    const root = await mkdtemp(join(tmpdir(), 'executioner-js-artifact-'));
    cleanup.push(root);
    const tarPath = join(root, 'workspace.tar');
    const fileData = Buffer.from('long path payload', 'utf8');
    const longName = `${'deep-name-'.repeat(11)}file.txt`;
    const tarData = writeTar(tarPath, [{ name: longName, kind: 'file', data: fileData }]);
    const artifact: WorkspaceArtifact = {
      sessionId: 'sess_test',
      artifact: { resourceType: 'artifact', uri: `file://${tarPath}` },
      manifest: { resourceType: 'artifact_manifest', uri: 'file:///unused' },
      format: 'tar',
      bytes: tarData.byteLength,
      hash: sha256(tarData),
      fileCount: 1,
      directoryCount: 0,
      symlinkCount: 0,
      entries: [
        {
          logicalPath: `/workspace/${longName}`,
          archivePath: longName,
          kind: 'file',
          bytes: fileData.byteLength,
          hash: sha256(fileData),
        },
      ],
      createdAt: 'now',
    };

    await materializeWorkspaceArtifact(artifact, join(root, 'restored'));

    expect(await readFile(join(root, 'restored', longName), 'utf8')).toBe('long path payload');
  });

  test('rejects invalid artifacts without leaving created destination parents', async () => {
    const root = await mkdtemp(join(tmpdir(), 'executioner-js-artifact-'));
    cleanup.push(root);
    const tarPath = join(root, 'workspace.tar');
    const fileData = Buffer.from('payload', 'utf8');
    const tarData = writeTar(tarPath, [{ name: 'file.txt', kind: 'file', data: fileData }]);
    const destinationParent = join(root, 'new-parent', 'nested');
    const artifact: WorkspaceArtifact = {
      sessionId: 'sess_test',
      artifact: { resourceType: 'artifact', uri: `file://${tarPath}` },
      manifest: { resourceType: 'artifact_manifest', uri: 'file:///unused' },
      format: 'zip',
      bytes: tarData.byteLength,
      hash: sha256(tarData),
      fileCount: 1,
      directoryCount: 0,
      symlinkCount: 0,
      entries: [{
        logicalPath: '/workspace/file.txt',
        archivePath: 'file.txt',
        kind: 'file',
        bytes: fileData.byteLength,
        hash: sha256(fileData),
      }],
      createdAt: 'now',
    };

    await expect(materializeWorkspaceArtifact(artifact, join(destinationParent, 'restored'))).rejects.toThrow('unsupported workspace artifact format');

    expect(existsSync(join(destinationParent, 'restored'))).toBe(false);
    expect(existsSync(destinationParent)).toBe(false);
    expect(existsSync(join(root, 'new-parent'))).toBe(false);
  });

  test('rejects oversized manifest file entries before extracting', async () => {
    const root = await mkdtemp(join(tmpdir(), 'executioner-js-artifact-'));
    cleanup.push(root);
    const tarPath = join(root, 'workspace.tar');
    const tarData = writeTar(tarPath, []);
    const artifact: WorkspaceArtifact = {
      sessionId: 'sess_test',
      artifact: { resourceType: 'artifact', uri: `file://${tarPath}` },
      manifest: { resourceType: 'artifact_manifest', uri: 'file:///unused' },
      format: 'tar',
      bytes: tarData.byteLength,
      hash: sha256(tarData),
      fileCount: 1,
      directoryCount: 0,
      symlinkCount: 0,
      entries: [{
        logicalPath: '/workspace/huge.bin',
        archivePath: 'huge.bin',
        kind: 'file',
        bytes: 100 * 1024 * 1024 + 1,
        hash: sha256(Buffer.alloc(0)),
      }],
      createdAt: 'now',
    };

    await expect(materializeWorkspaceArtifact(artifact, join(root, 'restored'))).rejects.toThrow('maximum size');
    expect(existsSync(join(root, 'restored'))).toBe(false);
  });

  test('rejects excessive manifest path depth', async () => {
    const root = await mkdtemp(join(tmpdir(), 'executioner-js-artifact-'));
    cleanup.push(root);
    const tarPath = join(root, 'workspace.tar');
    const tarData = writeTar(tarPath, []);
    const archivePath = Array.from({ length: 257 }, () => 'd').join('/');
    const artifact: WorkspaceArtifact = {
      sessionId: 'sess_test',
      artifact: { resourceType: 'artifact', uri: `file://${tarPath}` },
      manifest: { resourceType: 'artifact_manifest', uri: 'file:///unused' },
      format: 'tar',
      bytes: tarData.byteLength,
      hash: sha256(tarData),
      fileCount: 0,
      directoryCount: 0,
      symlinkCount: 1,
      entries: [{
        logicalPath: `/workspace/${archivePath}`,
        archivePath,
        kind: 'symlink',
        linkTarget: 'target.txt',
      }],
      createdAt: 'now',
    };

    await expect(materializeWorkspaceArtifact(artifact, join(root, 'restored'))).rejects.toThrow('maximum path depth');
    expect(existsSync(join(root, 'restored'))).toBe(false);
  });

  test('rejects unsafe manifest archive paths without materializing files', async () => {
    const root = await mkdtemp(join(tmpdir(), 'executioner-js-artifact-'));
    cleanup.push(root);
    const tarPath = join(root, 'workspace.tar');
    const fileData = Buffer.from('safe', 'utf8');
    const tarData = writeTar(tarPath, [{ name: 'safe.txt', kind: 'file', data: fileData }]);
    const artifact: WorkspaceArtifact = {
      sessionId: 'sess_test',
      artifact: { resourceType: 'artifact', uri: `file://${tarPath}` },
      manifest: { resourceType: 'artifact_manifest', uri: 'file:///unused' },
      format: 'tar',
      bytes: tarData.byteLength,
      hash: sha256(tarData),
      fileCount: 1,
      directoryCount: 0,
      symlinkCount: 0,
      entries: [
        {
          logicalPath: '/workspace/../escape.txt',
          archivePath: '../escape.txt',
          kind: 'file',
          bytes: fileData.byteLength,
          hash: sha256(fileData),
        },
      ],
      createdAt: 'now',
    };

    await expect(materializeWorkspaceArtifact(artifact, join(root, 'restored'))).rejects.toThrow('unsafe artifact path');
    expect(existsSync(join(root, 'restored'))).toBe(false);
    expect(existsSync(join(root, 'escape.txt'))).toBe(false);
  });

  test('rejects symlink artifact resource before reading tar bytes', async () => {
    const root = await mkdtemp(join(tmpdir(), 'executioner-js-artifact-'));
    cleanup.push(root);
    const tarPath = join(root, 'workspace.tar');
    const linkPath = join(root, 'workspace-link.tar');
    const fileData = Buffer.from('payload', 'utf8');
    const tarData = writeTar(tarPath, [{ name: 'file.txt', kind: 'file', data: fileData }]);
    await symlink(tarPath, linkPath);
    const artifact: WorkspaceArtifact = {
      sessionId: 'sess_test',
      artifact: { resourceType: 'artifact', uri: `file://${linkPath}` },
      manifest: { resourceType: 'artifact_manifest', uri: 'file:///unused' },
      format: 'tar',
      bytes: tarData.byteLength,
      hash: sha256(tarData),
      fileCount: 1,
      directoryCount: 0,
      symlinkCount: 0,
      entries: [{
        logicalPath: '/workspace/file.txt',
        archivePath: 'file.txt',
        kind: 'file',
        bytes: fileData.byteLength,
        hash: sha256(fileData),
      }],
      createdAt: 'now',
    };

    await expect(materializeWorkspaceArtifact(artifact, join(root, 'restored'))).rejects.toThrow('regular file');
    expect(existsSync(join(root, 'restored'))).toBe(false);
  });

  test('rejects symlink manifest resource before reading manifest bytes', async () => {
    const root = await mkdtemp(join(tmpdir(), 'executioner-js-artifact-'));
    cleanup.push(root);
    const tarPath = join(root, 'workspace.tar');
    const manifestPath = join(root, 'workspace.manifest.json');
    const manifestLink = join(root, 'workspace.manifest-link.json');
    const fileData = Buffer.from('payload', 'utf8');
    const tarData = writeTar(tarPath, [{ name: 'file.txt', kind: 'file', data: fileData }]);
    const artifact: WorkspaceArtifact = {
      sessionId: 'sess_test',
      artifact: { resourceType: 'artifact', uri: `file://${tarPath}` },
      manifest: { resourceType: 'artifact_manifest', uri: `file://${manifestLink}` },
      format: 'tar',
      bytes: tarData.byteLength,
      hash: sha256(tarData),
      fileCount: 1,
      directoryCount: 0,
      symlinkCount: 0,
      entries: [{
        logicalPath: '/workspace/file.txt',
        archivePath: 'file.txt',
        kind: 'file',
        bytes: fileData.byteLength,
        hash: sha256(fileData),
      }],
      createdAt: 'now',
    };
    await writeFile(manifestPath, JSON.stringify(artifact));
    await symlink(manifestPath, manifestLink);

    await expect(materializeWorkspaceArtifact(artifact, join(root, 'restored'))).rejects.toThrow('regular file');
    expect(existsSync(join(root, 'restored'))).toBe(false);
  });

  test('rejects fractional artifact byte counts', async () => {
    const root = await mkdtemp(join(tmpdir(), 'executioner-js-artifact-'));
    cleanup.push(root);
    const tarPath = join(root, 'workspace.tar');
    const fileData = Buffer.from('payload', 'utf8');
    const tarData = writeTar(tarPath, [{ name: 'file.txt', kind: 'file', data: fileData }]);
    const artifact: WorkspaceArtifact = {
      sessionId: 'sess_test',
      artifact: { resourceType: 'artifact', uri: `file://${tarPath}` },
      manifest: { resourceType: 'artifact_manifest', uri: 'file:///unused' },
      format: 'tar',
      bytes: tarData.byteLength + 0.5,
      hash: sha256(tarData),
      fileCount: 1,
      directoryCount: 0,
      symlinkCount: 0,
      entries: [
        {
          logicalPath: '/workspace/file.txt',
          archivePath: 'file.txt',
          kind: 'file',
          bytes: fileData.byteLength,
          hash: sha256(fileData),
        },
      ],
      createdAt: 'now',
    };

    await expect(materializeWorkspaceArtifact(artifact, join(root, 'restored'))).rejects.toThrow('artifact bytes');
    expect(existsSync(join(root, 'restored'))).toBe(false);
  });

  test('rejects negative artifact byte counts', async () => {
    const root = await mkdtemp(join(tmpdir(), 'executioner-js-artifact-'));
    cleanup.push(root);
    const tarPath = join(root, 'workspace.tar');
    const fileData = Buffer.from('payload', 'utf8');
    const tarData = writeTar(tarPath, [{ name: 'file.txt', kind: 'file', data: fileData }]);
    const artifact: WorkspaceArtifact = {
      sessionId: 'sess_test',
      artifact: { resourceType: 'artifact', uri: `file://${tarPath}` },
      manifest: { resourceType: 'artifact_manifest', uri: 'file:///unused' },
      format: 'tar',
      bytes: -1,
      hash: sha256(tarData),
      fileCount: 1,
      directoryCount: 0,
      symlinkCount: 0,
      entries: [
        {
          logicalPath: '/workspace/file.txt',
          archivePath: 'file.txt',
          kind: 'file',
          bytes: fileData.byteLength,
          hash: sha256(fileData),
        },
      ],
      createdAt: 'now',
    };

    await expect(materializeWorkspaceArtifact(artifact, join(root, 'restored'))).rejects.toThrow('artifact bytes must be non-negative');
    expect(existsSync(join(root, 'restored'))).toBe(false);
  });

  test('rejects unsafe integer artifact metadata before materializing', async () => {
    const root = await mkdtemp(join(tmpdir(), 'executioner-js-artifact-'));
    cleanup.push(root);
    const tarPath = join(root, 'workspace.tar');
    const fileData = Buffer.from('payload', 'utf8');
    const tarData = writeTar(tarPath, [{ name: 'file.txt', kind: 'file', data: fileData }]);
    const artifact = {
      sessionId: 'sess_test',
      artifact: { resourceType: 'artifact', uri: `file://${tarPath}` },
      manifest: { resourceType: 'artifact_manifest', uri: 'file:///unused' },
      format: 'tar',
      bytes: Number.MAX_SAFE_INTEGER + 1,
      hash: sha256(tarData),
      fileCount: 1,
      directoryCount: 0,
      symlinkCount: 0,
      entries: [
        {
          logicalPath: '/workspace/file.txt',
          archivePath: 'file.txt',
          kind: 'file',
          bytes: fileData.byteLength,
          hash: sha256(fileData),
        },
      ],
      createdAt: 'now',
    };

    await expect(materializeWorkspaceArtifact(artifact as WorkspaceArtifact, join(root, 'restored'))).rejects.toThrow('safe integer');
    expect(existsSync(join(root, 'restored'))).toBe(false);
  });

  test('rejects unknown artifact metadata fields before materializing', async () => {
    const root = await mkdtemp(join(tmpdir(), 'executioner-js-artifact-'));
    cleanup.push(root);
    const tarPath = join(root, 'workspace.tar');
    const fileData = Buffer.from('payload', 'utf8');
    const tarData = writeTar(tarPath, [{ name: 'file.txt', kind: 'file', data: fileData }]);
    const artifact = {
      sessionId: 'sess_test',
      artifact: { resourceType: 'artifact', uri: `file://${tarPath}` },
      manifest: { resourceType: 'artifact_manifest', uri: 'file:///unused' },
      format: 'tar',
      bytes: tarData.byteLength,
      hash: sha256(tarData),
      fileCount: 1,
      directoryCount: 0,
      symlinkCount: 0,
      entries: [
        {
          logicalPath: '/workspace/file.txt',
          archivePath: 'file.txt',
          kind: 'file',
          bytes: fileData.byteLength,
          hash: sha256(fileData),
        },
      ],
      createdAt: 'now',
      padding: 'unexpected',
    };

    await expect(materializeWorkspaceArtifact(artifact as WorkspaceArtifact, join(root, 'restored'))).rejects.toThrow('unknown workspace artifact field');
    expect(existsSync(join(root, 'restored'))).toBe(false);
  });

  test('rejects unknown artifact entry and resource fields before materializing', async () => {
    const root = await mkdtemp(join(tmpdir(), 'executioner-js-artifact-'));
    cleanup.push(root);
    const tarPath = join(root, 'workspace.tar');
    const fileData = Buffer.from('payload', 'utf8');
    const tarData = writeTar(tarPath, [{ name: 'file.txt', kind: 'file', data: fileData }]);
    const artifactWithResourcePadding = {
      sessionId: 'sess_test',
      artifact: { resourceType: 'artifact', uri: `file://${tarPath}`, padding: 'unexpected' },
      manifest: { resourceType: 'artifact_manifest', uri: 'file:///unused' },
      format: 'tar',
      bytes: tarData.byteLength,
      hash: sha256(tarData),
      fileCount: 1,
      directoryCount: 0,
      symlinkCount: 0,
      entries: [
        {
          logicalPath: '/workspace/file.txt',
          archivePath: 'file.txt',
          kind: 'file',
          bytes: fileData.byteLength,
          hash: sha256(fileData),
        },
      ],
      createdAt: 'now',
    };
    const artifactWithEntryPadding = {
      ...artifactWithResourcePadding,
      artifact: { resourceType: 'artifact', uri: `file://${tarPath}` },
      entries: [
        {
          logicalPath: '/workspace/file.txt',
          archivePath: 'file.txt',
          kind: 'file',
          bytes: fileData.byteLength,
          hash: sha256(fileData),
          padding: 'unexpected',
        },
      ],
    };

    await expect(materializeWorkspaceArtifact(artifactWithResourcePadding as WorkspaceArtifact, join(root, 'resource-restored'))).rejects.toThrow('unknown artifact resource field');
    await expect(materializeWorkspaceArtifact(artifactWithEntryPadding as WorkspaceArtifact, join(root, 'entry-restored'))).rejects.toThrow('unknown workspace artifact entry field');
    expect(existsSync(join(root, 'resource-restored'))).toBe(false);
    expect(existsSync(join(root, 'entry-restored'))).toBe(false);
  });

  test('rejects artifact resource hash bytes and metadata fields before materializing', async () => {
    const root = await mkdtemp(join(tmpdir(), 'executioner-js-artifact-'));
    cleanup.push(root);
    const tarPath = join(root, 'workspace.tar');
    const fileData = Buffer.from('payload', 'utf8');
    const tarData = writeTar(tarPath, [{ name: 'file.txt', kind: 'file', data: fileData }]);
    const artifact = {
      sessionId: 'sess_test',
      artifact: {
        resourceType: 'artifact',
        uri: `file://${tarPath}`,
        hash: 'sha256:smuggled',
        bytes: tarData.byteLength,
        metadata: { smuggled: true },
      },
      manifest: { resourceType: 'artifact_manifest', uri: 'file:///unused' },
      format: 'tar',
      bytes: tarData.byteLength,
      hash: sha256(tarData),
      fileCount: 1,
      directoryCount: 0,
      symlinkCount: 0,
      entries: [
        {
          logicalPath: '/workspace/file.txt',
          archivePath: 'file.txt',
          kind: 'file',
          bytes: fileData.byteLength,
          hash: sha256(fileData),
        },
      ],
      createdAt: 'now',
    };

    await expect(materializeWorkspaceArtifact(artifact as WorkspaceArtifact, join(root, 'restored'))).rejects.toThrow('unknown artifact resource field');
    expect(existsSync(join(root, 'restored'))).toBe(false);
  });

  test('rejects artifact file size mismatch before reading tar bytes', async () => {
    const root = await mkdtemp(join(tmpdir(), 'executioner-js-artifact-'));
    cleanup.push(root);
    const tarPath = join(root, 'workspace.tar');
    const fileData = Buffer.from('payload', 'utf8');
    const tarData = writeTar(tarPath, [{ name: 'file.txt', kind: 'file', data: fileData }]);
    const artifact: WorkspaceArtifact = {
      sessionId: 'sess_test',
      artifact: { resourceType: 'artifact', uri: `file://${tarPath}` },
      manifest: { resourceType: 'artifact_manifest', uri: 'file:///unused' },
      format: 'tar',
      bytes: tarData.byteLength - 1,
      hash: sha256(tarData),
      fileCount: 1,
      directoryCount: 0,
      symlinkCount: 0,
      entries: [
        {
          logicalPath: '/workspace/file.txt',
          archivePath: 'file.txt',
          kind: 'file',
          bytes: fileData.byteLength,
          hash: sha256(fileData),
        },
      ],
      createdAt: 'now',
    };

    await expect(materializeWorkspaceArtifact(artifact, join(root, 'restored'))).rejects.toThrow('artifact file size does not match metadata');
    expect(existsSync(join(root, 'restored'))).toBe(false);
  });

  test('rejects oversized declared artifact before reading tar bytes', async () => {
    const root = await mkdtemp(join(tmpdir(), 'executioner-js-artifact-'));
    cleanup.push(root);
    const tarPath = join(root, 'workspace.tar');
    const fileData = Buffer.from('payload', 'utf8');
    writeTar(tarPath, [{ name: 'file.txt', kind: 'file', data: fileData }]);
    const artifact: WorkspaceArtifact = {
      sessionId: 'sess_test',
      artifact: { resourceType: 'artifact', uri: `file://${tarPath}` },
      manifest: { resourceType: 'artifact_manifest', uri: 'file:///unused' },
      format: 'tar',
      bytes: 100 * 1024 * 1024 + 1,
      hash: 'sha256:declared-too-large',
      fileCount: 1,
      directoryCount: 0,
      symlinkCount: 0,
      entries: [
        {
          logicalPath: '/workspace/file.txt',
          archivePath: 'file.txt',
          kind: 'file',
          bytes: fileData.byteLength,
          hash: sha256(fileData),
        },
      ],
      createdAt: 'now',
    };

    await expect(materializeWorkspaceArtifact(artifact, join(root, 'restored'))).rejects.toThrow('maximum size');
    expect(existsSync(join(root, 'restored'))).toBe(false);
  });

  test('rejects oversized manifest resource', async () => {
    const root = await mkdtemp(join(tmpdir(), 'executioner-js-artifact-'));
    cleanup.push(root);
    const tarPath = join(root, 'workspace.tar');
    const manifestPath = join(root, 'workspace.manifest.json');
    const fileData = Buffer.from('payload', 'utf8');
    const tarData = writeTar(tarPath, [{ name: 'file.txt', kind: 'file', data: fileData }]);
    const artifact: WorkspaceArtifact = {
      sessionId: 'sess_test',
      artifact: { resourceType: 'artifact', uri: `file://${tarPath}` },
      manifest: { resourceType: 'artifact_manifest', uri: `file://${manifestPath}` },
      format: 'tar',
      bytes: tarData.byteLength,
      hash: sha256(tarData),
      fileCount: 1,
      directoryCount: 0,
      symlinkCount: 0,
      entries: [
        {
          logicalPath: '/workspace/file.txt',
          archivePath: 'file.txt',
          kind: 'file',
          bytes: fileData.byteLength,
          hash: sha256(fileData),
        },
      ],
      createdAt: 'now',
    };
    await writeFile(
      manifestPath,
      JSON.stringify({ ...artifact, padding: 'x'.repeat(11 * 1024 * 1024) }),
    );

    await expect(materializeWorkspaceArtifact(artifact, join(root, 'restored'))).rejects.toThrow('manifest resource exceeds');
    expect(existsSync(join(root, 'restored'))).toBe(false);
  });

  test('rejects tar entries with invalid header checksums', async () => {
    const root = await mkdtemp(join(tmpdir(), 'executioner-js-artifact-'));
    cleanup.push(root);
    const tarPath = join(root, 'workspace.tar');
    const fileData = Buffer.from('payload', 'utf8');
    const tarData = writeTar(tarPath, [{ name: 'file.txt', kind: 'file', data: fileData }]);
    tarData[148] = '7'.charCodeAt(0);
    writeFileSync(tarPath, tarData);
    const artifact: WorkspaceArtifact = {
      sessionId: 'sess_test',
      artifact: { resourceType: 'artifact', uri: `file://${tarPath}` },
      manifest: { resourceType: 'artifact_manifest', uri: 'file:///unused' },
      format: 'tar',
      bytes: tarData.byteLength,
      hash: sha256(tarData),
      fileCount: 1,
      directoryCount: 0,
      symlinkCount: 0,
      entries: [
        {
          logicalPath: '/workspace/file.txt',
          archivePath: 'file.txt',
          kind: 'file',
          bytes: fileData.byteLength,
          hash: sha256(fileData),
        },
      ],
      createdAt: 'now',
    };

    await expect(materializeWorkspaceArtifact(artifact, join(root, 'restored'))).rejects.toThrow('invalid tar header checksum');
    expect(existsSync(join(root, 'restored'))).toBe(false);
  });

  test('rejects tar archives missing end-of-archive markers', async () => {
    const root = await mkdtemp(join(tmpdir(), 'executioner-js-artifact-'));
    cleanup.push(root);
    const tarPath = join(root, 'workspace.tar');
    const fileData = Buffer.from('payload', 'utf8');
    const tarData = writeTar(tarPath, [{ name: 'file.txt', kind: 'file', data: fileData }]).subarray(0, -1024);
    writeFileSync(tarPath, tarData);
    const artifact: WorkspaceArtifact = {
      sessionId: 'sess_test',
      artifact: { resourceType: 'artifact', uri: `file://${tarPath}` },
      manifest: { resourceType: 'artifact_manifest', uri: 'file:///unused' },
      format: 'tar',
      bytes: tarData.byteLength,
      hash: sha256(tarData),
      fileCount: 1,
      directoryCount: 0,
      symlinkCount: 0,
      entries: [
        {
          logicalPath: '/workspace/file.txt',
          archivePath: 'file.txt',
          kind: 'file',
          bytes: fileData.byteLength,
          hash: sha256(fileData),
        },
      ],
      createdAt: 'now',
    };

    await expect(materializeWorkspaceArtifact(artifact, join(root, 'restored'))).rejects.toThrow('missing end-of-archive marker');
    expect(existsSync(join(root, 'restored'))).toBe(false);
  });

  test('rejects tar archives with only one end-of-archive zero block', async () => {
    const root = await mkdtemp(join(tmpdir(), 'executioner-js-artifact-'));
    cleanup.push(root);
    const tarPath = join(root, 'workspace.tar');
    const fileData = Buffer.from('payload', 'utf8');
    const tarData = writeTar(tarPath, [{ name: 'file.txt', kind: 'file', data: fileData }]).subarray(0, -512);
    writeFileSync(tarPath, tarData);
    const artifact: WorkspaceArtifact = {
      sessionId: 'sess_test',
      artifact: { resourceType: 'artifact', uri: `file://${tarPath}` },
      manifest: { resourceType: 'artifact_manifest', uri: 'file:///unused' },
      format: 'tar',
      bytes: tarData.byteLength,
      hash: sha256(tarData),
      fileCount: 1,
      directoryCount: 0,
      symlinkCount: 0,
      entries: [
        {
          logicalPath: '/workspace/file.txt',
          archivePath: 'file.txt',
          kind: 'file',
          bytes: fileData.byteLength,
          hash: sha256(fileData),
        },
      ],
      createdAt: 'now',
    };

    await expect(materializeWorkspaceArtifact(artifact, join(root, 'restored'))).rejects.toThrow('missing end-of-archive marker');
    expect(existsSync(join(root, 'restored'))).toBe(false);
  });

  test('rejects tar archives with trailing data after end-of-archive', async () => {
    const root = await mkdtemp(join(tmpdir(), 'executioner-js-artifact-'));
    cleanup.push(root);
    const tarPath = join(root, 'workspace.tar');
    const fileData = Buffer.from('payload', 'utf8');
    const tarData = Buffer.concat([
      writeTar(tarPath, [{ name: 'file.txt', kind: 'file', data: fileData }]),
      Buffer.from('trailing-data', 'utf8'),
    ]);
    writeFileSync(tarPath, tarData);
    const artifact: WorkspaceArtifact = {
      sessionId: 'sess_test',
      artifact: { resourceType: 'artifact', uri: `file://${tarPath}` },
      manifest: { resourceType: 'artifact_manifest', uri: 'file:///unused' },
      format: 'tar',
      bytes: tarData.byteLength,
      hash: sha256(tarData),
      fileCount: 1,
      directoryCount: 0,
      symlinkCount: 0,
      entries: [
        {
          logicalPath: '/workspace/file.txt',
          archivePath: 'file.txt',
          kind: 'file',
          bytes: fileData.byteLength,
          hash: sha256(fileData),
        },
      ],
      createdAt: 'now',
    };

    await expect(materializeWorkspaceArtifact(artifact, join(root, 'restored'))).rejects.toThrow('trailing data');
    expect(existsSync(join(root, 'restored'))).toBe(false);
  });

  test('rejects dangling GNU long-name tar records', async () => {
    const root = await mkdtemp(join(tmpdir(), 'executioner-js-artifact-'));
    cleanup.push(root);
    const tarPath = join(root, 'workspace.tar');
    const longName = `${'deep-name-'.repeat(11)}file.txt`;
    const longNamePayload = Buffer.from(`${longName}\0`, 'utf8');
    const padding = (512 - (longNamePayload.byteLength % 512)) % 512;
    const tarData = Buffer.concat([
      tarHeader('././@LongLink', 'L', longNamePayload.byteLength),
      longNamePayload,
      Buffer.alloc(padding),
      Buffer.alloc(1024),
    ]);
    writeFileSync(tarPath, tarData);
    const artifact: WorkspaceArtifact = {
      sessionId: 'sess_test',
      artifact: { resourceType: 'artifact', uri: `file://${tarPath}` },
      manifest: { resourceType: 'artifact_manifest', uri: 'file:///unused' },
      format: 'tar',
      bytes: tarData.byteLength,
      hash: sha256(tarData),
      fileCount: 0,
      directoryCount: 0,
      symlinkCount: 0,
      entries: [],
      createdAt: 'now',
    };

    await expect(materializeWorkspaceArtifact(artifact, join(root, 'restored'))).rejects.toThrow('long-name entry is missing');
    expect(existsSync(join(root, 'restored'))).toBe(false);
  });

  test('rejects non-UTF-8 archive paths instead of rewriting them', async () => {
    const root = await mkdtemp(join(tmpdir(), 'executioner-js-artifact-'));
    cleanup.push(root);
    const tarPath = join(root, 'workspace.tar');
    const fileData = Buffer.from('payload', 'utf8');
    const tarData = writeTarRawName(tarPath, Buffer.from([0x62, 0x61, 0x64, 0x2d, 0xff, 0x2e, 0x74, 0x78, 0x74]), fileData);
    const artifact: WorkspaceArtifact = {
      sessionId: 'sess_test',
      artifact: { resourceType: 'artifact', uri: `file://${tarPath}` },
      manifest: { resourceType: 'artifact_manifest', uri: 'file:///unused' },
      format: 'tar',
      bytes: tarData.byteLength,
      hash: sha256(tarData),
      fileCount: 1,
      directoryCount: 0,
      symlinkCount: 0,
      entries: [
        {
          logicalPath: '/workspace/bad-\ufffd.txt',
          archivePath: 'bad-\ufffd.txt',
          kind: 'file',
          bytes: fileData.byteLength,
          hash: sha256(fileData),
        },
      ],
      createdAt: 'now',
    };

    await expect(materializeWorkspaceArtifact(artifact, join(root, 'restored'))).rejects.toThrow('not valid UTF-8');
    expect(existsSync(join(root, 'restored'))).toBe(false);
  });

  test('rejects symlinked destination parents', async () => {
    const root = await mkdtemp(join(tmpdir(), 'executioner-js-artifact-'));
    cleanup.push(root);
    const tarPath = join(root, 'workspace.tar');
    const fileData = Buffer.from('payload', 'utf8');
    const tarData = writeTar(tarPath, [{ name: 'file.txt', kind: 'file', data: fileData }]);
    const outside = join(root, 'outside');
    const linkParent = join(root, 'link-parent');
    await mkdir(outside);
    await symlink(outside, linkParent);
    const artifact: WorkspaceArtifact = {
      sessionId: 'sess_test',
      artifact: { resourceType: 'artifact', uri: `file://${tarPath}` },
      manifest: { resourceType: 'artifact_manifest', uri: 'file:///unused' },
      format: 'tar',
      bytes: tarData.byteLength,
      hash: sha256(tarData),
      fileCount: 1,
      directoryCount: 0,
      symlinkCount: 0,
      entries: [
        {
          logicalPath: '/workspace/file.txt',
          archivePath: 'file.txt',
          kind: 'file',
          bytes: fileData.byteLength,
          hash: sha256(fileData),
        },
      ],
      createdAt: 'now',
    };

    await expect(materializeWorkspaceArtifact(artifact, join(linkParent, 'restored'))).rejects.toThrow('parent must not contain symlinks');
    expect(existsSync(join(outside, 'restored'))).toBe(false);
  });

  test('rejects symlinked destination ancestors', async () => {
    const root = await mkdtemp(join(tmpdir(), 'executioner-js-artifact-'));
    cleanup.push(root);
    const tarPath = join(root, 'workspace.tar');
    const fileData = Buffer.from('payload', 'utf8');
    const tarData = writeTar(tarPath, [{ name: 'file.txt', kind: 'file', data: fileData }]);
    const outside = join(root, 'outside');
    const linkParent = join(root, 'link-parent');
    await mkdir(join(outside, 'existing'), { recursive: true });
    await symlink(outside, linkParent);
    const artifact: WorkspaceArtifact = {
      sessionId: 'sess_test',
      artifact: { resourceType: 'artifact', uri: `file://${tarPath}` },
      manifest: { resourceType: 'artifact_manifest', uri: 'file:///unused' },
      format: 'tar',
      bytes: tarData.byteLength,
      hash: sha256(tarData),
      fileCount: 1,
      directoryCount: 0,
      symlinkCount: 0,
      entries: [
        {
          logicalPath: '/workspace/file.txt',
          archivePath: 'file.txt',
          kind: 'file',
          bytes: fileData.byteLength,
          hash: sha256(fileData),
        },
      ],
      createdAt: 'now',
    };

    await expect(materializeWorkspaceArtifact(artifact, join(linkParent, 'existing', 'restored'))).rejects.toThrow('parent must not contain symlinks');
    expect(existsSync(join(outside, 'existing', 'restored'))).toBe(false);
  });

  test('rejects excessive manifest entries', async () => {
    const root = await mkdtemp(join(tmpdir(), 'executioner-js-artifact-'));
    cleanup.push(root);
    const tarPath = join(root, 'workspace.tar');
    const tarData = writeTar(tarPath, []);
    const entries: WorkspaceArtifact['entries'] = Array.from({ length: 10_001 }, (_, index) => ({
      logicalPath: `/workspace/link-${index}.txt`,
      archivePath: `link-${index}.txt`,
      kind: 'symlink',
      linkTarget: 'target.txt',
    }));
    const artifact: WorkspaceArtifact = {
      sessionId: 'sess_test',
      artifact: { resourceType: 'artifact', uri: `file://${tarPath}` },
      manifest: { resourceType: 'artifact_manifest', uri: 'file:///unused' },
      format: 'tar',
      bytes: tarData.byteLength,
      hash: sha256(tarData),
      fileCount: 0,
      directoryCount: 0,
      symlinkCount: entries.length,
      entries,
      createdAt: 'now',
    };

    await expect(materializeWorkspaceArtifact(artifact, join(root, 'restored'))).rejects.toThrow('maximum entry count');
    expect(existsSync(join(root, 'restored'))).toBe(false);
  });
});

async function claimPendingInvocation(queueDir: string, invocationId: string): Promise<void> {
  const pendingPath = join(queueDir, 'pending', `${invocationId}.json`);
  const request = JSON.parse(await readFile(pendingPath, 'utf8'));
  await writeFile(
    join(queueDir, 'claimed', `${invocationId}.json`),
    JSON.stringify({
      workerId: 'js-test-worker',
      attemptId: 'attempt',
      leaseToken: 'lease',
      claimedAt: 'now',
      request,
    }),
  );
  await rm(pendingPath, { force: true });
}

async function waitForPendingInvocation(queueDir: string): Promise<{ invocationId: string }> {
  const started = Date.now();
  const pendingDir = join(queueDir, 'pending');
  while (Date.now() - started < 1_000) {
    const files = await readdir(pendingDir);
    const file = files.find((entry) => entry.endsWith('.json'));
    if (file) {
      return { invocationId: file.slice(0, -'.json'.length) };
    }
    await Bun.sleep(10);
  }
  throw new Error(`timed out waiting for pending invocation in ${pendingDir}`);
}

async function waitForPath(path: string): Promise<void> {
  const started = Date.now();
  while (Date.now() - started < 1_000) {
    if (existsSync(path)) {
      return;
    }
    await Bun.sleep(10);
  }
  throw new Error(`timed out waiting for ${path}`);
}

type TestTarEntry = {
  name: string;
  kind: 'file' | 'directory';
  data: Buffer;
};

function writeTar(path: string, entries: TestTarEntry[]): Buffer {
  const chunks: Buffer[] = [];
  for (const entry of entries) {
    if (Buffer.byteLength(entry.name, 'utf8') > 100) {
      chunks.push(tarHeader('././@LongLink', 'L', Buffer.byteLength(entry.name, 'utf8') + 1));
      chunks.push(Buffer.from(`${entry.name}\0`, 'utf8'));
      const longNamePadding = (512 - ((Buffer.byteLength(entry.name, 'utf8') + 1) % 512)) % 512;
      if (longNamePadding > 0) {
        chunks.push(Buffer.alloc(longNamePadding));
      }
    }
    const headerName = Buffer.byteLength(entry.name, 'utf8') > 100 ? entry.name.slice(0, 100) : entry.name;
    chunks.push(tarHeader(
      headerName,
      entry.kind === 'directory' ? '5' : '0',
      entry.kind === 'file' ? entry.data.byteLength : 0,
    ));
    if (entry.kind === 'file') {
      chunks.push(entry.data);
      const padding = (512 - (entry.data.byteLength % 512)) % 512;
      if (padding > 0) {
        chunks.push(Buffer.alloc(padding));
      }
    }
  }
  chunks.push(Buffer.alloc(1024));
  const archive = Buffer.concat(chunks);
  writeFileSync(path, archive);
  return archive;
}

function tarHeader(name: string, typeflag: string, size: number): Buffer {
    const header = Buffer.alloc(512);
    writeTarString(header, 0, 100, name);
    writeTarString(header, 100, 8, '0000644');
    writeTarString(header, 108, 8, '0000000');
    writeTarString(header, 116, 8, '0000000');
    writeTarString(header, 124, 12, size.toString(8).padStart(11, '0'));
    writeTarString(header, 136, 12, '00000000000');
    header.fill(0x20, 148, 156);
    header[156] = typeflag.charCodeAt(0);
    writeTarString(header, 257, 6, 'ustar');
    writeTarString(header, 263, 2, '00');
    const checksum = header.reduce((sum, byte) => sum + byte, 0);
    writeTarString(header, 148, 8, checksum.toString(8).padStart(6, '0'));
    header[154] = 0;
    header[155] = 0x20;
    return header;
}

function writeTarString(buffer: Buffer, offset: number, length: number, value: string): void {
  buffer.write(value, offset, Math.min(Buffer.byteLength(value), length), 'utf8');
}

function writeTarRawName(path: string, name: Buffer, data: Buffer): Buffer {
  const header = Buffer.alloc(512);
  name.copy(header, 0, 0, Math.min(name.byteLength, 100));
  writeTarString(header, 100, 8, '0000644');
  writeTarString(header, 108, 8, '0000000');
  writeTarString(header, 116, 8, '0000000');
  writeTarString(header, 124, 12, data.byteLength.toString(8).padStart(11, '0'));
  writeTarString(header, 136, 12, '00000000000');
  header.fill(0x20, 148, 156);
  header[156] = '0'.charCodeAt(0);
  writeTarString(header, 257, 6, 'ustar');
  writeTarString(header, 263, 2, '00');
  const checksum = header.reduce((sum, byte) => sum + byte, 0);
  writeTarString(header, 148, 8, checksum.toString(8).padStart(6, '0'));
  header[154] = 0;
  header[155] = 0x20;
  const padding = (512 - (data.byteLength % 512)) % 512;
  const archive = Buffer.concat([header, data, Buffer.alloc(padding), Buffer.alloc(1024)]);
  writeFileSync(path, archive);
  return archive;
}

function sha256(data: Buffer): string {
  return `sha256:${createHash('sha256').update(data).digest('hex')}`;
}
