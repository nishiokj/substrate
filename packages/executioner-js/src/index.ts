import { spawn, type ChildProcess } from 'node:child_process';
import { createHash } from 'node:crypto';
import { createServer } from 'node:net';
import { link, lstat, mkdtemp, mkdir, open, readFile, readdir, rm, rmdir, symlink, writeFile } from 'node:fs/promises';
import { constants as fsConstants, existsSync, lstatSync } from 'node:fs';
import { dirname, isAbsolute, join, basename, parse, posix, resolve } from 'node:path';
import { tmpdir } from 'node:os';
import { randomUUID } from 'node:crypto';
import { fileURLToPath } from 'node:url';

const FATAL_UTF8_DECODER = new TextDecoder('utf-8', { fatal: true });
const MAX_HTTP_ERROR_BODY_BYTES = 64 * 1024;
const MAX_HTTP_JSON_BODY_BYTES = 10 * 1024 * 1024;
const MAX_QUEUE_JSON_BYTES = 10 * 1024 * 1024;
const MAX_REQUEST_JSON_BYTES = 1024 * 1024;
const MAX_WORKSPACE_ARTIFACT_ENTRIES = 10_000;
const MAX_WORKSPACE_ARTIFACT_DEPTH = 256;
const MAX_WORKSPACE_ARTIFACT_MANIFEST_BYTES = 10 * 1024 * 1024;
const MAX_WORKSPACE_ARTIFACT_BYTES = 100 * 1024 * 1024;
const MAX_OUTPUT_BYTES = 10 * 1024 * 1024;
const MAX_TOOL_TIMEOUT_MS = 60 * 60 * 1000;
const MAX_PROCESS_COUNT = 2 ** 32 - 1;
const SESSION_STATES = ['starting', 'ready', 'closing', 'closed', 'destroyed', 'failed'] as const;
const WORKSPACE_MODES = ['new', 'existing', 'snapshot', 'template'] as const;

export type WorkspaceConfig =
  | { kind: 'new' }
  | { kind: 'existing'; root: string };

export type WorkerConfig =
  | { kind: 'managed'; id?: string; idleSleepMs?: number }
  | { kind: 'external' };

export type HostConfig =
  | { kind: 'managed'; stateDir?: string; host?: string; port?: number }
  | { kind: 'http'; baseUrl: string };

export type BackendConfig = {
  kind: 'file';
  queueDir?: string;
};

export type LifecycleConfig = {
  destroyOnClose?: boolean;
  cleanupQueueOnClose?: boolean;
  cleanupStateOnClose?: boolean;
};

export type ProcessPolicyConfig = {
  allowExec?: boolean;
  allowedCommands?: string[];
  deniedCommands?: string[];
  maxProcesses?: number;
};

export type NetworkPolicyConfig = {
  enabled?: boolean;
  allowHosts?: string[];
  denyHosts?: string[];
};

export type EnvPolicyConfig = {
  allowlist?: string[];
  denylist?: string[];
  injected?: Record<string, string>;
};

export type PolicyConfig = {
  readRoots?: string[];
  writeRoots?: string[];
  process?: ProcessPolicyConfig;
  network?: NetworkPolicyConfig;
  env?: EnvPolicyConfig;
  maxDurationMs?: number;
  maxOutputBytes?: number;
};

export type EnvironmentConfig = {
  binaryPath?: string;
  backend?: BackendConfig;
  host?: HostConfig;
  worker?: WorkerConfig;
  workspace?: WorkspaceConfig;
  policy?: PolicyConfig;
  lifecycle?: LifecycleConfig;
  submitTimeoutMs?: number;
};

export type ToolCall = {
  toolName: string;
  arguments: Record<string, unknown>;
  cwd?: string;
  invocationId?: string;
  timeoutMs?: number;
  maxOutputBytes?: number;
  metadata?: Record<string, unknown>;
};

export type ToolSubmitOptions = Omit<ToolCall, 'toolName' | 'arguments'>;

export type EditToolArguments = {
  path: string;
  oldString: string;
  newString: string;
  replaceAll?: boolean;
};

export type ListFilesOptions = {
  cwd?: string;
};

export type StateEffect = {
  id: string;
  invocationId: string;
  kind: string;
  resourceType: string;
  uri: string;
  operation: 'read' | 'create' | 'update' | 'delete' | 'execute';
  summary?: string;
  reversible: boolean;
  occurredAt: string;
};

export type SubmitResult = {
  invocationId: string;
  sessionId: string;
  toolName: string;
  status: 'success' | 'error' | 'timeout' | 'cancelled' | 'policy_denied';
  output: string;
  error?: string | null;
  summary?: string | null;
  effects: StateEffect[];
  durationMs: number;
  metadata: Record<string, unknown>;
};

export type SessionInfo = {
  id: string;
  state: typeof SESSION_STATES[number];
  workspace: {
    root: string;
    logicalRoot: string;
    mode: typeof WORKSPACE_MODES[number];
    fresh: boolean;
    managed: boolean;
  };
  createdAt: string;
  expiresAt?: string | null;
  metadata: Record<string, unknown>;
};

export type ResourceRef = {
  resourceType: string;
  uri: string;
};

export type WorkspaceArtifactEntry = {
  logicalPath: string;
  archivePath: string;
  kind: string;
  linkTarget?: string | null;
  bytes?: number | null;
  hash?: string | null;
};

export type WorkspaceArtifact = {
  sessionId: string;
  artifact: ResourceRef;
  manifest: ResourceRef;
  format: string;
  bytes: number;
  hash: string;
  fileCount: number;
  directoryCount: number;
  symlinkCount: number;
  entries: WorkspaceArtifactEntry[];
  createdAt: string;
};

type CreateSessionResponse = {
  session: SessionInfo;
};

type CompletedEnvelope = {
  eventType?: string;
  type?: string;
  invocationId: string;
  sessionId: string;
  attemptId?: string | null;
  leaseToken?: string | null;
  result: SubmitResult;
  completedAt: string;
};

type FailedEnvelope = {
  eventType?: string;
  type?: string;
  invocationId: string;
  sessionId: string;
  attemptId?: string | null;
  leaseToken?: string | null;
  error: {
    code: string;
    message: string;
    retryable: boolean;
  };
  failedAt: string;
};

type ClaimEnvelope = {
  workerId: string;
  attemptId: string;
  leaseToken: string;
  claimedAt: string;
  request: ToolCall;
};

type ManagedProcess = {
  process: ChildProcess;
  name: string;
};

export class ExecutionerEnvironment {
  private constructor(
    private readonly config: RequiredRuntimeConfig,
    private readonly sessionInfo: SessionInfo,
    private readonly processes: ManagedProcess[],
  ) {}

  static async create(config: EnvironmentConfig = {}): Promise<ExecutionerEnvironment> {
    const runtime = await materializeConfig(config);
    const processes: ManagedProcess[] = [];
    let session: SessionInfo | undefined;

    try {
      if (runtime.host.kind === 'managed') {
        const hostProcess = spawnProcess(runtime.binaryPath, [
          'host',
          '--addr',
          `${runtime.host.host}:${runtime.host.port}`,
          '--state-dir',
          runtime.host.stateDir,
        ], 'executioner-host');
        processes.push(hostProcess);
        await waitForHealth(runtime.baseUrl, runtime.submitTimeoutMs);
      }

      await ensureFileQueue(runtime.queueDir);

      session = await createSession(runtime);

      if (runtime.worker.kind === 'managed') {
        const workerProcess = spawnProcess(runtime.binaryPath, [
          'worker',
          'run',
          '--id',
          runtime.worker.id,
          '--host-url',
          runtime.baseUrl,
          '--queue-dir',
          runtime.queueDir,
          '--idle-sleep-ms',
          String(runtime.worker.idleSleepMs),
        ], 'executioner-worker');
        processes.push(workerProcess);
      }

      return new ExecutionerEnvironment(runtime, session, processes);
    } catch (error) {
      await cleanupPartialCreate(runtime, processes, session);
      throw error;
    }
  }

  get session(): SessionInfo {
    return this.sessionInfo;
  }

  async submit(call: ToolCall): Promise<SubmitResult> {
    assertObject(call, 'tool call');
    rejectUnknownFields(call, [
      'toolName',
      'arguments',
      'cwd',
      'invocationId',
      'timeoutMs',
      'maxOutputBytes',
      'metadata',
    ], 'tool call');
    assertObject(call.arguments, 'tool call arguments');
    assertToolName(call.toolName);
    if (call.cwd !== undefined) {
      jsonString(call.cwd, 'cwd');
    }
    if (call.timeoutMs !== undefined) {
      jsonToolTimeout(call.timeoutMs, 'timeoutMs');
    }
    if (call.maxOutputBytes !== undefined) {
      jsonOutputLimit(call.maxOutputBytes, 'maxOutputBytes');
    }
    if (call.metadata !== undefined) {
      jsonObject(call.metadata, 'metadata');
    }
    const invocationId = call.invocationId ?? `inv_${randomUUID().replaceAll('-', '')}`;
    assertInvocationId(invocationId);
    const request = {
      invocationId,
      sessionId: this.sessionInfo.id,
      toolName: call.toolName,
      arguments: call.arguments,
      cwd: call.cwd ?? '/workspace',
      timeoutMs: call.timeoutMs,
      maxOutputBytes: call.maxOutputBytes,
      metadata: call.metadata ?? {},
    };
    assertSerializedJsonSize('tool invocation request', request, MAX_REQUEST_JSON_BYTES);

    await ensureFileQueue(this.config.queueDir);
    ensureInvocationIdUnused(this.config.queueDir, invocationId);
    await writeJsonAtomic(
      join(this.config.queueDir, 'pending', `${invocationId}.json`),
      request,
    );

    return waitForResult(
      this.config.queueDir,
      invocationId,
      this.sessionInfo.id,
      call.toolName,
      this.config.submitTimeoutMs,
    );
  }

  async edit(args: EditToolArguments, options: ToolSubmitOptions = {}): Promise<SubmitResult> {
    return this.submit({
      ...options,
      toolName: 'Edit',
      arguments: { ...args },
    });
  }

  async listFiles(options: ListFilesOptions = {}): Promise<string[]> {
    const optionsObject = jsonObject(options, 'listFiles options') as ListFilesOptions;
    rejectUnknownFields(optionsObject, ['cwd'], 'listFiles options');
    const result = await this.submit({
      toolName: 'List',
      arguments: {},
      cwd: optionsObject.cwd ?? '/workspace',
    });
    return parseListFilesResult(result);
  }

  async list(options: ListFilesOptions = {}): Promise<string[]> {
    return this.listFiles(options);
  }

  async exportWorkspace(): Promise<WorkspaceArtifact> {
    assertSessionId(this.sessionInfo.id);
    return parseWorkspaceArtifact(await postJson(
      `${this.config.baseUrl}sessions/${this.sessionInfo.id}/artifacts/workspace`,
      null,
    ));
  }

  async materializeWorkspaceArtifact(
    artifact: WorkspaceArtifact,
    destination: string,
  ): Promise<void> {
    await materializeWorkspaceArtifact(artifact, destination);
  }

  async close(): Promise<SessionInfo> {
    assertSessionId(this.sessionInfo.id);
    const workers = this.processes.filter((managed) => managed.name !== 'executioner-host');
    const hosts = this.processes.filter((managed) => managed.name === 'executioner-host');
    for (const managed of [...workers].reverse()) {
      await terminateProcess(managed);
    }

    let session: SessionInfo;
    try {
      session = this.config.lifecycle.destroyOnClose
        ? parseSessionInfo(await deleteJson(`${this.config.baseUrl}sessions/${this.sessionInfo.id}`))
        : parseSessionInfo(await postJson(`${this.config.baseUrl}sessions/${this.sessionInfo.id}/close`, null));
    } finally {
      for (const managed of [...hosts].reverse()) {
        await terminateProcess(managed);
      }
      if (this.config.lifecycle.cleanupQueueOnClose) {
        await cleanupQueueDir(this.config.queueDir, this.config.sdkCreatedQueueDir);
      }
      if (this.config.lifecycle.cleanupStateOnClose && this.config.host.kind === 'managed') {
        await cleanupStateDir(this.config.host.stateDir, this.config.sdkCreatedStateDir);
      }
    }

    return session;
  }
}

export async function materializeWorkspaceArtifact(
  artifact: WorkspaceArtifact,
  destination: string,
): Promise<void> {
  artifact = parseWorkspaceArtifact(artifact);
  await validateMaterializeDestination(destination);
  const parent = dirname(destination);
  if (!parent) {
    throw new Error('materialize destination must have a parent');
  }
  await validateNoSymlinkedParent(parent, 'materialize destination parent');
  const cleanupParent = resolve(parent);
  const cleanupStop = nearestExistingAncestor(cleanupParent);
  let staging: string | null = null;

  try {
    await mkdir(parent, { recursive: true });
    staging = join(parent, `.substrate-materialize-${randomUUID().replaceAll('-', '')}`);
    await mkdir(staging);
    await materializeWorkspaceArtifactInto(artifact, staging);
    if (await pathOccupied(destination)) {
      await rmdir(destination);
    }
    await fsRename(staging, destination);
  } catch (error) {
    if (staging != null) {
      await rm(staging, { recursive: true, force: true });
    }
    await cleanupCreatedEmptyParents(cleanupParent, cleanupStop);
    throw error;
  }
}

async function cleanupPartialCreate(
  runtime: RequiredRuntimeConfig,
  processes: ManagedProcess[],
  session?: SessionInfo,
): Promise<void> {
  const workers = processes.filter((managed) => managed.name !== 'executioner-host');
  const hosts = processes.filter((managed) => managed.name === 'executioner-host');
  for (const managed of [...workers].reverse()) {
    await terminateProcess(managed);
  }
  try {
    if (session && /^[A-Za-z0-9_-]{1,128}$/.test(session.id)) {
      try {
        if (runtime.lifecycle.destroyOnClose) {
          await deleteJson(`${runtime.baseUrl}sessions/${session.id}`);
        } else {
          await postJson(`${runtime.baseUrl}sessions/${session.id}/close`, null);
        }
      } catch {
        // Best effort cleanup during failed construction.
      }
    }
  } finally {
    for (const managed of [...hosts].reverse()) {
      await terminateProcess(managed);
    }
  }
  if (runtime.lifecycle.cleanupQueueOnClose) {
    await cleanupQueueDir(runtime.queueDir, runtime.sdkCreatedQueueDir);
  }
  if (runtime.lifecycle.cleanupStateOnClose && runtime.host.kind === 'managed') {
    await cleanupStateDir(runtime.host.stateDir, runtime.sdkCreatedStateDir);
  }
}

type RequiredRuntimeConfig = {
  binaryPath: string;
  queueDir: string;
  sdkCreatedQueueDir: boolean;
  sdkCreatedStateDir: boolean;
  baseUrl: string;
  host: { kind: 'managed'; stateDir: string; host: string; port: number } | { kind: 'http'; baseUrl: string };
  worker: { kind: 'managed'; id: string; idleSleepMs: number } | { kind: 'external' };
  workspace: WorkspaceConfig;
  policy: RequiredPolicyConfig;
  lifecycle: Required<LifecycleConfig>;
  submitTimeoutMs: number;
};

type RequiredPolicyConfig = {
  readRoots: string[];
  writeRoots: string[];
  process: Required<ProcessPolicyConfig>;
  network: Required<NetworkPolicyConfig>;
  env: Required<EnvPolicyConfig>;
  maxDurationMs: number;
  maxOutputBytes: number;
};

async function materializeConfig(config: EnvironmentConfig): Promise<RequiredRuntimeConfig> {
  const configObject = jsonObject(config, 'environment config') as EnvironmentConfig;
  rejectUnknownFields(configObject, [
    'binaryPath',
    'backend',
    'host',
    'worker',
    'workspace',
    'policy',
    'lifecycle',
    'submitTimeoutMs',
  ], 'environment config');
  const binaryPath = resolveBinaryPath(optionalNonEmptyString(configObject.binaryPath, 'binaryPath'));
  const workspace = (configObject.workspace === undefined
    ? { kind: 'new' as const }
    : jsonObject(configObject.workspace, 'workspace')) as WorkspaceConfig;
  rejectUnknownFields(workspace, ['kind', 'root'], 'workspace');
  requireKind(workspace.kind, 'workspace.kind', ['new', 'existing']);
  if (workspace.kind === 'existing') {
    const root = absolutePathString(workspace.root, 'workspace.root');
    await validateNoSymlinkedParent(dirname(root), 'workspace.root parent');
  }

  const backendConfig = (configObject.backend === undefined
    ? { kind: 'file' as const }
    : jsonObject(configObject.backend, 'backend')) as BackendConfig;
  rejectUnknownFields(backendConfig, ['kind', 'queueDir'], 'backend');
  requireKind(backendConfig.kind, 'backend.kind', ['file']);
  const explicitQueueDir = optionalNonEmptyString(backendConfig.queueDir, 'backend.queueDir');
  const submitTimeoutMs = configObject.submitTimeoutMs === undefined
    ? 30_000
    : jsonPositiveInteger(configObject.submitTimeoutMs, 'submitTimeoutMs');

  const hostConfig = (configObject.host === undefined
    ? { kind: 'managed' as const }
    : jsonObject(configObject.host, 'host')) as HostConfig;
  rejectUnknownFields(hostConfig, ['kind', 'stateDir', 'host', 'port', 'baseUrl'], 'host');
  requireKind(hostConfig.kind, 'host.kind', ['managed', 'http']);
  const explicitStateDir = hostConfig.kind === 'http'
    ? undefined
    : optionalNonEmptyString(hostConfig.stateDir, 'host.stateDir');
  const httpBaseUrl = hostConfig.kind === 'http'
    ? normalizeBaseUrl(nonEmptyString(hostConfig.baseUrl, 'host.baseUrl'))
    : undefined;
  const managedHostName = hostConfig.kind === 'managed'
    ? optionalNonEmptyString(hostConfig.host, 'host.host') ?? '127.0.0.1'
    : undefined;
  const managedPort = hostConfig.kind === 'managed' && hostConfig.port !== undefined
    ? jsonTcpPort(hostConfig.port, 'host.port')
    : undefined;
  const sdkCreatedStateDir = hostConfig.kind !== 'http' && (explicitStateDir === undefined || !existsSync(explicitStateDir));

  const lifecycleConfig = (configObject.lifecycle === undefined
    ? {}
    : jsonObject(configObject.lifecycle, 'lifecycle')) as LifecycleConfig;
  rejectUnknownFields(lifecycleConfig, [
    'destroyOnClose',
    'cleanupQueueOnClose',
    'cleanupStateOnClose',
  ], 'lifecycle');
  const lifecycle = {
    destroyOnClose: optionalBoolean(lifecycleConfig.destroyOnClose, 'destroyOnClose') ?? true,
    cleanupQueueOnClose: optionalBoolean(lifecycleConfig.cleanupQueueOnClose, 'cleanupQueueOnClose') ?? explicitQueueDir === undefined,
    cleanupStateOnClose: optionalBoolean(lifecycleConfig.cleanupStateOnClose, 'cleanupStateOnClose') ?? sdkCreatedStateDir,
  };

  const workerConfig = (configObject.worker === undefined
    ? { kind: 'managed' as const }
    : jsonObject(configObject.worker, 'worker')) as WorkerConfig;
  rejectUnknownFields(workerConfig, ['kind', 'id', 'idleSleepMs'], 'worker');
  requireKind(workerConfig.kind, 'worker.kind', ['managed', 'external']);
  const worker = workerConfig.kind === 'external'
    ? workerConfig
    : {
        kind: 'managed' as const,
        id: optionalIdentifierString(workerConfig.id, 'worker.id') ?? 'executioner-js-worker',
        idleSleepMs: workerConfig.idleSleepMs === undefined
          ? 10
          : jsonPositiveInteger(workerConfig.idleSleepMs, 'worker.idleSleepMs'),
      };
  const policy = materializePolicy(configObject.policy);
  const queueDir = explicitQueueDir ?? await mkdtemp(join(tmpdir(), 'executioner-queue-'));
  const sdkCreatedQueueDir = explicitQueueDir === undefined || !existsSync(queueDir);
  const stateDir = hostConfig.kind === 'managed'
    ? explicitStateDir ?? await mkdtemp(join(tmpdir(), 'executioner-state-'))
    : undefined;
  const host = hostConfig.kind === 'http'
    ? {
        kind: 'http' as const,
        baseUrl: httpBaseUrl!,
      }
    : {
        kind: 'managed' as const,
        stateDir: stateDir!,
        host: managedHostName!,
        port: managedPort ?? await freePort(),
      };
  const baseUrl = host.kind === 'http'
    ? host.baseUrl
    : `http://${host.host}:${host.port}/`;
  return {
    binaryPath,
    queueDir,
    sdkCreatedQueueDir,
    sdkCreatedStateDir,
    baseUrl,
    host,
    worker,
    workspace,
    policy,
    lifecycle,
    submitTimeoutMs,
  };
}

function materializePolicy(policy?: PolicyConfig): RequiredPolicyConfig {
  const policyObject = (policy === undefined ? {} : jsonObject(policy, 'policy')) as PolicyConfig;
  rejectUnknownFields(policyObject, [
    'readRoots',
    'writeRoots',
    'process',
    'network',
    'env',
    'maxDurationMs',
    'maxOutputBytes',
  ], 'policy');
  const process = (policyObject.process === undefined ? {} : jsonObject(policyObject.process, 'process')) as ProcessPolicyConfig;
  rejectUnknownFields(process, ['allowExec', 'allowedCommands', 'deniedCommands', 'maxProcesses'], 'process');
  const network = (policyObject.network === undefined ? {} : jsonObject(policyObject.network, 'network')) as NetworkPolicyConfig;
  rejectUnknownFields(network, ['enabled', 'allowHosts', 'denyHosts'], 'network');
  const env = (policyObject.env === undefined ? {} : jsonObject(policyObject.env, 'env')) as EnvPolicyConfig;
  rejectUnknownFields(env, ['allowlist', 'denylist', 'injected'], 'env');
  const networkEnabled = optionalBoolean(network?.enabled, 'network.enabled') ?? false;
  const networkAllowHosts = optionalStringArray(network?.allowHosts, 'network.allowHosts') ?? [];
  const networkDenyHosts = optionalStringArray(network?.denyHosts, 'network.denyHosts') ?? [];
  if (networkEnabled || networkAllowHosts.length > 0 || networkDenyHosts.length > 0) {
    throw new Error('network policy is not enforceable yet; leave network disabled and host lists empty');
  }
  const readRoots = optionalStringArray(policyObject.readRoots, 'readRoots') ?? ['/workspace'];
  const writeRoots = optionalStringArray(policyObject.writeRoots, 'writeRoots') ?? ['/workspace'];
  validatePolicyRoots(readRoots, 'policy.readRoots');
  validatePolicyRoots(writeRoots, 'policy.writeRoots');
  return {
    readRoots,
    writeRoots,
    process: {
      allowExec: optionalBoolean(process?.allowExec, 'process.allowExec') ?? false,
      allowedCommands: optionalStringArray(process?.allowedCommands, 'process.allowedCommands') ?? [],
      deniedCommands: optionalStringArray(process?.deniedCommands, 'process.deniedCommands') ?? [],
      maxProcesses: process?.maxProcesses === undefined ? undefined : jsonProcessCount(process.maxProcesses, 'process.maxProcesses'),
    },
    network: {
      enabled: networkEnabled,
      allowHosts: networkAllowHosts,
      denyHosts: networkDenyHosts,
    },
    env: {
      allowlist: optionalStringArray(env?.allowlist, 'env.allowlist') ?? [],
      denylist: optionalStringArray(env?.denylist, 'env.denylist') ?? [],
      injected: optionalStringRecord(env?.injected, 'env.injected') ?? {},
    },
    maxDurationMs: policyObject.maxDurationMs === undefined ? 300_000 : jsonToolTimeout(policyObject.maxDurationMs, 'maxDurationMs'),
    maxOutputBytes: policyObject.maxOutputBytes === undefined ? 100_000 : jsonOutputLimit(policyObject.maxOutputBytes, 'maxOutputBytes'),
  };
}

function validatePolicyRoots(roots: string[], label: string): void {
  for (const root of roots) {
    const trimmed = root.replace(/\/+$/u, '');
    if (
      trimmed.length === 0
      || !(trimmed === '/workspace' || trimmed.startsWith('/workspace/'))
      || root.includes('\0')
      || trimmed.split('/').some((component) => component === '.' || component === '..')
    ) {
      throw new Error(`${label} entries must be /workspace logical roots without . or .. components`);
    }
  }
}

async function createSession(config: RequiredRuntimeConfig): Promise<SessionInfo> {
  const workspace = config.workspace.kind === 'existing'
    ? {
        mode: 'existing',
        root: config.workspace.root,
        mountAsWorkspace: true,
      }
    : {
        mode: 'new',
        mountAsWorkspace: true,
      };

  const response = parseCreateSessionResponse(await postJson(`${config.baseUrl}sessions`, {
    workspace,
    policy: config.policy,
    metadata: {},
  }));
  assertSessionId(response.session.id);
  return response.session;
}

async function waitForResult(
  queueDir: string,
  invocationId: string,
  sessionId: string,
  toolName: string,
  timeoutMs: number,
): Promise<SubmitResult> {
  assertInvocationId(invocationId);
  const started = Date.now();
  const completedPath = join(queueDir, 'completed', `${invocationId}.json`);
  const failedPath = join(queueDir, 'failed', `${invocationId}.json`);
  const pendingPath = join(queueDir, 'pending', `${invocationId}.json`);
  const claimedPath = join(queueDir, 'claimed', `${invocationId}.json`);

  while (Date.now() - started < timeoutMs) {
    await ensureFileQueue(queueDir);
    const completed = await readTerminalJson<CompletedEnvelope>(queueDir, completedPath, pendingPath);
    if (completed) {
      assertCompletedEnvelopeMatches(completed, invocationId, sessionId);
      const result = parseSubmitResult(completed.result);
      if (result.toolName !== toolName) {
        await quarantineTerminal(queueDir, completedPath);
        continue;
      }
      assertTerminalLeaseMaterial(completed, invocationId, 'result');
      if (!await terminalMatchesClaim(claimedPath, completed, invocationId, sessionId, toolName)) {
        await quarantineTerminal(queueDir, completedPath);
        continue;
      }
      return result;
    }
    const failed = await readTerminalJson<FailedEnvelope>(queueDir, failedPath, pendingPath);
    if (failed) {
      assertFailedEnvelopeMatches(failed, invocationId, sessionId);
      assertTerminalLeaseMaterial(failed, invocationId, 'failure');
      if (!await terminalMatchesClaim(claimedPath, failed, invocationId, sessionId)) {
        await quarantineTerminal(queueDir, failedPath);
        continue;
      }
      throw new Error(`Executioner invocation failed: ${failed.error.message}`);
    }
    await sleep(10);
  }

  throw new Error(`Timed out waiting for Executioner invocation ${invocationId}`);
}

function assertCompletedEnvelopeMatches(
  completed: CompletedEnvelope,
  invocationId: string,
  sessionId: string,
): void {
  const object = jsonObject(completed, 'completed terminal envelope');
  rejectUnknownFields(object, [
    'type',
    'eventType',
    'invocationId',
    'sessionId',
    'attemptId',
    'leaseToken',
    'result',
    'completedAt',
  ], 'completed terminal envelope');
  jsonString(requiredField(object, 'completedAt', 'completed terminal envelope completedAt'), 'completed terminal envelope completedAt');
  const result = jsonObject(requiredField(object, 'result', 'completed terminal envelope result'), 'completed terminal envelope result');
  if (terminalEventType(completed) !== 'tool.invocation.completed') {
    throw new Error(`Executioner terminal result event type mismatch for invocation ${invocationId}`);
  }
  if (completed.invocationId !== invocationId || result.invocationId !== invocationId) {
    throw new Error(`Executioner terminal result invocation mismatch for invocation ${invocationId}`);
  }
  if (completed.sessionId !== sessionId || result.sessionId !== sessionId) {
    throw new Error(`Executioner terminal result session mismatch for invocation ${invocationId}`);
  }
}

function assertFailedEnvelopeMatches(
  failed: FailedEnvelope,
  invocationId: string,
  sessionId: string,
): void {
  const object = jsonObject(failed, 'failed terminal envelope');
  rejectUnknownFields(object, [
    'type',
    'eventType',
    'invocationId',
    'sessionId',
    'attemptId',
    'leaseToken',
    'error',
    'failedAt',
  ], 'failed terminal envelope');
  jsonString(requiredField(object, 'failedAt', 'failed terminal envelope failedAt'), 'failed terminal envelope failedAt');
  if (terminalEventType(failed) !== 'tool.invocation.failed') {
    throw new Error(`Executioner terminal failure event type mismatch for invocation ${invocationId}`);
  }
  if (failed.invocationId !== invocationId) {
    throw new Error(`Executioner terminal failure invocation mismatch for invocation ${invocationId}`);
  }
  if (failed.sessionId !== sessionId) {
    throw new Error(`Executioner terminal failure session mismatch for invocation ${invocationId}`);
  }
  const errorValue = object.error;
  if (
    typeof errorValue !== 'object'
    || errorValue == null
    || Array.isArray(errorValue)
  ) {
    throw new Error(`Executioner terminal failure malformed for invocation ${invocationId}`);
  }
  const error = errorValue as Record<string, unknown>;
  rejectUnknownFields(error, ['code', 'message', 'retryable'], 'failed terminal error');
  if (
    typeof error.code !== 'string'
    || error.code.trim().length === 0
    || typeof error.message !== 'string'
    || error.message.trim().length === 0
    || typeof error.retryable !== 'boolean'
  ) {
    throw new Error(`Executioner terminal failure malformed for invocation ${invocationId}`);
  }
}

function assertTerminalLeaseMaterial(
  envelope: { attemptId?: unknown; leaseToken?: unknown },
  invocationId: string,
  terminalKind: 'result' | 'failure',
): void {
  if (
    typeof envelope.attemptId !== 'string'
    || envelope.attemptId.length === 0
    || typeof envelope.leaseToken !== 'string'
    || envelope.leaseToken.length === 0
  ) {
    throw new Error(`Executioner terminal ${terminalKind} missing lease material for invocation ${invocationId}`);
  }
}

async function terminalMatchesClaim(
  claimedPath: string,
  envelope: { attemptId?: unknown; leaseToken?: unknown },
  invocationId: string,
  sessionId: string,
  toolName?: string,
): Promise<boolean> {
  let claim: ClaimEnvelope;
  try {
    claim = await readCappedJson<ClaimEnvelope>(claimedPath, MAX_QUEUE_JSON_BYTES, 'claimed lease');
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code !== 'ENOENT') {
      return false;
    }
    throw new Error(`Executioner terminal claim missing or malformed for invocation ${invocationId}: ${(error as Error).message}`);
  }
  let request: Record<string, unknown>;
  try {
    const claimObject = jsonObject(claim, 'claimed lease');
    rejectUnknownFields(claimObject, ['workerId', 'attemptId', 'leaseToken', 'claimedAt', 'request'], 'claimed lease');
    jsonString(requiredField(claimObject, 'workerId', 'claimed lease workerId'), 'claimed lease workerId');
    jsonString(requiredField(claimObject, 'claimedAt', 'claimed lease claimedAt'), 'claimed lease claimedAt');
    request = jsonObject(requiredField(claimObject, 'request', 'claimed lease request'), 'claimed lease request');
  } catch {
    return false;
  }
  if (claim.attemptId !== envelope.attemptId || claim.leaseToken !== envelope.leaseToken) {
    return false;
  }
  return claimedRequestMatches(request, invocationId, sessionId, toolName);
}

function terminalEventType(envelope: { eventType?: string; type?: string }): string | undefined {
  if (envelope.eventType != null && typeof envelope.eventType !== 'string') {
    return undefined;
  }
  if (envelope.type != null && typeof envelope.type !== 'string') {
    return undefined;
  }
  if (envelope.eventType != null && envelope.type != null && envelope.eventType !== envelope.type) {
    return undefined;
  }
  return envelope.eventType ?? envelope.type;
}

function claimedRequestMatches(
  request: Record<string, unknown>,
  invocationId: string,
  sessionId: string,
  toolName?: string,
): boolean {
  try {
    rejectUnknownFields(request, [
      'invocationId',
      'sessionId',
      'toolName',
      'arguments',
      'cwd',
      'timeoutMs',
      'maxOutputBytes',
      'idempotencyKey',
      'requiredCapabilities',
      'metadata',
    ], 'claimed lease request');
    const claimedInvocationId = jsonString(requiredField(request, 'invocationId', 'claimed lease request invocationId'), 'claimed lease request invocationId');
    const claimedSessionId = jsonString(requiredField(request, 'sessionId', 'claimed lease request sessionId'), 'claimed lease request sessionId');
    const claimedToolName = jsonString(requiredField(request, 'toolName', 'claimed lease request toolName'), 'claimed lease request toolName');
    jsonObject(requiredField(request, 'arguments', 'claimed lease request arguments'), 'claimed lease request arguments');
    if (request.cwd != null) {
      jsonString(request.cwd, 'claimed lease request cwd');
    }
    if (request.timeoutMs != null) {
      jsonToolTimeout(request.timeoutMs, 'claimed lease request timeoutMs');
    }
    if (request.maxOutputBytes != null) {
      jsonOutputLimit(request.maxOutputBytes, 'claimed lease request maxOutputBytes');
    }
    if (request.idempotencyKey != null) {
      jsonString(request.idempotencyKey, 'claimed lease request idempotencyKey');
    }
    if (request.requiredCapabilities != null) {
      for (const capability of jsonArray(request.requiredCapabilities, 'claimed lease request requiredCapabilities')) {
        const capabilityObject = jsonObject(capability, 'claimed lease request capability');
        rejectUnknownFields(capabilityObject, ['kind', 'scope'], 'claimed lease request capability');
        jsonString(requiredField(capabilityObject, 'kind', 'claimed lease request capability kind'), 'claimed lease request capability kind');
        jsonObject(capabilityObject.scope ?? {}, 'claimed lease request capability scope');
      }
    }
    jsonObject(request.metadata ?? {}, 'claimed lease request metadata');
    return claimedInvocationId === invocationId
      && claimedSessionId === sessionId
      && (toolName === undefined || claimedToolName === toolName);
  } catch {
    return false;
  }
}

function assertInvocationId(invocationId: string): void {
  if (!isSafeIdentifier(invocationId)) {
    throw new Error("Invalid invocationId: only ASCII letters, numbers, '-' and '_' are allowed");
  }
}

function assertSessionId(sessionId: string): void {
  if (!isSafeIdentifier(sessionId)) {
    throw new Error("Invalid session id: only ASCII letters, numbers, '-' and '_' are allowed");
  }
}

function optionalIdentifierString(value: unknown, label: string): string | undefined {
  const string = optionalNonEmptyString(value, label);
  if (string !== undefined && !isSafeIdentifier(string)) {
    throw new Error(`Invalid ${label}: only ASCII letters, numbers, '-' and '_' are allowed`);
  }
  return string;
}

function isSafeIdentifier(value: string): boolean {
  return /^[A-Za-z0-9_-]{1,128}$/.test(value);
}

function assertToolName(toolName: unknown): asserts toolName is string {
  if (typeof toolName !== 'string' || toolName.length === 0) {
    throw new Error('toolName must be a non-empty string');
  }
}

function ensureInvocationIdUnused(queueDir: string, invocationId: string): void {
  assertInvocationId(invocationId);
  for (const child of ['pending', 'claimed', 'completed', 'failed']) {
    try {
      lstatSync(join(queueDir, child, `${invocationId}.json`));
      throw new Error(`duplicate invocationId: ${invocationId}`);
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== 'ENOENT') {
        throw error;
      }
    }
  }
}

async function ensureFileQueue(queueDir: string): Promise<void> {
  await ensureQueueRootDir(queueDir);
  await Promise.all([
    ensureQueueStateDir(join(queueDir, 'pending')),
    ensureQueueStateDir(join(queueDir, 'claimed')),
    ensureQueueStateDir(join(queueDir, 'completed')),
    ensureQueueStateDir(join(queueDir, 'failed')),
    ensureQueueStateDir(join(queueDir, 'rejected')),
  ]);
}

async function cleanupQueueDir(queueDir: string, sdkCreatedQueueDir: boolean): Promise<void> {
  if (sdkCreatedQueueDir) {
    await removePathWithoutFollowing(queueDir);
    return;
  }
  const metadata = await lstat(queueDir).catch((error) => {
    if ((error as NodeJS.ErrnoException).code === 'ENOENT') {
      return null;
    }
    throw error;
  });
  if (metadata?.isSymbolicLink()) {
    await removePathWithoutFollowing(queueDir);
    return;
  }

  await Promise.all([
    removePathWithoutFollowing(join(queueDir, 'pending')),
    removePathWithoutFollowing(join(queueDir, 'claimed')),
    removePathWithoutFollowing(join(queueDir, 'completed')),
    removePathWithoutFollowing(join(queueDir, 'failed')),
    removePathWithoutFollowing(join(queueDir, 'rejected')),
  ]);
}

async function cleanupStateDir(stateDir: string, sdkCreatedStateDir: boolean): Promise<void> {
  if (sdkCreatedStateDir) {
    await removePathWithoutFollowing(stateDir);
  }
}

async function removePathWithoutFollowing(path: string): Promise<void> {
  let metadata;
  try {
    metadata = await lstat(path);
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === 'ENOENT') {
      return;
    }
    throw error;
  }
  if (metadata.isSymbolicLink() || metadata.isFile()) {
    await rm(path, { force: true });
  } else {
    await rm(path, { recursive: true, force: true });
  }
}

async function ensureQueueRootDir(path: string): Promise<void> {
  await validateNoSymlinkedParent(dirname(path), 'queue directory parent');
  await mkdir(path, { recursive: true });
  const metadata = await lstat(path);
  if (metadata.isSymbolicLink() || !metadata.isDirectory()) {
    throw new Error(`queue directory must be a real directory: ${path}`);
  }
}

async function ensureQueueStateDir(path: string): Promise<void> {
  await validateNoSymlinkedParent(dirname(path), 'queue state directory parent');
  await mkdir(path, { recursive: true });
  const metadata = await lstat(path);
  if (metadata.isSymbolicLink() || !metadata.isDirectory()) {
    throw new Error(`queue state directory must be a real directory: ${path}`);
  }
}

function resolveBinaryPath(binaryPath?: string): string {
  if (binaryPath) {
    return binaryPath;
  }
  if (process.env.EXECUTIONER_BIN) {
    return process.env.EXECUTIONER_BIN;
  }

  const packageBinary = join(
    dirname(fileURLToPath(import.meta.url)),
    '../../../target/release/executioner',
  );
  return existsSync(packageBinary) ? packageBinary : 'executioner';
}

function spawnProcess(binaryPath: string, args: string[], name: string): ManagedProcess {
  const child = spawn(binaryPath, args, {
    stdio: ['ignore', 'pipe', 'pipe'],
    env: process.env,
  });
  child.stdout?.on('data', () => {
    // Drain stdout so a noisy managed child cannot block on a full pipe.
  });
  child.stderr.on('data', (chunk) => {
    process.stderr.write(`[${name}] ${chunk.toString()}`);
  });
  child.on('error', (error) => {
    process.stderr.write(`[${name}] ${error.message}\n`);
  });
  return { process: child, name };
}

async function terminateProcess(managed: ManagedProcess): Promise<void> {
  if (managed.process.exitCode != null || managed.process.signalCode != null) {
    return;
  }
  managed.process.kill('SIGTERM');
  if (await waitForProcessExit(managed.process, 2_000)) {
    return;
  }
  if (managed.process.exitCode === null) {
    managed.process.kill('SIGKILL');
    await waitForProcessExit(managed.process, 2_000);
  }
}

async function waitForProcessExit(process: ChildProcess, timeoutMs: number): Promise<boolean> {
  if (process.exitCode != null || process.signalCode != null) {
    return true;
  }
  return new Promise((resolve) => {
    const timeout = setTimeout(() => {
      process.off('exit', onExit);
      resolve(false);
    }, timeoutMs);
    const onExit = () => {
      clearTimeout(timeout);
      resolve(true);
    };
    process.once('exit', onExit);
  });
}

async function waitForHealth(baseUrl: string, timeoutMs: number): Promise<void> {
  const started = Date.now();
  while (Date.now() - started < timeoutMs) {
    try {
      const response = await fetch(`${baseUrl}health`, { redirect: 'error' });
      if (response.ok) {
        return;
      }
    } catch {
      // Host is still starting.
    }
    await sleep(25);
  }
  throw new Error(`Timed out waiting for Executioner host at ${baseUrl}`);
}

async function postJson(url: string, body: unknown): Promise<unknown> {
  const response = await fetch(url, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(body),
    redirect: 'error',
  });
  if (!response.ok) {
    throw new Error(`Executioner host returned ${response.status}: ${await cappedResponseText(response, MAX_HTTP_ERROR_BODY_BYTES)}`);
  }
  return cappedResponseJson(response, MAX_HTTP_JSON_BODY_BYTES);
}

async function deleteJson(url: string): Promise<unknown> {
  const response = await fetch(url, { method: 'DELETE', redirect: 'error' });
  if (!response.ok) {
    throw new Error(`Executioner host returned ${response.status}: ${await cappedResponseText(response, MAX_HTTP_ERROR_BODY_BYTES)}`);
  }
  return cappedResponseJson(response, MAX_HTTP_JSON_BODY_BYTES);
}

async function cappedResponseText(response: Response, maxBytes: number): Promise<string> {
  const reader = response.body?.getReader();
  if (!reader) {
    return '';
  }
  const chunks: Uint8Array[] = [];
  let byteCount = 0;
  let truncated = false;
  while (true) {
    const { done, value } = await reader.read();
    if (done) {
      break;
    }
    const remaining = maxBytes - byteCount;
    if (value.byteLength > remaining) {
      chunks.push(value.subarray(0, remaining));
      truncated = true;
      break;
    }
    chunks.push(value);
    byteCount += value.byteLength;
  }
  let text = new TextDecoder().decode(Buffer.concat(chunks.map((chunk) => Buffer.from(chunk))));
  if (truncated) {
    text += '\n...[truncated]';
  }
  return text;
}

async function cappedResponseJson(response: Response, maxBytes: number): Promise<unknown> {
  const bytes = await cappedResponseBytes(response, maxBytes);
  return JSON.parse(new TextDecoder().decode(bytes));
}

async function cappedResponseBytes(response: Response, maxBytes: number): Promise<Buffer> {
  const reader = response.body?.getReader();
  if (!reader) {
    return Buffer.alloc(0);
  }
  const chunks: Uint8Array[] = [];
  let byteCount = 0;
  while (true) {
    const { done, value } = await reader.read();
    if (done) {
      break;
    }
    const remaining = maxBytes - byteCount;
    if (value.byteLength > remaining) {
      throw new Error(`response body exceeds maximum size of ${maxBytes} bytes`);
    }
    chunks.push(value);
    byteCount += value.byteLength;
  }
  return Buffer.concat(chunks.map((chunk) => Buffer.from(chunk)));
}

async function readJson<T>(path: string): Promise<T> {
  return JSON.parse(await readFile(path, 'utf8')) as T;
}

async function readCappedJson<T>(path: string, maxBytes: number, label: string): Promise<T> {
  const body = await readRegularFileNoFollow(path, label, maxBytes + 1);
  if (body.byteLength > maxBytes) {
    throw new Error(`${label} exceeds maximum size of ${maxBytes} bytes`);
  }
  return JSON.parse(new TextDecoder().decode(body)) as T;
}

async function readTerminalJson<T>(queueDir: string, path: string, pendingPath: string): Promise<T | null> {
  let metadata;
  try {
    metadata = await lstat(path);
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === 'ENOENT') {
      return null;
    }
    await quarantineTerminal(queueDir, path);
    return null;
  }
  if (!metadata.isFile()) {
    await quarantineTerminal(queueDir, path);
    return null;
  }
  if (metadata.size > MAX_QUEUE_JSON_BYTES) {
    await quarantineTerminal(queueDir, path);
    return null;
  }
  if (await pathOccupied(pendingPath)) {
    await quarantineTerminal(queueDir, path);
    return null;
  }
  try {
    return await readTerminalJsonNoFollow<T>(path);
  } catch {
    await quarantineTerminal(queueDir, path);
    return null;
  }
}

async function readTerminalJsonNoFollow<T>(path: string): Promise<T> {
  const noFollow = fsConstants.O_NOFOLLOW ?? 0;
  const file = await open(path, fsConstants.O_RDONLY | noFollow);
  try {
    const metadata = await file.stat();
    if (!metadata.isFile() || metadata.size > MAX_QUEUE_JSON_BYTES) {
      throw new Error('terminal file must be a regular bounded file');
    }
    const buffer = Buffer.alloc(metadata.size);
    const { bytesRead } = await file.read(buffer, 0, buffer.byteLength, 0);
    return JSON.parse(new TextDecoder().decode(buffer.subarray(0, bytesRead))) as T;
  } finally {
    await file.close();
  }
}

async function pathOccupied(path: string): Promise<boolean> {
  try {
    await lstat(path);
    return true;
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === 'ENOENT') {
      return false;
    }
    return true;
  }
}

async function quarantineTerminal(queueDir: string, path: string): Promise<void> {
  await ensureFileQueue(queueDir);
  await mkdir(join(queueDir, 'rejected'), { recursive: true });
  const name = parse(basename(path)).name || 'terminal';
  const rejectedPath = join(
    queueDir,
    'rejected',
    `${name}.terminal.rejected.${randomUUID().replaceAll('-', '')}.json`,
  );
  try {
    await fsRename(path, rejectedPath);
  } catch {
    await rm(path, { force: true });
  }
}

async function writeJsonAtomic(path: string, value: unknown): Promise<void> {
  const tmpPath = `${path}.tmp.${randomUUID().replaceAll('-', '')}`;
  try {
    const payload = JSON.stringify(value, null, 2);
    if (Buffer.byteLength(payload, 'utf8') > MAX_QUEUE_JSON_BYTES) {
      throw new Error(`queue json exceeds maximum size of ${MAX_QUEUE_JSON_BYTES} bytes`);
    }
    await writeFile(tmpPath, payload);
    await link(tmpPath, path);
  } finally {
    await rm(tmpPath, { force: true });
  }
}

async function fsRename(from: string, to: string): Promise<void> {
  const { rename } = await import('node:fs/promises');
  await rename(from, to);
}

function assertObject(value: unknown, label: string): asserts value is Record<string, unknown> {
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    throw new Error(`${label} must be a JSON object`);
  }
}

function jsonObject(value: unknown, label: string): Record<string, unknown> {
  assertObject(value, label);
  return value;
}

function rejectUnknownFields(object: Record<string, unknown>, allowed: string[], label: string): void {
  const allowedSet = new Set(allowed);
  const unknown = Object.keys(object).filter((key) => !allowedSet.has(key)).sort();
  if (unknown.length > 0) {
    throw new Error(`unknown ${label} field: ${unknown[0]}`);
  }
}

function requireKind(value: unknown, label: string, allowed: string[]): void {
  if (typeof value !== 'string' || !allowed.includes(value)) {
    throw new Error(`${label} must be one of: ${allowed.join(', ')}`);
  }
}

function optionalBoolean(value: unknown, label: string): boolean | undefined {
  if (value === undefined) {
    return undefined;
  }
  return jsonBoolean(value, label);
}

function optionalStringArray(value: unknown, label: string): string[] | undefined {
  if (value === undefined) {
    return undefined;
  }
  if (!Array.isArray(value) || !value.every((entry) => typeof entry === 'string')) {
    throw new Error(`${label} must be a string array`);
  }
  return value;
}

function optionalStringRecord(value: unknown, label: string): Record<string, string> | undefined {
  if (value === undefined) {
    return undefined;
  }
  const object = jsonObject(value, label);
  for (const [key, entry] of Object.entries(object)) {
    if (typeof entry !== 'string') {
      throw new Error(`${label}.${key} must be a string`);
    }
  }
  return object as Record<string, string>;
}

function requiredField(object: Record<string, unknown>, key: string, label: string): unknown {
  if (!Object.hasOwn(object, key) || object[key] == null) {
    throw new Error(`${label} is required`);
  }
  return object[key];
}

function jsonArray(value: unknown, label: string): unknown[] {
  if (!Array.isArray(value)) {
    throw new Error(`${label} must be a JSON array`);
  }
  return value;
}

function jsonString(value: unknown, label: string): string {
  if (typeof value !== 'string') {
    throw new Error(`${label} must be a string`);
  }
  return value;
}

function nonEmptyString(value: unknown, label: string): string {
  const string = jsonString(value, label);
  if (string.length === 0) {
    throw new Error(`${label} must be non-empty`);
  }
  return string;
}

function optionalNonEmptyString(value: unknown, label: string): string | undefined {
  if (value === undefined) {
    return undefined;
  }
  return nonEmptyString(value, label);
}

function absolutePathString(value: unknown, label: string): string {
  const string = nonEmptyString(value, label);
  if (!isAbsolute(string)) {
    throw new Error(`${label} must be absolute`);
  }
  return string;
}

function jsonOptionalString(value: unknown, label: string): string | null | undefined {
  if (value == null) {
    return value;
  }
  return jsonString(value, label);
}

function jsonBoolean(value: unknown, label: string): boolean {
  if (typeof value !== 'boolean') {
    throw new Error(`${label} must be a boolean`);
  }
  return value;
}

function jsonNumber(value: unknown, label: string): number {
  if (typeof value !== 'number' || !Number.isFinite(value)) {
    throw new Error(`${label} must be a number`);
  }
  return value;
}

function jsonInteger(value: unknown, label: string): number {
  const number = jsonNumber(value, label);
  if (!Number.isInteger(number)) {
    throw new Error(`${label} must be an integer`);
  }
  if (!Number.isSafeInteger(number)) {
    throw new Error(`${label} must be a safe integer`);
  }
  return number;
}

function jsonNonNegativeInteger(value: unknown, label: string): number {
  const number = jsonInteger(value, label);
  if (number < 0) {
    throw new Error(`${label} must be non-negative`);
  }
  return number;
}

function jsonPositiveInteger(value: unknown, label: string): number {
  const number = jsonNonNegativeInteger(value, label);
  if (number === 0) {
    throw new Error(`${label} must be positive`);
  }
  return number;
}

function jsonTcpPort(value: unknown, label: string): number {
  const port = jsonNonNegativeInteger(value, label);
  if (port < 1 || port > 65535) {
    throw new Error(`${label} must be between 1 and 65535`);
  }
  return port;
}

function jsonOutputLimit(value: unknown, label: string): number {
  const limit = jsonNonNegativeInteger(value, label);
  if (limit > MAX_OUTPUT_BYTES) {
    throw new Error(`${label} exceeds maximum supported output size of ${MAX_OUTPUT_BYTES} bytes`);
  }
  return limit;
}

function jsonToolTimeout(value: unknown, label: string): number {
  const timeout = jsonNonNegativeInteger(value, label);
  if (timeout === 0) {
    throw new Error(`${label} must be positive`);
  }
  if (timeout > MAX_TOOL_TIMEOUT_MS) {
    throw new Error(`${label} exceeds maximum supported tool timeout of ${MAX_TOOL_TIMEOUT_MS}ms`);
  }
  return timeout;
}

function jsonProcessCount(value: unknown, label: string): number {
  const count = jsonNonNegativeInteger(value, label);
  if (count > MAX_PROCESS_COUNT) {
    throw new Error(`${label} exceeds maximum supported process count of ${MAX_PROCESS_COUNT}`);
  }
  return count;
}

function assertSerializedJsonSize(label: string, value: unknown, maxBytes: number): void {
  if (Buffer.byteLength(JSON.stringify(value), 'utf8') > maxBytes) {
    throw new Error(`${label} exceeds maximum JSON size of ${maxBytes} bytes`);
  }
}

function parseCreateSessionResponse(value: unknown): CreateSessionResponse {
  const object = jsonObject(value, 'create session response');
  rejectUnknownFields(object, ['session'], 'create session response');
  return { session: parseSessionInfo(requiredField(object, 'session', 'session')) };
}

function parseSessionInfo(value: unknown): SessionInfo {
  const object = jsonObject(value, 'session');
  rejectUnknownFields(object, [
    'id',
    'state',
    'workspace',
    'policy',
    'createdAt',
    'expiresAt',
    'metadata',
  ], 'session');
  if (object.policy != null) {
    jsonObject(object.policy, 'session policy');
  }
  const state = jsonString(requiredField(object, 'state', 'session state'), 'session state');
  if (!SESSION_STATES.includes(state as typeof SESSION_STATES[number])) {
    throw new Error(`unknown session state: ${state}`);
  }
  const workspace = jsonObject(requiredField(object, 'workspace', 'session workspace'), 'session workspace');
  rejectUnknownFields(workspace, [
    'root',
    'logicalRoot',
    'mode',
    'fresh',
    'managed',
  ], 'session workspace');
  const mode = jsonString(requiredField(workspace, 'mode', 'workspace mode'), 'workspace mode');
  if (!WORKSPACE_MODES.includes(mode as typeof WORKSPACE_MODES[number])) {
    throw new Error(`unknown workspace mode: ${mode}`);
  }
  return {
    id: jsonString(requiredField(object, 'id', 'session id'), 'session id'),
    state: state as typeof SESSION_STATES[number],
    workspace: {
      root: jsonString(requiredField(workspace, 'root', 'workspace root'), 'workspace root'),
      logicalRoot: jsonString(requiredField(workspace, 'logicalRoot', 'workspace logicalRoot'), 'workspace logicalRoot'),
      mode: mode as typeof WORKSPACE_MODES[number],
      fresh: jsonBoolean(requiredField(workspace, 'fresh', 'workspace fresh'), 'workspace fresh'),
      managed: jsonBoolean(requiredField(workspace, 'managed', 'workspace managed'), 'workspace managed'),
    },
    createdAt: jsonString(requiredField(object, 'createdAt', 'session createdAt'), 'session createdAt'),
    expiresAt: jsonOptionalString(object.expiresAt, 'session expiresAt'),
    metadata: jsonObject(object.metadata ?? {}, 'session metadata'),
  };
}

function parseSubmitResult(value: unknown): SubmitResult {
  const object = jsonObject(value, 'submit result');
  rejectUnknownFields(object, [
    'invocationId',
    'sessionId',
    'toolName',
    'status',
    'output',
    'error',
    'summary',
    'effects',
    'durationMs',
    'metadata',
  ], 'submit result');
  const status = jsonString(requiredField(object, 'status', 'submit result status'), 'submit result status') as SubmitResult['status'];
  if (!['success', 'error', 'timeout', 'cancelled', 'policy_denied'].includes(status)) {
    throw new Error(`unknown submit result status: ${status}`);
  }
  return {
    invocationId: jsonString(requiredField(object, 'invocationId', 'submit result invocationId'), 'submit result invocationId'),
    sessionId: jsonString(requiredField(object, 'sessionId', 'submit result sessionId'), 'submit result sessionId'),
    toolName: jsonString(requiredField(object, 'toolName', 'submit result toolName'), 'submit result toolName'),
    status,
    output: jsonString(requiredField(object, 'output', 'submit result output'), 'submit result output'),
    error: jsonOptionalString(object.error, 'submit result error') ?? null,
    summary: jsonOptionalString(object.summary, 'submit result summary') ?? null,
    effects: jsonArray(requiredField(object, 'effects', 'submit result effects'), 'submit result effects').map(parseStateEffect),
    durationMs: jsonNonNegativeInteger(requiredField(object, 'durationMs', 'submit result durationMs'), 'submit result durationMs'),
    metadata: jsonObject(object.metadata ?? {}, 'submit result metadata'),
  };
}

function parseStateEffect(value: unknown): StateEffect {
  const object = jsonObject(value, 'state effect');
  const resource = jsonObject(requiredField(object, 'resource', 'state effect resource'), 'state effect resource');
  rejectUnknownFields(object, [
    'id',
    'invocationId',
    'kind',
    'resource',
    'operation',
    'before',
    'after',
    'summary',
    'reversible',
    'occurredAt',
  ], 'state effect');
  rejectUnknownFields(resource, ['resourceType', 'uri'], 'state effect resource');
  validateOptionalStateRef(object.before, 'state effect before');
  validateOptionalStateRef(object.after, 'state effect after');
  const operation = jsonString(requiredField(object, 'operation', 'state effect operation'), 'state effect operation') as StateEffect['operation'];
  if (!['read', 'create', 'update', 'delete', 'execute'].includes(operation)) {
    throw new Error(`unknown state effect operation: ${operation}`);
  }
  return {
    id: jsonString(requiredField(object, 'id', 'state effect id'), 'state effect id'),
    invocationId: jsonString(requiredField(object, 'invocationId', 'state effect invocationId'), 'state effect invocationId'),
    kind: jsonString(requiredField(object, 'kind', 'state effect kind'), 'state effect kind'),
    resourceType: jsonString(requiredField(resource, 'resourceType', 'state effect resourceType'), 'state effect resourceType'),
    uri: jsonString(requiredField(resource, 'uri', 'state effect uri'), 'state effect uri'),
    operation,
    summary: jsonOptionalString(object.summary, 'state effect summary') ?? undefined,
    reversible: jsonBoolean(requiredField(object, 'reversible', 'state effect reversible'), 'state effect reversible'),
    occurredAt: jsonString(requiredField(object, 'occurredAt', 'state effect occurredAt'), 'state effect occurredAt'),
  };
}

function validateOptionalStateRef(value: unknown, label: string): void {
  if (value == null) {
    return;
  }
  const object = jsonObject(value, label);
  rejectUnknownFields(object, ['hash', 'bytes', 'contentRef', 'snapshotRef', 'metadata'], label);
  if (object.hash != null) {
    jsonString(object.hash, `${label} hash`);
  }
  if (object.bytes != null) {
    jsonNonNegativeInteger(object.bytes, `${label} bytes`);
  }
  if (object.contentRef != null) {
    jsonString(object.contentRef, `${label} contentRef`);
  }
  if (object.snapshotRef != null) {
    jsonString(object.snapshotRef, `${label} snapshotRef`);
  }
  if (object.metadata != null) {
    jsonObject(object.metadata, `${label} metadata`);
  }
}

function parseResourceRef(value: unknown, label: string): ResourceRef {
  const object = jsonObject(value, label);
  rejectUnknownFields(object, ['resourceType', 'uri'], label);
  return {
    resourceType: jsonString(requiredField(object, 'resourceType', `${label} resourceType`), `${label} resourceType`),
    uri: jsonString(requiredField(object, 'uri', `${label} uri`), `${label} uri`),
  };
}

function parseWorkspaceArtifactEntry(value: unknown): WorkspaceArtifactEntry {
  const object = jsonObject(value, 'workspace artifact entry');
  rejectUnknownFields(object, ['logicalPath', 'archivePath', 'kind', 'linkTarget', 'bytes', 'hash'], 'workspace artifact entry');
  const bytes = object.bytes == null ? object.bytes : jsonNonNegativeInteger(object.bytes, 'artifact entry bytes');
  return {
    logicalPath: jsonString(requiredField(object, 'logicalPath', 'artifact entry logicalPath'), 'artifact entry logicalPath'),
    archivePath: jsonString(requiredField(object, 'archivePath', 'artifact entry archivePath'), 'artifact entry archivePath'),
    kind: jsonString(requiredField(object, 'kind', 'artifact entry kind'), 'artifact entry kind'),
    linkTarget: jsonOptionalString(object.linkTarget, 'artifact entry linkTarget'),
    bytes,
    hash: jsonOptionalString(object.hash, 'artifact entry hash'),
  };
}

function parseWorkspaceArtifact(value: unknown): WorkspaceArtifact {
  const object = jsonObject(value, 'workspace artifact');
  rejectUnknownFields(
    object,
    [
      'sessionId',
      'artifact',
      'manifest',
      'format',
      'bytes',
      'hash',
      'fileCount',
      'directoryCount',
      'symlinkCount',
      'entries',
      'createdAt',
    ],
    'workspace artifact',
  );
  const bytes = jsonNonNegativeInteger(requiredField(object, 'bytes', 'artifact bytes'), 'artifact bytes');
  if (bytes > MAX_WORKSPACE_ARTIFACT_BYTES) {
    throw new Error(`workspace artifact exceeds maximum size of ${MAX_WORKSPACE_ARTIFACT_BYTES} bytes`);
  }
  return {
    sessionId: jsonString(requiredField(object, 'sessionId', 'artifact sessionId'), 'artifact sessionId'),
    artifact: parseResourceRef(requiredField(object, 'artifact', 'artifact resource'), 'artifact resource'),
    manifest: parseResourceRef(requiredField(object, 'manifest', 'artifact manifest'), 'artifact manifest'),
    format: jsonString(requiredField(object, 'format', 'artifact format'), 'artifact format'),
    bytes,
    hash: jsonString(requiredField(object, 'hash', 'artifact hash'), 'artifact hash'),
    fileCount: jsonNonNegativeInteger(requiredField(object, 'fileCount', 'artifact fileCount'), 'artifact fileCount'),
    directoryCount: jsonNonNegativeInteger(requiredField(object, 'directoryCount', 'artifact directoryCount'), 'artifact directoryCount'),
    symlinkCount: jsonNonNegativeInteger(requiredField(object, 'symlinkCount', 'artifact symlinkCount'), 'artifact symlinkCount'),
    entries: jsonArray(requiredField(object, 'entries', 'artifact entries'), 'artifact entries').map(parseWorkspaceArtifactEntry),
    createdAt: jsonString(requiredField(object, 'createdAt', 'artifact createdAt'), 'artifact createdAt'),
  };
}

function normalizeBaseUrl(url: string): string {
  if (url.startsWith('http:///') || url.startsWith('https:///')) {
    throw new Error('invalid host.baseUrl: host is required');
  }
  const parsed = new URL(url);
  if (parsed.protocol !== 'http:' && parsed.protocol !== 'https:') {
    throw new Error(`invalid host.baseUrl: unsupported protocol ${parsed.protocol}`);
  }
  if (!parsed.hostname) {
    throw new Error('invalid host.baseUrl: host is required');
  }
  if (parsed.username || parsed.password) {
    throw new Error('invalid host.baseUrl: credentials are not allowed');
  }
  if (parsed.search || parsed.hash) {
    throw new Error('invalid host.baseUrl: query strings and fragments are not allowed');
  }
  if (!parsed.pathname.endsWith('/')) {
    parsed.pathname = `${parsed.pathname}/`;
  }
  return parsed.toString();
}

function parseListFilesResult(result: SubmitResult): string[] {
  if (result.status !== 'success') {
    throw new Error(`List failed with status ${result.status}: ${result.error ?? result.output}`);
  }
  const truncated = result.metadata.truncated;
  if (truncated !== undefined && typeof truncated !== 'boolean') {
    throw new Error('List truncated metadata must be a boolean');
  }
  if (truncated === true) {
    throw new Error('List result was truncated; refusing partial directory listing');
  }
  const entries = result.metadata.entries;
  if (entries !== undefined && !Array.isArray(entries)) {
    throw new Error('List metadata entries must be an array');
  }
  if (entries !== undefined && !entries.every((entry) => typeof entry === 'string')) {
    throw new Error('List metadata entries must be strings');
  }
  if (entries !== undefined) {
    return entries;
  }
  return parseListFilesOutput(result.output);
}

function parseListFilesOutput(output: string): string[] {
  if (output.split('\n').some((line) => line.startsWith('...[truncated'))) {
    throw new Error('List result was truncated; refusing partial directory listing');
  }
  return output
    .split('\n')
    .filter((line) => line.length > 0 && !line.startsWith('...[truncated'));
}

async function materializeWorkspaceArtifactInto(
  artifact: WorkspaceArtifact,
  destination: string,
): Promise<void> {
  validateArtifactHeader(artifact);
  const tarPath = pathFromFileUri(artifact.artifact.uri);
  const tarBytes = await readRegularFileNoFollow(tarPath, 'workspace artifact path', MAX_WORKSPACE_ARTIFACT_BYTES + 1);
  if (tarBytes.byteLength > MAX_WORKSPACE_ARTIFACT_BYTES) {
    throw new Error(`workspace artifact exceeds maximum size of ${MAX_WORKSPACE_ARTIFACT_BYTES} bytes`);
  }
  if (tarBytes.byteLength !== artifact.bytes) {
    throw new Error('workspace artifact file size does not match metadata');
  }
  const actualHash = sha256(tarBytes);
  if (actualHash !== artifact.hash) {
    throw new Error('workspace artifact hash mismatch');
  }
  await validateManifestResourceIfAvailable(artifact);

  const entries = validateManifestEntries(artifact);
  const seenArchiveEntries = new Set<string>();
  for (const tarEntry of readTarEntries(tarBytes)) {
    const archivePath = safeArchivePath(tarEntry.name);
    const manifestEntry = entries.get(archivePath);
    if (!manifestEntry) {
      throw new Error(`artifact contains entry missing from manifest: ${archivePath}`);
    }
    if (seenArchiveEntries.has(archivePath)) {
      throw new Error(`duplicate artifact entry: ${archivePath}`);
    }
    seenArchiveEntries.add(archivePath);

    const targetPath = join(destination, archivePath);
    if (manifestEntry.kind === 'directory' && tarEntry.kind === 'directory') {
      await mkdir(targetPath, { recursive: true });
    } else if (manifestEntry.kind === 'file' && tarEntry.kind === 'file') {
      await mkdir(dirname(targetPath), { recursive: true });
      await writeFile(targetPath, tarEntry.data, { flag: 'wx' });
      if (
        manifestEntry.bytes !== tarEntry.data.byteLength
        || manifestEntry.hash !== sha256(tarEntry.data)
      ) {
        throw new Error(`artifact entry hash or byte length mismatch: ${archivePath}`);
      }
    } else {
      throw new Error(`artifact entry type does not match manifest: ${archivePath}`);
    }
  }

  for (const entry of artifact.entries) {
    if (entry.kind === 'file' && !seenArchiveEntries.has(entry.archivePath)) {
      throw new Error(`manifest file missing from artifact: ${entry.archivePath}`);
    }
    if (entry.kind === 'directory' && !seenArchiveEntries.has(entry.archivePath)) {
      throw new Error(`manifest directory missing from artifact: ${entry.archivePath}`);
    }
  }

  for (const entry of artifact.entries) {
    if (entry.kind !== 'symlink') {
      continue;
    }
    const archivePath = safeArchivePath(entry.archivePath);
    if (typeof entry.linkTarget !== 'string') {
      throw new Error(`manifest symlink entry is incomplete: ${entry.archivePath}`);
    }
    validateMaterializedSymlinkTarget(archivePath, entry.linkTarget);
    const linkPath = join(destination, archivePath);
    await mkdir(dirname(linkPath), { recursive: true });
    await symlink(entry.linkTarget, linkPath);
  }
}

async function readRegularFileNoFollow(path: string, label: string, maxBytes?: number): Promise<Buffer> {
  const noFollow = fsConstants.O_NOFOLLOW ?? 0;
  let file;
  try {
    const pathMetadata = await lstat(path);
    if (pathMetadata.isSymbolicLink() || !pathMetadata.isFile()) {
      throw new Error(`${label} must be a regular file`);
    }
    file = await open(path, fsConstants.O_RDONLY | noFollow);
    const metadata = await file.stat();
    if (!metadata.isFile()) {
      throw new Error(`${label} must be a regular file`);
    }
    const size = maxBytes === undefined ? metadata.size : Math.min(metadata.size, maxBytes);
    const buffer = Buffer.alloc(size);
    const { bytesRead } = await file.read(buffer, 0, buffer.byteLength, 0);
    return buffer.subarray(0, bytesRead);
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === 'ELOOP') {
      throw new Error(`${label} must be a regular file`);
    }
    throw error;
  } finally {
    await file?.close();
  }
}

async function validateMaterializeDestination(destination: string): Promise<void> {
  let metadata;
  try {
    metadata = await lstat(destination);
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === 'ENOENT') {
      return;
    }
    throw error;
  }
  if (metadata.isSymbolicLink()) {
    throw new Error('materialize destination must not be a symlink');
  }
  if (!metadata.isDirectory()) {
    throw new Error('materialize destination must be a directory');
  }
  if ((await readdir(destination)).length > 0) {
    throw new Error('materialize destination must be empty');
  }
}

async function validateNoSymlinkedParent(parent: string, label: string): Promise<void> {
  let current = resolve(parent);
  while (true) {
    try {
      const metadata = await lstat(current);
      if (metadata.isSymbolicLink() && !isPlatformRootSymlink(current)) {
        throw new Error(`${label} must not contain symlinks`);
      }
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== 'ENOENT') {
        throw error;
      }
    }
    const next = dirname(current);
    if (next === current) {
      return;
    }
    current = next;
  }
}

function isPlatformRootSymlink(path: string): boolean {
  return path === '/var' || path === '/tmp' || path === '/etc';
}

function nearestExistingAncestor(path: string): string | null {
  let current = resolve(path);
  while (true) {
    if (existsSync(current)) {
      return current;
    }
    const next = dirname(current);
    if (next === current) {
      return null;
    }
    current = next;
  }
}

async function cleanupCreatedEmptyParents(parent: string, stop: string | null): Promise<void> {
  let current = resolve(parent);
  while (true) {
    if (stop != null && current === stop) {
      return;
    }
    try {
      await rmdir(current);
    } catch {
      return;
    }
    const next = dirname(current);
    if (next === current) {
      return;
    }
    current = next;
  }
}

async function validateManifestResourceIfAvailable(artifact: WorkspaceArtifact): Promise<void> {
  if (!artifact.manifest.uri.startsWith('file://')) {
    throw new Error('workspace artifact manifest uri must be file://');
  }
  const manifestPath = pathFromFileUri(artifact.manifest.uri);
  try {
    await lstat(manifestPath);
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === 'ENOENT') {
      return;
    }
    throw error;
  }
  const manifest = parseWorkspaceArtifact(await readCappedJson<unknown>(
    manifestPath,
    MAX_WORKSPACE_ARTIFACT_MANIFEST_BYTES,
    'workspace artifact manifest resource',
  ));
  if (JSON.stringify(manifest) !== JSON.stringify(artifact)) {
    throw new Error('workspace artifact manifest resource does not match artifact metadata');
  }
}

function validateManifestEntries(artifact: WorkspaceArtifact): Map<string, WorkspaceArtifactEntry> {
  validateArtifactHeader(artifact);
  validateManifestCounts(artifact);
  const entries = new Map<string, WorkspaceArtifactEntry>();
  let totalFileBytes = 0;
  for (const entry of artifact.entries) {
    const archivePath = safeArchivePath(entry.archivePath);
    if (archivePath !== entry.archivePath) {
      throw new Error(`manifest entry path is not canonical: ${entry.archivePath}`);
    }
    if (!entry.logicalPath.startsWith('/workspace/')) {
      throw new Error(`manifest logical path must be under /workspace: ${entry.logicalPath}`);
    }
    if (entry.logicalPath !== `/workspace/${archivePath}`) {
      throw new Error(`manifest logical path does not match archive path: ${entry.archivePath}`);
    }

    if (entry.kind === 'file') {
      if (typeof entry.bytes !== 'number' || typeof entry.hash !== 'string' || entry.linkTarget != null) {
        throw new Error(`manifest file entry is incomplete: ${entry.archivePath}`);
      }
      totalFileBytes += entry.bytes;
      if (entry.bytes > MAX_WORKSPACE_ARTIFACT_BYTES || totalFileBytes > MAX_WORKSPACE_ARTIFACT_BYTES) {
        throw new Error(`workspace artifact manifest file bytes exceed maximum size of ${MAX_WORKSPACE_ARTIFACT_BYTES} bytes`);
      }
    } else if (entry.kind === 'directory') {
      if (entry.bytes != null || entry.hash != null || entry.linkTarget != null) {
        throw new Error(`manifest directory entry has file metadata: ${entry.archivePath}`);
      }
    } else if (entry.kind === 'symlink') {
      if (typeof entry.linkTarget !== 'string') {
        throw new Error(`manifest symlink entry is incomplete: ${entry.archivePath}`);
      }
      if (entry.bytes != null || entry.hash != null) {
        throw new Error(`manifest symlink entry has file metadata: ${entry.archivePath}`);
      }
      validateMaterializedSymlinkTarget(archivePath, entry.linkTarget);
    } else {
      throw new Error(`unknown manifest entry kind: ${entry.kind}`);
    }

    if (entries.has(archivePath)) {
      throw new Error(`duplicate manifest entry: ${entry.archivePath}`);
    }
    entries.set(archivePath, entry);
  }
  validateManifestParentDirectories(entries);
  return entries;
}

function validateArtifactHeader(artifact: WorkspaceArtifact): void {
  if (artifact.format !== 'tar') {
    throw new Error(`unsupported workspace artifact format: ${artifact.format}`);
  }
  if (artifact.artifact.resourceType !== 'artifact') {
    throw new Error('workspace artifact resource type must be artifact');
  }
  if (artifact.manifest.resourceType !== 'artifact_manifest') {
    throw new Error('workspace artifact manifest resource type must be artifact_manifest');
  }
  if (artifact.bytes > MAX_WORKSPACE_ARTIFACT_BYTES) {
    throw new Error(`workspace artifact exceeds maximum size of ${MAX_WORKSPACE_ARTIFACT_BYTES} bytes`);
  }
}

function validateManifestCounts(artifact: WorkspaceArtifact): void {
  if (artifact.entries.length > MAX_WORKSPACE_ARTIFACT_ENTRIES) {
    throw new Error(`workspace artifact exceeds maximum entry count of ${MAX_WORKSPACE_ARTIFACT_ENTRIES}`);
  }
  const fileCount = artifact.entries.filter((entry) => entry.kind === 'file').length;
  const directoryCount = artifact.entries.filter((entry) => entry.kind === 'directory').length;
  const symlinkCount = artifact.entries.filter((entry) => entry.kind === 'symlink').length;
  if (
    fileCount !== artifact.fileCount
    || directoryCount !== artifact.directoryCount
    || symlinkCount !== artifact.symlinkCount
  ) {
    throw new Error('manifest counts do not match entries');
  }
}

function validateManifestParentDirectories(entries: Map<string, WorkspaceArtifactEntry>): void {
  for (const archivePath of entries.keys()) {
    let parent = posix.dirname(archivePath);
    while (parent !== '.' && parent !== '/') {
      const parentEntry = entries.get(parent);
      if (!parentEntry) {
        throw new Error(`manifest parent directory missing for ${archivePath}: ${parent}`);
      }
      if (parentEntry.kind !== 'directory') {
        throw new Error(`manifest parent path is not a directory for ${archivePath}: ${parent}`);
      }
      parent = posix.dirname(parent);
    }
  }
}

function safeArchivePath(path: string): string {
  if (path.includes('\\') || path.includes('\0')) {
    throw new Error(`unsafe artifact path: ${path}`);
  }
  if (posix.isAbsolute(path)) {
    throw new Error(`unsafe artifact path: ${path}`);
  }
  const parts: string[] = [];
  for (const part of path.split('/')) {
    if (!part || part === '.') {
      continue;
    }
    if (part === '..') {
      throw new Error(`unsafe artifact path: ${path}`);
    }
    parts.push(part);
  }
  if (parts.length === 0) {
    throw new Error('artifact path must not be empty');
  }
  if (parts.length > MAX_WORKSPACE_ARTIFACT_DEPTH) {
    throw new Error(`artifact path exceeds maximum path depth of ${MAX_WORKSPACE_ARTIFACT_DEPTH}: ${path}`);
  }
  return parts.join('/');
}

function validateMaterializedSymlinkTarget(archivePath: string, target: string): void {
  if (target.includes('\\') || target.includes('\0') || posix.isAbsolute(target)) {
    throw new Error(`unsafe symlink target in manifest: ${archivePath}`);
  }
  const normalized = normalizePosixParts([...posix.dirname(archivePath).split('/'), ...target.split('/')]);
  if (!normalized) {
    throw new Error(`unsafe symlink target in manifest: ${archivePath}`);
  }
}

function normalizePosixParts(parts: string[]): string[] | null {
  const normalized: string[] = [];
  for (const part of parts) {
    if (!part || part === '.') {
      continue;
    }
    if (part === '..') {
      if (normalized.length === 0) {
        return null;
      }
      normalized.pop();
    } else {
      normalized.push(part);
    }
  }
  return normalized;
}

function pathFromFileUri(uri: string): string {
  if (!uri.startsWith('file://')) {
    throw new Error('artifact uri must be file://');
  }
  const path = uri.slice('file://'.length);
  if (!path.startsWith('/')) {
    throw new Error('artifact file uri must be absolute');
  }
  if (path.startsWith('//') || path.includes('?') || path.includes('#')) {
    throw new Error('artifact file uri must be a local file:/// absolute path without authority, query, or fragment');
  }
  return path;
}

function sha256(data: Buffer | Uint8Array): string {
  return `sha256:${createHash('sha256').update(data).digest('hex')}`;
}

type TarEntry = {
  name: string;
  kind: 'file' | 'directory' | 'other';
  data: Buffer;
};

function readTarEntries(archive: Buffer): TarEntry[] {
  const entries: TarEntry[] = [];
  let offset = 0;
  let pendingLongName: string | null = null;
  let sawEndOfArchive = false;
  while (offset + 512 <= archive.byteLength) {
    const header = archive.subarray(offset, offset + 512);
    if (header.every((byte) => byte === 0)) {
      if (offset + 1024 > archive.byteLength) {
        throw new Error('artifact tar is missing end-of-archive marker');
      }
      const secondZeroBlock = archive.subarray(offset + 512, offset + 1024);
      if (!secondZeroBlock.every((byte) => byte === 0)) {
        throw new Error('artifact tar is missing end-of-archive marker');
      }
      if (!archive.subarray(offset).every((byte) => byte === 0)) {
        throw new Error('artifact tar contains trailing data after end of archive');
      }
      sawEndOfArchive = true;
      break;
    }
    const name = readTarString(header, 0, 100);
    const prefix = readTarString(header, 345, 155);
    const storedChecksum = readTarOctal(header, 148, 8, 'checksum');
    const actualChecksum = tarHeaderChecksum(header);
    if (storedChecksum !== actualChecksum) {
      throw new Error(`invalid tar header checksum: ${name}`);
    }
    const size = readTarOctal(header, 124, 12, 'size');
    const typeflag = String.fromCharCode(header[156] ?? 0);
    const fullName = prefix ? `${prefix}/${name}` : name;
    const dataStart = offset + 512;
    const dataEnd = dataStart + size;
    if (dataEnd > archive.byteLength) {
      throw new Error(`truncated artifact entry: ${fullName}`);
    }
    const data = archive.subarray(dataStart, dataEnd);
    if (typeflag === 'L') {
      pendingLongName = readTarPayloadString(data);
      offset = dataStart + Math.ceil(size / 512) * 512;
      continue;
    }
    const entryName = pendingLongName ?? fullName;
    pendingLongName = null;
    const kind = typeflag === '5' ? 'directory' : (typeflag === '0' || typeflag === '\0') ? 'file' : 'other';
    entries.push({
      name: entryName,
      kind,
      data,
    });
    offset = dataStart + Math.ceil(size / 512) * 512;
  }
  if (pendingLongName != null) {
    throw new Error('artifact tar long-name entry is missing a following entry');
  }
  if (!sawEndOfArchive) {
    throw new Error('artifact tar is missing end-of-archive marker');
  }
  return entries;
}

function readTarString(buffer: Buffer, offset: number, length: number): string {
  const slice = buffer.subarray(offset, offset + length);
  const nul = slice.indexOf(0);
  try {
    return FATAL_UTF8_DECODER.decode(slice.subarray(0, nul >= 0 ? nul : undefined));
  } catch {
    throw new Error('artifact tar string is not valid UTF-8');
  }
}

function readTarPayloadString(buffer: Buffer): string {
  const nul = buffer.indexOf(0);
  try {
    return FATAL_UTF8_DECODER.decode(buffer.subarray(0, nul >= 0 ? nul : undefined));
  } catch {
    throw new Error('artifact tar string is not valid UTF-8');
  }
}

function readTarOctal(buffer: Buffer, offset: number, length: number, label: string): number {
  const raw = readTarString(buffer, offset, length).trim();
  if (!raw) {
    return 0;
  }
  if (!/^[0-7]+$/.test(raw)) {
    throw new Error(`invalid tar ${label} field`);
  }
  return Number.parseInt(raw, 8);
}

function tarHeaderChecksum(header: Buffer): number {
  let checksum = 0;
  for (let index = 0; index < header.byteLength; index += 1) {
    checksum += index >= 148 && index < 156 ? 0x20 : header[index] ?? 0;
  }
  return checksum;
}

async function freePort(): Promise<number> {
  return new Promise((resolve, reject) => {
    const server = createServer();
    server.on('error', reject);
    server.listen(0, '127.0.0.1', () => {
      const address = server.address();
      if (!address || typeof address === 'string') {
        server.close(() => reject(new Error('Unable to allocate local port')));
        return;
      }
      const port = address.port;
      server.close(() => resolve(port));
    });
  });
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
