# Packaging

Substrate packages keep the language SDKs separate from the Rust runtime.

The Python and TypeScript SDKs are pure language packages. Installing them must
not compile Rust, run Cargo, or require a local checkout of this repository.
Local managed execution is provided by an `executioner` runtime binary that is
discovered by the SDK at startup.

## Runtime Discovery

SDKs resolve the runtime binary in this order:

1. The explicit `binaryPath` / `binary_path` option.
2. The `EXECUTIONER_BIN` environment variable.
3. A `bin/executioner` file bundled inside the installed SDK package.
4. A platform sidecar runtime package, if one is installed.
5. `executioner` on `PATH`.

Remote-host usage does not require a local runtime binary:

```ts
await ExecutionerEnvironment.create({
  host: { kind: 'http', baseUrl: 'http://127.0.0.1:8765' },
  worker: { kind: 'external' },
});
```

Clients that are joining an environment created elsewhere should use attach
instead. Attach is also remote-only and does not require a runtime binary,
worker process, or file broker queue:

```ts
await ExecutionerEnvironment.attach({
  host: { kind: 'http', baseUrl: 'http://127.0.0.1:8765' },
  environmentId: 'env_shared',
});
```

Local managed usage starts the runtime binary automatically:

```ts
await ExecutionerEnvironment.create({ workspace: { kind: 'new' } });
```

## Registry Shape

Publish the SDK packages without a Rust build step:

- npm: `@substrate/sdk`
- PyPI: `substrate`

For npm, publish platform sidecar runtime packages. `@substrate/sdk` lists them
as optional dependencies, and each sidecar package should declare its `os` and
`cpu` fields so package managers install only the matching runtime:

- npm: `@substrate/executioner-darwin-arm64`,
  `@substrate/executioner-darwin-x64`, `@substrate/executioner-linux-arm64`,
  `@substrate/executioner-linux-x64`, `@substrate/executioner-win32-x64`

Each sidecar package should contain the prebuilt binary at `bin/executioner`
or `bin/executioner.exe`.

For Python, keep `substrate` pure by default. There are two supported local
runtime paths:

- Publish platform-specific `substrate` wheels that include
  `executioner_sdk/bin/executioner`. The included `setup.py` marks those wheels
  as platform-specific when that directory exists.
- Publish a `substrate-runtime` package exposing
  `substrate_runtime/bin/executioner`, then install local mode with
  `pip install "substrate[local]"`.

Both approaches keep Rust out of SDK installation: users receive a prebuilt
binary, not a local compile.
