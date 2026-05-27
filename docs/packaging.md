# Packaging

Substrate packages keep the language SDKs separate from the Rust runtime.

The Python and TypeScript SDKs are pure language packages. Installing them must
not compile Rust, run Cargo, or require a local checkout of this repository.
Local managed execution is provided by a `substrate-runtime` binary that is
discovered by the SDK at startup.

## Runtime Discovery

SDKs resolve the runtime binary in this order:

1. The explicit `binaryPath` / `binary_path` option.
2. The `SUBSTRATE_RUNTIME_BIN` environment variable.
3. A private/dev `bin/substrate-runtime` file inside the installed SDK package.
4. A platform sidecar runtime package, if one is installed.
5. `substrate-runtime` on `PATH`.

Public registry SDK artifacts should not ship the private/dev bundled binary.
Registry publishing keeps the SDK and runtime packages separate.

Remote-host usage does not require a local runtime binary:

```ts
await Environment.create({
  host: { kind: 'http', baseUrl: 'http://127.0.0.1:8765' },
  worker: { kind: 'external' },
});
```

Clients that are joining an environment created elsewhere should use attach
instead. Attach is also remote-only and does not require a runtime binary,
worker process, or file broker queue:

```ts
await Environment.attach({
  host: { kind: 'http', baseUrl: 'http://127.0.0.1:8765' },
  environmentId: 'env_shared',
});
```

Local managed usage starts the runtime binary automatically:

```ts
await Environment.create({ workspace: { kind: 'new' } });
```

## Registry Shape

Publish the SDK packages without a Rust build step:

- npm: `@substrate/sdk`
- PyPI: `substrate-sdk`

The Python distribution name is `substrate-sdk`, while the import surface stays
`substrate`:

```py
from substrate import Environment
```

For npm, publish platform sidecar runtime packages. Each sidecar package should
declare its `os` and `cpu` fields so package managers install only the matching
runtime:

- npm: `@substrate/runtime-darwin-arm64`,
  `@substrate/runtime-darwin-x64`, `@substrate/runtime-linux-arm64`,
  `@substrate/runtime-linux-x64`, `@substrate/runtime-win32-x64`

Each sidecar package should contain the prebuilt binary at `bin/substrate-runtime`
or `bin/substrate-runtime.exe`.

The SDK resolver will use a sidecar package if it is installed. `@substrate/sdk`
declares the platform packages that have been published as optional
dependencies.

For Python, keep the SDK wheel pure (`py3-none-any`) and publish a separate
`substrate-runtime` distribution with platform-specific wheels exposing
`substrate_runtime/bin/substrate-runtime`. `substrate-sdk` depends on
`substrate-runtime`, so a normal install should be one command:

```sh
pip install substrate-sdk
```

This keeps Rust out of SDK installation: users receive a prebuilt
binary, not a local compile.

## Validation

Before publishing a release, validate the artifacts themselves:

```sh
cd packages/substrate-js
bun run build
npm --cache .npm-cache pack --dry-run --json
```

The npm dry run should list only `README.md`, `package.json`, `dist/index.js`,
and `src/index.ts` for the SDK package.

```sh
cd packages/substrate-python
python3 -m build
python3 -m twine check dist/*
```

The Python SDK wheel should be tagged `py3-none-any` and should not contain a
runtime binary.
