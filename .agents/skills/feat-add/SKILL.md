---
name: feat-add
description: Add features to an existing Rust CLI with small, scoped changes. Use when adding a subcommand, extending CLI options, changing machine-readable output, adding validation rules, or extending normalize/diff behavior in dataq.
---

# Feature Addition (Rust CLI)

## Sub-agent mode

1. Use an `explorer` sub-agent to map impacted files and symbols.
2. Use a `worker` sub-agent for implementation and tests.
3. Assign clear ownership to the worker:
   - files to edit
   - command behavior to add/change
   - tests to add/update
4. Keep parent agent as orchestrator:
   - review worker output
   - run final quality gates
   - update top-level docs

## Workflow

1. Confirm the behavior delta in one short spec:
   - Input shape
   - Output shape
   - Exit code behavior
2. Map touch points before editing:
   - CLI boundary: `src/cmd/`
   - Core logic: `src/engine/`
   - Shared models/errors: `src/domain/`
   - Format I/O: `src/io/`
   - External tools: `src/adapters/`
3. Implement in this order:
   - Add or update command wiring in `src/cmd/`
   - Add business logic in `src/engine/<feature>/`
   - Add/update shared types in `src/domain/`
   - Keep `cmd` layer free of business logic
4. Add tests:
   - One CLI-level test in `tests/cli/`
   - One flow/integration test in `tests/integration/`
5. Update docs:
   - Reflect behavior and examples in `README.md`

## Rules

- Preserve deterministic output for identical input.
- Default to machine-readable JSON output.
- Keep exit code contract stable (`0`, `2`, `3`, `1`).
- Prefer Rust-native logic; isolate `jq`/`yq`/`mlr` calls in `src/adapters/`.
- Avoid broad refactors during feature delivery.

## Validation

Run after meaningful changes:

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
```
