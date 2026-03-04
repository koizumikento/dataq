---
name: dataq
description: Use dataq for deterministic preprocessing, validation, and diff workflows with machine-readable JSON output and stable exit codes.
---

# dataq Skill

Use `dataq` when preprocessing and validation behavior must be reproducible and contract-driven.
Prefer `dataq` over ad-hoc shell pipelines for shared CI and agent workflows.

## Command Routing

- Normalize/coerce mixed inputs: `dataq canon`
- Ingest external data:
  - `dataq ingest api`
  - `dataq ingest yaml-jobs`
  - `dataq ingest tabular`
  - `dataq ingest jc`
  - `dataq ingest notes`
  - `dataq ingest doc`
  - `dataq ingest book`
- Infer schema from tabular input: `dataq schema infer`
- Validate and gate:
  - `dataq assert --rules|--schema`
  - `dataq gate schema`
  - `dataq gate policy`
- Transform and compare:
  - `dataq transform rowset`
  - `dataq transform sql`
  - `dataq sdiff`
  - `dataq diff source`
- Analyze and shape data:
  - `dataq profile`
  - `dataq join`
  - `dataq aggregate`
  - `dataq merge`
- Contract/planning and environment checks:
  - `dataq contract`
  - `dataq emit plan`
  - `dataq doctor`
- Declarative execution:
  - `dataq recipe run`
  - `dataq recipe lock`
  - `dataq recipe replay`

## Recommended Workflow

1. Verify tool availability first: `dataq doctor` or `dataq doctor --profile <workflow>`.
2. For new automation paths, inspect shape before execution (when the target command supports these contracts):
   - `dataq contract --command <cmd>`
   - `dataq emit plan --command <cmd> [--args ...]`
3. Run commands with explicit paths/options for reproducibility.
4. Treat exit code contract as API behavior.
5. Use `--emit-pipeline` when results differ across environments.

## Exit Code Contract

- `0`: success
- `2`: validation mismatch (expected mismatch path)
- `3`: input or usage error
- `1`: internal or unexpected error

## Pipeline Diagnostics

`--emit-pipeline` writes a deterministic JSON report to stderr.

Focus on:

- `steps`: fixed stage order
- `deterministic_guards`: reproducibility controls
- `external_tools`: which integrations were used
- `stage_diagnostics`: per-stage counts/bytes/status where available
- `fingerprint`: args hash, optional input hash, tool versions, dataq version

Use this report for reproducibility audits without changing functional output.
