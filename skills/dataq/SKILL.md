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

## LLM / Agent Quickstart

Use this sequence when an agent needs to inspect data without spending too much context or inventing ad-hoc shell behavior:

```bash
dataq doctor --profile core
dataq contract --command profile
dataq emit plan --command aggregate --args '["--input","orders.json","--group-by","team","--metric","sum","--target","price","--sort-by","metric","--order","desc","--limit","10"]'
dataq profile --from json --input orders.json --brief --sort-fields unique_count --max-fields 20
dataq aggregate --input orders.json --group-by team --metric sum --target price --sort-by metric --order desc --limit 10
dataq transform sql --input orders.json --engine duckdb --query 'SELECT team, SUM(price) AS revenue FROM input GROUP BY team ORDER BY revenue DESC LIMIT 10'
```

- Use `doctor` for dependency preflight.
- Use `contract` before wiring stdout JSON into an agent tool call.
- Use `emit plan` to inspect static stages/tools without reading input data.
- Use `profile --brief` for compact field, null, unique-count, and type orientation.
- Use `aggregate --sort-by metric --order desc --limit <n>` for deterministic top-k group metrics.
- Use `transform sql` for reusable rowset reshaping; include `ORDER BY` when row order matters.
- Use `--emit-pipeline` to capture runtime diagnostics and fingerprints on stderr.
- Keep exploratory one-off analysis in `jq` / `yq` / `mlr` when useful, then codify reusable workflows as `dataq` commands.

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
