---
name: dataq
description: Use when an agent needs to choose, inspect, or run an existing dataq CLI or MCP workflow with deterministic machine-readable output. Do not use for authoring rules or recipes, changing dataq behavior, or AI inference.
---

# dataq

Route work to an existing `dataq` command, verify its published contract where supported, and preserve deterministic stdout and exit-code behavior.

## Do Not Use For

- Creating or editing assert rules or recipe definitions; use `dataq-rules-recipes`.
- Adding commands, options, schemas, or executable behavior; use `$feat-add`.
- Open-ended analysis or AI inference. `dataq` is deterministic preprocessing only.

## Command Routing

- Canonicalize: `dataq canon`
- Ingest: `dataq ingest api|yaml-jobs|jc|tabular|notes|doc|book`
- Validate and gate: `dataq assert`, `dataq gate schema|policy`
- Infer schema: `dataq schema infer`
- Compare: `dataq sdiff`, `dataq diff source`
- Inspect and shape: `dataq profile`, `dataq join`, `dataq aggregate`, `dataq merge`
- Transform: `dataq transform rowset|sql`
- Scan repository text: `dataq scan text`
- Execute definitions: `dataq recipe run|lock|replay`
- Inspect environment and interfaces: `dataq doctor`, `dataq contract`, `dataq emit plan`
- Install the embedded Codex skill: `dataq codex install-skill`
- Serve one JSON-RPC request over stdin/stdout: `dataq mcp`

Use `dataq <route> --help` before inventing flags. The CLI and MCP tool sets are not identical; for MCP, treat the response to `tools/list` as authoritative.

## Workflow

1. Select the narrowest route above and inspect its `--help`.
2. Preflight only when that workflow uses a profiled external capability. Valid `doctor --profile` values are:
   - `core` for the jq/yq/mlr core
   - `ci-jobs`, `doc`, `api`, `notes`, `book`, or `scan` for the matching workflow
3. Do not invent doctor profiles for other tools. Let the selected command report an actionable dependency error.
4. Inspect a contract or static plan only when its identifier is supported below.
5. Execute with explicit paths, formats, modes, and deterministic sort/order options. Preserve stdin-to-stdout operation where the command supports it.
6. Add runtime pipeline diagnostics only when provenance or an environment difference needs investigation.

`dataq doctor` without `--profile` checks the jq/yq/mlr baseline. Do not run it as a mandatory prelude to Rust-native commands.

## Contracts and Static Plans

`dataq contract` publishes output and per-command exit semantics. Use `--all` or one exact `--command` identifier:

```text
canon, ingest-api, ingest, ingest-jc, ingest-tabular, assert,
gate-schema, gate, schema-infer, sdiff, diff-source, profile,
ingest-doc, ingest-notes, ingest-book, join, aggregate, scan,
transform-rowset, transform-sql, merge, doctor, recipe-run,
recipe-lock, recipe-replay, emit-plan
```

`dataq emit plan` has a smaller identifier set:

```text
canon, assert, sdiff, profile, join, aggregate, merge,
transform-sql, doctor, contract, recipe, recipe.run, recipe run, mcp
```

Pass `--args` as one JSON array string. A static plan does not read input or prove runtime success.

```bash
dataq contract --command profile
dataq emit plan --command assert --args '["--rules","rules.json"]'
dataq emit plan --command aggregate --args '["--input","orders.json","--group-by","team","--metric","sum","--target","price"]'
```

Stop and inspect `--help` when an identifier is absent from these lists; do not translate a CLI path into a guessed contract or plan identifier.

## Agent Quickstart

Use a compact profile before selecting a reusable deterministic transform:

```bash
dataq contract --command profile
dataq profile --from json --input orders.json --brief --sort-fields unique_count --max-fields 20
dataq aggregate --input orders.json --group-by team --metric sum --target price --sort-by metric --order desc --limit 10
```

Use `contract` before wiring stdout JSON into another tool. Use `emit plan` when static stage/tool selection matters. Include an explicit sort or `ORDER BY` whenever row order is part of the result.

## Exit Semantics

For normal CLI commands, preserve the stable process contract:

- `0`: success
- `2`: an expected validation, policy, or opted-in diff mismatch
- `3`: input, usage, or actionable dependency error
- `1`: internal or unexpected error

Not every command uses exit `2`; inspect its contract. Never collapse `2` into an internal failure.

`dataq mcp` has two separate layers:

- The MCP process returns `0` after it writes a JSON-RPC response, including JSON-RPC error responses. Stdin/stdout transport failures return `3`.
- A `tools/call` result carries the dataq command status in `structuredContent.exit_code`; `isError` is true when that tool exit code is nonzero.

Do not substitute the MCP process exit code for the embedded tool exit code.

## Pipeline Diagnostics

- CLI: add global `--emit-pipeline`; functional JSON remains on stdout and the runtime pipeline report is written to stderr.
- MCP: pass `"emit_pipeline": true` in tool arguments; diagnostics are returned as `structuredContent.pipeline`.
- Static planning: `dataq emit plan` returns predicted stages and tools without consuming input; it is not a runtime report.

For runtime reports, inspect `steps`, `deterministic_guards`, `external_tools`, `stage_diagnostics`, and `fingerprint`. Keep stage order intact and do not merge stderr diagnostics into functional CLI stdout.

## Output

- Report the selected route, exact command or MCP tool, and any contract/plan identifier used.
- Preserve machine-readable payloads and surface the actual exit code.
- State any external dependency or unsupported inspection surface encountered.

## Guardrails

- Pass user-provided values as explicit argv entries or structured MCP arguments; never interpolate them into shell source.
- Let `dataq` produce transformed output, but treat source inputs as read-only unless a command explicitly documents in-place mutation; do not change the CLI implementation, contract, or schema with this usage skill.
- Stop on an unknown route, profile, contract identifier, plan identifier, or MCP tool and inspect the authoritative help/list response.
