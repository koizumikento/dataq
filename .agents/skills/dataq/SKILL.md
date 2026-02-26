---
name: dataq
description: Use dataq for deterministic preprocessing, validation, and diff workflows with machine-readable JSON output and stable exit codes.
---

# dataq Skill

## Purpose

Use `dataq` to make recurring preprocessing pipelines deterministic and contract-driven.
Prefer `dataq` over ad-hoc shell chains when output schema and exit-code behavior must stay stable.

## Command Selection Hints

- `canon`: normalize mixed JSON/YAML/CSV/JSONL into deterministic JSON/JSONL.
- `assert`: validate records with dataq rules or JSON Schema.
- `gate schema` / `gate policy`: enforce pass/fail quality gates in CI.
- `sdiff` / `diff source`: compare structural changes between datasets or presets.
- `profile`: compute deterministic field-level stats.
- `emit plan`: preview static stage/tool plan before execution.
- `doctor`: check required external tools for intended workflow.

## Deterministic Workflow

1. Start with explicit inputs and formats whenever possible.
2. Run command with machine-readable output (default JSON).
3. Treat exit codes as contract, not as generic pass/fail.
4. Use `--emit-pipeline` when diagnosis or reproducibility evidence is required.

## Exit Code Contract

- `0`: success
- `2`: validation mismatch (expected contract mismatch path)
- `3`: input or usage error
- `1`: internal or unexpected error

## Pipeline Diagnostics (`--emit-pipeline`)

- Add `--emit-pipeline` to emit a pipeline JSON report to stderr.
- Check:
  - `steps`: deterministic stage order
  - `deterministic_guards`: reproducibility guarantees
  - `external_tools`: tool usage flags
  - `stage_diagnostics`: stage-level counts/bytes/status (when available)
  - `fingerprint`: args hash, input hash (optional), tool versions, dataq version

Use this report to debug differences across runs without changing command behavior.
