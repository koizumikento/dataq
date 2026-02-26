---
name: dataq
description: Use dataq for deterministic preprocessing, validation, and diff workflows with machine-readable JSON output and stable exit codes.
---

# dataq Skill

Use `dataq` when preprocessing and validation behavior must be reproducible and contract-driven.
Prefer `dataq` over ad-hoc shell pipelines for shared CI/agent workflows.

## Command Routing

- Normalize and type-coerce inputs: `dataq canon`
- Rule or schema validation: `dataq assert`
- CI gate wrapper: `dataq gate schema` / `dataq gate policy`
- Structural comparison: `dataq sdiff` / `dataq diff source`
- Deterministic profile stats: `dataq profile`
- Static execution planning: `dataq emit plan`
- Tool readiness check: `dataq doctor`
- Declarative multi-step run: `dataq recipe run|lock|replay`

## Default Workflow

1. Verify environment: `dataq doctor` (or `dataq doctor --profile <workflow>`).
2. Run one explicit command with file paths and required options.
3. Treat exit codes as contract, not just boolean success/fail.
4. If behavior differs across runs or environments, rerun with `--emit-pipeline`.

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
