---
name: dataq-rules-recipes
description: Author and refine dataq assert rules and recipe files with deterministic validation loops and stable exit-code handling.
---

# dataq Rules + Recipes Skill

Use this skill for tasks that create or edit:

- `dataq assert --rules` rule files
- `dataq recipe run|lock|replay` recipe files

## Workflow

1. Normalize and inspect sample input when needed (`dataq canon`).
2. Draft minimal rules (`required_keys`, `fields`, `count`).
3. Add strict constraints incrementally (`enum`, `pattern`, `range`, `nullable`, `forbid_keys`).
4. Validate with `dataq assert --rules ...`.
5. Encode deterministic recipe step order and explicit args.
6. Validate reproducibility with run/lock/replay:
   - `dataq recipe run`
   - `dataq recipe lock`
   - `dataq recipe replay --strict`

## Exit Codes

- `0`: success
- `2`: validation mismatch
- `3`: input/usage error
- `1`: internal/unexpected error

## Guardrails

- Keep JSON output contracts machine-readable.
- Prefer explicit input formats/paths for CI.
- Use `--emit-pipeline` for diagnostics and reproducibility evidence.
