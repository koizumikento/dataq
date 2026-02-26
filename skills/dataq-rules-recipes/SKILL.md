---
name: dataq-rules-recipes
description: Author and refine dataq assert rules and recipe files with deterministic validation loops and stable exit-code handling.
---

# dataq Rules + Recipes Authoring

Use this skill when the task is to create or update:

- `dataq assert --rules` rule files
- `dataq recipe run|lock|replay` recipe files

## Deliverables

- A rules file that encodes expected structure/constraints.
- A recipe file that executes a deterministic pipeline in a fixed order.
- Validation commands and expected exit-code behavior.

## Rules Authoring Workflow

1. Inspect sample input and normalize first if needed (`dataq canon`).
2. Start from minimal rules:
   - `required_keys`
   - `fields.<path>.type`
   - `count`
3. Add constraints incrementally:
   - `enum`, `pattern`, `nullable`, `range`, `forbid_keys`
4. Validate with `dataq assert --rules <rules> --input <data>`.
5. When reuse is needed, split shared constraints and use `extends`.

## Recipe Authoring Workflow

1. Define pipeline intent and exact stage order.
2. Encode steps with explicit command args (no ambiguous defaults).
3. Run `dataq recipe run --file <recipe>`.
4. Freeze execution metadata with `dataq recipe lock --file <recipe>`.
5. Verify reproducibility with `dataq recipe replay --file <recipe> --lock <lock>`.

## Exit-Code Contract

- `0`: success
- `2`: validation mismatch (expected constraint mismatch path)
- `3`: input/usage error (invalid rules/recipe/input/tool availability)
- `1`: internal/unexpected error

## Authoring Guardrails

- Keep outputs machine-readable JSON by default.
- Prefer explicit file paths and formats over inference in CI-critical flows.
- Preserve deterministic ordering of keys/steps.
- Use `--emit-pipeline` when debugging stage behavior differences.

## Quick Skeletons

Rules skeleton:

```yaml
required_keys: []
forbid_keys: []
fields: {}
count:
  min: 0
```

Recipe verification loop:

```bash
dataq recipe run --file recipe.json
dataq recipe lock --file recipe.json --out recipe.lock.json
dataq recipe replay --file recipe.json --lock recipe.lock.json --strict
```
