---
name: dataq-rules-recipes
description: Use when a dataq automation task needs a new or revised `assert --rules` file or `dataq.recipe.v1` recipe, including lock/replay verification. Do not use for general dataq command selection, CLI or schema implementation changes, or routing-only evaluation.
---

# dataq Rules and Recipes Authoring

Author deterministic dataq rules and recipes, then prove them with the CLI's machine-readable validation and replay contracts.

## Do Not Use For

- General JSON/YAML transformation, dataq command selection, or one-off command execution; use `$dataq`.
- JSON Schema authoring for `dataq assert --schema`; this skill owns native `--rules` files, not JSON Schema design. Use `$dataq` only to select and run the command.
- Adding commands, changing accepted rule or recipe schemas, or changing output contracts; use `$feat-add`.
- Reviewing dataq implementation changes; use `$rev-pass`.
- Routing cases, implicit-invocation policy, or skill installation behavior; hand those off to their dedicated workflows.

## Rules Authoring Workflow

1. Read the runtime source of truth before drafting:

   ```bash
   dataq assert --rules-help
   ```

   `--rules-help` is a standalone discovery command; do not combine it with `--rules` or an input-validation run.

2. Inspect representative input. Normalize it with `dataq canon` only when input shape or scalar types are ambiguous.
3. Start with a valid minimal rules file and add only constraints supported by `--rules-help`:

   ```yaml
   required_keys: [id]
   forbid_keys: []
   fields:
     id:
       type: integer
   count:
     min: 1
   ```

4. Validate the intended input explicitly:

   ```bash
   dataq assert --input input.json --rules rules.yaml
   ```

5. When reusing rules with `extends`, preserve these semantics:
   - `extends` accepts one path or a path list. Relative paths resolve from the file that declares them.
   - Parents apply in list order, followed by the current file.
   - `required_keys` and `forbid_keys` form deterministic unions; later `fields` entries win by field path; the last defined `count` wins.
   - Rule paths use dot-separated object keys. Empty segments and array-index syntax are invalid.
6. Treat constraint mismatches as expected exit `2`. Unknown keys at any rule-schema level, malformed paths or constraints, missing/cyclic `extends`, and invalid input are exit `3`.

## Recipe Authoring Workflow

1. Express a fixed step order using recipe version `dataq.recipe.v1`. Supported step kinds are `canon`, `assert`, `profile`, and `sdiff`; give every step explicit arguments.
2. Start from a valid pipeline such as:

   ```yaml
   version: dataq.recipe.v1
   steps:
     - kind: canon
       args:
         input: ./input.json
         from: json
     - kind: assert
       args:
         rules_file: ./rules.yaml
   ```

   Relative step paths resolve from the recipe file's directory. An `assert`, `profile`, or `sdiff` step consumes prior in-memory values, so place a producing `canon` step first when needed.
   Every recipe `assert` step must specify exactly one of `rules`, `rules_file`, `schema`, or `schema_file`.
3. Run the complete verification loop:

   ```bash
   dataq recipe run --file recipe.yaml
   dataq doctor --profile core
   dataq recipe lock --file recipe.yaml --out recipe.lock.json
   dataq recipe replay --file recipe.yaml --lock recipe.lock.json --strict
   ```

4. Without `--out`, `recipe lock` emits lock JSON to stdout. With `--out`, it writes the lock file and leaves stdout empty.
5. `recipe lock` currently probes `jq`, `yq`, and `mlr` regardless of the recipe's steps. A missing, non-executable, or unversioned tool is exit `3`, so do not omit the doctor check.
6. In strict replay, any lock mismatch returns exit `2` and skips recipe execution. Non-strict replay reports lock mismatches and continues; a lock mismatch alone does not force exit `2`, although a subsequently executed `assert` or `sdiff` mismatch does.

## Exit-Code Contract

- `0`: authoring check or execution succeeded.
- `2`: data validation, diff, or strict lock verification mismatched.
- `3`: rules, recipe, input, arguments, or required tools are invalid or unavailable.
- `1`: an internal or unexpected failure occurred.

## Output

- The requested rules and/or recipe files with deterministic ordering and explicit paths.
- The validation commands run, their exit codes, and any assumptions about representative input or external tools.

## Guardrails

- Keep source data read-only and preserve machine-readable JSON output contracts.
- Do not change dataq implementation, schemas, or command behavior from this authoring workflow.
- This is a repository-local Codex skill. `dataq codex install-skill` installs only `$dataq`; do not claim it installs this skill or change the installer here.
