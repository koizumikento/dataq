---
name: rev-pass
description: "Use when one completed dataq feature needs a read-only, diff-scoped review pass with merge-blocking findings and an exact verdict. Do not use to implement fixes, map an unfinished feature, review an unspecified range, or audit the whole repository."
---

# Review Pass

Review one completed feature diff for correctness, regressions, contract violations, and missing verification. This skill is review-only and performs exactly one pass; the parent agent owns fixes, reruns, gates, rebases, commits, and merges.

## Do Not Use For

- Implementing or repairing a feature; hand that work to a `$feat-add` worker.
- Exploring impact before implementation, reviewing an unspecified working tree, or auditing unrelated repository history.
- Automatically looping after fixes. Each follow-up review is a new explicit `$rev-pass` invocation with fresh inputs.

## Trigger Boundary

- Intended: `Use $rev-pass to review feature X in the clean /abs/worktree on branch B for committed BASE...HEAD, limited to these paths, using the attached HEAD-tied gate results.`
- Near miss: `Add --strict to assert, update its tests and README, and fix any review findings.` Use `$feat-add`; do not select `$rev-pass`.

## Required Inputs

Require all of the following before inspecting the implementation:

1. The feature specification and acceptance criteria, including intended behavior, non-goals, output shape, and exit-code behavior.
2. The absolute feature worktree path, expected feature branch, and confirmation that the worktree is clean, including no untracked files.
3. Immutable base and head commit SHAs that both resolve to committed objects, plus the exact committed diff range `<base>...<head>`. The worktree's current `HEAD` must equal the supplied head SHA.
4. The authorized scope: expected changed paths and any explicitly excluded paths or known unrelated changes.
5. Gate evidence tied to the supplied head SHA: exact command, exit status, and concise result for every required gate. An explicit `not run` is evidence, but a required unrun gate is a required finding.

If any input is missing, ambiguous, internally inconsistent, or stale, stop before reviewing. A dirty worktree, non-commit endpoint, range other than the supplied committed `<base>...<head>`, or gate evidence for another SHA is invalid input. Return a concise list of the missing or invalid fields and request corrected inputs. Do not infer a range, expand scope, review only part of the diff, or emit a final verdict for a pass that did not run.

## Workflow

1. Read the repository `AGENTS.md` files that govern the authorized scope.
2. Verify the worktree root and current branch. Require empty porcelain status including untracked files, verify that base and head are commit objects, require current `HEAD` to equal head, and confirm that every gate result names that same head SHA. Stop as invalid input on any mismatch and emit no verdict.
3. Inspect name-status for exactly the supplied committed `<base>...<head>` range. Stop on paths outside the authorized scope, an unexpected submodule or generated artifact, or reviewable commits omitted from the supplied range; report the mismatch as invalid input.
4. Review only that immutable committed diff and the minimum surrounding code, tests, and documentation needed to validate it against the specification and repository rules. Do not edit files, run formatters in write mode, stage, commit, rebase, merge, or change branches.
5. Apply the dataq checklist below. Treat a failed required gate or an explicitly unrun required gate as a required finding. This is one pass; do not fix findings or review a second time in the same invocation.
6. Emit findings first in the exact format and deterministic order below. End a completed pass with exactly one verdict token on the final line.

## Dataq Review Checklist

- Requirements: every acceptance criterion is implemented without unrelated behavior changes or speculative abstraction.
- CLI contract: success is `0`, validation mismatch is `2`, input/usage error is `3`, and unexpected/internal error is `1`; machine-readable JSON remains the default where applicable.
- Determinism and I/O: identical input produces identical output; transform commands support `stdin -> stdout`; semantic key ordering is stable; timestamp handling is locale-independent and normalizes to RFC 3339 UTC when possible.
- External tools: user values are passed through explicit argument arrays without shell interpolation; `jq`, `yq`, and `mlr` availability errors are early and actionable; multi-stage pipelines have deterministic stage order and stage-level `--emit-pipeline` diagnostics.
- Rust boundaries: business logic stays out of the command layer; recoverable library failures use typed errors instead of `panic!`; public functions and types document input/output expectations; broad allowances and unrelated refactors are absent.
- Tests and docs: changed parser, normalizer, diff, validation, failure, and exit-code paths have targeted deterministic coverage; behavior and examples are reflected in the relevant README or `docs/*.md` files.
- Gates: evaluate the supplied results for `cargo fmt --all -- --check`, strict workspace Clippy, workspace tests, and `cargo llvm-cov` at the repository thresholds whenever Rust source or executable behavior changed.

## Severity and Required Status

- `P0`, `required: true`: data loss, security exposure, or a release-blocking failure with broad impact.
- `P1`, `required: true`: incorrect behavior, a public contract or acceptance-criterion violation, or a material regression.
- `P2`, `required: true`: a localized defect, unhandled failure path, or missing test/documentation needed to protect changed behavior.
- `P3`, `required: false`: a concrete, non-blocking maintainability or clarity improvement. Do not report style preferences without demonstrable risk.

Severity determines impact; `required` determines the verdict. Do not assign `required: false` to `P0`-`P2`, or `required: true` to `P3`.

## Output

Start with `## Findings`. For each finding, use:

```text
- [P1] <title>
  required: true
  file: <repository-relative path>
  line: <smallest relevant line or range>
  impact: <specific failure and affected user or contract>
  evidence: <diff/code/test evidence>
  fix: <concrete direction, without editing>
```

For normal code or documentation findings, `file` must be a repository-relative path and `line` must be the smallest real relevant line or range. For a failed or unrun required gate that has no source location, use the literal sentinel `file: <quality-gate>` and `line: 0`; put the exact gate command in both the title and `evidence`. Never fabricate a repository path or source line for a gate finding.

Use one item per root cause. Sort by severity (`P0` through `P3`), then the literal `file` value bytewise, starting line numerically, and title bytewise. Multiple `<quality-gate>` findings at the same severity therefore sort by their exact-command titles; their position relative to normal findings follows the same literal bytewise path rule. If there are no findings, write `No findings.` immediately below the heading.

After findings, include `## Review Evidence` with the reviewed worktree, branch, base SHA, head SHA, diff range, scoped paths, and gate results. Include residual risks only when evidence cannot eliminate a concrete uncertainty.

The final line of every completed pass must be exactly:

- `REQUIRED_FIXES` when at least one finding has `required: true`.
- `NO_REQUIRED_FIXES` when no finding has `required: true`, including when only `P3` findings exist.

## Guardrails

- Remain read-only even when a fix is obvious; send required findings to the parent agent.
- Never review `main`, an integration branch, or another feature lane as a substitute for the supplied feature worktree and range.
- Never review staged, unstaged, or untracked changes; require them to be committed into the supplied range and the worktree to be clean.
- Never broaden scope to compensate for missing inputs, stale gate evidence, or unexpected changes.
- Do not claim gates passed unless their supplied evidence is tied to the reviewed head SHA.
- A parent may invoke this skill again after fixes, but the new invocation must provide a new head SHA, range, scope confirmation, and gate evidence.
