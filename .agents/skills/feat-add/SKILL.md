---
name: feat-add
description: "Use when exactly one dataq Rust CLI feature—such as a subcommand, option/output contract, or validation/normalization/diff behavior—must be implemented with tests and docs in an isolated branch and worktree. Do not use for multiple features, review-only work, or tiny typo/docs-only edits."
---

# Feature Addition (Rust CLI)

Deliver exactly one dataq feature from an agreed behavior delta through implementation, tests, documentation, review, and merge readiness. Run this skill explicitly as a sub-agent workflow; one feature owns one branch, one worktree, and one PR.

## Do Not Use For

- Multiple independent features or a plan whose items can ship separately. Create one `$feat-add` lane per feature instead.
- Review-only work after implementation. Use `$rev-pass` in a read-only review sub-agent.
- Tiny typo or docs-only corrections that do not change a command contract or executable behavior. Edit those normally.
- Repository initialization, release preparation, or unrelated refactors.

An intended prompt is: `Use $feat-add as a sub-agent to add --strict mode to assert and update tests and docs.` A neighboring prompt that must not select this skill is: `Use $rev-pass to review the current branch without changing it.`

## Preconditions

The parent agent must satisfy and record all of these before implementation:

1. Write one short feature spec covering input, output, exit-code behavior, determinism requirements, and any external-tool stages.
2. Name the integration branch. Fetch its remote/tracking ref when one exists, resolve the integration HEAD at feature start, and record that exact commit as the base SHA.
3. Create one feature branch at that exact base SHA, check it out in its own worktree, and reserve that worktree for one implementation worker.
4. In the new worktree, verify the initial feature `HEAD` equals the recorded base SHA exactly. Also confirm the feature branch is neither `main` nor the integration branch and that its status is clean.
5. Define the worker's allowed feature scope. The worker owns every necessary code, test, `README.md`, and `docs/*.md` edit in that worktree.

Use `git fetch <remote> <integration-branch>` when applicable, `git rev-parse <resolved-integration-ref>^{commit}`, `git worktree add -b <feature-branch> <worktree> <base-sha>`, `git branch --show-current`, `git rev-parse HEAD`, `git status --short`, and `git worktree list --porcelain` to make these facts inspectable. An ancestry or merge-base check alone is not proof that the feature started at the required commit. Stop if exact initial `HEAD == base SHA`, branch, worktree, clean status, or ownership cannot be verified.

## Roles

- **Explorer:** read-only. Inspect repository guidance, symbols, registries, tests, and docs; produce an impact map. Do not edit files, stage changes, commit, rebase, or merge.
- **Worker:** the only content writer in the feature worktree. Implement the feature and all matching tests and documentation, and resolve file content when a rebase conflicts. Do not share content write access with another agent.
- **Parent:** orchestration and Git lifecycle only. Fetch and resolve the base, establish the lane, approve scope, stage approved paths, create scoped commits, push/open the draft PR, run full gates, invoke reviews, operate rebase/merge, and clean up. The parent may inspect content but must not edit feature files or choose conflict-resolution content.
- **Reviewer:** read-only. Run one explicit `$rev-pass` at a time against immutable committed `base...HEAD`; never review an uncommitted working-tree diff.

## Workflow

1. **Map impact before editing.** The parent gives a read-only explorer the feature spec, integration branch, and base SHA. The explorer identifies exact files and symbols, including applicable registration points:
   - CLI parsing and dispatch: `src/main.rs`
   - Command modules and registry: `src/cmd/<feature>.rs` and `src/cmd/mod.rs`
   - Core logic and registry: `src/engine/<feature>.rs` or `src/engine/<feature>/mod.rs`, plus `src/engine/mod.rs`
   - Shared types and errors: `src/domain/` and `src/domain/mod.rs`
   - Format I/O and registry: `src/io/` and `src/io/mod.rs`
   - External-tool adapters and registry: `src/adapters/` and `src/adapters/mod.rs`
   - Library exports when applicable: `src/lib.rs`
   - CLI tests and registry: `tests/cli/<feature>_cli.rs` and `tests/cli.rs`
   - Flow/integration tests and registry: `tests/integration/<feature>_flow.rs` and `tests/integration.rs`
   - Deterministic fixtures, `README.md`, and relevant `docs/*.md`
2. **Gate the impact map.** The explorer returns required versus optional touchpoints, target symbols, test cases, documentation updates, contract risks, and any prerequisite. The parent confirms it still represents one independently shippable feature.
3. **Stop on cross-feature scope.** If the map exposes an independently shippable prerequisite, unrelated refactor, or second behavior contract, do not batch it. Create and merge a dedicated prerequisite feature first, then start a fresh branch/worktree from the latest integration HEAD.
4. **Start one worker.** Pass the approved spec, base SHA, impact map, allowed scope, branch, and worktree path to exactly one worker. Before editing, the worker verifies `pwd`, current branch, clean status, and exact initial `HEAD == base SHA`. Stop and report any mismatch instead of repairing the wrong lane; ancestry alone is insufficient.
5. **Implement the smallest complete change.** Keep command parsing and orchestration in `src/cmd/` or `src/main.rs`, business logic in `src/engine/`, reusable types/errors in `src/domain/`, format handling in `src/io/`, and external process calls in `src/adapters/`. Register every new module and test file in the applicable registry.
6. **Add tests and docs in the same lane.** Cover the new success path, relevant mismatch/usage/error paths, deterministic output, and parser/normalizer/diff edge cases. Update `README.md` and relevant `docs/*.md` whenever behavior or a public contract changes. The worker, not the parent, owns these edits.
7. **Pre-validate worker content.** The worker runs focused tests and formatting while iterating, then reports changed paths and exact results. Do not hand an uncommitted working tree to the reviewer.
8. **Create the first immutable revision.** The parent inspects status and diff, confirms every change is approved and feature-scoped, stages only those paths, and creates scoped commit(s) without editing their content. Record the committed HEAD SHA and require a clean worktree. Push the branch and open its single draft PR.
9. **Gate and review the committed diff.** On that clean committed HEAD, the parent runs all required quality gates and verifies status remains clean. Then invoke `$rev-pass` with the spec, exact base SHA, exact HEAD SHA, gate results, and immutable `base...HEAD` diff. A working-tree diff is not a review target.
10. **Fix with new immutable revisions.** Return every failed-gate repair and required review finding to the same worker. The worker edits content and reruns focused checks. The parent rechecks scope, creates new scoped fix commit(s) without amending the previously reviewed HEAD, verifies clean status, pushes the new HEAD, and runs fresh full gates and a fresh one-pass `$rev-pass` against `base...new-HEAD`. Repeat until gates pass and the reviewer reports `NO_REQUIRED_FIXES`.
11. **Rebase immediately before merge.** The parent fetches and resolves the latest integration HEAD to a new exact SHA, verifies all feature changes are committed and status is clean, then rebases the committed feature branch onto that SHA. If content conflicts occur, the parent pauses Git while the same worker writes the resolution; the parent then verifies, stages, and continues the rebase without editing content. Record the rebased base and HEAD SHAs, rerun all required gates, and rerun `$rev-pass` whenever the scoped diff changed or integration changes can alter feature behavior.
12. **Update and merge the one PR.** Push the rebased branch safely, update the existing draft PR, and mark it ready only when the post-rebase gates pass and the latest applicable review has no required fixes. Merge that one PR into the integration branch; do not open an aggregation PR for the feature.

## Quality Gates

The parent runs these against a clean committed HEAD after the first scoped commit, after every required-fix commit, and after the final rebase when Rust source or executable behavior changed:

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
cargo llvm-cov --workspace --all-features --fail-under-lines 80 --fail-under-regions 75
```

For a genuinely docs-only edit these Cargo gates are optional, but `$feat-add` should normally hand such work back to normal editing under **Do Not Use For**.

## Output

The worker reports:

- Feature spec, integration branch, resolved starting base SHA, feature branch, and worktree.
- Changed paths and a concise behavior summary, including tests and docs.
- Focused checks and each repair made for a failed gate or required review finding.
- Remaining risks, assumptions, or a stop condition; never claim merge readiness when one exists.

The parent records every committed HEAD reviewed, full gate result, `$rev-pass` result, draft PR, latest resolved integration SHA, rebased HEAD SHA, and post-rebase validation before declaring the feature ready for its single PR.

## Guardrails

- Preserve deterministic output for identical input and machine-readable JSON by default.
- Preserve exit codes: `0` success, `2` validation mismatch, `3` input/usage error, and `1` internal/unexpected error.
- Use typed errors for recoverable library failures; do not add `panic!` for them.
- Pass user-provided values to `jq`, `yq`, `mlr`, or other tools through explicit argument arrays, never shell interpolation. Validate dependencies early and keep emitted pipelines inspectable.
- Keep edits small and reversible. Do not absorb unrelated cleanup or speculative abstractions.
- Stop if working on `main` or an integration branch, in the wrong worktree, from an unverified base, when initial `HEAD` differs from the base SHA, or amid unowned changes.
- Never run gates or `$rev-pass` as proof for an uncommitted revision; commit the scoped content and require clean status first.
- Required fixes become new scoped commits followed by fresh gates and review; do not amend the already reviewed HEAD.
- Parent Git operations never authorize parent content edits.
- Stop and split the work if a second feature or prerequisite appears.
- Stop before merge if a quality gate fails, coverage is below threshold, or `$rev-pass` still reports a required fix.
