# AGENTS.md instructions for /Users/koizumikenjin/workspace/dataq

## Purpose

`dataq` is a Rust-native CLI for deterministic data preprocessing.
It does not perform AI inference.

Core goals:
- Deterministic output for identical input
- Machine-readable output by default
- Stable exit-code contract
- Pragmatic interoperability with `jq`, `yq`, and `mlr`

## Scope and precedence

- This file applies to the entire repository.
- If nested `AGENTS.md` files are added later, deeper files override this one within their subtree.
- Direct user/system/developer instructions override this file.

## Working style

- Keep edits small, focused, and reversible.
- Update docs when behavior changes.
- Prefer root-cause fixes over one-off patches.
- Avoid speculative abstractions in early stages.

## Branch and worktree strategy

- For multi-feature plans, split work by feature into separate branches and separate `git worktree` directories.
- Run sub-agents in parallel on those split worktrees to reduce edit collisions.
- Prefer integrating by merging the split feature branches (`git merge`) into the target branch.
- Avoid integration via cherry-pick aggregation as the default path.
- If a temporary integration branch is needed for validation, merge feature branches into it, then merge that branch to `main`.
- After integration is complete and `main` is green, clean up temporary worktrees and branches.
- Cleanup order:
  - remove feature worktrees (`git worktree remove <path>`)
  - prune stale worktree metadata (`git worktree prune`)
  - delete merged local feature/integration branches (`git branch -d <branch>`; use `-D` only when necessary)
  - delete remote temporary branches when no longer needed (`git push origin --delete <branch>`)
- Keep long-lived branches (`main` and active release branches) intact during cleanup.

## Skill usage

- Skill paths:
  - `.agents/skills/feat-add/`
  - `.agents/skills/rev-pass/`
- Invoke explicitly:
  - `$feat-add`
  - `$rev-pass`
  - (both have implicit invocation disabled)
- Run `$feat-add` as a sub-agent workflow:
  - use `explorer` to map impact first
  - use `worker` to implement the feature and tests
  - keep parent agent for final review and gate execution
- Use `$feat-add` when:
  - adding a new subcommand in `src/cmd/`
  - extending existing command options/output schema
  - adding validation/normalization/diff behavior in `src/engine/`
  - implementing feature work that also needs tests + README update
- Run `$rev-pass` after implementation:
  - `$rev-pass` is one review pass only (no auto-loop inside the skill)
  - spawn a review sub-agent with `$rev-pass`
  - receive findings and fix required issues
  - run the review sub-agent again
  - repeat fix + review until no required fixes remain
- Required default process:
  - do not finish implementation after first review pass
  - continue iterations until the review result indicates no required fixes
- Prefer normal editing (without the skill) for tiny typo/docs-only changes.
- Example prompt: `Use $feat-add as a sub-agent to add --strict mode to assert and update tests.`
- Example prompt: `Use $rev-pass as a sub-agent to review this implementation; fix findings and re-run review until clean.`

## Rust baseline

- Use stable Rust and keep it current (currently Rust `1.91.1` is published on the official install page as of 2025-11-10).
- Use Rust Edition `2024`.
- Keep `rust-version` explicit in `Cargo.toml`.
- Prefer pinning toolchain via `rust-toolchain.toml` once the workspace is initialized.
- Keep rustfmt style aligned with edition (`style_edition = "2024"` when using config).
- Public APIs should return typed errors; avoid `panic!` for recoverable failures.
- In binaries, `anyhow` is acceptable for top-level error context.
- In libraries, prefer structured errors via `thiserror`.

## Data and determinism rules

- Default output format is machine-readable JSON.
- Support `stdin -> stdout` pipelines for all transform commands.
- Keep key ordering deterministic where output semantics require it.
- Normalize timestamps to RFC 3339 UTC when possible.
- Do not rely on locale-dependent parsing or formatting.

## External tool rules (`jq`, `yq`, `mlr`)

- Never use shell string interpolation for user-provided values.
- Invoke external tools with explicit argument arrays.
- Validate tool availability early and fail with actionable errors.
- Keep pipeline generation inspectable (`--emit-pipeline` behavior).

## CLI contract rules

- Exit code `0`: success
- Exit code `2`: validation mismatch
- Exit code `3`: input/usage error
- Exit code `1`: internal/unexpected error

## Quality gates

Run these before finishing meaningful code changes:

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
```

If the workspace is not initialized yet, initialize first, then run the same gates.

## Cargo lint policy

Use workspace-level lint configuration and keep warnings strict by default.
Prefer `Cargo.toml` lint tables instead of ad-hoc per-file allowances where practical.

## Documentation and tests

- Public functions/types should include rustdoc that states input/output expectations.
- Add targeted unit tests for parser/normalizer/diff edge cases.
- Keep fixture-based tests deterministic and easy to diff.

## References

- OpenAI Codex AGENTS guide:
  - https://developers.openai.com/codex/guides/agents-md
- OpenAI agent engineering practices:
  - https://openai.com/index/introducing-improvements-to-the-responses-api/
  - https://openai.com/index/unifying-and-unrolling-llm-tools/
- Rust Edition 2024:
  - https://blog.rust-lang.org/2025/02/20/Rust-1.85.0/
  - https://doc.rust-lang.org/edition-guide/rust-2024/rustfmt-style-edition.html
- Rust API and style guidance:
  - https://rust-lang.github.io/api-guidelines/checklist.html
  - https://rust-lang.github.io/rust-clippy/
  - https://doc.rust-lang.org/cargo/reference/lints.html
  - https://doc.rust-lang.org/book/ch09-00-error-handling.html
  - https://doc.rust-lang.org/cargo/commands/cargo-test.html
