---
name: rev-pass
description: Perform one review pass on recent code changes and return bug/risk-focused findings. Use when implementation is complete and a sub-agent should review with concrete, severity-ordered findings.
---

# Review Pass

## Sub-agent mode

1. Run this skill in a review sub-agent after implementation and tests.
2. Perform a single review pass for bugs, regressions, missing tests, and contract mismatches.
3. Report findings first, sorted by severity with file and line references.
4. If no findings exist, state that explicitly.

## Review checklist

- Verify behavior against stated requirements.
- Check exit-code and output contract consistency.
- Check edge cases and failure paths.
- Check determinism and reproducibility risks.
- Check tests for coverage of changed behavior.

## Finding format

- One finding per item.
- Include:
  - severity (`P0`/`P1`/`P2`/`P3`)
  - file path
  - line reference
  - concise impact
  - concrete fix direction
