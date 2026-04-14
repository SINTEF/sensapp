# Root Dependency Upgrade Pass

## Goal

Update the root Rust dependencies to the newest available versions that SensApp can realistically adopt now, then make the codebase pass the full validation matrix again.

## Scope

- refresh root dependencies reported by `cargo outdated --root-deps-only`
- fix compile, clippy, and test breakages caused by the upgrades
- run the local backend matrix against real services
- run BigQuery checks if the environment supports them

## Notes

- Existing branch work already included a partial low-risk dependency refresh.
- This pass should preserve unrelated in-flight changes and focus on dependency fallout only.