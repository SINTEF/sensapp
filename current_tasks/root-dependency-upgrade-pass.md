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
- A compatible `cargo update` refreshed 265 locked packages. `hybridmap 0.1.3`
  does not compile with `smallvec 2.0.0-beta.1`, so the lockfile retains
  `smallvec 2.0.0-alpha.12` until that upstream mismatch is fixed.
- `cargo check --locked`, `cargo check --locked --all-features`,
  `cargo clippy --locked --tests -- -D warnings`, default `cargo test --locked`,
  and `cargo-audit` with the documented RSA exception pass.
- Larger root version migrations, including Arrow 60, SQLx 0.9, cached 4,
  jsonwebtoken 11, and utoipa 6, require separate API and service test review.
