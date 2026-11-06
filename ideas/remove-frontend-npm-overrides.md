# Remove Frontend npm Overrides

`frontend/package.json` overrides two transitive packages to clear `npm audit`:

- `js-yaml` 4.3.2 or newer, under `@hey-api/json-schema-ref-parser`, which still pins 4.2.0
  (checked 6 November 2026).
- `minimatch` 11.0.1 or newer, under eslint and typescript-eslint, which ask for minimatch 10
  (GHSA-g3hv-75wg-m249, fixed only in 11.0.1). Lint tooling only; eslint lints the same 80
  files with it.

Once the upstream packages depend on patched versions themselves, remove the override and
rerun `npm audit`, client generation, lint, tests, build, and a live API smoke test.
