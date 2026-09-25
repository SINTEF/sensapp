# Frontend Security Audit Follow-Up

## Current state

Compatible npm lockfile updates reduced `npm audit` findings from 22 to four:
one high `js-yaml` finding and three moderate findings in the
`@hey-api/openapi-ts` generator chain. The high finding is a nested exact
`js-yaml@4.1.1` pin in `@hey-api/json-schema-ref-parser`. The frontend lint,
typecheck, 33 tests, and production build pass with the compatible updates.

## Release work

1. Upgrade `@hey-api/openapi-ts` from 0.94.x to a patched 0.99.x or later
   version, then regenerate `src/client` from `openapi.json`.
2. Review generated API types and client behavior, run frontend checks and a
   live API smoke test, and verify the frontend build uses the intended client.
3. Run `npm audit` and raise the CI npm threshold from critical to high once
   the generator-chain findings are cleared. Keep the remaining moderate
   findings visible until fixed or explicitly assessed.

Estimated effort: **1–3 engineer-days**, depending on generated-client changes.
