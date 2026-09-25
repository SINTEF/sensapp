# Frontend Security Audit Follow-Up (September 2026)

The OpenAPI generator was upgraded from 0.94.x to 0.99.0 and the client was
regenerated. The deprecated standalone `@hey-api/client-fetch` package was
removed. Because the generator's parser still brings in a vulnerable
`js-yaml`, `package.json` overrides that nested dependency to 4.3.2 or newer.

`npm audit` reports zero findings. Lint, typecheck, 34 frontend tests,
production build, and a live readiness/catalog request with the generated
client all pass. CI now blocks on high severity npm advisories.

The override can be removed when the generator's parser declares a patched
`js-yaml` dependency. See `ideas/remove-frontend-js-yaml-override.md`.
