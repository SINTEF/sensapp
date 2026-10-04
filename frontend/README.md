# SensApp frontend

The explorer UI of SensApp: React 19, TypeScript, Vite, Tailwind with daisyUI, TanStack Query, Zustand and ECharts. The build is served by SensApp under `/ui/`.

See [docs/FRONTEND.md](../docs/FRONTEND.md) for how it is served, how it authenticates and how to develop it.

```bash
npm ci
npm run dev          # http://localhost:5173/ui/, the API is proxied to http://localhost:3000
npm run lint
npm run typecheck
npm test
npm run build        # dist/, served by SensApp (SENSAPP_UI_DIR)
```

`src/client` is generated from `openapi.json` by `npm run openapi-ts`, and committed.
