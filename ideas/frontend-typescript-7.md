# Frontend on TypeScript 7

TypeScript 7.0 (the native compiler) is out, but typescript-eslint 8.72 accepts TypeScript
`>=4.8.4 <6.1.0` (checked 6 November 2026), so the frontend is on TypeScript 6.0.

When typescript-eslint supports 7.x, bump `typescript`, then run lint, typecheck, tests, and
the build. The build script uses `tsc -b`, check it still works the same with the native compiler.
