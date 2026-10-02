# Docs and housekeeping after the hardening work

## Goal

Write down what the last weeks of work decided, so that it is not only in commit messages and chat.

## Checklist

- [ ] `docs/BACKENDS.md`: which backends are maintained and which are experimental (CI coverage, features,
  what each is for, known limits), linked from `docs/CONFIGURATION.md` and ticked in `TODO.md`
  (backend trade-offs; production-oriented versus research-oriented features).
- [ ] OpenAPI: the write endpoints (`/publish`, InfluxDB write, Prometheus remote write) document `503`
  (with `Retry-After`, load shedding) and `504`; the read endpoints document `503`/`504`; the OpenAPI
  smoke test checks it.
- [ ] TLS: `docs/CLICKHOUSE.md` says it is verified by hand against a server with a private CA, on purpose;
  no TLS integration test in CI is wanted (decision of 2 Oct 2026).
- [ ] Test harness databases of TimescaleDB: decision recorded in `ideas/` (a one-line cleanup command in the
  note is enough, no teardown code), and a cargo-make task to run it.
- [ ] `TODO.md` brought up to date (what is done, what this package adds).

## Progress

(none yet)
