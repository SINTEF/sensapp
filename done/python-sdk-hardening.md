# Python SDK hardening

## Goal

Strengthen the Python SDK so it is closer to a trustworthy PyPI package.

## Scope

- expand unit coverage for edge cases and request handling
- add live integration tests against a real SensApp process with SQLite
- add a dedicated GitHub workflow for linting, unit tests, integration tests, and wheel smoke tests
- document backend bugs in a repository file if the integration work exposes them

## Notes

- Prefer catching backend regressions with integration tests instead of compensating for them in the client.
- Keep the integration setup self-contained and SQLite-based so CI stays fast and deterministic.