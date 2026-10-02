# Python 3.14 and Dependency Refresh

## Goal

Target Python 3.14 only while the SDK has no users, and refresh compatible Rust,
Python, and frontend dependencies before the pre-production release.

## Progress

- [x] Switch SDK metadata and CI to Python 3.14.
- [x] Upgrade PyArrow, pandas, pytest, and pytest-asyncio; pass 49 SDK unit tests.
- [x] Refresh compatible Cargo and npm dependencies.
- [x] Upgrade the OpenAPI generator and clear npm audit findings.
- [x] Pass live Python SDK and regenerated frontend client checks against a
  running SQLite-backed SensApp API.
- [x] Pass default Rust tests, all-features compile, Clippy, and security audit.
- [ ] Pass GitHub CI on the sprint PR.
