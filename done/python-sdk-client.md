# Python SDK client

## Goal

Create a Python SDK for SensApp that feels natural to use from Python code, prefers Apache Arrow for data exchange, and is ready to evolve into a PyPI package.

## Scope

- standalone Python package inside this repository
- sync client for the current HTTP API
- Arrow-first helpers for upload and download
- tests with mocked HTTP transport and Arrow round-trips
- concise package documentation and simple examples

## Notes

- SensApp uses different Arrow schemas for upload and download, so the SDK should make that explicit instead of hiding it.
- Keep dependencies small and the public API boring and predictable.
