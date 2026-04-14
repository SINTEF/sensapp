# SensApp for Agents

SensApp is a **sensor data platform** build in Rust, to handle time-series data from the edge to big data.

It's mostly a HTTP REST API supporting various formats and being compatible with Prometheus and InfluxDB. It connects to a variety of databases using a bespoke and good schema.

You will find more information about this in the [README.md](README.md), [ARCHITECTURE.md](docs/ARCHITECTURE.md), and [DATAMODEL.md](docs/DATAMODEL.md) files.

## Development rules

Always run the tests and linters before submitting changes. Use `cargo check`, `cargo test`, and `cargo clippy`. You may use `cargo fmt` to format the code.

Moreover, you can run the linters on the tests with `cargo clippy --tests`.

## Development guide

- Breaking changes are encouraged and we do not need to maintain backward compatibility. This is a pre-production project.
- We follow a KISS (Keep It Simple, Stupid) approach. Avoid unnecessary abstractions and complexity.
- During development, we focus on PostGreSQL as a main storage backend. Thus, you must only write tests in a generic way and not cut corners.
- unit tests and integrations tests are very helpful and appreciated. Consider doing them even when not actively requested. Testing is important.

## Task tracking

We use a folder-based task tracking system to maintain visibility into ongoing work, future ideas, and completed tasks:

- **`current_tasks/`** — Active tasks currently being worked on. Each task should be a separate markdown file describing the goal, context, and progress. When starting a major task, create a file here.
- **`ideas/`** — Ideas for future work, improvements, or features. Add a markdown file for each idea, even if it's rough or speculative.
- **`done/`** — Completed tasks. When a task from `current_tasks/` is finished, move it here. This provides a historical record of what has been accomplished.

**Rules:**
- Always check `current_tasks/` before starting work to understand what's in progress.
- When completing a major task, move its file from `current_tasks/` to `done/`.
- New ideas should go in `ideas/`, not in `current_tasks/` unless actively being worked on.
- Keep file names descriptive (e.g., `mqtt-ingestor-integration.md`, `rrdcached-fetch-support.md`).
- Maintain these folders — they are the project's source of truth for work status.
