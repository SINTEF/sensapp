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
