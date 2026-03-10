# Integrate MQTT Ingestor from sprint-antoine

## Context
The `sprint-antoine` branch contains a working MQTT ingestor implementation using the `rumqttc` crate.
It includes:
- `src/config/mqtt.rs` — MQTT configuration (broker URL, credentials, topics)
- `src/ingestors/mqtt/mod.rs` — Module setup
- `src/ingestors/mqtt/mqtt_client.rs` — MQTT client that subscribes to topics, parses SenML messages, and publishes them to storage

## Why it wasn't merged
The sprint-antoine branch diverged too far from main (39 merge conflicts, 61 commits behind).
The MQTT ingestor depends on sprint-antoine's event bus system (`src/bus/`), which is WIP and has debug prints.

## What needs to be done
1. Port the MQTT client code to work with the current `src/http/` module structure
2. Remove the dependency on the event bus — call storage directly or use a simpler pattern
3. Add MQTT configuration to the config module
4. Add MQTT dependencies to Cargo.toml (`rumqttc`)
5. Write integration tests
6. Update documentation

## Reference
Branch: `origin/sprint-antoine`
Key files: `src/ingestors/mqtt/mqtt_client.rs`, `src/config/mqtt.rs`
