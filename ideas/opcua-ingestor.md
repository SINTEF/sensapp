# OPC-UA Ingestor

## Context
The `sprint-antoine` branch contains an OPC-UA ingestor using the `opcua` crate, including:
- `src/ingestors/opcua/opcua_browser.rs` — Browse OPC-UA server namespace for available nodes
- `src/ingestors/opcua/opcua_client.rs` — Subscribe to OPC-UA data change notifications
- `src/ingestors/opcua/opcua_utils.rs` — Utility functions for OPC-UA value conversion
- `src/config/opcua.rs` — Configuration for OPC-UA connection parameters
- `docs/OPCUA_AUTODISCOVERY.md` — Documentation for auto-discovery feature

## Why it's an idea (not current task)
The OPC-UA ingestor is more experimental than MQTT and depends on the event bus system.
The `opcua` crate is large and adds significant build complexity.
This should be considered after MQTT is integrated.

## What it would involve
1. Port OPC-UA code to work without the event bus
2. Add `opcua` dependency (feature-gated)
3. Implement auto-discovery of OPC-UA nodes
4. Support multiple data types from OPC-UA
5. Integration testing with an OPC-UA simulator

## Reference
Branch: `origin/sprint-antoine`
Key files: `src/ingestors/opcua/`, `docs/OPCUA_AUTODISCOVERY.md`
