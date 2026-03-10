# Event Bus / Message System

## Context
The `sprint-antoine` branch had an event bus system (`src/bus/`) for decoupling ingestors from storage backends:
- `EventBus` — broadcast channel for `Publish` messages
- `Message` / `PublishMessage` — wraps a `Batch` with a sync acknowledgment channel
- `wait_for_all` — waits for all storage backends to acknowledge persistence

## Why it's an idea
The implementation was WIP with debug prints. The current architecture calls storage directly,
which is simpler and works well for the single HTTP ingestor. An event bus becomes valuable
when multiple ingestors (MQTT, OPC-UA, etc.) need to publish simultaneously.

## Considerations
- Could use `tokio::sync::broadcast` instead of `async_broadcast`
- May be needed when MQTT and OPC-UA ingestors are added
- Keep it simple — avoid over-engineering until there's a clear need

## Reference
Branch: `origin/sprint-antoine`
Files: `src/bus/event_bus.rs`, `src/bus/message.rs`, `src/bus/wait_for_all.rs`
