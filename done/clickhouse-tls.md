# ClickHouse TLS

## Goal

Allow SensApp to connect directly to a ClickHouse HTTPS endpoint.

## Context

The ClickHouse connection parser currently constructs an `http://` URL regardless of deployment needs.

## Work

- Accept a simple, documented secure ClickHouse connection form while preserving existing HTTP configuration.
- Ensure both normal queries and database creation use HTTPS for secure connections.
- Add focused parser and connection tests, and update the ClickHouse deployment guide.

## Done when

A TLS ClickHouse URL is passed to the client for all connection paths, and existing HTTP URLs still work.

## Progress

- Added `clickhouses://` with HTTPS and a default port of 8443.
- Enabled ClickHouse client's Rustls TLS feature.
- Reused the same endpoint URL for regular queries and database creation.
- Added connection parsing tests and deployment guidance.
