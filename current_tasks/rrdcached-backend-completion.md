# Complete RRDCached Storage Backend

## Context

The rrdcached branch has been merged. The storage backend now supports:

- TCP and Unix socket client abstraction (trait-based)
- Create, batch insert, flush operations
- List series via RRDcached LIST command with pagination support
- Fetch support for querying sensor data
- Label-based query support (query_sensors_by_labels)
- Integration test framework
- Local Docker test service and cargo-make checks

## What needs to be done

1. ~~Complete `query_sensor_data` implementation~~ — **DONE**
2. ~~Implement proper pagination in `list_series`~~ — **DONE** (limit/bookmark support)
3. ~~Implement `query_sensors_by_labels`~~ — **DONE** (name matching + label filtering)
4. ~~Improve error handling and reconnection logic~~ — **DONE** (retry-on-reconnectable client failures + unit tests)
5. ~~Test with real rrdcached instances~~ — **DONE** (dedicated integration tests + local compose service)
6. ~~Add CI coverage for the `rrdcached` feature~~ — **DONE** (backend-specific build and test path in GitHub Actions)
7. Consider supporting more RRD consolidation functions
8. Decide whether RRDCached should gain stronger test cleanup/isolation semantics or remain on backend-specific tests only
9. Update documentation with production deployment guide

## Current assessment

The backend is now feature-complete enough for its current experimental scope. CI coverage is in place, but the full generic backend matrix is still not a valid target for RRDCached because persistent `.rrd` files and limited metadata support break cleanup assumptions used by some shared tests.

## Reference

Docs: `docs/RRDCACHED.md`
Tests: `tests/rrdcached_integration.rs`
Code: `src/storage/rrdcached/mod.rs`
