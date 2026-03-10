# Complete RRDCached Storage Backend

## Context
The rrdcached branch has been merged. The storage backend now supports:
- TCP and Unix socket client abstraction (trait-based)
- Create, batch insert, flush operations
- List series via RRDcached LIST command
- Basic fetch support (WIP)
- Integration test framework

## What needs to be done
1. Complete `query_sensor_data` implementation — currently partially working with fetch
2. Implement proper pagination in `list_series` (currently ignores limit/bookmark)
3. Improve error handling and reconnection logic
4. Test with real rrdcached instances
5. Consider supporting more RRD consolidation functions
6. Update documentation with production deployment guide

## Reference
Docs: `docs/RRDCACHED.md`
Tests: `tests/rrdcached_integration.rs`
Code: `src/storage/rrdcached/mod.rs`
