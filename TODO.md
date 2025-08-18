# SensApp Research Prototype Refactoring TODO

This document tracks the comprehensive refactoring plan for SensApp to create a focused research prototype for storage backend comparison.

## Phase 1: Remove Event Bus & Simplify Architecture (Week 1)

### 🔄 Architecture Simplification

- [ ] Add connection pooling for storage backends
- [ ] Create proper storage factory with runtime selection via `settings.toml`
- [ ] Simplify `main.rs` initialization
- [ ] Keep ALL storage backends for research comparison

## Phase 2: Add ClickHouse Storage Backend (Week 1)

### 🗄️ ClickHouse Implementation

- [ ] Create `src/storage/clickhouse/` module
- [ ] Implement ClickHouse storage trait
- [ ] Add ClickHouse migrations
- [ ] Include ClickHouse in storage factory
- [ ] Document ClickHouse-specific optimizations (LowCardinality, etc.)
- [ ] Add ClickHouse connection string parsing
- [ ] Test ClickHouse backend with sample data

### ✅ Completed

- [ ] None yet

## Phase 3: Add Comprehensive Read API (Week 2)

### 📊 Data Retrieval API

- [ ] Implement `GET /sensors` endpoint (list all sensors)
- [ ] Implement `GET /sensors/{id}` endpoint (sensor metadata)
- [ ] Implement `GET /sensors/{id}/data` endpoint (time series data)
- [ ] Support time range queries with `start/end` parameters
- [ ] Support data format selection: CSV, JSON Lines, SenML, Apache Avro
- [ ] Add optional pagination (`limit/offset` parameters)
- [ ] Add aggregation endpoints for research metrics
- [ ] Implement data export in multiple formats
- [ ] Add query performance metrics collection

### ✅ Completed

- [ ] None yet

## Phase 4: Testing Infrastructure (Week 2)

### 🧪 Unit Testing

- [ ] Unit tests for SQLite storage backend
- [ ] Unit tests for PostgreSQL storage backend
- [ ] Unit tests for TimescaleDB storage backend
- [ ] Unit tests for DuckDB storage backend
- [ ] Unit tests for BigQuery storage backend
- [ ] Unit tests for RRDCached storage backend
- [ ] Unit tests for ClickHouse storage backend (when implemented)
- [ ] Unit tests for data type inference (`src/infer/`)
- [ ] Unit tests for CSV parsing (`src/importers/`)
- [ ] Unit tests for Prometheus parsing (`src/parsing/`)

### 🔗 Integration Testing

- [ ] Integration tests for HTTP endpoints with all data formats
- [ ] Integration tests for MQTT ingestion
- [ ] Integration tests for InfluxDB compatibility endpoints
- [ ] Integration tests for Prometheus compatibility endpoints
- [ ] Cross-storage backend data consistency tests

### ⚡ Performance Benchmarks

- [ ] Write latency benchmarks per storage backend
- [ ] Read latency benchmarks per storage backend
- [ ] Storage space efficiency comparison
- [ ] Concurrent write performance tests
- [ ] Test harness for automated storage backend comparison

### ✅ Completed

- [ ] None yet

## Phase 5: Configuration & Observability (Week 3)

### ⚙️ Configuration Management

- [ ] Move ALL storage configs to `settings.toml`
- [ ] Add runtime storage backend selection
- [ ] Add connection pooling configuration
- [ ] Add batch processing configuration
- [ ] Environment variable override support

### 📈 Metrics & Monitoring

- [ ] Add Prometheus metrics endpoint (`/metrics`)
- [ ] Write latency metrics per storage backend
- [ ] Query latency metrics per storage backend
- [ ] Data ingestion rate metrics
- [ ] Storage-specific metrics (space usage, connections)
- [ ] Error rate metrics per backend
- [ ] Queue depth and processing metrics

### 📝 Structured Logging

- [ ] Implement structured logging with `tracing`
- [ ] Log storage backend selection decisions
- [ ] Log performance metrics
- [ ] Log error details with context
- [ ] Add request tracing correlation IDs

### 🏥 Health Monitoring

- [ ] Health check endpoint (`/health`)
- [ ] Storage backend connectivity checks
- [ ] Database migration status checks
- [ ] Resource usage health indicators

### ✅ Completed

- [ ] None yet

## Phase 6: Streamline Ingestion (Week 3)

### 🔧 Ingestion Simplification

- [ ] Keep HTTP ingestion (all format support)
- [ ] Keep MQTT for ingestion only
- [ ] Remove OPC UA to separate crate/service
- [ ] Remove AMQP planning/references
- [ ] Optimize batch processing without event bus
- [ ] Add ingestion rate limiting
- [ ] Add backpressure handling

### ✅ Completed

- [ ] None yet

## Phase 7: Research Tools & Documentation (Week 4)

### 🔬 Research Features

- [ ] Add storage backend comparison endpoints
- [ ] Create benchmark suite for storage comparison
- [ ] Add metrics dashboard configuration (Grafana)
- [ ] Performance comparison report generation
- [ ] Data consistency verification tools
- [ ] Storage backend switching without data loss

### 📚 Documentation

- [ ] Document storage backend trade-offs
- [ ] Create research data collection guide
- [ ] Performance comparison methodology
- [ ] Deployment guide for each storage backend
- [ ] API documentation updates
- [ ] Configuration reference guide

### ✅ Completed

- [ ] None yet

## Key Principles for Research Prototype

- ✅ Maintain all storage backends for comparison
- ✅ Focus on measurement and observability
- ✅ Make storage selection runtime configurable
- ✅ Prioritize research flexibility over production optimization
- ✅ Keep ingestion simple (HTTP + MQTT only)
- ✅ Remove unnecessary complexity (event bus, OPC UA)

## Session Notes

### Session 1: Analysis and Planning (Current)

**Completed:**

- Created comprehensive project plan and TODO document
- Analyzed event bus usage patterns across codebase
- Identified all locations where event bus is used for storage calls
- Designed direct storage call pattern to replace event bus
- Started refactoring: Updated `HttpServerState` to remove event bus dependency

**Key Findings:**

- Event bus creates single-threaded bottleneck in `main.rs:178-210`
- All storage calls are serialized through single consumer task
- Event bus is used in 11 files: CSV importers, MQTT/OPC UA clients, HTTP endpoints, batch builder
- Current pattern: `event_bus.publish(batch)` → single consumer → `storage.publish(batch)`
- New pattern: Direct `storage.publish(batch)` calls for parallel processing

**Next Session Plan:**

- Update `batch_builder.rs` to use direct storage calls instead of event bus
- Refactor one simple HTTP endpoint as proof-of-concept
- Update `main.rs` to remove event bus consumer task
- Test that storage calls work without event bus

- Session 2: _Continue event bus removal_
- Session 3: _To be filled in next session_

---

_This document should be updated after each work session to track progress and maintain momentum across multiple sessions._
